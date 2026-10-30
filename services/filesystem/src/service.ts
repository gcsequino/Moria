import type { PoolClient } from 'pg';
import { randomUUID } from 'node:crypto';
import { FilesystemStore, validateFilesystemChange } from './store.js';
import { verifiedContent, type FilesystemStorageBinding } from './content.js';
import { identity } from '../../storage/src/locations.js';
import { fingerprint } from '../../../packages/compiler/src/index.js';
import {
  FilesystemAuthority,
  type FilesystemContext,
  type FilesystemSnapshot,
} from '../../security/src/filesystem.js';
import { CursorStore, type CursorBinding } from '../../ontology/src/cursors.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import type {
  FileContent,
  FilesystemFileWrite,
  FilesystemChange,
  FilesystemEntry,
  FilesystemOperation,
} from '../../../packages/contracts/filesystem.js';

/** Session contexts must come from verified authentication/delegation composition. */
export class FilesystemService {
  private readonly content?: Readonly<FilesystemStorageBinding>;
  constructor(
    readonly store: FilesystemStore,
    readonly security: FilesystemAuthority,
    binding?: FilesystemStorageBinding,
  ) {
    if (store.catalog !== security.authority.catalog)
      throw new Error('Filesystem and security must share a catalog');
    if (binding) {
      identity(binding.warehouseId);
      identity(binding.tableId);
      if (
        !Number.isSafeInteger(binding.maxFileBytes) ||
        binding.maxFileBytes < 0 ||
        typeof binding.storage !== 'function'
      )
        throw new Error('Invalid filesystem storage binding');
      this.content = Object.freeze({ ...binding });
    }
  }

  async visible(
    client: PoolClient,
    state: FilesystemSnapshot,
    id: string,
    operation: FilesystemOperation,
  ) {
    try {
      await this.security.check(client, state, id, operation);
    } catch (error) {
      if (['FORBIDDEN', 'NOT_FOUND'].includes((error as MoriaError).code))
        throw new MoriaError('NOT_FOUND', 'Entry not found');
      throw error;
    }
  }

  protectedRead<T>(
    input: FilesystemContext,
    run: (client: PoolClient, state: FilesystemSnapshot) => Promise<T>,
  ): Promise<T> {
    const context = structuredClone(input);
    return this.store.catalog.transaction(
      async (client) => {
        await client.query("SET LOCAL statement_timeout='10s'");
        const state = await this.security.current(client, context);
        const result = await run(client, state);
        await this.security.finish(client, state);
        return result;
      },
      context.session.environment,
      'repeatable-read',
      'shared',
      'shared',
    );
  }

  get(context: FilesystemContext, id: string): Promise<FilesystemEntry> {
    return this.protectedRead(context, async (client, state) => {
      await this.visible(client, state, id, 'read');
      return this.store.get(id, client);
    });
  }

  resolve(context: FilesystemContext, path: string): Promise<FilesystemEntry> {
    return this.protectedRead(context, async (client, state) => {
      const entry = await this.store.resolve(path, client);
      await this.visible(client, state, entry.id, 'read');
      return entry;
    });
  }

  version(context: FilesystemContext, id: string, revision: string): Promise<FilesystemEntry> {
    return this.protectedRead(context, async (client, state) => {
      await this.visible(client, state, id, 'read');
      return this.store.version(id, revision, client);
    });
  }

  history(context: FilesystemContext, id: string, before?: string) {
    if (
      before !== undefined &&
      (!/^[1-9][0-9]{0,18}$/.test(before) || BigInt(before) > 9223372036854775807n)
    )
      throw new MoriaError('FILESYSTEM_INVALID', 'Invalid history revision');
    return this.protectedRead(context, async (client, state) => {
      await this.visible(client, state, id, 'read');
      const rows = (
        await client.query(
          'SELECT snapshot FROM moria.fs_entry_versions WHERE entry_id=$1 AND ($2::bigint IS NULL OR revision<$2) ORDER BY revision DESC LIMIT 101',
          [id, before ?? null],
        )
      ).rows.map((row) => row.snapshot as FilesystemEntry);
      return {
        entries: rows.slice(0, 100),
        ...(rows.length > 100 ? { nextRevision: rows[99].revision } : {}),
      };
    });
  }

  private async managedChange(client: PoolClient, id: string, destination?: string) {
    if (
      (
        await client.query(
          `SELECT 1 FROM moria.fs_roles WHERE configuration->>'configuration'=$1 OR configuration->>'memory'=$1 OR configuration->>'runs'=$1 OR configuration->>'projects'=$1 LIMIT 1`,
          [id],
        )
      ).rowCount
    ) {
      const current = await this.store.get(id, client);
      if (!destination || destination !== current.parentId)
        throw new MoriaError(
          'FILESYSTEM_INVALID',
          'Managed home directories must remain in their managed parent',
        );
    }
    const role = (
      await client.query('SELECT kind,retired FROM moria.fs_roles WHERE entry_id=$1', [id])
    ).rows[0];
    if (role && ((!destination && !role.retired) || role.kind === 'system'))
      throw new MoriaError(
        'FILESYSTEM_INVALID',
        'Managed directories require their lifecycle operation',
      );
    if (!destination) return;
    const sources = await client.query(
      `WITH RECURSIVE tree AS (
      SELECT id FROM moria.fs_entries WHERE id=$1 UNION ALL
      SELECT e.id FROM moria.fs_entries e JOIN tree t ON e.parent_id=t.id WHERE NOT e.deleted
    ), ancestors AS (
      SELECT id,parent_id FROM moria.fs_entries WHERE id=$2 UNION ALL
      SELECT e.id,e.parent_id FROM moria.fs_entries e JOIN ancestors a ON e.id=a.parent_id
    ) SELECT 1 WHERE EXISTS(SELECT 1 FROM moria.fs_roles WHERE entry_id IN (SELECT id FROM tree) AND kind IN ('ontology','repository') AND NOT retired)
      AND EXISTS(SELECT 1 FROM moria.fs_roles WHERE entry_id IN (SELECT id FROM ancestors) AND kind IN ('ontology','repository') AND NOT retired)`,
      [id, destination],
    );
    if (sources.rowCount)
      throw new MoriaError('SOURCE_INVALID', 'Source roots cannot overlap or nest');
    if (role && ['group', 'user', 'agent'].includes(role.kind)) {
      const current = await this.store.get(id, client);
      if (current.parentId !== destination)
        throw new MoriaError(
          'FILESYSTEM_INVALID',
          'Principal and group homes must stay in their managed parent',
        );
    }
  }

  list(
    context: FilesystemContext,
    parentId: string,
    cursor?: string,
  ): Promise<{ entries: FilesystemEntry[]; nextCursor?: string }> {
    if (cursor !== undefined && (typeof cursor !== 'string' || !/^[A-Za-z0-9_-]{43}$/.test(cursor)))
      throw new MoriaError('CURSOR_INVALID', 'Invalid cursor');
    return this.protectedRead(context, async (client, state) => {
      await this.visible(client, state, parentId, 'list');
      const parent = await this.store.get(parentId, client);
      const member = state.members[0],
        cursors = new CursorStore(this.store.catalog);
      const binding: CursorBinding = {
        environment: member.session.environment,
        principal: member.id,
        application: member.session.application,
        delegation: state.members.slice(1).map((item) => item.id),
        contract: 'filesystem-list-v1',
        securityEpoch: member.state.epoch,
        query: { parentId },
      };
      const pin = cursor
        ? await cursors.resumeIn(client, cursor, binding)
        : {
            revision: (
              await client.query('SELECT revision FROM moria.environments WHERE id=$1', [
                binding.environment,
              ])
            ).rows[0].revision,
            expiresAt: (
              await client.query("SELECT clock_timestamp()+interval '15 minutes' AS expires_at")
            ).rows[0].expires_at.toISOString(),
            after: null,
          };
      const after = pin.after as {
        name: string;
        id: string;
        parentRevision: string;
        instanceEpoch: string;
      } | null;
      if (
        after &&
        (after.parentRevision !== parent.revision || after.instanceEpoch !== state.instanceEpoch)
      )
        throw new MoriaError('CURSOR_INVALIDATED', 'Directory or authorization changed');
      const candidates = await this.store.scan(client, parentId, after);
      const entries: FilesystemEntry[] = [];
      for (const entry of candidates.slice(0, 100)) {
        try {
          await this.visible(client, state, entry.id, 'read');
          entries.push(entry);
        } catch (error) {
          if ((error as MoriaError).code !== 'NOT_FOUND') throw error;
        }
      }
      if (candidates.length <= 100) return { entries };
      const last = candidates[99];
      return {
        entries,
        nextCursor: await cursors.saveIn(client, binding, pin, {
          name: last.name,
          id: last.id,
          parentRevision: parent.revision,
          instanceEpoch: state.instanceEpoch,
        }),
      };
    });
  }

  apply(input: FilesystemContext, key: string, inputChanges: readonly FilesystemChange[]) {
    return this.mutate(input, key, inputChanges);
  }

  copy(
    input: FilesystemContext,
    key: string,
    request: {
      id: string;
      expectedRevision: string;
      parentId: string;
      expectedParentRevision: string;
      name: string;
    },
  ) {
    const context = structuredClone(input),
      change = structuredClone(request);
    return this.store.catalog.transaction(
      async (client) => {
        const state = await this.security.current(client, context);
        await this.security.check(client, state, change.id, 'read');
        await this.security.check(client, state, change.id, 'export');
        await this.security.check(client, state, change.parentId, 'create');
        const original = await this.store.get(change.id, client);
        const destination = await this.store.get(change.parentId, client);
        if (
          original.projectId !== destination.projectId ||
          original.environmentId !== destination.environmentId
        )
          throw new MoriaError(
            'FILESYSTEM_INVALID',
            'Copy cannot cross project or environment boundaries',
          );
        if (original.kind !== 'file')
          throw new MoriaError('FILESYSTEM_INVALID', 'Copy operates on individual files');
        if (original.revision !== change.expectedRevision)
          throw new MoriaError('FILESYSTEM_CONFLICT', 'Source revision changed');
        const receipt = await this.store.apply(
          {
            environment: context.session.environment,
            principal: context.session.principal,
            application: context.session.application,
            delegation: context.delegation?.map((member) => member.principal) ?? [],
          },
          key,
          [
            {
              op: 'create',
              kind: 'file',
              parentId: change.parentId,
              expectedParentRevision: change.expectedParentRevision,
              name: change.name,
              content: original.content!,
            },
          ],
          {
            environment: context.session.environment,
            client,
            requestHash: fingerprint({ copy: change }),
            current: async () => {},
            change: async () => {},
            receipt: async () => {},
          },
        );
        const copied = receipt.entries.find(
          (entry) => entry.parentId === change.parentId && entry.name === change.name,
        )!;
        await client.query('INSERT INTO moria.fs_provenance VALUES($1,$2) ON CONFLICT DO NOTHING', [
          copied.id,
          original.id,
        ]);
        const markings = await this.security.effectiveMarkings(client, original.id);
        await client.query(
          `INSERT INTO moria.fs_policies(entry_id,retained_markings) VALUES($1,$2) ON CONFLICT(entry_id) DO UPDATE SET retained_markings=COALESCE((SELECT jsonb_agg(DISTINCT value) FROM jsonb_array_elements(fs_policies.retained_markings || EXCLUDED.retained_markings)),'[]')`,
          [copied.id, JSON.stringify(markings)],
        );
        await this.security.check(client, state, copied.id, 'read');
        await this.security.finish(client, state);
        return receipt;
      },
      context.session.environment,
      'serializable',
      'exclusive',
      'exclusive',
    );
  }

  async writeFile(
    input: FilesystemContext,
    key: string,
    inputRequest: FilesystemFileWrite,
    source: ReadableStream<Uint8Array>,
    signal?: AbortSignal,
  ) {
    if (!this.content)
      throw new MoriaError('UNSUPPORTED_CAPABILITY', 'File storage is not configured');
    const context = structuredClone(input),
      request = structuredClone(inputRequest);
    if (!request || !['create', 'write'].includes(request.op))
      throw new MoriaError('FILESYSTEM_INVALID', 'Expected a file create or write');
    const content: FileContent = {
      ...request.content,
      location: {
        version: 1,
        warehouseId: this.content.warehouseId,
        tableId: this.content.tableId,
        objectId: randomUUID(),
      },
    };
    const change: FilesystemChange =
      request.op === 'create' ? { ...request, kind: 'file', content } : { ...request, content };
    validateFilesystemChange(change);
    if (content.size > this.content.maxFileBytes)
      throw new MoriaError('LIMIT_EXCEEDED', 'File exceeds size limit');
    signal = AbortSignal.any([...(signal ? [signal] : []), AbortSignal.timeout(30 * 60 * 1000)]);
    const check = () =>
      this.protectedRead(context, async (client, state) => {
        const target = change.op === 'create' ? change.parentId : change.id;
        await this.security.check(client, state, target, 'read');
        await this.security.check(
          client,
          state,
          target,
          change.op === 'create' ? 'create' : 'write',
        );
        const entry = await this.store.get(target, client);
        if (entry.kind !== (change.op === 'create' ? 'directory' : 'file'))
          throw new MoriaError('FILESYSTEM_INVALID', 'Invalid file target');
        return true;
      });
    await check();
    await this.store.catalog.pool.query(
      'INSERT INTO moria.fs_content_candidates(object_id,location) VALUES($1,$2)',
      [content.location.objectId, JSON.stringify(content.location)],
    );
    const storage = await this.content.storage(
      { location: content.location, operations: ['create'] },
      { checkCurrent: check },
    );
    let integrityError: unknown;
    try {
      await storage.write(
        content.location,
        verifiedContent(source, content, signal, (error) => {
          integrityError = error;
        }),
        { size: content.size, signal },
      );
    } catch (error) {
      throw integrityError ?? error;
    }
    return this.mutate(context, key, [change], content);
  }

  collect(input: FilesystemContext) {
    const context = structuredClone(input);
    if (!this.content)
      throw new MoriaError('UNSUPPORTED_CAPABILITY', 'File storage is not configured');
    const binding = this.content;
    return this.store.catalog.transaction(
      async (client) => {
        const state = await this.security.current(client, context);
        for (const member of state.members) {
          if (
            !member.state.identity.grantedOperations.has('instance:manage') ||
            !member.state.identity.applicationScopes.has('instance:manage')
          )
            throw new MoriaError('FORBIDDEN', 'Instance administration is required');
        }
        const rows = (
          await client.query(`SELECT object_id,location FROM moria.fs_content_candidates c
        WHERE created_at<clock_timestamp()-interval '24 hours' ORDER BY created_at LIMIT 50`)
        ).rows;
        let removed = 0,
          retained = 0;
        const stagingRemoved = (await binding.collectStaging?.()) ?? 0;
        for (const row of rows) {
          const referenced = (
            await client.query(
              `SELECT 1 FROM moria.fs_entry_versions WHERE snapshot->'content'->'location'=$1::jsonb LIMIT 1`,
              [JSON.stringify(row.location)],
            )
          ).rowCount;
          if (referenced) {
            retained++;
          } else {
            const storage = await binding.storage(
              { location: row.location, operations: ['delete'] },
              {
                checkCurrent: async () => {
                  await this.security.finish(client, state);
                  return true;
                },
              },
            );
            if (!storage.remove)
              throw new MoriaError(
                'UNSUPPORTED_CAPABILITY',
                'Storage does not support orphan collection',
              );
            await storage.remove(row.location, { signal: AbortSignal.timeout(10000) });
            removed++;
          }
          await client.query('DELETE FROM moria.fs_content_candidates WHERE object_id=$1', [
            row.object_id,
          ]);
        }
        await this.security.audit(
          client,
          state.members[0].id,
          'collectFilesystemOrphans',
          String(removed),
        );
        await this.security.finish(client, state);
        return {
          removed,
          retained,
          examined: rows.length,
          ...(binding.collectStaging ? { stagingRemoved } : {}),
        };
      },
      context.session.environment,
      'serializable',
      'exclusive',
      'exclusive',
    );
  }

  async readFile(input: FilesystemContext, id: string, revision?: string, signal?: AbortSignal) {
    if (!this.content)
      throw new MoriaError('UNSUPPORTED_CAPABILITY', 'File storage is not configured');
    const context = structuredClone(input);
    const entry =
      revision === undefined
        ? await this.get(context, id)
        : await this.version(context, id, revision);
    if (entry.kind !== 'file' || !entry.content)
      throw new MoriaError('FILESYSTEM_INVALID', 'Entry is not a file');
    const expected = fingerprint(entry.content);
    const check = async () => {
      const current = await this.version(context, id, entry.revision);
      if (fingerprint(current.content) !== expected)
        throw new MoriaError('CONTENT_MISMATCH', 'File revision binding changed');
      return true;
    };
    const storage = await this.content.storage(
      { location: entry.content.location, operations: ['read'] },
      { checkCurrent: check },
    );
    return {
      entry,
      stream: verifiedContent(
        await storage.read(entry.content.location, { signal }),
        entry.content,
        signal,
      ),
    };
  }

  private mutate(
    input: FilesystemContext,
    key: string,
    inputChanges: readonly FilesystemChange[],
    qualified?: FileContent,
  ) {
    const context = structuredClone(input),
      changes = structuredClone(inputChanges);
    let state: FilesystemSnapshot;
    return this.store.apply(
      {
        environment: context.session.environment,
        principal: context.session.principal,
        application: context.session.application,
        delegation: context.delegation?.map((session) => session.principal) ?? [],
      },
      key,
      changes,
      {
        environment: context.session.environment,
        ...(qualified
          ? {
              requestHash: fingerprint(
                changes.map((change) =>
                  'content' in change && change.content
                    ? {
                        ...change,
                        content: {
                          digest: change.content.digest,
                          size: change.content.size,
                          mediaType: change.content.mediaType,
                        },
                      }
                    : change,
                ),
              ),
            }
          : {}),
        current: async (client) => {
          state = await this.security.current(client, context);
        },
        change: async (client, change, replay) => {
          const check = (id: string, operation: FilesystemOperation) =>
            this.security.check(client, state, id, operation);
          if (change.op === 'create') {
            await check(change.parentId, 'read');
            await check(change.parentId, 'create');
            if (
              (change.kind !== 'directory' &&
                !(
                  qualified &&
                  change.kind === 'file' &&
                  fingerprint(change.content) === fingerprint(qualified)
                )) ||
              change.scope !== undefined
            )
              throw new MoriaError(
                'UNSUPPORTED_CAPABILITY',
                'Content and scope registration require trusted service composition',
              );
          } else {
            await check(change.id, 'read');
            await check(
              change.id,
              change.op === 'move' ? 'move' : change.op === 'delete' ? 'delete' : 'write',
            );
            if (
              change.op === 'write' &&
              !(qualified && fingerprint(change.content) === fingerprint(qualified))
            )
              throw new MoriaError(
                'UNSUPPORTED_CAPABILITY',
                'File writes require qualified content transfer',
              );
            if (change.op === 'move') {
              await check(change.parentId, 'read');
              await check(change.parentId, 'create');
              if (!replay) {
                const current = await this.store.get(change.id, client);
                await this.managedChange(client, change.id, change.parentId);
                await check(current.parentId!, 'read');
                await check(current.parentId!, 'delete');
                await this.security.retain(client, state.members[0].id, change.id);
              }
            } else if (change.op === 'delete' && !replay) {
              const current = await this.store.get(change.id, client);
              await this.managedChange(client, change.id);
              await check(current.parentId!, 'read');
              await check(current.parentId!, 'delete');
            }
          }
        },
        receipt: async (client, receipt) => {
          for (const entry of receipt.entries)
            await this.security.check(client, state, entry.id, 'read');
          await this.security.finish(client, state);
        },
      },
    );
  }
}
