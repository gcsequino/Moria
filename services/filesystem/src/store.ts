import { randomUUID } from 'node:crypto';
import type { PoolClient } from 'pg';
import { Catalog } from '../../catalog/src/catalog.js';
import { fingerprint } from '../../../packages/compiler/src/index.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import {
  filesystemRoot,
  type FileContent,
  type FilesystemActor,
  type FilesystemChange,
  type FilesystemEntry,
  type FilesystemReceipt,
} from '../../../packages/contracts/filesystem.js';
import { location } from '../../storage/src/locations.js';

const columns = `id::text,parent_id::text AS "parentId",name,kind,revision::text,
  project_id AS "projectId",environment_id AS "environmentId",resource_id::text AS "resourceId",
  content,deleted`;

export interface FilesystemMutationGuard {
  environment: string;
  client?: PoolClient;
  requestHash?: string;
  current(client: PoolClient): Promise<void>;
  change(client: PoolClient, change: FilesystemChange, replay: boolean): Promise<void>;
  receipt(client: PoolClient, receipt: FilesystemReceipt): Promise<void>;
}

function invalid(message: string): never {
  throw new MoriaError('FILESYSTEM_INVALID', message);
}
function record(value: unknown, required: string[], optional: string[] = []): Record<string, any> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) invalid('Expected a record');
  const result = value as Record<string, any>;
  if (
    required.some((key) => !Object.hasOwn(result, key)) ||
    Object.keys(result).some((key) => ![...required, ...optional].includes(key))
  )
    invalid('Unexpected or missing fields');
  return result;
}
function token(value: unknown): asserts value is string {
  if (typeof value !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$/.test(value))
    invalid('Invalid identity or request key');
}
function uuid(value: unknown): asserts value is string {
  if (typeof value !== 'string' || !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(value))
    invalid('Expected a canonical UUID');
}
function revision(value: unknown): asserts value is string {
  if (
    typeof value !== 'string' ||
    !/^[1-9][0-9]{0,18}$/.test(value) ||
    BigInt(value) > 9223372036854775807n
  )
    invalid('Expected a positive int64 revision');
}
function name(value: unknown): asserts value is string {
  if (
    typeof value !== 'string' ||
    !value ||
    Buffer.byteLength(value) > 128 ||
    Buffer.from(value).toString('utf8') !== value ||
    value !== value.normalize('NFC') ||
    /[\x00-\x1f\x7f/\\]/.test(value) ||
    value === '.' ||
    value === '..'
  )
    invalid('Invalid entry name');
}
function content(value: unknown): asserts value is FileContent {
  const item = record(value, ['location', 'digest', 'size', 'mediaType']);
  location(item.location);
  if (
    typeof item.digest !== 'string' ||
    !/^sha256:[0-9a-f]{64}$/.test(item.digest) ||
    !Number.isSafeInteger(item.size) ||
    item.size < 0 ||
    typeof item.mediaType !== 'string' ||
    !/^[a-z0-9!#$&^_.+-]+\/[a-z0-9!#$&^_.+-]+$/.test(item.mediaType) ||
    item.mediaType.length > 128
  )
    invalid('Invalid immutable content descriptor');
}
export function validateFilesystemChange(value: unknown): asserts value is FilesystemChange {
  const change = value as FilesystemChange;
  switch (change?.op) {
    case 'create': {
      record(
        change,
        ['op', 'parentId', 'expectedParentRevision', 'name', 'kind'],
        ['scope', 'resourceId', 'content'],
      );
      uuid(change.parentId);
      revision(change.expectedParentRevision);
      name(change.name);
      if (!['directory', 'file', 'resource'].includes(change.kind)) invalid('Invalid entry kind');
      if (change.scope !== undefined) {
        const scope = record(change.scope, ['projectId'], ['environmentId']);
        token(scope.projectId);
        if (scope.environmentId !== undefined) token(scope.environmentId);
      }
      if (change.kind === 'file') content(change.content);
      else if (change.content !== undefined) invalid('Only files contain bytes');
      if (change.kind === 'resource') uuid(change.resourceId);
      else if (change.resourceId !== undefined) invalid('Only resources have resource IDs');
      break;
    }
    case 'write':
      record(change, ['op', 'id', 'expectedRevision', 'content']);
      uuid(change.id);
      revision(change.expectedRevision);
      content(change.content);
      break;
    case 'move':
      record(change, [
        'op',
        'id',
        'expectedRevision',
        'parentId',
        'expectedParentRevision',
        'expectedSourceParentRevision',
        'name',
      ]);
      uuid(change.id);
      revision(change.expectedRevision);
      uuid(change.parentId);
      revision(change.expectedParentRevision);
      revision(change.expectedSourceParentRevision);
      name(change.name);
      break;
    case 'delete':
      record(change, ['op', 'id', 'expectedRevision', 'expectedParentRevision']);
      uuid(change.id);
      revision(change.expectedRevision);
      revision(change.expectedParentRevision);
      break;
    default:
      invalid('Unsupported filesystem change');
  }
}
function expect(entry: FilesystemEntry, expected: string) {
  if (entry.revision !== expected)
    throw new MoriaError('FILESYSTEM_CONFLICT', 'Entry revision changed');
}
async function entry(client: PoolClient, id: string): Promise<FilesystemEntry> {
  const result = await client.query(
    `SELECT ${columns} FROM moria.fs_entries WHERE id=$1 AND NOT deleted`,
    [id],
  );
  if (!result.rowCount) throw new MoriaError('NOT_FOUND', 'Entry not found');
  return result.rows[0];
}
async function directory(client: PoolClient, id: string): Promise<FilesystemEntry> {
  const result = await entry(client, id);
  if (result.kind !== 'directory') invalid('Parent must be a directory');
  return result;
}
async function placement(
  client: PoolClient,
  parentId: string,
  entryName: string,
  movingId?: string,
) {
  const ancestors = await client.query(
    `WITH RECURSIVE ancestors AS (
    SELECT id,parent_id,name FROM moria.fs_entries WHERE id=$1
    UNION ALL SELECT e.id,e.parent_id,e.name FROM moria.fs_entries e JOIN ancestors a ON e.id=a.parent_id
  ) SELECT count(*)::int AS depth,sum(octet_length(name)+1)::int AS bytes FROM ancestors`,
    [parentId],
  );
  let depth = ancestors.rows[0].depth,
    bytes = ancestors.rows[0].bytes + Buffer.byteLength(entryName);
  if (movingId) {
    const descendants = await client.query(
      `WITH RECURSIVE descendants AS (
      SELECT id,0 AS depth,0 AS bytes FROM moria.fs_entries WHERE id=$1
      UNION ALL SELECT e.id,d.depth+1,d.bytes+octet_length(e.name)+1 FROM moria.fs_entries e
        JOIN descendants d ON e.parent_id=d.id WHERE NOT e.deleted
    ) SELECT max(depth) AS depth,max(bytes) AS bytes FROM descendants`,
      [movingId],
    );
    depth += descendants.rows[0].depth;
    bytes += descendants.rows[0].bytes;
  }
  if (depth > 64 || bytes > 4096) invalid('Placement exceeds path limits');
}

/** Trusted internal persistence only. Callers must enforce current policy and qualify
 * content before mutation; this store is not exposed to users or arbitrary workers. */
export class FilesystemStore {
  constructor(readonly catalog: Catalog) {}

  async get(id: string, client?: PoolClient): Promise<FilesystemEntry> {
    uuid(id);
    if (client) return entry(client, id);
    return this.catalog.transaction((client) => entry(client, id), undefined, 'repeatable-read');
  }

  async resolve(path: string, client?: PoolClient): Promise<FilesystemEntry> {
    if (typeof path !== 'string' || !path.startsWith('/') || Buffer.byteLength(path) > 4096)
      invalid('Expected an absolute logical path');
    const parts = path === '/' ? [] : path.slice(1).split('/');
    if (parts.length > 64) invalid('Path exceeds maximum depth');
    parts.forEach(name);
    const read = async (client: PoolClient) => {
      let current = await entry(client, filesystemRoot);
      for (const part of parts) {
        if (current.kind !== 'directory') throw new MoriaError('NOT_FOUND', 'Entry not found');
        const found = await client.query(
          'SELECT id FROM moria.fs_entries WHERE parent_id=$1 AND name=$2 AND NOT deleted',
          [current.id, part],
        );
        if (!found.rowCount) throw new MoriaError('NOT_FOUND', 'Entry not found');
        current = await entry(client, found.rows[0].id);
      }
      return current;
    };
    return client ? read(client) : this.catalog.transaction(read, undefined, 'repeatable-read');
  }

  async list(parentId: string): Promise<FilesystemEntry[]> {
    uuid(parentId);
    return this.catalog.transaction(
      async (client) => {
        await directory(client, parentId);
        const result = await client.query(
          `SELECT ${columns} FROM moria.fs_entries WHERE parent_id=$1 AND NOT deleted ORDER BY name,id LIMIT 1001`,
          [parentId],
        );
        // shortcut: lists stop at 1,000 children; add revision-bound pagination before larger directories.
        if (result.rows.length > 1000)
          throw new MoriaError('LIMIT_EXCEEDED', 'Directory requires pagination');
        return result.rows;
      },
      undefined,
      'repeatable-read',
    );
  }

  async version(id: string, version: string, client?: PoolClient): Promise<FilesystemEntry> {
    uuid(id);
    revision(version);
    const result = await (client ?? this.catalog.pool).query(
      'SELECT snapshot FROM moria.fs_entry_versions WHERE entry_id=$1 AND revision=$2',
      [id, version],
    );
    if (!result.rowCount) throw new MoriaError('NOT_FOUND', 'Entry revision not found');
    return result.rows[0].snapshot;
  }

  async scan(
    client: PoolClient,
    parentId: string,
    after: { name: string; id: string } | null,
  ): Promise<FilesystemEntry[]> {
    uuid(parentId);
    if (after) {
      name(after.name);
      uuid(after.id);
    }
    await directory(client, parentId);
    const result = await client.query(
      `SELECT ${columns} FROM moria.fs_entries
      WHERE parent_id=$1 AND NOT deleted
        AND ($2::text IS NULL OR (name,id)>($2::text COLLATE "C",$3::uuid))
      ORDER BY name,id LIMIT 101`,
      [parentId, after?.name ?? null, after?.id ?? null],
    );
    return result.rows;
  }

  async apply(
    actorInput: FilesystemActor,
    key: string,
    input: readonly FilesystemChange[],
    guard?: FilesystemMutationGuard,
  ): Promise<FilesystemReceipt> {
    const actor = structuredClone(actorInput),
      changes = structuredClone(input);
    record(actor, ['environment', 'principal', 'application'], ['delegation']);
    token(actor.environment);
    token(actor.principal);
    token(actor.application);
    token(key);
    if (actor.delegation !== undefined) {
      if (!Array.isArray(actor.delegation) || actor.delegation.length > 16)
        invalid('Invalid delegation chain');
      actor.delegation.forEach(token);
    }
    actor.delegation ??= [];
    if (!Array.isArray(changes) || !changes.length || changes.length > 100)
      invalid('Expected 1–100 ordered changes');
    changes.forEach(validateFilesystemChange);
    const scopeHash = fingerprint(actor),
      requestHash = guard?.requestHash ?? fingerprint(changes);
    try {
      const mutate = async (client: PoolClient) => {
        await client.query("SET LOCAL statement_timeout='10s'");
        await guard?.current(client);
        const previous = await client.query(
          'SELECT request_hash,receipt FROM moria.fs_mutations WHERE scope_hash=$1 AND request_key=$2',
          [scopeHash, key],
        );
        if (previous.rowCount) {
          if (previous.rows[0].request_hash !== requestHash)
            throw new MoriaError(
              'IDEMPOTENCY_CONFLICT',
              'Request key already committed different changes',
            );
          for (const change of changes) await guard?.change(client, change, true);
          await guard?.receipt(client, previous.rows[0].receipt);
          return previous.rows[0].receipt;
        }
        const mutationId = randomUUID(),
          touched = new Map<string, FilesystemEntry>();
        const remember = async (value: FilesystemEntry) => {
          touched.set(value.id, value);
          await client.query(
            'INSERT INTO moria.fs_entry_versions(entry_id,revision,snapshot,mutation_id) VALUES($1,$2,$3,$4)',
            [value.id, value.revision, JSON.stringify(value), mutationId],
          );
        };
        const touch = async (id: string) => {
          const result = await client.query(
            `UPDATE moria.fs_entries SET revision=revision+1 WHERE id=$1 RETURNING ${columns}`,
            [id],
          );
          await remember(result.rows[0]);
        };
        for (const change of changes) {
          await guard?.change(client, change, false);
          if (change.op === 'create') {
            const parent = await directory(client, change.parentId);
            expect(parent, change.expectedParentRevision);
            const project = change.scope?.projectId ?? parent.projectId;
            const environment = change.scope?.environmentId ?? parent.environmentId;
            if (
              (parent.projectId !== null && project !== parent.projectId) ||
              (parent.environmentId !== null && environment !== parent.environmentId) ||
              (change.scope !== undefined && change.kind !== 'directory')
            )
              invalid('Scope boundaries must be compatible directories');
            if (change.kind === 'resource' && (!project || !environment))
              invalid('Runtime resources require a project and environment');
            await placement(client, parent.id, change.name);
            const created = await client.query(
              `INSERT INTO moria.fs_entries(id,parent_id,name,kind,project_id,environment_id,resource_id,content)
              VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING ${columns}`,
              [
                randomUUID(),
                parent.id,
                change.name,
                change.kind,
                project,
                environment,
                change.resourceId ?? null,
                change.content ? JSON.stringify(change.content) : null,
              ],
            );
            await remember(created.rows[0]);
            await touch(parent.id);
            continue;
          }
          const current = await entry(client, change.id);
          expect(current, change.expectedRevision);
          if (current.id === filesystemRoot) invalid('Root cannot be changed directly');
          if (change.op === 'write') {
            if (current.kind !== 'file') invalid('Only file contents can be replaced');
            const updated = await client.query(
              `UPDATE moria.fs_entries SET content=$2,revision=revision+1 WHERE id=$1 RETURNING ${columns}`,
              [current.id, JSON.stringify(change.content)],
            );
            await remember(updated.rows[0]);
            continue;
          }
          const parent = await directory(client, current.parentId!);
          expect(
            parent,
            change.op === 'move'
              ? change.expectedSourceParentRevision
              : change.expectedParentRevision,
          );
          if (change.op === 'move') {
            const target = await directory(client, change.parentId);
            expect(target, change.expectedParentRevision);
            if (
              parent.projectId !== target.projectId ||
              parent.environmentId !== target.environmentId
            )
              invalid('Moving across scope boundaries requires explicit transfer');
            const cycle = await client.query(
              `WITH RECURSIVE ancestors AS (
              SELECT id,parent_id FROM moria.fs_entries WHERE id=$1
              UNION ALL SELECT e.id,e.parent_id FROM moria.fs_entries e JOIN ancestors a ON e.id=a.parent_id
            ) SELECT 1 FROM ancestors WHERE id=$2`,
              [target.id, current.id],
            );
            if (cycle.rowCount) invalid('Move would create a directory cycle');
            await placement(client, target.id, change.name, current.id);
            const updated = await client.query(
              `UPDATE moria.fs_entries SET parent_id=$2,name=$3,revision=revision+1 WHERE id=$1 RETURNING ${columns}`,
              [current.id, target.id, change.name],
            );
            await remember(updated.rows[0]);
            await touch(parent.id);
            if (target.id !== parent.id) await touch(target.id);
          } else {
            if (current.kind === 'resource')
              throw new MoriaError(
                'UNSUPPORTED_CAPABILITY',
                'Retire typed resources through their domain service',
              );
            const children = await client.query(
              'SELECT 1 FROM moria.fs_entries WHERE parent_id=$1 AND NOT deleted LIMIT 1',
              [current.id],
            );
            if (children.rowCount) invalid('Directory must be empty before deletion');
            const deleted = await client.query(
              `UPDATE moria.fs_entries SET deleted=true,revision=revision+1 WHERE id=$1 RETURNING ${columns}`,
              [current.id],
            );
            await remember(deleted.rows[0]);
            await touch(parent.id);
          }
        }
        const receipt: FilesystemReceipt = { mutationId, entries: [...touched.values()] };
        await guard?.receipt(client, receipt);
        await client.query(
          'INSERT INTO moria.fs_mutations(id,scope_hash,request_key,request_hash,actor,receipt) VALUES($1,$2,$3,$4,$5,$6)',
          [mutationId, scopeHash, key, requestHash, JSON.stringify(actor), JSON.stringify(receipt)],
        );
        return receipt;
      };
      return guard?.client
        ? await mutate(guard.client)
        : await this.catalog.transaction(
            mutate,
            guard?.environment,
            'serializable',
            'exclusive',
            'exclusive',
          );
    } catch (error) {
      if ((error as { code?: string }).code === '23505')
        throw new MoriaError('FILESYSTEM_CONFLICT', 'Name or resource placement already exists');
      throw error;
    }
  }
}
