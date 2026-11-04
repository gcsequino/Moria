import { randomUUID } from 'node:crypto';
import type { PoolClient } from 'pg';
import { FilesystemService } from './service.js';
import type { FilesystemContext, FilesystemSnapshot } from '../../security/src/filesystem.js';
import {
  compileSource,
  sourceDescriptor,
  type SourceKind,
  type SourceDescriptor,
} from '../../../packages/compiler/src/source.js';
import {
  compileManifest,
  fingerprint,
  type CompilerContext,
} from '../../../packages/compiler/src/index.js';
import { authorize } from '../../../packages/authorization/src/access.js';
import {
  MoriaError,
  resourceRef,
  type Manifest,
  type Resource,
} from '../../../packages/contracts/types.js';
import type {
  FilesystemEntry,
  FilesystemMarking,
  SourceRole,
  SourceCheckout,
} from '../../../packages/contracts/filesystem.js';
export type { SourceRole, SourceCheckout } from '../../../packages/contracts/filesystem.js';
interface PublishedSource {
  digest: string;
  rootId: string;
  snapshot: SourceCheckout & { descriptor: SourceDescriptor };
  compiled: ReturnType<typeof compileManifest>;
  owned: string[];
  markings: FilesystemMarking[];
}
const roleColumns = `entry_id::text AS "entryId",kind,project_id::text AS "projectId",deployment_unit_id::text AS "deploymentUnitId",namespace,configuration,revision::text,latest_release AS "latestRelease"`;

export class SourceService {
  constructor(
    readonly filesystem: FilesystemService,
    readonly compilerContext?: CompilerContext,
  ) {}

  private async role(client: PoolClient, id: string): Promise<SourceRole> {
    const result = await client.query(
      `SELECT ${roleColumns} FROM moria.fs_roles WHERE entry_id=$1 AND NOT retired AND kind IN ('ontology','repository')`,
      [id],
    );
    if (!result.rowCount) throw new MoriaError('NOT_FOUND', 'Source root not found');
    return result.rows[0];
  }

  private async json(context: FilesystemContext, entry: FilesystemEntry): Promise<string> {
    if (!entry.content || entry.content.size > 16 * 1024 * 1024)
      throw new MoriaError('SOURCE_INVALID', 'Source JSON exceeds the file bound');
    const read = await this.filesystem.readFile(context, entry.id, entry.revision);
    return new TextDecoder('utf-8', { fatal: true }).decode(
      await new Response(read.stream).arrayBuffer(),
    );
  }

  async register(
    input: FilesystemContext,
    id: string,
    expectedRevision: string,
    kind: SourceKind,
  ): Promise<SourceRole> {
    if (!['ontology', 'repository'].includes(kind))
      throw new MoriaError('SOURCE_INVALID', 'Unknown source root kind');
    const context = structuredClone(input);
    const candidate = await this.filesystem.protectedRead(context, async (client, state) => {
      await this.filesystem.security.check(client, state, id, 'register');
      const root = await this.filesystem.store.get(id, client);
      if (
        root.kind !== 'directory' ||
        !root.projectId ||
        !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(root.projectId) ||
        root.environmentId
      )
        throw new MoriaError('SOURCE_INVALID', 'Source roots require a project source directory');
      const file = await client.query(
        "SELECT id FROM moria.fs_entries WHERE parent_id=$1 AND name=$2 AND kind='file' AND NOT deleted",
        [id, `.${kind}.json`],
      );
      if (!file.rowCount) throw new MoriaError('SOURCE_INVALID', 'Source descriptor is required');
      return { root, descriptor: await this.filesystem.store.get(file.rows[0].id, client) };
    });
    const descriptor = sourceDescriptor(kind, await this.json(context, candidate.descriptor));
    try {
      return await this.filesystem.store.catalog.transaction(
        async (client) => {
          const state = await this.filesystem.security.current(client, context);
          await this.filesystem.security.check(client, state, id, 'register');
          await this.filesystem.security.check(client, state, candidate.descriptor.id, 'read');
          const root = await this.filesystem.store.get(id, client),
            file = await this.filesystem.store.get(candidate.descriptor.id, client);
          if (root.revision !== expectedRevision || file.revision !== candidate.descriptor.revision)
            throw new MoriaError('FILESYSTEM_CONFLICT', 'Source changed during registration');
          const nested = await client.query(
            `WITH RECURSIVE ancestors AS (
          SELECT id,parent_id FROM moria.fs_entries WHERE id=$1
          UNION ALL SELECT e.id,e.parent_id FROM moria.fs_entries e JOIN ancestors a ON e.id=a.parent_id
        ), descendants AS (
          SELECT id FROM moria.fs_entries WHERE id=$1
          UNION ALL SELECT e.id FROM moria.fs_entries e JOIN descendants d ON e.parent_id=d.id WHERE NOT e.deleted
        ) SELECT 1 FROM moria.fs_roles WHERE NOT retired AND kind IN ('ontology','repository')
          AND (entry_id IN (SELECT id FROM ancestors) OR entry_id IN (SELECT id FROM descendants)) LIMIT 1`,
            [id],
          );
          if (nested.rowCount)
            throw new MoriaError('SOURCE_INVALID', 'Source roots cannot overlap or nest');
          await client.query(
            `INSERT INTO moria.fs_roles(entry_id,kind,project_id,deployment_unit_id,namespace,configuration)
          VALUES($1,$2,$3,$4,$5,$6)`,
            [
              id,
              kind,
              root.projectId,
              randomUUID(),
              descriptor.namespace,
              JSON.stringify(descriptor),
            ],
          );
          await this.filesystem.security.finish(client, state);
          return this.role(client, id);
        },
        context.session.environment,
        'serializable',
        'exclusive',
        'exclusive',
      );
    } catch (error) {
      if ((error as { code?: string }).code === '23505')
        throw new MoriaError('FILESYSTEM_CONFLICT', 'Source root or namespace already registered');
      throw error;
    }
  }

  private async capture(
    client: PoolClient,
    state: FilesystemSnapshot,
    id: string,
  ): Promise<SourceCheckout> {
    await this.filesystem.visible(client, state, id, 'read');
    await this.filesystem.security.check(client, state, id, 'export');
    const root = await this.filesystem.store.get(id, client),
      role = await this.role(client, id);
    const rows = (
      await client.query(
        `WITH RECURSIVE tree AS (
      SELECT id,name,kind,revision,content,''::text COLLATE "C" AS path FROM moria.fs_entries WHERE id=$1 AND NOT deleted
      UNION ALL SELECT e.id,e.name,e.kind,e.revision,e.content,
        CASE WHEN t.path='' THEN e.name ELSE t.path||'/'||e.name END
        FROM moria.fs_entries e JOIN tree t ON e.parent_id=t.id WHERE NOT e.deleted
    ) SELECT id,kind,path FROM tree LIMIT 2001`,
        [root.id],
      )
    ).rows;
    if (rows.length > 2000)
      throw new MoriaError('LIMIT_EXCEEDED', 'Source unit exceeds 2,000 entries');
    const files: SourceCheckout['files'] = [],
      directories: SourceCheckout['directories'] = [];
    let bytes = 0;
    for (const row of rows) {
      await this.filesystem.security.check(client, state, row.id, 'read');
      const entry = await this.filesystem.store.get(row.id, client);
      if (entry.kind === 'resource')
        throw new MoriaError('SOURCE_INVALID', 'Runtime resources cannot be source files');
      if (entry.kind === 'file') {
        files.push({ path: row.path, entry });
        bytes += entry.content!.size;
      } else directories.push({ path: row.path, id: entry.id, revision: entry.revision });
    }
    if (bytes > 64 * 1024 * 1024)
      throw new MoriaError('LIMIT_EXCEEDED', 'Source unit exceeds 64 MiB');
    files.sort((a, b) => Buffer.compare(Buffer.from(a.path), Buffer.from(b.path)));
    directories.sort((a, b) => Buffer.compare(Buffer.from(a.path), Buffer.from(b.path)));
    const digest = fingerprint({
      rootId: id,
      projectId: role.projectId,
      deploymentUnitId: role.deploymentUnitId,
      namespace: role.namespace,
      files: files.map(({ path, entry }) => ({
        path,
        id: entry.id,
        revision: entry.revision,
        digest: entry.content!.digest,
      })),
      directories,
    });
    return { version: 1, rootId: id, role, digest, files, directories };
  }

  checkout(context: FilesystemContext, id: string): Promise<SourceCheckout> {
    return this.filesystem.protectedRead(context, (client, state) =>
      this.capture(client, state, id),
    );
  }

  private async release(
    client: PoolClient,
    state: FilesystemSnapshot,
    digest: string,
    seen = new Set<string>(),
    cache = new Map<string, PublishedSource>(),
  ): Promise<PublishedSource> {
    if (!/^sha256:[0-9a-f]{64}$/.test(digest) || seen.has(digest))
      throw new MoriaError('SOURCE_INVALID', 'Invalid or cyclic dependency release');
    if (cache.has(digest)) return cache.get(digest)!;
    if (cache.size + seen.size >= 64)
      throw new MoriaError('LIMIT_EXCEEDED', 'Dependency graph exceeds 64 releases');
    seen.add(digest);
    const result = await client.query(
      'SELECT digest,root_id::text AS "rootId",snapshot,compiled,owned,markings FROM moria.fs_source_releases WHERE digest=$1',
      [digest],
    );
    if (!result.rowCount) throw new MoriaError('NOT_FOUND', 'Entry not found');
    const release = result.rows[0] as PublishedSource;
    await this.filesystem.visible(client, state, release.rootId, 'read');
    await this.filesystem.store.get(release.rootId, client);
    await this.role(client, release.rootId);
    const markings = [...release.markings];
    for (const directory of release.snapshot.directories)
      await this.filesystem.visible(client, state, directory.id, 'read');
    for (const { entry } of release.snapshot.files) {
      await this.filesystem.visible(client, state, entry.id, 'read');
      markings.push(
        ...(await this.filesystem.security.ancestry(client, entry.id)).flatMap(
          (node) => node.markings,
        ),
      );
    }
    for (const member of state.members) {
      if (markings.some((item) => item.environment !== member.session.environment))
        throw new MoriaError('FORBIDDEN', 'Release destination is not authorized');
      authorize(
        member.state.identity,
        'filesystem',
        'read',
        markings.map((item) => item.marking),
      );
    }
    for (const dependency of release.snapshot.descriptor.dependencies) {
      const imported = await this.release(client, state, dependency.digest, new Set(seen), cache);
      if (imported.rootId !== dependency.rootId)
        throw new MoriaError('SOURCE_INVALID', 'Dependency release belongs to another root');
    }
    cache.set(digest, release);
    return release;
  }

  getRelease(context: FilesystemContext, digest: string) {
    return this.filesystem.protectedRead(context, (client, state) =>
      this.release(client, state, digest),
    );
  }

  async publish(
    input: FilesystemContext,
    id: string,
    expectedSourceDigest: string,
    exports: Resource[] = [],
  ) {
    const context = structuredClone(input),
      definitions = structuredClone(exports);
    const snapshot = await this.checkout(context, id);
    if (snapshot.digest !== expectedSourceDigest)
      throw new MoriaError('FILESYSTEM_CONFLICT', 'Source snapshot changed');
    const files = [];
    for (const file of snapshot.files)
      files.push({
        path: file.path,
        text: /(?:\.(?:object|link|action|resource)\.json|\.ontology\.json|\.repository\.json)$/.test(
          file.path,
        )
          ? await this.json(context, file.entry)
          : '',
      });
    const descriptorText = files.find((file) => file.path === `.${snapshot.role.kind}.json`)?.text;
    if (!descriptorText) throw new MoriaError('SOURCE_INVALID', 'Source descriptor is required');
    const descriptor = sourceDescriptor(snapshot.role.kind, descriptorText);
    if (
      descriptor.namespace !== snapshot.role.namespace ||
      descriptor.runtime !== snapshot.role.configuration.runtime
    )
      throw new MoriaError(
        'SOURCE_INVALID',
        'Source namespace/runtime changes require explicit registration',
      );
    const imports: Manifest[] = [];
    for (const dependency of descriptor.dependencies) {
      const release = await this.getRelease(context, dependency.digest);
      if (release.rootId !== dependency.rootId || release.rootId === id)
        throw new MoriaError('SOURCE_INVALID', 'Invalid source dependency');
      imports.push(release.compiled.manifest);
    }
    const assembled = compileSource(
      {
        kind: snapshot.role.kind,
        projectId: snapshot.role.projectId,
        deploymentUnitId: snapshot.role.deploymentUnitId,
        files,
        exports: definitions,
        imports,
      },
      this.compilerContext,
    );
    const descriptorEntry = snapshot.files.find(
      (file) => file.path === `.${snapshot.role.kind}.json`,
    )!.entry.id;
    const resourceFiles = new Map<string, Set<string>>();
    for (const [index, file] of files.entries()) {
      if (!/\.(?:object|link|action|resource)\.json$/.test(file.path)) continue;
      const reference = resourceRef(JSON.parse(file.text) as Resource);
      resourceFiles.set(reference, new Set([snapshot.files[index].entry.id]));
    }
    const declaredReferences = new Set(resourceFiles.keys());
    for (const reference of assembled.owned) {
      const entries = resourceFiles.get(reference) ?? new Set<string>();
      if (snapshot.role.kind === 'repository' && !declaredReferences.has(reference))
        for (const file of snapshot.files)
          if (!/\.(?:object|link|action|resource)\.json$/.test(file.path))
            entries.add(file.entry.id);
      entries.add(descriptorEntry);
      resourceFiles.set(reference, entries);
    }
    const digest = fingerprint({
      source: snapshot.digest,
      compiled: assembled.compiled.fingerprint,
    });
    return this.filesystem.store.catalog.transaction(
      async (client) => {
        const state = await this.filesystem.security.current(client, context);
        await this.filesystem.security.check(client, state, id, 'publish');
        if ((await this.capture(client, state, id)).digest !== snapshot.digest)
          throw new MoriaError('FILESYSTEM_CONFLICT', 'Source changed during compilation');
        for (const dependency of descriptor.dependencies)
          await this.release(client, state, dependency.digest);
        const markings: FilesystemMarking[] = [];
        for (const resource of assembled.compiled.manifest.resources)
          markings.push(
            ...(resource.metadata.markings ?? []).map((marking) => ({
              environment: context.session.environment,
              marking,
            })),
          );
        for (const { entry } of snapshot.files)
          markings.push(...(await this.filesystem.security.effectiveMarkings(client, entry.id)));
        for (const member of state.members)
          authorize(
            member.state.identity,
            'filesystem',
            'publish',
            markings.map((item) => item.marking),
          );
        await client.query(
          `INSERT INTO moria.fs_source_releases(digest,root_id,snapshot,compiled,owned,markings,created_by)
        VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(digest) DO NOTHING`,
          [
            digest,
            id,
            JSON.stringify({ ...snapshot, descriptor }),
            JSON.stringify(assembled.compiled),
            JSON.stringify(assembled.owned),
            JSON.stringify(markings),
            state.members[0].id,
          ],
        );
        for (const [reference, entries] of resourceFiles)
          for (const entry of entries)
            await client.query(
              'INSERT INTO moria.fs_source_resource_files(digest,reference,entry_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING',
              [digest, reference, entry],
            );
        for (const reference of assembled.owned)
          await client.query(
            `INSERT INTO moria.fs_resource_identities(root_id,reference,resource_id) VALUES($1,$2,$3) ON CONFLICT(root_id,reference) DO NOTHING`,
            [id, reference, randomUUID()],
          );
        await client.query('UPDATE moria.fs_roles SET latest_release=$2 WHERE entry_id=$1', [
          id,
          digest,
        ]);
        await this.filesystem.security.finish(client, state);
        return {
          rootId: id,
          digest,
          sourceDigest: snapshot.digest,
          resources: (
            await client.query(
              'SELECT reference,resource_id::text AS id FROM moria.fs_resource_identities WHERE root_id=$1 AND reference=ANY($2::text[]) ORDER BY reference',
              [id, assembled.owned],
            )
          ).rows,
        };
      },
      context.session.environment,
      'serializable',
      'exclusive',
      'exclusive',
    );
  }

  private async desired(
    client: PoolClient,
    release: PublishedSource,
    environment: string,
    deletions: string[],
  ) {
    await this.origins(client, release);
    const active = (
      await client.query(
        'SELECT revision,resources FROM moria.fs_active_releases WHERE root_id=$1 AND environment=$2',
        [release.rootId, environment],
      )
    ).rows[0];
    const owned = new Map<string, Resource>(
      (active?.resources ?? []).map((resource: Resource) => [resourceRef(resource), resource]),
    );
    for (const resource of release.compiled.manifest.resources)
      if (release.owned.includes(resourceRef(resource))) owned.set(resourceRef(resource), resource);
    for (const reference of deletions) {
      if (!owned.has(reference) || reference.includes('/Namespace/'))
        throw new MoriaError(
          'SOURCE_INVALID',
          'Deletion must name an owned non-namespace resource',
        );
      owned.delete(reference);
    }
    const imported = release.compiled.manifest.resources.filter(
      (resource) => !release.owned.includes(resourceRef(resource)),
    );
    const compiled = compileManifest(
      { ...release.compiled.manifest, resources: [...owned.values(), ...imported] },
      this.compilerContext,
    );
    return { expectedRevision: active?.revision ?? '0', resources: [...owned.values()], compiled };
  }

  plan(input: FilesystemContext, id: string, digest: string, deletions: string[] = []) {
    const context = structuredClone(input),
      removed = structuredClone(deletions);
    if (
      !Array.isArray(removed) ||
      removed.length > 2000 ||
      removed.some((item) => typeof item !== 'string')
    )
      throw new MoriaError('SOURCE_INVALID', 'Invalid deletion list');
    return this.filesystem.store.catalog.transaction(
      async (client) => {
        const state = await this.filesystem.security.current(client, context),
          release = await this.release(client, state, digest);
        if (release.rootId !== id)
          throw new MoriaError('SOURCE_INVALID', 'Release belongs to another root');
        await this.filesystem.security.check(client, state, id, 'publish');
        const desired = await this.desired(client, release, context.session.environment, removed),
          planId = randomUUID();
        await this.authorizePlacements(client, state, id, desired.resources, removed);
        const expires = await client.query(
          `INSERT INTO moria.fs_release_plans(id,root_id,digest,environment,actor_hash,expected_revision,environment_epoch,instance_epoch,deletions)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING expires_at`,
          [
            planId,
            id,
            digest,
            context.session.environment,
            fingerprint(state.members.map((member) => [member.id, member.session.application])),
            desired.expectedRevision,
            state.members[0].state.epoch,
            state.instanceEpoch,
            JSON.stringify(removed),
          ],
        );
        await this.filesystem.security.finish(client, state);
        return {
          id: planId,
          rootId: id,
          digest,
          environment: context.session.environment,
          expectedRevision: desired.expectedRevision,
          resources: desired.resources.map(resourceRef),
          deletions: removed,
          expiresAt: expires.rows[0].expires_at.toISOString(),
        };
      },
      context.session.environment,
      'serializable',
      'exclusive',
      'exclusive',
    );
  }

  apply(input: FilesystemContext, planId: string) {
    if (typeof planId !== 'string' || !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(planId))
      throw new MoriaError('SOURCE_INVALID', 'Invalid release plan ID');
    const context = structuredClone(input);
    return this.filesystem.store.catalog.transaction(
      async (client) => {
        const state = await this.filesystem.security.current(client, context);
        const plan = (
          await client.query(
            'SELECT *,expires_at<=clock_timestamp() AS expired FROM moria.fs_release_plans WHERE id=$1',
            [planId],
          )
        ).rows[0];
        if (
          !plan ||
          plan.environment !== context.session.environment ||
          plan.actor_hash !==
            fingerprint(state.members.map((member) => [member.id, member.session.application]))
        )
          throw new MoriaError('NOT_FOUND', 'Release plan not found');
        const release = await this.release(client, state, plan.digest);
        await this.filesystem.security.check(client, state, plan.root_id, 'publish');
        if (plan.receipt) {
          await this.authorizePlacements(
            client,
            state,
            plan.root_id,
            plan.receipt.resources,
            plan.deletions,
          );
          await this.filesystem.security.finish(client, state);
          return plan.receipt;
        }
        if (plan.expired) throw new MoriaError('PLAN_EXPIRED', 'Release plan expired');
        const desired = await this.desired(client, release, plan.environment, plan.deletions);
        await this.authorizePlacements(
          client,
          state,
          plan.root_id,
          desired.resources,
          plan.deletions,
        );
        if (desired.expectedRevision !== plan.expected_revision)
          throw new MoriaError('FILESYSTEM_CONFLICT', 'Active release changed');
        if (
          plan.environment_epoch !== state.members[0].state.epoch ||
          plan.instance_epoch !== state.instanceEpoch
        )
          throw new MoriaError(
            'AUTHORIZATION_CHANGED',
            'Release plan requires current authorization',
          );
        const activated = await client.query(
          `INSERT INTO moria.fs_active_releases(root_id,environment,revision,digest,resources) VALUES($1,$2,1,$3,$4)
        ON CONFLICT(root_id,environment) DO UPDATE SET revision=fs_active_releases.revision+1,digest=EXCLUDED.digest,resources=EXCLUDED.resources RETURNING revision`,
          [plan.root_id, plan.environment, plan.digest, JSON.stringify(desired.resources)],
        );
        await this.materialize(client, state, release, desired.resources);
        const receipt = {
          rootId: plan.root_id,
          environment: plan.environment,
          digest: plan.digest,
          revision: activated.rows[0].revision,
          resources: desired.resources.map(resourceRef),
          runtimeReady: false,
        };
        await client.query('UPDATE moria.fs_release_plans SET receipt=$2 WHERE id=$1', [
          planId,
          JSON.stringify(receipt),
        ]);
        await this.filesystem.security.finish(client, state);
        return receipt;
      },
      context.session.environment,
      'serializable',
      'exclusive',
      'exclusive',
    );
  }

  private async authorizePlacements(
    client: PoolClient,
    state: FilesystemSnapshot,
    id: string,
    resources: readonly (Resource | string)[],
    deletions: readonly string[],
  ) {
    const removed = new Set(deletions),
      references = [
        ...new Set([
          ...resources.map((resource) =>
            typeof resource === 'string' ? resource : resourceRef(resource),
          ),
          ...deletions,
        ]),
      ];
    const entries = (
      await client.query(
        'SELECT entry_id,reference FROM moria.fs_resource_bindings WHERE root_id=$1 AND environment=$2 AND reference=ANY($3::text[])',
        [id, state.members[0].session.environment, references],
      )
    ).rows;
    for (const entry of entries) {
      await this.filesystem.security.check(client, state, entry.entry_id, 'read', true);
      await this.filesystem.security.check(client, state, entry.entry_id, 'publish', true);
      if (removed.has(entry.reference))
        await this.filesystem.security.check(client, state, entry.entry_id, 'delete', true);
    }
  }

  private async origins(client: PoolClient, release: PublishedSource) {
    const rows = (
      await client.query(
        `WITH RECURSIVE releases AS (
      SELECT digest,snapshot,owned FROM moria.fs_source_releases WHERE digest=$1
      UNION SELECT d.digest,d.snapshot,d.owned FROM releases r
        CROSS JOIN LATERAL jsonb_array_elements(r.snapshot->'descriptor'->'dependencies') dependency
        JOIN moria.fs_source_releases d ON d.digest=dependency->>'digest'
    ) SELECT snapshot->'role'->>'projectId' AS project_id,jsonb_array_elements_text(owned) AS reference FROM releases`,
        [release.digest],
      )
    ).rows;
    const origins = new Map<string, string>();
    for (const row of rows) {
      if (origins.has(row.reference) && origins.get(row.reference) !== row.project_id)
        throw new MoriaError('SOURCE_INVALID', 'Imported references are ambiguous across projects');
      origins.set(row.reference, row.project_id);
    }
    return origins;
  }

  private async materialize(
    client: PoolClient,
    state: FilesystemSnapshot,
    release: PublishedSource,
    resources: Resource[],
  ) {
    const fs = this.filesystem,
      session = state.members[0].session;
    const project = (
      await client.query(
        `WITH RECURSIVE ancestors AS (
      SELECT id,parent_id,project_id FROM moria.fs_entries WHERE id=$1 UNION ALL
      SELECT e.id,e.parent_id,e.project_id FROM moria.fs_entries e JOIN ancestors a ON e.id=a.parent_id
    ) SELECT a.id FROM ancestors a LEFT JOIN moria.fs_entries p ON p.id=a.parent_id
      WHERE a.project_id=$2 AND p.project_id IS DISTINCT FROM a.project_id`,
        [release.rootId, release.snapshot.role.projectId],
      )
    ).rows[0];
    if (!project) throw new MoriaError('SOURCE_INVALID', 'Project root is unavailable');
    const create = async (
      parentId: string,
      name: string,
      environment?: string,
      resourceId?: string,
    ) => {
      const existing = (
        await client.query(
          'SELECT id FROM moria.fs_entries WHERE parent_id=$1 AND name=$2 AND NOT deleted',
          [parentId, name],
        )
      ).rows[0];
      if (existing) {
        const entry = await fs.store.get(existing.id, client);
        if (
          entry.kind !== (resourceId ? 'resource' : 'directory') ||
          (environment && entry.environmentId !== environment) ||
          (resourceId && entry.resourceId !== resourceId)
        )
          throw new MoriaError(
            'FILESYSTEM_CONFLICT',
            'Runtime placement differs from its registered identity',
          );
        return entry;
      }
      await fs.security.check(client, state, parentId, 'create');
      const parent = await fs.store.get(parentId, client);
      const receipt = await fs.store.apply(
        {
          environment: session.environment,
          principal: session.principal,
          application: session.application,
        },
        randomUUID(),
        [
          {
            op: 'create',
            parentId,
            expectedParentRevision: parent.revision,
            name,
            kind: resourceId ? 'resource' : 'directory',
            ...(environment
              ? {
                  scope: { projectId: release.snapshot.role.projectId, environmentId: environment },
                }
              : {}),
            ...(resourceId ? { resourceId } : {}),
          },
        ],
        {
          environment: session.environment,
          client,
          current: async () => {},
          change: async () => {},
          receipt: async () => {},
        },
      );
      return receipt.entries.find((entry) => entry.parentId === parentId && entry.name === name)!;
    };
    const environments = await create(project.id, 'environments');
    const environment = await create(
      environments.id,
      `env-${fingerprint(session.environment).slice(7, 23)}`,
      session.environment,
    );
    const unit = await create(environment.id, release.snapshot.role.deploymentUnitId);
    for (const entry of [environments, environment, unit])
      await client.query(
        `INSERT INTO moria.fs_roles(entry_id,kind,configuration) VALUES($1,'system',$2) ON CONFLICT(entry_id) DO NOTHING`,
        [entry.id, JSON.stringify({ managed: 'runtime', environment: session.environment })],
      );
    await fs.security.check(client, state, unit.id, 'publish');
    await client.query(
      'UPDATE moria.fs_resource_bindings SET active=false WHERE root_id=$1 AND environment=$2',
      [release.rootId, session.environment],
    );
    for (const resource of resources) {
      const reference = resourceRef(resource),
        identity = (
          await client.query(
            'SELECT resource_id FROM moria.fs_resource_identities WHERE root_id=$1 AND reference=$2',
            [release.rootId, reference],
          )
        ).rows[0];
      if (!identity) throw new MoriaError('SOURCE_INVALID', 'Resource has no permanent identity');
      let entry = (
        await client.query(
          'SELECT entry_id FROM moria.fs_resource_bindings WHERE environment=$1 AND reference=$2 AND project_id=$3',
          [session.environment, reference, release.snapshot.role.projectId],
        )
      ).rows[0];
      if (!entry)
        entry = {
          entry_id: (
            await create(
              (await create(unit.id, resource.kind)).id,
              resource.metadata.name,
              undefined,
              identity.resource_id,
            )
          ).id,
        };
      const bound = (
        await client.query('SELECT root_id FROM moria.fs_resource_bindings WHERE entry_id=$1', [
          entry.entry_id,
        ])
      ).rows[0];
      if (bound && bound.root_id !== release.rootId)
        throw new MoriaError('SOURCE_INVALID', 'Resource reference belongs to another source root');
      if (bound) await fs.security.retain(client, state.members[0].id, entry.entry_id);
      await client.query(
        `INSERT INTO moria.fs_resource_bindings(environment,reference,entry_id,root_id,digest,project_id) VALUES($1,$2,$3,$4,$5,$6)
        ON CONFLICT(environment,project_id,reference) DO UPDATE SET digest=EXCLUDED.digest,active=true`,
        [
          session.environment,
          reference,
          entry.entry_id,
          release.rootId,
          release.digest,
          release.snapshot.role.projectId,
        ],
      );
      await client.query(
        'INSERT INTO moria.fs_resource_release_history VALUES($1,$2) ON CONFLICT DO NOTHING',
        [entry.entry_id, release.digest],
      );
      const labels = (resource.metadata.markings ?? []).map((marking) => ({
        environment: session.environment,
        marking,
      }));
      await client.query(
        `INSERT INTO moria.fs_policies(entry_id,markings) VALUES($1,$2)
        ON CONFLICT(entry_id) DO UPDATE SET markings=COALESCE((SELECT jsonb_agg(DISTINCT value) FROM jsonb_array_elements(fs_policies.markings || EXCLUDED.markings)),'[]')`,
        [entry.entry_id, JSON.stringify(labels)],
      );
    }
    const references = new Set(release.compiled.manifest.resources.map(resourceRef)),
      origins = await this.origins(client, release);
    for (const resource of resources) {
      const reference = resourceRef(resource),
        sources = new Set<string>(),
        pending: unknown[] = [resource.spec];
      let nodes = 0;
      while (pending.length) {
        if (++nodes > 100000)
          throw new MoriaError('LIMIT_EXCEEDED', 'Resource dependency graph exceeds bounds');
        const value = pending.pop();
        if (typeof value === 'string' && references.has(value) && value !== reference)
          sources.add(value);
        else if (value && typeof value === 'object') pending.push(...Object.values(value));
      }
      for (const source of sources)
        await client.query(
          'INSERT INTO moria.fs_resource_dependencies(environment,reference,source_reference,project_id,source_project_id) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING',
          [
            session.environment,
            reference,
            source,
            release.snapshot.role.projectId,
            origins.get(source),
          ],
        );
      if (resource.kind === 'Transform')
        for (const output of resource.spec.outputs) {
          const target = (
            await client.query(
              'SELECT entry_id FROM moria.fs_resource_bindings WHERE environment=$1 AND project_id=$2 AND reference=$3 AND active',
              [session.environment, origins.get(output), output],
            )
          ).rows[0];
          if (!target)
            throw new MoriaError(
              'SOURCE_INVALID',
              'Transform outputs must have active resource placements',
            );
          await fs.security.check(client, state, target.entry_id, 'write');
          await client.query(
            'INSERT INTO moria.fs_resource_dependencies(environment,reference,source_reference,project_id,source_project_id) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING',
            [
              session.environment,
              output,
              reference,
              origins.get(output),
              release.snapshot.role.projectId,
            ],
          );
        }
    }
  }

  retire(input: FilesystemContext, id: string, expectedRevision: string) {
    const context = structuredClone(input);
    return this.filesystem.store.catalog.transaction(
      async (client) => {
        const state = await this.filesystem.security.current(client, context);
        await this.filesystem.security.check(client, state, id, 'administer');
        await this.filesystem.security.check(client, state, id, 'publish');
        const role = await this.role(client, id);
        if (role.revision !== expectedRevision)
          throw new MoriaError('FILESYSTEM_CONFLICT', 'Source registration changed');
        if (
          (
            await client.query(
              'SELECT 1 FROM moria.fs_resource_bindings WHERE root_id=$1 AND environment<>$2 AND active LIMIT 1',
              [id, context.session.environment],
            )
          ).rowCount
        )
          for (const member of state.members)
            authorize(member.state.identity, 'instance', 'manage');
        if (
          (
            await client.query(
              `WITH RECURSIVE dependencies AS (
          SELECT a.root_id AS active_root,r.root_id,r.digest,r.snapshot FROM moria.fs_active_releases a
            JOIN moria.fs_roles role ON role.entry_id=a.root_id AND NOT role.retired
            JOIN moria.fs_source_releases r ON r.digest=a.digest
          UNION SELECT p.active_root,d.root_id,d.digest,d.snapshot FROM dependencies p
            CROSS JOIN LATERAL jsonb_array_elements(p.snapshot->'descriptor'->'dependencies') dependency
            JOIN moria.fs_source_releases d ON d.digest=dependency->>'digest'
        ) SELECT 1 FROM dependencies WHERE root_id=$1 AND active_root<>$1::uuid LIMIT 1`,
              [id],
            )
          ).rowCount
        )
          throw new MoriaError(
            'SOURCE_INVALID',
            'Active source releases still depend on this root',
          );
        await client.query(
          'UPDATE moria.fs_roles SET retired=true,revision=revision+1 WHERE entry_id=$1',
          [id],
        );
        await client.query('UPDATE moria.fs_resource_bindings SET active=false WHERE root_id=$1', [
          id,
        ]);
        await this.filesystem.security.audit(client, state.members[0].id, 'retireSource', id);
        await this.filesystem.security.finish(client, state);
        return { entryId: id, retired: true };
      },
      context.session.environment,
      'serializable',
      'exclusive',
      'exclusive',
    );
  }

  resource(context: FilesystemContext, reference: string, projectId?: string) {
    if (typeof reference !== 'string' || !reference || reference.length > 512)
      throw new MoriaError('SOURCE_INVALID', 'Invalid resource reference');
    const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
    if (projectId !== undefined && (typeof projectId !== 'string' || !uuid.test(projectId)))
      throw new MoriaError('SOURCE_INVALID', 'Invalid project ID');
    if (!uuid.test(reference) && !projectId)
      throw new MoriaError(
        'SOURCE_INVALID',
        'A project ID is required for a named resource reference',
      );
    return this.filesystem.protectedRead(context, async (client, state) => {
      const binding = (
        await client.query(
          `SELECT b.entry_id,b.root_id,b.digest,b.reference,e.resource_id FROM moria.fs_resource_bindings b
        JOIN moria.fs_entries e ON e.id=b.entry_id WHERE b.environment=$1 AND (b.reference=$2 OR e.resource_id::text=$2) AND b.active AND ($3::uuid IS NULL OR b.project_id=$3)`,
          [context.session.environment, reference, projectId ?? null],
        )
      ).rows[0];
      if (!binding) throw new MoriaError('NOT_FOUND', 'Entry not found');
      await this.filesystem.visible(client, state, binding.entry_id, 'read');
      const active = (
        await client.query(
          'SELECT resources FROM moria.fs_active_releases WHERE root_id=$1 AND environment=$2',
          [binding.root_id, context.session.environment],
        )
      ).rows[0];
      const resource = (active?.resources as Resource[] | undefined)?.find(
        (item) => resourceRef(item) === binding.reference,
      );
      if (!resource) throw new MoriaError('NOT_FOUND', 'Entry not found');
      return {
        entryId: binding.entry_id,
        markings: await this.filesystem.security.effectiveMarkings(client, binding.entry_id),
        resourceId: binding.resource_id,
        digest: binding.digest,
        resource,
      };
    });
  }
}
