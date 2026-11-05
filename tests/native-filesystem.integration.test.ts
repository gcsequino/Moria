import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import type { FilesystemEntry } from '../packages/contracts/filesystem.js';
import { compileSource } from '../packages/compiler/src/source.js';
import { fingerprint } from '../packages/compiler/src/index.js';
import { publishBrowserSource } from '../packages/platform/src/source.js';
import { filesystemClient } from '../packages/platform/src/filesystem.js';
import { createTransport } from '../packages/platform/src/transport.js';
import { filesystemCommand } from '../apps/cli/src/filesystem.js';
import { configuredFilesystem } from '../services/filesystem/src/local.js';
import { startS3 } from './fixtures/storage-s3/server.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { database } from './fixtures/catalog-storage/database.js';

for (const provider of ['posix', 's3'] as const)
  test(
    `native filesystem replicas enforce current namespace authority and atomic receipts (${provider})`,
    { timeout: 180000 },
    async (t) => {
      const db = await database();
      let s3: Awaited<ReturnType<typeof startS3>> | undefined;
      const processes: ChildProcess[] = [];
      const exits: Promise<unknown>[] = [];
      t.after(async () => {
        for (const process of processes) process.kill('SIGKILL');
        await Promise.allSettled(exits);
        await s3?.close();
        await db.close();
      });
      const environment: string = randomUUID(),
        application = 'native-fs-app',
        principal = 'owner',
        viewer = 'viewer';
      await db.catalog.ensureEnvironment(environment);
      const authority = new Authority(db.catalog),
        sessions = new WorkloadSessions(authority);
      const operations = [
        'read',
        'list',
        'create',
        'write',
        'move',
        'delete',
        'export',
        'administer',
        'register',
        'publish',
      ].map((op) => `filesystem:${op}`);
      for (const name of [principal, viewer])
        await authority.syncPrincipal(environment, 'fixture', name, {
          kind: 'workload',
          enabled: true,
          markings: name === principal ? ['private'] : [],
          attributes: { groups: [] },
        });
      await authority.setApplication(environment, 'fixture', application, [
        ...operations,
        'instance:manage',
      ]);
      for (const name of [principal, viewer])
        await authority.setGrants(
          environment,
          'fixture',
          name,
          application,
          name === principal ? [...operations, 'instance:manage'] : operations,
        );
      const ownerToken = (await sessions.mint({ environment, principal, application }, 'fixture'))
        .token;
      const viewerToken = (
        await sessions.mint({ environment, principal: viewer, application }, 'fixture')
      ).token;
      const owner = await sessions.authenticate(`Bearer ${ownerToken}`),
        observer = await sessions.authenticate(`Bearer ${viewerToken}`);
      if (provider === 's3') s3 = await startS3();
      const fs = await configuredFilesystem(
        db.catalog,
        join(db.root, 'filesystem'),
        s3
          ? {
              MORIA_FILESYSTEM_BACKEND: 's3',
              MORIA_FILESYSTEM_S3_ENDPOINT: s3.endpoint,
              MORIA_FILESYSTEM_S3_BUCKET: s3.bucket,
              MORIA_FILESYSTEM_S3_REGION: 'us-east-1',
              MORIA_FILESYSTEM_S3_PREFIX: 'fs-content',
              MORIA_FILESYSTEM_S3_ACCESS_KEY_ID: s3.credentials.accessKeyId,
              MORIA_FILESYSTEM_S3_SECRET_ACCESS_KEY: s3.credentials.secretAccessKey,
            }
          : {},
      );
      const home = await fs.management.ensureHome({ session: owner }),
        viewerHome = await fs.management.ensureHome({ session: observer });
      const trusted = { environment, principal, application };
      const create = async (parentId: string, name: string) => {
        const parent = await fs.filesystem.store.get(parentId);
        return (
          await fs.filesystem.store.apply(trusted, randomUUID(), [
            {
              op: 'create',
              parentId,
              expectedParentRevision: parent.revision,
              name,
              kind: 'directory',
            },
          ])
        ).entries.find((entry) => entry.name === name)!;
      };
      const homeEntry = await fs.filesystem.store.get(home.home.id);
      const project = (
        await fs.filesystem.store.apply(trusted, randomUUID(), [
          {
            op: 'create',
            parentId: homeEntry.id,
            expectedParentRevision: homeEntry.revision,
            name: 'project',
            kind: 'directory',
            scope: { projectId: randomUUID(), environmentId: environment },
          },
        ])
      ).entries.find((entry) => entry.name === 'project')!;
      await fs.management.setPolicy({ session: owner }, project.id, '0', {
        markings: [],
        grants: [
          {
            subject: { kind: 'principal', id: viewerHome.principalId },
            operations: ['read', 'list'],
            inherit: true,
            effect: 'allow',
          },
        ],
      });
      const privateParent = await create(project.id, 'private-parent'),
        publicParent = await create(project.id, 'public-parent');
      await fs.management.setPolicy({ session: owner }, privateParent.id, '0', {
        markings: [{ environment, marking: 'private' }],
        grants: [],
      });
      const moving = await create(privateParent.id, 'retained-child');
      for (let offset = 0; offset < 110; offset += 100) {
        const parent = await fs.filesystem.store.get(project.id);
        const count = Math.min(100, 110 - offset);
        const result = await fs.filesystem.store.apply(
          trusted,
          randomUUID(),
          Array.from({ length: count }, (_, i) => ({
            op: 'create' as const,
            parentId: project.id,
            expectedParentRevision: String(BigInt(parent.revision) + BigInt(i)),
            name:
              offset + i < 105
                ? `a-hidden-${String(offset + i).padStart(3, '0')}`
                : `z-visible-${offset + i}`,
            kind: 'directory' as const,
          })),
        );
        for (const entry of result.entries.filter(
          (entry) => entry.id !== project.id && entry.name.startsWith('a-hidden-'),
        ))
          await fs.management.setPolicy({ session: owner }, entry.id, '0', {
            markings: [{ environment, marking: 'private' }],
            grants: [],
          });
      }
      const role = `native_fs_${randomUUID().replaceAll('-', '')}`,
        password = randomUUID();
      await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
      await db.pool.query(
        `GRANT USAGE ON SCHEMA moria TO ${role};GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role};GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
      );
      for (const kernel of [
        'native_serving_filesystem_resource_access_state',
        'native_serving_resource_access_state',
      ]) {
        const client = await db.pool.connect();
        try {
          await client.query('BEGIN');
          await client.query(`SET LOCAL ROLE ${role}`);
          await assert.rejects(
            client.query(`SELECT moria.${kernel}('{}'::jsonb,'forged','read')`),
            { code: '42501' },
          );
        } finally {
          await client.query('ROLLBACK');
          client.release();
        }
      }
      const connection = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
      const binary = process.env.MORIA_RUNTIME_BINARY ?? '/usr/local/bin/moria-indexing';
      const start = async () => {
        const process = spawn(binary, ['serve'], {
          env: {
            PATH: '/usr/local/bin:/usr/bin:/bin',
            MORIA_INDEXING_DATABASE_SECRET_REF: 'NATIVE_FILESYSTEM_DATABASE',
            NATIVE_FILESYSTEM_DATABASE: connection,
            MORIA_INDEXING_DATABASE_TLS: 'disable',
            MORIA_NATIVE_LISTEN: '127.0.0.1:0',
            MORIA_NATIVE_FILESYSTEM_STORAGE_REVISION: 'v1',
            MORIA_NATIVE_STORAGE_BINDINGS: JSON.stringify(
              s3
                ? [
                    {
                      namespace: { warehouseId: 'filesystem', tableId: 'content', revision: 'v1' },
                      provider: {
                        kind: 's3',
                        binding: {
                          warehouseId: 'filesystem',
                          tableId: 'content',
                          endpoint: s3.endpoint,
                          bucket: s3.bucket,
                          prefix: 'fs-content',
                          region: 'us-east-1',
                          accessKeySecretRef: 'NATIVE_FS_S3_ACCESS',
                          secretKeySecretRef: 'NATIVE_FS_S3_SECRET',
                          sessionTokenSecretRef: null,
                          limits: { maxObjectBytes: 64 * 1024 * 1024, chunkBytes: 1024 * 1024 },
                        },
                      },
                    },
                  ]
                : [
                    {
                      namespace: { warehouseId: 'filesystem', tableId: 'content', revision: 'v1' },
                      provider: {
                        kind: 'posix',
                        root: join(db.root, 'filesystem'),
                        limits: { maxObjectBytes: 64 * 1024 * 1024, chunkBytes: 1024 * 1024 },
                      },
                    },
                  ],
            ),
            ...(s3
              ? {
                  NATIVE_FS_S3_ACCESS: s3.credentials.accessKeyId,
                  NATIVE_FS_S3_SECRET: s3.credentials.secretAccessKey,
                  AWS_ACCESS_KEY_ID: 'wrong-ambient-key',
                  AWS_SECRET_ACCESS_KEY: 'wrong-ambient-secret',
                  AWS_ENDPOINT_URL: 'http://127.0.0.1:1',
                }
              : {}),
          },
          stdio: ['ignore', 'pipe', 'pipe'],
        });
        processes.push(process);
        let stdout = '',
          stderr = '';
        process.stdout!.on('data', (data) => {
          stdout += data.toString();
        });
        process.stderr!.on('data', (data) => {
          stderr += data.toString();
        });
        exits.push(new Promise((resolve) => process.once('exit', resolve)));
        for (let attempt = 0; attempt < 500; attempt++) {
          for (const line of stdout.split('\n')) {
            try {
              const event = JSON.parse(line);
              if (event.event === 'serving') return `http://${event.address}`;
            } catch {}
          }
          if (process.exitCode !== null) throw new Error(`native filesystem exited: ${stderr}`);
          await delay(10);
        }
        throw new Error(`native filesystem startup timeout: ${stderr}`);
      };
      const a = await start(),
        b = await start();
      const request = async (
        base: string,
        path: string,
        method = 'GET',
        body?: unknown,
        key?: string,
        token = ownerToken,
      ) => {
        const response = await fetch(`${base}/api/management/v1/fs${path}`, {
          method,
          headers: {
            authorization: `Bearer ${token}`,
            ...(body === undefined ? {} : { 'content-type': 'application/json' }),
            ...(key ? { 'idempotency-key': key } : {}),
          },
          ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        });
        return { response, value: await response.json() };
      };
      const checked = async (promise: ReturnType<typeof request>, status = 200) => {
        const { response, value } = await promise;
        assert.equal(response.status, status, JSON.stringify(value));
        return value;
      };
      const get = (base: string, id: string, token = ownerToken) =>
        checked(request(base, `/entries/${id}`, 'GET', undefined, undefined, token));
      const mutate = (base: string, key: string, changes: unknown[]) =>
        checked(request(base, '/changes', 'POST', { changes }, key));

      await t.test(
        'permission filtering precedes directory limits and direct hidden reads remain concealed',
        async () => {
          const value = await checked(
            request(a, `/entries/${project.id}/children`, 'GET', undefined, undefined, viewerToken),
          );
          const names = (value.entries as FilesystemEntry[]).map((entry) => entry.name);
          assert.equal(names.filter((name) => name.startsWith('z-visible-')).length, 5);
          assert.ok(!names.some((name) => name.startsWith('a-hidden-')));
          assert.equal(value.nextCursor, undefined);
          await checked(
            request(b, `/entries/${privateParent.id}`, 'GET', undefined, undefined, viewerToken),
            404,
          );
          assert.equal((await get(b, project.id)).id, project.id);
          await checked(
            request(a, `/entries/${project.id}/policy`, 'GET', undefined, undefined, viewerToken),
            404,
          );
        },
      );
      await t.test(
        'opaque directory cursors resume concurrently across replicas with one original expiry',
        async () => {
          const first = await checked(request(a, `/entries/${project.id}/children`));
          assert.equal(first.entries.length, 100);
          assert.equal(first.nextCursor.length, 43);
          const [left, right] = await Promise.all([
            checked(request(a, `/entries/${project.id}/children?cursor=${first.nextCursor}`)),
            checked(request(b, `/entries/${project.id}/children?cursor=${first.nextCursor}`)),
          ]);
          assert.deepEqual(left.entries, right.entries);
          assert.equal(left.entries.length, 12);
          const hash = createHash('sha256').update(first.nextCursor).digest('hex');
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(DISTINCT expires_at)::int AS n FROM moria.read_cursors WHERE binding_hash=(SELECT binding_hash FROM moria.read_cursors WHERE token_hash=$1)',
                [hash],
              )
            ).rows[0].n,
            1,
          );
        },
      );
      await t.test(
        'same-key races commit one namespace mutation and conflict on different input',
        async () => {
          const parent = await get(a, project.id),
            key = randomUUID();
          const changes = [
            {
              op: 'create',
              parentId: project.id,
              expectedParentRevision: parent.revision,
              name: 'one-result',
              kind: 'directory',
            },
          ];
          const results = await Promise.all(
            Array.from({ length: 12 }, (_, i) => mutate(i % 2 ? a : b, key, changes)),
          );
          assert.ok(results.every((result) => result.mutationId === results[0].mutationId));
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*)::int AS n FROM moria.fs_mutations WHERE request_key=$1',
                [key],
              )
            ).rows[0].n,
            1,
          );
          assert.equal(
            (
              await db.pool.query(
                "SELECT count(*)::int AS n FROM moria.fs_entries WHERE parent_id=$1 AND name='one-result' AND NOT deleted",
                [project.id],
              )
            ).rows[0].n,
            1,
          );
          assert.equal(
            (
              await checked(
                request(
                  a,
                  '/changes',
                  'POST',
                  { changes: [{ ...changes[0], name: 'different' }] },
                  key,
                ),
                409,
              )
            ).error.code,
            'IDEMPOTENCY_CONFLICT',
          );
        },
      );
      await t.test(
        'a later failed change rolls back earlier entries, versions and parent touches',
        async () => {
          const parent = await get(a, project.id),
            key = randomUUID();
          const value = await checked(
            request(
              a,
              '/changes',
              'POST',
              {
                changes: [
                  {
                    op: 'create',
                    parentId: project.id,
                    expectedParentRevision: parent.revision,
                    name: 'rolled-back',
                    kind: 'directory',
                  },
                  {
                    op: 'create',
                    parentId: project.id,
                    expectedParentRevision: String(BigInt(parent.revision) + 1n),
                    name: 'one-result',
                    kind: 'directory',
                  },
                ],
              },
              key,
            ),
            409,
          );
          assert.equal(value.error.code, 'FILESYSTEM_CONFLICT');
          assert.equal((await get(b, project.id)).revision, parent.revision);
          assert.equal(
            (
              await db.pool.query(
                "SELECT count(*)::int AS n FROM moria.fs_entries WHERE parent_id=$1 AND name='rolled-back'",
                [project.id],
              )
            ).rows[0].n,
            0,
          );
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*)::int AS n FROM moria.fs_mutations WHERE request_key=$1',
                [key],
              )
            ).rows[0].n,
            0,
          );
        },
      );
      await t.test(
        'moves retain old inherited markings, audit the epoch and invalidate unrelated directory continuations',
        async () => {
          const first = await checked(request(a, `/entries/${project.id}/children`));
          const projectBefore = await get(a, project.id),
            entry = await get(a, moving.id),
            source = await get(a, privateParent.id),
            target = await get(a, publicParent.id);
          const result = await mutate(a, randomUUID(), [
            {
              op: 'move',
              id: entry.id,
              expectedRevision: entry.revision,
              parentId: target.id,
              expectedParentRevision: target.revision,
              expectedSourceParentRevision: source.revision,
              name: 'moved-private',
            },
          ]);
          assert.equal(
            result.entries.find((item: FilesystemEntry) => item.id === entry.id).parentId,
            target.id,
          );
          assert.equal((await get(a, project.id)).revision, projectBefore.revision);
          await checked(
            request(b, `/entries/${entry.id}`, 'GET', undefined, undefined, viewerToken),
            404,
          );
          assert.equal(
            (
              await checked(
                request(b, `/entries/${project.id}/children?cursor=${first.nextCursor}`),
                409,
              )
            ).error.code,
            'CURSOR_INVALIDATED',
          );
          const policy = (
            await db.pool.query(
              'SELECT retained_markings FROM moria.fs_policies WHERE entry_id=$1',
              [entry.id],
            )
          ).rows[0];
          assert.deepEqual(policy.retained_markings, [{ environment, marking: 'private' }]);
          assert.equal(
            (
              await db.pool.query(
                "SELECT count(*)::int AS n FROM moria.instance_audit WHERE operation='retainMoveMarkings' AND target=$1",
                [entry.id],
              )
            ).rows[0].n,
            1,
          );
          const history = await checked(request(a, `/entries/${entry.id}/history`));
          assert.equal(history.entries.length, 2);
        },
      );
      await t.test(
        'history orders numeric revisions beyond twelve and paginates without losing newer versions',
        async () => {
          const entry = await create(publicParent.id, 'history-0');
          for (let index = 1; index <= 104; index++) {
            const current = await get(a, entry.id),
              parent = await get(a, publicParent.id);
            await mutate(a, randomUUID(), [
              {
                op: 'move',
                id: entry.id,
                expectedRevision: current.revision,
                parentId: parent.id,
                expectedParentRevision: parent.revision,
                expectedSourceParentRevision: parent.revision,
                name: `history-${index}`,
              },
            ]);
          }
          const expected = (
            await db.pool.query(
              'SELECT v.revision::text FROM moria.fs_entry_versions v WHERE v.entry_id=$1 ORDER BY v.revision DESC',
              [entry.id],
            )
          ).rows.map((row) => row.revision);
          assert.equal(expected.length, 105);
          const first = await checked(request(a, `/entries/${entry.id}/history`));
          assert.deepEqual(
            first.entries.map((version: FilesystemEntry) => version.revision),
            expected.slice(0, 100),
          );
          assert.equal(first.nextRevision, expected[99]);
          const next = await checked(
            request(b, `/entries/${entry.id}/history?before=${first.nextRevision}`),
          );
          assert.deepEqual(
            next.entries.map((version: FilesystemEntry) => version.revision),
            expected.slice(100),
          );
          assert.equal(next.nextRevision, undefined);
        },
      );
      await t.test(
        'native home provisioning binds only the verified principal and administrative group/enable operations retain current authority',
        async () => {
          const user = `native-user-${provider}`;
          await authority.syncPrincipal(environment, 'fixture', user, {
            kind: 'workload',
            enabled: true,
            markings: [],
            attributes: { groups: ['native-group'] },
          });
          await authority.setGrants(environment, 'fixture', user, application, operations);
          const token = (
            await sessions.mint({ environment, principal: user, application }, 'fixture')
          ).token;
          const [first, second] = await Promise.all([
            checked(request(a, '/home', 'GET', undefined, undefined, token)),
            checked(request(b, '/home', 'GET', undefined, undefined, token)),
          ]);
          assert.equal(first.principalId, second.principalId);
          assert.equal(first.home.id, second.home.id);
          assert.equal(first.home.name, first.principalId);
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*)::int AS n FROM moria.instance_principal_bindings WHERE environment=$1 AND principal=$2',
                [environment, user],
              )
            ).rows[0].n,
            1,
          );
          const groups = await checked(request(a, '/resolve', 'POST', { path: '/groups' }));
          const group = await checked(
            request(a, '/groups', 'POST', {
              name: `native-group-${provider}`,
              groupKey: 'native-group',
              expectedRevision: groups.revision,
            }),
          );
          await get(b, group.entry.id, token);
          await checked(
            request(
              a,
              '/principals',
              'POST',
              { id: first.principalId, enabled: false },
              undefined,
              token,
            ),
            403,
          );
          await checked(
            request(a, '/principals', 'POST', { id: first.principalId, enabled: false }),
          );
          await checked(
            request(b, `/entries/${first.home.id}`, 'GET', undefined, undefined, token),
            401,
          );
          await checked(
            request(b, '/principals', 'POST', { id: first.principalId, enabled: true }),
          );
          await checked(
            request(a, `/entries/${first.home.id}`, 'GET', undefined, undefined, token),
            401,
          );
          const renewed = (
            await sessions.mint({ environment, principal: user, application }, 'fixture')
          ).token;
          await get(a, first.home.id, renewed);
        },
      );
      await t.test(
        'native source publication verifies qualified compiler artifacts and atomically activates permanent resource identities',
        async () => {
          const context = await checked(
            request(a, '/compiler-context', 'POST', { principalAttributes: {} }),
          );
          assert.equal(context.fingerprint, fingerprint({}));
          await checked(
            request(b, '/compiler-context', 'POST', {
              principalAttributes: { region: { type: 'string', nullable: false } },
            }),
          );
          await checked(
            request(a, '/compiler-context', 'POST', {
              principalAttributes: { region: { type: 'int64', nullable: false } },
            }),
            409,
          );
          const sourceProject = await checked(
            request(a, '/projects', 'POST', {
              parentId: home.home.id,
              name: `native-source-${provider}`,
              expectedRevision: (await get(a, home.home.id)).revision,
            }),
          );
          const projectChildren = await checked(
            request(a, `/entries/${sourceProject.entry.id}/children`),
          );
          const sourceParent = (projectChildren.entries as FilesystemEntry[]).find(
            (entry) => entry.name === 'source',
          )!;
          const parent = await get(a, sourceParent.id);
          const created = await mutate(a, randomUUID(), [
            {
              op: 'create',
              parentId: parent.id,
              expectedParentRevision: parent.revision,
              name: 'ontology',
              kind: 'directory',
            },
          ]);
          const root = (created.entries as FilesystemEntry[]).find(
            (entry) => entry.name === 'ontology',
          )!;
          const files = [
            {
              path: '.ontology.json',
              text: JSON.stringify({
                version: 1,
                name: 'Native source',
                namespace: 'native_source',
              }),
            },
            {
              path: 'stations.resource.json',
              text: JSON.stringify({
                apiVersion: 'moria/v1alpha2',
                kind: 'Dataset',
                metadata: { namespace: 'native_source', name: 'stations' },
                spec: {
                  columns: { id: { type: 'uid', nullable: false } },
                  writeModes: ['replace'],
                },
              }),
            },
            {
              path: 'station.object.json',
              text: JSON.stringify({
                apiVersion: 'moria/v1alpha2',
                kind: 'ObjectType',
                metadata: { namespace: 'native_source', name: 'Station' },
                spec: {
                  primaryKey: 'id',
                  properties: { id: { type: 'uid', nullable: false } },
                  source: { resource: 'native_source/Dataset/stations', mapping: { id: 'id' } },
                },
              }),
            },
          ];
          const upload = async (parentId: string, name: string, value: unknown) => {
            const bytes = new TextEncoder().encode(JSON.stringify(value));
            const parent = await get(a, parentId);
            const response = await fetch(
              `${a}/api/management/v1/fs/entries/${parentId}/files?${new URLSearchParams({ name, expectedRevision: parent.revision })}`,
              {
                method: 'POST',
                headers: {
                  authorization: `Bearer ${ownerToken}`,
                  'idempotency-key': randomUUID(),
                  'content-type': 'application/json',
                  'x-moria-size': String(bytes.length),
                  'x-moria-digest': `sha256:${createHash('sha256').update(bytes).digest('hex')}`,
                },
                body: bytes,
              },
            );
            const result = await response.json();
            assert.equal(response.status, 200, JSON.stringify(result));
            return (result.entries as FilesystemEntry[]).find((entry) => entry.name === name)!;
          };
          for (const file of files) await upload(root.id, file.path, JSON.parse(file.text));
          const role = await checked(
            request(a, `/sources/${root.id}/register`, 'POST', {
              kind: 'ontology',
              expectedRevision: (await get(a, root.id)).revision,
            }),
          );
          const checkout = await checked(request(b, `/sources/${root.id}/checkout`, 'POST', {}));
          const compilerContext = {
            principalAttributes: { region: { type: 'string' as const, nullable: false } },
          };
          const assembled = compileSource(
            {
              kind: 'ontology',
              projectId: role.projectId,
              deploymentUnitId: role.deploymentUnitId,
              files: [...files].sort((a, b) => a.path.localeCompare(b.path)),
            },
            compilerContext,
          );
          const artifact = {
            version: 1,
            rootId: root.id,
            sourceDigest: checkout.digest,
            ...assembled,
            compilerContext,
          };
          const uploadArtifact = async (value: unknown, name: string, parentId = home.home.id) => {
            const bytes = new TextEncoder().encode(JSON.stringify(value));
            const parent = await get(a, parentId);
            const path = `/sources/${root.id}/artifacts?${new URLSearchParams({ sourceDigest: checkout.digest, parentId, name, expectedRevision: parent.revision })}`;
            const response = await fetch(`${a}/api/management/v1/fs${path}`, {
              method: 'POST',
              headers: {
                authorization: `Bearer ${ownerToken}`,
                'idempotency-key': randomUUID(),
                'content-type': 'application/json',
                'x-moria-size': String(bytes.length),
                'x-moria-digest': `sha256:${createHash('sha256').update(bytes).digest('hex')}`,
              },
              body: bytes,
            });
            const result = await response.json();
            assert.equal(response.status, 200, JSON.stringify(result));
            return (result.entries as FilesystemEntry[]).find((entry) => entry.name === name)!;
          };
          const unapproved = await upload(home.home.id, `unapproved-${provider}.json`, artifact);
          await checked(
            request(a, `/sources/${root.id}/publish`, 'POST', {
              expectedSourceDigest: checkout.digest,
              artifact: {
                entryId: unapproved.id,
                revision: unapproved.revision,
                digest: unapproved.content!.digest,
              },
            }),
            400,
          );
          const bodyMarked = structuredClone(artifact);
          bodyMarked.compiled.manifest.resources[0].metadata.markings = ['private'];
          bodyMarked.compiled.fingerprint = fingerprint(bodyMarked.compiled.manifest);
          const bodyOnly = await uploadArtifact(bodyMarked, `body-marked-${provider}.json`);
          assert.deepEqual(
            (
              await db.pool.query('SELECT markings FROM moria.fs_policies WHERE entry_id=$1', [
                bodyOnly.id,
              ])
            ).rows[0].markings,
            [{ environment, marking: 'private' }],
          );
          await checked(
            request(a, `/entries/${root.id}/policy`, 'POST', {
              expectedRevision: '0',
              policy: { markings: [{ environment, marking: 'private' }], grants: [] },
            }),
          );
          const artifactFile = await uploadArtifact(artifact, `compiled-${provider}.json`);
          const artifactRef = {
            entryId: artifactFile.id,
            revision: artifactFile.revision,
            digest: artifactFile.content!.digest,
          };
          const publication = await checked(
            request(a, `/sources/${root.id}/publish`, 'POST', {
              expectedSourceDigest: checkout.digest,
              artifact: artifactRef,
            }),
          );
          const phases: string[] = [];
          const browserPublication = await publishBrowserSource(
            filesystemClient(
              createTransport({
                baseUrl: b,
                fetch: (url, init) =>
                  fetch(url, {
                    ...init,
                    headers: { ...init?.headers, authorization: `Bearer ${ownerToken}` },
                  }),
              }),
            ),
            root.id,
            (message) => phases.push(message),
          );
          assert.equal(browserPublication.digest, publication.digest);
          assert(phases.some((message) => message.includes('native configuration validation')));
          const priorToken = process.env.MORIA_API_TOKEN;
          try {
            process.env.MORIA_API_TOKEN = ownerToken;
            const cli = (await filesystemCommand(['publish', root.id], { url: a })) as {
              digest: string;
            };
            assert.equal(cli.digest, publication.digest);
          } finally {
            if (priorToken === undefined) delete process.env.MORIA_API_TOKEN;
            else process.env.MORIA_API_TOKEN = priorToken;
          }
          assert.deepEqual(
            (
              await db.pool.query('SELECT markings FROM moria.fs_policies WHERE entry_id=$1', [
                artifactFile.id,
              ])
            ).rows[0].markings,
            [{ environment, marking: 'private' }],
          );
          assert.equal(
            publication.digest,
            fingerprint({ source: checkout.digest, compiled: assembled.compiled.fingerprint }),
          );
          const release = await checked(
            request(b, `/releases/${encodeURIComponent(publication.digest)}`),
          );
          assert.equal(release.compiled.fingerprint, assembled.compiled.fingerprint);
          const plan = await checked(
            request(a, `/sources/${root.id}/plan`, 'POST', {
              digest: publication.digest,
              deletions: [],
            }),
          );
          const activation = await checked(request(b, `/plans/${plan.id}/apply`, 'POST', {}));
          assert.equal(activation.runtimeReady, false);
          assert.equal(activation.revision, '1');
          assert.deepEqual(
            await checked(request(a, `/plans/${plan.id}/apply`, 'POST', {})),
            activation,
          );
          const placements = (
            await db.pool.query(
              'SELECT e.resource_id,b.reference FROM moria.fs_resource_bindings b JOIN moria.fs_entries e ON e.id=b.entry_id WHERE b.root_id=$1 AND b.active',
              [root.id],
            )
          ).rows;
          assert.equal(placements.length, 3);
          for (const identity of publication.resources)
            assert.equal(
              placements.find((row) => row.reference === identity.reference).resource_id,
              identity.id,
            );
          const changed = structuredClone(artifact);
          changed.compiled.manifest.resources.find(
            (resource) => resource.kind === 'Dataset',
          )!.spec.writeModes = ['append'];
          changed.compiled.fingerprint = fingerprint(changed.compiled.manifest);
          const forged = await uploadArtifact(changed, `forged-${provider}.json`);
          await checked(
            request(a, `/sources/${root.id}/publish`, 'POST', {
              expectedSourceDigest: checkout.digest,
              artifact: {
                entryId: forged.id,
                revision: forged.revision,
                digest: forged.content!.digest,
              },
            }),
            400,
          );
          await assert.rejects(
            db.pool.query("UPDATE moria.fs_source_releases SET owned='[]' WHERE digest=$1", [
              publication.digest,
            ]),
            /IMMUTABLE_SOURCE_ARTIFACT/,
          );
          const peerName = `compiler-peer-${provider}`;
          await authority.syncPrincipal(environment, 'fixture', peerName, {
            kind: 'workload',
            enabled: true,
            markings: ['private'],
            attributes: { groups: [] },
          });
          await authority.setGrants(environment, 'fixture', peerName, application, operations);
          const peerToken = (
            await sessions.mint({ environment, principal: peerName, application }, 'fixture')
          ).token;
          const peerHome = await checked(
            request(a, '/home', 'GET', undefined, undefined, peerToken),
          );
          const peerGrant = {
            subject: { kind: 'principal', id: peerHome.principalId },
            operations: ['read', 'export'],
            inherit: true,
            effect: 'allow',
          };
          await checked(
            request(a, `/entries/${root.id}/policy`, 'POST', {
              expectedRevision: '1',
              policy: { markings: [{ environment, marking: 'private' }], grants: [peerGrant] },
            }),
          );
          await checked(
            request(a, `/entries/${artifactFile.id}/policy`, 'POST', {
              expectedRevision: '0',
              policy: { markings: [{ environment, marking: 'private' }], grants: [peerGrant] },
            }),
          );
          await get(b, artifactFile.id, peerToken);
          await fs.filesystem.get(
            { session: await sessions.authenticate(`Bearer ${peerToken}`) },
            artifactFile.id,
          );
          const homeNow = await get(a, home.home.id);
          const copyCreated = await mutate(a, randomUUID(), [
            {
              op: 'create',
              parentId: homeNow.id,
              expectedParentRevision: homeNow.revision,
              name: `private-artifacts-${provider}`,
              kind: 'directory',
            },
          ]);
          const copyParent = (copyCreated.entries as FilesystemEntry[]).find(
            (entry) => entry.name === `private-artifacts-${provider}`,
          )!;
          await checked(
            request(a, `/entries/${copyParent.id}/policy`, 'POST', {
              expectedRevision: '0',
              policy: { markings: [{ environment, marking: 'private' }], grants: [] },
            }),
          );
          const copyReceipt = await checked(
            request(
              a,
              '/copy',
              'POST',
              {
                id: artifactFile.id,
                expectedRevision: artifactFile.revision,
                parentId: copyParent.id,
                expectedParentRevision: (await get(a, copyParent.id)).revision,
                name: 'copied.json',
              },
              randomUUID(),
            ),
          );
          const copied = (copyReceipt.entries as FilesystemEntry[]).find(
            (entry) => entry.name === 'copied.json',
          )!;
          await checked(
            request(a, `/entries/${copied.id}/policy`, 'POST', {
              expectedRevision: '0',
              policy: { markings: [], grants: [peerGrant] },
            }),
          );
          await get(b, copied.id, peerToken);
          await checked(
            request(a, `/entries/${root.id}/policy`, 'POST', {
              expectedRevision: '2',
              policy: {
                markings: [{ environment, marking: 'private' }],
                grants: [{ ...peerGrant, operations: ['read'], effect: 'deny' }],
              },
            }),
          );
          await checked(
            request(b, `/entries/${artifactFile.id}`, 'GET', undefined, undefined, peerToken),
            404,
          );
          const raw = await fetch(`${b}/api/management/v1/fs/entries/${artifactFile.id}/content`, {
            headers: { authorization: `Bearer ${peerToken}` },
          });
          assert.equal(raw.status, 404);
          await assert.rejects(
            fs.filesystem.get(
              { session: await sessions.authenticate(`Bearer ${peerToken}`) },
              artifactFile.id,
            ),
            { code: 'NOT_FOUND' },
          );
          await checked(
            request(b, `/entries/${copied.id}`, 'GET', undefined, undefined, peerToken),
            404,
          );
          await assert.rejects(
            fs.filesystem.get(
              { session: await sessions.authenticate(`Bearer ${peerToken}`) },
              copied.id,
            ),
            { code: 'NOT_FOUND' },
          );
          await checked(
            request(a, `/sources/${root.id}/retire`, 'POST', { expectedRevision: role.revision }),
          );
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*)::int AS n FROM moria.fs_resource_bindings WHERE root_id=$1 AND active',
                [root.id],
              )
            ).rows[0].n,
            0,
          );
        },
      );
      await t.test(
        'qualified native file bytes preserve Node storage compatibility, immutable versions and replay across replicas',
        async () => {
          const bytes = new Uint8Array(2 * 1024 * 1024 + 17).map((_, i) => i % 251);
          const digest = (value: Uint8Array) =>
            `sha256:${createHash('sha256').update(value).digest('hex')}`;
          const upload = async (
            base: string,
            path: string,
            body: Uint8Array,
            key: string,
            declared = body,
          ) =>
            fetch(`${base}/api/management/v1/fs${path}`, {
              method: 'POST',
              headers: {
                authorization: `Bearer ${ownerToken}`,
                'idempotency-key': key,
                'content-type': 'application/octet-stream',
                'x-moria-size': String(declared.length),
                'x-moria-digest': digest(declared),
              },
              body: new Uint8Array(body),
            });
          const parent = await get(a, privateParent.id),
            key = randomUUID(),
            path = `/entries/${parent.id}/files?${new URLSearchParams({ name: 'payload.bin', expectedRevision: parent.revision })}`;
          const receipt = await (await upload(a, path, bytes, key)).json();
          assert.ok(receipt.mutationId, JSON.stringify(receipt));
          const entry = receipt.entries.find(
            (entry: FilesystemEntry) => entry.kind === 'file',
          ) as FilesystemEntry;
          assert.equal(entry.content?.digest, digest(bytes));
          const downloaded = await fetch(`${b}/api/management/v1/fs/entries/${entry.id}/content`, {
            headers: { authorization: `Bearer ${ownerToken}` },
          });
          assert.equal(downloaded.status, 200);
          assert.equal(downloaded.headers.get('x-moria-digest'), digest(bytes));
          assert.deepEqual(new Uint8Array(await downloaded.arrayBuffer()), bytes);
          const legacy = await fs.filesystem.readFile({ session: owner }, entry.id);
          assert.deepEqual(new Uint8Array(await new Response(legacy.stream).arrayBuffer()), bytes);
          const replay = await (await upload(b, path, bytes, key)).json();
          assert.deepEqual(replay, receipt);
          const after = await get(b, parent.id);
          assert.equal(BigInt(after.revision), BigInt(parent.revision) + 1n);
          const next = new Uint8Array([0, 255, 7, 9]);
          const replaced = await upload(
            b,
            `/entries/${entry.id}/content?expectedRevision=${entry.revision}`,
            next,
            randomUUID(),
          );
          assert.equal(replaced.status, 200, await replaced.text());
          const historical = await fetch(
            `${a}/api/management/v1/fs/entries/${entry.id}/content?revision=${entry.revision}`,
            { headers: { authorization: `Bearer ${ownerToken}` } },
          );
          assert.equal(historical.status, 200);
          assert.deepEqual(new Uint8Array(await historical.arrayBuffer()), bytes);
          assert.equal((await upload(a, path, next, key)).status, 409);
          assert.equal(
            (
              await fetch(`${a}/api/management/v1/fs/entries/${entry.id}/content`, {
                headers: { authorization: `Bearer ${viewerToken}` },
              })
            ).status,
            404,
          );
          const current = await get(a, parent.id),
            badPath = `/entries/${parent.id}/files?${new URLSearchParams({ name: 'bad.bin', expectedRevision: current.revision })}`;
          const corrupt = await upload(
            a,
            badPath,
            new Uint8Array([1, 2, 3, 4]),
            randomUUID(),
            new Uint8Array([4, 3, 2, 1]),
          );
          assert.equal(corrupt.status, 409, await corrupt.text());
          assert.equal((await get(b, parent.id)).revision, current.revision);
          assert.equal(
            (
              await db.pool.query(
                "SELECT count(*)::int AS n FROM moria.fs_entries WHERE parent_id=$1 AND name='bad.bin'",
                [parent.id],
              )
            ).rows[0].n,
            0,
          );
        },
      );
      await t.test(
        'native file copy preserves source provenance and policy writes enforce CAS, deny precedence and marking monotonicity',
        async () => {
          const source = (
            await db.pool.query(
              "SELECT id FROM moria.fs_entries WHERE parent_id=$1 AND name='payload.bin'",
              [privateParent.id],
            )
          ).rows[0].id;
          const original = await get(a, source),
            parent = await get(a, publicParent.id),
            key = randomUUID();
          const copy = {
            id: source,
            expectedRevision: original.revision,
            parentId: parent.id,
            expectedParentRevision: parent.revision,
            name: 'private-copy.bin',
          };
          const receipt = await checked(request(a, '/copy', 'POST', copy, key));
          const copied = receipt.entries.find(
            (entry: FilesystemEntry) => entry.kind === 'file',
          ) as FilesystemEntry;
          assert.deepEqual(copied.content, original.content);
          assert.deepEqual(await checked(request(b, '/copy', 'POST', copy, key)), receipt);
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*)::int AS n FROM moria.fs_provenance WHERE entry_id=$1 AND source_id=$2',
                [copied.id, source],
              )
            ).rows[0].n,
            1,
          );
          await checked(
            request(b, `/entries/${copied.id}`, 'GET', undefined, undefined, viewerToken),
            404,
          );
          const denied = {
            markings: [],
            grants: [
              {
                subject: { kind: 'principal', id: viewerHome.principalId },
                operations: ['read'],
                inherit: false,
                effect: 'deny',
              },
            ],
          };
          const results = await Promise.all([
            request(a, `/entries/${parent.id}/policy`, 'POST', {
              expectedRevision: '0',
              policy: denied,
            }),
            request(b, `/entries/${parent.id}/policy`, 'POST', {
              expectedRevision: '0',
              policy: denied,
            }),
          ]);
          assert.deepEqual(results.map((result) => result.response.status).sort(), [200, 409]);
          assert.equal((await checked(request(a, `/entries/${parent.id}/policy`))).revision, '1');
          await checked(
            request(b, `/entries/${parent.id}`, 'GET', undefined, undefined, viewerToken),
            404,
          );
          const policy = await checked(request(a, `/entries/${privateParent.id}/policy`));
          await checked(
            request(a, `/entries/${privateParent.id}/policy`, 'POST', {
              expectedRevision: policy.revision,
              policy: { markings: [], grants: [] },
            }),
            422,
          );
          assert.equal(
            (await checked(request(a, `/entries/${privateParent.id}/policy`))).revision,
            policy.revision,
          );
          await checked(
            request(a, `/entries/${privateParent.id}/policy`, 'POST', {
              expectedRevision: policy.revision,
              policy: {
                markings: policy.markings,
                grants: [
                  {
                    subject: { kind: 'principal', id: randomUUID() },
                    operations: ['read'],
                    inherit: true,
                    effect: 'allow',
                  },
                ],
              },
            }),
            400,
          );
          assert.equal(
            (
              await db.pool.query(
                "SELECT count(*)::int AS n FROM moria.instance_audit WHERE operation='setFilesystemPolicy' AND target=$1",
                [parent.id],
              )
            ).rows[0].n,
            1,
          );
        },
      );
      await t.test(
        'managed/cyclic placements, untrusted content and malformed names fail atomically',
        async () => {
          const parent = await get(a, project.id),
            entry = await get(a, publicParent.id),
            target = await get(a, moving.id);
          await checked(
            request(
              a,
              '/changes',
              'POST',
              {
                changes: [
                  {
                    op: 'move',
                    id: entry.id,
                    expectedRevision: entry.revision,
                    parentId: target.id,
                    expectedParentRevision: target.revision,
                    expectedSourceParentRevision: parent.revision,
                    name: 'cycle',
                  },
                ],
              },
              randomUUID(),
            ),
            400,
          );
          await checked(
            request(
              a,
              '/changes',
              'POST',
              {
                changes: [
                  {
                    op: 'create',
                    parentId: project.id,
                    expectedParentRevision: parent.revision,
                    name: 'e\u0301',
                    kind: 'directory',
                  },
                ],
              },
              randomUUID(),
            ),
            400,
          );
          await checked(
            request(
              a,
              '/changes',
              'POST',
              {
                changes: [
                  {
                    op: 'create',
                    parentId: project.id,
                    expectedParentRevision: parent.revision,
                    name: 'unsafe',
                    kind: 'directory',
                    scope: { projectId: randomUUID() },
                  },
                ],
              },
              randomUUID(),
            ),
            422,
          );
          assert.equal((await get(b, project.id)).revision, parent.revision);
        },
      );
      await t.test(
        'a sealed upload survives uploader suspension and database loss, then recovers under the next fence',
        async () => {
          const parent = await get(b, privateParent.id),
            key = randomUUID(),
            bytes = new Uint8Array([1, 4, 9, 16, 25, 36]);
          const digest = `sha256:${createHash('sha256').update(bytes).digest('hex')}`;
          const path = `/api/management/v1/fs/entries/${parent.id}/files?${new URLSearchParams({ name: 'recovered.bin', expectedRevision: parent.revision })}`;
          const send = (base: string) =>
            fetch(`${base}${path}`, {
              method: 'POST',
              headers: {
                authorization: `Bearer ${ownerToken}`,
                'idempotency-key': key,
                'content-type': 'application/octet-stream',
                'x-moria-size': String(bytes.length),
                'x-moria-digest': digest,
              },
              body: new Uint8Array(bytes),
            });
          const lock = await db.pool.connect();
          const barrier = 773211;
          await lock.query('SELECT pg_advisory_lock($1)', [barrier]);
          await db.pool.query(
            `CREATE FUNCTION moria.native_fs_fixture_receipt_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.request_key='${key}' THEN PERFORM pg_advisory_xact_lock(${barrier}); END IF; RETURN NEW; END $$;CREATE TRIGGER native_fs_fixture_receipt_barrier BEFORE INSERT ON moria.fs_mutations FOR EACH ROW EXECUTE FUNCTION moria.native_fs_fixture_receipt_barrier()`,
          );
          let stopped = false;
          try {
            const pending = send(a).then(
              async (response) => ({ status: response.status, body: await response.json() }),
              (error) => ({ status: 0, body: String(error) }),
            );
            let backend: number | undefined;
            for (let i = 0; i < 500 && !backend; i++) {
              backend = (
                await db.pool.query(
                  "SELECT pid FROM pg_stat_activity WHERE usename=$1 AND query LIKE 'INSERT INTO moria.fs_mutations%' AND wait_event='advisory'",
                  [role],
                )
              ).rows[0]?.pid;
              if (!backend) await delay(10);
            }
            assert.ok(backend, 'native metadata publication reached the actual SQL barrier');
            assert.ok(processes[0].kill('SIGSTOP'));
            stopped = true;
            const candidate = (
              await db.pool.query(
                'SELECT id,location,fence,state FROM moria.native_filesystem_uploads WHERE request_key=$1',
                [key],
              )
            ).rows[0];
            assert.equal(candidate.fence, '1');
            assert.equal(
              (
                await db.pool.query(
                  "SELECT count(*)::int AS n FROM moria.fs_entries WHERE parent_id=$1 AND name='recovered.bin'",
                  [parent.id],
                )
              ).rows[0].n,
              0,
            );
            await db.pool.query('SELECT pg_terminate_backend($1)', [backend]);
            await lock.query('SELECT pg_advisory_unlock($1)', [barrier]);
            const early = await send(b);
            assert.equal(early.status, 503, await early.text());
            let expired = false;
            for (let i = 0; i < 200 && !expired; i++) {
              expired = (
                await db.pool.query(
                  'SELECT lease_until<=clock_timestamp() AS expired FROM moria.native_filesystem_uploads WHERE id=$1',
                  [candidate.id],
                )
              ).rows[0].expired;
              if (!expired) await delay(200);
            }
            assert.equal(
              expired,
              true,
              'the original thirty-second lease expired on the database clock',
            );
            const recovered = await send(b);
            const receipt = await recovered.json();
            assert.equal(recovered.status, 200, JSON.stringify(receipt));
            const current = (
              await db.pool.query(
                'SELECT location,fence,state,receipt FROM moria.native_filesystem_uploads WHERE id=$1',
                [candidate.id],
              )
            ).rows[0];
            assert.equal(current.fence, '2');
            assert.equal(current.state, 'committed');
            assert.deepEqual(current.location, candidate.location);
            assert.deepEqual(current.receipt, receipt);
            assert.ok(processes[0].kill('SIGCONT'));
            stopped = false;
            const stale = await pending;
            assert.notEqual(stale.status, 200, JSON.stringify(stale));
            const entry = receipt.entries.find(
              (entry: FilesystemEntry) => entry.kind === 'file',
            ) as FilesystemEntry;
            const read = await fetch(`${b}/api/management/v1/fs/entries/${entry.id}/content`, {
              headers: { authorization: `Bearer ${ownerToken}` },
            });
            assert.equal(read.status, 200);
            assert.deepEqual(new Uint8Array(await read.arrayBuffer()), bytes);
            assert.equal(
              (
                await db.pool.query(
                  'SELECT count(*)::int AS n FROM moria.fs_mutations WHERE request_key=$1',
                  [key],
                )
              ).rows[0].n,
              1,
            );
            assert.equal(
              (
                await db.pool.query(
                  "SELECT count(*)::int AS n FROM moria.fs_entries WHERE parent_id=$1 AND name='recovered.bin'",
                  [parent.id],
                )
              ).rows[0].n,
              1,
            );
          } finally {
            if (stopped) processes[0].kill('SIGCONT');
            await lock.query('SELECT pg_advisory_unlock_all()');
            lock.release();
            await db.pool.query(
              'DROP TRIGGER native_fs_fixture_receipt_barrier ON moria.fs_mutations;DROP FUNCTION moria.native_fs_fixture_receipt_barrier()',
            );
          }
        },
      );
    },
  );
