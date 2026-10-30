import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { CatalogStorage, type CatalogStorageBinding } from '../services/catalog/src/storage.js';
import {
  metadataStream,
  type MetadataPublication,
} from '../services/catalog/src/storage-publication.js';
import { Authority } from '../services/security/src/authority.js';
import { FilesystemAuthority } from '../services/security/src/filesystem.js';
import { FilesystemStore } from '../services/filesystem/src/store.js';
import { filesystemRoot } from '../packages/contracts/filesystem.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import {
  createAuthorizedLocalStorage,
  createAuthorizedS3Storage,
} from '../services/storage/src/authority.js';
import { StorageError } from '../services/storage/src/locations.js';
import { database } from './fixtures/catalog-storage/database.js';
import { startS3 } from './fixtures/storage-s3/server.js';
import { iceberg } from './iceberg-helper.js';

for (const provider of ['local', 's3'] as const) {
  test(`${provider}: durable scoped metadata publication`, { timeout: 180000 }, async (t) => {
    const db = await database();
    t.after(db.close);
    const server = provider === 's3' ? await startS3() : undefined;
    if (server) {
      t.after(server.close);
    }
    const { catalog, pool } = db;
    const authority = new Authority(catalog);
    const sessions = new WorkloadSessions(authority);
    const staged = await iceberg({
      operation: 'stage',
      warehouse: join(db.root, 'iceberg'),
      schema: {
        type: 'struct',
        'schema-id': 0,
        fields: [{ id: 1, name: 'value', required: true, type: 'string' }],
      },
      rows: [{ value: 'synthetic' }],
    });
    const bytes = await readFile(staged.metadataLocation);
    const root = join(db.root, 'objects');
    await mkdir(root);
    async function setup(datasets = ['test/Dataset/a', 'test/Dataset/b']) {
      const environment = randomUUID();
      await catalog.ensureEnvironment(environment);
      for (const dataset of datasets) {
        await catalog.ensureDataset(environment, dataset);
      }
      await authority.syncPrincipal(environment, 'admin', 'engine', {
        enabled: true,
        kind: 'workload',
        attributes: {},
        markings: ['protected'],
      });
      const operations = datasets.flatMap((dataset) => [`${dataset}:read`, `${dataset}:write`]);
      await authority.setApplication(environment, 'admin', 'app', operations);
      await authority.setGrants(environment, 'admin', 'engine', 'app', operations);
      const minted = await sessions.mint(
        { environment, principal: 'engine', application: 'app' },
        'admin',
      );
      const auth = await sessions.authenticate(`Bearer ${minted.token}`);
      const execution = randomUUID();
      const fence = await catalog.claim(environment, execution, 'worker');
      let afterWrite: (index: number) => Promise<void> = async () => {};
      let beforeWrite: (index: number) => Promise<void> = async () => {};
      let writes = 0;
      const configurations: CatalogStorageBinding[] = datasets.map((dataset, index) => ({
        environment,
        dataset,
        kind: 'Dataset',
        markings: ['protected'],
        warehouseId: environment,
        tableId: `table${index}`,
        icebergTableUuid: staged.tableUuid,
        storage: async (scope, current) => {
          const binding = {
            root,
            warehouseId: environment,
            tableId: `table${index}`,
            maxObjectBytes: 1024 * 1024,
            chunkBytes: 256,
            endpoint: server?.endpoint ?? '',
            bucket: server?.bucket ?? '',
            credentials: server?.credentials ?? { accessKeyId: '', secretAccessKey: '' },
            prefix: 'publication',
            region: 'us-east-1',
          };
          const storage =
            provider === 'local'
              ? await createAuthorizedLocalStorage(binding, scope, current)
              : await createAuthorizedS3Storage(binding, scope, current);
          return {
            ...storage,
            write: async (logical, stream, options) => {
              assert.deepEqual(scope.operations, ['create']);
              await beforeWrite(index);
              await storage.write(logical, stream, options);
              writes++;
              await afterWrite(index);
            },
          };
        },
      }));
      const service = new CatalogStorage(authority, configurations);
      const request = (): MetadataPublication => ({
        execution,
        worker: 'worker',
        fence,
        outputs: datasets.map((dataset) => ({
          dataset,
          expectedRevision: '0',
          snapshotId: staged.snapshotId,
          size: bytes.length,
          stream: metadataStream(bytes),
        })),
        lineage: {},
        checkpoint: {
          connection: 'source',
          stream: 'rows',
          configRevision: '1',
          expectedVersion: '0',
          value: { offset: '10' },
        },
      });
      const invisible = async () => {
        assert.deepEqual(
          (await catalog.heads(environment, datasets)).map((head) => head.revision),
          ['0', '0'],
        );
        for (const dataset of datasets)
          await assert.rejects(service.read(auth, dataset), { code: 'NOT_FOUND' });
        for (const table of ['snapshot_history', 'publications', 'checkpoints']) {
          assert.equal(
            (await pool.query(`SELECT 1 FROM moria.${table} WHERE environment=$1`, [environment]))
              .rowCount,
            0,
          );
        }
      };
      return {
        environment,
        datasets,
        auth,
        execution,
        fence,
        service,
        request,
        invisible,
        configurations,
        writes: () => writes,
        before: (callback: typeof beforeWrite) => {
          beforeWrite = callback;
        },
        after: (callback: typeof afterWrite) => {
          afterWrite = callback;
        },
      };
    }

    await t.test(
      'filesystem input classifications cannot be omitted from publication outputs',
      async () => {
        const f = await setup([randomUUID(), randomUUID()]);
        const security = new FilesystemAuthority(authority);
        const store = new FilesystemStore(catalog);
        const instanceId = await security.createPrincipal('admin', 'workload');
        await security.bindPrincipal('admin', f.environment, 'engine', instanceId);
        const operations = [
          ...f.datasets.flatMap((dataset) => [`${dataset}:read`, `${dataset}:write`]),
          'filesystem:read',
          'filesystem:write',
        ];
        await authority.setApplication(f.environment, 'admin', 'app', operations);
        await authority.setGrants(f.environment, 'admin', 'engine', 'app', operations);
        await authority.syncPrincipal(f.environment, 'admin', 'engine', {
          enabled: true,
          kind: 'workload',
          attributes: {},
          markings: ['protected', 'classified'],
        });
        await security.setPolicy('admin', filesystemRoot, '0', {
          markings: [],
          grants: [
            {
              subject: { kind: 'principal', id: instanceId },
              operations: ['read', 'write'],
              inherit: true,
              effect: 'allow',
            },
          ],
        });
        const actor = { environment: f.environment, principal: 'admin', application: 'app' };
        const parent = (
          await store.apply(actor, randomUUID(), [
            {
              op: 'create',
              kind: 'directory',
              parentId: filesystemRoot,
              expectedParentRevision: (await store.get(filesystemRoot)).revision,
              name: `publication-${randomUUID()}`,
              scope: { projectId: randomUUID(), environmentId: f.environment },
            },
          ])
        ).entries.find((entry) => entry.parentId === filesystemRoot)!;
        for (const [index, dataset] of f.datasets.entries()) {
          const entry = (
            await store.apply(actor, randomUUID(), [
              {
                op: 'create',
                kind: 'resource',
                parentId: parent.id,
                expectedParentRevision: (await store.get(parent.id)).revision,
                name: `dataset-${index}`,
                resourceId: dataset,
              },
            ])
          ).entries.find((entry) => entry.resourceId === dataset)!;
          await security.setPolicy('admin', entry.id, '0', {
            markings: index === 0 ? [{ environment: f.environment, marking: 'classified' }] : [],
            grants: [],
          });
        }
        await f.service.publish(f.auth, f.request());
        const writes = f.writes();
        const execution = randomUUID();
        const request = {
          ...f.request(),
          execution,
          fence: await catalog.claim(f.environment, execution, 'worker'),
          lineage: { [f.datasets[0]]: '1' },
        };
        for (const output of request.outputs) output.expectedRevision = '1';
        request.checkpoint!.expectedVersion = '1';
        await assert.rejects(f.service.publish(f.auth, request), { code: 'MARKING_DOWNGRADE' });
        assert.equal(
          f.writes(),
          writes,
          'classified input must be rejected before any object write',
        );
        assert.deepEqual(
          (await catalog.heads(f.environment, f.datasets)).map((head) => head.revision),
          ['1', '1'],
        );
      },
    );

    await t.test(
      'Iceberg read-back, atomic two-output heads/history/checkpoint and acknowledgment retry',
      async () => {
        const f = await setup();
        f.after(async (index) => {
          if (index === 0) {
            await f.invisible();
          }
        });
        const receipt = await f.service.publish(f.auth, f.request());
        assert.deepEqual(
          receipt.outputs.map((output) => output.revision),
          ['1', '1'],
        );
        for (const dataset of f.datasets) {
          const read = await f.service.read(f.auth, dataset);
          assert.equal(read.execution, f.execution);
          assert.equal(read.snapshotId, staged.snapshotId);
          const chunks = [];
          for await (const chunk of read.stream) {
            chunks.push(chunk);
          }
          assert.deepEqual(Buffer.concat(chunks), bytes);
        }
        assert.equal(
          (
            await pool.query('SELECT version FROM moria.checkpoints WHERE environment=$1', [
              f.environment,
            ])
          ).rows[0].version,
          '1',
        );
        assert.equal(
          (
            await pool.query('SELECT * FROM moria.snapshot_history WHERE environment=$1', [
              f.environment,
            ])
          ).rowCount,
          2,
        );
        // Discard the first acknowledgment. Retry also works after the lease expires.
        await pool.query(
          "UPDATE moria.leases SET expires_at=clock_timestamp()-interval '1 second' WHERE environment=$1",
          [f.environment],
        );
        assert.deepEqual(await f.service.publish(f.auth, f.request()), receipt);
        assert.equal(f.writes(), 2);
        assert.deepEqual(
          (await catalog.heads(f.environment, f.datasets)).map((head) => head.revision),
          ['1', '1'],
        );
        const conflicting = f.request();
        conflicting.outputs[0].stream = metadataStream(Buffer.concat([bytes, Buffer.from('\n')]));
        conflicting.outputs[0].size++;
        await assert.rejects(f.service.publish(f.auth, conflicting), {
          code: 'PUBLICATION_CONFLICT',
        });
        const provenance = f.request();
        provenance.lineage[f.datasets[0]] = 'different-input-snapshot';
        await assert.rejects(f.service.publish(f.auth, provenance), {
          code: 'PUBLICATION_CONFLICT',
        });
      },
    );

    async function setupLineage() {
      const f = await setup();
      const input = 'test/Dataset/input';
      await catalog.ensureDataset(f.environment, input);
      await authority.syncPrincipal(f.environment, 'admin', 'engine', {
        enabled: true,
        kind: 'workload',
        attributes: {},
        markings: ['protected', 'inherited'],
      });
      const operations = [
        ...f.datasets.flatMap((dataset) => [`${dataset}:read`, `${dataset}:write`]),
        `${input}:read`,
      ];
      await authority.setApplication(f.environment, 'admin', 'app', operations);
      await authority.setGrants(f.environment, 'admin', 'engine', 'app', operations);
      const bindings: CatalogStorageBinding[] = [
        ...f.configurations.map((binding) => ({
          ...binding,
          markings: ['protected', 'inherited'],
        })),
        {
          ...f.configurations[0],
          dataset: input,
          markings: ['inherited'],
        },
      ];
      const service = new CatalogStorage(authority, bindings);
      // The trusted engine declares the exact snapshot generated by PyIceberg above.
      const lineage = { [input]: staged.snapshotId };
      const request = () => ({ ...f.request(), lineage: { ...lineage } });
      const state = async () => {
        const result: Record<string, unknown[]> = {};
        for (const table of ['dataset_heads', 'snapshot_history', 'checkpoints', 'publications']) {
          result[table] = (
            await pool.query(
              `SELECT * FROM moria.${table} AS state
               WHERE environment=$1 ORDER BY to_jsonb(state)::text`,
              [f.environment],
            )
          ).rows;
        }
        return result;
      };
      return { ...f, input, operations, bindings, service, lineage, request, state };
    }

    await t.test('nonempty lineage persists exact provenance for both outputs', async () => {
      const f = await setupLineage();
      const receipt = await f.service.publish(f.auth, f.request());
      assert.equal(f.writes(), 2);
      const history = await pool.query(
        `SELECT dataset,revision,snapshot_id,execution,lineage
         FROM moria.snapshot_history WHERE environment=$1 ORDER BY dataset`,
        [f.environment],
      );
      assert.deepEqual(
        history.rows,
        f.datasets.map((dataset) => ({
          dataset,
          revision: '1',
          snapshot_id: staged.snapshotId,
          execution: f.execution,
          lineage: f.lineage,
        })),
      );
      for (const dataset of f.datasets) {
        const read = await f.service.read(f.auth, dataset);
        assert.equal(read.execution, f.execution);
        assert.equal(read.snapshotId, staged.snapshotId);
        const chunks = [];
        for await (const chunk of read.stream) {
          chunks.push(chunk);
        }
        assert.deepEqual(Buffer.concat(chunks), bytes);
      }
      const committed = await f.state();
      assert.deepEqual(await f.service.publish(f.auth, f.request()), receipt);
      assert.equal(f.writes(), 2);
      assert.deepEqual(await f.state(), committed);
    });

    for (const denial of ['input grant', 'input application scope', 'inherited marking'] as const) {
      await t.test(`nonempty lineage rejects missing ${denial} without publication`, async () => {
        const f = await setupLineage();
        const outputOperations = f.operations.filter(
          (operation) => operation !== `${f.input}:read`,
        );
        let service = f.service;
        if (denial === 'input grant') {
          await authority.setGrants(f.environment, 'admin', 'engine', 'app', outputOperations);
        } else if (denial === 'input application scope') {
          await authority.setApplication(f.environment, 'admin', 'app', outputOperations);
        } else {
          // Only the second output drops the input label; the principal holds both labels.
          service = new CatalogStorage(
            authority,
            f.bindings.map((binding) =>
              binding.dataset === f.datasets[1] ? { ...binding, markings: ['protected'] } : binding,
            ),
          );
        }
        const before = await f.state();
        await assert.rejects(service.publish(f.auth, f.request()), {
          code: denial === 'inherited marking' ? 'AUTHORITY_DENIED' : 'FORBIDDEN',
        });
        assert.equal(f.writes(), 0);
        assert.deepEqual(await f.state(), before);
        await f.invisible();
      });
    }

    for (const failure of [
      'upload',
      'uncertain',
      'cancel',
      'revoke',
      'epoch',
      'expired',
      'replaced',
      'head',
      'checkpoint',
    ] as const) {
      await t.test(`${failure} cannot publish a partial candidate`, async () => {
        const f = await setup();
        const abort = new AbortController();
        f.after(async (index) => {
          if (index !== 0) {
            return;
          }
          await f.invisible();
          if (failure === 'uncertain') {
            throw new StorageError('IO_ERROR', 'uncertain');
          }
          if (failure === 'cancel') {
            abort.abort();
          }
          if (failure === 'expired' || failure === 'replaced') {
            await pool.query(
              "UPDATE moria.leases SET expires_at=clock_timestamp()-interval '1 second' WHERE environment=$1",
              [f.environment],
            );
            if (failure === 'replaced')
              await catalog.claim(f.environment, f.execution, 'replacement');
          }
          if (failure === 'head')
            await pool.query(
              'UPDATE moria.dataset_heads SET revision=1 WHERE environment=$1 AND dataset=$2',
              [f.environment, f.datasets[1]],
            );
          if (failure === 'checkpoint')
            await pool.query(
              "INSERT INTO moria.checkpoints VALUES($1,'source','rows','1',1,'null'::jsonb)",
              [f.environment],
            );
        });
        f.before(async (index) => {
          if (index === 1 && failure === 'upload') {
            throw new StorageError('IO_ERROR');
          }
        });
        if (failure === 'revoke' || failure === 'epoch') {
          // Last acknowledged upload is a deterministic barrier before final publication.
          f.after(async (index) => {
            if (index !== 1) {
              return;
            }
            if (failure === 'revoke') {
              await sessions.revoke(f.auth, 'admin');
            } else {
              await authority.setGrants(f.environment, 'admin', 'engine', 'app', []);
            }
          });
        }
        const codes = {
          upload: 'IO_ERROR',
          uncertain: 'IO_ERROR',
          cancel: 'CANCELLED',
          revoke: 'AUTHORIZATION_CHANGED',
          epoch: 'AUTHORIZATION_CHANGED',
          expired: 'LEASE_LOST',
          replaced: 'LEASE_LOST',
          head: 'HEAD_CONFLICT',
          checkpoint: 'CHECKPOINT_CONFLICT',
        };
        await assert.rejects(f.service.publish(f.auth, f.request(), abort.signal), {
          code: codes[failure],
        });
        assert.equal(
          (
            await pool.query('SELECT 1 FROM moria.publications WHERE environment=$1', [
              f.environment,
            ])
          ).rowCount,
          0,
        );
        assert.equal(
          (
            await pool.query('SELECT 1 FROM moria.snapshot_history WHERE environment=$1', [
              f.environment,
            ])
          ).rowCount,
          0,
        );
        assert.deepEqual(
          (await catalog.heads(f.environment, f.datasets)).map((head) => head.revision),
          failure === 'head' ? ['0', '1'] : ['0', '0'],
        );
        const checkpoints = await pool.query(
          'SELECT version,value FROM moria.checkpoints WHERE environment=$1',
          [f.environment],
        );
        assert.deepEqual(
          checkpoints.rows,
          failure === 'checkpoint' ? [{ version: '1', value: null }] : [],
        );
        if (failure !== 'revoke' && failure !== 'epoch') {
          for (const dataset of f.datasets)
            await assert.rejects(f.service.read(f.auth, dataset), { code: 'NOT_FOUND' });
        }
        if (failure === 'uncertain' || failure === 'upload') {
          f.after(async () => {});
          f.before(async () => {});
          await assert.rejects(f.service.publish(f.auth, f.request()), { code: 'ALREADY_EXISTS' });
          await f.invisible();
        }
      });
    }

    await t.test('session expiry is checked inside the final publication transaction', async () => {
      const f = await setup();
      const original = catalog.publish.bind(catalog);
      catalog.publish = async (request, checkCurrent) => {
        await pool.query(
          "UPDATE moria.workload_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token_hash=$1",
          [f.auth.sessionHash],
        );
        return original(request, checkCurrent);
      };
      try {
        await assert.rejects(f.service.publish(f.auth, f.request()), { code: 'UNAUTHENTICATED' });
        assert.equal(f.writes(), 2);
        assert.deepEqual(
          (await catalog.heads(f.environment, f.datasets)).map((head) => head.revision),
          ['0', '0'],
        );
        assert.equal(
          (
            await pool.query('SELECT 1 FROM moria.publications WHERE environment=$1', [
              f.environment,
            ])
          ).rowCount,
          0,
        );
      } finally {
        catalog.publish = original;
      }
    });

    await t.test('cancellation interrupts a stalled metadata source', async () => {
      const f = await setup();
      const abort = new AbortController();
      let pulled!: () => void;
      const barrier = new Promise<void>((resolve) => {
        pulled = resolve;
      });
      let cancelled = false;
      const request = f.request();
      request.outputs[0].stream = new ReadableStream<Uint8Array>(
        {
          pull() {
            pulled();
          },
          cancel() {
            cancelled = true;
          },
        },
        { highWaterMark: 0 },
      );
      const publishing = f.service.publish(f.auth, request, abort.signal);
      await barrier;
      abort.abort();
      await assert.rejects(publishing, { code: 'CANCELLED' });
      assert.equal(cancelled, true);
      assert.equal(f.writes(), 0);
      await f.invisible();
    });

    await t.test('competing publication wins atomically', async () => {
      const f = await setup();
      let winner: Awaited<ReturnType<CatalogStorage['publish']>> | undefined;
      f.after(async (index) => {
        if (index !== 1) {
          return;
        }
        f.after(async () => {});
        const execution = randomUUID();
        const fence = await catalog.claim(f.environment, execution, 'worker');
        winner = await f.service.publish(f.auth, { ...f.request(), execution, fence });
      });
      await assert.rejects(f.service.publish(f.auth, f.request()), { code: 'HEAD_CONFLICT' });
      assert.deepEqual(
        winner?.outputs.map((output) => output.revision),
        ['1', '1'],
      );
      assert.equal(
        (await pool.query('SELECT 1 FROM moria.publications WHERE environment=$1', [f.environment]))
          .rowCount,
        1,
      );
    });

    await t.test(
      'reject invalid metadata, table, snapshot, sizes and caller location',
      async () => {
        const f = await setup();
        for (const content of [Buffer.from('arbitrary bytes'), Buffer.from('{}')]) {
          const request = f.request();
          request.outputs[0].stream = metadataStream(content);
          request.outputs[0].size = content.length;
          await assert.rejects(f.service.publish(f.auth, request), { code: 'INVALID_INPUT' });
        }
        const snapshot = f.request();
        snapshot.outputs[0].snapshotId = '1';
        await assert.rejects(f.service.publish(f.auth, snapshot), { code: 'INVALID_INPUT' });
        const wrongTable = new CatalogStorage(
          authority,
          f.configurations.map((binding) => ({ ...binding, icebergTableUuid: randomUUID() })),
        );
        await assert.rejects(wrongTable.publish(f.auth, f.request()), { code: 'INVALID_INPUT' });
        for (const size of [bytes.length - 1, bytes.length + 1, 1024 * 1024 + 1]) {
          const request = f.request();
          request.outputs[0].size = size;
          await assert.rejects(f.service.publish(f.auth, request), { code: 'INVALID_INPUT' });
        }
        await assert.rejects(
          f.service.publish(f.auth, { ...f.request(), path: '/untrusted' } as MetadataPublication),
          { code: 'INVALID_INPUT' },
        );
        assert.equal(f.writes(), 0);
        await f.invisible();
      },
    );

    await t.test(
      'write grants, application scope, markings, session and exact Dataset are required',
      async () => {
        const f = await setup();
        await assert.rejects(
          f.service.publish({ ...f.auth, sessionHash: undefined }, f.request()),
          { code: 'UNAUTHENTICATED' },
        );
        const view = f.request();
        view.outputs[0].dataset = 'test/RestrictedView/a';
        await assert.rejects(f.service.publish(f.auth, view), { code: 'AUTHORITY_DENIED' });
        const lineage = f.request();
        lineage.lineage['test/RestrictedView/a'] = '1';
        await assert.rejects(f.service.publish(f.auth, lineage), { code: 'AUTHORITY_DENIED' });
        const marked = new CatalogStorage(
          authority,
          f.configurations.map((binding) => ({ ...binding, markings: ['unheld'] })),
        );
        await assert.rejects(marked.publish(f.auth, f.request()), { code: 'FORBIDDEN' });
        await authority.setGrants(f.environment, 'admin', 'engine', 'app', []);
        await assert.rejects(f.service.publish(f.auth, f.request()), { code: 'FORBIDDEN' });
        await authority.setGrants(
          f.environment,
          'admin',
          'engine',
          'app',
          f.datasets.map((dataset) => `${dataset}:write`),
        );
        await authority.setApplication(f.environment, 'admin', 'app', []);
        await assert.rejects(f.service.publish(f.auth, f.request()), { code: 'FORBIDDEN' });
        assert.equal(f.writes(), 0);
      },
    );
  });
}
