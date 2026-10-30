import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import {
  CatalogStorage,
  encodeMetadataLocation,
  decodeMetadataLocation,
  type CatalogStorageBinding,
} from '../services/catalog/src/storage.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { createLocalStorage } from '../services/storage/src/local.js';
import { createS3Storage } from '../services/storage/src/s3.js';
import {
  createAuthorizedLocalStorage,
  createAuthorizedS3Storage,
} from '../services/storage/src/authority.js';
import { database } from './fixtures/catalog-storage/database.js';
import { startS3 } from './fixtures/storage-s3/server.js';
import { iceberg } from './iceberg-helper.js';
import { FilesystemStore } from '../services/filesystem/src/store.js';
import { FilesystemAuthority } from '../services/security/src/filesystem.js';
import { filesystemRoot } from '../packages/contracts/filesystem.js';

const logical = {
  version: 1 as const,
  warehouseId: 'warehouse',
  tableId: 'table',
  objectId: 'published',
};
const encoded = encodeMetadataLocation(logical);

test('metadata location encoding is strict and versioned', () => {
  assert.deepEqual(decodeMetadataLocation(encoded), logical);
  for (const value of [
    '/warehouse/metadata.json',
    's3://bucket/key',
    encoded.replace('v1:', 'v2:'),
    encoded + ' ',
    encoded.replace('"version":1', '"version":1,"version":1'),
    encoded.replace('"version":1', '"version":2'),
    encoded.replace('"version":1', '"extra":0,"version":1'),
    encoded.replace('published', '../escape'),
    'moria-storage:v1:{',
  ]) {
    assert.throws(() => decodeMetadataLocation(value), { code: 'INVALID_INPUT' });
  }
});

for (const provider of ['local', 's3'] as const) {
  test(
    `${provider}: published catalog metadata with real authority and storage`,
    { timeout: 120000 },
    async (t) => {
      const db = await database();
      t.after(db.close);
      const server = provider === 's3' ? await startS3() : undefined;
      if (server) t.after(server.close);
      const { catalog, pool } = db;
      const authority = new Authority(catalog);
      const sessions = new WorkloadSessions(authority);
      const environment = 'metadata-test';
      const dataset = 'test/Dataset/data';
      await catalog.ensureEnvironment(environment);
      await catalog.ensureDataset(environment, dataset);
      await authority.syncPrincipal(environment, 'admin', 'reader', {
        enabled: true,
        kind: 'workload',
        attributes: {},
        markings: ['protected'],
      });
      await authority.setApplication(environment, 'admin', 'app', [`${dataset}:read`]);
      await authority.setGrants(environment, 'admin', 'reader', 'app', [`${dataset}:read`]);
      const session = async () => {
        const minted = await sessions.mint(
          { environment, principal: 'reader', application: 'app' },
          'admin',
        );
        return sessions.authenticate(`Bearer ${minted.token}`);
      };
      const binding = {
        root: join(db.root, 'storage'),
        ...logical,
        maxObjectBytes: 1024 * 1024,
        chunkBytes: 256,
        endpoint: server?.endpoint ?? '',
        credentials: server?.credentials ?? { accessKeyId: '', secretAccessKey: '' },
        bucket: server?.bucket ?? '',
        prefix: 'catalog',
        region: 'us-east-1',
      };
      await mkdir(binding.root, { recursive: true });
      const raw =
        provider === 'local' ? await createLocalStorage(binding) : await createS3Storage(binding);
      let factories = 0;
      const configuration: CatalogStorageBinding = {
        environment,
        dataset,
        kind: 'Dataset',
        markings: ['protected'],
        warehouseId: logical.warehouseId,
        tableId: logical.tableId,
        storage: async (scope, current) => {
          factories++;
          return provider === 'local'
            ? createAuthorizedLocalStorage(binding, scope, current)
            : createAuthorizedS3Storage(binding, scope, current);
        },
      };
      const service = new CatalogStorage(authority, [configuration]);
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
      const write = async (objectId: string, content = bytes) => {
        let offset = 0;
        await raw.write(
          { ...logical, objectId },
          new ReadableStream<Uint8Array>(
            {
              pull(controller) {
                if (offset === content.length) {
                  controller.close();
                } else {
                  controller.enqueue(content.subarray(offset, offset + 256));
                  offset = Math.min(offset + 256, content.length);
                }
              },
            },
            { highWaterMark: 0 },
          ),
          { size: content.length },
        );
      };
      await write('published');
      await write('orphan', Buffer.from('orphan candidate'));
      let revision = '0';
      const publish = async (metadataLocation = encoded, snapshotId = staged.snapshotId) => {
        const execution = randomUUID();
        const fence = await catalog.claim(environment, execution, 'worker');
        const state = await authority.current(environment, 'reader', 'app');
        const receipt = await catalog.publish({
          environment,
          execution,
          worker: 'worker',
          fence,
          securityEpoch: state.epoch,
          outputs: [{ dataset, expectedRevision: revision, metadataLocation, snapshotId }],
          lineage: {},
        });
        revision = receipt.outputs[0].revision;
        return execution;
      };
      const collect = async (stream: ReadableStream<Uint8Array>) => {
        const chunks: Uint8Array[] = [];
        for await (const chunk of stream) {
          assert.ok(chunk.length <= 256);
          chunks.push(chunk);
        }
        return Buffer.concat(chunks);
      };
      await t.test(
        'unpublished candidates cannot be read and inputs cannot choose objects',
        async () => {
          const auth = await session();
          await assert.rejects(service.read(auth, dataset), { code: 'NOT_FOUND' });
          for (const target of [
            'orphan',
            'test/RestrictedView/data',
            'other/Dataset/data',
            encoded,
          ]) {
            await assert.rejects(service.read(auth, target), { code: 'AUTHORITY_DENIED' });
          }
          assert.equal(factories, 0);
        },
      );
      await t.test(
        'standard Iceberg metadata and provenance; publication does not switch a live stream',
        async () => {
          const execution = await publish();
          const auth = await session();
          const pinned = await service.read(auth, dataset);
          assert.equal(pinned.revision, '1');
          assert.equal(pinned.snapshotId, staged.snapshotId);
          assert.deepEqual(pinned.markings, ['protected']);
          assert.equal(pinned.execution, execution);
          const reader = pinned.stream.getReader();
          const first = (await reader.read()).value!;
          const nextMetadata = await iceberg({
            operation: 'stage',
            warehouse: join(db.root, 'iceberg-next'),
            schema: {
              type: 'struct',
              'schema-id': 0,
              fields: [{ id: 1, name: 'value', required: true, type: 'string' }],
            },
            rows: [{ value: 'next synthetic snapshot' }],
          });
          const nextBytes = await readFile(nextMetadata.metadataLocation);
          assert.notDeepEqual(nextBytes, bytes);
          await write('next', nextBytes);
          const nextExecution = await publish(
            encodeMetadataLocation({ ...logical, objectId: 'next' }),
            nextMetadata.snapshotId,
          );
          const chunks = [first];
          while (true) {
            const item = await reader.read();
            if (item.done) break;
            chunks.push(item.value);
          }
          reader.releaseLock();
          assert.deepEqual(Buffer.concat(chunks), bytes);
          assert.equal(JSON.parse(bytes.toString())['format-version'], 2);
          const next = await service.read(auth, dataset);
          assert.equal(next.revision, '2');
          assert.equal(next.snapshotId, nextMetadata.snapshotId);
          assert.equal(next.execution, nextExecution);
          assert.deepEqual(await collect(next.stream), nextBytes);
          await publish();
        },
      );
      await t.test(
        'racing reads and publications always return a matching history pair',
        async () => {
          const auth = await session();
          for (let index = 0; index < 5; index++) {
            const [read] = await Promise.all([
              service.read(auth, dataset),
              publish(encoded, String(1000 + index)),
            ]);
            const history = (
              await pool.query(
                'SELECT * FROM moria.snapshot_history WHERE environment=$1 AND dataset=$2 AND revision=$3',
                [environment, dataset, read.revision],
              )
            ).rows[0];
            assert.equal(read.snapshotId, history.snapshot_id);
            assert.equal(read.execution, history.execution);
            assert.deepEqual(await collect(read.stream), bytes);
          }
        },
      );
      await t.test(
        'session, environment, application and marking denials precede storage',
        async () => {
          const auth = await session();
          const count = factories;
          await assert.rejects(service.read({ ...auth, sessionHash: undefined }, dataset), {
            code: 'UNAUTHENTICATED',
          });
          await assert.rejects(service.read({ ...auth, environment: 'other' }, dataset), {
            code: 'AUTHORITY_DENIED',
          });
          await assert.rejects(service.read({ ...auth, application: 'other' }, dataset));
          const marked = new CatalogStorage(authority, [
            { ...configuration, markings: ['unheld'] },
          ]);
          await assert.rejects(marked.read(auth, dataset), { code: 'FORBIDDEN' });
          assert.equal(factories, count);
          assert.throws(
            () =>
              new CatalogStorage(authority, [
                { ...configuration, kind: 'RestrictedView' } as unknown as CatalogStorageBinding,
              ]),
            { code: 'INVALID_INPUT' },
          );
        },
      );
      await t.test(
        'authenticated sessions cannot be rebound to another granted application',
        async () => {
          const auth = await session();
          await authority.setApplication(environment, 'admin', 'other', [`${dataset}:read`]);
          await authority.setGrants(environment, 'admin', 'reader', 'other', [`${dataset}:read`]);
          const count = factories;
          await assert.rejects(service.read({ ...auth, application: 'other' }, dataset), {
            code: 'UNAUTHENTICATED',
          });
          assert.equal(factories, count);
        },
      );
      await t.test(
        'resolved adapter scope denies other objects and create operations',
        async () => {
          const scoped = new CatalogStorage(authority, [
            {
              ...configuration,
              storage: async (scope, current) => {
                assert.deepEqual(scope.operations, ['read']);
                const storage = await configuration.storage(scope, current);
                await assert.rejects(storage.read({ ...scope.location, objectId: 'orphan' }), {
                  code: 'AUTHORITY_DENIED',
                });
                await assert.rejects(storage.read({ ...scope.location, tableId: 'other' }), {
                  code: 'BINDING_MISMATCH',
                });
                const source = new ReadableStream<Uint8Array>();
                await assert.rejects(storage.write(scope.location, source, { size: 0 }), {
                  code: 'AUTHORITY_DENIED',
                });
                assert.equal(source.locked, false);
                return storage;
              },
            },
          ]);
          assert.deepEqual(
            await collect((await scoped.read(await session(), dataset)).stream),
            bytes,
          );
        },
      );
      await t.test(
        'failed publication leaves an orphan unreachable and history mismatches fail closed',
        async () => {
          const execution = randomUUID();
          const fence = await catalog.claim(environment, execution, 'worker');
          const state = await authority.current(environment, 'reader', 'app');
          await assert.rejects(
            catalog.publish({
              environment,
              execution,
              worker: 'worker',
              fence,
              securityEpoch: state.epoch,
              outputs: [
                {
                  dataset,
                  expectedRevision: '0',
                  snapshotId: 'orphan',
                  metadataLocation: encodeMetadataLocation({ ...logical, objectId: 'orphan' }),
                },
              ],
              lineage: {},
            }),
            { code: 'HEAD_CONFLICT' },
          );
          const auth = await session();
          assert.deepEqual(await collect((await service.read(auth, dataset)).stream), bytes);
          await pool.query(
            'UPDATE moria.snapshot_history SET metadata_location=$4 WHERE environment=$1 AND dataset=$2 AND revision=$3',
            [environment, dataset, revision, 'invalid'],
          );
          try {
            const count = factories;
            await assert.rejects(service.read(auth, dataset), { code: 'NOT_FOUND' });
            assert.equal(factories, count);
          } finally {
            await pool.query(
              'UPDATE moria.snapshot_history SET metadata_location=$4 WHERE environment=$1 AND dataset=$2 AND revision=$3',
              [environment, dataset, revision, encoded],
            );
          }
        },
      );
      for (const change of ['revoke', 'expire', 'grants', 'application', 'markings'] as const) {
        await t.test(`${change} stops existing streams and denies new reads`, async () => {
          const auth = await session();
          const reader = (await service.read(auth, dataset)).stream.getReader();
          assert.equal((await reader.read()).done, false);
          if (change === 'revoke') await sessions.revoke(auth, 'admin');
          if (change === 'expire')
            await pool.query(
              "UPDATE moria.workload_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token_hash=$1",
              [auth.sessionHash],
            );
          if (change === 'grants')
            await authority.setGrants(environment, 'admin', 'reader', 'app', []);
          if (change === 'application')
            await authority.setApplication(environment, 'admin', 'app', [], false);
          if (change === 'markings')
            await authority.syncPrincipal(environment, 'admin', 'reader', {
              enabled: true,
              kind: 'workload',
              attributes: {},
              markings: [],
            });
          await assert.rejects(reader.read(), { code: 'AUTHORITY_DENIED' });
          reader.releaseLock();
          await assert.rejects(service.read(auth, dataset));
          await authority.setGrants(environment, 'admin', 'reader', 'app', [`${dataset}:read`]);
          await authority.setApplication(environment, 'admin', 'app', [`${dataset}:read`]);
          await authority.syncPrincipal(environment, 'admin', 'reader', {
            enabled: true,
            kind: 'workload',
            attributes: {},
            markings: ['protected'],
          });
        });
      }
      await t.test(
        'bad published references fail before factory; missing objects and cancellation cleanly fail',
        async () => {
          const auth = await session();
          for (const reference of [
            '/physical/legacy.json',
            encoded.replace('v1:', 'v9:'),
            encodeMetadataLocation({ ...logical, warehouseId: 'other' }),
            encodeMetadataLocation({ ...logical, tableId: 'other' }),
          ]) {
            await publish(reference);
            const count = factories;
            await assert.rejects(service.read(auth, dataset));
            assert.equal(factories, count);
          }
          await publish(encodeMetadataLocation({ ...logical, objectId: 'missing' }));
          await assert.rejects(service.read(auth, dataset), { code: 'NOT_FOUND' });
          await publish();
          const abort = new AbortController();
          const reading = (await service.read(auth, dataset, abort.signal)).stream.getReader();
          await reading.read();
          abort.abort();
          await assert.rejects(reading.read(), { code: 'CANCELLED' });
          reading.releaseLock();
          const cancelled = (await service.read(auth, dataset)).stream.getReader();
          await cancelled.read();
          await cancelled.cancel();
          assert.equal((await cancelled.read()).done, true);
          cancelled.releaseLock();
          assert.deepEqual(await collect((await service.read(auth, dataset)).stream), bytes);
        },
      );
      await t.test(
        'filesystem classifications are returned and changes abort even a cleared reader mid-stream',
        async () => {
          const security = new FilesystemAuthority(authority),
            store = new FilesystemStore(catalog),
            resourceId = randomUUID();
          const instanceId = await security.createPrincipal('admin', 'workload');
          await security.bindPrincipal('admin', environment, 'reader', instanceId);
          const operations = [`${dataset}:read`, `${resourceId}:read`, 'filesystem:read'];
          await authority.setApplication(environment, 'admin', 'app', operations);
          await authority.setGrants(environment, 'admin', 'reader', 'app', operations);
          await authority.syncPrincipal(environment, 'admin', 'reader', {
            kind: 'workload',
            enabled: true,
            attributes: {},
            markings: ['protected', 'restricted'],
          });
          await security.setPolicy('admin', filesystemRoot, '0', {
            markings: [],
            grants: [
              {
                subject: { kind: 'principal', id: instanceId },
                operations: ['read'],
                inherit: true,
                effect: 'allow',
              },
            ],
          });
          const actor = { environment, principal: 'admin', application: 'app' };
          const parentReceipt = await store.apply(actor, randomUUID(), [
            {
              op: 'create',
              kind: 'directory',
              parentId: filesystemRoot,
              expectedParentRevision: (await store.get(filesystemRoot)).revision,
              name: 'runtime',
              scope: { projectId: randomUUID(), environmentId: environment },
            },
          ]);
          const parent = parentReceipt.entries.find((entry) => entry.name === 'runtime')!;
          const receipt = await store.apply(actor, randomUUID(), [
            {
              op: 'create',
              kind: 'resource',
              parentId: parent.id,
              expectedParentRevision: parent.revision,
              name: 'dataset',
              resourceId,
            },
          ]);
          const entry = receipt.entries.find((item) => item.name === 'dataset')!;
          await security.setPolicy('admin', entry.id, '0', {
            markings: [{ environment, marking: 'protected' }],
            grants: [],
          });
          await pool.query(
            'INSERT INTO moria.dataset_heads SELECT environment,$3,revision,metadata_location,snapshot_id FROM moria.dataset_heads WHERE environment=$1 AND dataset=$2',
            [environment, dataset, resourceId],
          );
          await pool.query(
            'INSERT INTO moria.snapshot_history SELECT environment,$3,revision,metadata_location,snapshot_id,execution,lineage FROM moria.snapshot_history WHERE environment=$1 AND dataset=$2',
            [environment, dataset, resourceId],
          );
          const scoped = new CatalogStorage(authority, [{ ...configuration, dataset: resourceId }]);
          const opened = await scoped.read(await session(), resourceId),
            reader = opened.stream.getReader();
          assert.deepEqual(opened.markings, ['protected']);
          assert.equal((await reader.read()).done, false);
          await security.setPolicy('admin', entry.id, '1', {
            markings: [
              { environment, marking: 'protected' },
              { environment, marking: 'restricted' },
            ],
            grants: [],
          });
          await assert.rejects(reader.read(), { code: 'AUTHORITY_DENIED' });
          reader.releaseLock();
        },
      );
    },
  );
}
