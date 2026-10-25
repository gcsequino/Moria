import assert from 'node:assert/strict';
import test from 'node:test';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdir, readFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { CatalogStorage, type CatalogStorageBinding } from '../services/catalog/src/storage.js';
import { metadataStream } from '../services/catalog/src/storage-publication.js';
import type { SnapshotPublication } from '../services/catalog/src/snapshot-publication.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import {
  createAuthorizedLocalStorage,
  createAuthorizedS3Storage,
} from '../services/storage/src/authority.js';
import { StorageError } from '../services/storage/src/locations.js';
import { database } from './fixtures/catalog-storage/database.js';
import { startS3 } from './fixtures/storage-s3/server.js';

for (const provider of ['local', 's3'] as const) {
  test(`${provider}: complete scoped Iceberg publication`, { timeout: 300000 }, async (t) => {
    const db = await database();
    t.after(db.close);
    const server = provider === 's3' ? await startS3() : undefined;
    if (server) t.after(server.close);
    const { catalog, pool } = db;
    const authority = new Authority(catalog);
    const sessions = new WorkloadSessions(authority);
    const fixtures: {
      metadata: string;
      snapshotId: string;
      tableUuid: string;
      bytes: Buffer;
      objects: { name: string; bytes: Buffer }[];
    }[] = [];
    for (let index = 0; index < 2; index++) {
      const root = join(db.root, `fixture${index}`);
      await mkdir(root);
      const { stdout } = await promisify(execFile)(resolve('.venv/bin/python'), [
        'tests/fixtures/iceberg-scoped-storage/generate.py',
        root,
        '--snapshot',
        `moria://warehouse/table${index}`,
      ]);
      const generated = JSON.parse(stdout) as {
        metadata: string;
        snapshotId: string;
        tableUuid: string;
        objects: Record<string, string>;
      };
      fixtures.push({
        ...generated,
        bytes: await readFile(generated.metadata),
        objects: await Promise.all(
          Object.entries(generated.objects).map(async ([name, path]) => ({
            name,
            bytes: await readFile(path),
          })),
        ),
      });
    }
    const root = join(db.root, 'storage');
    await mkdir(root);
    async function setup() {
      const environment = randomUUID();
      const datasets = ['test/Dataset/a', 'test/Dataset/b'];
      await catalog.ensureEnvironment(environment);
      for (const dataset of datasets) await catalog.ensureDataset(environment, dataset);
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
      const fence = await catalog.claim(environment, execution, 'worker', 300);
      let writes = 0;
      let beforeWrite = async (_count: number) => {};
      let afterWrite = async (_count: number) => {};
      let partial = false;
      const bindings: CatalogStorageBinding[] = datasets.map((dataset, index) => ({
        environment,
        dataset,
        kind: 'Dataset',
        markings: ['protected'],
        warehouseId: 'warehouse',
        tableId: `table${index}`,
        icebergTableUuid: fixtures[index].tableUuid,
        storage: async (scope, current) => {
          const config = {
            root,
            warehouseId: 'warehouse',
            tableId: `table${index}`,
            maxObjectBytes: 16 * 1024 * 1024,
            chunkBytes: 256,
            endpoint: server?.endpoint ?? '',
            bucket: server?.bucket ?? '',
            credentials: server?.credentials ?? { accessKeyId: '', secretAccessKey: '' },
            prefix: 'snapshots',
            region: 'us-east-1',
          };
          const storage =
            provider === 'local'
              ? await createAuthorizedLocalStorage(config, scope, current)
              : await createAuthorizedS3Storage(config, scope, current);
          return {
            ...storage,
            write: async (logical, source, options) => {
              assert.deepEqual(scope.operations, ['create']);
              await beforeWrite(writes);
              if (partial) {
                let sent = false;
                source = new ReadableStream({
                  pull(controller) {
                    if (sent) controller.error(new Error('synthetic interrupted intake'));
                    else {
                      sent = true;
                      controller.enqueue(new Uint8Array(16));
                    }
                  },
                });
              }
              await storage.write(logical, source, options);
              writes++;
              await afterWrite(writes);
            },
          };
        },
      }));
      const service = new CatalogStorage(authority, bindings);
      const request = (): SnapshotPublication => ({
        execution,
        worker: 'worker',
        fence,
        outputs: datasets.map((dataset, index) => ({
          dataset,
          expectedRevision: '0',
          snapshotId: fixtures[index].snapshotId,
          size: fixtures[index].bytes.length,
          stream: metadataStream(fixtures[index].bytes),
          objects: fixtures[index].objects.map(({ name, bytes }) => ({
            name,
            size: bytes.length,
            stream: metadataStream(bytes),
          })),
        })),
        lineage: {},
        checkpoint: {
          connection: 'synthetic',
          stream: 'rows',
          configRevision: '1',
          expectedVersion: '0',
          value: { offset: '300' },
        },
      });
      const state = async () => {
        const result: Record<string, unknown[]> = {};
        for (const table of ['dataset_heads', 'snapshot_history', 'checkpoints', 'publications']) {
          result[table] = (
            await pool.query(
              `SELECT * FROM moria.${table} AS state WHERE environment=$1 ORDER BY to_jsonb(state)::text`,
              [environment],
            )
          ).rows;
        }
        return result;
      };
      return {
        environment,
        datasets,
        auth,
        execution,
        service,
        bindings,
        request,
        state,
        writes: () => writes,
        before: (callback: typeof beforeWrite) => {
          beforeWrite = callback;
        },
        after: (callback: typeof afterWrite) => {
          afterWrite = callback;
        },
        partial: () => {
          partial = true;
        },
      };
    }

    await t.test(
      'real multi-file rows, provenance, atomic output group, and lost acknowledgment',
      async () => {
        const f = await setup();
        const before = await f.state();
        f.after(async () => {
          assert.deepEqual(await f.state(), before);
        });
        const result = await f.service.publishSnapshot(f.auth, f.request());
        assert.deepEqual(
          result.receipt.outputs.map((output) => output.revision),
          ['1', '1'],
        );
        assert.equal(
          f.writes(),
          fixtures.reduce((count, fixture) => count + fixture.objects.length + 1, 0),
        );
        const reader = new CatalogStorage(
          authority,
          f.bindings.map((binding, index) => ({
            ...binding,
            readObjects: result.outputs[index].readObjects,
          })),
        );
        for (const [index, dataset] of f.datasets.entries()) {
          const scan = await reader.readRows(f.auth, dataset);
          assert.equal(scan.environment, f.environment);
          assert.equal(scan.dataset, dataset);
          assert.equal(scan.revision, '1');
          assert.equal(scan.snapshotId, fixtures[index].snapshotId);
          assert.equal(scan.execution, f.execution);
          const rows = [];
          for await (const row of scan.rows) rows.push(row);
          rows.sort((a, b) => Number(a.id) - Number(b.id));
          assert.deepEqual(
            rows,
            Array.from({ length: 300 }, (_, i) => ({
              id: String(i),
              value: `synthetic-${i}`,
              enabled: i % 2 === 0,
              large: '9007199254740993',
              amount: '1234567890123456.1234',
              day: '2026-01-02',
              at: '2026-01-02T03:04:05.123456Z',
              ratio: 1.25,
              optional: null,
              tags: ['one', 'two'],
            })),
          );
        }
        const committed = await f.state();
        await pool.query(
          "UPDATE moria.leases SET expires_at=clock_timestamp()-interval '1 second' WHERE environment=$1",
          [f.environment],
        );
        assert.deepEqual(await f.service.publishSnapshot(f.auth, f.request()), result);
        assert.deepEqual(await f.state(), committed);
        const conflict = f.request();
        conflict.outputs[0].expectedRevision = '1';
        await assert.rejects(f.service.publishSnapshot(f.auth, conflict), {
          code: 'PUBLICATION_CONFLICT',
        });
        const changed = f.request();
        const objectIndex = fixtures[0].objects.findIndex(({ name }) =>
          name.endsWith('.metadata.json'),
        );
        assert.ok(objectIndex >= 0);
        const bytes = Buffer.concat([fixtures[0].objects[objectIndex].bytes, Buffer.from('\n')]);
        const objects = [...changed.outputs[0].objects];
        objects[objectIndex] = {
          ...objects[objectIndex],
          size: bytes.length,
          stream: metadataStream(bytes),
        };
        const changedRequest = {
          ...changed,
          outputs: changed.outputs.map((output, index) =>
            index === 0 ? { ...output, objects } : output,
          ),
        };
        await assert.rejects(f.service.publishSnapshot(f.auth, changedRequest), {
          code: 'PUBLICATION_CONFLICT',
        });
        assert.deepEqual(await f.state(), committed);
      },
    );

    for (const fault of [
      'missing sort orders',
      'empty sort orders',
      'incompatible current schema',
    ] as const) {
      await t.test(`${fault} is rejected before staging`, async () => {
        const f = await setup();
        const before = await f.state();
        const request = f.request();
        const original = fixtures[0].bytes.toString();
        let changed: string;
        if (fault === 'incompatible current schema') {
          const schema = JSON.parse(original).schemas[0];
          schema['schema-id'] = 1;
          const field = schema.fields.find((field: { name: string }) => field.name === 'value');
          assert.ok(field);
          field.name = 'renamed';
          // Mutate only the schema declaration; retain exact 64-bit snapshot IDs
          // and the selected snapshot's explicit schema-id=0.
          changed = original
            .replace('"schemas":[', `"schemas":[${JSON.stringify(schema)},`)
            .replace('"current-schema-id":0', '"current-schema-id":1');
        } else {
          changed = original.replace(
            '"sort-orders":[{"order-id":0,"fields":[]}],',
            fault === 'missing sort orders' ? '' : '"sort-orders":[],',
          );
        }
        assert.notEqual(changed, original);
        const bytes = Buffer.from(changed);
        await assert.rejects(
          f.service.publishSnapshot(f.auth, {
            ...request,
            outputs: [
              { ...request.outputs[0], size: bytes.length, stream: metadataStream(bytes) },
              request.outputs[1],
            ],
          }),
          { code: 'INVALID_INPUT' },
        );
        assert.equal(f.writes(), 0);
        assert.deepEqual(await f.state(), before);
      });
    }

    for (const fault of [
      'conflicting historical schema',
      'root fallback schema',
      'unresolved root snapshot schema',
      'unresolved retained snapshot schema',
      'duplicate root schema',
      'duplicate retained schema',
    ] as const) {
      await t.test(`${fault} is rejected before staging`, async () => {
        const f = await setup();
        const before = await f.state();
        const request = f.request();
        // Preserve integer snapshot IDs exactly: JSON.parse would round Iceberg longs.
        const withoutSnapshotSchemas = (bytes: Buffer) =>
          Buffer.from(bytes.toString().replaceAll(/,"schema-id":0(?=})/g, ''));
        let bytes = fixtures[0].bytes;
        let objects = [...request.outputs[0].objects];
        if (fault === 'conflicting historical schema' || fault === 'root fallback schema') {
          if (fault === 'root fallback schema') {
            bytes = withoutSnapshotSchemas(bytes);
            objects = fixtures[0].objects.map(({ name, bytes: original }) => {
              const content = name.endsWith('.metadata.json')
                ? withoutSnapshotSchemas(original)
                : original;
              return { name, size: content.length, stream: metadataStream(content) };
            });
          }
          const retainedName = 'moria://warehouse/table0/metadata/retained-root.metadata.json';
          objects.push({ name: retainedName, size: bytes.length, stream: metadataStream(bytes) });
          let root = bytes
            .toString()
            .replace(
              '"metadata-log":[',
              `"metadata-log":[{"metadata-file":"${retainedName}","timestamp-ms":1},`,
            );
          root = root.replace(
            '"name":"optional","type":"string","required":false',
            '"name":"optional","type":"string","required":true',
          );
          assert.notEqual(root, bytes.toString());
          assert.ok(root.includes('"name":"optional","type":"string","required":true'));
          if (fault === 'root fallback schema') {
            root = root.replace('"schema-id":0', '"schema-id":1');
            root = root.replace('"current-schema-id":0', '"current-schema-id":1');
          }
          bytes = Buffer.from(root);
        } else {
          const mutate = (original: Buffer) => {
            const text = original.toString();
            if (fault.startsWith('unresolved')) {
              const offset = text.lastIndexOf(',"schema-id":0}');
              assert.ok(offset >= 0);
              const changed = `${text.slice(0, offset)},"schema-id":987}${text.slice(offset + ',"schema-id":0}'.length)}`;
              assert.notEqual(changed, text);
              return Buffer.from(changed);
            }
            const schema = JSON.parse(text).schemas[0];
            return Buffer.from(
              text.replace('"schemas":[', `"schemas":[${JSON.stringify(schema)},`),
            );
          };
          if (fault.includes('retained')) {
            const index = fixtures[0].objects.findIndex(
              ({ name, bytes: content }) =>
                name.endsWith('.metadata.json') && content.toString().includes('"snapshot-id":'),
            );
            assert.ok(index >= 0);
            const original = fixtures[0].objects[index].bytes;
            const content = mutate(original);
            if (fault === 'unresolved retained snapshot schema') {
              // Keep repeated snapshot definitions identical, so resolution is the failing invariant.
              const snapshotId = [...original.toString().matchAll(/"snapshot-id":([0-9]+)/g)][0][1];
              const snapshot = new RegExp(`("snapshot-id":${snapshotId},.*?,"schema-id":)0`);
              const changed = bytes
                .toString()
                .replace(snapshot, (_match, prefix: string) => `${prefix}987`);
              assert.notEqual(changed, bytes.toString());
              // Resolve the ID in the root only; the retained file must resolve it independently.
              const rootSchema = { ...JSON.parse(changed).schemas[0], 'schema-id': 987 };
              bytes = Buffer.from(
                changed.replace('"schemas":[', `"schemas":[${JSON.stringify(rootSchema)},`),
              );
            }
            objects[index] = {
              ...objects[index],
              size: content.length,
              stream: metadataStream(content),
            };
          } else {
            bytes = mutate(bytes);
          }
        }
        await assert.rejects(
          f.service.publishSnapshot(f.auth, {
            ...request,
            outputs: [
              { ...request.outputs[0], size: bytes.length, stream: metadataStream(bytes), objects },
              request.outputs[1],
            ],
          }),
          { code: 'INVALID_INPUT' },
        );
        assert.equal(f.writes(), 0);
        assert.deepEqual(await f.state(), before);
      });
    }

    for (const fault of [
      'missing',
      'extra',
      'duplicate',
      'conflicting duplicate',
      'cross-table',
      'physical',
      'url',
      'traversal',
      'malformed manifest',
      'malformed parquet',
      'partitioned',
      'wrong snapshot',
      'dangling snapshot',
      'wrong uuid',
      'sorted',
      'foreign reference',
      'physical reference',
      'url reference',
      'traversal reference',
      'short',
      'oversized',
    ] as const) {
      await t.test(`${fault} is rejected before staging`, async () => {
        const f = await setup();
        const before = await f.state();
        const request = f.request();
        const first = request.outputs[0];
        let objects = [...first.objects];
        let bytes = fixtures[0].bytes;
        if (fault === 'missing') objects.pop();
        if (fault === 'extra')
          objects.push({
            name: 'moria://warehouse/table0/extra',
            size: 1,
            stream: metadataStream(Buffer.from('x')),
          });
        if (fault === 'duplicate' || fault === 'conflicting duplicate')
          objects.push({
            ...objects[0],
            stream: metadataStream(
              fault === 'duplicate' ? fixtures[0].objects[0].bytes : Buffer.alloc(objects[0].size),
            ),
          });
        if (['cross-table', 'physical', 'url', 'traversal'].includes(fault)) {
          const names = {
            'cross-table': 'moria://warehouse/table1/foreign',
            physical: '/tmp/foreign',
            url: 'https://example.invalid/foreign',
            traversal: 'moria://warehouse/table0/../foreign',
          };
          objects[0] = { ...objects[0], name: names[fault as keyof typeof names] };
        }
        if (fault === 'malformed manifest' || fault === 'malformed parquet') {
          const index = objects.findIndex(({ name }) =>
            name.endsWith(fault === 'malformed manifest' ? '.avro' : '.parquet'),
          );
          objects[index] = {
            ...objects[index],
            stream: metadataStream(Buffer.alloc(objects[index].size)),
          };
        }
        if (fault === 'partitioned')
          bytes = Buffer.from(
            bytes
              .toString()
              .replace(
                '"partition-specs":[{"spec-id":0,"fields":[]}]',
                '"partition-specs":[{"spec-id":0,"fields":[{"source-id":1,"field-id":1000,"transform":"identity","name":"id"}]}]',
              ),
          );
        if (fault === 'sorted') {
          bytes = Buffer.from(
            bytes
              .toString()
              .replace(
                '"sort-orders":[{"order-id":0,"fields":[]}]',
                '"sort-orders":[{"order-id":1,"fields":[{"source-id":1,"transform":"identity","direction":"asc","null-order":"nulls-first"}]}]',
              )
              .replace('"default-sort-order-id":0', '"default-sort-order-id":1'),
          );
        }
        const foreignReferences = {
          'foreign reference': 'moria://warehouse/foreign/',
          'physical reference': '/tmp/',
          'url reference': 'https://example.invalid/',
          'traversal reference': 'moria://warehouse/table0/../',
        };
        if (fault in foreignReferences) {
          // Rewrite real metadata's manifest-list/history references, retaining its table identity.
          bytes = Buffer.from(
            bytes
              .toString()
              .replaceAll(
                'moria://warehouse/table0/',
                foreignReferences[fault as keyof typeof foreignReferences],
              ),
          );
        }
        if (fault === 'dangling snapshot') {
          bytes = Buffer.from(
            bytes.toString().replace(/"current-snapshot-id":[0-9]+/, '"current-snapshot-id":1'),
          );
        }
        if (fault === 'wrong uuid')
          bytes = Buffer.from(bytes.toString().replace(fixtures[0].tableUuid, randomUUID()));
        if (fault === 'short') objects[0] = { ...objects[0], size: objects[0].size + 1 };
        if (fault === 'oversized') objects[0] = { ...objects[0], size: 16 * 1024 * 1024 + 1 };
        const output = {
          ...first,
          objects,
          stream: metadataStream(bytes),
          size: bytes.length,
          snapshotId:
            fault === 'wrong snapshot' || fault === 'dangling snapshot' ? '1' : first.snapshotId,
        };
        await assert.rejects(
          f.service.publishSnapshot(f.auth, { ...request, outputs: [output, request.outputs[1]] }),
        );
        assert.equal(f.writes(), 0);
        assert.deepEqual(await f.state(), before);
      });
    }

    for (const fault of [
      'partial',
      'uncertain',
      'cancel',
      'revoke',
      'expiry',
      'grant',
      'stale fence',
      'two-output conflict',
    ] as const) {
      await t.test(`${fault} preserves publication state`, async () => {
        const f = await setup();
        const abort = new AbortController();
        let expected = await f.state();
        if (fault === 'partial') f.partial();
        f.after(async (count) => {
          if (count !== 1) return;
          assert.deepEqual(await f.state(), expected);
          if (fault === 'uncertain') throw new StorageError('IO_ERROR', 'uncertain');
          if (fault === 'cancel') abort.abort();
          if (fault === 'revoke') await sessions.revoke(f.auth, 'admin');
          if (fault === 'expiry')
            await pool.query(
              "UPDATE moria.workload_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE environment=$1",
              [f.environment],
            );
          if (fault === 'grant')
            await authority.setGrants(f.environment, 'admin', 'engine', 'app', []);
          if (fault === 'stale fence') {
            await pool.query(
              "UPDATE moria.leases SET expires_at=clock_timestamp()-interval '1 second' WHERE environment=$1",
              [f.environment],
            );
            await catalog.claim(f.environment, f.execution, 'replacement');
          }
          if (fault === 'two-output conflict') {
            await pool.query(
              'UPDATE moria.dataset_heads SET revision=1 WHERE environment=$1 AND dataset=$2',
              [f.environment, f.datasets[1]],
            );
            expected = await f.state();
          }
        });
        const codes = {
          partial: 'IO_ERROR',
          uncertain: 'IO_ERROR',
          cancel: 'CANCELLED',
          revoke: 'AUTHORIZATION_CHANGED',
          expiry: 'UNAUTHENTICATED',
          grant: 'AUTHORIZATION_CHANGED',
          'stale fence': 'LEASE_LOST',
          'two-output conflict': 'HEAD_CONFLICT',
        };
        await assert.rejects(f.service.publishSnapshot(f.auth, f.request(), abort.signal), {
          code: codes[fault],
        });
        assert.deepEqual(await f.state(), expected);
        if (fault === 'uncertain') {
          f.after(async () => {});
          await assert.rejects(f.service.publishSnapshot(f.auth, f.request()), {
            code: 'ALREADY_EXISTS',
          });
          assert.deepEqual(await f.state(), expected);
        }
      });
    }

    await t.test('cancellation interrupts stalled engine intake', async () => {
      const f = await setup();
      const request = f.request();
      const controller = new AbortController();
      let started!: () => void;
      const intake = new Promise<void>((done) => {
        started = done;
      });
      let cancelled = false;
      const objects = [...request.outputs[0].objects];
      objects[0] = {
        ...objects[0],
        stream: new ReadableStream(
          {
            pull() {
              started();
              return new Promise(() => {});
            },
            cancel() {
              cancelled = true;
            },
          },
          { highWaterMark: 0 },
        ),
      };
      const before = await f.state();
      const publication = f.service.publishSnapshot(
        f.auth,
        {
          ...request,
          outputs: [{ ...request.outputs[0], objects }, request.outputs[1]],
        },
        controller.signal,
      );
      const rejected = assert.rejects(publication, { code: 'CANCELLED' });
      await intake;
      controller.abort();
      await rejected;
      assert.equal(cancelled, true);
      assert.equal(f.writes(), 0);
      assert.deepEqual(await f.state(), before);
    });
  });
}
