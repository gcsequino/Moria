import assert from 'node:assert/strict';
import test from 'node:test';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdir, readFile, writeFile, unlink } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { PutObjectCommand, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import {
  CatalogStorage,
  decodeMetadataLocation,
  type CatalogStorageBinding,
} from '../services/catalog/src/storage.js';
import { inventoryLocation, inventoryLimit } from '../services/catalog/src/reference-inventory.js';
import { metadataStream } from '../services/catalog/src/storage-publication.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import {
  createAuthorizedLocalStorage,
  createAuthorizedS3Storage,
} from '../services/storage/src/authority.js';
import { StorageError, type StorageLocation } from '../services/storage/src/locations.js';
import { database } from './fixtures/catalog-storage/database.js';
import { startS3 } from './fixtures/storage-s3/server.js';

for (const provider of ['local', 's3'] as const) {
  test(`${provider}: durable scoped reference recovery`, { timeout: 300000 }, async (t) => {
    const db = await database();
    t.after(db.close);
    const server = provider === 's3' ? await startS3() : undefined;
    if (server) t.after(server.close);
    const authority = new Authority(db.catalog);
    const sessions = new WorkloadSessions(authority);
    const fixtureRoot = join(db.root, 'fixture');
    await mkdir(fixtureRoot);
    const generated = JSON.parse(
      (
        await promisify(execFile)(resolve('.venv/bin/python'), [
          'tests/fixtures/iceberg-scoped-storage/generate.py',
          fixtureRoot,
          '--snapshot',
          'moria://warehouse/table',
        ])
      ).stdout,
    ) as {
      metadata: string;
      snapshotId: string;
      tableUuid: string;
      objects: Record<string, string>;
    };
    const metadata = await readFile(generated.metadata);
    const objects = await Promise.all(
      Object.entries(generated.objects).map(async ([name, path]) => ({
        name,
        bytes: await readFile(path),
      })),
    );
    const root = join(db.root, 'storage');
    await mkdir(root);
    const config = {
      root,
      warehouseId: 'warehouse',
      tableId: 'table',
      maxObjectBytes: 16 * 1024 * 1024,
      chunkBytes: 256 * 1024,
      endpoint: server?.endpoint ?? '',
      bucket: server?.bucket ?? '',
      credentials: server?.credentials ?? { accessKeyId: '', secretAccessKey: '' },
      prefix: 'snapshots',
      region: 'us-east-1',
    };
    const key = (logical: StorageLocation) =>
      `snapshots/warehouse/table/objects/${logical.objectId}`;
    const path = (logical: StorageLocation) =>
      join(root, 'warehouse', 'table', 'objects', logical.objectId);
    // Fault injection changes only disposable synthetic provider state, outside the service.
    const get = async (logical: StorageLocation) => {
      if (!server) return readFile(path(logical));
      const response = await server.client.send(
        new GetObjectCommand({ Bucket: server.bucket, Key: key(logical) }),
      );
      return Buffer.from(await response.Body!.transformToByteArray());
    };
    const put = async (logical: StorageLocation, bytes?: Buffer) => {
      if (server) {
        if (bytes)
          await server.client.send(
            new PutObjectCommand({ Bucket: server.bucket, Key: key(logical), Body: bytes }),
          );
        else
          await server.client.send(
            new DeleteObjectCommand({ Bucket: server.bucket, Key: key(logical) }),
          );
      } else if (bytes) await writeFile(path(logical), bytes);
      else await unlink(path(logical));
    };
    async function setup() {
      const environment = randomUUID();
      const dataset = 'test/Dataset/a';
      await db.catalog.ensureEnvironment(environment);
      await db.catalog.ensureDataset(environment, dataset);
      await authority.syncPrincipal(environment, 'admin', 'engine', {
        enabled: true,
        kind: 'workload',
        attributes: {},
        markings: ['protected'],
      });
      const operations = [`${dataset}:read`, `${dataset}:write`];
      await authority.setApplication(environment, 'admin', 'app', operations);
      await authority.setGrants(environment, 'admin', 'engine', 'app', operations);
      const minted = await sessions.mint(
        { environment, principal: 'engine', application: 'app' },
        'admin',
      );
      const auth = await sessions.authenticate(`Bearer ${minted.token}`);
      const execution = randomUUID();
      const fence = await db.catalog.claim(environment, execution, 'worker', 300);
      const plain = {
        environment,
        dataset,
        kind: 'Dataset' as const,
        markings: ['protected'],
        warehouseId: 'warehouse',
        tableId: 'table',
        icebergTableUuid: generated.tableUuid,
      };
      let hook = async (_logical: StorageLocation, _phase: 'before' | 'after') => {};
      let partialInventory = false;
      let writes = 0;
      const binding: CatalogStorageBinding = {
        ...plain,
        storage: async (scope, current) => {
          const storage =
            provider === 'local'
              ? await createAuthorizedLocalStorage(config, scope, current)
              : await createAuthorizedS3Storage(config, scope, current);
          return {
            ...storage,
            read: async (logical, options) => {
              assert.deepEqual(scope.operations, ['read']);
              assert.deepEqual(scope.location, logical);
              return storage.read(logical, options);
            },
            write: async (logical, source, options) => {
              assert.deepEqual(scope.operations, ['create']);
              assert.deepEqual(scope.location, logical);
              await hook(logical, 'before');
              if (++writes === objects.length + 2 && partialInventory) {
                await source.cancel();
                let sent = false;
                source = new ReadableStream({
                  pull(controller) {
                    if (sent) controller.error(new Error('synthetic interrupted inventory'));
                    else {
                      sent = true;
                      controller.enqueue(Buffer.from('['));
                    }
                  },
                });
              }
              await storage.write(logical, source, options);
              await hook(logical, 'after');
            },
          };
        },
      };
      const declaration = {
        execution,
        worker: 'worker',
        fence,
        outputs: [{ dataset, expectedRevision: '0', snapshotId: generated.snapshotId }],
        lineage: {},
        checkpoint: {
          connection: 'synthetic',
          stream: 'rows',
          configRevision: '1',
          expectedVersion: '0',
          value: { offset: '300' },
        },
      };
      const request = () => ({
        ...declaration,
        outputs: declaration.outputs.map((output) => ({
          ...output,
          size: metadata.length,
          stream: metadataStream(metadata),
          objects: objects.map(({ name, bytes }) => ({
            name,
            size: bytes.length,
            stream: metadataStream(bytes),
          })),
        })),
      });
      const fresh = () => new CatalogStorage(authority, [binding]);
      const pin = async () =>
        decodeMetadataLocation(
          (
            await db.pool.query(
              'SELECT metadata_location FROM moria.dataset_heads WHERE environment=$1 AND dataset=$2',
              [environment, dataset],
            )
          ).rows[0].metadata_location,
        );
      const state = async () => {
        const result = [];
        for (const table of ['dataset_heads', 'snapshot_history', 'checkpoints', 'publications'])
          result.push(
            (
              await db.pool.query(
                `SELECT to_jsonb(s) AS value FROM moria.${table} s WHERE environment=$1 ORDER BY to_jsonb(s)::text`,
                [environment],
              )
            ).rows,
          );
        return result;
      };
      const child = async (retry = false) => {
        const { stdout } = await promisify(execFile)(
          process.execPath,
          ['--import', 'tsx', 'tests/fixtures/iceberg-scoped-storage/recovery-child.ts'],
          {
            env: {
              ...process.env,
              MORIA_REFERENCE_RECOVERY_TEST: JSON.stringify({
                database: {
                  host: db.pool.options.host,
                  port: db.pool.options.port,
                  user: db.pool.options.user,
                  password: db.pool.options.password,
                  database: 'postgres',
                },
                provider,
                config,
                binding: plain,
                auth,
                retry: retry ? { ...generated, request: declaration } : undefined,
              }),
            },
            timeout: 60000,
          },
        );
        return JSON.parse(stdout);
      };
      return {
        environment,
        dataset,
        auth,
        execution,
        binding,
        fresh,
        request,
        declaration,
        pin,
        state,
        child,
        hook: (value: typeof hook) => {
          hook = value;
        },
        partial: () => {
          partialInventory = true;
        },
      };
    }
    const rowsOf = async (scan: Awaited<ReturnType<CatalogStorage['readRows']>>) => {
      const rows = [];
      for await (const row of scan.rows) rows.push(row);
      return rows.sort((a, b) => Number(a.id) - Number(b.id));
    };
    const expected = Array.from({ length: 300 }, (_, i) => ({
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
    }));

    await t.test(
      'fresh process reads durable rows/provenance and recovers exact receipt',
      async () => {
        const f = await setup();
        const receipt = (await f.fresh().publishSnapshot(f.auth, f.request())).receipt;
        const result = await f.child();
        assert.deepEqual(
          result.rows.sort((a: { id: string }, b: { id: string }) => Number(a.id) - Number(b.id)),
          expected,
        );
        const { rows: _rows, ...provenance } = result;
        assert.deepEqual(provenance, {
          environment: f.environment,
          dataset: f.dataset,
          revision: '1',
          snapshotId: generated.snapshotId,
          execution: f.execution,
        });
        await db.pool.query(
          "UPDATE moria.leases SET expires_at=clock_timestamp()-interval '1 second' WHERE environment=$1",
          [f.environment],
        );
        const committed = await f.state();
        assert.deepEqual(await f.child(true), receipt);
        assert.deepEqual(await f.state(), committed);
        const changed = f.request();
        changed.outputs[0].expectedRevision = '1';
        await assert.rejects(f.fresh().publishSnapshot(f.auth, changed), {
          code: 'PUBLICATION_CONFLICT',
        });
      },
    );

    await t.test(
      'metadata-only publication remains metadata-readable and cannot invent an inventory',
      async () => {
        const f = await setup();
        const request = f.request();
        await f.fresh().publish(f.auth, {
          ...request,
          outputs: request.outputs.map(({ objects: _objects, ...output }) => output),
        });
        const published = await f.fresh().read(f.auth, f.dataset);
        assert.deepEqual(Buffer.from(await new Response(published.stream).arrayBuffer()), metadata);
        await assert.rejects(f.fresh().readRows(f.auth, f.dataset), { code: 'NOT_FOUND' });
      },
    );

    await t.test(
      'an existing iterator stays pinned across competing publication and fresh reader',
      async () => {
        const f = await setup();
        await f.fresh().publishSnapshot(f.auth, f.request());
        const scan = await f.fresh().readRows(f.auth, f.dataset);
        const old = await f.pin();
        const request = f.request();
        request.execution = randomUUID();
        request.fence = await db.catalog.claim(f.environment, request.execution, 'worker', 300);
        request.outputs[0].expectedRevision = '1';
        request.checkpoint.expectedVersion = '1';
        await f.fresh().publishSnapshot(f.auth, request);
        const latest = await f.pin();
        assert.notDeepEqual(latest, old);
        const latestInventory = await get(inventoryLocation(latest));
        await put(inventoryLocation(latest), await get(inventoryLocation(old)));
        await assert.rejects(f.fresh().readRows(f.auth, f.dataset));
        await put(inventoryLocation(latest), latestInventory);
        const fresh = await f.fresh().readRows(f.auth, f.dataset);
        assert.equal(scan.execution, f.execution);
        assert.equal(fresh.execution, request.execution);
        assert.deepEqual(await rowsOf(scan), expected);
        assert.deepEqual(await rowsOf(fresh), expected);
        assert.ok((await get(inventoryLocation(old))).length > 0);
      },
    );

    await t.test(
      'malformed, swapped, missing and oversized durable content fails before rows',
      async () => {
        const f = await setup();
        await f.fresh().publishSnapshot(f.auth, f.request());
        const logical = await f.pin();
        const inventory = inventoryLocation(logical);
        const original = await get(inventory);
        const parsed = JSON.parse(original.toString());
        const other = await setup();
        await other.fresh().publishSnapshot(other.auth, other.request());
        const mutations: (Buffer | undefined)[] = [
          undefined,
          Buffer.alloc(inventoryLimit + 1, 32),
          Buffer.from(original.toString() + '\n'),
          Buffer.from(JSON.stringify([...parsed, parsed[0]])),
          Buffer.from(
            JSON.stringify(
              parsed.map((entry: object, index: number) =>
                index ? entry : { ...entry, extra: true },
              ),
            ),
          ),
          Buffer.from(original.toString().replace('"name":', '"size":1,"name":')),
          Buffer.from(JSON.stringify([...parsed].reverse())),
          Buffer.from(
            JSON.stringify(
              parsed.map((entry: { size: number }, index: number) =>
                index ? entry : { ...entry, size: entry.size + 1 },
              ),
            ),
          ),
          Buffer.from(
            JSON.stringify(
              parsed.map((entry: { location: object }, index: number) =>
                index ? entry : { ...entry, location: { ...entry.location, extra: 1 } },
              ),
            ),
          ),
          Buffer.from(
            JSON.stringify(
              parsed.map((entry: { name: string }, index: number) =>
                index ? entry : { ...entry, name: 'moria://warehouse/foreign/a' },
              ),
            ),
          ),
          await get(inventoryLocation(await other.pin())),
        ];
        for (const bytes of mutations) {
          await put(inventory, bytes);
          await assert.rejects(f.fresh().readRows(f.auth, f.dataset));
          await put(inventory, original);
        }
        for (const target of [logical, parsed[0].location as StorageLocation]) {
          const bytes = await get(target);
          for (const bad of [undefined, Buffer.alloc(bytes.length), bytes.subarray(1)]) {
            await put(target, bad);
            await assert.rejects(f.fresh().readRows(f.auth, f.dataset));
            await put(target, bytes);
          }
        }
        const wrongUuid = new CatalogStorage(authority, [
          { ...f.binding, icebergTableUuid: randomUUID() },
        ]);
        await assert.rejects(wrongUuid.readRows(f.auth, f.dataset));
        assert.deepEqual(await rowsOf(await f.fresh().readRows(f.auth, f.dataset)), expected);
      },
    );

    for (const fault of ['failed', 'partial', 'uncertain', 'cancelled'] as const) {
      await t.test(`${fault} inventory write cannot advance catalog state`, async () => {
        const f = await setup();
        const before = await f.state();
        const controller = new AbortController();
        let writes = 0;
        if (fault === 'partial') f.partial();
        f.hook(async (_logical, phase) => {
          if (phase === 'before') writes++;
          if (writes !== objects.length + 2) return;
          assert.deepEqual(await f.state(), before);
          if (fault === 'failed' && phase === 'before') throw new StorageError('IO_ERROR');
          if (fault === 'uncertain' && phase === 'after')
            throw new StorageError('IO_ERROR', 'uncertain');
          if (fault === 'cancelled' && phase === 'before') controller.abort();
        });
        await assert.rejects(f.fresh().publishSnapshot(f.auth, f.request(), controller.signal), {
          code: fault === 'cancelled' ? 'CANCELLED' : 'IO_ERROR',
        });
        assert.deepEqual(await f.state(), before);
        f.hook(async () => {});
        await assert.rejects(f.fresh().publishSnapshot(f.auth, f.request()), {
          code: 'ALREADY_EXISTS',
        });
        assert.deepEqual(await f.state(), before);
      });
    }

    for (const fault of ['revoked', 'expired'] as const) {
      await t.test(`${fault} sessions fail recovery and pinned row release`, async () => {
        const f = await setup();
        await f.fresh().publishSnapshot(f.auth, f.request());
        const scan = await f.fresh().readRows(f.auth, f.dataset);
        if (fault === 'revoked') await sessions.revoke(f.auth, 'admin');
        else
          await db.pool.query(
            "UPDATE moria.workload_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE environment=$1",
            [f.environment],
          );
        await assert.rejects(scan.rows.next());
        await assert.rejects(f.fresh().readRows(f.auth, f.dataset));
      });
    }

    await t.test(
      'cancellation and deadline cover inventory intake and initial SQL pin',
      async () => {
        const f = await setup();
        await f.fresh().publishSnapshot(f.auth, f.request());
        const inventory = inventoryLocation(await f.pin());
        for (const deadline of [false, true]) {
          let started!: () => void;
          const intake = new Promise<void>((done) => {
            started = done;
          });
          let cancelled = false;
          const binding: CatalogStorageBinding = {
            ...f.binding,
            storage: async (scope, current) => {
              const storage = await f.binding.storage(scope, current);
              return {
                ...storage,
                read: async (logical, options) => {
                  if (logical.objectId !== inventory.objectId)
                    return storage.read(logical, options);
                  // Open the real authorized object before stalling the consumer-facing stream.
                  const actual = await storage.read(logical, options);
                  return new ReadableStream(
                    {
                      pull() {
                        started();
                        return new Promise(() => {});
                      },
                      async cancel() {
                        cancelled = true;
                        await actual.cancel();
                      },
                    },
                    { highWaterMark: 0 },
                  );
                },
              };
            },
          };
          const controller = new AbortController();
          const pending = new CatalogStorage(authority, [binding]).readRows(f.auth, f.dataset, {
            signal: controller.signal,
            timeoutMs: deadline ? 200 : 60000,
          });
          const rejected = assert.rejects(pending, { code: 'CANCELLED' });
          await intake;
          if (!deadline) controller.abort();
          await rejected;
          assert.equal(cancelled, true);
        }
        const lock = await db.pool.connect();
        try {
          await lock.query(
            "SELECT pg_advisory_lock(hashtextextended('moria/environment/' || $1,0))",
            [f.environment],
          );
          await assert.rejects(f.fresh().readRows(f.auth, f.dataset, { timeoutMs: 50 }), {
            code: 'CANCELLED',
          });
        } finally {
          await lock.query(
            "SELECT pg_advisory_unlock(hashtextextended('moria/environment/' || $1,0))",
            [f.environment],
          );
          lock.release();
        }
        assert.deepEqual(await rowsOf(await f.fresh().readRows(f.auth, f.dataset)), expected);
      },
    );
  });
}
