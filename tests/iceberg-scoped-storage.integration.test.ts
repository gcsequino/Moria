import assert from 'node:assert/strict';
import test from 'node:test';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdir, readFile, readdir } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { getEventListeners } from 'node:events';
import {
  CatalogStorage,
  encodeMetadataLocation,
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
import { scopedObjects, type ScopedObject } from '../workers/iceberg/scoped-read.js';
import { database } from './fixtures/catalog-storage/database.js';
import { startS3 } from './fixtures/storage-s3/server.js';

const logical = {
  version: 1 as const,
  warehouseId: 'warehouse',
  tableId: 'table',
  objectId: 'metadata',
};
const children = async () => {
  const tasks = await readdir(`/proc/${process.pid}/task`);
  const pids = await Promise.all(
    tasks.map(async (task) =>
      (await readFile(`/proc/${process.pid}/task/${task}/children`, 'utf8')).trim(),
    ),
  );
  return pids.join(' ').split(/\s+/).filter(Boolean).sort();
};

for (const provider of ['local', 's3'] as const) {
  test(`${provider}: scoped real Iceberg snapshot reads`, { timeout: 180000 }, async (t) => {
    const db = await database();
    t.after(db.close);
    const server = provider === 's3' ? await startS3() : undefined;
    if (server) t.after(server.close);
    const authority = new Authority(db.catalog);
    const sessions = new WorkloadSessions(authority);
    const environment = 'scoped-read';
    const dataset = 'test/Dataset/data';
    await db.catalog.ensureEnvironment(environment);
    await db.catalog.ensureDataset(environment, dataset);
    await authority.syncPrincipal(environment, 'admin', 'reader', {
      enabled: true,
      kind: 'workload',
      attributes: {},
      markings: [],
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
    const warehouse = join(db.root, 'fixture');
    await mkdir(warehouse);
    const { stdout } = await promisify(execFile)(resolve('.venv/bin/python'), [
      'tests/fixtures/iceberg-scoped-storage/generate.py',
      warehouse,
    ]);
    const fixture = JSON.parse(stdout) as {
      metadata: string;
      wideMetadata: string;
      wideSnapshotId: string;
      snapshotId: string;
      tableUuid: string;
      objects: Record<string, string>;
    };
    const binding = {
      root: join(db.root, 'storage'),
      ...logical,
      maxObjectBytes: 16 * 1024 * 1024,
      chunkBytes: 256,
      endpoint: server?.endpoint ?? '',
      credentials: server?.credentials ?? { accessKeyId: '', secretAccessKey: '' },
      bucket: server?.bucket ?? '',
      prefix: 'scoped',
      region: 'us-east-1',
    };
    await mkdir(binding.root);
    const raw =
      provider === 'local' ? await createLocalStorage(binding) : await createS3Storage(binding);
    const write = async (objectId: string, bytes: Buffer) => {
      await raw.write(
        { ...logical, objectId },
        new ReadableStream({
          start(controller) {
            for (let offset = 0; offset < bytes.length; offset += 256) {
              controller.enqueue(bytes.subarray(offset, offset + 256));
            }
            controller.close();
          },
        }),
        { size: bytes.length },
      );
    };
    const objects: ScopedObject[] = [];
    for (const [name, path] of Object.entries(fixture.objects)) {
      const bytes = await readFile(path);
      const objectId = `file-${objects.length}`;
      await write(objectId, bytes);
      objects.push({ name, location: { ...logical, objectId }, size: bytes.length });
    }
    const metadata = await readFile(fixture.metadata);
    await write('metadata', metadata);
    let accesses = 0;
    const ranges = new Set<string>();
    const offsets = new Set<number>();
    const configuration: CatalogStorageBinding = {
      environment,
      dataset,
      kind: 'Dataset',
      markings: [],
      warehouseId: 'warehouse',
      tableId: 'table',
      icebergTableUuid: fixture.tableUuid,
      readObjects: objects,
      storage: async (scope, current) => {
        accesses++;
        const storage =
          provider === 'local'
            ? await createAuthorizedLocalStorage(binding, scope, current)
            : await createAuthorizedS3Storage(binding, scope, current);
        return {
          ...storage,
          read: async (location, options) => {
            if (options?.range) {
              assert.ok(options.range.length <= 256 * 1024);
              ranges.add(location.objectId);
              offsets.add(options.range.offset);
            }
            return storage.read(location, options);
          },
        };
      },
    };
    const service = new CatalogStorage(authority, [configuration]);
    let revision = '0';
    const publish = async (snapshotId = fixture.snapshotId, objectId = 'metadata') => {
      const execution = randomUUID();
      const fence = await db.catalog.claim(environment, execution, 'worker');
      const state = await authority.current(environment, 'reader', 'app');
      const receipt = await db.catalog.publish({
        environment,
        execution,
        worker: 'worker',
        fence,
        securityEpoch: state.epoch,
        outputs: [
          {
            dataset,
            expectedRevision: revision,
            snapshotId,
            metadataLocation: encodeMetadataLocation({ ...logical, objectId }),
          },
        ],
        lineage: {},
      });
      revision = receipt.outputs[0].revision;
      return execution;
    };
    const execution = await publish();
    const baseline = await children();
    await t.test(
      'exact rows, range reads and pinned provenance survive competing publication',
      async () => {
        const scan = await service.readRows(await session(), dataset);
        assert.equal(scan.execution, execution);
        assert.equal(scan.revision, '1');
        assert.equal(scan.snapshotId, fixture.snapshotId);
        const first = await scan.rows.next();
        assert.equal(first.done, false);
        // Publish a genuinely different retained snapshot while old rows are buffered.
        const older = Object.entries(fixture.objects).find(([name]) => name.includes('/00001-'))!;
        const oldMetadata = await readFile(older[1]);
        // JSON numbers lose int64 precision; extract the exact serialized ID.
        const exactId = /"current-snapshot-id"\s*:\s*(\d+)/.exec(oldMetadata.toString())![1];
        await write('older', oldMetadata);
        await publish(exactId, 'older');
        const rows = [first.value];
        for await (const row of scan.rows) rows.push(row);
        assert.deepEqual(
          rows.sort((a, b) => Number(a!.id) - Number(b!.id)),
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
        assert.ok(ranges.size >= 5);
        assert.ok(offsets.size > 1);
        const next = await service.readRows(await session(), dataset);
        assert.equal(next.snapshotId, exactId);
        const oldRows = [];
        for await (const row of next.rows) oldRows.push(row);
        assert.equal(oldRows.length, 150);
        await publish();
        assert.deepEqual(await children(), baseline);
      },
    );
    for (const action of ['revoke', 'expire', 'cancel', 'return'] as const) {
      await t.test(`${action} stops buffered rows and reaps child`, async () => {
        const auth = await session();
        const abort = new AbortController();
        const scan = await service.readRows(auth, dataset, { signal: abort.signal });
        await scan.rows.next();
        if (action === 'return') await scan.rows.return(undefined);
        else {
          if (action === 'revoke') await sessions.revoke(auth, 'admin');
          if (action === 'expire')
            await db.pool.query(
              "UPDATE moria.workload_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token_hash=$1",
              [auth.sessionHash],
            );
          if (action === 'cancel') abort.abort();
          await assert.rejects(scan.rows.next());
        }
        const count = accesses;
        assert.equal((await scan.rows.next()).done, true);
        assert.equal(accesses, count);
        assert.deepEqual(await children(), baseline);
      });
    }
    for (const action of ['cancel', 'deadline'] as const) {
      await t.test(`${action} interrupts initial pinning before unlock`, async () => {
        const auth = await session();
        const abort = new AbortController();
        const locker = await db.pool.connect();
        const count = accesses;
        let pending: Promise<void> | undefined;
        let timer: ReturnType<typeof setTimeout> | undefined;
        const waitFor = async (condition: () => Promise<boolean>) => {
          const end = Date.now() + 2000;
          while (!(await condition())) {
            assert.ok(Date.now() < end, 'PostgreSQL state did not converge');
            await new Promise((done) => setTimeout(done, 5));
          }
        };
        try {
          await locker.query(
            "SELECT pg_advisory_lock(hashtextextended('moria/environment/' || $1,0))",
            [environment],
          );
          pending = assert.rejects(
            service.readRows(auth, dataset, {
              signal: abort.signal,
              timeoutMs: action === 'deadline' ? 1000 : 60000,
            }),
            { code: 'CANCELLED' },
          );
          await waitFor(async () => {
            const result = await locker.query(
              `SELECT 1 FROM pg_stat_activity
               WHERE datname=current_database() AND pid <> pg_backend_pid()
               AND wait_event_type='Lock'
               AND query LIKE 'SELECT pg_advisory_lock_shared%'`,
            );
            return result.rowCount === 1;
          });
          if (action === 'cancel') abort.abort();
          await Promise.race([
            pending,
            new Promise<never>((_resolve, reject) => {
              timer = setTimeout(() => reject(new Error('pinning did not settle')), 1500);
            }),
          ]);
          // The transaction is still blocked: rejection cannot depend on unlock.
          const blocked = await locker.query(
            `SELECT 1 FROM pg_stat_activity
             WHERE datname=current_database() AND wait_event_type='Lock'
             AND query LIKE 'SELECT pg_advisory_lock_shared%'`,
          );
          assert.equal(blocked.rowCount, 1);
          assert.equal(getEventListeners(abort.signal, 'abort').length, 0);
          assert.equal(accesses, count);
          assert.deepEqual(await children(), baseline);
        } finally {
          clearTimeout(timer);
          abort.abort();
          await locker.query(
            "SELECT pg_advisory_unlock(hashtextextended('moria/environment/' || $1,0))",
            [environment],
          );
          locker.release();
          await pending;
        }
        await waitFor(async () => db.pool.idleCount === db.pool.totalCount);
        assert.equal(db.pool.waitingCount, 0);
        const locks = await db.pool.query("SELECT 1 FROM pg_locks WHERE locktype='advisory'");
        assert.equal(locks.rowCount, 0);
        assert.equal(accesses, count);
        assert.deepEqual(await children(), baseline);
      });
    }
    for (const action of ['return', 'throw', 'complete'] as const) {
      await t.test(`${action} cleans up the readRows lifetime`, async () => {
        const abort = new AbortController();
        const scan = await service.readRows(await session(), dataset, { signal: abort.signal });
        if (action === 'return') {
          assert.equal((await scan.rows.return()).done, true);
        } else if (action === 'throw') {
          const error = new Error('consumer stopped');
          await assert.rejects(scan.rows.throw(error), (actual) => actual === error);
        } else {
          let rows = 0;
          for await (const _row of scan.rows) rows++;
          assert.equal(rows, 300);
        }
        assert.equal((await scan.rows.next()).done, true);
        assert.equal(getEventListeners(abort.signal, 'abort').length, 0);
        assert.deepEqual(await children(), baseline);
      });
    }
    for (const phase of ['before startup', 'buffered rows', 'after child exit'] as const) {
      for (const action of ['deadline', 'cancel'] as const) {
        await t.test(`${action} interrupts contended authority ${phase}`, async (t) => {
          const abort = new AbortController();
          const scan = await service.readRows(await session(), dataset, {
            signal: abort.signal,
            timeoutMs: phase === 'before startup' ? 300 : 5000,
          });
          if (phase === 'buffered rows') {
            assert.equal((await scan.rows.next()).done, false);
            assert.equal((await children()).length, baseline.length + 1);
          }
          if (phase === 'after child exit') {
            for (let row = 0; row < 300; row++) {
              assert.equal((await scan.rows.next()).done, false);
            }
            assert.equal((await children()).length, baseline.length + 1);
          }
          const locker = await db.pool.connect();
          let postExitLocked = false;
          let pending: Promise<unknown> | undefined;
          const within = async <T>(promise: Promise<T>, ms: number): Promise<T> => {
            let timer: ReturnType<typeof setTimeout> | undefined;
            try {
              return await Promise.race([
                promise,
                new Promise<never>((_resolve, reject) => {
                  timer = setTimeout(() => reject(new Error('authority wait did not settle')), ms);
                }),
              ]);
            } finally {
              clearTimeout(timer);
            }
          };
          const waitFor = async (condition: () => Promise<boolean>) => {
            const end = Date.now() + 2000;
            while (!(await condition())) {
              assert.ok(Date.now() < end, 'PostgreSQL contention state did not converge');
              await new Promise((done) => setTimeout(done, 5));
            }
          };
          try {
            const lock = () =>
              locker.query(
                "SELECT pg_advisory_lock(hashtextextended('moria/environment/' || $1,0))",
                [environment],
              );
            if (phase === 'after child exit') {
              const transaction = db.catalog.transaction.bind(db.catalog);
              const contended: typeof db.catalog.transaction = async (fn, ...args) => {
                // Insert contention only after the real reader has exited. Every
                // authority check still runs through the real catalog and PostgreSQL.
                if (!postExitLocked && (await children()).length === baseline.length) {
                  await lock();
                  postExitLocked = true;
                }
                return transaction(fn, ...args);
              };
              t.mock.method(db.catalog, 'transaction', contended);
            } else {
              await lock();
            }
            const count = accesses;
            pending = assert.rejects(scan.rows.next(), { code: 'CANCELLED' });
            // Observe a real blocked backend, rather than assuming a delay means contention.
            await waitFor(async () => {
              const result = await locker.query(
                `SELECT 1 FROM pg_stat_activity
                 WHERE pid <> pg_backend_pid() AND wait_event_type='Lock'
                 AND query LIKE 'SELECT pg_advisory_lock_shared%'`,
              );
              return result.rowCount === 1;
            });
            if (phase === 'after child exit') {
              assert.equal(postExitLocked, true);
              assert.deepEqual(await children(), baseline);
            }
            if (action === 'cancel') abort.abort();
            await within(pending, phase === 'before startup' || action === 'cancel' ? 1000 : 5500);
            // Cleanup must complete while the exclusive lock is STILL held.
            assert.equal((await within(scan.rows.return(undefined), 1000)).done, true);
            assert.equal((await scan.rows.next()).done, true);
            assert.equal(accesses, count);
            assert.equal(getEventListeners(abort.signal, 'abort').length, 0);
            assert.deepEqual(await children(), baseline);
          } finally {
            t.mock.restoreAll();
            await locker.query(
              "SELECT pg_advisory_unlock(hashtextextended('moria/environment/' || $1,0))",
              [environment],
            );
            locker.release();
            await pending;
            await scan.rows.return(undefined);
          }
          // The catalog owns SQL cleanup; its late result cannot restart the scan.
          await waitFor(async () => db.pool.idleCount === db.pool.totalCount);
          assert.equal(db.pool.waitingCount, 0);
          const locks = await db.pool.query("SELECT 1 FROM pg_locks WHERE locktype='advisory'");
          assert.equal(locks.rowCount, 0);
          assert.deepEqual(await children(), baseline);
        });
      }
    }
    await t.test(
      'referenced URLs, physical paths and cross-table manifests never reach storage',
      async () => {
        for (const [index, reference] of [
          '/tmp/not-a-capability',
          'file:///tmp/not-a-capability',
          's3://bucket/key',
          'https://example.invalid/object',
          'moria://warehouse/other/manifest.avro',
          'moria://warehouse/table/../manifest.avro',
        ].entries()) {
          const objectId = `denied-${index}`;
          await write(
            objectId,
            Buffer.from(
              metadata
                .toString()
                .replace(
                  /"manifest-list"\s*:\s*"[^"]+"/g,
                  `"manifest-list":${JSON.stringify(reference)}`,
                ),
            ),
          );
          await publish(fixture.snapshotId, objectId);
          const before = accesses;
          const scan = await service.readRows(await session(), dataset);
          await assert.rejects(scan.rows.next());
          assert.equal(accesses, before + 1);
          assert.deepEqual(await children(), baseline);
        }
        await publish();
      },
    );
    await t.test('oversized real Parquet row fails the frame bound and reaps child', async () => {
      await write('wide', await readFile(fixture.wideMetadata));
      await publish(fixture.wideSnapshotId, 'wide');
      const scan = await service.readRows(await session(), dataset);
      await assert.rejects(scan.rows.next());
      assert.deepEqual(await children(), baseline);
      await publish();
    });
    await t.test('return before first pull does not open a stream or start a child', async () => {
      const scan = await service.readRows(await session(), dataset);
      const count = accesses;
      await scan.rows.return(undefined);
      assert.equal(accesses, count);
      assert.deepEqual(await children(), baseline);
    });
    await t.test('table UUID mismatch and wrong object bytes fail without rows', async () => {
      const wrongTable = new CatalogStorage(authority, [
        { ...configuration, icebergTableUuid: randomUUID() },
      ]);
      await assert.rejects((await wrongTable.readRows(await session(), dataset)).rows.next());
      const wrongBytes = new CatalogStorage(authority, [
        {
          ...configuration,
          readObjects: objects.map((object) => ({ ...object, location: logical })),
        },
      ]);
      await assert.rejects((await wrongBytes.readRows(await session(), dataset)).rows.next());
      assert.deepEqual(await children(), baseline);
    });
    await t.test('child failure stops already buffered rows', async () => {
      const scan = await service.readRows(await session(), dataset);
      await scan.rows.next();
      const spawned = (await children()).filter((pid) => !baseline.includes(pid));
      assert.equal(spawned.length, 1);
      process.kill(Number(spawned[0]), 'SIGKILL');
      await new Promise((done) => setTimeout(done, 100));
      await assert.rejects(scan.rows.next());
      assert.deepEqual(await children(), baseline);
    });
    await t.test('deadline while paused stops child and denies buffered rows', async () => {
      const scan = await service.readRows(await session(), dataset, { timeoutMs: 15000 });
      await scan.rows.next();
      await new Promise((done) => setTimeout(done, 15100));
      assert.deepEqual(await children(), baseline);
      await assert.rejects(scan.rows.next(), { code: 'CANCELLED' });
    });
    await t.test('unknown names, wrong table and snapshot mismatch fail closed', async () => {
      assert.throws(
        () =>
          new CatalogStorage(authority, [
            {
              ...configuration,
              readObjects: [{ ...objects[0], location: { ...logical, tableId: 'other' } }],
            },
          ]),
      );
      for (const name of [
        'file:///etc/passwd',
        's3://bucket/key',
        'moria://warehouse/table/../escape',
        'moria://warehouse/other/data',
      ]) {
        assert.throws(() => scopedObjects([{ ...objects[0], name }], 'warehouse', 'table'));
      }
      const missing = new CatalogStorage(authority, [{ ...configuration, readObjects: [] }]);
      const denied = await missing.readRows(await session(), dataset);
      await assert.rejects(denied.rows.next());
      await publish('1');
      const mismatch = await service.readRows(await session(), dataset);
      await assert.rejects(mismatch.rows.next());
      await publish();
      const auth = await session();
      await assert.rejects(
        async () => {
          const timed = await service.readRows(auth, dataset, { timeoutMs: 1 });
          await timed.rows.next();
        },
        { code: 'CANCELLED' },
      );
      assert.deepEqual(await children(), baseline);
    });
  });
}

test('trusted capability inventories are bounded before engine startup', () => {
  const object = { name: 'moria://warehouse/table/file', location: logical, size: 1 };
  for (const size of [0, -1, 16 * 1024 * 1024 + 1, Number.NaN, 1.5]) {
    assert.throws(() => scopedObjects([{ ...object, size }], 'warehouse', 'table'));
  }
  assert.throws(() =>
    scopedObjects(
      Array.from({ length: 129 }, (_, i) => ({
        ...object,
        name: `moria://warehouse/table/file-${i}`,
      })),
      'warehouse',
      'table',
    ),
  );
  assert.throws(() =>
    scopedObjects(
      Array.from({ length: 5 }, (_, i) => ({
        ...object,
        name: `moria://warehouse/table/file-${i}`,
        size: 16 * 1024 * 1024,
      })),
      'warehouse',
      'table',
    ),
  );
  assert.throws(() => scopedObjects([object, object], 'warehouse', 'table'));
});
