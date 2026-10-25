import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createLocalStorage } from '../services/storage/src/local.js';
import { createS3Storage } from '../services/storage/src/s3.js';
import {
  createAuthorizedLocalStorage,
  createAuthorizedS3Storage,
  type StorageAuthorityScope,
  type TrustedStorageAuthority,
} from '../services/storage/src/authority.js';
import type { ReadOptions, WriteOptions } from '../services/storage/src/locations.js';
import { startS3 } from './fixtures/storage-s3/server.js';
import { proxyS3 } from './fixtures/storage-s3/proxy.js';
import { logical, limits, source, code, collect } from './fixtures/storage-contract/helpers.js';

for (const provider of ['local', 's3'] as const) {
  test(`${provider}: shared streaming contract`, { timeout: 60000 }, async (t) => {
    const root = await mkdtemp(join(tmpdir(), 'storage-contract-'));
    const server = provider === 's3' ? await startS3() : undefined;
    const binding = {
      ...limits,
      root,
      endpoint: server?.endpoint ?? '',
      credentials: server?.credentials ?? { accessKeyId: '', secretAccessKey: '' },
      bucket: server?.bucket ?? '',
      prefix: 'contract',
      region: 'us-east-1',
    };
    const storage =
      provider === 'local' ? await createLocalStorage(binding) : await createS3Storage(binding);
    const authorized = (scope: StorageAuthorityScope, authority: TrustedStorageAuthority) =>
      provider === 'local'
        ? createAuthorizedLocalStorage(binding, scope, authority)
        : createAuthorizedS3Storage(binding, scope, authority);
    try {
      await t.test('logical bindings, capability discovery and sanitized errors', async () => {
        for (const value of ['../escape', 'a/b', '%2f', 'https://other', 'a\\b']) {
          await assert.rejects(
            storage.read({ ...logical, objectId: value }),
            code('INVALID_INPUT'),
          );
        }
        await assert.rejects(
          storage.read({ ...logical, tableId: 'other' }),
          code('BINDING_MISMATCH'),
        );
        await assert.rejects(
          storage.read({ ...logical, warehouseId: 'other' }),
          code('BINDING_MISMATCH'),
        );
        await assert.rejects(storage.read(logical), code('NOT_FOUND'));
        for (const capability of ['overwrite', 'delete', 'multipart', 'publish']) {
          assert.throws(
            () => storage.requireCapability(capability),
            code('UNSUPPORTED_CAPABILITY'),
          );
        }
        for (const value of [null, 1, [], { signal: {} }]) {
          await assert.rejects(storage.read(logical, value as ReadOptions), code('INVALID_INPUT'));
          const input = source([]);
          await assert.rejects(
            storage.write(logical, input, value as WriteOptions),
            code('INVALID_INPUT'),
          );
          assert.equal(input.locked, false);
        }
      });
      await t.test('exact size and bounded chunks fail without final objects', async () => {
        for (const [chunks, size, expected] of [
          [[[1]], 2, 'INVALID_INPUT'],
          [[[1, 2]], 1, 'LIMIT_EXCEEDED'],
          [[[1, 2, 3, 4, 5]], 5, 'LIMIT_EXCEEDED'],
        ] as const) {
          const input = source(chunks.map((chunk) => [...chunk]));
          await assert.rejects(storage.write(logical, input, { size }), code(expected));
          assert.equal(input.locked, false);
          await assert.rejects(storage.read(logical), code('NOT_FOUND'));
        }
        const bad = new ReadableStream<Uint8Array>({
          pull() {
            throw new Error('synthetic secret');
          },
        });
        await assert.rejects(storage.write(logical, bad, { size: 1 }), code('IO_ERROR'));
      });
      await t.test('create-only competing writers, exact ranges and bounded reads', async () => {
        const results = await Promise.allSettled([
          storage.write(
            logical,
            source([
              [1, 2, 3, 4],
              [5, 6],
            ]),
            { size: 6 },
          ),
          storage.write(
            logical,
            source([
              [7, 8, 9, 10],
              [11, 12],
            ]),
            { size: 6 },
          ),
        ]);
        assert.equal(results.filter((r) => r.status === 'fulfilled').length, 1);
        const rejected = results.find((r) => r.status === 'rejected');
        assert.ok(rejected?.status === 'rejected');
        code('ALREADY_EXISTS')(rejected.reason);
        const bytes = await collect(storage);
        assert.deepEqual(
          bytes,
          results[0].status === 'fulfilled' ? [1, 2, 3, 4, 5, 6] : [7, 8, 9, 10, 11, 12],
        );
        const ranges = [
          [0, 6],
          [1, 4],
          [6, 0],
          [0, 0],
        ];
        for (const [offset, length] of ranges) {
          const actual = [];
          for await (const chunk of await storage.read(logical, { range: { offset, length } })) {
            assert.ok(chunk.length <= 4);
            actual.push(...chunk);
          }
          assert.deepEqual(actual, bytes.slice(offset, offset + length));
        }
        for (const range of [
          null,
          [],
          {},
          { offset: -1, length: 1 },
          { offset: 6, length: 1 },
          { offset: 7, length: 0 },
        ]) {
          await assert.rejects(
            storage.read(logical, { range } as ReadOptions),
            code('INVALID_INPUT'),
          );
        }
        const empty = { ...logical, objectId: 'empty' };
        await storage.write(empty, source([]), { size: 0 });
        assert.deepEqual(await collect(storage, empty), []);
      });
      await t.test('cancellation releases pending sources and read streams', async () => {
        const controller = new AbortController();
        let cancel = false;
        let entered!: () => void;
        const pending = new Promise<void>((resolve) => {
          entered = resolve;
        });
        const input = new ReadableStream<Uint8Array>(
          {
            pull() {
              entered();
              return new Promise<void>(() => {});
            },
            cancel() {
              cancel = true;
            },
          },
          { highWaterMark: 0 },
        );
        const write = storage.write({ ...logical, objectId: 'cancelled' }, input, {
          size: 1,
          signal: controller.signal,
        });
        await pending;
        controller.abort();
        await assert.rejects(write, code('CANCELLED'));
        assert.equal(cancel, true);
        assert.equal(input.locked, false);
        await assert.rejects(
          storage.read({ ...logical, objectId: 'cancelled' }),
          code('NOT_FOUND'),
        );
        const readAbort = new AbortController();
        const reader = (await storage.read(logical, { signal: readAbort.signal })).getReader();
        await reader.read();
        readAbort.abort();
        await assert.rejects(reader.read(), code('CANCELLED'));
        const stream = await storage.read(logical);
        await stream.cancel();
      });
      await t.test(
        'current authority before access, on demand, after source and before finalization',
        async () => {
          let allowed = true;
          let checks = 0;
          const authority = {
            async checkCurrent() {
              checks++;
              return allowed;
            },
          };
          const scoped = await authorized({ location: logical, operations: ['read'] }, authority);
          const input = source([]);
          await assert.rejects(scoped.write(logical, input, { size: 0 }), code('AUTHORITY_DENIED'));
          assert.equal(input.locked, false);
          await assert.rejects(
            scoped.read({ ...logical, objectId: 'other' }),
            code('AUTHORITY_DENIED'),
          );
          const reader = (await scoped.read(logical)).getReader();
          await reader.read();
          const before = checks;
          await new Promise<void>((resolve) => setImmediate(resolve));
          assert.equal(checks, before);
          allowed = false;
          await assert.rejects(reader.read(), code('AUTHORITY_DENIED'));
          const object = { ...logical, objectId: 'revoked' };
          const writer = await authorized({ location: object, operations: ['create'] }, authority);
          allowed = true;
          const revoked = new ReadableStream<Uint8Array>(
            {
              pull(value) {
                allowed = false;
                value.enqueue(new Uint8Array([1]));
              },
            },
            { highWaterMark: 0 },
          );
          await assert.rejects(
            writer.write(object, revoked, { size: 1 }),
            code('AUTHORITY_DENIED'),
          );
          assert.equal(revoked.locked, false);
          await assert.rejects(storage.read(object), code('NOT_FOUND'));
          // EOF is observed, then the final authority check denies publication.
          let eof = false;
          let afterEof = 0;
          const final = await authorized(
            { location: object, operations: ['create'] },
            {
              async checkCurrent() {
                return !eof || ++afterEof < (provider === 's3' ? 3 : 2);
              },
            },
          );
          const empty = new ReadableStream<Uint8Array>(
            {
              pull(value) {
                eof = true;
                value.close();
              },
            },
            { highWaterMark: 0 },
          );
          await assert.rejects(final.write(object, empty, { size: 0 }), code('AUTHORITY_DENIED'));
          await assert.rejects(storage.read(object), code('NOT_FOUND'));
        },
      );
      await t.test(
        'uncertain finalization preserves complete bytes and rejects retry',
        async () => {
          const object = { ...logical, objectId: 'uncertain' };
          if (provider === 'local') {
            // Existing real-filesystem fault fixture fails a durability barrier after link.
            const isolated = await mkdtemp(join(tmpdir(), 'contract-uncertain-'));
            try {
              const result = await promisify(execFile)(
                process.execPath,
                [
                  '--import',
                  'tsx',
                  new URL('./fixtures/storage-durability/child.ts', import.meta.url).pathname,
                  isolated,
                  'fault',
                  'write:warehouse/table/objects:sync:after',
                ],
                { timeout: 10000 },
              );
              const observed = JSON.parse(result.stdout);
              assert.equal(observed.code, 'IO_ERROR');
              assert.equal(observed.writeOutcome, 'uncertain');
              assert.equal(observed.triggered, true);
              const local = await createLocalStorage({ ...limits, root: isolated });
              assert.deepEqual(await collect(local), [1, 2, 3, 4, 5, 6, 7, 8]);
              await assert.rejects(
                local.write(logical, source([[9]]), { size: 1 }),
                code('ALREADY_EXISTS'),
              );
            } finally {
              await rm(isolated, { recursive: true, force: true });
            }
          } else {
            let dropped = false;
            const proxy = await proxyS3(server!.endpoint, async (request, status) => {
              if (
                request.method === 'POST' &&
                request.url?.includes('uploadId=') &&
                status === 200
              ) {
                dropped = true;
                return 'drop';
              }
            });
            try {
              const remote = await createS3Storage({ ...binding, endpoint: proxy.endpoint });
              await assert.rejects(
                remote.write(object, source([[1, 2]]), { size: 2 }),
                code('IO_ERROR', true),
              );
              assert.equal(dropped, true);
              assert.deepEqual(await collect(storage, object), [1, 2]);
              await assert.rejects(
                storage.write(object, source([[9]]), { size: 1 }),
                code('ALREADY_EXISTS'),
              );
            } finally {
              await proxy.close();
            }
          }
        },
      );
    } finally {
      await server?.close();
      await rm(root, { recursive: true, force: true });
    }
  });
}
