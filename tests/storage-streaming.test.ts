import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, rm, readdir, stat, truncate, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createLocalStorage } from '../services/storage/src/local.js';
import {
  StorageError,
  type ReadOptions,
  type WriteOptions,
  type StreamingStorage,
} from '../services/storage/src/locations.js';

const logical = {
  version: 1 as const,
  warehouseId: 'warehouse',
  tableId: 'table',
  objectId: 'object',
};
const code = (expected: string) => (error: unknown) =>
  error instanceof StorageError && error.code === expected;
function source(chunks: number[][]): ReadableStream<Uint8Array> {
  return new ReadableStream(
    {
      pull(controller) {
        const chunk = chunks.shift();
        if (chunk) controller.enqueue(Uint8Array.from(chunk));
        else controller.close();
      },
    },
    { highWaterMark: 0 },
  );
}
async function collect(stream: ReadableStream<Uint8Array>): Promise<number[]> {
  const output: number[] = [];
  for await (const chunk of stream) {
    assert.ok(chunk.length <= 4);
    output.push(...chunk);
  }
  return output;
}
async function fixture(run: (storage: StreamingStorage, table: string) => Promise<void>) {
  const root = await mkdtemp(join(tmpdir(), 'storage-stream-'));
  try {
    const storage = await createLocalStorage({
      root,
      warehouseId: 'warehouse',
      tableId: 'table',
      maxObjectBytes: 64,
      chunkBytes: 4,
    });
    await run(storage, join(root, 'warehouse', 'table'));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

test('multi-chunk, empty objects and strict ranges', { timeout: 5000 }, async () =>
  fixture(async (storage) => {
    await storage.write(logical, source([[0, 1, 2, 3], [4, 5, 6, 7], [8]]), { size: 9 });
    assert.deepEqual(await collect(await storage.read(logical)), [0, 1, 2, 3, 4, 5, 6, 7, 8]);
    assert.deepEqual(
      await collect(await storage.read(logical, { range: { offset: 3, length: 5 } })),
      [3, 4, 5, 6, 7],
    );
    assert.deepEqual(
      await collect(await storage.read(logical, { range: { offset: 9, length: 0 } })),
      [],
    );
    for (const range of [
      { offset: -1, length: 1 },
      { offset: 0, length: 10 },
      { offset: 10, length: 0 },
      { offset: 0.5, length: 1 },
      { offset: NaN, length: 1 },
      { offset: 0, length: Infinity },
    ]) {
      await assert.rejects(storage.read(logical, { range }), code('INVALID_INPUT'));
    }
    const empty = { ...logical, objectId: 'empty' };
    await storage.write(empty, source([]), { size: 0 });
    assert.deepEqual(await collect(await storage.read(empty)), []);
  }),
);

test(
  'empty chunk producers allow deadline cancellation and staging cleanup',
  { timeout: 10000 },
  async () =>
    fixture(async (_storage, table) => {
      // A separate process enforces the timeout even if the producer starves its event loop.
      await promisify(execFile)(
        process.execPath,
        [
          '--import',
          'tsx',
          '--input-type=module',
          '--eval',
          `
          import assert from 'node:assert/strict';
          import { createLocalStorage } from './services/storage/src/local.ts';
          const storage = await createLocalStorage({
            root: process.argv[1], warehouseId: 'warehouse', tableId: 'table',
            maxObjectBytes: 64, chunkBytes: 4,
          });
          const abort = new AbortController();
          let timer;
          let pulls = 0;
          let cancelled = false;
          const source = new ReadableStream({
            pull(controller) {
              if (pulls++ === 0) {
                timer = setTimeout(() => abort.abort(), 10);
              }
              controller.enqueue(new Uint8Array(0));
            },
            cancel() {
              cancelled = true;
            },
          }, { highWaterMark: 0 });
          try {
            await assert.rejects(storage.write({
              version: 1, warehouseId: 'warehouse', tableId: 'table', objectId: 'object',
            }, source, { size: 0, signal: abort.signal }), { code: 'CANCELLED' });
            assert.ok(pulls > 1);
            assert.ok(cancelled);
            assert.equal(source.locked, false);
          } finally {
            clearTimeout(timer);
          }
        `,
          join(table, '..', '..'),
        ],
        { timeout: 5000, killSignal: 'SIGKILL' },
      );
      assert.deepEqual(await readdir(join(table, 'staging')), []);
      assert.deepEqual(await readdir(join(table, 'objects')), []);
    }),
);

test(
  'malformed transfer options reject without locking or creating files',
  { timeout: 5000 },
  async () =>
    fixture(async (storage, table) => {
      const malformed = [
        null,
        false,
        0,
        '',
        'options',
        [],
        { signal: null },
        { signal: {} },
        { signal: { aborted: false, addEventListener() {}, removeEventListener() {} } },
        { signal: Object.create(AbortSignal.prototype) },
      ];
      for (const options of malformed) {
        const input = source([]);
        const writeOptions =
          options && typeof options === 'object' && !Array.isArray(options)
            ? { size: 0, ...options }
            : options;
        await assert.rejects(
          storage.write(logical, input, writeOptions as WriteOptions),
          code('INVALID_INPUT'),
        );
        assert.equal(input.locked, false);
        await assert.rejects(storage.read(logical, options as ReadOptions), code('INVALID_INPUT'));
        assert.deepEqual(await readdir(join(table, 'staging')), []);
        assert.deepEqual(await readdir(join(table, 'objects')), []);
      }
      const input = source([]);
      await assert.rejects(
        storage.write(logical, input, undefined as unknown as WriteOptions),
        code('INVALID_INPUT'),
      );
      assert.equal(input.locked, false);
    }),
);

test(
  'listener registration failure releases the reader before filesystem access',
  { timeout: 5000 },
  async () =>
    fixture(async (storage, table) => {
      const signal = new AbortController().signal;
      let sourceCancelled = false;
      const input = new ReadableStream<Uint8Array>(
        {
          cancel() {
            sourceCancelled = true;
          },
        },
        { highWaterMark: 0 },
      );
      signal.addEventListener = (...args: Parameters<EventTarget['addEventListener']>) => {
        EventTarget.prototype.addEventListener.apply(signal, args);
        throw new Error('synthetic listener registration failure');
      };
      await assert.rejects(storage.write(logical, input, { size: 0, signal }), code('IO_ERROR'));
      assert.ok(sourceCancelled);
      assert.equal(input.locked, false);
      assert.deepEqual(await readdir(join(table, 'staging')), []);
      assert.deepEqual(await readdir(join(table, 'objects')), []);
    }),
);

test('supplied malformed ranges fail before filesystem access', { timeout: 5000 }, async () =>
  fixture(async (storage, table) => {
    await storage.write(logical, source([[1, 2]]), { size: 2 });
    for (const range of [null, false, 0, '', true, 1, 'range', [], {}, { offset: 0 }]) {
      const options = { range } as unknown as ReadOptions;
      await assert.rejects(storage.read(logical, options), code('INVALID_INPUT'));
    }
    await rm(join(table, 'objects'), { recursive: true });
    for (const range of [null, false, 0, '', [], {}]) {
      await assert.rejects(
        storage.read(logical, { range } as unknown as ReadOptions),
        code('INVALID_INPUT'),
      );
    }
  }),
);

test(
  'failed, malformed and cancelled writes clean staging and never finalize',
  { timeout: 5000 },
  async () =>
    fixture(async (storage, table) => {
      for (const [chunks, size, expected] of [
        [[[1]], 2, 'INVALID_INPUT'],
        [[[1, 2]], 1, 'LIMIT_EXCEEDED'],
        [[[1, 2, 3, 4, 5]], 5, 'LIMIT_EXCEEDED'],
        [[], -1, 'INVALID_INPUT'],
        [[], 65, 'INVALID_INPUT'],
        [[], 0.5, 'INVALID_INPUT'],
      ] as const) {
        await assert.rejects(
          storage.write(logical, source(chunks.map((c) => [...c])), { size }),
          code(expected),
        );
        assert.deepEqual(await readdir(join(table, 'staging')), []);
        assert.deepEqual(await readdir(join(table, 'objects')), []);
      }
      let pulls = 0;
      const broken = new ReadableStream<Uint8Array>(
        {
          pull(c) {
            if (pulls++ === 0) c.enqueue(Uint8Array.of(1));
            else c.error(new Error('synthetic source failure'));
          },
        },
        { highWaterMark: 0 },
      );
      await assert.rejects(storage.write(logical, broken, { size: 2 }), code('IO_ERROR'));
      const abort = new AbortController();
      let requested!: () => void;
      const started = new Promise<void>((resolve) => {
        requested = resolve;
      });
      let sourceCancelled = false;
      const stalled = new ReadableStream<Uint8Array>(
        {
          pull() {
            requested();
          },
          cancel() {
            sourceCancelled = true;
          },
        },
        { highWaterMark: 0 },
      );
      const writing = storage.write(logical, stalled, { size: 1, signal: abort.signal });
      await started;
      assert.deepEqual(await readdir(join(table, 'objects')), []);
      const stages = await readdir(join(table, 'staging'));
      assert.equal(stages.length, 1);
      assert.equal((await stat(join(table, 'staging', stages[0]))).mode & 0o777, 0o600);
      abort.abort();
      await assert.rejects(writing, code('CANCELLED'));
      assert.ok(sourceCancelled);
      assert.deepEqual(await readdir(join(table, 'staging')), []);
      assert.deepEqual(await readdir(join(table, 'objects')), []);
      await assert.rejects(
        storage.write(logical, source([]), { size: 0, signal: abort.signal }),
        code('CANCELLED'),
      );
    }),
);

test(
  'atomic competing finalization does not clobber existing objects',
  { timeout: 5000 },
  async () =>
    fixture(async (storage, table) => {
      const results = await Promise.allSettled([
        storage.write(logical, source([[1]]), { size: 1 }),
        storage.write(logical, source([[2]]), { size: 1 }),
      ]);
      assert.equal(results.filter((r) => r.status === 'fulfilled').length, 1);
      const rejected = results.find((r) => r.status === 'rejected');
      assert.ok(rejected?.status === 'rejected' && code('ALREADY_EXISTS')(rejected.reason));
      const before = await collect(await storage.read(logical));
      await assert.rejects(
        storage.write(logical, source([[3]]), { size: 1 }),
        code('ALREADY_EXISTS'),
      );
      assert.deepEqual(await collect(await storage.read(logical)), before);
      assert.deepEqual(await readdir(join(table, 'staging')), []);
    }),
);

test(
  'bounded producer demand, read cancellation, abort and injected truncation',
  { timeout: 5000 },
  async () =>
    fixture(async (storage, table) => {
      let produced = 0;
      const stream = new ReadableStream<Uint8Array>(
        {
          async pull(c) {
            // The preceding chunk must have reached disk before the next pull.
            if (produced > 0) {
              const names = await readdir(join(table, 'staging'));
              assert.equal((await stat(join(table, 'staging', names[0]))).size, produced * 4);
            }
            if (produced === 16) c.close();
            else {
              produced++;
              c.enqueue(Uint8Array.of(1, 2, 3, 4));
            }
          },
        },
        { highWaterMark: 0 },
      );
      await storage.write(logical, stream, { size: 64 });
      const reader = (await storage.read(logical)).getReader();
      assert.equal((await reader.read()).value?.length, 4);
      await reader.cancel();
      assert.equal((await reader.read()).done, true);
      const abort = new AbortController();
      const abortReader = (await storage.read(logical, { signal: abort.signal })).getReader();
      abort.abort();
      await assert.rejects(abortReader.read(), code('CANCELLED'));
      await assert.rejects(storage.read(logical, { signal: abort.signal }), code('CANCELLED'));
      const truncated = (await storage.read(logical)).getReader();
      await truncated.read();
      await truncate(join(table, 'objects', 'object'), 4);
      await assert.rejects(truncated.read(), code('IO_ERROR'));
      await writeFile(join(table, 'objects', 'object'), Buffer.alloc(65));
      await assert.rejects(storage.read(logical), code('LIMIT_EXCEEDED'));
    }),
);

test('runtime chunk validation and mutable range snapshots', { timeout: 5000 }, async () =>
  fixture(async (storage, table) => {
    const malformed = new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue('invalid' as unknown as Uint8Array);
        c.close();
      },
    });
    await assert.rejects(storage.write(logical, malformed, { size: 1 }), code('INVALID_INPUT'));
    const locked = source([]);
    const lock = locked.getReader();
    await assert.rejects(storage.write(logical, locked, { size: 0 }), code('INVALID_INPUT'));
    lock.releaseLock();
    assert.deepEqual(await readdir(join(table, 'staging')), []);
    await storage.write(logical, source([[1, 2, 3, 4]]), { size: 4 });
    const range = { offset: 1, length: 2 };
    const reading = storage.read(logical, { range });
    range.offset = -1;
    range.length = 1000;
    assert.deepEqual(await collect(await reading), [2, 3]);
  }),
);
