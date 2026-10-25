import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, readFile, readdir, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createLocalStorage } from '../services/storage/src/local.js';
import {
  createAuthorizedLocalStorage,
  type StorageOperation,
  type TrustedStorageAuthority,
} from '../services/storage/src/authority.js';
import {
  StorageError,
  type ReadOptions,
  type WriteOptions,
} from '../services/storage/src/locations.js';

const logical = Object.freeze({
  version: 1 as const,
  warehouseId: 'warehouse',
  tableId: 'table',
  objectId: 'object',
});
const code = (expected: string) => (error: unknown) => {
  assert.ok(error instanceof StorageError);
  assert.equal(error.code, expected);
  assert.equal(error.message, expected);
  assert.equal(error.cause, undefined);
  return true;
};
function source(chunks: number[][]) {
  return new ReadableStream<Uint8Array>(
    {
      pull(controller) {
        const chunk = chunks.shift();
        if (chunk) {
          controller.enqueue(Uint8Array.from(chunk));
        } else {
          controller.close();
        }
      },
    },
    { highWaterMark: 0 },
  );
}
async function fixture(run: (context: Awaited<ReturnType<typeof setup>>) => Promise<void>) {
  const context = await setup();
  try {
    await run(context);
  } finally {
    await rm(context.root, { recursive: true, force: true });
  }
}
async function setup() {
  const root = await mkdtemp(join(tmpdir(), 'storage-authority-'));
  const binding = {
    root,
    warehouseId: 'warehouse',
    tableId: 'table',
    maxObjectBytes: 64,
    chunkBytes: 4,
  };
  const raw = await createLocalStorage(binding);
  const table = join(root, 'warehouse', 'table');
  const state = { revoked: false, now: 0, expires: 10, fail: false, checks: 0 };
  const authority: TrustedStorageAuthority = {
    async checkCurrent(request) {
      state.checks++;
      assert.deepEqual(request.location, logical);
      assert.ok(Object.isFrozen(request));
      assert.ok(Object.isFrozen(request.location));
      if (state.fail) {
        throw new Error('/private/authority synthetic failure');
      }
      return !state.revoked && state.now < state.expires;
    },
  };
  const scoped = (operations: StorageOperation[] = ['read', 'create']) =>
    createAuthorizedLocalStorage(binding, { location: logical, operations }, authority);
  return { root, table, binding, raw, state, authority, scoped };
}

test(
  'exact scope and explicit operations deny before filesystem access; empty scope denies',
  { timeout: 5000 },
  async () =>
    fixture(async ({ scoped, table, state }) => {
      const storage = await scoped();
      assert.deepEqual(Object.keys(storage).sort(), [
        'read',
        'requireCapability',
        'version',
        'write',
      ]);
      const readOnly = await scoped(['read']);
      const createOnly = await scoped(['create']);
      const empty = await scoped([]);
      await rm(join(table, 'objects'), { recursive: true });
      await rm(join(table, 'staging'), { recursive: true });
      for (const [field, expected] of [
        ['warehouseId', 'BINDING_MISMATCH'],
        ['tableId', 'BINDING_MISMATCH'],
        ['objectId', 'AUTHORITY_DENIED'],
      ] as const) {
        const other = { ...logical, [field]: 'other' };
        await assert.rejects(storage.read(other), code(expected));
        const input = source([[1]]);
        await assert.rejects(storage.write(other, input, { size: 1 }), code(expected));
        assert.equal(input.locked, false);
      }
      await assert.rejects(
        readOnly.write(logical, source([]), { size: 0 }),
        code('AUTHORITY_DENIED'),
      );
      await assert.rejects(createOnly.read(logical), code('AUTHORITY_DENIED'));
      await assert.rejects(empty.read(logical), code('AUTHORITY_DENIED'));
      await assert.rejects(empty.write(logical, source([]), { size: 0 }), code('AUTHORITY_DENIED'));
      assert.equal(state.checks, 0);
    }),
);

for (const change of ['revoked', 'expired', 'failure'] as const) {
  test(
    `${change} denies before access and on subsequent read demand`,
    { timeout: 5000 },
    async () =>
      fixture(async ({ raw, scoped, state, table }) => {
        await raw.write(
          logical,
          source([
            [1, 2, 3, 4],
            [5, 6, 7, 8],
          ]),
          { size: 8 },
        );
        const storage = await scoped();
        const reader = (await storage.read(logical)).getReader();
        assert.deepEqual(Array.from((await reader.read()).value!), [1, 2, 3, 4]);
        const checks = state.checks;
        await new Promise<void>((resolve) => setImmediate(resolve));
        assert.equal(state.checks, checks);
        if (change === 'revoked') {
          state.revoked = true;
        } else if (change === 'expired') {
          state.now = state.expires;
        } else {
          state.fail = true;
        }
        await assert.rejects(reader.read(), code('AUTHORITY_DENIED'));
        reader.releaseLock();
        await assert.rejects(storage.read(logical), code('AUTHORITY_DENIED'));
        const input = source([[9]]);
        await assert.rejects(storage.write(logical, input, { size: 1 }), code('AUTHORITY_DENIED'));
        assert.equal(input.locked, false);
        assert.deepEqual(await readdir(join(table, 'staging')), []);
        assert.deepEqual(
          Array.from(await readFile(join(table, 'objects', 'object'))),
          [1, 2, 3, 4, 5, 6, 7, 8],
        );
      }),
  );

  test(
    `${change} during source consumption cancels, unlocks and removes private staging`,
    { timeout: 5000 },
    async () =>
      fixture(async ({ raw, scoped, state, table }) => {
        await raw.write(logical, source([[9]]), { size: 1 });
        const storage = await scoped();
        let pulls = 0;
        let cancelled = false;
        const input = new ReadableStream<Uint8Array>(
          {
            async pull(controller) {
              pulls++;
              const stages = await readdir(join(table, 'staging'));
              assert.equal(stages.length, 1);
              const staged = await stat(join(table, 'staging', stages[0]));
              assert.equal(staged.mode & 0o777, 0o600);
              assert.equal(staged.size, (pulls - 1) * 4);
              if (pulls === 2) {
                if (change === 'revoked') {
                  state.revoked = true;
                } else if (change === 'expired') {
                  state.now = state.expires;
                } else {
                  state.fail = true;
                }
              }
              controller.enqueue(Uint8Array.of(1, 2, 3, 4));
            },
            cancel() {
              cancelled = true;
            },
          },
          { highWaterMark: 0 },
        );
        await assert.rejects(storage.write(logical, input, { size: 8 }), code('AUTHORITY_DENIED'));
        assert.equal(pulls, 2);
        assert.ok(cancelled);
        assert.equal(input.locked, false);
        assert.deepEqual(await readdir(join(table, 'staging')), []);
        assert.deepEqual(Array.from(await readFile(join(table, 'objects', 'object'))), [9]);
      }),
  );
}

test(
  'successful bounded transfers, ranges, no-clobber and strict validation',
  { timeout: 5000 },
  async () =>
    fixture(async ({ scoped, table, state }) => {
      const storage = await scoped();
      await storage.write(logical, source([[1, 2, 3, 4], [5]]), { size: 5 });
      const chunks = [];
      for await (const chunk of await storage.read(logical, { range: { offset: 1, length: 4 } })) {
        assert.ok(chunk.length <= 4);
        chunks.push(...chunk);
      }
      assert.deepEqual(chunks, [2, 3, 4, 5]);
      await assert.rejects(
        storage.write(logical, source([[9]]), { size: 1 }),
        code('ALREADY_EXISTS'),
      );
      for (const options of [
        null,
        [],
        { signal: {} },
        { range: null },
        { range: { offset: -1, length: 1 } },
      ]) {
        await assert.rejects(storage.read(logical, options as ReadOptions), code('INVALID_INPUT'));
      }
      for (const options of [undefined, null, [], { size: -1 }, { size: 0, signal: {} }]) {
        const input = source([]);
        await assert.rejects(
          storage.write(logical, input, options as WriteOptions),
          code('INVALID_INPUT'),
        );
        assert.equal(input.locked, false);
      }
      await assert.rejects(
        storage.read({ ...logical, objectId: '../escape' }),
        code('INVALID_INPUT'),
      );
      const count = state.checks;
      assert.throws(() => storage.requireCapability('delete'), code('UNSUPPORTED_CAPABILITY'));
      assert.equal(state.checks, count);
      assert.deepEqual(await readdir(join(table, 'staging')), []);
      assert.deepEqual(
        Array.from(await readFile(join(table, 'objects', 'object'))),
        [1, 2, 3, 4, 5],
      );
    }),
);

test('cancellation releases stalled write source and read handles', { timeout: 5000 }, async () =>
  fixture(async ({ scoped, raw, table }) => {
    const storage = await scoped();
    const abort = new AbortController();
    let started!: () => void;
    const ready = new Promise<void>((resolve) => {
      started = resolve;
    });
    let cancelled = false;
    const input = new ReadableStream<Uint8Array>(
      {
        pull() {
          started();
        },
        cancel() {
          cancelled = true;
        },
      },
      { highWaterMark: 0 },
    );
    const writing = storage.write(logical, input, { size: 1, signal: abort.signal });
    await ready;
    abort.abort();
    await assert.rejects(writing, code('CANCELLED'));
    assert.ok(cancelled);
    assert.equal(input.locked, false);
    assert.deepEqual(await readdir(join(table, 'staging')), []);
    assert.deepEqual(await readdir(join(table, 'objects')), []);
    await raw.write(logical, source([[1], [2]]), { size: 2 });
    const reader = (await storage.read(logical)).getReader();
    await reader.read();
    await reader.cancel();
    assert.equal((await reader.read()).done, true);
    reader.releaseLock();
    const abortRead = new AbortController();
    const reading = (await storage.read(logical, { signal: abortRead.signal })).getReader();
    abortRead.abort();
    await assert.rejects(reading.read(), code('CANCELLED'));
    reading.releaseLock();
  }),
);

test(
  'authority outage is fail closed, abortable, and never leaks checker errors',
  { timeout: 5000 },
  async () =>
    fixture(async ({ binding, table }) => {
      for (const decision of [false, undefined, 'true']) {
        const storage = await createAuthorizedLocalStorage(
          binding,
          { location: logical, operations: ['read'] },
          {
            async checkCurrent() {
              return decision as boolean;
            },
          },
        );
        await assert.rejects(storage.read(logical), code('AUTHORITY_DENIED'));
      }
      let entered!: () => void;
      const ready = new Promise<void>((resolve) => {
        entered = resolve;
      });
      let checks = 0;
      const storage = await createAuthorizedLocalStorage(
        binding,
        { location: logical, operations: ['create'] },
        {
          async checkCurrent() {
            if (++checks === 1) {
              return true;
            }
            entered();
            return new Promise<boolean>(() => {});
          },
        },
      );
      const abort = new AbortController();
      const input = source([[1]]);
      const writing = storage.write(logical, input, { size: 1, signal: abort.signal });
      await ready;
      abort.abort();
      await assert.rejects(writing, code('CANCELLED'));
      assert.equal(input.locked, false);
      assert.deepEqual(await readdir(join(table, 'staging')), []);
      assert.deepEqual(await readdir(join(table, 'objects')), []);
    }),
);

test(
  'read release and pre-finalization checks discard newly denied bytes',
  { timeout: 5000 },
  async () =>
    fixture(async ({ binding, raw, table }) => {
      await raw.write(logical, source([[9]]), { size: 1 });
      let checks = 0;
      const reading = await createAuthorizedLocalStorage(
        binding,
        { location: logical, operations: ['read'] },
        {
          async checkCurrent() {
            return ++checks < 3;
          },
        },
      );
      const reader = (await reading.read(logical)).getReader();
      await assert.rejects(reader.read(), code('AUTHORITY_DENIED'));
      reader.releaseLock();
      checks = 0;
      const writing = await createAuthorizedLocalStorage(
        binding,
        { location: logical, operations: ['create'] },
        {
          async checkCurrent() {
            return ++checks < 6;
          },
        },
      );
      const input = source([[1]]);
      await assert.rejects(writing.write(logical, input, { size: 1 }), code('AUTHORITY_DENIED'));
      assert.equal(checks, 6);
      assert.equal(input.locked, false);
      assert.deepEqual(await readdir(join(table, 'staging')), []);
      assert.deepEqual(Array.from(await readFile(join(table, 'objects', 'object'))), [9]);
    }),
);

test(
  'service scope and request snapshots cannot be broadened by later mutation',
  { timeout: 5000 },
  async () =>
    fixture(async ({ binding, raw }) => {
      await raw.write(logical, source([[1, 2, 3]]), { size: 3 });
      const scopedLocation = { ...logical, objectId: String(logical.objectId) };
      const operations: StorageOperation[] = ['read'];
      const storage = await createAuthorizedLocalStorage(
        binding,
        { location: scopedLocation, operations },
        {
          async checkCurrent() {
            return true;
          },
        },
      );
      scopedLocation.objectId = 'other';
      operations.push('create');
      await assert.rejects(
        storage.write(logical, source([]), { size: 0 }),
        code('AUTHORITY_DENIED'),
      );
      await assert.rejects(storage.read(scopedLocation), code('AUTHORITY_DENIED'));
      const request = { ...logical, objectId: String(logical.objectId) };
      const range = { offset: 1, length: 1 };
      const reading = storage.read(request, { range });
      request.objectId = 'other';
      range.offset = 99;
      const reader = (await reading).getReader();
      assert.deepEqual(Array.from((await reader.read()).value!), [2]);
      await reader.cancel();
      reader.releaseLock();
    }),
);
