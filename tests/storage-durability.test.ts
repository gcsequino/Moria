import assert from 'node:assert/strict';
import { test } from 'node:test';
import { execFile, fork } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdtemp, readdir, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createLocalStorage } from '../services/storage/src/local.js';

const childPath = new URL('./fixtures/storage-durability/child.ts', import.meta.url);
const logical = {
  version: 1 as const,
  warehouseId: 'warehouse',
  tableId: 'table',
  objectId: 'object',
};
const bytes = Buffer.from([1, 2, 3, 4, 5, 6, 7, 8]);
function binding(root: string) {
  return { root, warehouseId: 'warehouse', tableId: 'table', maxObjectBytes: 64, chunkBytes: 4 };
}
function source() {
  return new ReadableStream<Uint8Array>({
    start(value) {
      value.enqueue(Buffer.from([9]));
      value.close();
    },
  });
}
async function fixture(run: (root: string) => Promise<void>) {
  const root = await mkdtemp(join(tmpdir(), 'storage-durability-'));
  try {
    await run(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}
interface Result {
  code: string;
  writeOutcome?: string;
  events: string[];
  triggered: boolean;
  openHandles: number;
  locked: boolean;
}
async function run(root: string, mode: string, point = '', errno = 'EIO'): Promise<Result> {
  const { stdout } = await promisify(execFile)(
    process.execPath,
    ['--import', 'tsx', childPath.pathname, root, mode, point, errno],
    { timeout: 10000, killSignal: 'SIGKILL' },
  );
  const result = JSON.parse(stdout) as Result;
  assert.equal(result.openHandles, 0);
  assert.equal(result.locked, false);
  if (point) {
    assert.equal(result.triggered, true, point);
  }
  return result;
}
async function killAt(root: string, point: string): Promise<void> {
  const child = fork(childPath, [root, 'kill', point], {
    execArgv: ['--import', 'tsx'],
    stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  });
  let reached = false;
  let stderr = '';
  child.stderr?.on('data', (chunk) => {
    stderr += String(chunk);
  });
  const timer = setTimeout(() => child.kill('SIGKILL'), 10000);
  try {
    await new Promise<void>((resolve, reject) => {
      child.once('error', reject);
      child.on('message', (message) => {
        assert.deepEqual(message, { event: point });
        reached = true;
        child.kill('SIGKILL');
      });
      child.once('exit', (_code, signal) => {
        if (reached && signal === 'SIGKILL') {
          resolve();
        } else {
          reject(new Error(`Child did not reach ${point}: ${stderr}`));
        }
      });
    });
  } finally {
    clearTimeout(timer);
    child.kill('SIGKILL');
  }
}
async function collect(root: string): Promise<Buffer> {
  const storage = await createLocalStorage(binding(root));
  const chunks: Uint8Array[] = [];
  for await (const chunk of await storage.read(logical)) {
    assert.ok(chunk.length <= 4);
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

test(
  'acknowledgment follows file and binding directory durability barriers',
  { timeout: 15000 },
  async () => {
    await fixture(async (root) => {
      const result = await run(root, 'observe');
      assert.equal(result.code, 'OK');
      const barriers = result.events.filter(
        (event) => event.endsWith('sync:after') || event === 'ack',
      );
      assert.deepEqual(barriers, [
        'init:warehouse/table/objects:sync:after',
        'init:warehouse/table/staging:sync:after',
        'init:warehouse/table:sync:after',
        'init:warehouse:sync:after',
        'init:.:sync:after',
        'write:file:sync:after',
        'write:warehouse/table/objects:sync:after',
        'write:warehouse/table/staging:sync:after',
        'ack',
      ]);
      assert.ok(
        result.events.indexOf('write:file:close:after') <
          result.events.indexOf('write:link:before'),
      );
      assert.ok(
        result.events.indexOf('write:warehouse/table/objects:close:after') <
          result.events.indexOf('write:unlink:before'),
      );
      assert.ok(
        result.events.indexOf('write:unlink:after') <
          result.events.indexOf('write:warehouse/table/staging:sync:before'),
      );
      assert.deepEqual(await collect(root), bytes);
      assert.deepEqual(await readdir(join(root, 'warehouse/table/staging')), []);
      // Existing directories are synchronized too, including after interrupted initialization.
      const again = await run(root, 'observe');
      assert.equal(again.code, 'ALREADY_EXISTS');
      assert.deepEqual(
        again.events.filter((event) => event.startsWith('init:')),
        result.events.filter((event) => event.startsWith('init:')),
      );
      assert.deepEqual(await collect(root), bytes);
    });
  },
);

for (const directory of [
  'warehouse/table/objects',
  'warehouse/table/staging',
  'warehouse/table',
  'warehouse',
  '.',
]) {
  for (const errno of ['EIO', 'EINVAL']) {
    test(
      `initialization fails explicitly on ${directory} sync ${errno}`,
      { timeout: 15000 },
      async () => {
        await fixture(async (root) => {
          const result = await run(root, 'fault', `init:${directory}:sync:before`, errno);
          assert.equal(result.code, 'IO_ERROR');
          assert.equal(result.writeOutcome, undefined);
          assert.ok(!result.events.includes('write:link:before'));
          assert.deepEqual(await readdir(join(root, 'warehouse/table/objects')), []);
          assert.equal((await run(root, 'observe')).code, 'OK');
          assert.deepEqual(await collect(root), bytes);
        });
      },
    );
  }
}

for (const point of [
  'write:file:sync:before',
  'write:link:before',
  'write:warehouse/table/objects:sync:before',
  'write:warehouse/table/staging:sync:before',
  'write:unlink:before',
  'write:file:close:after',
  'write:warehouse/table/objects:close:after',
]) {
  test(
    `write fault at ${point} closes handles and preserves finalization outcome`,
    { timeout: 15000 },
    async () => {
      await fixture(async (root) => {
        const uncertain = point.includes('warehouse/table/') || point.includes('unlink');
        const result = await run(root, 'fault', point);
        assert.equal(result.code, 'IO_ERROR');
        assert.equal(result.writeOutcome, uncertain ? 'uncertain' : undefined);
        assert.ok(!result.events.includes('ack'));
        const staging = join(root, 'warehouse/table/staging');
        const final = join(root, 'warehouse/table/objects/object');
        assert.equal((await readdir(staging)).length, point.includes('unlink') ? 1 : 0);
        if (uncertain) {
          assert.deepEqual(await readFile(final), bytes);
          const storage = await createLocalStorage(binding(root));
          await assert.rejects(storage.write(logical, source(), { size: 1 }), {
            code: 'ALREADY_EXISTS',
          });
          if (point.includes('unlink')) {
            await assert.rejects(storage.read(logical), { code: 'UNSAFE_PATH' });
            assert.equal(
              result.events.filter((event) => event === 'write:unlink:before').length,
              2,
            );
          } else {
            assert.deepEqual(await collect(root), bytes);
          }
        } else {
          await assert.rejects(stat(final), { code: 'ENOENT' });
        }
      });
    },
  );
}

for (const point of [
  'write:file:sync:after',
  'write:link:after',
  'write:warehouse/table/objects:sync:after',
  'write:warehouse/table/staging:sync:after',
]) {
  test(`cancellation at ${point} respects finalization`, { timeout: 15000 }, async () => {
    await fixture(async (root) => {
      const result = await run(root, 'cancel', point);
      assert.equal(result.code, point === 'write:file:sync:after' ? 'CANCELLED' : 'OK');
      assert.equal(result.writeOutcome, undefined);
      assert.deepEqual(await readdir(join(root, 'warehouse/table/staging')), []);
      if (result.code === 'OK') {
        assert.deepEqual(await collect(root), bytes);
      } else {
        assert.deepEqual(await readdir(join(root, 'warehouse/table/objects')), []);
      }
    });
  });
}

for (const point of [
  'write:file:write:after',
  'write:link:before',
  'write:link:after',
  'write:warehouse/table/objects:sync:after',
  'write:unlink:after',
  'write:warehouse/table/staging:close:after',
  'ack',
]) {
  test(
    `SIGKILL at ${point} preserves restart identity and never clobbers`,
    { timeout: 15000 },
    async () => {
      await fixture(async (root) => {
        await killAt(root, point);
        const table = join(root, 'warehouse/table');
        const beforeFinal = point === 'write:file:write:after' || point === 'write:link:before';
        const twoLinks =
          point === 'write:link:after' || point === 'write:warehouse/table/objects:sync:after';
        const stagingBefore = await readdir(join(table, 'staging'));
        assert.equal(stagingBefore.length, beforeFinal || twoLinks ? 1 : 0);
        // An unknown object must survive initialization, reads, and failed/successful writes.
        await writeFile(join(table, 'staging/unknown'), 'unknown');
        const storage = await createLocalStorage(binding(root));
        await assert.rejects(storage.read({ ...logical, tableId: 'other' }), {
          code: 'BINDING_MISMATCH',
        });
        if (beforeFinal) {
          await assert.rejects(storage.read(logical), { code: 'NOT_FOUND' });
          await storage.write(logical, source(), { size: 1 });
          assert.deepEqual(await collect(root), Buffer.from([9]));
        } else {
          assert.deepEqual(await readFile(join(table, 'objects/object')), bytes);
          await assert.rejects(storage.write(logical, source(), { size: 1 }), {
            code: 'ALREADY_EXISTS',
          });
          if (twoLinks) {
            assert.equal((await stat(join(table, 'objects/object'))).nlink, 2);
            await assert.rejects(storage.read(logical), { code: 'UNSAFE_PATH' });
          } else {
            assert.deepEqual(await collect(root), bytes);
            const range = await storage.read(logical, { range: { offset: 3, length: 2 } });
            assert.deepEqual(
              Buffer.from(await new Response(range).arrayBuffer()),
              bytes.subarray(3, 5),
            );
          }
        }
        assert.deepEqual(
          await readdir(join(table, 'staging')),
          [...stagingBefore, 'unknown'].sort(),
        );
        assert.equal(await readFile(join(table, 'staging/unknown'), 'utf8'), 'unknown');
      });
    },
  );
}
