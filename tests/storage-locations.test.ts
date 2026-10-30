import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, rm, symlink, mkdir, writeFile, readFile, link } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createLocalStorage } from '../services/storage/src/local.js';
import { location, StorageError } from '../services/storage/src/locations.js';

const logical = {
  version: 1 as const,
  warehouseId: 'warehouse',
  tableId: 'table',
  objectId: 'object',
};
const code = (expected: string) => (error: unknown) =>
  error instanceof StorageError && error.code === expected;

test('logical locations reject paths, encodings, unknown versions and authority fields', () => {
  assert.deepEqual(location(logical), logical);
  for (const objectId of [
    '',
    '.',
    '..',
    '/etc/passwd',
    'a/b',
    'a\\b',
    '%2e%2e',
    '%252f',
    'a:b',
    'a\0b',
    'é',
    'a b',
    'a'.repeat(129),
  ]) {
    assert.throws(() => location({ ...logical, objectId }), code('INVALID_INPUT'));
  }
  for (const extra of [
    { version: 2 },
    { root: '/tmp' },
    { credentialReference: 'secret' },
    { tableId: '../table' },
  ]) {
    assert.throws(() => location({ ...logical, ...extra }), code('INVALID_INPUT'));
  }
});

test(
  'explicit binding scopes identities and rejects unsupported capabilities',
  { timeout: 5000 },
  async () => {
    const root = await mkdtemp(join(tmpdir(), 'storage-binding-'));
    try {
      const config = {
        root,
        warehouseId: 'warehouse',
        tableId: 'table',
        maxObjectBytes: 32,
        chunkBytes: 4,
      };
      const storage = await createLocalStorage(config);
      config.tableId = 'other';
      for (const changed of [{ warehouseId: 'other' }, { tableId: 'other' }]) {
        await assert.rejects(storage.read({ ...logical, ...changed }), code('BINDING_MISMATCH'));
        await assert.rejects(
          storage.write({ ...logical, ...changed }, new ReadableStream(), { size: 0 }),
          code('BINDING_MISMATCH'),
        );
      }
      storage.requireCapability('range');
      storage.requireCapability('delete');
      for (const capability of ['multipart', 'overwrite', 'catalog-publication']) {
        assert.throws(() => storage.requireCapability(capability), code('UNSUPPORTED_CAPABILITY'));
      }
      await assert.rejects(storage.read(logical), code('NOT_FOUND'));
      for (const changed of [{ root: 'relative' }, { chunkBytes: 0 }, { maxObjectBytes: -1 }]) {
        await assert.rejects(createLocalStorage({ ...config, ...changed }), code('INVALID_INPUT'));
      }
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  },
);

test('real filesystem symlink and hardlink escapes are rejected', { timeout: 5000 }, async () => {
  const base = await mkdtemp(join(tmpdir(), 'storage-path-'));
  try {
    const root = join(base, 'root');
    const outside = join(base, 'outside');
    await mkdir(root);
    await mkdir(outside);
    await writeFile(join(outside, 'sentinel'), 'untouched');
    const config = {
      root,
      warehouseId: 'warehouse',
      tableId: 'table',
      maxObjectBytes: 32,
      chunkBytes: 4,
    };
    await symlink(outside, join(root, 'warehouse'));
    await assert.rejects(createLocalStorage(config), code('UNSAFE_PATH'));
    await rm(join(root, 'warehouse'));
    await symlink(root, join(base, 'alias'));
    await assert.rejects(
      createLocalStorage({ ...config, root: join(base, 'alias') }),
      code('UNSAFE_PATH'),
    );
    const storage = await createLocalStorage(config);
    const objects = join(root, 'warehouse', 'table', 'objects');
    await symlink(join(outside, 'sentinel'), join(objects, 'object'));
    await assert.rejects(storage.read(logical), code('UNSAFE_PATH'));
    await rm(join(objects, 'object'));
    await link(join(outside, 'sentinel'), join(objects, 'object'));
    await assert.rejects(storage.read(logical), code('UNSAFE_PATH'));
    await rm(objects, { recursive: true });
    await symlink(outside, objects);
    await assert.rejects(storage.read(logical), code('UNSAFE_PATH'));
    await assert.rejects(
      storage.write(
        logical,
        new ReadableStream({
          start(c) {
            c.close();
          },
        }),
        { size: 0 },
      ),
      code('UNSAFE_PATH'),
    );
    assert.equal(await readFile(join(outside, 'sentinel'), 'utf8'), 'untouched');
  } finally {
    await rm(base, { recursive: true, force: true });
  }
});
