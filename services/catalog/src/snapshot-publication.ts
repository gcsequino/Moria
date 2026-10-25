import { spawn } from 'node:child_process';
import { resolve } from 'node:path';
import { StorageError } from '../../storage/src/locations.js';
import { cancelled } from '../../storage/src/transfer.js';
import { scopedObjects, type ScopedObject } from '../../../workers/iceberg/scoped-read.js';
import { metadataBytes, type MetadataPublication } from './storage-publication.js';
import { inventoryBytes, snapshotIdentity, snapshotObjectId } from './reference-inventory.js';

export interface SnapshotObject {
  readonly name: string;
  readonly size: number;
  readonly stream: ReadableStream<Uint8Array>;
}

export interface SnapshotPublication extends Omit<MetadataPublication, 'outputs'> {
  readonly outputs: readonly (MetadataPublication['outputs'][number] & {
    readonly objects: readonly SnapshotObject[];
  })[];
}

export const snapshotBytesLimit = 64 * 1024 * 1024;
export const snapshotObjectLimit = 16 * 1024 * 1024;

/** Capture only declarations and stream handles, before the first asynchronous boundary. */
export function snapshotObjects(objects: readonly SnapshotObject[]): readonly SnapshotObject[] {
  if (!Array.isArray(objects) || objects.length > 128) {
    throw new StorageError('LIMIT_EXCEEDED');
  }
  const names = new Set<string>();
  return Object.freeze(
    objects.map((object) => {
      if (Object.keys(object).sort().join(',') !== 'name,size,stream' || names.has(object.name)) {
        throw new StorageError('INVALID_INPUT');
      }
      names.add(object.name);
      return Object.freeze({ name: object.name, size: object.size, stream: object.stream });
    }),
  );
}

export async function prepareSnapshot(
  metadata: Buffer,
  objects: readonly SnapshotObject[],
  binding: { warehouseId: string; tableId: string; icebergTableUuid?: string },
  snapshotId: string,
  identityScope: readonly string[],
  check: () => Promise<void>,
  signal: AbortSignal,
) {
  // Validate names and sizes before intake. Object identities are service-owned.
  const declarations = scopedObjects(
    objects.map((object) => ({
      name: object.name,
      size: object.size,
      location: {
        version: 1,
        warehouseId: binding.warehouseId,
        tableId: binding.tableId,
        objectId: 'pending',
      },
    })),
    binding.warehouseId,
    binding.tableId,
  );
  const staged: { reference: ScopedObject; bytes: Buffer }[] = [];
  for (const [index, object] of objects.entries()) {
    const bytes = await metadataBytes(
      object.stream,
      object.size,
      check,
      signal,
      snapshotObjectLimit,
      65536,
      256 * 1024,
    );
    const objectId = snapshotObjectId(identityScope, object.name, bytes);
    staged.push({
      bytes,
      reference: Object.freeze({
        ...declarations[index],
        location: Object.freeze({ ...declarations[index].location, objectId }),
      }),
    });
  }
  staged.sort((a, b) =>
    a.reference.name < b.reference.name ? -1 : a.reference.name > b.reference.name ? 1 : 0,
  );
  await validateSnapshot(
    metadata,
    staged,
    binding.icebergTableUuid!,
    snapshotId,
    `moria://${binding.warehouseId}/${binding.tableId}`,
    signal,
  );
  await check();
  // Domain-separated closure commitment enters the existing catalog request hash
  // through metadata_location. Metadata-only identity and APIs remain unchanged.
  const inventory = inventoryBytes(staged.map(({ reference }) => reference));
  return { staged, inventory, identityBytes: snapshotIdentity(metadata, inventory) };
}

export async function validateSnapshot(
  metadata: Buffer,
  objects: readonly { reference: ScopedObject; bytes: Buffer }[],
  tableUuid: string,
  snapshotId: string,
  location: string,
  signal: AbortSignal,
) {
  cancelled(signal);
  const child = spawn(
    resolve('.venv/bin/python'),
    ['-I', resolve('workers/iceberg/validate_snapshot.py')],
    {
      cwd: resolve('workers/iceberg'),
      env: {
        HOME: '/nonexistent',
        LANG: 'C.UTF-8',
        OPENBLAS_NUM_THREADS: '1',
        OMP_NUM_THREADS: '1',
        PYICEBERG_MAX_WORKERS: '1',
      },
      stdio: ['pipe', 'pipe', 'ignore'],
    },
  );
  const abort = () => {
    child.kill('SIGKILL');
  };
  signal.addEventListener('abort', abort, { once: true });
  if (signal.aborted) abort();
  child.stdin.on('error', () => {});
  let diagnostics = '';
  child.stdout.on('data', (chunk: Buffer) => {
    // Drain continuously, but retain only a bounded fixed-token protocol.
    diagnostics = (diagnostics + chunk.toString('ascii')).slice(0, 256);
  });
  let spawnFailed = false;
  let inputFailed = false;
  child.once('error', () => {
    spawnFailed = true;
  });
  const exit = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((done) => {
    child.once('close', (code, signal) => done({ code, signal }));
  });
  const failed = (result: Awaited<typeof exit>) => {
    const tokens = diagnostics.trim().split('\n');
    const phase =
      ['importing', 'receiving', 'validating', 'complete'].findLast((token) =>
        tokens.includes(token),
      ) ?? 'startup';
    const failure =
      ['memory', 'io', 'invalid', 'internal'].find((token) => tokens.includes(token)) ?? 'unknown';
    const error = new StorageError(
      failure === 'invalid' && result.code === 1 && !result.signal ? 'INVALID_INPUT' : 'IO_ERROR',
    );
    error.cause = { validator: { ...result, phase, failure, spawnFailed, inputFailed } };
    return error;
  };
  const send = async (frame: unknown) => {
    cancelled(signal);
    const line = JSON.stringify(frame) + '\n';
    if (Buffer.byteLength(line) > 2 * 1024 * 1024) throw new StorageError('LIMIT_EXCEEDED');
    await new Promise<void>((done, reject) =>
      child.stdin.write(line, (error) => {
        if (error) {
          inputFailed = true;
          reject(new StorageError('IO_ERROR'));
        } else {
          done();
        }
      }),
    );
  };
  try {
    await send({
      metadata: metadata.toString('base64'),
      tableUuid,
      snapshotId,
      location,
      objects: objects.map(({ reference }) => ({ name: reference.name, size: reference.size })),
    });
    for (const { bytes } of objects) {
      for (let offset = 0; offset < bytes.length; offset += 256 * 1024) {
        await send({ data: bytes.subarray(offset, offset + 256 * 1024).toString('base64') });
      }
    }
    child.stdin.end();
    const result = await exit;
    cancelled(signal);
    if (result.code !== 0 || diagnostics !== 'importing\nreceiving\nvalidating\ncomplete\n') {
      throw failed(result);
    }
  } catch (error) {
    if (!inputFailed) throw error;
    // EPIPE can precede close when the child fails during import or intake.
    // Preserve its actual termination instead of losing it to the write error.
    const result = await exit;
    cancelled(signal);
    throw failed(result);
  } finally {
    abort();
    await exit;
    signal.removeEventListener('abort', abort);
  }
}
