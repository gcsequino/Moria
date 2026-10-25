import { createHash } from 'node:crypto';
import { scopedObjects, type ScopedObject } from '../../../workers/iceberg/scoped-read.js';
import { location, StorageError, type StorageLocation } from '../../storage/src/locations.js';
import { cancelled } from '../../storage/src/transfer.js';

export const inventoryLimit = 256 * 1024;

export function inventoryLocation(metadata: StorageLocation): StorageLocation {
  return location({
    ...metadata,
    objectId: createHash('sha256')
      .update('moria-reference-inventory-v1\0')
      .update(JSON.stringify(metadata))
      .digest('hex'),
  });
}

export function inventoryBytes(objects: readonly ScopedObject[]): Buffer {
  const bytes = Buffer.from(JSON.stringify(objects));
  if (bytes.length > inventoryLimit) throw new StorageError('LIMIT_EXCEEDED');
  return bytes;
}

export function snapshotIdentity(metadata: Buffer, inventory: Buffer): Buffer {
  return Buffer.concat([Buffer.from('moria-complete-snapshot-v1\0'), metadata, inventory]);
}

export function snapshotObjectId(scope: readonly string[], name: string, bytes: Buffer): string {
  return createHash('sha256')
    .update('moria-snapshot-object-v1\0')
    .update(JSON.stringify(scope))
    .update('\0')
    .update(name)
    .update('\0')
    .update(bytes)
    .digest('hex');
}

export function parseInventory(bytes: Buffer, metadata: StorageLocation): readonly ScopedObject[] {
  try {
    const objects = scopedObjects(
      JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)),
      metadata.warehouseId,
      metadata.tableId,
    );
    const sorted = [...objects].sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    // Reconstruction rejects duplicate/extra keys, alternate encodings and ordering.
    if (!inventoryBytes(sorted).equals(bytes)) throw new StorageError('INVALID_INPUT');
    return Object.freeze(sorted);
  } catch (error) {
    if (error instanceof StorageError) throw error;
    throw new StorageError('INVALID_INPUT');
  }
}

/** Unknown-size provider intake: bound bytes, frames and allocations before parsing. */
export async function boundedBytes(
  stream: ReadableStream<Uint8Array>,
  limit: number,
  check: () => Promise<void>,
  signal: AbortSignal,
): Promise<Buffer> {
  const reader = stream.getReader();
  const abort = () => {
    void reader.cancel().catch(() => {});
  };
  signal.addEventListener('abort', abort, { once: true });
  if (signal.aborted) abort();
  const chunks: Buffer[] = [];
  let size = 0;
  let frames = 0;
  try {
    while (true) {
      cancelled(signal);
      await check();
      const item = await reader.read();
      cancelled(signal);
      await check();
      if (item.done) break;
      if (
        !(item.value instanceof Uint8Array) ||
        ++frames > 65536 ||
        item.value.length > 256 * 1024 ||
        item.value.length > limit - size
      )
        throw new StorageError('LIMIT_EXCEEDED');
      size += item.value.length;
      chunks.push(Buffer.from(item.value));
    }
    if (!size) throw new StorageError('INVALID_INPUT');
    return Buffer.concat(chunks, size);
  } finally {
    signal.removeEventListener('abort', abort);
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}
