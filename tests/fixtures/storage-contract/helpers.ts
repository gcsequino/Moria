import assert from 'node:assert/strict';
import { StorageError, type StreamingStorage } from '../../../services/storage/src/locations.js';
export const logical = {
  version: 1 as const,
  warehouseId: 'warehouse',
  tableId: 'table',
  objectId: 'object',
};
export const limits = {
  warehouseId: 'warehouse',
  tableId: 'table',
  maxObjectBytes: 64,
  chunkBytes: 4,
};
export function source(chunks: number[][]) {
  return new ReadableStream<Uint8Array>(
    {
      pull(controller) {
        const value = chunks.shift();
        if (value) {
          controller.enqueue(Uint8Array.from(value));
        } else controller.close();
      },
    },
    { highWaterMark: 0 },
  );
}
export function code(expected: string, uncertain = false) {
  return (error: unknown) => {
    assert.ok(error instanceof StorageError);
    assert.equal(error.code, expected);
    assert.equal(error.message, expected);
    assert.equal(error.cause, undefined);
    assert.equal(error.writeOutcome, uncertain ? 'uncertain' : undefined);
    return true;
  };
}
export async function collect(storage: StreamingStorage, object = logical) {
  const chunks = [];
  for await (const chunk of await storage.read(object)) {
    assert.ok(chunk.length <= 4);
    chunks.push(...chunk);
  }
  return chunks;
}
