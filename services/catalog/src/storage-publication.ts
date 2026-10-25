import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { resolve } from 'node:path';
import type { Publication } from './catalog.js';
import { StorageError } from '../../storage/src/locations.js';
import { cancelled, integer } from '../../storage/src/transfer.js';

export interface MetadataPublication {
  readonly execution: string;
  readonly worker: string;
  readonly fence: string;
  readonly outputs: readonly {
    dataset: string;
    expectedRevision: string;
    snapshotId: string;
    size: number;
    stream: ReadableStream<Uint8Array>;
  }[];
  readonly lineage: Publication['lineage'];
  readonly checkpoint?: Publication['checkpoint'];
}

export const metadataLimit = 1024 * 1024;

/** Bound both allocation and intake; a stalled trusted engine can be cancelled. */
export async function metadataBytes(
  stream: ReadableStream<Uint8Array>,
  size: number,
  check: () => Promise<void>,
  signal?: AbortSignal,
  limit = metadataLimit,
  frames = Number.POSITIVE_INFINITY,
  frameBytes = limit,
): Promise<Buffer> {
  integer(size, 1, limit);
  const bytes = Buffer.alloc(size);
  const reader = stream.getReader();
  const abort = () => {
    void reader.cancel().catch(() => {});
  };
  signal?.addEventListener('abort', abort, { once: true });
  let offset = 0;
  let count = 0;
  try {
    while (true) {
      cancelled(signal);
      await check();
      const item = await reader.read();
      cancelled(signal);
      await check();
      if (item.done) {
        break;
      }
      if (
        ++count > frames ||
        !(item.value instanceof Uint8Array) ||
        item.value.length > frameBytes ||
        item.value.length > size - offset
      ) {
        throw new StorageError('INVALID_INPUT');
      }
      bytes.set(item.value, offset);
      offset += item.value.length;
    }
    if (offset !== size) {
      throw new StorageError('INVALID_INPUT');
    }
    return bytes;
  } finally {
    signal?.removeEventListener('abort', abort);
    void reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

/** Reuse pinned PyIceberg's metadata model, without opening any referenced file. */
export async function validateMetadata(
  bytes: Buffer,
  tableUuid: string,
  snapshotId: string,
  signal?: AbortSignal,
): Promise<void> {
  cancelled(signal);
  const program = `
import sys
from pyiceberg.table.metadata import TableMetadataUtil
try:
    metadata = TableMetadataUtil.parse_raw(sys.stdin.buffer.read())
    snapshot = metadata.current_snapshot()
    if metadata.format_version != 2 or str(metadata.table_uuid) != sys.argv[1]:
        raise ValueError()
    if snapshot is None or str(snapshot.snapshot_id) != sys.argv[2]:
        raise ValueError()
except Exception:
    sys.exit(1)
`;
  await new Promise<void>((done, reject) => {
    const child = spawn(resolve('.venv/bin/python'), ['-c', program, tableUuid, snapshotId], {
      stdio: ['pipe', 'ignore', 'ignore'],
    });
    const abort = () => child.kill();
    const timeout = setTimeout(abort, 30000);
    signal?.addEventListener('abort', abort, { once: true });
    if (signal?.aborted) {
      abort();
    }
    child.stdin.on('error', () => {});
    child.on('error', () => {
      clearTimeout(timeout);
      signal?.removeEventListener('abort', abort);
      reject(new StorageError('IO_ERROR'));
    });
    child.on('close', (code) => {
      clearTimeout(timeout);
      signal?.removeEventListener('abort', abort);
      if (signal?.aborted) {
        reject(new StorageError('CANCELLED'));
      } else if (code !== 0) {
        reject(new StorageError('INVALID_INPUT'));
      } else {
        done();
      }
    });
    child.stdin.end(bytes);
  });
}

export function metadataObjectId(
  environment: string,
  execution: string,
  dataset: string,
  bytes: Buffer,
) {
  return createHash('sha256')
    .update(JSON.stringify([environment, execution, dataset]))
    .update(bytes)
    .digest('hex');
}

export function metadataStream(bytes: Buffer): ReadableStream<Uint8Array> {
  let offset = 0;
  return new ReadableStream<Uint8Array>(
    {
      pull(controller) {
        if (offset === bytes.length) {
          controller.close();
          return;
        }
        const end = Math.min(offset + 256, bytes.length);
        controller.enqueue(bytes.subarray(offset, end));
        offset = end;
      },
    },
    { highWaterMark: 0 },
  );
}
