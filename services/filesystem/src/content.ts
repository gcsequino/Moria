import { createHash } from 'node:crypto';
import { MoriaError } from '../../../packages/contracts/types.js';
import type { FileContent } from '../../../packages/contracts/filesystem.js';
import type {
  StorageAuthorityScope,
  TrustedStorageAuthority,
} from '../../storage/src/authority.js';
import type { StreamingStorage } from '../../storage/src/locations.js';

export interface FilesystemStorageBinding {
  warehouseId: string;
  tableId: string;
  maxFileBytes: number;
  collectStaging?(): Promise<number>;
  storage(
    scope: StorageAuthorityScope,
    authority: TrustedStorageAuthority,
  ): Promise<StreamingStorage>;
}

/** Integrity is checked before a writer observes EOF and can finalize its candidate. */
export function verifiedContent(
  source: ReadableStream<Uint8Array>,
  content: Pick<FileContent, 'digest' | 'size'>,
  signal?: AbortSignal,
  failed?: (error: unknown) => void,
): ReadableStream<Uint8Array> {
  const reader = source.getReader(),
    hash = createHash('sha256');
  let size = 0,
    closed = false;
  function release() {
    if (!closed) {
      closed = true;
      signal?.removeEventListener('abort', abort);
      reader.releaseLock();
    }
  }
  const abort = () => {
    void reader.cancel(signal?.reason).catch(() => {});
  };
  signal?.addEventListener('abort', abort, { once: true });
  return new ReadableStream(
    {
      async pull(controller) {
        try {
          if (signal?.aborted) throw new MoriaError('CANCELLED', 'Transfer cancelled');
          const chunk = await reader.read();
          if (signal?.aborted) throw new MoriaError('CANCELLED', 'Transfer cancelled');
          if (chunk.done) {
            if (size !== content.size || `sha256:${hash.digest('hex')}` !== content.digest)
              throw new MoriaError('CONTENT_MISMATCH', 'Content digest or byte count differs');
            release();
            controller.close();
            return;
          }
          if (
            !(chunk.value instanceof Uint8Array) ||
            chunk.value.byteLength > 1048576 ||
            size + chunk.value.byteLength > content.size
          )
            throw new MoriaError('CONTENT_MISMATCH', 'Invalid content size or chunk');
          // Producers may reuse their buffers while storage rechecks authority.
          const bytes = new Uint8Array(chunk.value);
          size += bytes.byteLength;
          hash.update(bytes);
          controller.enqueue(bytes);
        } catch (error) {
          failed?.(error);
          void reader.cancel(error).catch(() => {});
          release();
          controller.error(error);
        }
      },
      cancel(reason) {
        void reader.cancel(reason).catch(() => {});
        release();
      },
    },
    { highWaterMark: 0 },
  );
}
