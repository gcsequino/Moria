import { integer, transferSignal, cancelled } from './transfer.js';
import { constants } from 'node:fs';
import { lstat, realpath, mkdir, open, link, unlink } from 'node:fs/promises';
import { isAbsolute, join, parse, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { setImmediate } from 'node:timers/promises';
import {
  identity,
  location,
  StorageError,
  type StorageLocation,
  type StreamingStorage,
  type ReadOptions,
  type WriteOptions,
} from './locations.js';

/** Only trusted service configuration may construct a binding. Never expose this factory to users. */
export interface LocalStorageBinding {
  readonly root: string;
  readonly warehouseId: string;
  readonly tableId: string;
  readonly maxObjectBytes: number;
  readonly chunkBytes: number;
}

function failure(error: unknown): StorageError {
  if (error instanceof StorageError) {
    return error;
  }
  const code = (error as NodeJS.ErrnoException)?.code;
  if (code === 'ENOENT') {
    return new StorageError('NOT_FOUND');
  }
  if (code === 'EEXIST') {
    return new StorageError('ALREADY_EXISTS');
  }
  if (code === 'ELOOP') {
    return new StorageError('UNSAFE_PATH');
  }
  return new StorageError('IO_ERROR');
}

/** Reject symlinks in every component. Concurrent host namespace mutation is outside this boundary. */
async function directory(path: string): Promise<void> {
  let current = parse(path).root;
  for (const component of path.slice(current.length).split('/').filter(Boolean)) {
    current = join(current, component);
    const stat = await lstat(current);
    if (!stat.isDirectory() || stat.isSymbolicLink()) {
      throw new StorageError('UNSAFE_PATH');
    }
  }
  if ((await realpath(path)) !== path) {
    throw new StorageError('UNSAFE_PATH');
  }
}

/** A failed or unsupported directory fsync is a durability failure, never a fallback. */
async function syncDirectory(path: string): Promise<void> {
  const handle = await open(
    path,
    constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW,
  );
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

export async function createLocalStorage(
  configured: LocalStorageBinding,
  /** Internal trusted-service hook; never supplied by request options. */
  verify?: (
    location: StorageLocation,
    operation: 'read' | 'create',
    signal?: AbortSignal,
  ) => Promise<void>,
): Promise<StreamingStorage> {
  const binding = { ...configured };
  identity(binding.warehouseId);
  identity(binding.tableId);
  integer(binding.maxObjectBytes, 0, Number.MAX_SAFE_INTEGER);
  integer(binding.chunkBytes, 1, 1024 * 1024);
  if (
    typeof binding.root !== 'string' ||
    !isAbsolute(binding.root) ||
    resolve(binding.root) !== binding.root
  ) {
    throw new StorageError('INVALID_INPUT');
  }
  const root = binding.root;
  const table = join(root, binding.warehouseId, binding.tableId);
  const objects = join(table, 'objects');
  const staging = join(table, 'staging');
  try {
    await directory(root);
    for (const path of [join(root, binding.warehouseId), table, objects, staging]) {
      try {
        await mkdir(path, { mode: 0o700 });
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'EEXIST') {
          throw error;
        }
      }
      await directory(path);
    }
    // Include existing entries: another initializer may have created them without
    // reaching its own sync. The preexisting root's parent is the host's responsibility.
    for (const path of [objects, staging, table, join(root, binding.warehouseId), root]) {
      await syncDirectory(path);
    }
  } catch (error) {
    throw failure(error);
  }

  function target(input: StorageLocation): string {
    const logical = location(input);
    if (logical.warehouseId !== binding.warehouseId || logical.tableId !== binding.tableId) {
      throw new StorageError('BINDING_MISMATCH');
    }
    return join(objects, logical.objectId);
  }

  return {
    version: 1,
    requireCapability(capability) {
      if (!['read', 'range', 'create'].includes(capability)) {
        throw new StorageError('UNSUPPORTED_CAPABILITY');
      }
    },
    async read(input, options: ReadOptions = {}) {
      const logical = location(input);
      const path = target(logical);
      const signal = transferSignal(options);
      cancelled(signal);
      const suppliedRange = options.range;
      if (
        suppliedRange !== undefined &&
        (suppliedRange === null ||
          typeof suppliedRange !== 'object' ||
          Array.isArray(suppliedRange))
      ) {
        throw new StorageError('INVALID_INPUT');
      }
      const range = suppliedRange === undefined ? undefined : { ...suppliedRange };
      if (range) {
        integer(range.offset, 0, binding.maxObjectBytes);
        integer(range.length, 0, binding.maxObjectBytes);
      }
      await verify?.(logical, 'read', signal);
      cancelled(signal);
      try {
        await directory(objects);
        const file = await open(
          path,
          constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
        );
        let closed = false;
        const close = async () => {
          if (!closed) {
            closed = true;
            if (signal) {
              EventTarget.prototype.removeEventListener.call(signal, 'abort', abort);
            }
            await file.close();
          }
        };
        let controller: ReadableStreamDefaultController<Uint8Array>;
        const abort = () => {
          controller.error(new StorageError('CANCELLED'));
          void close().catch(() => {});
        };
        try {
          const stat = await file.stat();
          if (!stat.isFile() || stat.nlink !== 1) {
            throw new StorageError('UNSAFE_PATH');
          }
          if (stat.size > binding.maxObjectBytes) {
            throw new StorageError('LIMIT_EXCEEDED');
          }
          let offset = range?.offset ?? 0;
          const length = range?.length ?? stat.size;
          if (offset > stat.size || length > stat.size - offset) {
            throw new StorageError('INVALID_INPUT');
          }
          const end = offset + length;
          cancelled(signal);
          return new ReadableStream<Uint8Array>(
            {
              start(value) {
                controller = value;
                signal?.addEventListener('abort', abort, { once: true });
              },
              async pull(value) {
                try {
                  await verify?.(logical, 'read', signal);
                  cancelled(signal);
                  if (offset === end) {
                    await close();
                    value.close();
                    return;
                  }
                  const buffer = Buffer.alloc(Math.min(binding.chunkBytes, end - offset));
                  const { bytesRead } = await file.read(buffer, 0, buffer.length, offset);
                  await verify?.(logical, 'read', signal);
                  cancelled(signal);
                  if (bytesRead === 0) {
                    throw new StorageError('IO_ERROR');
                  }
                  offset += bytesRead;
                  value.enqueue(buffer.subarray(0, bytesRead));
                } catch (error) {
                  await close();
                  value.error(failure(error));
                }
              },
              async cancel() {
                await close();
              },
            },
            { highWaterMark: 0 },
          );
        } catch (error) {
          await close();
          throw error;
        }
      } catch (error) {
        throw failure(error);
      }
    },
    async write(input, source, options: WriteOptions) {
      const logical = location(input);
      const path = target(logical);
      const signal = transferSignal(options);
      const size = options.size;
      integer(size, 0, binding.maxObjectBytes);
      cancelled(signal);
      if (!(source instanceof ReadableStream) || source.locked) {
        throw new StorageError('INVALID_INPUT');
      }
      await verify?.(logical, 'create', signal);
      cancelled(signal);
      let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
      const temporary = join(staging, randomUUID());
      let file;
      let created = false;
      let finalized = false;
      let writeFailure: StorageError | undefined;
      let total = 0;
      const abort = () => {
        void reader?.cancel(new StorageError('CANCELLED')).catch(() => {});
      };
      try {
        reader = source.getReader();
        signal?.addEventListener('abort', abort, { once: true });
        await directory(staging);
        file = await open(
          temporary,
          constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,
          0o600,
        );
        created = true;
        while (true) {
          await verify?.(logical, 'create', signal);
          cancelled(signal);
          const part = await reader.read();
          await verify?.(logical, 'create', signal);
          cancelled(signal);
          if (part.done) {
            break;
          }
          if (!(part.value instanceof Uint8Array)) {
            throw new StorageError('INVALID_INPUT');
          }
          if (part.value.byteLength === 0) {
            // Empty chunks perform no filesystem I/O; yield so deadline timers can fire.
            await setImmediate();
            continue;
          }
          if (part.value.byteLength > binding.chunkBytes || part.value.byteLength > size - total) {
            throw new StorageError('LIMIT_EXCEEDED');
          }
          let written = 0;
          while (written < part.value.byteLength) {
            cancelled(signal);
            const result = await file.write(part.value, written, part.value.byteLength - written);
            if (result.bytesWritten === 0) {
              throw new StorageError('IO_ERROR');
            }
            written += result.bytesWritten;
          }
          total += written;
        }
        if (total !== size) {
          throw new StorageError('INVALID_INPUT');
        }
        await file.sync();
        await file.close();
        file = undefined;
        await directory(staging);
        await directory(objects);
        await verify?.(logical, 'create', signal);
        cancelled(signal);
        // Atomic no-clobber finalization. Cancellation after this point may lose to success.
        await link(temporary, path);
        finalized = true;
        await syncDirectory(objects);
        await unlink(temporary);
        created = false;
        // Persist removal of the second link before acknowledging a readable object.
        await syncDirectory(staging);
      } catch (error) {
        void reader?.cancel().catch(() => {});
        writeFailure = failure(error);
      } finally {
        try {
          if (signal) {
            EventTarget.prototype.removeEventListener.call(signal, 'abort', abort);
          }
        } finally {
          reader?.releaseLock();
        }
        try {
          await file?.close();
        } catch (error) {
          writeFailure ??= failure(error);
        } finally {
          if (created) {
            try {
              await unlink(temporary);
            } catch (error) {
              writeFailure ??= failure(error);
            }
          }
        }
      }
      if (writeFailure) {
        throw new StorageError(writeFailure.code, finalized ? 'uncertain' : undefined);
      }
    },
  };
}
