import { s3ClientDefaults } from './s3-client.js';
import { Agent as HttpAgent } from 'node:http';
import { Agent as HttpsAgent } from 'node:https';
import { setImmediate } from 'node:timers/promises';
import {
  S3Client,
  HeadObjectCommand,
  GetObjectCommand,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
} from '@aws-sdk/client-s3';
import {
  identity,
  location,
  StorageError,
  type StorageLocation,
  type StreamingStorage,
} from './locations.js';
import { cancelled, integer, transferSignal } from './transfer.js';

const partBytes = 5 * 1024 * 1024;

/** Trusted service configuration only. No SDK default credential/file discovery. */
export interface S3StorageBinding {
  readonly warehouseId: string;
  readonly tableId: string;
  readonly endpoint: string;
  readonly region: string;
  readonly bucket: string;
  readonly prefix: string;
  readonly credentials: {
    readonly accessKeyId: string;
    readonly secretAccessKey: string;
    readonly sessionToken?: string;
  };
  readonly maxObjectBytes: number;
  readonly chunkBytes: number;
}

type Verify = (
  location: StorageLocation,
  operation: 'read' | 'create',
  signal?: AbortSignal,
) => Promise<void>;

function status(error: unknown): number | undefined {
  return (error as { $metadata?: { httpStatusCode?: number } })?.$metadata?.httpStatusCode;
}

function failure(error: unknown, signal?: AbortSignal): StorageError {
  if (error instanceof StorageError) {
    return error;
  }
  if (signal?.aborted) {
    return new StorageError('CANCELLED');
  }
  if (status(error) === 404) {
    return new StorageError('NOT_FOUND');
  }
  if (status(error) === 412) {
    return new StorageError('ALREADY_EXISTS');
  }
  return new StorageError('IO_ERROR');
}

export async function createS3Storage(
  configured: S3StorageBinding,
  verify?: Verify,
): Promise<StreamingStorage> {
  const binding = { ...configured, credentials: { ...configured.credentials } };
  identity(binding.warehouseId);
  identity(binding.tableId);
  integer(binding.maxObjectBytes, 0, partBytes * 10000);
  integer(binding.chunkBytes, 1, 1024 * 1024);
  let endpoint: URL;
  try {
    endpoint = new URL(binding.endpoint);
    if (
      endpoint.username ||
      endpoint.password ||
      endpoint.search ||
      endpoint.hash ||
      endpoint.pathname !== '/' ||
      (endpoint.protocol !== 'https:' &&
        !(endpoint.protocol === 'http:' && ['127.0.0.1', '[::1]'].includes(endpoint.hostname))) ||
      typeof binding.bucket !== 'string' ||
      !/^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/.test(binding.bucket) ||
      typeof binding.prefix !== 'string' ||
      binding.prefix.length > 512 ||
      typeof binding.region !== 'string' ||
      !/^[a-z0-9-]{1,64}$/.test(binding.region)
    ) {
      throw new Error();
    }
    if (binding.prefix !== '') {
      for (const segment of binding.prefix.split('/')) {
        identity(segment);
      }
    }
    for (const value of [binding.credentials.accessKeyId, binding.credentials.secretAccessKey]) {
      if (typeof value !== 'string' || value.length === 0) {
        throw new Error();
      }
    }
    if (
      binding.credentials.sessionToken !== undefined &&
      typeof binding.credentials.sessionToken !== 'string'
    ) {
      throw new Error();
    }
  } catch {
    throw new StorageError('INVALID_INPUT');
  }

  // No retries: a lost completion acknowledgment must stay uncertain. Closing
  // connections also avoids reusing a peer-closed socket after a 412 response.
  const client = new S3Client({
    ...s3ClientDefaults,
    endpoint: endpoint.href,
    region: binding.region,
    credentials: binding.credentials,
    requestHandler: {
      httpAgent: new HttpAgent({ keepAlive: false }),
      httpsAgent: new HttpsAgent({ keepAlive: false }),
      connectionTimeout: 10000,
      requestTimeout: 30000,
      throwOnRequestTimeout: true,
    },
  });
  function target(input: StorageLocation) {
    const logical = location(input);
    if (logical.warehouseId !== binding.warehouseId || logical.tableId !== binding.tableId) {
      throw new StorageError('BINDING_MISMATCH');
    }
    return {
      Bucket: binding.bucket,
      Key: [binding.prefix, logical.warehouseId, logical.tableId, 'objects', logical.objectId]
        .filter(Boolean)
        .join('/'),
    };
  }
  async function check(
    logical: StorageLocation,
    operation: 'read' | 'create',
    signal?: AbortSignal,
  ) {
    cancelled(signal);
    await verify?.(logical, operation, signal);
    cancelled(signal);
  }
  return Object.freeze({
    version: 1 as const,
    requireCapability(capability: string) {
      if (!['read', 'range', 'create'].includes(capability)) {
        throw new StorageError('UNSUPPORTED_CAPABILITY');
      }
    },
    async read(input, options = {}) {
      const logical = location(input);
      const object = target(logical);
      const signal = transferSignal(options);
      cancelled(signal);
      const supplied = options.range;
      if (
        supplied !== undefined &&
        (supplied === null || typeof supplied !== 'object' || Array.isArray(supplied))
      ) {
        throw new StorageError('INVALID_INPUT');
      }
      const range = supplied === undefined ? undefined : { ...supplied };
      if (range) {
        integer(range.offset, 0, binding.maxObjectBytes);
        integer(range.length, 0, binding.maxObjectBytes);
      }
      await check(logical, 'read', signal);
      try {
        const head = await client.send(new HeadObjectCommand(object), { abortSignal: signal });
        const size = head.ContentLength;
        if (!Number.isSafeInteger(size) || size! < 0 || !head.ETag) {
          throw new StorageError('IO_ERROR');
        }
        if (size! > binding.maxObjectBytes) {
          throw new StorageError('LIMIT_EXCEEDED');
        }
        let offset = range?.offset ?? 0;
        const length = range?.length ?? size!;
        if (offset > size! || length > size! - offset) {
          throw new StorageError('INVALID_INPUT');
        }
        const end = offset + length;
        const stop = new AbortController();
        const transfer = signal ? AbortSignal.any([signal, stop.signal]) : stop.signal;
        let controller: ReadableStreamDefaultController<Uint8Array>;
        let closed = false;
        const cleanup = () => {
          closed = true;
          if (signal) {
            EventTarget.prototype.removeEventListener.call(signal, 'abort', abort);
          }
        };
        const abort = () => {
          if (!closed) {
            cleanup();
            controller.error(new StorageError('CANCELLED'));
            stop.abort();
          }
        };
        return new ReadableStream<Uint8Array>(
          {
            start(value) {
              controller = value;
              if (signal) {
                EventTarget.prototype.addEventListener.call(signal, 'abort', abort, { once: true });
              }
              if (signal?.aborted) {
                abort();
              }
            },
            async pull(value) {
              try {
                await check(logical, 'read', transfer);
                if (offset === end) {
                  cleanup();
                  value.close();
                  return;
                }
                const count = Math.min(binding.chunkBytes, end - offset);
                const response = await client.send(
                  new GetObjectCommand({
                    ...object,
                    Range: `bytes=${offset}-${offset + count - 1}`,
                    IfMatch: head.ETag,
                  }),
                  { abortSignal: transfer },
                );
                try {
                  if (
                    response.ContentLength !== count ||
                    response.ContentRange !== `bytes ${offset}-${offset + count - 1}/${size}` ||
                    response.ETag !== head.ETag ||
                    !response.Body
                  ) {
                    throw new StorageError('IO_ERROR');
                  }
                  const buffer = new Uint8Array(count);
                  let received = 0;
                  for await (const chunk of response.Body as AsyncIterable<Uint8Array>) {
                    if (chunk.length > count - received) {
                      throw new StorageError('IO_ERROR');
                    }
                    buffer.set(chunk, received);
                    received += chunk.length;
                  }
                  if (received !== count) {
                    throw new StorageError('IO_ERROR');
                  }
                  await check(logical, 'read', transfer);
                  offset += count;
                  if (!closed) {
                    value.enqueue(buffer);
                  }
                } finally {
                  (response.Body as { destroy?: () => void } | undefined)?.destroy?.();
                }
              } catch (error) {
                if (!closed) {
                  cleanup();
                  value.error(
                    status(error) === 412 ? new StorageError('IO_ERROR') : failure(error, transfer),
                  );
                }
              }
            },
            cancel() {
              cleanup();
              stop.abort();
            },
          },
          { highWaterMark: 0 },
        );
      } catch (error) {
        throw failure(error, signal);
      }
    },
    async write(input, source, options) {
      const logical = location(input);
      const object = target(logical);
      const signal = transferSignal(options);
      const size = options.size;
      integer(size, 0, binding.maxObjectBytes);
      if (!(source instanceof ReadableStream) || source.locked) {
        throw new StorageError('INVALID_INPUT');
      }
      await check(logical, 'create', signal);
      let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
      let uploadId: string | undefined;
      let completionSent = false;
      let completed = false;
      let abort: (() => void) | undefined;
      try {
        reader = source.getReader();
        abort = () => {
          void reader?.cancel().catch(() => {});
        };
        if (signal) {
          EventTarget.prototype.addEventListener.call(signal, 'abort', abort, { once: true });
        }
        cancelled(signal);
        const created = await client.send(new CreateMultipartUploadCommand(object), {
          abortSignal: signal,
        });
        uploadId = created.UploadId;
        if (!uploadId) {
          throw new StorageError('IO_ERROR');
        }
        const parts: { ETag: string; PartNumber: number }[] = [];
        const buffer = new Uint8Array(Math.min(partBytes, size));
        let used = 0;
        let received = 0;
        const upload = async () => {
          await check(logical, 'create', signal);
          const PartNumber = parts.length + 1;
          const result = await client.send(
            new UploadPartCommand({
              ...object,
              UploadId: uploadId,
              PartNumber,
              Body: buffer.subarray(0, used),
              ContentLength: used,
            }),
            { abortSignal: signal },
          );
          if (!result.ETag) {
            throw new StorageError('IO_ERROR');
          }
          parts.push({ PartNumber, ETag: result.ETag });
          used = 0;
        };
        while (true) {
          await check(logical, 'create', signal);
          const item = await reader.read();
          await check(logical, 'create', signal);
          if (item.done) {
            break;
          }
          if (!(item.value instanceof Uint8Array)) {
            throw new StorageError('INVALID_INPUT');
          }
          if (
            item.value.byteLength > binding.chunkBytes ||
            item.value.byteLength > size - received
          ) {
            throw new StorageError('LIMIT_EXCEEDED');
          }
          received += item.value.byteLength;
          let offset = 0;
          while (offset < item.value.byteLength) {
            const count = Math.min(buffer.length - used, item.value.byteLength - offset);
            buffer.set(item.value.subarray(offset, offset + count), used);
            used += count;
            offset += count;
            if (used === buffer.length) {
              await upload();
            }
          }
          if (item.value.byteLength === 0) {
            await setImmediate();
          }
        }
        if (received !== size) {
          throw new StorageError('INVALID_INPUT');
        }
        if (used || parts.length === 0) {
          await upload();
        }
        await check(logical, 'create', signal);
        completionSent = true;
        await client.send(
          new CompleteMultipartUploadCommand({
            ...object,
            UploadId: uploadId,
            MultipartUpload: { Parts: parts },
            IfNoneMatch: '*',
          }),
          { abortSignal: signal },
        );
        completed = true;
      } catch (error) {
        if (reader) {
          void reader.cancel().catch(() => {});
        }
        if (completionSent && status(error) !== 412) {
          throw new StorageError(signal?.aborted ? 'CANCELLED' : 'IO_ERROR', 'uncertain');
        }
        throw failure(error, signal);
      } finally {
        if (signal && abort) {
          EventTarget.prototype.removeEventListener.call(signal, 'abort', abort);
        }
        reader?.releaseLock();
        if (uploadId && !completed) {
          // Cleanup uses its own deadline, even after caller cancellation. Abort
          // never removes a completed object. Failure leaves an orphan for service reconciliation.
          try {
            await client.send(new AbortMultipartUploadCommand({ ...object, UploadId: uploadId }), {
              abortSignal: AbortSignal.timeout(10000),
            });
          } catch {
            /* Preserve the original sanitized failure and uncertainty. */
          }
        }
      }
    },
  } satisfies StreamingStorage);
}
