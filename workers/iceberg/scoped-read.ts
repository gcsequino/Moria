import { openPython, ProcessFailure } from '../../services/process/src/python.js';
import { storageProcessError } from './process-errors.js';
import {
  StorageError,
  location,
  type StorageLocation,
} from '../../services/storage/src/locations.js';
import { cancelled } from '../../services/storage/src/transfer.js';

export interface ScopedObject {
  readonly name: string;
  readonly location: StorageLocation;
  readonly size: number;
}

export function scopedObjects(
  objects: readonly ScopedObject[],
  warehouse: string,
  table: string,
): readonly ScopedObject[] {
  if (!Array.isArray(objects) || objects.length > 128) throw new StorageError('LIMIT_EXCEEDED');
  const names = new Set<string>();
  let total = 0;
  return Object.freeze(
    objects.map((object) => {
      const logical = location(object.location);
      const prefix = `moria://${warehouse}/${table}/`;
      if (
        typeof object.name !== 'string' ||
        object.name.length > 512 ||
        !object.name.startsWith(prefix) ||
        !object.name
          .slice(prefix.length)
          .split('/')
          .every((part: string) => /^[a-zA-Z0-9_-][a-zA-Z0-9_.-]*$/.test(part) && part !== '..') ||
        logical.warehouseId !== warehouse ||
        logical.tableId !== table ||
        names.has(object.name)
      ) {
        throw new StorageError('BINDING_MISMATCH');
      }
      if (!Number.isSafeInteger(object.size) || object.size < 1 || object.size > 16 * 1024 * 1024)
        throw new StorageError('LIMIT_EXCEEDED');
      total += object.size;
      if (total > 64 * 1024 * 1024) throw new StorageError('LIMIT_EXCEEDED');
      names.add(object.name);
      return Object.freeze({ name: object.name, location: logical, size: object.size });
    }),
  );
}

/** Internal trusted composition. Only CatalogStorage supplies these capabilities. */
export async function* scopedRows(
  pin: {
    metadata: (signal: AbortSignal) => Promise<ReadableStream<Uint8Array>>;
    snapshotId: string;
    tableUuid: string;
    objects: readonly ScopedObject[];
    check: (signal: AbortSignal) => Promise<void>;
    read: (
      object: ScopedObject,
      offset: number,
      length: number,
      signal: AbortSignal,
    ) => Promise<ReadableStream<Uint8Array>>;
  },
  options: { signal?: AbortSignal; timeoutMs?: number } = {},
): AsyncGenerator<Readonly<Record<string, unknown>>> {
  const timeout = options.timeoutMs ?? 60000;
  if (!Number.isSafeInteger(timeout) || timeout < 1 || timeout > 60000) {
    throw new StorageError('INVALID_INPUT');
  }
  const controller = new AbortController();
  const abort = () => controller.abort(options.signal?.reason);
  options.signal?.addEventListener('abort', abort, { once: true });
  if (options.signal?.aborted) abort();
  const timer = setTimeout(
    () => controller.abort(new DOMException('Scoped read deadline', 'TimeoutError')),
    timeout,
  );
  const check = async () => {
    worker?.check();
    cancelled(controller.signal);
    let onAbort: (() => void) | undefined;
    try {
      const aborted = new Promise<never>((_resolve, reject) => {
        onAbort = () => reject(new StorageError('CANCELLED'));
        controller.signal.addEventListener('abort', onAbort, { once: true });
      });
      // The catalog transaction may still be waiting for its environment lock.
      // Observe its eventual rejection, but never let it delay scan cleanup or
      // authorize output after cancellation has won the race.
      await Promise.race([pin.check(controller.signal), aborted]);
      worker?.check();
      cancelled(controller.signal);
    } catch (error) {
      if (error instanceof StorageError && error.code === 'CANCELLED' && worker?.failure)
        throw worker.failure;
      throw error;
    } finally {
      if (onAbort) controller.signal.removeEventListener('abort', onAbort);
    }
  };
  let worker: ReturnType<typeof openPython> | undefined;
  let metadata: ReadableStream<Uint8Array> | undefined;
  try {
    await check();
    const chunks: Uint8Array[] = [];
    let size = 0;
    metadata = await pin.metadata(controller.signal);
    const metadataReader = metadata.getReader();
    const cancelMetadata = () => {
      void metadataReader.cancel().catch(() => {});
    };
    controller.signal.addEventListener('abort', cancelMetadata, { once: true });
    try {
      while (true) {
        await check();
        const item = await metadataReader.read();
        await check();
        if (item.done) break;
        size += item.value.length;
        if (size > 1024 * 1024) throw new StorageError('LIMIT_EXCEEDED');
        chunks.push(item.value);
      }
    } finally {
      await metadataReader.cancel().catch(() => {});
      metadataReader.releaseLock();
      controller.signal.removeEventListener('abort', cancelMetadata);
    }
    await check();
    let ended = false;
    worker = openPython('scoped-read', { signal: controller.signal, timeoutMs: timeout });
    void worker.completion.then((outcome) => {
      if (!ended || outcome.kind !== 'success') controller.abort();
    });
    const send = (frame: unknown) => worker!.writeJson(frame);
    try {
      await send({
        metadata: Buffer.concat(chunks).toString('base64'),
        snapshotId: pin.snapshotId,
        tableUuid: pin.tableUuid,
        objects: Object.fromEntries(pin.objects.map((object) => [object.name, object.size])),
      });
      let columns: string[] = [];
      const objects = new Map(pin.objects.map((object) => [object.name, object]));
      let started = false,
        rows = 0,
        transferred = 0,
        requests = 0,
        output = 0;
      for await (const line of worker.lines()) {
        worker.check();
        if (ended) throw new StorageError('INVALID_INPUT');
        let frame: any;
        try {
          frame = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(line));
        } catch {
          worker.protocol();
        }
        await check();
        if (frame.type === 'read') {
          const object = objects.get(frame.name);
          if (!object) throw new StorageError('AUTHORITY_DENIED');
          const { offset, length } = frame;
          if (
            Object.keys(frame).sort().join(',') !== 'length,name,offset,type' ||
            !Number.isSafeInteger(offset) ||
            !Number.isSafeInteger(length) ||
            offset < 0 ||
            length < 1 ||
            length > 256 * 1024 ||
            offset + length > object.size
          )
            throw new StorageError('INVALID_INPUT');
          transferred += length;
          if (++requests > 8192 || transferred > 128 * 1024 * 1024)
            throw new StorageError('LIMIT_EXCEEDED');
          const buffers: Uint8Array[] = [];
          let received = 0;
          for await (const bytes of await pin.read(object, offset, length, controller.signal)) {
            received += bytes.length;
            if (received > length) throw new StorageError('LIMIT_EXCEEDED');
            buffers.push(bytes);
          }
          if (received !== length) throw new StorageError('IO_ERROR');
          await check();
          await send({ data: Buffer.concat(buffers).toString('base64') });
        } else if (frame.type === 'start' && !started) {
          if (
            frame.snapshotId !== pin.snapshotId ||
            frame.tableUuid !== pin.tableUuid ||
            Object.keys(frame).sort().join(',') !== 'schema,snapshotId,tableUuid,type' ||
            frame.schema?.type !== 'struct' ||
            !Array.isArray(frame.schema.fields) ||
            frame.schema.fields.length > 256 ||
            frame.schema.fields.some((field: any) => typeof field.name !== 'string')
          )
            throw new StorageError('BINDING_MISMATCH');
          columns = frame.schema.fields.map((field: { name: string }) => field.name).sort();
          if (new Set(columns).size !== columns.length) throw new StorageError('INVALID_INPUT');
          started = true;
          await send({ next: true });
        } else if (frame.type === 'batch' && started) {
          if (
            Object.keys(frame).sort().join(',') !== 'rows,type' ||
            !Array.isArray(frame.rows) ||
            frame.rows.length < 1 ||
            frame.rows.length > 100
          )
            throw new StorageError('INVALID_INPUT');
          rows += frame.rows.length;
          output += line.length;
          if (rows > 100000 || output > 32 * 1024 * 1024) throw new StorageError('LIMIT_EXCEEDED');
          for (const row of frame.rows) {
            if (!row || typeof row !== 'object' || Array.isArray(row))
              throw new StorageError('INVALID_INPUT');
            const scalar = (value: unknown) =>
              value === null ||
              typeof value === 'string' ||
              typeof value === 'boolean' ||
              (typeof value === 'number' && Number.isFinite(value));
            if (
              JSON.stringify(Object.keys(row).sort()) !== JSON.stringify(columns) ||
              Object.values(row).some((value) =>
                Array.isArray(value)
                  ? value.some((item) => item === null || !scalar(item))
                  : !scalar(value),
              )
            ) {
              throw new StorageError('INVALID_INPUT');
            }
            await check();
            yield Object.freeze(row);
          }
          await send({ next: true });
        } else if (
          frame.type === 'end' &&
          started &&
          frame.rows === String(rows) &&
          Object.keys(frame).sort().join(',') === 'rows,type'
        ) {
          ended = true;
          await send({ next: true });
        } else throw new StorageError('INVALID_INPUT');
      }
      await worker.finish();
      cancelled(controller.signal);
      if (!ended) throw new StorageError('IO_ERROR');
      await check();
    } finally {
      await worker.close();
    }
  } catch (error) {
    await worker?.close();
    throw error instanceof ProcessFailure ? storageProcessError(worker?.failure ?? error) : error;
  } finally {
    controller.abort();
    await worker?.close();
    await metadata?.cancel().catch(() => {});
    clearTimeout(timer);
    options.signal?.removeEventListener('abort', abort);
  }
}
