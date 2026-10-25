import { spawn } from 'node:child_process';
import { resolve } from 'node:path';
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
    check: () => Promise<void>;
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
  const abort = () => controller.abort();
  options.signal?.addEventListener('abort', abort, { once: true });
  if (options.signal?.aborted) abort();
  const timer = setTimeout(abort, timeout);
  const check = async () => {
    cancelled(controller.signal);
    await pin.check();
    cancelled(controller.signal);
  };
  let stop: (() => void) | undefined;
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
    const child = spawn(
      resolve('.venv/bin/python'),
      ['-I', resolve('workers/iceberg/scoped_reader.py')],
      {
        cwd: resolve('workers/iceberg'),
        env: {
          HOME: '/nonexistent',
          LANG: 'C.UTF-8',
          OPENBLAS_NUM_THREADS: '1',
          OMP_NUM_THREADS: '1',
          PYICEBERG_MAX_WORKERS: '1',
        },
        stdio: ['pipe', 'pipe', 'pipe'],
      },
    );
    const exit = new Promise<number | null>((done) => {
      child.once('error', () => {
        controller.abort();
        done(null);
      });
      child.once('close', (code) => {
        if (!ended || code !== 0) controller.abort();
        clearTimeout(timer);
        options.signal?.removeEventListener('abort', abort);
        done(code);
      });
    });
    stop = () => {
      child.kill('SIGKILL');
    };
    controller.signal.addEventListener('abort', stop, { once: true });
    child.stderr.resume();
    child.stdin.on('error', () => {});
    const send = async (frame: unknown) => {
      cancelled(controller.signal);
      const line = JSON.stringify(frame) + '\n';
      if (Buffer.byteLength(line) > 4 * 1024 * 1024) throw new StorageError('LIMIT_EXCEEDED');
      await new Promise<void>((done, reject) =>
        child.stdin.write(line, (error) => (error ? reject(error) : done())),
      );
    };
    try {
      await send({
        metadata: Buffer.concat(chunks).toString('base64'),
        snapshotId: pin.snapshotId,
        tableUuid: pin.tableUuid,
        objects: Object.fromEntries(pin.objects.map((object) => [object.name, object.size])),
      });
      let columns: string[] = [];
      const objects = new Map(pin.objects.map((object) => [object.name, object]));
      let pending = Buffer.alloc(0),
        started = false,
        rows = 0,
        transferred = 0,
        requests = 0,
        output = 0;
      for await (const chunk of child.stdout) {
        cancelled(controller.signal);
        pending = Buffer.concat([pending, chunk]);
        for (let newline = pending.indexOf(10); newline >= 0; newline = pending.indexOf(10)) {
          if (newline > 4 * 1024 * 1024 || ended) throw new StorageError('LIMIT_EXCEEDED');
          const frame = JSON.parse(
            new TextDecoder('utf-8', { fatal: true }).decode(pending.subarray(0, newline)),
          );
          pending = pending.subarray(newline + 1);
          if (pending.length) throw new StorageError('INVALID_INPUT');
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
            output += newline;
            if (rows > 100000 || output > 32 * 1024 * 1024)
              throw new StorageError('LIMIT_EXCEEDED');
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
        if (pending.length > 4 * 1024 * 1024) throw new StorageError('LIMIT_EXCEEDED');
      }
      const code = await exit;
      cancelled(controller.signal);
      if (code !== 0 || pending.length || !ended) throw new StorageError('IO_ERROR');
      await check();
    } finally {
      stop();
      await exit;
      controller.signal.removeEventListener('abort', stop);
    }
  } finally {
    controller.abort();
    stop?.();
    await metadata?.cancel().catch(() => {});
    clearTimeout(timer);
    options.signal?.removeEventListener('abort', abort);
  }
}
