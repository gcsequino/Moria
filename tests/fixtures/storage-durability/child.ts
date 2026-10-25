import fs, { type FileHandle } from 'node:fs/promises';
import { syncBuiltinESMExports } from 'node:module';
import { relative } from 'node:path';
import { createLocalStorage } from '../../../services/storage/src/local.js';
import { StorageError } from '../../../services/storage/src/locations.js';

// Only this subprocess fixture instruments builtins. Production has no fault controls.
const [root, mode, point, faultCode = 'EIO'] = process.argv.slice(2);
const events: string[] = [];
const handles = new Set<FileHandle>();
const controller = new AbortController();
let phase = 'init';
let triggered = false;

async function boundary(event: string): Promise<void> {
  events.push(event);
  if (event !== point) {
    return;
  }
  triggered = true;
  if (mode === 'kill') {
    process.send?.({ event });
    await new Promise<void>(() => {});
  } else if (mode === 'fault') {
    throw Object.assign(new Error('synthetic filesystem failure'), { code: faultCode });
  } else if (mode === 'cancel') {
    controller.abort();
  }
}

const originalOpen = fs.open.bind(fs);
fs.open = async (...args: Parameters<typeof fs.open>) => {
  const file = await originalOpen(...args);
  handles.add(file);
  const path = relative(root, String(args[0])) || '.';
  const name = path.startsWith('warehouse/table/staging/') ? 'file' : path;
  const sync = file.sync.bind(file);
  file.sync = async () => {
    await boundary(`${phase}:${name}:sync:before`);
    await sync();
    await boundary(`${phase}:${name}:sync:after`);
  };
  const close = file.close.bind(file);
  file.close = async () => {
    await close();
    handles.delete(file);
    await boundary(`${phase}:${name}:close:after`);
  };
  const write = file.write.bind(file);
  file.write = (async (...writeArgs: Parameters<typeof file.write>) => {
    const result = await write(...writeArgs);
    await boundary('write:file:write:after');
    return result;
  }) as typeof file.write;
  return file;
};
const originalLink = fs.link.bind(fs);
fs.link = async (...args) => {
  await boundary('write:link:before');
  await originalLink(...args);
  // Never inject a thrown error after the real link: that is not a failed syscall.
  await boundary('write:link:after');
};
const originalUnlink = fs.unlink.bind(fs);
fs.unlink = async (...args) => {
  await boundary('write:unlink:before');
  await originalUnlink(...args);
  await boundary('write:unlink:after');
};
syncBuiltinESMExports();

let result: { code: string; writeOutcome?: string } = { code: 'OK' };
const source = new ReadableStream<Uint8Array>(
  {
    start(value) {
      value.enqueue(Uint8Array.from([1, 2, 3, 4]));
      value.enqueue(Uint8Array.from([5, 6, 7, 8]));
      value.close();
    },
  },
  { highWaterMark: 0 },
);
try {
  const storage = await createLocalStorage({
    root,
    warehouseId: 'warehouse',
    tableId: 'table',
    maxObjectBytes: 64,
    chunkBytes: 4,
  });
  phase = 'write';
  await storage.write(
    { version: 1, warehouseId: 'warehouse', tableId: 'table', objectId: 'object' },
    source,
    { size: 8, signal: controller.signal },
  );
  await boundary('ack');
} catch (error) {
  if (!(error instanceof StorageError)) {
    throw error;
  }
  result = { code: error.code, writeOutcome: error.writeOutcome };
}
process.stdout.write(
  JSON.stringify({
    ...result,
    events,
    triggered,
    openHandles: handles.size,
    locked: source.locked,
  }),
);
