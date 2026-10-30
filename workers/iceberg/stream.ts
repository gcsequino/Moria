import { spawn } from 'node:child_process';
import { resolve } from 'node:path';
import { MoriaError, VALUE_VERSION } from '../../packages/contracts/types.js';

/** Trusted local adapter, not a caller-facing metadata/path authorization API.
 * Pulling stdout applies pipe backpressure while ObjectStore awaits DB batches.
 * Caller must pin/authorize metadata and fence publication independently. */
export async function* icebergRows(
  pin: { metadataLocation: string; snapshotId: string },
  options: { batchRows?: number; timeoutMs?: number; signal?: AbortSignal } = {},
): AsyncGenerator<unknown> {
  const batchRows = options.batchRows ?? 1000,
    timeoutMs = options.timeoutMs ?? 30 * 60 * 1000;
  if (
    !Number.isInteger(batchRows) ||
    batchRows < 1 ||
    batchRows > 1000 ||
    !Number.isInteger(timeoutMs) ||
    timeoutMs < 1 ||
    timeoutMs > 30 * 60 * 1000
  )
    throw new MoriaError('STREAM_INVALID', 'Invalid stream limits');
  let timedOut = false;
  const checkCancelled = () => {
    if (options.signal?.aborted) throw new MoriaError('CANCELLED', 'Iceberg read cancelled');
    if (timedOut) throw new MoriaError('WORKER_TIMEOUT', 'Iceberg stream time limit exceeded');
  };
  checkCancelled();
  const python =
    process.env.MORIA_PYTHON ??
    resolve(process.platform === 'win32' ? '.venv/Scripts/python.exe' : '.venv/bin/python');
  const child = spawn(python, ['workers/iceberg/worker.py'], { stdio: ['pipe', 'pipe', 'pipe'] });
  const exit = new Promise<number | null>((done) => {
    child.once('error', () => done(null));
    child.once('close', done);
  });
  let killTimer: ReturnType<typeof setTimeout> | undefined;
  const terminate = () => {
    child.kill();
    killTimer ??= setTimeout(() => {
      if (child.exitCode === null) child.kill('SIGKILL');
    }, 10000);
  };
  options.signal?.addEventListener('abort', terminate, { once: true });
  // Drain errors without retaining unbounded or potentially sensitive diagnostics.
  child.stderr.resume();
  child.stdin.on('error', () => {});
  const timer = setTimeout(() => {
    timedOut = true;
    terminate();
  }, timeoutMs);
  child.stdin.end(
    JSON.stringify({
      operation: 'stream',
      valueVersion: VALUE_VERSION,
      metadataLocation: pin.metadataLocation,
      snapshotId: pin.snapshotId,
      batchRows,
    }),
  );
  let pending = Buffer.alloc(0),
    started = false,
    ended = false,
    count = 0n;
  const invalid = (): never => {
    throw new MoriaError('STREAM_INVALID', 'Invalid or incomplete Iceberg stream');
  };
  try {
    for await (const chunk of child.stdout) {
      checkCancelled();
      pending = Buffer.concat([pending, chunk]);
      for (let newline = pending.indexOf(10); newline >= 0; newline = pending.indexOf(10)) {
        if (newline > 4 * 1024 * 1024) invalid();
        const line = pending.subarray(0, newline);
        pending = pending.subarray(newline + 1);
        let frame: any;
        try {
          frame = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(line));
        } catch {
          invalid();
        }
        if (!frame || frame.valueVersion !== VALUE_VERSION || ended) invalid();
        if (!started) {
          if (
            frame.type !== 'start' ||
            frame.snapshotId !== pin.snapshotId ||
            typeof frame.tableUuid !== 'string'
          )
            invalid();
          started = true;
          continue;
        }
        if (frame.type === 'end') {
          if (frame.rows !== String(count)) invalid();
          ended = true;
          continue;
        }
        if (
          frame.type !== 'batch' ||
          !Array.isArray(frame.rows) ||
          frame.rows.length < 1 ||
          frame.rows.length > batchRows
        )
          invalid();
        for (const row of frame.rows) {
          checkCancelled();
          count++;
          yield row;
        }
      }
      if (pending.length > 4 * 1024 * 1024) invalid();
    }
    const code = await exit;
    checkCancelled();
    if (timedOut) throw new MoriaError('WORKER_TIMEOUT', 'Iceberg stream time limit exceeded');
    if (code !== 0) throw new MoriaError('WORKER_FAILED', 'Iceberg stream worker failed');
    if (pending.length || !started || !ended) invalid();
  } finally {
    clearTimeout(timer);
    options.signal?.removeEventListener('abort', terminate);
    if (child.exitCode === null) terminate();
    await exit;
    if (killTimer) clearTimeout(killTimer);
  }
}
