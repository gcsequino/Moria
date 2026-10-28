import { openPython, ProcessFailure } from '../../services/process/src/python.js';
import { workerProcessError } from './process-errors.js';
import { MoriaError } from '../../packages/contracts/types.js';

/** Trusted pinned metadata only; publication and authorization remain with callers. */
export async function* icebergRows(
  pin: { metadataLocation: string; snapshotId: string },
  options: { batchRows?: number; timeoutMs?: number; signal?: AbortSignal } = {},
): AsyncGenerator<unknown> {
  const batchRows = options.batchRows ?? 1000;
  if (
    !Number.isInteger(batchRows) ||
    batchRows < 1 ||
    batchRows > 1000 ||
    (options.timeoutMs !== undefined &&
      (!Number.isSafeInteger(options.timeoutMs) ||
        options.timeoutMs < 1 ||
        options.timeoutMs > 30 * 60 * 1000))
  )
    throw new MoriaError('STREAM_INVALID', 'Invalid stream limits');
  let worker: ReturnType<typeof openPython> | undefined;
  try {
    worker = openPython('stream', options);
    await worker.writeJson({
      operation: 'stream',
      metadataLocation: pin.metadataLocation,
      snapshotId: pin.snapshotId,
      batchRows,
    });
    worker.end();
    let started = false,
      ended = false,
      count = 0n;
    const checkCancelled = () => worker!.check();
    const invalid = (): never => worker!.protocol();
    for await (const line of worker.lines()) {
      worker.check();
      let frame: any;
      try {
        frame = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(line));
      } catch {
        invalid();
      }
      if (!frame || ended) invalid();
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
    await worker.finish();
    if (!started || !ended) invalid();
  } catch (error) {
    await worker?.close();
    throw error instanceof ProcessFailure ? workerProcessError(worker?.failure ?? error) : error;
  } finally {
    await worker?.close();
  }
}
