import { openPython, ProcessFailure } from '../../services/process/src/python.js';

export async function iceberg(
  request: unknown,
  options: { signal?: AbortSignal } = {},
): Promise<any> {
  const worker = openPython('stage', options);
  try {
    await worker.writeJson(request);
    worker.end();
    let result: unknown,
      received = false;
    for await (const line of worker.lines()) {
      if (received) worker.protocol();
      try {
        result = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(line));
      } catch {
        worker.protocol();
      }
      received = true;
    }
    await worker.finish();
    if (!received) worker.protocol();
    return result;
  } catch (error) {
    await worker.close();
    throw error instanceof ProcessFailure ? (worker.failure ?? error) : error;
  } finally {
    await worker.close();
  }
}
