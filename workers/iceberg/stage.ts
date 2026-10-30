import { VALUE_VERSION, MoriaError } from '../../packages/contracts/types.js';
import { canonicalJson } from '../../packages/contracts/values.js';
import { openPython, ProcessFailure } from '../../services/process/src/python.js';

export async function iceberg(
  request: unknown,
  options: { signal?: AbortSignal } = {},
): Promise<any> {
  if (
    !request ||
    typeof request !== 'object' ||
    Array.isArray(request) ||
    ((request as any).valueVersion !== undefined && (request as any).valueVersion !== VALUE_VERSION)
  )
    return Promise.reject(
      new MoriaError('UNSUPPORTED_VALUE_VERSION', 'Unsupported worker value version'),
    );
  let input: string;
  try {
    input = canonicalJson({ ...request, valueVersion: VALUE_VERSION });
  } catch {
    return Promise.reject(
      new MoriaError('VALUE_INVALID', 'Worker request must contain JSON wire values'),
    );
  }
  const worker = openPython('stage', options);
  try {
    await worker.writeJson(JSON.parse(input));
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
    if ((result as any)?.valueVersion !== VALUE_VERSION)
      throw new MoriaError('UNSUPPORTED_VALUE_VERSION', 'Unsupported worker value version');
    return result;
  } catch (error) {
    await worker.close();
    throw error instanceof ProcessFailure ? (worker.failure ?? error) : error;
  } finally {
    await worker.close();
  }
}
