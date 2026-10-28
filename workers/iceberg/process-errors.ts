import type { ProcessFailure } from '../../services/process/src/python.js';
import { StorageError } from '../../services/storage/src/locations.js';
import { MoriaError } from '../../packages/contracts/types.js';

export function storageProcessError(failure: ProcessFailure): StorageError {
  const kind = failure.outcome.kind;
  const error = new StorageError(
    kind === 'invalid-input'
      ? 'INVALID_INPUT'
      : ['cancelled', 'timeout'].includes(kind)
        ? 'CANCELLED'
        : kind === 'limit'
          ? 'LIMIT_EXCEEDED'
          : 'IO_ERROR',
  );
  return Object.defineProperty(error, 'cause', { value: failure });
}
export function workerProcessError(failure: ProcessFailure): MoriaError {
  const kind = failure.outcome.kind;
  const code =
    kind === 'timeout'
      ? 'WORKER_TIMEOUT'
      : kind === 'cancelled'
        ? 'CANCELLED'
        : ['protocol', 'limit'].includes(kind)
          ? 'STREAM_INVALID'
          : 'WORKER_FAILED';
  return Object.defineProperty(new MoriaError(code, failure.message), 'cause', { value: failure });
}
