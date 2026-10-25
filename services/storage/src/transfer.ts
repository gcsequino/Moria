import { StorageError, type TransferOptions } from './locations.js';

export function integer(value: number, minimum: number, maximum: number): void {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new StorageError('INVALID_INPUT');
  }
}

export function transferSignal(options: TransferOptions): AbortSignal | undefined {
  try {
    if (options === null || typeof options !== 'object' || Array.isArray(options)) {
      throw new StorageError('INVALID_INPUT');
    }
    const signal = options.signal;
    if (
      signal !== undefined &&
      (!(signal instanceof AbortSignal) ||
        typeof signal.aborted !== 'boolean' ||
        typeof signal.addEventListener !== 'function' ||
        typeof signal.removeEventListener !== 'function')
    ) {
      throw new StorageError('INVALID_INPUT');
    }
    return signal;
  } catch {
    throw new StorageError('INVALID_INPUT');
  }
}

export function cancelled(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw new StorageError('CANCELLED');
  }
}
