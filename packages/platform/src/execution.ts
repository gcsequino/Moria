import type { CancellationReceipt } from '../../contracts/execution.js';
import { ApiError } from './transport.js';

export function checkedCancellation(id: string, value: unknown): CancellationReceipt {
  if (
    !value ||
    typeof value !== 'object' ||
    Array.isArray(value) ||
    Object.keys(value).length !== 2 ||
    !('id' in value) ||
    value.id !== id ||
    !('cancelRequested' in value) ||
    value.cancelRequested !== true
  )
    throw new ApiError(502, 'CANCELLATION_PROTOCOL', 'Invalid cancellation control receipt');
  return { id, cancelRequested: true };
}
