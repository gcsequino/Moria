import type { Catalog } from '../../catalog/src/catalog.js';
import type { StageLease } from './store.js';
import { MoriaError } from '../../../packages/contracts/types.js';

/** Process-local supervision only. Database fencing remains the commit authority.
 * Renewals never overlap; stopping drains an outstanding renewal and removes timers.
 * Database outages may delay cleanup until the configured pool/query timeout. */
export async function withLease<T>(
  catalog: Catalog,
  environment: string,
  lease: StageLease,
  work: (signal: AbortSignal) => Promise<T>,
  options: { signal?: AbortSignal; seconds?: number; timeoutMs?: number } = {},
): Promise<T> {
  lease = structuredClone(lease);
  const seconds = options.seconds ?? 60;
  if (!Number.isInteger(seconds) || seconds < 1 || seconds > 3600)
    throw new MoriaError('LEASE_INVALID', 'Lease duration outside allowed range');
  const timeoutMs = options.timeoutMs ?? 30 * 60 * 1000;
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 30 * 60 * 1000)
    throw new MoriaError('DEADLINE_INVALID', 'Indexing deadline outside allowed range');
  const controller = new AbortController();
  const deadline = setTimeout(
    () =>
      controller.abort(new MoriaError('WORKER_TIMEOUT', 'Indexing execution deadline exceeded')),
    timeoutMs,
  );
  const cancel = () => controller.abort(new MoriaError('CANCELLED', 'Indexing cancelled'));
  options.signal?.addEventListener('abort', cancel, { once: true });
  if (options.signal?.aborted) cancel();
  let timer: ReturnType<typeof setTimeout> | undefined,
    pending: Promise<void> | undefined,
    stopped = false;
  const renew = () =>
    catalog.renew(environment, lease.execution, lease.worker, lease.fence, seconds);
  const schedule = () => {
    if (!stopped && !controller.signal.aborted)
      timer = setTimeout(
        () => {
          pending = renew()
            .catch((error) => {
              controller.abort(error);
            })
            .then(schedule);
        },
        Math.floor((seconds * 1000) / 3),
      );
  };
  try {
    controller.signal.throwIfAborted();
    await renew();
    controller.signal.throwIfAborted();
    schedule();
    try {
      return await work(controller.signal);
    } catch (error) {
      // Preserve lease loss as the cause when the stream reports cancellation.
      if (controller.signal.aborted) throw controller.signal.reason;
      throw error;
    }
  } finally {
    stopped = true;
    clearTimeout(deadline);
    if (timer) clearTimeout(timer);
    options.signal?.removeEventListener('abort', cancel);
    await pending;
  }
}
