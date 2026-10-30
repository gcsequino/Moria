import { AsyncLocalStorage } from 'node:async_hooks';

interface AccessFlow {
  markings: Map<string, { environment: string; marking: string }>;
  reads: Map<string, { resource: string; operation: string }>;
}
const access = new AsyncLocalStorage<AccessFlow>();

export function recordAccess(
  environment: string,
  resource: string,
  operation: string,
  markings: readonly string[],
) {
  const flow = access.getStore();
  if (!flow) return;
  for (const marking of markings)
    flow.markings.set(JSON.stringify([environment, marking]), { environment, marking });
  if (operation === 'read' || operation === 'list')
    flow.reads.set(JSON.stringify([resource, operation]), { resource, operation });
}

export function accessMarkings() {
  return [...(access.getStore()?.markings.values() ?? [])];
}

/** Classification survives a domain transaction rollback, including data-dependent errors. */
export async function captureAccess<T>(run: () => Promise<T>) {
  const flow: AccessFlow = { markings: new Map(), reads: new Map() };
  let value: T | undefined,
    error: unknown,
    failed = false;
  try {
    value = await access.run(flow, run);
  } catch (failure) {
    error = failure;
    failed = true;
  }
  return {
    value,
    error,
    failed,
    markings: [...flow.markings.values()],
    reads: [...flow.reads.values()],
  };
}
