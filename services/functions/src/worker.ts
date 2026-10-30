import { MoriaError } from '../../../packages/contracts/types.js';

export interface FunctionLimits {
  timeoutSeconds: number;
  memoryMiB?: number;
  cpuMillis?: number;
}

export function functionLimits(limits: FunctionLimits) {
  const memory = limits.memoryMiB ?? 256,
    cpu = limits.cpuMillis ?? 1000;
  if (
    !Number.isInteger(memory) ||
    memory < 32 ||
    memory > 1024 ||
    !Number.isInteger(cpu) ||
    cpu < 1 ||
    cpu > 30000 ||
    !Number.isInteger(limits.timeoutSeconds) ||
    limits.timeoutSeconds < 1 ||
    limits.timeoutSeconds > 30
  )
    throw new MoriaError('FUNCTION_LIMIT_INVALID', 'Function limits exceed runtime bounds');
  return { memory, cpu };
}

/** Trusted sandbox adapter. It receives code and data, never caller credentials. */
export interface FunctionWorker {
  /** Idempotently stop and remove this invocation's sandbox, including after a crash. */
  remove(id: string): Promise<void>;
  /** Enforce limits and bounded protocol; reject on abort, invalid output or accounting failure.
   * Resolve only after successful guest/process exit and all RPC/log callbacks settle.
   */
  execute(
    id: string,
    artifact: { uri: string; digest: string },
    limits: FunctionLimits,
    start: unknown,
    rpc: (request: unknown) => Promise<unknown>,
    log: (message: string) => Promise<void>,
    signal: AbortSignal,
  ): Promise<unknown>;
}
