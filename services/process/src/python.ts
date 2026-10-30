import { spawn } from 'node:child_process';
import { resolve } from 'node:path';

export type PythonTask = 'stage' | 'stream' | 'scoped-read' | 'metadata' | 'snapshot';
type FailureKind =
  | 'startup'
  | 'ipc'
  | 'exit'
  | 'signal'
  | 'timeout'
  | 'cancelled'
  | 'limit'
  | 'invalid-input'
  | 'protocol';
export interface ProcessOutcome {
  readonly task: PythonTask;
  readonly kind: 'success' | FailureKind;
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly pid: number | undefined;
  readonly stderrBytes: number;
}
export class ProcessFailure extends Error {
  constructor(readonly outcome: ProcessOutcome) {
    super(`Python ${outcome.task} process ${outcome.kind}`);
    this.name = 'ProcessFailure';
  }
}
const MiB = 1024 * 1024;
const profiles = {
  stage: {
    file: 'worker.py',
    timeout: 60000,
    input: 64 * MiB,
    output: 64 * MiB,
    frame: 64 * MiB,
    isolated: false,
  },
  stream: {
    file: 'worker.py',
    timeout: 30 * 60 * 1000,
    input: MiB,
    output: 256 * MiB,
    frame: 4 * MiB,
    isolated: false,
  },
  'scoped-read': {
    file: 'scoped_reader.py',
    timeout: 60000,
    input: 192 * MiB,
    output: 64 * MiB,
    frame: 4 * MiB,
    isolated: true,
  },
  metadata: {
    file: 'validate_metadata.py',
    timeout: 30000,
    input: 2 * MiB,
    output: 1024,
    frame: 2 * MiB,
    isolated: true,
  },
  snapshot: {
    file: 'validate_snapshot.py',
    timeout: 60000,
    input: 96 * MiB,
    output: 1024,
    frame: 2 * MiB,
    isolated: true,
  },
} as const;

/** Fixed trusted tasks only. No request-selected executable, environment or limit expansion. */
export function openPython(
  task: PythonTask,
  options: { signal?: AbortSignal; timeoutMs?: number } = {},
) {
  const profile = profiles[task];
  if (!profile) throw new Error('Unknown Python task');
  const timeout = options.timeoutMs ?? profile.timeout;
  if (!Number.isSafeInteger(timeout) || timeout < 1 || timeout > profile.timeout)
    throw new Error('Invalid Python deadline');
  if (options.signal?.aborted)
    throw new ProcessFailure({
      task,
      kind: 'cancelled',
      code: null,
      signal: null,
      pid: undefined,
      stderrBytes: 0,
    });
  const child = spawn(
    profile.isolated
      ? resolve('.venv/bin/python')
      : (process.env.MORIA_PYTHON ?? resolve('.venv/bin/python')),
    [...(profile.isolated ? ['-I'] : []), resolve('workers/iceberg', profile.file)],
    {
      cwd: profile.isolated ? resolve('workers/iceberg') : process.cwd(),
      env: profile.isolated
        ? {
            HOME: '/nonexistent',
            LANG: 'C.UTF-8',
            OPENBLAS_NUM_THREADS: '1',
            OMP_NUM_THREADS: '1',
            PYICEBERG_MAX_WORKERS: '1',
          }
        : { ...process.env, PYTHONUTF8: '1' },
      stdio: ['pipe', 'pipe', 'pipe'],
    },
  );
  const controller = new AbortController();
  let reason: FailureKind | undefined,
    closed = false,
    spawned = false;
  let code: number | null = null,
    signal: NodeJS.Signals | null = null;
  let inputBytes = 0,
    outputBytes = 0,
    stderrBytes = 0;
  let killTimer: ReturnType<typeof setTimeout> | undefined;
  const current = (): ProcessOutcome =>
    Object.freeze({ task, kind: reason ?? 'success', code, signal, pid: child.pid, stderrBytes });
  const check = () => {
    if (reason) throw new ProcessFailure(current());
  };
  // shortcut: fixed tasks launch no descendants; add group termination before admitting process trees.
  const terminate = () => {
    if (closed) return;
    child.kill('SIGTERM');
    child.stdin.destroy();
    child.stdout.destroy();
    child.stderr.destroy();
    killTimer ??= setTimeout(() => {
      if (!closed) child.kill('SIGKILL');
    }, 250);
  };
  const fail = (kind: FailureKind) => {
    reason ??= kind;
    controller.abort();
    terminate();
  };
  const abort = () =>
    fail(options.signal?.reason?.name === 'TimeoutError' ? 'timeout' : 'cancelled');
  const timer = setTimeout(() => fail('timeout'), timeout);
  const classify = (exitCode: number | null, exitSignal: NodeJS.Signals | null) => {
    code = exitCode;
    signal = exitSignal;
    if (!reason || reason === 'ipc') {
      if (signal && !reason) reason = 'signal';
      else if (code === 65 && ['metadata', 'snapshot'].includes(task)) reason = 'invalid-input';
      else if (code !== null && code !== 0) reason = 'exit';
    }
  };
  const completion = new Promise<ProcessOutcome>((done) => {
    child.once('spawn', () => {
      spawned = true;
    });
    child.once('error', () => fail(spawned ? 'ipc' : 'startup'));
    child.once('exit', (exitCode, exitSignal) => {
      classify(exitCode, exitSignal);
      if (reason) {
        controller.abort();
        child.stdin.destroy();
        child.stdout.destroy();
        child.stderr.destroy();
      }
    });
    child.once('close', (exitCode, exitSignal) => {
      closed = true;
      classify(exitCode, exitSignal);
      clearTimeout(timer);
      if (killTimer) clearTimeout(killTimer);
      options.signal?.removeEventListener('abort', abort);
      if (reason) controller.abort();
      done(current());
    });
  });
  child.stdin.on('error', () => fail('ipc'));
  child.stderr.on('data', (bytes: Buffer) => {
    stderrBytes = Math.min(65537, stderrBytes + bytes.length);
    if (stderrBytes > 65536) fail('limit');
  });
  options.signal?.addEventListener('abort', abort, { once: true });
  if (options.signal?.aborted) abort();
  const wait = async <T>(operation: Promise<T>) => {
    check();
    let interrupted: (() => void) | undefined;
    try {
      return await Promise.race([
        operation,
        new Promise<never>((_resolve, reject) => {
          interrupted = () => reject(new ProcessFailure(current()));
          controller.signal.addEventListener('abort', interrupted, { once: true });
          if (controller.signal.aborted) interrupted();
        }),
      ]);
    } finally {
      if (interrupted) controller.signal.removeEventListener('abort', interrupted);
    }
  };
  const write = async (bytes: Uint8Array) => {
    check();
    if (bytes.length > profile.frame + 1 || inputBytes + bytes.length > profile.input) {
      fail('limit');
      check();
    }
    inputBytes += bytes.length;
    try {
      await wait(
        new Promise<void>((done, reject) =>
          child.stdin.write(bytes, (error) => (error ? reject(error) : done())),
        ),
      );
      check();
    } catch {
      fail(reason ?? 'ipc');
      check();
    }
  };
  return Object.freeze({
    completion,
    get failure() {
      return reason ? new ProcessFailure(current()) : undefined;
    },
    protocol(): never {
      fail('protocol');
      throw new ProcessFailure(current());
    },
    signal: controller.signal,
    check,
    write,
    async writeJson(value: unknown) {
      check();
      let budget = profile.frame as number;
      const text = JSON.stringify(value, function (key, item) {
        const strings = [typeof item === 'string' ? item : '', Array.isArray(this) ? '' : key];
        if (strings.some((part) => part.length > profile.frame)) {
          fail('limit');
          check();
        }
        const scalar = item === null || typeof item !== 'object';
        budget -=
          (key && !Array.isArray(this) ? Buffer.byteLength(JSON.stringify(key)) + 1 : 0) +
          (scalar ? Buffer.byteLength(JSON.stringify(item) ?? '') : 2) +
          1;
        if (budget < 0) {
          fail('limit');
          check();
        }
        return item;
      });
      if (text === undefined) throw new Error('JSON frame required');
      await write(Buffer.from(text + '\n'));
    },
    end() {
      check();
      child.stdin.end();
    },
    async *lines(): AsyncGenerator<Buffer> {
      let pending: Buffer[] = [];
      let pendingBytes = 0;
      try {
        for await (const chunk of child.stdout) {
          check();
          outputBytes += chunk.length;
          if (outputBytes > profile.output) {
            fail('limit');
            check();
          }
          let offset = 0;
          for (let newline = chunk.indexOf(10); newline >= 0; newline = chunk.indexOf(10, offset)) {
            if (pendingBytes + newline - offset > profile.frame) {
              fail('limit');
              check();
            }
            if (task === 'scoped-read' && newline + 1 !== chunk.length) {
              fail('protocol');
              check();
            }
            const line = Buffer.concat(
              [...pending, chunk.subarray(offset, newline)],
              pendingBytes + newline - offset,
            );
            pending = [];
            pendingBytes = 0;
            offset = newline + 1;
            yield line;
            check();
          }
          if (pendingBytes + chunk.length - offset > profile.frame) {
            fail('limit');
            check();
          }
          if (offset < chunk.length) {
            pendingBytes += chunk.length - offset;
            pending.push(Buffer.from(chunk.subarray(offset)));
            if (pending.length > 65536) {
              fail('limit');
              check();
            }
          }
        }
        await completion;
        check();
        if (pendingBytes) {
          fail('protocol');
          check();
        }
      } catch (error) {
        if (!reason) fail('ipc');
        check();
        throw error;
      }
    },
    async finish() {
      const outcome = await completion;
      if (outcome.kind !== 'success') throw new ProcessFailure(outcome);
    },
    async close() {
      if (!closed) {
        reason ??= 'cancelled';
        controller.abort();
      }
      terminate();
      // Keep the owner responsible until close confirms reaping and pipe release.
      await completion;
    },
  });
}
