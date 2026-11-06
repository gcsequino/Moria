import { execFile, spawn } from 'node:child_process';
import { promisify } from 'node:util';
import { once } from 'node:events';
import { request } from 'node:http';
import { randomUUID } from 'node:crypto';
import { MoriaError } from '../../../packages/contracts/types.js';
import { functionLimits, type FunctionLimits, type FunctionWorker } from './worker.js';

const exec = promisify(execFile);
const name = (id: string) => {
  if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(id))
    throw new MoriaError('FUNCTION_INVALID', 'Invalid invocation ID');
  return `moria-function-${id}`;
};
async function docker(args: string[], timeout = 10000) {
  try {
    return (
      await exec('docker', ['--host=unix:///var/run/docker.sock', ...args], {
        timeout,
        maxBuffer: 1048576,
      })
    ).stdout;
  } catch {
    throw new MoriaError('FUNCTION_WORKER_UNAVAILABLE', 'Function container operation failed');
  }
}

async function cpuUsage(id: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const connection = request(
      {
        socketPath: '/var/run/docker.sock',
        // Use the daemon's default API so older worker hosts remain supported.
        path: `/containers/${name(id)}/stats?stream=false&one-shot=true`,
      },
      (response) => {
        const chunks: Buffer[] = [];
        let size = 0;
        response.on('data', (chunk: Buffer) => {
          size += chunk.length;
          if (size > 1048576) connection.destroy(new Error('Oversized worker stats'));
          else chunks.push(chunk);
        });
        response.on('error', reject);
        response.on('end', () => {
          try {
            const usage = JSON.parse(Buffer.concat(chunks).toString('utf8')).cpu_stats?.cpu_usage
              ?.total_usage;
            if (response.statusCode !== 200 || !Number.isFinite(usage) || usage < 0)
              throw new Error('Invalid worker stats');
            resolve(usage / 1000000);
          } catch (error) {
            reject(error);
          }
        });
      },
    );
    connection.setTimeout(1000, () => connection.destroy(new Error('Worker stats unavailable')));
    connection.once('error', reject);
    connection.end();
  });
}

/** Explicit function-worker role only. API processes never construct this backend. */
export class DockerFunctionWorker implements FunctionWorker {
  async remove(id: string) {
    // Removal is idempotent; inspect distinguishes an absent container from a daemon failure.
    const names = await docker(['container', 'ls', '-aq', '--filter', `name=^/${name(id)}$`]);
    if (names.trim()) {
      try {
        await docker(['stop', '--time=10', name(id)], 12000);
        await docker(['rm', '--force', name(id)]);
      } catch (error) {
        if ((await docker(['container', 'ls', '-aq', '--filter', `name=^/${name(id)}$`])).trim())
          throw error;
      }
    }
  }
  async execute(
    id: string,
    artifact: { uri: string; digest: string },
    limits: FunctionLimits,
    start: unknown,
    rpc: (request: unknown) => Promise<unknown>,
    log: (message: string) => Promise<void>,
    signal: AbortSignal,
  ) {
    const { memory, cpu } = functionLimits(limits);
    if (
      !/^oci:\/\/[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,249}$/.test(artifact.uri) ||
      !/^sha256:[0-9a-f]{64}$/.test(artifact.digest)
    )
      throw new MoriaError('FUNCTION_ARTIFACT_INVALID', 'Expected a digest-pinned OCI repository');
    const image = `${artifact.uri.slice(6)}@${artifact.digest}`;
    const metadata = JSON.parse(await docker(['image', 'inspect', image]))[0];
    if (
      metadata.Os !== 'linux' ||
      !metadata.RepoDigests?.includes(image) ||
      Object.keys(metadata.Config?.Volumes ?? {}).length
    )
      throw new MoriaError(
        'FUNCTION_ARTIFACT_INVALID',
        'Expected a preloaded Linux image without declared volumes',
      );
    if (signal.aborted) throw signal.reason;
    await docker([
      'create',
      '--name',
      name(id),
      '--label',
      'moria.component=function',
      '--pull=never',
      '--network=none',
      '--read-only',
      '--user=65532:65532',
      '--cap-drop=ALL',
      '--security-opt=no-new-privileges',
      '--pids-limit=64',
      `--memory=${memory}m`,
      `--memory-swap=${memory}m`,
      '--cpus=1',
      `--ulimit=cpu=${Math.ceil(cpu / 1000)}:${Math.ceil(cpu / 1000)}`,
      '--ipc=none',
      '--tmpfs=/tmp:rw,noexec,nosuid,nodev,size=16777216,mode=1777',
      '--no-healthcheck',
      '--log-driver=none',
      '--interactive',
      image,
    ]);
    let failure: unknown,
      result: unknown,
      received = false,
      acknowledged = false,
      total = 0,
      buffer = Buffer.alloc(0),
      chain = Promise.resolve();
    let completionNonce: string | undefined;
    const calls = new Set<string>();
    const child = spawn(
      'docker',
      ['--host=unix:///var/run/docker.sock', 'start', '--attach', '--interactive', name(id)],
      {
        stdio: ['pipe', 'pipe', 'pipe'],
      },
    );
    let closed = false;
    const exited = new Promise<number | null>((resolve, reject) => {
      child.once('error', reject);
      child.once('close', (code) => {
        closed = true;
        resolve(code);
      });
    });
    let cleanupTimer: ReturnType<typeof setTimeout> | undefined;
    let cleanup: Promise<void> | undefined;
    const remove = () => (cleanup ??= this.remove(id));
    const stop = () => {
      void remove().catch(() => {});
      cleanupTimer ??= setTimeout(() => child.kill('SIGKILL'), 12000);
    };
    const abort = () => {
      failure ??= signal.reason ?? new MoriaError('CANCELLED', 'Function cancelled');
      stop();
    };
    signal.addEventListener('abort', abort, { once: true });
    const fail = (error: unknown) => {
      failure ??= error;
      stop();
    };
    let checking = false,
      monitor = Promise.resolve();
    const cpuTimer = setInterval(() => {
      if (checking || closed || failure) return;
      checking = true;
      monitor = cpuUsage(id)
        .then((usage) => {
          if (usage > cpu)
            fail(new MoriaError('FUNCTION_CPU_LIMIT', 'Function CPU budget exceeded'));
        })
        .catch(() => {
          if (!closed && !failure)
            fail(
              new MoriaError('FUNCTION_WORKER_UNAVAILABLE', 'Worker resource accounting failed'),
            );
        })
        .finally(() => {
          checking = false;
        });
    }, 200);
    const write = async (value: unknown) => {
      if (!child.stdin.write(JSON.stringify(value) + '\n')) await once(child.stdin, 'drain');
    };
    child.stdin.on('error', () => {});
    child.stderr.on('data', (chunk: Buffer) => {
      total += chunk.length;
      if (total > 262144) fail(new MoriaError('FUNCTION_PROTOCOL', 'Worker output exceeds bounds'));
      // Image diagnostics are untrusted output and never copied into host logs.
    });
    child.stdout.on('data', (chunk: Buffer) => {
      total += chunk.length;
      buffer = Buffer.concat([buffer, chunk]);
      if (total > 262144)
        return fail(new MoriaError('FUNCTION_PROTOCOL', 'Worker output exceeds bounds'));
      let end: number;
      while ((end = buffer.indexOf(10)) !== -1) {
        const line = buffer.subarray(0, end);
        buffer = buffer.subarray(end + 1);
        if (line.length > 65536)
          return fail(new MoriaError('FUNCTION_PROTOCOL', 'Worker frame exceeds bounds'));
        chain = chain
          .then(async () => {
            if (failure) return;
            const message = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(line));
            if (
              !message ||
              message.version !== 1 ||
              acknowledged ||
              (received && message.type !== 'completed')
            )
              throw new MoriaError('FUNCTION_PROTOCOL', 'Invalid worker protocol');
            if (
              message.type === 'completed' &&
              received &&
              message.nonce === completionNonce &&
              Object.keys(message).sort().join(',') === 'nonce,type,version'
            )
              acknowledged = true;
            else if (
              message.type === 'result' &&
              Object.keys(message).sort().join(',') === 'type,value,version'
            ) {
              received = true;
              result = message.value;
              const usage = await cpuUsage(id);
              if (
                (await docker(['inspect', '--format', '{{.State.Running}}', name(id)])).trim() !==
                'true'
              )
                throw new MoriaError(
                  'FUNCTION_WORKER_UNAVAILABLE',
                  'Worker exited before resource accounting',
                );
              if (usage > cpu)
                throw new MoriaError('FUNCTION_CPU_LIMIT', 'Function CPU budget exceeded');
              completionNonce = randomUUID();
              await write({ version: 1, type: 'complete', nonce: completionNonce });
            } else if (
              message.type === 'error' &&
              Object.keys(message).sort().join(',') === 'type,version'
            )
              throw new MoriaError('FUNCTION_FAILED', 'Function code failed');
            else if (
              message.type === 'log' &&
              typeof message.message === 'string' &&
              Object.keys(message).sort().join(',') === 'message,type,version'
            )
              await log(message.message);
            else if (
              ['osdk', 'http'].includes(message.type) &&
              typeof message.id === 'string' &&
              /^[0-9]{1,3}$/.test(message.id) &&
              !calls.has(message.id) &&
              calls.size < 32 &&
              Object.keys(message).sort().join(',') ===
                (message.type === 'http'
                  ? 'body,headers,id,method,path,type,version'
                  : 'id,input,method,path,type,version')
            ) {
              calls.add(message.id);
              try {
                await write({
                  version: 1,
                  type: message.type === 'http' ? 'httpResult' : 'osdkResult',
                  id: message.id,
                  value: await rpc(message),
                });
              } catch (error) {
                if (!(error instanceof MoriaError)) throw error;
                await write({
                  version: 1,
                  type: message.type === 'http' ? 'httpError' : 'osdkError',
                  id: message.id,
                  code: error.code,
                });
              }
            } else throw new MoriaError('FUNCTION_PROTOCOL', 'Invalid worker protocol');
          })
          .catch(fail);
      }
      if (buffer.length > 65536)
        fail(new MoriaError('FUNCTION_PROTOCOL', 'Worker frame exceeds bounds'));
    });
    try {
      if (signal.aborted) abort();
      await write(start);
      const code = await exited;
      await chain;
      if (failure) throw failure;
      if (code !== 0 || !received || !acknowledged || buffer.length)
        throw new MoriaError('FUNCTION_FAILED', 'Function worker exited without a valid result');
      return result;
    } finally {
      clearInterval(cpuTimer);
      await monitor;
      if (cleanupTimer) clearTimeout(cleanupTimer);
      signal.removeEventListener('abort', abort);
      child.stdin.destroy();
      await remove();
    }
  }
}
