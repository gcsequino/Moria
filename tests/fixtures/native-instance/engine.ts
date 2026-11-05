import assert from 'node:assert/strict';
import { spawn, execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { realpath } from 'node:fs/promises';
import { basename, dirname, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { setTimeout as delay } from 'node:timers/promises';

type Pool = {
  root: string;
  slots: { registration: Record<string, unknown>; device: string }[];
};

export function instanceEnginePool(): Pool {
  const value = JSON.parse(
    process.env.MORIA_NATIVE_INSTANCE_SCRATCH_POOL_JSON ?? 'null',
  ) as Pool | null;
  assert.ok(
    value && value.slots.length === 2,
    'The composed engine requires two freshly provisioned dedicated ext4 scratch slots',
  );
  return value;
}

export async function startInstanceEngine(input: {
  host: string;
  runtimeEnvironment: NodeJS.ProcessEnv;
  operatorEnvironment: NodeJS.ProcessEnv;
  pool?: Pool;
}) {
  const controller = process.env.MORIA_TEST_CONTROLLER;
  assert.ok(controller, 'A task-owned controller is required for the privileged scratch janitor');
  const binary = process.env.MORIA_RUNTIME_BINARY ?? '/usr/local/bin/moria-runtime';
  const start = (
    arguments_: string[],
    environment: NodeJS.ProcessEnv,
    body?: unknown,
    announce = false,
  ) => {
    const keys = Object.keys(environment).filter((key) => environment[key] !== undefined);
    const child = spawn(
      'docker',
      [
        'exec',
        '--user',
        '0',
        '-i',
        ...keys.flatMap((key) => ['--env', key]),
        controller,
        ...(announce
          ? [
              '/app/.venv/bin/python',
              '-c',
              'import json,os,sys\nprint(json.dumps({"event":"instance-engine","processId":str(os.getpid())}),flush=True)\nos.execv(sys.argv[1],sys.argv[1:])',
            ]
          : []),
        binary,
        ...arguments_,
      ],
      {
        env: { ...process.env, ...environment },
        stdio: ['pipe', 'pipe', 'pipe'],
      },
    );
    let output = '',
      error = '';
    child.stdout.on('data', (bytes) => {
      output = (output + bytes).slice(-65536);
    });
    child.stderr.on('data', (bytes) => {
      error = (error + bytes).slice(-65536);
    });
    child.stdin.on('error', () => {});
    child.stdin.end(body === undefined ? '' : JSON.stringify(body));
    const done = new Promise<void>((resolve, reject) => {
      child.once('error', reject);
      child.once('exit', (code, signal) =>
        code === 0
          ? resolve()
          : reject(Error('Native engine lifecycle failed: ' + (code ?? signal) + ' ' + error)),
      );
    });
    void done.catch(() => {});
    return { child, done, output: () => output };
  };
  const pool = input.pool ?? instanceEnginePool();
  for (const slot of pool.slots) {
    await start(['scratch', 'register'], input.operatorEnvironment, {
      ...slot.registration,
      hostId: input.host,
      resourceClass: 'engine-spill',
    }).done;
  }
  const worker = start(['pipelines', 'worker'], input.runtimeEnvironment, undefined, true);
  let pid: string | undefined;
  const signal = async (name: 'TERM' | 'KILL') => {
    const child = spawn('docker', ['exec', '--user', '0', controller, 'kill', '-' + name, pid!]);
    await new Promise<void>((resolve, reject) => {
      child.once('error', reject);
      child.once('exit', (code) =>
        code === 0 ? resolve() : reject(Error('Owned engine signal failed')),
      );
    });
  };
  try {
    let nativeReady = false;
    const deadline = Date.now() + 10000;
    while (!nativeReady) {
      for (const line of worker.output().split('\n')) {
        try {
          const value = JSON.parse(line);
          if (/^[1-9][0-9]*$/.test(value.processId)) {
            if (value.event === 'instance-engine') pid = value.processId;
            if (value.event === 'pipeline-worker') {
              assert.equal(value.processId, pid);
              nativeReady = true;
            }
          }
        } catch (error) {
          if (error instanceof SyntaxError) continue;
          throw error;
        }
      }
      if (worker.child.exitCode !== null) await worker.done;
      assert.ok(Date.now() < deadline, 'Native engine did not report its own readiness');
      if (!nativeReady) await delay(25);
    }
  } catch (error) {
    if (pid && worker.child.exitCode === null) {
      await signal('TERM');
      await Promise.race([worker.done.catch(() => {}), delay(11000)]);
      if (worker.child.exitCode === null) {
        await signal('KILL');
        await worker.done.catch(() => {});
      }
    } else worker.child.kill('SIGTERM');
    throw error;
  }
  return {
    restoreOwnership: async (directory: string) => {
      assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
      assert.match(basename(directory), /^catalog-storage-[A-Za-z0-9]+$/);
      assert.equal(await realpath(directory), resolve(directory));
      const uid = process.getuid?.(),
        gid = process.getgid?.();
      assert.ok(uid !== undefined && gid !== undefined);
      await promisify(execFile)('docker', [
        'exec',
        '--user',
        '0',
        controller,
        'chown',
        '-R',
        '--no-dereference',
        `${uid}:${gid}`,
        directory,
      ]);
    },
    close: async () => {
      if (worker.child.exitCode !== null) return;
      await signal('TERM');
      await Promise.race([worker.done.catch(() => {}), delay(11000)]);
      if (worker.child.exitCode === null) {
        await signal('KILL');
        await worker.done.catch(() => {});
      }
    },
  };
}
