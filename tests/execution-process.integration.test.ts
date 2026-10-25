import { test } from 'node:test';
import assert from 'node:assert/strict';
import { fork, type ChildProcess } from 'node:child_process';
import { randomUUID, randomBytes } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve } from 'node:path';
import { createServer } from 'node:net';
import EmbeddedPostgres from 'embedded-postgres';
import pg from 'pg';
import { digest } from '../services/security/src/browser-sessions.js';
import { schema } from '../services/database/src/schema.js';

async function freePort() {
  const server = createServer();
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>((r) => server.close(() => r()));
  return port;
}
async function eventually(check: () => Promise<boolean>) {
  const deadline = Date.now() + 15000;
  while (!(await check())) {
    assert.ok(Date.now() < deadline, 'bounded database barrier timed out');
    await new Promise((r) => setTimeout(r, 50));
  }
}
function child(role: string, env: NodeJS.ProcessEnv, pausedListen = 0) {
  const process = fork(
    resolve('tests/fixtures/execution-process/child.ts'),
    [role, String(pausedListen)],
    {
      env,
      stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
    },
  );
  const messages: { event: string; id?: string }[] = [];
  process.on('message', (message) => messages.push(message as (typeof messages)[number]));
  // Startup errors are deliberately generic; never collect connection secrets.
  process.stderr?.resume();
  return {
    process,
    messages,
    async event(event: string) {
      await eventually(async () => {
        if (messages.some((message) => message.event === event)) return true;
        assert.equal(process.exitCode, null, 'child exited before IPC barrier');
        assert.equal(process.signalCode, null, 'child terminated before IPC barrier');
        return false;
      });
      return messages.splice(
        messages.findIndex((message) => message.event === event),
        1,
      )[0];
    },
  };
}
async function stop(process: ChildProcess, signal: NodeJS.Signals = 'SIGTERM') {
  if (process.exitCode !== null || process.signalCode !== null) return;
  process.kill(signal);
  await eventually(async () => process.exitCode !== null || process.signalCode !== null);
}

test(
  'external API and supervisors isolate lifecycle, serialize work and fence recovery',
  { timeout: 120000 },
  async (t) => {
    const directory = await mkdtemp(resolve(tmpdir(), 'moria-process-'));
    const port = await freePort();
    const apiPort = await freePort();
    const snowPort = await freePort();
    const publicPort = await freePort();
    const publicSnowPort = await freePort();
    const password = randomUUID();
    const db = new EmbeddedPostgres({
      databaseDir: resolve(directory, 'postgres'),
      user: 'process_owner',
      password,
      port,
      persistent: true,
      authMethod: 'scram-sha-256',
      initdbFlags: ['--encoding=UTF8', '--locale=C'],
      postgresFlags: ['-c', 'listen_addresses=127.0.0.1'],
      onLog: () => {},
      onError: () => {},
    });
    const children: ChildProcess[] = [];
    let pool: pg.Pool | undefined;
    t.after(async () => {
      await Promise.all(children.map((process) => stop(process, 'SIGKILL')));
      await pool?.end();
      await db.stop();
      await rm(directory, { recursive: true, force: true });
    });
    await db.initialise();
    await db.start();
    pool = new pg.Pool({
      host: '127.0.0.1',
      port,
      user: 'process_owner',
      password,
      database: 'postgres',
    });
    await schema(pool, 'apply');
    await pool.query(`CREATE ROLE process_runtime LOGIN PASSWORD '${password}'`);
    await pool.query('GRANT USAGE ON SCHEMA moria TO process_runtime');
    await pool.query(
      'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA moria TO process_runtime',
    );
    await pool.query('REVOKE UPDATE, DELETE ON moria.security_audit FROM process_runtime');
    await pool.query('REVOKE ALL ON moria.schema_version FROM process_runtime');
    await pool.query('GRANT SELECT ON moria.schema_version TO process_runtime');
    await pool.query('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA moria TO process_runtime');
    await pool.query('CREATE TABLE public.process_publications(id text PRIMARY KEY)');
    await pool.query('GRANT INSERT ON public.process_publications TO process_runtime');
    const env = {
      ...process.env,
      MORIA_DATABASE_MODE: 'external',
      MORIA_DATABASE_SECRET_REF: 'PROCESS_TEST_DATABASE',
      PROCESS_TEST_DATABASE: `postgresql://process_runtime:${password}@127.0.0.1:${port}/postgres`,
      MORIA_DATABASE_TLS: 'disable',
      MORIA_DATABASE_POOL_MAX: '4',
      MORIA_DATABASE_REPLICAS: '3',
      MORIA_DATABASE_CONNECTION_BUDGET: '13',
      MORIA_PUBLIC_ORIGIN: 'https://management.example.test',
      MORIA_PUBLIC_PORT: String(publicPort),
      MORIA_LOCAL_DIR: resolve(directory, 'workspace'),
      MORIA_PORT: String(apiPort),
    };
    const launch = (role: string, pausedListen = 0) => {
      const result = child(role, env, pausedListen);
      children.push(result.process);
      return result;
    };
    let api = launch('api');
    await api.event('ready');
    assert.equal((await fetch(`http://127.0.0.1:${apiPort}/management/v1/jobs`)).status, 401);
    // Seed only a synthetic one-time recovery ticket; exchange it through real auth.
    const ticket = randomBytes(32).toString('base64url');
    await pool.query(
      "INSERT INTO moria.auth_flows(state_hash,environment,application,expires_at) VALUES($1,'local-workspace','local-ui',clock_timestamp()+interval '5 minutes')",
      [digest(ticket)],
    );
    const origin = `http://127.0.0.1:${apiPort}`;
    const recovered = await fetch(`${origin}/api/auth/recover`, {
      method: 'POST',
      headers: { origin, 'content-type': 'application/json' },
      body: JSON.stringify({ ticket }),
    });
    assert.equal(recovered.status, 200);
    const cookie = recovered.headers.get('set-cookie')!.split(';')[0];
    const request = async (path: string, body?: unknown) => {
      const response = await fetch(`${origin}/management/v1/${path}`, {
        method: body === undefined ? 'GET' : 'POST',
        headers: { origin, cookie, 'content-type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
      assert.ok(response.ok, `authorized ${path}: ${response.status}`);
      return response.json();
    };
    const firstInput = { requestKey: 'process/first', input: null };
    const first = (await request('jobs/sample/enqueue', firstInput)).id;
    assert.deepEqual((await pool.query('SELECT input FROM moria.jobs WHERE id=$1', [first])).rows, [
      { input: null },
    ]);
    assert.equal((await request('jobs/sample/enqueue', firstInput)).id, first);
    for (const conflictingInput of [{}, { input: {} }]) {
      const conflict = await fetch(`${origin}/management/v1/jobs/sample/enqueue`, {
        method: 'POST',
        headers: { origin, cookie, 'content-type': 'application/json' },
        body: JSON.stringify({ requestKey: firstInput.requestKey, ...conflictingInput }),
      });
      assert.equal(conflict.status, 409);
      assert.equal((await conflict.json()).error.code, 'IDEMPOTENCY_CONFLICT');
    }
    assert.deepEqual((await pool.query('SELECT input FROM moria.jobs WHERE id=$1', [first])).rows, [
      { input: null },
    ]);
    await request('jobs/periodic/schedule', { enabled: true, intervalMinutes: 60 });
    await new Promise((r) => setTimeout(r, 2300));
    assert.equal((await pool.query('SELECT count(*) FROM moria.job_attempts')).rows[0].count, '0');
    assert.equal((await pool.query('SELECT count(*) FROM moria.jobs')).rows[0].count, '1');
    await request('jobs/periodic/schedule', { enabled: false, intervalMinutes: 60 });
    assert.equal((await request('jobs')).jobs[0].id, first);
    const supervisor = launch('supervisor');
    await supervisor.event('ready');
    assert.equal((await supervisor.event('started')).id, first);
    await stop(api.process);
    assert.equal((await pool.query('SELECT 1 AS alive')).rows[0].alive, 1);
    assert.equal(supervisor.process.exitCode, null);
    // The supervisor leaves both API ports available even while executing.
    for (const listenerPort of [apiPort, snowPort, publicPort, publicSnowPort]) {
      const listener = createServer();
      await new Promise<void>((r, reject) => {
        listener.once('error', reject);
        listener.listen(listenerPort, '127.0.0.1', r);
      });
      await new Promise<void>((r) => listener.close(() => r()));
    }
    // Pause before the native bind, then deliver SIGTERM before resuming it.
    // Cover the first local listener and a public listener after local binds.
    const supervisorConnections = (
      await pool.query(
        "SELECT pid FROM pg_stat_activity WHERE usename='process_runtime' ORDER BY pid",
      )
    ).rows;
    for (const role of ['api', 'combined']) {
      for (const pausedListen of [1, 2]) {
        const starting = launch(role, pausedListen);
        await starting.event('listen-pending');
        starting.process.kill('SIGTERM');
        await starting.event('startup-signal');
        starting.process.send({ command: 'resume-listen' });
        await eventually(async () => starting.process.exitCode !== null);
        assert.equal(starting.process.exitCode, 0);
        assert.equal(starting.process.signalCode, null);
        assert.equal(
          starting.messages.filter((m) => m.event === 'listen-called').length,
          pausedListen,
        );
        assert.equal(starting.messages.filter((m) => m.event === 'jobs-start').length, 0);
        assert.equal(starting.messages.filter((m) => m.event === 'jobs-stop').length, 1);
        assert.equal(starting.messages.filter((m) => m.event === 'ready').length, 0);
        for (const listenerPort of [apiPort, snowPort, publicPort, publicSnowPort]) {
          const listener = createServer();
          await new Promise<void>((r, reject) => {
            listener.once('error', reject);
            listener.listen(listenerPort, '127.0.0.1', r);
          });
          await new Promise<void>((r) => listener.close(() => r()));
        }
        assert.deepEqual(
          (
            await pool.query(
              "SELECT pid FROM pg_stat_activity WHERE usename='process_runtime' ORDER BY pid",
            )
          ).rows,
          supervisorConnections,
        );
        assert.equal((await pool.query('SELECT 1 AS alive')).rows[0].alive, 1);
        assert.equal(supervisor.process.exitCode, null);
        assert.equal(supervisor.process.signalCode, null);
      }
    }
    api = launch('api');
    await api.event('ready');
    assert.equal(
      (await request('jobs/sample/enqueue', { requestKey: 'process/repeat', input: null })).id,
      first,
    );
    const second = launch('supervisor');
    await second.event('ready');
    await second.event('tick');
    assert.equal(
      (await pool.query('SELECT count(*) FROM moria.job_attempts WHERE job_id=$1', [first])).rows[0]
        .count,
      '1',
    );
    supervisor.process.send({ command: 'release' });
    await supervisor.event('published');
    await eventually(
      async () =>
        (await pool!.query('SELECT count(*) FROM public.process_publications')).rows[0].count ===
        '1',
    );
    await stop(second.process);
    const recovery = (await request('jobs/sample/enqueue', { requestKey: 'process/recovery' })).id;
    assert.deepEqual(
      (await pool.query('SELECT input FROM moria.jobs WHERE id=$1', [recovery])).rows,
      [{ input: {} }],
    );
    assert.equal(
      (
        await request('jobs/sample/enqueue', {
          requestKey: 'process/recovery',
          input: {},
        })
      ).id,
      recovery,
    );
    await supervisor.event('started');
    // Lose just the supervisor lock connection while its handler is at an IPC barrier.
    await pool.query(`SELECT pg_terminate_backend(a.pid) FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid
    WHERE a.usename='process_runtime' AND l.locktype='advisory' AND l.mode='ExclusiveLock' AND l.granted`);
    const replacement = launch('worker');
    await replacement.event('ready');
    assert.equal((await replacement.event('started')).id, recovery);
    supervisor.process.send({ command: 'release' });
    await supervisor.event('rejected');
    await stop(supervisor.process);
    replacement.process.send({ command: 'release' });
    await replacement.event('published');
    assert.deepEqual(
      (
        await pool.query('SELECT status FROM moria.job_attempts WHERE job_id=$1 ORDER BY attempt', [
          recovery,
        ])
      ).rows.map((row) => row.status),
      ['interrupted', 'succeeded'],
    );
    assert.equal(
      (await pool.query('SELECT count(*) FROM public.process_publications WHERE id=$1', [recovery]))
        .rows[0].count,
      '1',
    );
    const crashed = (await request('jobs/sample/enqueue', { requestKey: 'process/hard-crash' })).id;
    assert.equal((await replacement.event('started')).id, crashed);
    await stop(replacement.process, 'SIGKILL');
    const finalSupervisor = launch('worker');
    await finalSupervisor.event('ready');
    assert.equal((await finalSupervisor.event('started')).id, crashed);
    // Graceful shutdown drains the active handler before releasing its own pool.
    finalSupervisor.process.kill('SIGTERM');
    await finalSupervisor.event('draining');
    finalSupervisor.process.send({ command: 'release' });
    await finalSupervisor.event('published');
    await eventually(async () => finalSupervisor.process.exitCode !== null);
    assert.deepEqual(
      (
        await pool.query('SELECT status FROM moria.job_attempts WHERE job_id=$1 ORDER BY attempt', [
          crashed,
        ])
      ).rows.map((row) => row.status),
      ['interrupted', 'succeeded'],
    );
    assert.equal((await fetch(`http://127.0.0.1:${apiPort}/management/v1/jobs`)).status, 401);
    const failed = randomUUID();
    await pool.query(
      "INSERT INTO moria.jobs(id,kind,input,request_key,requested_by,status) VALUES($1,'sample','{}','process/failed','synthetic-test','failed')",
      [failed],
    );
    assert.equal((await request('jobs/retry', { id: failed })).status, 'queued');
    assert.equal(
      (await request('jobs')).jobs.find((job: { id: string }) => job.id === failed).status,
      'queued',
    );
    await stop(api.process);
    assert.equal(
      (await pool.query("SELECT count(*) FROM pg_stat_activity WHERE usename='process_runtime'"))
        .rows[0].count,
      '0',
    );
  },
);
