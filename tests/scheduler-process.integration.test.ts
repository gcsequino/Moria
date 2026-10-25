import { test } from 'node:test';
import assert from 'node:assert/strict';
import { fork, type ChildProcess } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve } from 'node:path';
import { createServer } from 'node:net';
import EmbeddedPostgres from 'embedded-postgres';
import pg from 'pg';
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
function child(role: string, env: NodeJS.ProcessEnv, pausedListen = false) {
  const process = fork(
    resolve('tests/fixtures/scheduler-process/child.ts'),
    [role, pausedListen ? 'pause' : ''],
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
  'schedulers commit durable firings independently of serial workers',
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
    const launch = (role: string, pausedListen = false) => {
      const result = child(role, env, pausedListen);
      children.push(result.process);
      return result;
    };
    const scheduler = launch('scheduler');
    await scheduler.event('ready');
    await scheduler.event('schedule-tick');
    await stop(scheduler.process);
    const due = '2020-01-01T00:00:00.000Z';
    await pool.query(
      "UPDATE moria.job_schedules SET enabled=true,next_run=$1 WHERE kind='periodic'",
      [due],
    );
    const worker = launch('worker');
    await worker.event('ready');
    await worker.event('tick');
    assert.equal((await pool.query('SELECT count(*) FROM moria.jobs')).rows[0].count, '0');
    assert.equal(
      (
        await pool.query("SELECT next_run FROM moria.job_schedules WHERE kind='periodic'")
      ).rows[0].next_run.toISOString(),
      due,
    );
    await stop(worker.process);

    // Hold the first scheduler after enqueue but before advancement in its transaction.
    const crashed = launch('scheduler', true);
    await crashed.event('before-advance');
    assert.equal((await pool.query('SELECT count(*) FROM moria.jobs')).rows[0].count, '0');
    const competitor = launch('scheduler');
    await competitor.event('ready');
    await stop(crashed.process, 'SIGKILL');
    await competitor.event('schedule-tick');
    const restarted = launch('scheduler');
    await restarted.event('ready');
    await restarted.event('schedule-tick');
    assert.equal((await pool.query('SELECT count(*) FROM moria.jobs')).rows[0].count, '1');
    assert.equal((await pool.query('SELECT count(*) FROM moria.job_requests')).rows[0].count, '1');
    assert.equal((await pool.query('SELECT count(*) FROM moria.job_attempts')).rows[0].count, '0');
    assert.equal(
      (await pool.query('SELECT request_key FROM moria.job_requests')).rows[0].request_key,
      `schedule:periodic:${due}`,
    );
    await stop(restarted.process);
    await stop(competitor.process);

    // A concurrent configuration update waits for dispatch, then owns the final state.
    await pool.query("UPDATE moria.job_schedules SET next_run='2020-01-02' WHERE kind='periodic'");
    const configuring = launch('scheduler', true);
    await configuring.event('before-advance');
    configuring.process.send({ command: 'configure' });
    await eventually(
      async () =>
        Number(
          (
            await pool!.query(
              "SELECT count(*) FROM pg_stat_activity WHERE usename='process_runtime' AND wait_event_type='Lock' AND query LIKE 'UPDATE moria.job_schedules SET enabled=%'",
            )
          ).rows[0].count,
        ) === 1,
    );
    const duplicate = launch('scheduler');
    await duplicate.event('ready');
    await eventually(
      async () =>
        Number(
          (
            await pool!.query(
              "SELECT count(*) FROM pg_stat_activity WHERE usename='process_runtime' AND wait_event_type='Lock' AND query LIKE 'SELECT * FROM moria.job_schedules WHERE enabled%'",
            )
          ).rows[0].count,
        ) === 1,
    );
    configuring.process.send({ command: 'advance' });
    await configuring.event('configured');
    await configuring.event('schedule-tick');
    await duplicate.event('schedule-tick');
    const configured = (
      await pool.query(
        "SELECT enabled,interval_minutes,next_run>clock_timestamp()-interval '1 minute' AS fresh FROM moria.job_schedules WHERE kind='periodic'",
      )
    ).rows[0];
    assert.deepEqual(configured, { enabled: false, interval_minutes: 120, fresh: true });
    assert.equal((await pool.query('SELECT count(*) FROM moria.jobs')).rows[0].count, '1');
    assert.equal((await pool.query('SELECT count(*) FROM moria.job_requests')).rows[0].count, '2');
    await stop(configuring.process);
    await stop(duplicate.process);

    const executing = launch('worker');
    await executing.event('ready');
    const first = (await executing.event('started')).id;
    const fence = (await pool.query('SELECT fence FROM moria.jobs WHERE id=$1', [first])).rows[0]
      .fence;
    await pool.query(
      "UPDATE moria.job_schedules SET enabled=true,next_run=$1 WHERE kind='sample'",
      [due],
    );
    const independent = launch('scheduler');
    await independent.event('ready');
    await independent.event('schedule-tick');
    assert.equal((await pool.query('SELECT count(*) FROM moria.jobs')).rows[0].count, '2');
    assert.equal((await pool.query('SELECT count(*) FROM moria.job_attempts')).rows[0].count, '1');
    assert.equal(
      (await pool.query('SELECT fence FROM moria.jobs WHERE id=$1', [first])).rows[0].fence,
      fence,
    );
    // Scheduler shutdown does not affect the blocked worker or external database.
    await stop(independent.process);
    assert.equal(executing.process.exitCode, null);
    executing.process.kill('SIGTERM');
    await executing.event('draining');
    executing.process.send({ command: 'release' });
    await executing.event('published');
    await eventually(async () => executing.process.exitCode !== null);
    assert.equal(executing.process.exitCode, 0);
    assert.equal(
      (await pool.query('SELECT count(*) FROM public.process_publications')).rows[0].count,
      '1',
    );
    assert.equal(
      (await pool.query("SELECT count(*) FROM pg_stat_activity WHERE usename='process_runtime'"))
        .rows[0].count,
      '0',
    );
  },
);
