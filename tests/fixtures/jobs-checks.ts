import { fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import type { Catalog } from '../../services/catalog/src/catalog.js';
import { Jobs } from '../../services/execution/src/jobs.js';
import { MoriaError } from '../../packages/contracts/types.js';

export async function jobsChecks(catalog: Catalog) {
  let fail = false,
    rollback = false;
  const kind = 'test-jobs';
  const jobs = new Jobs(catalog, {
    [kind]: async (job) => {
      await job.phase('Test publication');
      if (fail) throw new Error('Transient source failure');
      await catalog.transaction(async (c) => {
        await job.guard(c);
        await c.query(
          'INSERT INTO public.job_test_publications(id) VALUES($1) ON CONFLICT DO NOTHING',
          [job.id],
        );
        await job.complete(c, { published: true });
        if (rollback) throw new Error('Crash before transaction commit');
      });
      return { published: true };
    },
  });
  await jobs.migrate();
  await catalog.pool.query('CREATE TABLE public.job_test_publications(id uuid PRIMARY KEY)');
  const unknown = new Jobs(catalog, { 'unregistered-workload': async () => {} });
  await unknown.migrate();
  const unknownJob = await unknown.enqueue(
    'unregistered-workload',
    {},
    'jobs-test:unknown',
    'test',
  );
  await unknown.schedule('unregistered-workload', true, 60);
  await jobs.tick();
  assert.equal(
    (await catalog.pool.query('SELECT status FROM moria.jobs WHERE id=$1', [unknownJob.id])).rows[0]
      .status,
    'queued',
  );
  assert.equal(
    (await catalog.pool.query('SELECT 1 FROM moria.job_attempts WHERE job_id=$1', [unknownJob.id]))
      .rowCount,
    0,
  );
  await catalog.pool.query("UPDATE moria.jobs SET status='running' WHERE id=$1", [unknownJob.id]);
  await catalog.pool.query(
    "INSERT INTO moria.job_attempts(job_id,attempt,status) VALUES($1,1,'running')",
    [unknownJob.id],
  );
  await jobs.tick();
  assert.equal(
    (await catalog.pool.query('SELECT status FROM moria.jobs WHERE id=$1', [unknownJob.id])).rows[0]
      .status,
    'running',
  );
  assert.equal(
    (
      await catalog.pool.query('SELECT status FROM moria.job_attempts WHERE job_id=$1', [
        unknownJob.id,
      ])
    ).rows[0].status,
    'running',
  );
  await catalog.pool.query("UPDATE moria.jobs SET status='failed' WHERE id=$1", [unknownJob.id]);
  await catalog.pool.query("UPDATE moria.job_attempts SET status='interrupted' WHERE job_id=$1", [
    unknownJob.id,
  ]);
  await unknown.schedule('unregistered-workload', false, 60);
  const first = await jobs.enqueue(kind, { a: 1, b: 2 }, 'jobs-test:first', 'test');
  assert.equal((await jobs.enqueue(kind, { b: 2, a: 1 }, 'jobs-test:first', 'test')).id, first.id);
  assert.equal(
    (await jobs.enqueue(kind, { a: 1, b: 2 }, 'jobs-test:coalesce', 'test')).id,
    first.id,
  );
  await assert.rejects(
    jobs.enqueue(kind, { a: 3 }, 'jobs-test:first', 'test'),
    (e: unknown) => e instanceof MoriaError && e.code === 'IDEMPOTENCY_CONFLICT',
  );
  await assert.rejects(
    jobs.enqueue(kind, { different: true }, 'jobs-test:busy', 'test'),
    (e: unknown) => e instanceof MoriaError && e.code === 'WORKLOAD_BUSY',
  );
  await jobs.schedule(kind, true, 60);
  await catalog.pool.query(
    "UPDATE moria.job_schedules SET next_run=clock_timestamp()-interval '20 days' WHERE kind=$1",
    [kind],
  );
  await jobs.tick();
  assert.equal(
    (await catalog.pool.query('SELECT count(*)::int AS n FROM moria.jobs WHERE kind=$1', [kind]))
      .rows[0].n,
    1,
  );
  assert.equal(
    (
      await catalog.pool.query(
        'SELECT next_run>clock_timestamp() AS future FROM moria.job_schedules WHERE kind=$1',
        [kind],
      )
    ).rows[0].future,
    true,
  );
  await jobs.schedule(kind, false, 60);
  assert.equal(
    (await catalog.pool.query('SELECT status FROM moria.jobs WHERE id=$1', [first.id])).rows[0]
      .status,
    'succeeded',
  );
  assert.equal(
    (await jobs.enqueue(kind, { a: 1, b: 2 }, 'jobs-test:coalesce', 'test')).id,
    first.id,
  );
  rollback = true;
  const second = await jobs.enqueue(kind, {}, 'jobs-test:rollback', 'test');
  await jobs.tick();
  assert.equal(
    (
      await catalog.pool.query('SELECT 1 FROM public.job_test_publications WHERE id=$1', [
        second.id,
      ])
    ).rowCount,
    0,
  );
  assert.equal(
    (await catalog.pool.query('SELECT result FROM moria.jobs WHERE id=$1', [second.id])).rows[0]
      .result,
    null,
  );
  rollback = false;
  fail = true;
  for (let i = 0; i < 2; i++) {
    await catalog.pool.query('UPDATE moria.jobs SET available_at=clock_timestamp() WHERE id=$1', [
      second.id,
    ]);
    await jobs.tick();
  }
  assert.equal(
    (await catalog.pool.query('SELECT status FROM moria.jobs WHERE id=$1', [second.id])).rows[0]
      .status,
    'failed',
  );
  fail = false;
  await jobs.retry(second.id);
  await jobs.tick();
  assert.deepEqual(
    (
      await catalog.pool.query(
        'SELECT status FROM moria.job_attempts WHERE job_id=$1 ORDER BY attempt',
        [second.id],
      )
    ).rows.map((r) => r.status),
    ['failed', 'failed', 'failed', 'succeeded'],
  );
  const interrupted = await jobs.enqueue(kind, {}, 'jobs-test:restart', 'test');
  const oldFence = randomUUID();
  await catalog.pool.query(
    "UPDATE moria.jobs SET status='running',attempt=1,fence=$2,heartbeat_at=clock_timestamp() WHERE id=$1",
    [interrupted.id, oldFence],
  );
  await catalog.pool.query(
    "INSERT INTO moria.job_attempts(job_id,attempt,status) VALUES($1,1,'running')",
    [interrupted.id],
  );
  const restarted = new Jobs(catalog, jobs.handlers);
  await restarted.tick();
  assert.deepEqual(
    (
      await catalog.pool.query(
        'SELECT status FROM moria.job_attempts WHERE job_id=$1 ORDER BY attempt',
        [interrupted.id],
      )
    ).rows.map((r) => r.status),
    ['interrupted', 'succeeded'],
  );
  assert.notEqual(
    (await catalog.pool.query('SELECT fence FROM moria.jobs WHERE id=$1', [interrupted.id])).rows[0]
      .fence,
    oldFence,
  );
  const fenced = new Jobs(catalog, {
    'test-fenced': async (job) => {
      await catalog.pool.query('UPDATE moria.jobs SET fence=$2 WHERE id=$1', [
        job.id,
        randomUUID(),
      ]);
      await assert.rejects(
        catalog.transaction(async (c) => {
          await job.guard(c);
          await c.query('INSERT INTO public.job_test_publications(id) VALUES($1)', [job.id]);
        }),
        (e: unknown) => e instanceof MoriaError && e.code === 'LEASE_LOST',
      );
      throw new Error('Old worker exits');
    },
  });
  await fenced.migrate();
  const stale = await fenced.enqueue('test-fenced', {}, 'jobs-test:fenced', 'test');
  await fenced.tick();
  assert.equal(
    (await catalog.pool.query('SELECT 1 FROM public.job_test_publications WHERE id=$1', [stale.id]))
      .rowCount,
    0,
  );
  assert.equal(
    (await catalog.pool.query('SELECT status FROM moria.job_attempts WHERE job_id=$1', [stale.id]))
      .rows[0].status,
    'running',
    'stale worker must not fail replacement attempt',
  );
  // Leave no active fixture workload for subsequent integration tests.
  await catalog.pool.query("UPDATE moria.jobs SET status='failed' WHERE id=$1", [stale.id]);
  await catalog.pool.query("UPDATE moria.job_attempts SET status='interrupted' WHERE job_id=$1", [
    stale.id,
  ]);
  // Kill a real worker process while its publication transaction is open, then
  // again after commit but before the handler acknowledges completion.
  for (const mode of ['before', 'after']) {
    const crashKind = `test-crash-${mode}`;
    const supervisor = new Jobs(catalog, {
      [crashKind]: async (job) => {
        await catalog.transaction(async (c) => {
          await job.guard(c);
          await c.query(
            'INSERT INTO public.job_test_publications(id) VALUES($1) ON CONFLICT DO NOTHING',
            [job.id],
          );
          await job.complete(c, { published: true });
        });
      },
    });
    await supervisor.migrate();
    const pending = await supervisor.enqueue(crashKind, {}, `jobs-test:crash-${mode}`, 'test');
    const child = fork(fileURLToPath(new URL('./jobs-crash-child.ts', import.meta.url)), [], {
      execArgv: ['--import', 'tsx'],
      stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
    });
    try {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(
          () => reject(new Error('Crash fixture did not reach publication boundary')),
          15000,
        );
        child.once('message', () => {
          clearTimeout(timer);
          resolve();
        });
        child.once('error', (e) => {
          clearTimeout(timer);
          reject(e);
        });
        child.once('exit', () => {
          clearTimeout(timer);
          reject(new Error('Crash worker exited early'));
        });
        const options = catalog.pool.options;
        child.send({
          mode,
          kind: crashKind,
          connection: {
            host: options.host,
            port: options.port,
            user: options.user,
            password: options.password,
            database: options.database,
            max: 4,
          },
        });
      });
      await new Promise<void>((resolve) => {
        child.once('exit', () => resolve());
        child.kill('SIGKILL');
      });
      const saved = (
        await catalog.pool.query('SELECT 1 FROM public.job_test_publications WHERE id=$1', [
          pending.id,
        ])
      ).rowCount;
      assert.equal(saved, mode === 'after' ? 1 : 0);
      await supervisor.tick();
      assert.equal(
        (await catalog.pool.query('SELECT status FROM moria.jobs WHERE id=$1', [pending.id]))
          .rows[0].status,
        'succeeded',
      );
      assert.equal(
        (
          await catalog.pool.query(
            'SELECT count(*)::int AS n FROM public.job_test_publications WHERE id=$1',
            [pending.id],
          )
        ).rows[0].n,
        1,
      );
      assert.deepEqual(
        (
          await catalog.pool.query(
            'SELECT status FROM moria.job_attempts WHERE job_id=$1 ORDER BY attempt',
            [pending.id],
          )
        ).rows.map((r) => r.status),
        mode === 'before' ? ['interrupted', 'succeeded'] : ['succeeded'],
      );
    } finally {
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    }
  }
}
