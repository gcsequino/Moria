import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import type { TestContext } from 'node:test';
import type { Pool } from 'pg';
import { transactionCommitProxy } from '../runtime/postgres-proxy.js';

type Lease = {
  execution: string;
  task: string | null;
  owner: string;
  fence: string;
  incarnation: string;
};
type Native = (command: string, input: unknown, extra?: Record<string, string>) => Promise<any>;

export async function renewalLockFixture(
  t: TestContext,
  pool: Pool,
  native: Native,
  fresh: (publication: boolean) => Promise<Lease>,
  connection: string,
) {
  const leaseUntil = async (lease: Lease) =>
    (
      await pool.query(
        lease.task
          ? 'SELECT lease_until::text until FROM moria.runtime_execution_tasks WHERE id=$1'
          : 'SELECT lease_until::text until FROM moria.runtime_executions WHERE id=$1',
        [lease.task ?? lease.execution],
      )
    ).rows[0].until as string;
  const lock = async (lease: Lease) => {
    const blocker = await pool.connect();
    try {
      await blocker.query('BEGIN');
      await blocker.query('SELECT id FROM moria.runtime_executions WHERE id=$1 FOR SHARE', [
        lease.execution,
      ]);
      return blocker;
    } catch (error) {
      await blocker.query('ROLLBACK');
      blocker.release();
      throw error;
    }
  };
  const proxyFor = async () => {
    const url = new URL(connection);
    // The selector never matches: this proxy forwards every response and records SQLSTATEs only.
    const proxy = await transactionCommitProxy(
      Number(url.port),
      '__renewal_probe_no_response_hold__',
    );
    url.port = String(proxy.port);
    return { proxy, extra: { JOB_DATABASE: url.href } };
  };
  const firstTimeout = async (proxy: Awaited<ReturnType<typeof proxyFor>>['proxy']) => {
    const end = Date.now() + 9000;
    while (!proxy.errors.some((error) => error.code === '55P03') && Date.now() < end)
      await delay(10);
    assert.equal(
      proxy.errors.filter((error) => error.code === '55P03').length,
      1,
      'The real server must abort the first blocked renewal',
    );
    await delay(1000);
  };
  for (const publication of [false, true]) {
    await t.test(
      `${publication ? 'publication' : 'task'} renewal recovers from a six-second shared-row lock`,
      async () => {
        const lease = await fresh(publication);
        assert.equal((await native('renew', lease)).renewed, true);
        const before = await leaseUntil(lease);
        const blocker = await lock(lease);
        const { proxy, extra } = await proxyFor();
        try {
          const pending = native('renew', lease, extra).then(
            (value) => ({ value, error: undefined }),
            (error) => ({ value: undefined, error }),
          );
          await firstTimeout(proxy);
          await blocker.query('ROLLBACK');
          const result = await pending;
          assert.ifError(result.error);
          assert.equal(result.value.renewed, true);
          assert.equal(proxy.errors.filter((error) => error.code === '55P03').length, 1);
          assert.ok(Date.parse(await leaseUntil(lease)) > Date.parse(before));
          console.log(
            JSON.stringify({
              event: 'renewal_lock_recovery',
              phase: publication ? 'publication' : 'task',
              execution: lease.execution,
              serverAborts: 1,
            }),
          );
        } finally {
          await blocker.query('ROLLBACK');
          blocker.release();
          await proxy.close();
          await native('cancel', { id: lease.execution });
        }
      },
    );
  }
  await t.test(
    'persistent renewal lock contention stops after four server-aborted transactions',
    async () => {
      const lease = await fresh(false);
      assert.equal((await native('renew', lease)).renewed, true);
      const before = await leaseUntil(lease);
      const blocker = await lock(lease);
      const { proxy, extra } = await proxyFor();
      const started = Date.now();
      try {
        await assert.rejects(native('renew', lease, extra), /RESOURCE_BUSY|55P03/);
        const elapsed = Date.now() - started;
        assert.equal(proxy.errors.filter((error) => error.code === '55P03').length, 4);
        assert.ok(
          elapsed >= 20000 && elapsed < 26000,
          `Four unchanged five-second lock timeouts must remain bounded: ${elapsed}`,
        );
        assert.equal(await leaseUntil(lease), before, 'Denied renewal cannot extend the lease');
        console.log(
          JSON.stringify({
            event: 'renewal_lock_bounded',
            execution: lease.execution,
            serverAborts: 4,
          }),
        );
      } finally {
        await blocker.query('ROLLBACK');
        blocker.release();
        await proxy.close();
        await native('cancel', { id: lease.execution });
      }
    },
  );
  for (const negative of ['revoked', 'expired', 'fence'] as const) {
    await t.test(
      `renewal retry rechecks ${negative} authority after the lock timeout`,
      async () => {
        const lease = await fresh(false);
        assert.equal((await native('renew', lease)).renewed, true);
        const blocker = await lock(lease);
        const { proxy, extra } = await proxyFor();
        let binding: string | undefined;
        try {
          const pending = native('renew', lease, extra).then(
            (value) => ({ value, error: undefined }),
            (error) => ({ value: undefined, error }),
          );
          await firstTimeout(proxy);
          if (negative === 'revoked') {
            binding = (
              await pool.query('SELECT binding FROM moria.runtime_executions WHERE id=$1', [
                lease.execution,
              ])
            ).rows[0].binding;
            await pool.query(
              'UPDATE moria.runtime_execution_bindings SET enabled=false WHERE id=$1',
              [binding],
            );
          } else if (negative === 'expired') {
            await pool.query(
              "UPDATE moria.runtime_execution_tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
              [lease.task],
            );
          } else {
            await pool.query('UPDATE moria.runtime_execution_tasks SET fence=fence+1 WHERE id=$1', [
              lease.task,
            ]);
          }
          const deniedUntil = await leaseUntil(lease);
          await blocker.query('ROLLBACK');
          const result = await pending;
          assert.ok(result.error, 'A changed authority cannot be renewed');
          assert.equal(result.value, undefined);
          assert.match(
            String(result.error),
            negative === 'revoked' ? /BINDING_CHANGED/ : /LEASE_LOST/,
          );
          assert.equal(proxy.errors.filter((error) => error.code === '55P03').length, 1);
          assert.equal(await leaseUntil(lease), deniedUntil);
          console.log(
            JSON.stringify({
              event: 'renewal_lock_negative',
              kind: negative,
              execution: lease.execution,
              serverAborts: 1,
            }),
          );
        } finally {
          await blocker.query('ROLLBACK');
          blocker.release();
          await proxy.close();
          if (binding)
            await pool.query(
              'UPDATE moria.runtime_execution_bindings SET enabled=true WHERE id=$1',
              [binding],
            );
          await native('cancel', { id: lease.execution });
        }
      },
    );
  }
}
