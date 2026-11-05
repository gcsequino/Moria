import assert from 'node:assert/strict';
import { randomInt } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import type pg from 'pg';

/** Hold the actual receipt and one protected replay at their PostgreSQL statements. */
export async function evaluationReplaySnapshot(
  pool: pg.Pool,
  execution: string,
  suite: string,
  replayHash: string,
) {
  for (const id of [execution, suite]) assert.match(id, /^[0-9a-f-]{36}$/);
  assert.match(replayHash, /^[0-9a-f]{64}$/);
  const original = (
    await pool.query(
      "SELECT pg_get_functiondef('moria.native_resource_release_access(text,text,text,text)'::regprocedure) AS definition",
    )
  ).rows[0].definition as string;
  const receiptGate = randomInt(1, 2 ** 29);
  const replayGate = receiptGate + 1;
  const blocker = await pool.connect();
  let closed = false;
  const unlock = (key: number) => blocker.query('SELECT pg_advisory_unlock($1::bigint)', [key]);
  await blocker.query('SELECT pg_advisory_lock($1::bigint), pg_advisory_lock($2::bigint)', [
    receiptGate,
    replayGate,
  ]);
  try {
    const delegate = original.replace(
      'moria.native_resource_release_access(',
      'moria.native_release_before_evaluation_replay_test(',
    );
    await pool.query(`
      ${delegate};
      REVOKE ALL ON FUNCTION moria.native_release_before_evaluation_replay_test(text,text,text,text) FROM PUBLIC;
      CREATE FUNCTION public.hold_evaluation_receipt_for_replay() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF NEW.execution='${execution}'::uuid THEN
          PERFORM pg_advisory_xact_lock(${receiptGate}::bigint);
        END IF;
        RETURN NEW;
      END $$;
      CREATE TRIGGER evaluation_receipt_replay_gate BEFORE INSERT ON moria.runtime_evaluation_receipts
        FOR EACH ROW EXECUTE FUNCTION public.hold_evaluation_receipt_for_replay();
      CREATE OR REPLACE FUNCTION moria.native_resource_release_access(token_hash text,resource text,operation text,release_digest text) RETURNS jsonb
      LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
      BEGIN
        IF token_hash='${replayHash}' AND resource='${suite}' AND operation='run' THEN
          PERFORM pg_advisory_xact_lock(${replayGate}::bigint);
        END IF;
        RETURN moria.native_release_before_evaluation_replay_test(token_hash,resource,operation,release_digest);
      END $$`);
  } catch (error) {
    await unlock(receiptGate);
    await unlock(replayGate);
    blocker.release();
    throw error;
  }
  const wait = async (key: number, signal: AbortSignal) => {
    while (!closed) {
      signal.throwIfAborted();
      const { rowCount } = await pool.query(
        "SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=0 AND objid=$1 AND objsubid=1 AND NOT granted",
        [key],
      );
      if (rowCount) return;
      await delay(10, undefined, { signal });
    }
    throw new Error('Evaluation replay snapshot gate closed before the expected statement');
  };
  return {
    async replay<T>(
      request: () => Promise<T>,
      committed: Promise<void>,
      signal: AbortSignal,
    ): Promise<T> {
      let cancel!: () => void;
      const aborted = new Promise<never>((_, reject) => {
        cancel = () => reject(signal.reason);
        if (signal.aborted) cancel();
        else signal.addEventListener('abort', cancel, { once: true });
      });
      try {
        await Promise.race([wait(receiptGate, signal), aborted]);
        const response = request();
        const early = response.then(() =>
          assert.fail('Evaluation replay returned before its protected read was released'),
        );
        await Promise.race([wait(replayGate, signal), early, aborted]);
        await unlock(receiptGate);
        await Promise.race([committed, early, aborted]);
        await unlock(replayGate);
        return await Promise.race([response, aborted]);
      } finally {
        signal.removeEventListener('abort', cancel);
      }
    },
    async close() {
      closed = true;
      try {
        await unlock(receiptGate);
        await unlock(replayGate);
        await pool.query(original);
        await pool.query(
          'DROP TRIGGER evaluation_receipt_replay_gate ON moria.runtime_evaluation_receipts; DROP FUNCTION public.hold_evaluation_receipt_for_replay(); DROP FUNCTION moria.native_release_before_evaluation_replay_test(text,text,text,text)',
        );
      } finally {
        blocker.release();
      }
    },
  };
}
