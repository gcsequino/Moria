import assert from 'node:assert/strict';
import { randomInt } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import type pg from 'pg';

export async function childProgressLockFixture(
  pool: pg.Pool,
  tool: string,
): Promise<{ done: Promise<void>; close: () => Promise<void> }> {
  assert.match(tool, /^[0-9a-f-]{36}$/);
  const original = (
    await pool.query(
      "SELECT pg_get_functiondef('moria.native_resource_release_access(text,text,text,text)'::regprocedure) definition",
    )
  ).rows[0].definition as string;
  await pool.query(
    original.replace(
      'moria.native_resource_release_access(',
      'moria.native_resource_release_access_before_progress_test(',
    ),
  );
  await pool.query(
    'REVOKE ALL ON FUNCTION moria.native_resource_release_access_before_progress_test(text,text,text,text) FROM PUBLIC; CREATE TABLE public.child_progress_probe_state(armed boolean NOT NULL); INSERT INTO public.child_progress_probe_state VALUES(true)',
  );
  const gate = randomInt(1, 2 ** 30);
  const blocker = await pool.connect();
  try {
    await blocker.query('SELECT pg_advisory_lock($1::bigint)', [gate]);
    await pool.query(`CREATE OR REPLACE FUNCTION moria.native_resource_release_access(token_hash text,resource text,operation text,release_digest text) RETURNS jsonb
      LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
      DECLARE private_hash text:=token_hash; requested_resource text:=resource; observed_parent uuid; prior_name text;
      BEGIN
        IF requested_resource='${tool}' AND operation='invoke' AND (SELECT armed FROM public.child_progress_probe_state) THEN
          SELECT s.execution INTO observed_parent FROM moria.runtime_job_sessions s
            JOIN moria.runtime_job_delegations d ON d.execution=s.execution AND d.origin_kind='service'
            JOIN moria.runtime_executions e ON e.id=s.execution AND e.spec->>'domain'='agent'
            WHERE s.token_hash=private_hash AND EXISTS(SELECT 1 FROM moria.runtime_function_invocations f
              JOIN moria.runtime_executions child ON child.id=f.execution WHERE f.parent_id=e.id
                AND child.resource::text=requested_resource AND child.status IN('queued','running','preparing'));
          IF FOUND AND pg_try_advisory_xact_lock(${gate + 1}::bigint) THEN
            prior_name:=current_setting('application_name');
            PERFORM set_config('application_name','moria-child-progress/'||observed_parent::text,true);
            PERFORM pg_advisory_xact_lock(${gate}::bigint);
            UPDATE public.child_progress_probe_state SET armed=false;
            PERFORM set_config('application_name',prior_name,true);
          END IF;
        END IF;
        RETURN moria.native_resource_release_access_before_progress_test(token_hash,resource,operation,release_digest);
      END $$`);
  } catch (error) {
    await blocker.query('SELECT pg_advisory_unlock($1::bigint)', [gate]).catch(() => {});
    blocker.release();
    throw error;
  }
  let stopped = false;
  const done = (async () => {
    try {
      const end = Date.now() + 90000;
      while (!stopped && Date.now() < end) {
        const waiting = (
          await pool.query(
            "SELECT a.pid,a.application_name FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE l.locktype='advisory' AND l.classid=0 AND l.objid=$1 AND l.objsubid=1 AND NOT l.granted AND a.application_name LIKE 'moria-child-progress/%' LIMIT 1",
            [gate],
          )
        ).rows[0];
        if (!waiting) {
          await delay(10);
          continue;
        }
        const parent = (waiting.application_name as string).split('/')[1];
        assert.match(parent, /^[0-9a-f-]{36}$/);
        assert.equal(
          (
            await pool.query(
              'SELECT id FROM moria.runtime_executions WHERE id=$1 FOR SHARE NOWAIT',
              [parent],
            )
          ).rows[0].id,
          parent,
          'Slow child authorization must allow the child claim to share its live parent row',
        );
        await assert.rejects(
          pool.query('SELECT id FROM moria.runtime_executions WHERE id=$1 FOR UPDATE NOWAIT', [
            parent,
          ]),
          (error: any) => error.code === '55P03',
          'The progress transaction must retain its shared parent authority fence',
        );
        console.log(
          JSON.stringify({
            event: 'child_progress_parent_lock',
            pid: waiting.pid,
            parent,
            sharedProbe: true,
            exclusiveProbeBlocked: true,
          }),
        );
        return;
      }
      assert.fail('The actual service Agent must reach its native child progress authorization');
    } finally {
      await blocker.query('SELECT pg_advisory_unlock($1::bigint)', [gate]);
    }
  })();
  void done.catch(() => {});
  return {
    done,
    close: async () => {
      stopped = true;
      try {
        await blocker.query('SELECT pg_advisory_unlock($1::bigint)', [gate]);
        await done.catch(() => {});
        await pool.query(original);
      } finally {
        blocker.release();
      }
      // The owned database removes inactive probe objects after workers stop.
    },
  };
}
