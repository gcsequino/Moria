import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import type pg from 'pg';

export async function evaluationProgressRetryFixture(
  pool: pg.Pool,
  agent: string,
): Promise<{ done: Promise<void>; arm: (evaluation: string) => void; close: () => Promise<void> }> {
  assert.match(agent, /^[0-9a-f-]{36}$/);
  const originalRelease = (
    await pool.query(
      "SELECT pg_get_functiondef('moria.native_resource_release_access(text,text,text,text)'::regprocedure) definition",
    )
  ).rows[0].definition as string;
  const originalResume = (
    await pool.query(
      "SELECT pg_get_functiondef('moria.runtime_evaluation_agent_resume(text,uuid)'::regprocedure) definition",
    )
  ).rows[0].definition as string;
  await pool.query(
    originalRelease.replace(
      'moria.native_resource_release_access(',
      'moria.native_release_before_evaluation_retry_test(',
    ),
  );
  await pool.query(`REVOKE ALL ON FUNCTION moria.native_release_before_evaluation_retry_test(text,text,text,text) FROM PUBLIC;
    CREATE TABLE public.evaluation_progress_targets(kind text PRIMARY KEY,case_execution uuid NOT NULL,child uuid NOT NULL);
    CREATE TABLE public.evaluation_progress_successes(kind text NOT NULL,fault_number bigint NOT NULL,case_execution uuid NOT NULL,child uuid NOT NULL,PRIMARY KEY(kind,fault_number,case_execution));
    CREATE TABLE public.evaluation_progress_resumes(case_execution uuid NOT NULL,child uuid NOT NULL,faults bigint NOT NULL,successes bigint NOT NULL);
    CREATE SEQUENCE public.evaluation_progress_intermittent START 1;
    CREATE SEQUENCE public.evaluation_progress_persistent START 1;`);
  await pool.query(`CREATE OR REPLACE FUNCTION moria.native_resource_release_access(token_hash text,resource text,operation text,release_digest text) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
    DECLARE private_hash text:=token_hash; requested_resource text:=resource; selected_kind text; selected_case uuid; selected_child uuid; injected_count bigint; result jsonb;
    BEGIN
      IF requested_resource='${agent}' AND operation='invoke' THEN
        SELECT target.kind,target.case_execution,target.child INTO selected_kind,selected_case,selected_child
          FROM public.evaluation_progress_targets target JOIN moria.runtime_job_sessions credential ON credential.execution=target.case_execution
          JOIN moria.runtime_evaluation_cases c ON c.execution=target.case_execution AND c.agent_execution=target.child AND c.outcome IN('pending','running')
          WHERE credential.token_hash=private_hash;
        IF FOUND THEN
          IF selected_kind='intermittent' THEN
            SELECT CASE WHEN is_called THEN last_value ELSE 0 END INTO injected_count FROM public.evaluation_progress_intermittent;
            IF injected_count<5 AND (injected_count=0 OR EXISTS(SELECT 1 FROM public.evaluation_progress_successes witness WHERE witness.kind=selected_kind AND witness.case_execution=selected_case AND witness.fault_number=injected_count)) THEN
              PERFORM nextval('public.evaluation_progress_intermittent');
              RAISE EXCEPTION 'EVALUATION_PROGRESS_INTERMITTENT' USING ERRCODE='40001';
            END IF;
          ELSE
            SELECT CASE WHEN is_called THEN last_value ELSE 0 END INTO injected_count FROM public.evaluation_progress_persistent;
            IF injected_count<4 THEN
              PERFORM nextval('public.evaluation_progress_persistent');
              RAISE EXCEPTION 'EVALUATION_PROGRESS_PERSISTENT' USING ERRCODE='40001';
            END IF;
          END IF;
          result:=moria.native_release_before_evaluation_retry_test(token_hash,resource,operation,release_digest);
          INSERT INTO public.evaluation_progress_successes(kind,fault_number,case_execution,child) VALUES(selected_kind,injected_count,selected_case,selected_child) ON CONFLICT DO NOTHING;
          RETURN result;
        END IF;
      END IF;
      RETURN moria.native_release_before_evaluation_retry_test(token_hash,resource,operation,release_digest);
    END $$`);
  const resumeReturns = originalResume
    .split('\n')
    .filter((line) => line.trim() === 'RETURN accepted;');
  assert.equal(resumeReturns.length, 1);
  const resumeReturn = resumeReturns[0];
  const resumeProbe = originalResume.replace(
    resumeReturn,
    `  IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname=session_user) THEN
    INSERT INTO public.evaluation_progress_resumes(case_execution,child,faults,successes)
      SELECT case_execution_id,accepted,(SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM public.evaluation_progress_persistent),
        (SELECT count(*) FROM public.evaluation_progress_successes witness WHERE witness.kind='persistent' AND witness.case_execution=case_execution_id)
      FROM public.evaluation_progress_targets target WHERE target.kind='persistent' AND target.case_execution=case_execution_id AND target.child=accepted;
  END IF;
` + resumeReturn,
  );
  assert.equal(resumeProbe.split('moria.runtime_executions').length - 1, 2);
  await pool.query(resumeProbe);
  let stopped = false;
  let evaluation: string | undefined;
  const targets = new Map<string, { execution: string; child: string }>();
  const done = (async () => {
    const end = Date.now() + 90000;
    while (!stopped && Date.now() < end) {
      if (!evaluation) {
        await delay(20);
        continue;
      }
      const cases = (
        await pool.query(
          "SELECT c.execution,c.agent_execution,c.repeat FROM moria.runtime_evaluation_cases c JOIN moria.runtime_job_delegations d ON d.execution=c.execution AND d.origin_kind='service' WHERE c.evaluation=$1 AND c.case_id='pass' AND c.repeat IN(0,1) AND c.agent_execution IS NOT NULL",
          [evaluation],
        )
      ).rows;
      for (const row of cases) {
        const kind = row.repeat === 0 ? 'intermittent' : 'persistent';
        const known = targets.get(kind);
        if (known) {
          assert.deepEqual(
            known,
            { execution: row.execution, child: row.agent_execution },
            'The accepted case and Agent UUID must remain pinned',
          );
        } else {
          await pool.query(
            'INSERT INTO public.evaluation_progress_targets(kind,case_execution,child) VALUES($1,$2,$3)',
            [kind, row.execution, row.agent_execution],
          );
          targets.set(kind, { execution: row.execution, child: row.agent_execution });
        }
      }
      const counts = (
        await pool.query(`SELECT
          (SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM public.evaluation_progress_intermittent)::int intermittent,
          (SELECT count(*) FROM public.evaluation_progress_successes WHERE kind='intermittent' AND fault_number BETWEEN 1 AND 5)::int committed_successes,
          (SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM public.evaluation_progress_persistent)::int persistent,
          (SELECT count(*) FROM public.evaluation_progress_resumes WHERE faults=4 AND successes=0)::int bounded_resumes,
          (SELECT count(*) FROM public.evaluation_progress_successes WHERE kind='persistent')::int recovered`)
      ).rows[0];
      if (
        counts.intermittent === 5 &&
        counts.committed_successes === 5 &&
        counts.persistent === 4 &&
        counts.bounded_resumes > 0 &&
        counts.recovered > 0
      ) {
        const identity = (
          await pool.query(
            'SELECT target.kind,target.case_execution,target.child,t.attempts,e.input_hash=c.input_hash AS exact_input,d.parent_execution=c.execution AS exact_parent FROM public.evaluation_progress_targets target JOIN moria.runtime_evaluation_cases c ON c.execution=target.case_execution AND c.agent_execution=target.child JOIN moria.runtime_executions e ON e.id=target.child JOIN moria.runtime_execution_tasks t ON t.execution=e.id JOIN moria.runtime_job_delegations d ON d.execution=e.id ORDER BY target.kind',
          )
        ).rows;
        assert.equal(identity.length, 2);
        for (const row of identity) {
          assert.equal(row.attempts, 1);
          assert.equal(row.exact_input, true);
          assert.equal(row.exact_parent, true);
        }
        console.log(
          JSON.stringify({ event: 'evaluation_progress_transaction_retries', ...counts, identity }),
        );
        return;
      }
      await delay(20);
    }
    const state = (
      await pool.query(
        'SELECT target.kind,c.outcome,case_work.status AS case_status,child.status AS agent_status,child.error AS agent_error,task.status AS task_status,sandbox.state AS sandbox_state FROM public.evaluation_progress_targets target JOIN moria.runtime_evaluation_cases c ON c.execution=target.case_execution JOIN moria.runtime_executions case_work ON case_work.id=c.execution JOIN moria.runtime_executions child ON child.id=target.child LEFT JOIN moria.runtime_execution_tasks task ON task.execution=child.id LEFT JOIN moria.runtime_sandbox_attempts sandbox ON sandbox.execution=child.id ORDER BY target.kind',
      )
    ).rows;
    assert.fail(
      'Five intermittent committed progress transactions and the four-conflict boundary must execute on their original Agent UUIDs. ' +
        JSON.stringify(state),
    );
  })();
  void done.catch(() => {});
  return {
    done,
    arm: (id) => {
      assert.match(id, /^[0-9a-f-]{36}$/);
      assert.equal(evaluation, undefined);
      evaluation = id;
    },
    close: async () => {
      stopped = true;
      await done.catch(() => {});
      const state = (
        await pool.query(`SELECT
        (SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM public.evaluation_progress_intermittent)::int intermittent,
        (SELECT count(*) FROM public.evaluation_progress_successes WHERE kind='intermittent' AND fault_number BETWEEN 1 AND 5)::int committed_successes,
        (SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM public.evaluation_progress_persistent)::int persistent,
        (SELECT count(*) FROM public.evaluation_progress_resumes WHERE faults=4 AND successes=0)::int bounded_resumes,
        (SELECT count(*) FROM public.evaluation_progress_successes WHERE kind='persistent')::int recovered`)
      ).rows[0];
      console.log(
        JSON.stringify({ event: 'evaluation_progress_transaction_retry_state', ...state }),
      );
      await pool.query(originalRelease);
      await pool.query(originalResume);
    },
  };
}
