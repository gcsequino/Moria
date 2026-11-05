import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import type pg from 'pg';

export async function evaluationResumeFixture(
  pool: pg.Pool,
  evaluation: string,
  api: string,
  token: string,
): Promise<{ done: Promise<void>; close: () => Promise<void> }> {
  assert.match(evaluation, /^[0-9a-f-]{36}$/);
  const original = (
    await pool.query(
      "SELECT pg_get_functiondef('moria.runtime_execution_reap(uuid)'::regprocedure) definition",
    )
  ).rows[0].definition as string;
  await pool.query(
    original.replace(
      'moria.runtime_execution_reap(',
      'moria.runtime_execution_reap_before_resume_test(',
    ),
  );
  await pool.query(
    'CREATE TABLE public.evaluation_resume_gate(ready boolean NOT NULL); INSERT INTO public.evaluation_resume_gate VALUES(false); CREATE SEQUENCE public.evaluation_resume_conflict MINVALUE 0 START 0',
  );
  await pool.query(
    [
      'CREATE OR REPLACE FUNCTION moria.runtime_execution_reap(target uuid DEFAULT NULL) RETURNS integer',
      'LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$ BEGIN',
      'IF (SELECT ready FROM public.evaluation_resume_gate) THEN',
      'IF EXISTS(SELECT 1 FROM moria.runtime_evaluation_cases c JOIN moria.runtime_sandbox_attempts a ON a.execution=c.agent_execution',
      "WHERE c.evaluation='" +
        evaluation +
        "'::uuid AND c.agent_execution=target AND a.observation_pending IS NOT NULL",
      "AND EXISTS(SELECT 1 FROM moria.runtime_function_invocations f JOIN moria.runtime_executions e ON e.id=f.execution WHERE f.parent_id=target AND e.status='running')) THEN",
      "IF nextval('public.evaluation_resume_conflict')=0 THEN RAISE EXCEPTION 'EVALUATION_RESUME_CONFLICT' USING ERRCODE='40001'; END IF;",
      'END IF; END IF; RETURN moria.runtime_execution_reap_before_resume_test(target); END $$',
    ].join('\n'),
  );
  let stopped = false;
  const done: Promise<void> = (async () => {
    const end = Date.now() + 90000;
    let accepted:
      | {
          execution: string;
          agent_execution: string;
          resource: string;
          input: unknown;
          hash: string;
        }
      | undefined;
    while (!stopped && Date.now() < end) {
      accepted = (
        await pool.query(
          "SELECT c.execution,c.agent_execution,e.resource::text,c.input,s.token_hash hash FROM moria.runtime_evaluation_cases c JOIN moria.runtime_executions e ON e.id=c.agent_execution JOIN moria.runtime_job_sessions s ON s.execution=c.execution AND s.task IS NOT NULL JOIN moria.runtime_sandbox_attempts a ON a.execution=e.id WHERE c.evaluation=$1 AND c.outcome IN('pending','running') AND a.observation_pending IS NOT NULL AND EXISTS(SELECT 1 FROM moria.runtime_function_invocations f JOIN moria.runtime_executions child ON child.id=f.execution WHERE f.parent_id=e.id AND child.status='running') ORDER BY c.case_id,c.repeat LIMIT 1",
          [evaluation],
        )
      ).rows[0];
      if (accepted) break;
      await delay(50);
    }
    assert(accepted && !stopped, 'A live case must reach its real pending Function callback');
    assert.equal(
      (
        await pool.query('SELECT moria.runtime_evaluation_agent_resume($1,$2) child', [
          accepted.hash,
          accepted.execution,
        ])
      ).rows[0].child,
      accepted.agent_execution,
    );
    const other = (
      await pool.query(
        'SELECT execution FROM moria.runtime_evaluation_cases WHERE evaluation=$1 AND execution<>$2 LIMIT 1',
        [evaluation, accepted.execution],
      )
    ).rows[0].execution as string;
    await assert.rejects(
      pool.query('SELECT moria.runtime_evaluation_agent_resume($1,$2)', [accepted.hash, other]),
      { message: 'EVALUATION_CASE_AUTHORITY' },
    );
    const differentPurpose = (
      await pool.query(
        'SELECT token_hash FROM moria.runtime_job_sessions WHERE execution=$1 AND task IS NOT NULL',
        [accepted.agent_execution],
      )
    ).rows[0].token_hash as string;
    await assert.rejects(
      pool.query('SELECT moria.runtime_evaluation_agent_resume($1,$2)', [
        differentPurpose,
        accepted.execution,
      ]),
      { message: 'EVALUATION_CASE_AUTHORITY' },
    );
    const client = await pool.connect();
    try {
      await client.query(
        'CREATE TEMP TABLE evaluation_resume_executions AS SELECT * FROM moria.runtime_executions WHERE id=ANY($1::uuid[])',
        [[accepted.execution, accepted.agent_execution]],
      );
      const definition = (
        await client.query(
          "SELECT pg_get_functiondef('moria.runtime_evaluation_agent_resume(text,uuid)'::regprocedure) definition",
        )
      ).rows[0].definition as string;
      assert.equal(definition.split('moria.runtime_executions').length - 1, 2);
      // Exercise exact production predicates on corrupted copies without locking live tables.
      await client.query(
        definition
          .replace('moria.runtime_evaluation_agent_resume(', 'pg_temp.evaluation_resume_identity(')
          .replaceAll('moria.runtime_executions', 'pg_temp.evaluation_resume_executions'),
      );
      const copy = (
        await client.query(
          "SELECT pg_get_functiondef('pg_temp.evaluation_resume_identity(text,uuid)'::regprocedure) definition",
        )
      ).rows[0].definition as string;
      assert.equal(
        copy
          .replace(
            /pg_temp(?:_[0-9]+)?\.evaluation_resume_identity\(/,
            'moria.runtime_evaluation_agent_resume(',
          )
          .replaceAll('pg_temp.evaluation_resume_executions', 'moria.runtime_executions'),
        definition,
      );
      assert.equal(
        (
          await client.query('SELECT pg_temp.evaluation_resume_identity($1,$2) child', [
            accepted.hash,
            accepted.execution,
          ])
        ).rows[0].child,
        accepted.agent_execution,
      );
      for (const update of [
        "principal='not-the-accepted-case-actor'",
        "request_key='not-the-accepted-case-key'",
        "input_hash=repeat('0',64)",
        "context=jsonb_set(context,'{parentExecution}',to_jsonb('" + other + "'::text))",
      ]) {
        await client.query(
          'UPDATE pg_temp.evaluation_resume_executions SET ' + update + ' WHERE id=$1',
          [accepted.agent_execution],
        );
        await assert.rejects(
          client.query('SELECT pg_temp.evaluation_resume_identity($1,$2)', [
            accepted.hash,
            accepted.execution,
          ]),
          { message: 'EVALUATION_PIN_INVALID' },
        );
        await client.query(
          'UPDATE pg_temp.evaluation_resume_executions copied SET principal=actual.principal,request_key=actual.request_key,input_hash=actual.input_hash,context=actual.context FROM moria.runtime_executions actual WHERE actual.id=copied.id',
        );
      }
    } finally {
      await client.query('DROP FUNCTION IF EXISTS pg_temp.evaluation_resume_identity(text,uuid)');
      await client.query('DROP TABLE IF EXISTS pg_temp.evaluation_resume_executions');
      client.release();
    }
    console.log(
      JSON.stringify({
        event: 'evaluation_resume_identity_probes',
        livePositive: true,
        wrongCase: true,
        wrongPurpose: true,
        storedMetadataNegatives: 4,
        productionPredicateParity: true,
      }),
    );
    for (const replay of [false, true]) {
      const response: Response = await fetch(
        api +
          (replay
            ? '/api/v1/agents/' + accepted.resource + '/executions'
            : '/api/v1/agent-executions/' + accepted.agent_execution),
        {
          method: replay ? 'POST' : 'GET',
          headers: {
            authorization: 'Bearer ' + token,
            'content-type': 'application/json',
            ...(replay ? { 'idempotency-key': 'evaluation/' + accepted.execution + '/agent' } : {}),
          },
          ...(replay ? { body: JSON.stringify({ input: accepted.input, wait: false }) } : {}),
        },
      );
      const value = (await response.json()) as { error: { code: string } };
      assert.equal(response.status, 409, JSON.stringify(value));
      assert.equal(value.error.code, 'FUNCTION_OBSERVATION_INCOMPLETE');
      assert.equal('result' in value, false);
    }
    console.log(
      JSON.stringify({ event: 'evaluation_resume_public_withheld', get: true, replay: true }),
    );
    await pool.query('UPDATE public.evaluation_resume_gate SET ready=true');
    while (!stopped && Date.now() < end) {
      if (
        (await pool.query('SELECT is_called FROM public.evaluation_resume_conflict')).rows[0]
          .is_called
      ) {
        console.log(
          JSON.stringify({ event: 'evaluation_resume_conflict_injected', sqlstate: '40001' }),
        );
        return;
      }
      await delay(50);
    }
    assert.fail('The actual pending-child progress check must encounter the injected 40001');
  })();
  void done.catch(() => {});
  return {
    done,
    close: async () => {
      stopped = true;
      await done.catch(() => {});
      await pool.query(original);
      // The owned fixture database removes inactive probe objects after workers stop.
      await pool.query('UPDATE public.evaluation_resume_gate SET ready=false');
    },
  };
}
