"""Build the focused Tool admission fixture from the published Evaluation test."""

import hashlib
import sys


def main():
    source = sys.stdin.read()
    first_case = "    let first = '';\n"
    service_case = "    await t.test(\n      'a Source automation dispatches a service evaluation"
    service_setup_end = '        let resume: Awaited<ReturnType<typeof evaluationResumeFixture>>'
    configuration = '          const configuration = {'
    firing = '          assert.ok(firing);'
    prelude = r'''    await db.pool.query(
      'INSERT INTO moria.runtime_evaluation_environments(environment,application,maximum_cases,maximum_repeats,maximum_parallel) VALUES($1,$2,12,4,2)',
      [environment, application],
    );
    publicToken = (await sessions.mint({ environment, principal, application }, 'fixture')).token;
    evaluationWorker = start(['evaluations', 'worker']);
    agentWorker = start(['agents', 'worker']);
    functionWorker = start(['functions', 'worker']);
'''
    hold = r'''        const gate = 731681234;
        const signature = 'text,text,text,text,text,text';
        const definition: string = (await db.pool.query(
          "SELECT pg_get_functiondef('moria.native_resource_filesystem_authority(text,text,text,text,text,text)'::regprocedure) AS definition",
        )).rows[0].definition;
        const originalName = 'moria.native_resource_filesystem_authority(';
        const copyName = 'moria.native_resource_filesystem_authority_before_admit_probe(';
        assert.ok(definition.includes(originalName));
        await db.pool.query(definition.replace(originalName, copyName));
        await db.pool.query(`REVOKE ALL ON FUNCTION ${copyName}${signature}) FROM PUBLIC`);
        await db.pool.query('CREATE TABLE public.moria_tool_admit_probe(armed boolean NOT NULL); INSERT INTO public.moria_tool_admit_probe VALUES(true)');
        const barrier = await db.pool.connect();
        await barrier.query('SELECT pg_advisory_lock($1::bigint)', [gate]);
        let held = true;
        try {
          await db.pool.query(`CREATE OR REPLACE FUNCTION moria.native_resource_filesystem_authority(
            token_hash text,expected_environment text,expected_principal text,expected_application text,resource text,operation text
          ) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
          DECLARE actor uuid; prior_name text;
          BEGIN
            IF resource='${tool}' AND operation='invoke' AND (SELECT armed FROM public.moria_tool_admit_probe) THEN
              SELECT j.execution INTO actor FROM moria.runtime_job_sessions j
              JOIN moria.runtime_executions e ON e.id=j.execution
              WHERE j.token_hash=native_resource_filesystem_authority.token_hash
                AND e.spec->>'domain'='agent' AND e.status='running'
                AND NOT EXISTS(SELECT 1 FROM moria.runtime_function_invocations f WHERE f.parent_id=e.id);
              IF FOUND AND pg_try_advisory_xact_lock(${gate + 1}::bigint) THEN
                prior_name:=current_setting('application_name');
                PERFORM set_config('application_name','moria-tool-admit/'||actor::text,true);
                PERFORM pg_advisory_xact_lock(${gate}::bigint);
                PERFORM set_config('application_name',prior_name,true);
              END IF;
            END IF;
            RETURN moria.native_resource_filesystem_authority_before_admit_probe(
              token_hash,expected_environment,expected_principal,expected_application,resource,operation);
          END $$`);
'''
    witness = r'''
          let parent = '';
          await until(async () => {
            const row = (await db.pool.query(
              `SELECT a.application_name FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid
               WHERE l.locktype='advisory' AND l.classid=0 AND l.objid=$1 AND l.objsubid=1
                 AND NOT l.granted AND a.application_name LIKE 'moria-tool-admit/%'`,
              [gate],
            )).rows[0];
            parent = row?.application_name.split('/')[1] ?? '';
            return !!parent;
          }, 60000);
          assert.match(parent, /^[0-9a-f-]{36}$/);
          const live = async () => (
            await db.pool.query(
              `SELECT i.incarnation,e.id AS execution,t.id AS task,t.owner,t.fence::text,t.lease_until::text AS "leaseUntil"
               FROM moria.runtime_executions e JOIN moria.runtime_execution_tasks t ON t.execution=e.id
               JOIN moria.runtime_job_delegations d ON d.execution=e.id
               JOIN moria.runtime_evaluation_cases c ON c.execution=d.parent_execution AND c.agent_execution=e.id
               CROSS JOIN moria.indexing_instance i
               WHERE e.id=$1 AND c.evaluation=$2 AND t.status='running' AND t.lease_until>clock_timestamp()`,
              [parent, firing.child],
            )
          ).rows[0];
          const priorLease = await live();
          assert.ok(priorLease);
          assert.deepEqual(JSON.parse(await start(['execution', 'renew'], priorLease).done), { renewed: true });
          const renewed = await live();
          assert.equal(renewed.owner, priorLease.owner);
          assert.equal(renewed.fence, priorLease.fence);
          assert.notEqual(renewed.leaseUntil, priorLease.leaseUntil);
          await db.pool.query('UPDATE public.moria_tool_admit_probe SET armed=false');
          await barrier.query('SELECT pg_advisory_unlock($1::bigint)', [gate]);
          held = false;
          let witness: any;
          await until(async () => {
            witness = (await db.pool.query(
              `SELECT e.status,e.error,s.observation_pending,s.observation_blocked,s.cleaned,
                (SELECT jsonb_agg(DISTINCT v.payload->>'code') FROM moria.runtime_execution_events v WHERE v.execution=e.id AND v.kind IN ('brokerDenied','brokerFailed')) AS codes,
                (SELECT count(*)::int FROM moria.runtime_function_invocations f WHERE f.parent_id=e.id) AS children,
                (SELECT count(*)::int FROM moria.runtime_function_receipts r JOIN moria.runtime_function_invocations f ON f.execution=r.execution WHERE f.parent_id=e.id) AS receipts,
                (SELECT count(*)::int FROM moria.runtime_agent_receipts r WHERE r.execution=e.id) AS agent_receipts
               FROM moria.runtime_executions e JOIN moria.runtime_sandbox_attempts s ON s.execution=e.id WHERE e.id=$1`,
              [parent],
            )).rows[0];
            return witness?.status === 'failed' || witness?.status === 'succeeded';
          }, 30000).catch((error: unknown) => assert.fail(JSON.stringify({ error: String(error), witness })));
          console.log(JSON.stringify({event:'function_admission_race',sameFence:true,leaseAdvanced:true,...witness}));
          if (process.env.MORIA_FUNCTION_ADMIT_RACE_FIXED === '1') {
            assert.equal(witness.status, 'succeeded');
            assert.equal(witness.observation_pending, null);
            assert.equal(witness.observation_blocked, false);
            assert.equal(witness.children, 1);
            assert.equal(witness.receipts, 1);
            assert.equal(witness.agent_receipts, 1);
          } else {
            assert.deepEqual(witness.codes, ['CONCURRENT_MODIFICATION']);
            assert.equal(witness.status, 'failed');
            assert.equal(witness.error, 'FUNCTION_OBSERVATION_INCOMPLETE');
            assert.equal(witness.observation_pending, '1');
            assert.equal(witness.observation_blocked, true);
            assert.equal(witness.children, 0);
            assert.equal(witness.receipts, 0);
            assert.equal(witness.agent_receipts, 0);
          }
        } finally {
          if (held) await barrier.query('SELECT pg_advisory_unlock($1::bigint)', [gate]);
          barrier.release();
          await db.pool.query(definition);
          await db.pool.query(`DROP FUNCTION ${copyName}${signature}); DROP TABLE public.moria_tool_admit_probe`);
        }
      },
    );
  },
);
'''
    setup = source.index(configuration, source.index(service_case))
    accepted = source.index(firing, setup) + len(firing)
    result = (
        source[:source.index(first_case)]
        + prelude
        + source[source.index(service_case):source.index(service_setup_end)]
        + hold
        + source[setup:accepted]
        + witness
    )
    assert hashlib.sha256(result.encode()).hexdigest() == 'd35103b5a60e1c393537e230a1c4c8440ac0b6cdcb196af313da4ec3608bc82a'
    sys.stdout.write(result)


if __name__ == "__main__":
    main()
