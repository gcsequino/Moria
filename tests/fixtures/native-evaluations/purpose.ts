import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import type pg from 'pg';

export async function evaluationPurposeFixture(pool: pg.Pool, execution: string): Promise<void> {
  const client = await pool.connect();
  const check = (proof: string | null) =>
    client.query(
      'SELECT moria.runtime_binding_authority(e.binding,n.security_epoch,i.security_epoch,$2::uuid) FROM moria.runtime_executions e JOIN moria.environments n ON n.id=e.environment CROSS JOIN moria.instance_state i WHERE e.id=$1',
      [execution, proof],
    );
  try {
    const accepted = (
      await client.query(
        "SELECT d.origin_kind,d.origin_binding=e.binding AS exact_origin,e.context->>'automationFiring' firing FROM moria.runtime_executions e JOIN moria.runtime_job_delegations d ON d.execution=e.id WHERE e.id=$1",
        [execution],
      )
    ).rows[0];
    assert.equal(accepted.origin_kind, 'service');
    assert.equal(accepted.exact_origin, true);
    assert(accepted.firing, 'The counter uses an actual accepted Source automation child');
    await client.query("SET track_functions='pl'");
    for (const proof of [execution, null]) {
      await client.query(
        "SELECT pg_stat_reset_single_function_counters('moria.runtime_automation_binding_guard(uuid)'::regprocedure::oid)",
      );
      await check(proof);
      await client.query('SELECT pg_stat_force_next_flush()');
      let calls = 0;
      for (let attempt = 0; attempt < 30; attempt++) {
        await client.query('SELECT pg_stat_clear_snapshot()');
        calls =
          (
            await client.query(
              "SELECT calls::int FROM pg_stat_user_functions WHERE funcid='moria.runtime_automation_binding_guard(uuid)'::regprocedure::oid",
            )
          ).rows[0]?.calls ?? 0;
        if (calls) break;
        await delay(10);
      }
      assert.equal(calls, 1, 'One binding check must traverse its exact purpose once');
    }
    await assert.rejects(check(randomUUID()), {
      code: 'P0001',
      message: 'JOB_DELEGATION_REQUIRED',
    });
    await client.query('BEGIN');
    try {
      assert.equal(
        (
          await client.query(
            "UPDATE moria.runtime_source_automation_profiles SET enabled=false WHERE id=(SELECT s.profile FROM moria.runtime_executions e JOIN moria.runtime_source_automation_firings f ON f.id=(e.context->>'automationFiring')::uuid JOIN moria.runtime_source_automation_schedules s ON s.id=f.schedule WHERE e.id=$1)",
            [execution],
          )
        ).rowCount,
        1,
      );
      assert.equal(
        (
          await client.query(
            "UPDATE moria.runtime_service_targets target SET enabled=false FROM moria.runtime_executions e JOIN moria.runtime_execution_services service ON service.binding=e.binding WHERE e.id=$1 AND target.authority=service.authority AND target.environment=e.environment AND target.resource=e.resource AND target.domain=e.spec->>'domain' AND target.resource_class=e.resource_class",
            [execution],
          )
        ).rowCount,
        1,
      );
      await assert.rejects(check(execution), {
        code: 'P0001',
        message: 'AUTOMATION_DISPATCH_REVOKED',
      });
    } finally {
      await client.query('ROLLBACK');
    }
    await check(execution);
    console.log(
      JSON.stringify({
        event: 'evaluation_purpose_single_traversal',
        exactOrigin: true,
        proofNull: true,
        wrongProof: true,
        revokedErrorPrecedence: true,
      }),
    );
  } finally {
    await client.query("SET track_functions='none'");
    client.release();
  }
}
