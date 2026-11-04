import assert from 'node:assert/strict';
import type {managedRetentionDataset} from './dataset.js';
type Fixture = Pick<Awaited<ReturnType<typeof managedRetentionDataset>>, 'db'|'environment'>;

/** Synthetic historical DATA only, after actual native jobs have closed. No lease or deadline is changed. */
export async function ageClosedDatasetHistory({db,environment}:Fixture) {
    const client = await db.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('LOCK TABLE moria.runtime_dataset_roots,moria.runtime_dataset_issued_objects,moria.runtime_executions IN ACCESS EXCLUSIVE MODE');
      const active = (await client.query(
        "SELECT count(*) FROM moria.runtime_executions WHERE environment=$1 AND (status IN ('queued','running','preparing') OR lease_until>clock_timestamp())",
        [environment],
      )).rows[0].count;
      assert.equal(active, '0');
      const issuedSql="SELECT jsonb_agg(to_jsonb(o)-'created_at' ORDER BY o.object_id) AS value FROM moria.runtime_dataset_issued_objects o WHERE producer IN (SELECT scope FROM moria.runtime_dataset_producers p JOIN moria.runtime_executions e ON e.id=p.execution WHERE e.environment=$1)";
      const executionSql="SELECT jsonb_agg(to_jsonb(e)-'finished_at' ORDER BY id) AS value FROM moria.runtime_executions e WHERE environment=$1";
      const issuedBefore=(await client.query(issuedSql,[environment])).rows[0].value;
      const executionBefore=(await client.query(executionSql,[environment])).rows[0].value;
      assert.ok((issuedBefore?.length??0)<=1024&&(executionBefore?.length??0)<=1024);
      const before = (await client.query(
        "SELECT jsonb_agg(to_jsonb(r)-ARRAY['created_at','retention_origin'] ORDER BY key) value FROM moria.runtime_dataset_roots r WHERE environment=$1",
        [environment],
      )).rows[0].value;
      await client.query('ALTER TABLE moria.runtime_dataset_roots DISABLE TRIGGER runtime_dataset_root_immutable');
      await client.query('ALTER TABLE moria.runtime_dataset_issued_objects DISABLE TRIGGER runtime_dataset_issued_immutable');
      await client.query("UPDATE moria.runtime_dataset_roots SET created_at=clock_timestamp()-interval '2 days',retention_origin=clock_timestamp()-interval '2 days' WHERE environment=$1", [environment]);
      await client.query("UPDATE moria.runtime_dataset_issued_objects SET created_at=clock_timestamp()-interval '2 days' WHERE producer IN (SELECT scope FROM moria.runtime_dataset_producers p JOIN moria.runtime_executions e ON e.id=p.execution WHERE e.environment=$1)", [environment]);
      await client.query("UPDATE moria.runtime_executions SET finished_at=clock_timestamp()-interval '31 days' WHERE environment=$1 AND status NOT IN ('queued','running','preparing')", [environment]);
      await client.query('ALTER TABLE moria.runtime_dataset_roots ENABLE TRIGGER runtime_dataset_root_immutable');
      await client.query('ALTER TABLE moria.runtime_dataset_issued_objects ENABLE TRIGGER runtime_dataset_issued_immutable');
      assert.deepEqual((await client.query(
        "SELECT jsonb_agg(to_jsonb(r)-ARRAY['created_at','retention_origin'] ORDER BY key) value FROM moria.runtime_dataset_roots r WHERE environment=$1",
        [environment],
      )).rows[0].value, before);
      assert.deepEqual((await client.query(issuedSql,[environment])).rows[0].value,issuedBefore);
      assert.deepEqual((await client.query(executionSql,[environment])).rows[0].value,executionBefore);
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
}

/** Import a pre-aged, CLOSED and never-claimed candidate fixture; live GC authority is never fabricated or aged. */
export async function ageClosedUnclaimedCandidates({db,environment}:Fixture,policy:string,only?:string[]) {
  assert(only===undefined||(only.length>0&&only.length<=1024&&new Set(only).size===only.length));
  const client=await db.pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('LOCK TABLE moria.runtime_dataset_gc_candidates,moria.runtime_executions IN ACCESS EXCLUSIVE MODE');
    assert.equal((await client.query("SELECT count(*) FROM moria.runtime_executions WHERE environment=$1 AND (status IN ('queued','running','preparing') OR lease_until>clock_timestamp())",[environment])).rows[0].count,'0');
    const candidates=(await client.query(`SELECT to_jsonb(c)-ARRAY['orphaned_at','not_before','next_check'] AS original,c.id,
      c.status='pending' AND c.owner IS NULL AND c.fence=0 AND c.attempts=0 AND c.lease_until IS NULL
      AND c.admission_pid IS NULL AND c.admission_started IS NULL AND moria.runtime_dataset_producer_closed(c.producer) AS closed
      FROM moria.runtime_dataset_gc_candidates c JOIN moria.runtime_dataset_retention_policies p ON p.id=c.policy
      WHERE c.policy=$1 AND p.environment=$2 AND ($3::uuid[] IS NULL OR c.id=ANY($3)) ORDER BY c.id`,[policy,environment,only??null])).rows;
    assert.ok(candidates.length>0&&candidates.length<=1024);if(only)assert.equal(candidates.length,only.length);
    assert.ok(candidates.every(row=>row.closed));
    await client.query('ALTER TABLE moria.runtime_dataset_gc_candidates DISABLE TRIGGER runtime_dataset_gc_candidate_immutable');
    await client.query("UPDATE moria.runtime_dataset_gc_candidates SET orphaned_at=clock_timestamp()-interval '2 days',not_before=clock_timestamp()-interval '1 day',next_check=clock_timestamp() WHERE id=ANY($1::uuid[])",[candidates.map(row=>row.id)]);
    await client.query('ALTER TABLE moria.runtime_dataset_gc_candidates ENABLE TRIGGER runtime_dataset_gc_candidate_immutable');
    assert.deepEqual((await client.query("SELECT to_jsonb(c)-ARRAY['orphaned_at','not_before','next_check'] AS original,c.id FROM moria.runtime_dataset_gc_candidates c WHERE id=ANY($1::uuid[]) ORDER BY c.id",[candidates.map(row=>row.id)])).rows,candidates.map(({closed,...row})=>row));
    await client.query('COMMIT');
  } catch(error) {await client.query('ROLLBACK');throw error;}finally {client.release();}
}
