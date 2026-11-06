import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import type { TestContext } from 'node:test';
import { managedRetentionDataset } from './dataset.js';
import { ageClosedDatasetHistory } from './history.js';
import { journalRows } from './journal.js';

export async function graphPruneRollback(t: TestContext) {
  const fixture = await managedRetentionDataset({ primaryKey: ['id'] });
  t.after(fixture.close);
  const { db, environment, dataset } = fixture;
  await assert.rejects(
    db.pool.query('SELECT moria.runtime_dataset_graph_read($1,ARRAY[]::text[])', [
      { kind: 'maintenance' },
    ]),
    (error: unknown) =>
      error instanceof Error &&
      error.message === 'UNDECLARED_DATASET' &&
      (error as { code?: string }).code === 'P0001',
  );
  const rows = [{ id: randomUUID(), name: 'before', tags: ['rollback'] }];
  await fixture.publish(rows, { graph: true });
  const second = await fixture.publish(
    rows.map((row) => ({ ...row, name: 'after' })),
    { graph: true },
  );
  await ageClosedDatasetHistory(fixture);
  const policy = await fixture.command(
    ['dataset-retention', 'approve'],
    {
      requestKey: 'rollback-policy',
      environment,
      dataset,
      retainSeconds: 86400,
      recheckSeconds: 30,
    },
    fixture.operatorEnv,
  );
  await db.pool.query(
    "CREATE FUNCTION public.reject_prune_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'GC_POLICY_CHANGED';END $$; CREATE CONSTRAINT TRIGGER reject_prune_commit AFTER INSERT ON moria.publications DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.execution LIKE 'dataset-maintenance/%') EXECUTE FUNCTION public.reject_prune_commit();",
  );
  const expire = (requestKey: string) =>
    fixture.command(
      ['dataset-retention', 'expire'],
      {
        policy: policy.policy,
        requestKey,
        owner: randomUUID(),
        beforeRevision: second.head.revision,
        maximumRoots: 64,
        timeSeconds: 120,
      },
      fixture.operatorEnv,
    );
  await assert.rejects(
    expire('rollback-failed-commit'),
    /^Error: dataset-retention\/expire: \{"error":"GC_POLICY_CHANGED"\}\n$/,
  );
  assert.deepEqual((await db.catalog.heads(environment, [dataset]))[0], second.head);
  assert.equal((await journalRows(db.pool, environment, dataset)).length, 2);
  assert.equal(
    (await db.pool.query('SELECT count(*) FROM moria.runtime_dataset_expired_roots')).rows[0].count,
    '0',
  );
  const failed = (
    await db.pool.query(
      "SELECT id,status,new_metadata,receipt FROM moria.runtime_dataset_maintenance_runs WHERE request_key='rollback-failed-commit'",
    )
  ).rows[0];
  assert.equal(failed.status, 'failed');
  assert.equal(failed.new_metadata, null);
  assert.equal(failed.receipt, null);
  const stage = (
    await db.pool.query(
      "SELECT s.status,(SELECT count(*) FROM moria.runtime_dataset_graph_references WHERE kind='history' AND owner=s.root)::text AS history_refs FROM moria.runtime_dataset_graph_stages s WHERE s.maintenance=$1",
      [failed.id],
    )
  ).rows;
  assert.equal(stage.length, 1);
  assert.equal(stage[0].status, 'ready');
  assert.equal(stage[0].history_refs, '0');
  await db.pool.query(
    'DROP TRIGGER reject_prune_commit ON moria.publications;DROP FUNCTION public.reject_prune_commit()',
  );
  const committed = await expire('rollback-retry-new-run');
  assert.equal(committed.headChanged, true);
  assert.equal((await journalRows(db.pool, environment, dataset)).length, 3);
  await fixture.read((await db.catalog.heads(environment, [dataset]))[0]);
  t.diagnostic(
    JSON.stringify({
      deferredCommitFailureRolledBackHeadHistoryExpiryReceiptAndJournal: true,
      realMaintenanceStageRemainedUnpublished: true,
      freshRunCommitted: true,
    }),
  );
}
