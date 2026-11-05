import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import pg from 'pg';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';
import { ageClosedDatasetHistory } from './fixtures/native-retention/history.js';

test(
  'actual replacement and metadata pruning append exact contiguous publication edges',
  { timeout: 120000 },
  async (t) => {
    const fixture = await managedRetentionDataset({ primaryKey: ['id'] });
    t.after(fixture.close);
    const { db, environment, dataset } = fixture;
    const rows = [{ id: randomUUID(), name: 'first', tags: ['typed'] }];
    const first = await fixture.publish(rows);
    const second = await fixture.publish([{ ...rows[0], name: 'second' }]);
    const edges = async () =>
      (
        await db.pool.query(
          'SELECT * FROM moria.runtime_dataset_commit_deltas WHERE environment=$1 AND dataset=$2 ORDER BY result_revision',
          [environment, dataset],
        )
      ).rows;
    const recorded = await edges();
    assert.equal(recorded.length, 2);
    for (const [i, edge] of recorded.entries()) {
      const output = i === 0 ? first : second;
      assert.equal(edge.commit, edge.candidate);
      assert.equal(edge.execution, output.execution);
      assert.equal(edge.producer_kind, 'execution');
      assert.equal(edge.basis_revision, String(i));
      assert.equal(edge.result_revision, String(i + 1));
      assert.equal(edge.result_snapshot, output.head.snapshotId);
      assert.equal(edge.result_metadata, output.head.metadataLocation);
      assert.equal(edge.change_kind, 'replace');
      assert.equal(edge.complete_rows_root, edge.result_root);
      assert.equal(edge.delete_keys_root, null);
      assert.deepEqual(edge.ordered_primary_key, ['id']);
      assert.match(edge.dataset_schema_hash, /^sha256:[0-9a-f]{64}$/);
      assert.equal(
        edge.binding_revision,
        (
          await db.pool.query(
            'SELECT revision FROM moria.native_dataset_bindings WHERE environment=$1 AND resource::text=$2',
            [environment, dataset],
          )
        ).rows[0].revision,
      );
      const publication = (
        await db.pool.query(
          'SELECT receipt FROM moria.publications WHERE environment=$1 AND execution=$2',
          [environment, output.execution],
        )
      ).rows[0];
      assert.deepEqual(edge.sealed_receipt, publication.receipt);
    }
    assert.equal(recorded[0].basis_root, null);
    assert.equal(recorded[1].basis_root, recorded[0].result_root);
    assert.equal(recorded[1].basis_metadata, first.head.metadataLocation);
    await assert.rejects(
      db.pool.query('UPDATE moria.runtime_dataset_commit_deltas SET mode=$1 WHERE commit=$2', [
        'append',
        recorded[0].commit,
      ]),
      /IMMUTABLE_RECOVERY_CONTENT/,
    );
    const ordinary = new pg.Pool({ connectionString: fixture.nativeEnv.RETENTION_DATABASE });
    t.after(() => ordinary.end());
    await assert.rejects(
      ordinary.query(
        'INSERT INTO moria.runtime_dataset_commit_deltas SELECT * FROM moria.runtime_dataset_commit_deltas LIMIT 1',
      ),
      /DATASET_DELTA_PRODUCER_REQUIRED/,
    );
    await ageClosedDatasetHistory(fixture);
    const policy = await fixture.command(
      ['dataset-retention', 'approve'],
      {
        requestKey: 'delta-prune-policy',
        environment,
        dataset,
        retainSeconds: 86400,
        recheckSeconds: 30,
      },
      fixture.operatorEnv,
    );
    const expired = await fixture.command(
      ['dataset-retention', 'expire'],
      {
        policy: policy.policy,
        requestKey: 'delta-prune',
        owner: randomUUID(),
        beforeRevision: second.head.revision,
        maximumRoots: 64,
        timeSeconds: 120,
      },
      fixture.operatorEnv,
    );
    assert.equal(expired.headChanged, true);
    const current = await edges();
    assert.equal(current.length, 3);
    const unchanged = current[2];
    assert.equal(unchanged.producer_kind, 'maintenance');
    assert.equal(unchanged.mode, null);
    assert.equal(unchanged.commit, expired.run);
    assert.equal(unchanged.maintenance, expired.run);
    assert.equal(unchanged.policy, policy.policy);
    assert.equal(unchanged.basis_revision, second.head.revision);
    assert.equal(unchanged.basis_root, current[1].result_root);
    assert.equal(unchanged.result_snapshot, second.head.snapshotId);
    assert.equal(unchanged.change_kind, 'unchanged');
    assert.equal(unchanged.unchanged_reason, 'metadataPrune');
    assert.equal(unchanged.complete_rows_root, null);
    assert.equal(unchanged.delete_keys_root, null);
    assert.equal(unchanged.equivalence_receipt.basisRoot, unchanged.basis_root);
    assert.equal(unchanged.equivalence_receipt.resultRoot, unchanged.result_root);
    assert.equal(unchanged.equivalence_receipt.snapshot, second.head.snapshotId);
    assert.match(unchanged.equivalence_receipt.currentSnapshotHash, /^sha256:[0-9a-f]{64}$/);
    assert.deepEqual(unchanged.sealed_receipt, expired);
    await fixture.read((await db.catalog.heads(environment, [dataset]))[0]);
  },
);

test(
  'failure while recording a real edge rolls back Dataset publication and completion',
  { timeout: 120000 },
  async (t) => {
    const fixture = await managedRetentionDataset({ primaryKey: ['id'] });
    t.after(fixture.close);
    const { db, environment, dataset } = fixture;
    const first = await fixture.publish([{ id: randomUUID(), name: 'before', tags: [] }]);
    const before = (
      await db.pool.query('SELECT count(*) FROM moria.publications WHERE environment=$1', [
        environment,
      ])
    ).rows[0].count;
    await db.pool
      .query(`CREATE FUNCTION moria.fixture_delta_publication_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'FIXTURE_DELTA_PUBLICATION_FAULT';END $$;
    CREATE TRIGGER fixture_delta_publication_fault AFTER INSERT ON moria.runtime_dataset_commit_deltas FOR EACH ROW EXECUTE FUNCTION moria.fixture_delta_publication_fault()`);
    try {
      await assert.rejects(
        fixture.publish([{ id: randomUUID(), name: 'after', tags: [] }]),
        /FIXTURE_DELTA_PUBLICATION_FAULT/,
      );
    } finally {
      await db.pool.query(
        'DROP TRIGGER fixture_delta_publication_fault ON moria.runtime_dataset_commit_deltas;DROP FUNCTION moria.fixture_delta_publication_fault()',
      );
    }
    assert.deepEqual((await db.catalog.heads(environment, [dataset]))[0], first.head);
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.runtime_dataset_commit_deltas WHERE environment=$1 AND dataset=$2',
          [environment, dataset],
        )
      ).rows[0].count,
      '1',
    );
    assert.equal(
      (
        await db.pool.query('SELECT count(*) FROM moria.publications WHERE environment=$1', [
          environment,
        ])
      ).rows[0].count,
      before,
    );
    assert.equal(
      (
        await db.pool.query(
          "SELECT count(*) FROM moria.runtime_executions WHERE environment=$1 AND status='succeeded'",
          [environment],
        )
      ).rows[0].count,
      '1',
    );
    await fixture.read(first.head);
  },
);
