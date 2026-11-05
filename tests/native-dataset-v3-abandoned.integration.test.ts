import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';
import { ageClosedDatasetHistory } from './fixtures/native-retention/history.js';

test(
  'closed unpublished v3 candidates retire through their real stage without maintenance authority',
  { timeout: 300000 },
  async (t) => {
    const fixture = await managedRetentionDataset({ primaryKey: ['id'] });
    t.after(fixture.close);
    const { db, environment, dataset } = fixture;
    const rows = Array.from({ length: 8 }, (_, i) => ({
      id: randomUUID(),
      name: 'published-' + i,
      tags: ['exact'],
    }));
    const baseline = await fixture.publish(rows, { graph: true });
    const binding = await fixture.bind([], { [dataset]: baseline.head.revision });
    const output = await fixture.invoke({
      root: db.root,
      token: fixture.token,
      binding,
      mode: 'writer-supplied',
      graphOutput: true,
      prepareOnly: true,
      snapshots: [fixture.snapshot],
      writerRows: { [dataset]: rows.map((row) => ({ ...row, name: 'unpublished-' + row.name })) },
      s3Bindings: [fixture.datasetStorage],
    });
    assert.match(output, /"event":"prepared"/);
    assert.doesNotMatch(output, /"event":"published"/);
    const execution = (
      await db.pool.query('SELECT id FROM moria.runtime_executions WHERE binding=$1', [binding])
    ).rows[0].id as string;
    const candidate = (
      await db.pool.query(
        'SELECT c.id,c.metadata_location,s.id AS stage,s.node,s.status,r.key AS root FROM moria.runtime_snapshot_candidates c JOIN moria.runtime_dataset_graph_stages s ON s.candidate=c.id JOIN moria.runtime_dataset_roots r ON r.key=moria.runtime_dataset_root_key(c.metadata_location) WHERE c.execution=$1',
        [execution],
      )
    ).rows;
    assert.equal(candidate.length, 1);
    assert.equal(candidate[0].status, 'candidate');
    assert.match(candidate[0].metadata_location, /^moria-snapshot-v3:/);
    assert.equal(
      (
        await db.pool.query(
          "SELECT count(*) FROM moria.runtime_dataset_graph_references WHERE kind='stage' AND owner=$1",
          [candidate[0].stage],
        )
      ).rows[0].count,
      '1',
    );
    const journal = await db.pool.query(
      'SELECT * FROM moria.runtime_dataset_commit_deltas WHERE dataset=$1 ORDER BY result_revision',
      [dataset],
    );
    assert.equal(journal.rows.length, 1);
    await fixture.command(['execution', 'cancel'], { id: execution });
    await ageClosedDatasetHistory(fixture);
    const policy = await fixture.command(
      ['dataset-retention', 'approve'],
      {
        requestKey: 'unpublished-policy',
        environment,
        dataset,
        retainSeconds: 86400,
        recheckSeconds: 30,
      },
      fixture.operatorEnv,
    );
    const operations: string[] = [];
    let after: string | null = null;
    for (let i = 0; i < 16; i++) {
      const result: { operation: string; scanned: number; after: string | null } = await fixture
        .command(
          ['dataset-retention', 'plan'],
          { policy: policy.policy, requestKey: 'unpublished-drain-' + i, after, limit: 256 },
          fixture.operatorEnv,
        )
        .catch(async (error) => {
          assert.deepEqual((await db.catalog.heads(environment, [dataset]))[0], baseline.head);
          assert.deepEqual(
            (
              await db.pool.query(
                'SELECT * FROM moria.runtime_dataset_commit_deltas WHERE dataset=$1 ORDER BY result_revision',
                [dataset],
              )
            ).rows,
            journal.rows,
          );
          const rolledBack = (
            await db.pool.query(
              'SELECT status FROM moria.runtime_dataset_graph_stages WHERE id=$1',
              [candidate[0].stage],
            )
          ).rows[0].status;
          const expired = (
            await db.pool.query(
              'SELECT count(*) FROM moria.runtime_dataset_expired_roots WHERE root=$1',
              [candidate[0].root],
            )
          ).rows[0].count;
          t.diagnostic(
            JSON.stringify({
              phase: 'privateExpiryFailure',
              stage: rolledBack,
              expiredRoots: expired,
              headUnchanged: true,
              journalUnchanged: true,
            }),
          );
          throw error;
        });
      operations.push(result.operation);
      after = result.scanned === 256 ? result.after : null;
      if (
        after === null &&
        (
          await db.pool.query(
            'SELECT count(*) FROM moria.runtime_dataset_graph_queue WHERE processed_at IS NULL',
          )
        ).rows[0].count === '0' &&
        (
          await db.pool.query(
            'SELECT count(*) FROM moria.runtime_dataset_expired_roots WHERE root=$1',
            [candidate[0].root],
          )
        ).rows[0].count === '1' &&
        (
          await db.pool.query(
            "SELECT count(*) FROM moria.runtime_dataset_gc_candidates c JOIN moria.runtime_dataset_graph_nodes n ON n.content->'object'->>'objectId'=c.object_id WHERE c.policy=$1 AND n.id=$2",
            [policy.policy, candidate[0].node],
          )
        ).rows[0].count === '1'
      )
        break;
    }
    assert.deepEqual((await db.catalog.heads(environment, [dataset]))[0], baseline.head);
    assert.deepEqual(
      (
        await db.pool.query(
          'SELECT * FROM moria.runtime_dataset_commit_deltas WHERE dataset=$1 ORDER BY result_revision',
          [dataset],
        )
      ).rows,
      journal.rows,
    );
    assert.equal(
      (
        await db.pool.query('SELECT status FROM moria.runtime_dataset_graph_stages WHERE id=$1', [
          candidate[0].stage,
        ])
      ).rows[0].status,
      'abandoned',
    );
    assert.equal(
      (
        await db.pool.query(
          "SELECT count(*) FROM moria.runtime_dataset_graph_references WHERE kind='stage' AND owner=$1",
          [candidate[0].stage],
        )
      ).rows[0].count,
      '0',
    );
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.runtime_dataset_graph_queue WHERE processed_at IS NULL',
        )
      ).rows[0].count,
      '0',
    );
    const expiry = (
      await db.pool.query(
        'SELECT maintenance FROM moria.runtime_dataset_expired_roots WHERE root=$1',
        [candidate[0].root],
      )
    ).rows;
    assert.equal(expiry.length, 1);
    assert(operations.includes(expiry[0].maintenance));
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.runtime_dataset_maintenance_runs WHERE id=ANY($1::uuid[])',
          [operations],
        )
      ).rows[0].count,
      '0',
    );
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.runtime_dataset_graph_retirements WHERE root=$1',
          [candidate[0].root],
        )
      ).rows[0].count,
      '0',
    );
    assert.equal(
      (
        await db.pool.query(
          'SELECT live_references FROM moria.runtime_dataset_graph_counts WHERE node=$1 AND incarnation=(SELECT incarnation FROM moria.indexing_instance)',
          [candidate[0].node],
        )
      ).rows[0].live_references,
      '0',
    );
    const garbage = (
      await db.pool.query(
        "SELECT count(*) FROM moria.runtime_dataset_gc_candidates c JOIN moria.runtime_dataset_graph_nodes n ON n.content->'object'->>'objectId'=c.object_id WHERE c.policy=$1 AND n.id=$2",
        [policy.policy, candidate[0].node],
      )
    ).rows[0].count;
    assert.equal(garbage, '1');
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.runtime_dataset_expired_roots WHERE root=moria.runtime_dataset_root_key($1)',
          [baseline.head.metadataLocation],
        )
      ).rows[0].count,
      '0',
    );
    await fixture.read(baseline.head);
    t.diagnostic(
      JSON.stringify({
        actualPreparedCandidate: true,
        actualCancelledProducer: true,
        stageAbandoned: true,
        queueDrained: true,
        privateRootExpired: true,
        privateRootGarbageCandidate: true,
        noMaintenanceInvented: true,
        publishedHeadAndJournalPreserved: true,
      }),
    );
  },
);
