import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import pg from 'pg';
import { DeleteObjectCommand, HeadObjectCommand, S3ServiceException } from '@aws-sdk/client-s3';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';
import { bootstrapGraph } from './fixtures/native-retention/graph.js';
import { retentionRecovery } from './fixtures/native-retention/recovery.js';
import {
  journalRows,
  assertRestoredReplacementJournal,
} from './fixtures/native-retention/journal.js';
import {
  ageClosedDatasetHistory,
  ageClosedUnclaimedCandidates,
} from './fixtures/native-retention/history.js';

test(
  'graph custody survives exact sidecar collection and independent clean-host recovery',
  { timeout: 300000 },
  async (t) => {
    const fixture = await managedRetentionDataset({ primaryKey: ['id'] });
    t.after(fixture.close);
    const { db, environment, dataset } = fixture;
    const firstRows = Array.from({ length: 8 }, (_, i) => ({
      id: randomUUID(),
      name: 'first-' + i,
      tags: ['exact', String(i)],
    }));
    const first = await fixture.publish(firstRows);
    await bootstrapGraph(fixture, first.head, firstRows);
    const sourceRoot = (
      await db.pool.query(
        'SELECT g.node,n.content,q.physical_version FROM moria.runtime_dataset_graph_roots g JOIN moria.runtime_dataset_graph_nodes n ON n.id=g.node JOIN moria.runtime_dataset_graph_qualifications q USING(node,physical_fingerprint,incarnation) WHERE g.root=moria.runtime_dataset_root_key($1)',
        [first.head.metadataLocation],
      )
    ).rows[0];
    const secondRows = firstRows.map((row) => ({ ...row, name: 'second-' + row.name }));
    const second = await fixture.publish(secondRows);
    await bootstrapGraph(fixture, second.head, secondRows);
    await ageClosedDatasetHistory(fixture);
    const recovery = await retentionRecovery(fixture);
    const capturedJournal = await journalRows(db.pool, environment, dataset);
    assert.equal(capturedJournal.length, 2);
    const captured = await recovery.capture('graph-exact-backup');
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.runtime_dataset_graph_nodes n JOIN moria.runtime_dataset_graph_qualifications q ON q.node=n.id JOIN moria.runtime_dataset_graph_counts c USING(node,physical_fingerprint,incarnation) WHERE q.incarnation=(SELECT incarnation FROM moria.indexing_instance) AND c.live_references>0 AND NOT EXISTS(SELECT 1 FROM moria.runtime_recovery_members m WHERE m.recovery_set=$1 AND m.content=n.content AND m.backup IS NOT NULL AND m.backup_version IS NOT NULL)',
          [captured.recoverySet],
        )
      ).rows[0].count,
      '0',
    );
    let policy = await fixture.command(
      ['dataset-retention', 'approve'],
      {
        requestKey: 'graph-custody-policy',
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
        requestKey: 'graph-expire-first',
        owner: randomUUID(),
        beforeRevision: second.head.revision,
        maximumRoots: 64,
        timeSeconds: 120,
      },
      fixture.operatorEnv,
    );
    assert(
      expired.expiredRoots.includes(
        (
          await db.pool.query('SELECT moria.runtime_dataset_root_key($1) AS key', [
            first.head.metadataLocation,
          ])
        ).rows[0].key,
      ),
    );
    const originalPolicy = policy.policy;
    await fixture.command(
      ['dataset-retention', 'disable'],
      { policy: originalPolicy },
      fixture.operatorEnv,
    );
    policy = await fixture.command(
      ['dataset-retention', 'approve'],
      {
        requestKey: 'graph-successor-policy',
        environment,
        dataset,
        retainSeconds: 86400,
        recheckSeconds: 30,
      },
      fixture.operatorEnv,
    );
    assert.notEqual(policy.policy, originalPolicy);
    assert.equal(
      (
        await db.pool.query(
          'SELECT enabled FROM moria.runtime_dataset_retention_policies WHERE id=$1',
          [originalPolicy],
        )
      ).rows[0].enabled,
      false,
    );
    assert(
      BigInt(
        (
          await db.pool.query(
            'SELECT count(*) FROM moria.runtime_dataset_graph_retirements r JOIN moria.runtime_dataset_maintenance_runs m ON m.id=r.maintenance WHERE m.policy=$1 AND NOT r.completed',
            [originalPolicy],
          )
        ).rows[0].count,
      ) > 0n,
    );
    for (let i = 0; i < 8; i++) {
      await fixture.command(
        ['dataset-retention', 'plan'],
        { policy: policy.policy, requestKey: 'graph-drain-' + i, after: null, limit: 256 },
        fixture.operatorEnv,
      );
      if (i === 0) {
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.runtime_dataset_graph_retirements r JOIN moria.runtime_dataset_maintenance_runs m ON m.id=r.maintenance WHERE m.policy=$1 AND NOT r.completed',
              [originalPolicy],
            )
          ).rows[0].count,
          '0',
        );
        assert(
          BigInt(
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.runtime_dataset_graph_queue q JOIN moria.runtime_dataset_maintenance_runs m ON m.id=q.origin WHERE m.policy=$1 AND q.processed_at IS NULL',
                [originalPolicy],
              )
            ).rows[0].count,
          ) > 0n,
        );
        assert.equal(
          (
            await db.pool.query(
              "SELECT count(*) FROM moria.runtime_dataset_gc_candidates c JOIN moria.runtime_dataset_graph_nodes n ON n.content->'object'->>'warehouseId'=c.warehouse_id AND n.content->'object'->>'tableId'=c.table_id AND n.content->'object'->>'objectId'=c.object_id WHERE c.policy=$1",
              [policy.policy],
            )
          ).rows[0].count,
          '0',
        );
      }
      if (
        (
          await db.pool.query(
            'SELECT count(*) FROM moria.runtime_dataset_graph_queue WHERE processed_at IS NULL',
          )
        ).rows[0].count === '0'
      )
        break;
    }
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.runtime_dataset_graph_queue WHERE processed_at IS NULL',
        )
      ).rows[0].count,
      '0',
    );
    const candidate = (
      await db.pool.query(
        'SELECT id FROM moria.runtime_dataset_gc_candidates WHERE policy=$1 AND object_id=$2',
        [policy.policy, sourceRoot.content.object.objectId],
      )
    ).rows[0];
    assert(candidate);
    const immediate = await fixture.command(
      ['dataset-retention', 'once'],
      { policy: policy.policy, owner: randomUUID(), timeSeconds: 30 },
      fixture.operatorEnv,
    );
    assert.equal(immediate.claimed, false);
    await ageClosedUnclaimedCandidates(fixture, policy.policy);
    let removed = false;
    for (let i = 0; i < 128; i++) {
      const result = await fixture.command(
        ['dataset-retention', 'once'],
        { policy: policy.policy, owner: randomUUID(), timeSeconds: 30 },
        fixture.operatorEnv,
      );
      const versions = (
        await db.pool.query(
          "SELECT version FROM moria.runtime_dataset_gc_versions WHERE candidate=$1 AND result='removed'",
          [candidate.id],
        )
      ).rows;
      if (
        versions.some(
          (row) => JSON.stringify(row.version) === JSON.stringify(sourceRoot.physical_version),
        )
      ) {
        removed = true;
        break;
      }
      assert.equal(result.claimed, true);
    }
    assert.equal(removed, true);
    const restored = await recovery.restore(captured.manifest);
    try {
      await assertRestoredReplacementJournal(fixture, restored, capturedJournal);
      await restored.read(first.head, firstRows, 'graph-read');
      await restored.read(second.head, secondRows, 'graph-read');
      await restored.read(second.head, secondRows);
      const fresh = (
        await restored.pool.query(
          'SELECT q.physical_version,c.live_references FROM moria.runtime_dataset_graph_qualifications q JOIN moria.runtime_dataset_graph_counts c USING(node,physical_fingerprint,incarnation) WHERE q.node=$1 AND q.incarnation=(SELECT incarnation FROM moria.indexing_instance)',
          [sourceRoot.node],
        )
      ).rows;
      assert.equal(fresh.length, 1);
      assert(BigInt(fresh[0].live_references) > 0n);
      assert.notDeepEqual(fresh[0].physical_version, sourceRoot.physical_version);
      await assert.rejects(
        restored.pool.query(
          'UPDATE moria.runtime_dataset_graph_qualifications SET physical_version=$2 WHERE node=$1 AND incarnation=(SELECT incarnation FROM moria.indexing_instance)',
          [sourceRoot.node, sourceRoot.physical_version],
        ),
        /IMMUTABLE_RECOVERY_CONTENT/,
      );
      const unchanged = (
        await restored.pool.query(
          'SELECT physical_version FROM moria.runtime_dataset_graph_qualifications WHERE node=$1 AND incarnation=(SELECT incarnation FROM moria.indexing_instance)',
          [sourceRoot.node],
        )
      ).rows;
      assert.deepEqual(unchanged[0].physical_version, fresh[0].physical_version);
      await restored.rejectPhysicalReuse();
    } finally {
      await restored.close();
    }
    const missing = (
      await db.pool.query(
        "SELECT m.backup,m.backup_version FROM moria.runtime_dataset_graph_edges e JOIN moria.runtime_dataset_graph_nodes n ON n.id=e.child JOIN moria.runtime_recovery_members m ON m.content=n.content AND m.recovery_set=$2 WHERE e.parent=$1 AND n.kind='directory' ORDER BY e.label LIMIT 1",
        [sourceRoot.node, captured.recoverySet],
      )
    ).rows[0];
    assert(missing);
    assert.equal(missing.backup.object.warehouseId, fixture.backupNamespace.warehouseId);
    assert.equal(missing.backup.object.tableId, fixture.backupNamespace.tableId);
    assert.match(missing.backup_version.token, /^s3v1\//);
    const [, version] = JSON.parse(missing.backup_version.token.slice('s3v1/'.length));
    assert(version && version !== 'null');
    const databasesBefore = new Set(
      (
        await db.pool.query(
          "SELECT datname FROM pg_database WHERE datname LIKE 'retention_restore_%'",
        )
      ).rows.map((row) => row.datname),
    );
    const missingBackupKey = `${fixture.backupStorage.prefix}/${missing.backup.object.warehouseId}/${missing.backup.object.tableId}/objects/${missing.backup.object.objectId}`;
    const exactBackupVersion = {
      Bucket: fixture.s3.bucket,
      Key: missingBackupKey,
      VersionId: version,
    };
    const present = await fixture.s3.client.send(new HeadObjectCommand(exactBackupVersion));
    assert.equal(present.VersionId, version);
    await fixture.s3.client.send(new DeleteObjectCommand(exactBackupVersion));
    await assert.rejects(
      fixture.s3.client.send(new HeadObjectCommand(exactBackupVersion)),
      (error: unknown) =>
        error instanceof S3ServiceException && error.$metadata.httpStatusCode === 404,
    );
    await assert.rejects(
      recovery.restore(captured.manifest),
      /^Error: restore-from: \{"databaseCode":null,"error":"INDEXING_FAILED"\}\n$/,
    );
    const failedDatabases = (
      await db.pool.query(
        "SELECT datname FROM pg_database WHERE datname LIKE 'retention_restore_%'",
      )
    ).rows
      .map((row) => row.datname)
      .filter((name) => !databasesBefore.has(name));
    assert.equal(failedDatabases.length, 1);
    assert.match(failedDatabases[0], /^retention_restore_[0-9a-f]{32}$/);
    const failedUrl = new URL(fixture.operatorEnv.RETENTION_DATABASE!);
    failedUrl.pathname = '/' + failedDatabases[0];
    const failedPool = new pg.Pool({ connectionString: failedUrl.href });
    try {
      assert.equal(
        (await failedPool.query('SELECT dispatch_approved FROM public.moria_restore_bootstrap'))
          .rows[0].dispatch_approved,
        false,
      );
      const disabled = (
        await failedPool.query(
          'SELECT c.dispatch_enabled,r.status FROM moria.runtime_recovery_control c JOIN moria.runtime_restores r ON r.id=c.restore_id WHERE c.singleton',
        )
      ).rows;
      assert.equal(disabled.length, 1);
      assert.equal(disabled[0].dispatch_enabled, false);
      assert.equal(disabled[0].status, 'disabled');
      assert.equal(
        (
          await failedPool.query(
            'SELECT count(*) FROM moria.runtime_dataset_graph_qualifications WHERE incarnation=(SELECT incarnation FROM moria.indexing_instance)',
          )
        ).rows[0].count,
        '0',
      );
    } finally {
      await failedPool.end();
    }
    t.diagnostic(
      JSON.stringify({
        heads: 2,
        allGraphBytesBackedUp: true,
        sourceSidecarExactVersionRemoved: true,
        orphanGracePreserved: true,
        freshPhysicalQualification: true,
        independentTypedGraphReadback: true,
        immutableReplacementJournalRestored: true,
        journalRootsResolveFreshProviderVersions: true,
        oldProducerReplayRejected: true,
        incrementalReplayActivated: false,
        successorPolicyDrainsOriginalWork: true,
        pendingPropagationProtectsGraphObjects: true,
        conflictingTargetVersionRejected: true,
        reusedTargetRejected: true,
        missingGraphBackupChildRejectedWithDispatchDisabled: true,
      }),
    );
  },
);
