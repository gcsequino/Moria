import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import type pg from 'pg';
import { GetObjectCommand, HeadObjectCommand, S3ServiceException } from '@aws-sdk/client-s3';
import type { managedRetentionDataset } from './dataset.js';

type Fixture = Awaited<ReturnType<typeof managedRetentionDataset>>;
export async function journalRows(pool: pg.Pool, environment: string, dataset: string) {
  return (
    await pool.query(
      `SELECT to_jsonb(d)-'created_at' AS record FROM moria.runtime_dataset_commit_deltas d
    WHERE environment=$1 AND dataset=$2 ORDER BY result_revision LIMIT 65`,
      [environment, dataset],
    )
  ).rows.map((row) => row.record);
}

export async function assertRestoredReplacementJournal(
  fixture: Fixture,
  restored: { pool: pg.Pool; restoredStorage: Fixture['datasetStorage'] },
  captured: Awaited<ReturnType<typeof journalRows>>,
) {
  const { environment, dataset } = fixture;
  assert.equal(captured.length, 2);
  const copied = await journalRows(restored.pool, environment, dataset);
  assert.deepEqual(copied, captured);
  assert.notEqual(restored.restoredStorage.prefix, fixture.datasetStorage.prefix);
  const roots = new Set<string>();
  for (const edge of copied) {
    assert.equal(edge.producer_kind, 'execution');
    assert.equal(edge.change_kind, 'replace');
    assert.equal(edge.mode, 'replace');
    assert.equal(edge.commit, edge.candidate);
    assert.equal(edge.complete_rows_root, edge.result_root);
    assert.match(edge.dataset_schema_hash, /^sha256:[0-9a-f]{64}$/);
    assert.match(edge.key_schema_hash, /^sha256:[0-9a-f]{64}$/);
    assert.deepEqual(edge.ordered_primary_key, ['id']);
    const producer = (
      await restored.pool.query(
        `SELECT c.execution,c.delta_profile,p.receipt,e.result FROM moria.runtime_snapshot_candidates c
      JOIN moria.runtime_executions e ON e.id=c.execution
      JOIN moria.publications p ON p.environment=c.environment AND p.execution=c.execution::text WHERE c.id=$1`,
        [edge.candidate],
      )
    ).rows[0];
    assert(producer);
    assert.equal(producer.execution, edge.execution);
    assert.equal(producer.delta_profile.datasetSchemaHash, edge.dataset_schema_hash);
    assert.deepEqual(producer.delta_profile.datasetSchema, edge.dataset_schema);
    assert.equal(producer.delta_profile.bindingRevision, edge.binding_revision);
    assert.equal(producer.delta_profile.keySchemaHash, edge.key_schema_hash);
    assert.deepEqual(producer.delta_profile.orderedPrimaryKey, edge.ordered_primary_key);
    assert.deepEqual(producer.receipt, edge.sealed_receipt);
    assert.deepEqual(producer.result, edge.sealed_receipt);
    roots.add(edge.result_root);
    if (edge.basis_root) roots.add(edge.basis_root);
  }
  assert.equal(copied[1].basis_root, copied[0].result_root);
  for (const root of roots) {
    const rows = (
      await restored.pool.query(
        `SELECT r.root->'root' AS content,r.physical_fingerprint AS original_physical,
      original.incarnation AS original_incarnation,q.physical_fingerprint,q.incarnation,
      m.physical_version,old.physical_version AS original_version,q.restore,receiver.restore AS producer_restore,control.restore_id
      FROM moria.runtime_dataset_roots r JOIN moria.runtime_dataset_producers original ON original.scope=r.producer
      JOIN moria.native_dataset_bindings b ON b.environment=r.environment AND b.resource::text=r.dataset AND b.enabled
      CROSS JOIN moria.indexing_instance instance
      JOIN moria.runtime_dataset_root_qualifications q ON q.root=r.key AND q.sealed AND q.incarnation=instance.incarnation AND q.physical_fingerprint=b.physical_binding_fingerprint
      JOIN moria.runtime_dataset_producers receiver ON receiver.scope=q.producer
      JOIN moria.runtime_recovery_control control ON control.restore_id=q.restore AND control.dispatch_enabled
      JOIN moria.runtime_dataset_qualified_members m ON m.root=q.root AND m.physical_fingerprint=q.physical_fingerprint AND m.incarnation=q.incarnation AND m.content=r.root->'root'
      JOIN moria.runtime_dataset_root_members old ON old.root=r.key AND old.content=m.content
      WHERE r.key=$1 AND r.environment=$2 AND r.dataset=$3 AND r.sealed
        AND q.namespace->>'warehouseId'=b.warehouse_id AND q.namespace->>'tableId'=b.table_id`,
        [root, environment, dataset],
      )
    ).rows;
    assert.equal(rows.length, 1);
    const selected = rows[0];
    assert.notEqual(selected.physical_fingerprint, selected.original_physical);
    assert.notEqual(selected.incarnation, selected.original_incarnation);
    assert.equal(selected.restore, selected.restore_id);
    assert.equal(selected.producer_restore, selected.restore_id);
    assert.notDeepEqual(selected.physical_version, selected.original_version);
    const version = (wire: { token: string }) => {
      assert.match(wire.token, /^s3v1\//);
      const parsed = JSON.parse(wire.token.slice('s3v1/'.length));
      assert.equal(typeof parsed[1], 'string');
      assert(parsed[1] && parsed[1] !== 'null');
      return parsed[1] as string;
    };
    const currentVersion = version(selected.physical_version),
      oldVersion = version(selected.original_version);
    const content = selected.content;
    const key = `${restored.restoredStorage.prefix}/${content.object.warehouseId}/${content.object.tableId}/objects/${content.object.objectId}`;
    const exact = { Bucket: fixture.s3.bucket, Key: key, VersionId: currentVersion };
    const head = await fixture.s3.client.send(new HeadObjectCommand(exact));
    assert.equal(head.VersionId, currentVersion);
    assert.equal(head.ContentLength, content.size);
    const response = await fixture.s3.client.send(new GetObjectCommand(exact));
    assert.equal(response.VersionId, currentVersion);
    assert(response.Body);
    const bytes = await response.Body.transformToByteArray();
    assert.equal('sha256:' + createHash('sha256').update(bytes).digest('hex'), content.sha256);
    await assert.rejects(
      fixture.s3.client.send(new HeadObjectCommand({ ...exact, VersionId: oldVersion })),
      (error: unknown) =>
        error instanceof S3ServiceException && error.$metadata.httpStatusCode === 404,
    );
  }
  await assert.rejects(
    restored.pool.query(
      `SELECT moria.runtime_dataset_commit_record_execution(c.session_hash,c.execution,c.id,d.sealed_receipt)
    FROM moria.runtime_dataset_commit_deltas d JOIN moria.runtime_snapshot_candidates c ON c.id=d.candidate WHERE d.commit=$1`,
      [copied[0].commit],
    ),
    /CANDIDATE_STALE/,
  );
  assert.deepEqual(await journalRows(restored.pool, environment, dataset), captured);
}
