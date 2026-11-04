import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { HeadObjectCommand } from '@aws-sdk/client-s3';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';

test(
  'managed Dataset native writers journal exact versioned objects and real execution pins',
  { timeout: 180000 },
  async (t) => {
    const fixture = await managedRetentionDataset();
    t.after(fixture.close);
    const { db, environment, dataset, s3, datasetStorage } = fixture;
    const rows = Array.from({ length: 64 }, (_, i) => ({
      id: randomUUID(),
      name: 'first-' + i,
      tags: ['retained', 'typed-' + i],
    }));
    const first = await fixture.publish(rows);
    const second = await fixture.publish(
      rows.map((row) => ({ ...row, name: 'second-' + row.name })),
    );
    assert.notEqual(first.head.snapshotId, second.head.snapshotId);
    const roots = (
      await db.pool.query(
        'SELECT key,sealed,reader_profile,metadata_location FROM moria.runtime_dataset_roots WHERE environment=$1 AND dataset=$2 ORDER BY created_at',
        [environment, dataset],
      )
    ).rows;
    assert.equal(roots.length, 2);
    assert(
      roots.every(
        (root) => root.sealed && root.reader_profile === 'iceberg-v2-cow-flat-bounded-v2',
      ),
    );
    const members = (
      await db.pool.query(
        'SELECT m.* FROM moria.runtime_dataset_root_members m JOIN moria.runtime_dataset_roots r ON r.key=m.root WHERE r.environment=$1 AND r.dataset=$2',
        [environment, dataset],
      )
    ).rows;
    assert(members.length >= 6);
    for (const member of members) {
      assert.match(member.physical_version.token, /^s3v1\//);
      const [etag, version] = JSON.parse(member.physical_version.token.slice('s3v1/'.length));
      assert(version && version !== 'null');
      const physical = await s3.client.send(
        new HeadObjectCommand({
          Bucket: s3.bucket,
          Key: `${datasetStorage.prefix}/${member.warehouse_id}/${member.table_id}/objects/${member.object_id}`,
          VersionId: version,
        }),
      );
      assert.equal(physical.ETag, etag);
      assert.equal(physical.ContentLength, member.physical_version.size);
    }
    const issued = (
      await db.pool.query(
        'SELECT count(*) FROM moria.runtime_dataset_issued_objects o JOIN moria.runtime_dataset_producers p ON p.scope=o.producer WHERE p.execution=ANY($1::uuid[])',
        [[first.execution, second.execution]],
      )
    ).rows[0].count;
    assert(BigInt(issued) > 0n);
    const writerPin = (
      await db.pool.query(
        "SELECT p.root_key FROM moria.runtime_dataset_root_pins p WHERE p.owner_kind='execution' AND p.owner_id=$1",
        [second.execution],
      )
    ).rows;
    assert(
      writerPin.some(
        (pin) =>
          pin.root_key ===
          roots.find((root) => root.metadata_location === first.head.metadataLocation)!.key,
      ),
    );
    await fixture.read(second.head);
    const held = await fixture.read(first.head, false);
    const pin = (
      await db.pool.query(
        "SELECT p.root_key,moria.runtime_dataset_execution_pin_live(p.owner_id) AS active FROM moria.runtime_dataset_root_pins p WHERE p.owner_kind='execution' AND p.owner_id=$1",
        [held.execution],
      )
    ).rows;
    assert.equal(pin.length, 1);
    assert.equal(pin[0].active, true);
    await fixture.command(['execution', 'cancel'], { id: held.execution });
    const status = (
      await db.pool.query('SELECT status FROM moria.runtime_executions WHERE id=$1', [
        held.execution,
      ])
    ).rows[0].status;
    assert.equal(status, 'cancelled');
    const approval = {
      requestKey: 'retention-native-policy',
      environment,
      dataset,
      retainSeconds: 86400,
      recheckSeconds: 30,
    };
    await assert.rejects(fixture.command(['dataset-retention', 'approve'], approval));
    const policy = await fixture.command(['dataset-retention', 'approve'], approval, fixture.operatorEnv);
    const expire = (requestKey: string) => fixture.command(['dataset-retention', 'expire'], {
      policy: policy.policy,
      requestKey,
      owner: randomUUID(),
      beforeRevision: second.head.revision,
      maximumRoots: 64,
      timeSeconds: 120,
    }, fixture.operatorEnv);
    const young = await expire('actual-young-root');
    assert.deepEqual(young.expiredRoots, []);
    assert.equal(young.headChanged, false);

    // Import synthetic historical age only after actual native producers and readers have closed.
    const client = await db.pool.connect();
    try {
      await client.query('BEGIN');
      const active = (await client.query(
        "SELECT count(*) FROM moria.runtime_executions WHERE environment=$1 AND (status IN ('queued','running','preparing') OR lease_until>clock_timestamp())",
        [environment],
      )).rows[0].count;
      assert.equal(active, '0');
      await client.query('LOCK TABLE moria.runtime_dataset_roots,moria.runtime_dataset_issued_objects IN ACCESS EXCLUSIVE MODE');
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
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
    const expired = await expire('synthetic-aged-closed-root');
    assert.equal(expired.headChanged, true);
    assert.deepEqual(expired.expiredRoots, [roots.find((root) => root.metadata_location === first.head.metadataLocation)!.key]);
    const current = (await db.catalog.heads(environment, [dataset]))[0];
    assert.equal(current.snapshotId, second.head.snapshotId);
    assert.notEqual(current.metadataLocation, second.head.metadataLocation);
    await fixture.read(current);
    await assert.rejects(fixture.read(first.head), /DATASET_SNAPSHOT_EXPIRED/);
    const planned = await fixture.command(['dataset-retention', 'plan'], {
      policy: policy.policy,
      requestKey: 'actual-first-orphan-observation',
      after: null,
      limit: 256,
    }, fixture.operatorEnv);
    assert(planned.added > 0);
    assert.equal(planned.graceSeconds, 86400);
    const immediate = await fixture.command(['dataset-retention', 'once'], {
      policy: policy.policy, owner: randomUUID(), timeSeconds: 30,
    }, fixture.operatorEnv);
    assert.equal(immediate.claimed, false);
    assert.equal((await db.pool.query('SELECT count(*) FROM moria.runtime_dataset_gc_versions')).rows[0].count, '0');

  },
);
