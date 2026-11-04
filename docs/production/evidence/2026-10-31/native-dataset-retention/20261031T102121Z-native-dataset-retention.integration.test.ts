import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import pg from 'pg';
import { HeadObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';
import { retentionRecovery } from './fixtures/native-retention/recovery.js';
import {
  ageClosedDatasetHistory,
  ageClosedUnclaimedCandidates,
} from './fixtures/native-retention/history.js';

test(
  'managed Dataset expiry and exact cleanup preserve current rows and independent recovery copies',
  { timeout: 300000 },
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
    const policy = await fixture.command(
      ['dataset-retention', 'approve'],
      approval,
      fixture.operatorEnv,
    );
    const ordinary = new pg.Pool({ connectionString: fixture.nativeEnv.RETENTION_DATABASE });
    try {
      await assert.rejects(
        ordinary.query(
          'UPDATE moria.runtime_dataset_retention_policies SET enabled=false WHERE id=$1',
          [policy.policy],
        ),
        /RECOVERY_OPERATOR_REQUIRED/,
      );
      await assert.rejects(
        ordinary.query(
          "INSERT INTO moria.runtime_dataset_retention_policies SELECT (jsonb_populate_record(NULL::moria.runtime_dataset_retention_policies,to_jsonb(p)||jsonb_build_object('id',$2::text,'request_key','forged-native-policy','approved_role',current_user))).* FROM moria.runtime_dataset_retention_policies p WHERE id=$1",
          [policy.policy, randomUUID()],
        ),
        /RECOVERY_OPERATOR_REQUIRED/,
      );
      assert.equal(
        (
          await db.pool.query(
            'SELECT enabled FROM moria.runtime_dataset_retention_policies WHERE id=$1',
            [policy.policy],
          )
        ).rows[0].enabled,
        true,
      );
    } finally {
      await ordinary.end();
    }

    const expire = (requestKey: string) =>
      fixture.command(
        ['dataset-retention', 'expire'],
        {
          policy: policy.policy,
          requestKey,
          owner: randomUUID(),
          beforeRevision: (BigInt(second.head.revision) + 1n).toString(),
          maximumRoots: 64,
          timeSeconds: 120,
        },
        fixture.operatorEnv,
      );
    const young = await expire('actual-young-root');
    assert.deepEqual(young.expiredRoots, []);
    assert.equal(young.headChanged, false);

    await ageClosedDatasetHistory(fixture);
    const recovery = await retentionRecovery(fixture);
    const captured = await recovery.capture('before-dataset-expiration');
    const expired = await expire('synthetic-aged-closed-root');
    assert.equal(expired.headChanged, true);
    assert.deepEqual(expired.expiredRoots, roots.map((root) => root.key).sort());
    const current = (await db.catalog.heads(environment, [dataset]))[0];
    assert.equal(current.snapshotId, second.head.snapshotId);
    assert.notEqual(current.metadataLocation, second.head.metadataLocation);
    await fixture.read(current);
    await assert.rejects(fixture.read(first.head), /DATASET_SNAPSHOT_EXPIRED/);
    const planned = await fixture.command(
      ['dataset-retention', 'plan'],
      {
        policy: policy.policy,
        requestKey: 'actual-first-orphan-observation',
        after: null,
        limit: 256,
      },
      fixture.operatorEnv,
    );
    assert(planned.added > 0);
    assert.equal(planned.graceSeconds, 86400);
    const immediate = await fixture.command(
      ['dataset-retention', 'once'],
      {
        policy: policy.policy,
        owner: randomUUID(),
        timeSeconds: 30,
      },
      fixture.operatorEnv,
    );
    assert.equal(immediate.claimed, false);
    assert.equal(
      (await db.pool.query('SELECT count(*) FROM moria.runtime_dataset_gc_versions')).rows[0].count,
      '0',
    );
    const candidates = (
      await db.pool.query(
        'SELECT c.*,m.physical_version FROM moria.runtime_dataset_gc_candidates c LEFT JOIN LATERAL(SELECT physical_version FROM moria.runtime_dataset_root_members m WHERE m.warehouse_id=c.warehouse_id AND m.table_id=c.table_id AND m.object_id=c.object_id LIMIT 1)m ON true WHERE c.policy=$1 ORDER BY c.id',
        [policy.policy],
      )
    ).rows;
    assert.equal(candidates.length, planned.added);
    for (const candidate of candidates) {
      candidate.certified = candidate.physical_version !== null;
      const key = `${datasetStorage.prefix}/${candidate.warehouse_id}/${candidate.table_id}/objects/${candidate.object_id}`;
      try {
        const actual = await s3.client.send(new HeadObjectCommand({ Bucket: s3.bucket, Key: key }));
        assert.ok(actual.VersionId && actual.VersionId !== 'null');
        candidate.physical_version ??= {
          size: actual.ContentLength,
          token: 's3v1/' + JSON.stringify([actual.ETag, actual.VersionId]),
        };
      } catch (error: any) {
        if (error.$metadata?.httpStatusCode !== 404) throw error;
        assert.equal(candidate.physical_version, null);
      }
    }

    const replaced = candidates.find((candidate) => candidate.certified);
    assert.ok(replaced);
    const replacementKey = `${datasetStorage.prefix}/${replaced.warehouse_id}/${replaced.table_id}/objects/${replaced.object_id}`;
    const replacement = await s3.client.send(
      new PutObjectCommand({
        Bucket: s3.bucket,
        Key: replacementKey,
        Body: Buffer.from('external-newer-version'),
      }),
    );
    await ageClosedUnclaimedCandidates(fixture, policy.policy);
    for (let i = 0; i < candidates.length; i++) {
      const collected = await fixture.command(
        ['dataset-retention', 'once'],
        {
          policy: policy.policy,
          owner: randomUUID(),
          timeSeconds: 30,
        },
        fixture.operatorEnv,
      );
      assert.equal(collected.claimed, true);
      const candidate = candidates.find((candidate) => candidate.id === collected.claim);
      assert.ok(candidate);
      assert.equal(collected.removedVersion, candidate.physical_version !== null);
    }
    for (const candidate of candidates) {
      if (candidate.physical_version === null) continue;
      const [, version] = JSON.parse(candidate.physical_version.token.slice('s3v1/'.length));
      await assert.rejects(
        s3.client.send(
          new HeadObjectCommand({
            Bucket: s3.bucket,
            Key: `${datasetStorage.prefix}/${candidate.warehouse_id}/${candidate.table_id}/objects/${candidate.object_id}`,
            VersionId: version,
          }),
        ),
        (error: any) => error.$metadata?.httpStatusCode === 404,
      );
    }
    assert.equal(
      (await s3.client.send(new HeadObjectCommand({ Bucket: s3.bucket, Key: replacementKey })))
        .VersionId,
      replacement.VersionId,
    );
    await fixture.read(current);
    const newerCapture = await recovery.capture('after-dataset-expiration-and-source-cleanup');
    assert.notEqual(newerCapture.recoverySet, captured.recoverySet);
    const restored = await recovery.restore(captured.manifest);
    try {
      const restoredPolicy = await fixture.command(
        ['dataset-retention', 'approve'],
        { ...approval, requestKey: 'restored-native-policy' },
        restored.env,
      );
      const qualified = (
        await restored.pool.query(
          'SELECT count(DISTINCT q.root)::int roots,count(m.object_id)::int members,bool_and(q.sealed AND q.incarnation=i.incarnation AND q.physical_fingerprint=p.physical_fingerprint) AS current FROM moria.runtime_dataset_root_qualifications q JOIN moria.runtime_dataset_roots r ON r.key=q.root JOIN moria.runtime_dataset_qualified_members m ON m.root=q.root AND m.physical_fingerprint=q.physical_fingerprint AND m.incarnation=q.incarnation CROSS JOIN moria.indexing_instance i JOIN moria.runtime_dataset_retention_policies p ON p.id=$1 WHERE r.environment=$2 AND r.dataset=$3 AND r.metadata_location=ANY($4::text[])',
          [restoredPolicy.policy, environment, dataset, [first.head.metadataLocation, second.head.metadataLocation]],
        )
      ).rows[0];
      assert.equal(qualified.roots, 2);
      assert.equal(qualified.current, true);
      assert.equal(qualified.members, members.length);
      const secondRows = rows.map((row) => ({ ...row, name: 'second-' + row.name }));
      await restored.read(first.head, rows);
      await restored.read(second.head, secondRows);
      const restoredCurrent = (await restored.catalog.heads(environment, [dataset]))[0];
      assert.equal(restoredCurrent.snapshotId, second.head.snapshotId);
      await restored.read(restoredCurrent, secondRows);
      await restored.rejectPhysicalReuse();
      await restored.read(restoredCurrent, secondRows);
    } finally {
      await restored.close();
    }
  },
);
