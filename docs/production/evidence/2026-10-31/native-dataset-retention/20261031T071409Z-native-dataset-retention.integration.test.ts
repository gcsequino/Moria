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
  },
);
