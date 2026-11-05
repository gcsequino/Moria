import assert from 'node:assert/strict';
import { randomUUID, createHash } from 'node:crypto';
import {
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3ServiceException,
} from '@aws-sdk/client-s3';
import type { managedRetentionDataset } from './dataset.js';

type Fixture = Awaited<ReturnType<typeof managedRetentionDataset>>;
type Head = { revision: string; snapshotId: string | null; metadataLocation: string | null };
export type QualificationFault =
  'profile' | 'physical' | 'incarnation' | 'memberVersion' | 'missingMember';

/** Corrupt only this synthetic closed producer's receipt, then restore every guard before the tested operation. */
export async function qualificationFault(fixture: Fixture, head: Head, fault: QualificationFault) {
  const { db } = fixture;
  const rows = (
    await db.pool.query(
      'SELECT r.key,r.producer,m.content,m.physical_version FROM moria.runtime_dataset_roots r JOIN moria.runtime_dataset_root_members m ON m.root=r.key WHERE r.environment=$1 AND r.dataset=$2 AND r.key=moria.runtime_dataset_root_key($3) ORDER BY m.object_id LIMIT 1',
      [fixture.environment, fixture.dataset, head.metadataLocation],
    )
  ).rows;
  assert.equal(rows.length, 1);
  const root = rows[0];
  assert.equal(
    (
      await db.pool.query('SELECT moria.runtime_dataset_producer_closed($1) AS closed', [
        root.producer,
      ])
    ).rows[0].closed,
    true,
  );
  if (fault === 'memberVersion') {
    const content = root.content;
    assert(BigInt(content.size) <= 1024n * 1024n);
    assert.match(root.physical_version.token, /^s3v1\//);
    const [, version] = JSON.parse(root.physical_version.token.slice('s3v1/'.length));
    assert(version && version !== 'null');
    const object = {
      Bucket: fixture.s3.bucket,
      Key: `${fixture.datasetStorage.prefix}/${content.object.warehouseId}/${content.object.tableId}/objects/${content.object.objectId}`,
    };
    const prior = await fixture.s3.client.send(
      new GetObjectCommand({ ...object, VersionId: version }),
    );
    assert(prior.Body);
    const bytes = await prior.Body.transformToByteArray();
    assert.equal(bytes.length, Number(content.size));
    assert.equal('sha256:' + createHash('sha256').update(bytes).digest('hex'), content.sha256);
    const replaced = await fixture.s3.client.send(
      new PutObjectCommand({ ...object, Body: bytes, Metadata: prior.Metadata }),
    );
    assert(replaced.VersionId && replaced.VersionId !== 'null');
    assert.notEqual(replaced.VersionId, version);
    assert.equal(
      (await fixture.s3.client.send(new HeadObjectCommand(object))).VersionId,
      replaced.VersionId,
    );
    await fixture.s3.client.send(new DeleteObjectCommand({ ...object, VersionId: version }));
    await assert.rejects(
      fixture.s3.client.send(new HeadObjectCommand({ ...object, VersionId: version })),
      (error: unknown) =>
        error instanceof S3ServiceException && error.$metadata.httpStatusCode === 404,
    );
    return;
  }
  const client = await db.pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(
      'LOCK TABLE moria.runtime_dataset_roots,moria.runtime_dataset_root_members,moria.runtime_dataset_producers IN ACCESS EXCLUSIVE MODE',
    );
    assert.equal(
      (
        await client.query('SELECT moria.runtime_dataset_producer_closed($1) AS closed', [
          root.producer,
        ])
      ).rows[0].closed,
      true,
    );
    if (fault === 'profile' || fault === 'physical') {
      await client.query(
        'ALTER TABLE moria.runtime_dataset_roots DISABLE TRIGGER runtime_dataset_root_immutable',
      );
      const changed =
        fault === 'profile'
          ? await client.query(
              "UPDATE moria.runtime_dataset_roots SET reader_profile='unsupported-synthetic-reader' WHERE key=$1",
              [root.key],
            )
          : await client.query(
              'UPDATE moria.runtime_dataset_roots SET physical_fingerprint=$2 WHERE key=$1',
              [root.key, 'sha256:' + 'b'.repeat(64)],
            );
      assert.equal(changed.rowCount, 1);
      await client.query(
        'ALTER TABLE moria.runtime_dataset_roots ENABLE TRIGGER runtime_dataset_root_immutable',
      );
    } else if (fault === 'incarnation') {
      await client.query(
        'ALTER TABLE moria.runtime_dataset_producers DISABLE TRIGGER runtime_dataset_producer_immutable',
      );
      assert.equal(
        (
          await client.query(
            'UPDATE moria.runtime_dataset_producers SET incarnation=$2 WHERE scope=$1',
            [root.producer, randomUUID()],
          )
        ).rowCount,
        1,
      );
      await client.query(
        'ALTER TABLE moria.runtime_dataset_producers ENABLE TRIGGER runtime_dataset_producer_immutable',
      );
    } else {
      await client.query(
        'ALTER TABLE moria.runtime_dataset_root_members DISABLE TRIGGER runtime_dataset_member_immutable',
      );
      assert.equal(
        (
          await client.query(
            'DELETE FROM moria.runtime_dataset_root_members WHERE root=$1 AND warehouse_id=$2 AND table_id=$3 AND object_id=$4',
            [
              root.key,
              root.content.object.warehouseId,
              root.content.object.tableId,
              root.content.object.objectId,
            ],
          )
        ).rowCount,
        1,
      );
      await client.query(
        'ALTER TABLE moria.runtime_dataset_root_members ENABLE TRIGGER runtime_dataset_member_immutable',
      );
    }
    assert.equal(
      (
        await client.query(
          "SELECT count(*) FROM pg_trigger WHERE tgrelid IN ('moria.runtime_dataset_roots'::regclass,'moria.runtime_dataset_root_members'::regclass,'moria.runtime_dataset_producers'::regclass) AND NOT tgisinternal AND tgenabled<>'O'",
        )
      ).rows[0].count,
      '0',
    );
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}
