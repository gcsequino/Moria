import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { DeleteObjectCommand, HeadObjectCommand, S3ServiceException } from '@aws-sdk/client-s3';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';
import { retentionRecovery } from './fixtures/native-retention/recovery.js';
import { currentWrapperCopy, versionTrace } from './fixtures/native-retention/version-trace.js';

test(
  'v3 backup rejects a missing qualified version despite an identical current copy',
  { timeout: 120000 },
  async (t) => {
    const trace = versionTrace();
    const fixture = await managedRetentionDataset({ primaryKey: ['id'], wrapS3: trace.wrap });
    t.after(fixture.close);
    const rows = Array.from({ length: 8 }, (_, i) => ({
      id: randomUUID(),
      name: 'pinned-' + i,
      tags: ['exact'],
    }));
    const first = await fixture.publish(rows, { graph: true });
    assert.match(first.head.metadataLocation!, /^moria-snapshot-v3:/);
    const copied = await currentWrapperCopy(fixture, first.head.metadataLocation, 'root');
    await fixture.s3.client.send(
      new DeleteObjectCommand({ ...copied.object, VersionId: copied.pinned }),
    );
    await assert.rejects(
      fixture.s3.client.send(new HeadObjectCommand({ ...copied.object, VersionId: copied.pinned })),
      (error: unknown) =>
        error instanceof S3ServiceException && error.$metadata.httpStatusCode === 404,
    );
    assert.equal(
      (await fixture.s3.client.send(new HeadObjectCommand(copied.object))).VersionId,
      copied.current,
    );
    const recovery = await retentionRecovery(fixture);
    trace.select('/' + copied.object.Bucket + '/' + copied.object.Key);
    await assert.rejects(recovery.capture('v3-missing-pinned-root'), (error: unknown) => {
      assert(error instanceof Error);
      const lines = error.message.trim().split(String.fromCharCode(10));
      assert(lines[0]!.startsWith('recovery/capture: '));
      assert.deepEqual(JSON.parse(lines.at(-1)!), {
        databaseCode: null,
        error: 'RECOVERY_ENUMERATION_FAILED',
      });
      return true;
    });
    assert(
      trace.entries.some(
        (entry) =>
          entry.method === 'GET' && entry.requestVersion === copied.pinned && entry.status === 404,
      ),
    );
    assert(
      !trace.entries.some(
        (entry) => entry.method === 'GET' && entry.requestVersion === copied.current,
      ),
    );
    const sets = (
      await fixture.db.pool.query(
        'SELECT status,sealed_manifest,content_inventory FROM moria.runtime_recovery_sets WHERE request_key=$1',
        ['v3-missing-pinned-root'],
      )
    ).rows;
    assert.equal(sets.length, 1);
    assert.equal(sets[0].status, 'failed');
    assert.equal(sets[0].sealed_manifest, null);
    assert.equal(sets[0].content_inventory, null);
    assert.deepEqual(
      (await fixture.db.catalog.heads(fixture.environment, [fixture.dataset]))[0],
      first.head,
    );
    assert.equal(
      (await fixture.db.pool.query('SELECT count(*) FROM moria.runtime_restores')).rows[0].count,
      '0',
    );
    assert.equal(
      (await fixture.db.pool.query('SELECT dispatch_approved FROM public.moria_restore_bootstrap'))
        .rows[0].dispatch_approved,
      true,
    );
    t.diagnostic(
      JSON.stringify({
        pinnedVersion: copied.pinned,
        currentVersion: copied.current,
        requests: trace.entries,
        failedCaptureUnsealed: true,
        headUnchanged: true,
        noRestoreCreated: true,
      }),
    );
  },
);
