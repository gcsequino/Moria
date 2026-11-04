import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import test from 'node:test';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { managedRetentionDataset } from '/work/tests/fixtures/native-retention/dataset.js';
import { ageClosedDatasetHistory } from '/work/tests/fixtures/native-retention/history.js';

test(
  'finite metadata pruning preserves a live old-root reader and all physical retention pins',
  { timeout: 120000 },
  async (t) => {
    const fixture = await managedRetentionDataset();
    t.after(fixture.close);
    const firstRows = Array.from({ length: 64 }, (_, i) => ({
      id: randomUUID(),
      name: 'old-' + i,
      tags: ['historical', 'typed'],
    }));
    const first = await fixture.publish(firstRows),
      secondRows = firstRows.map((row) => ({ ...row, name: 'current-' + row.name })),
      second = await fixture.publish(secondRows);
    await ageClosedDatasetHistory(fixture);
    const policy = await fixture.command(
      ['dataset-retention', 'approve'],
      {
        requestKey: 'metadata-policy',
        environment: fixture.environment,
        dataset: fixture.dataset,
        retainSeconds: 86400,
        recheckSeconds: 30,
      },
      fixture.operatorEnv,
    );
    const readJson = async (content: any) => {
      assert(content.size > 0 && content.size < 1024 * 1024);
      const response = await fixture.s3.client.send(
        new GetObjectCommand({
          Bucket: fixture.s3.bucket,
          Key: `${fixture.datasetStorage.prefix}/${content.object.warehouseId}/${content.object.tableId}/objects/${content.object.objectId}`,
        }),
      );
      assert.equal(response.ContentLength, content.size);
      const body = await response.Body!.transformToByteArray();
      assert.equal('sha256:' + createHash('sha256').update(body).digest('hex'), content.sha256);
      return JSON.parse(Buffer.from(body).toString());
    };
    const metadata = async (location: string) => {
      assert(location.startsWith('moria-snapshot-v2:'));
      const reference = JSON.parse(location.slice('moria-snapshot-v2:'.length));
      const root = await readJson(reference.root);
      return await readJson(root.proof.metadata);
    };
    const before = await metadata(second.head.metadataLocation!);
    assert.equal(before.snapshots.length, 2);
    const binding = await fixture.bind(
      [
        {
          resource: fixture.dataset,
          revision: first.head.revision,
          snapshot: first.head.snapshotId,
        },
      ],
      {},
    );
    const child = spawn(
      fixture.harness,
      ['--ignored', '--exact', 'catalog_publication', '--nocapture', '--test-threads=1'],
      {
        env: {
          ...fixture.nativeEnv,
          MORIA_NATIVE_CATALOG_FIXTURE: JSON.stringify({
            root: fixture.db.root,
            token: fixture.token,
            binding,
            mode: 'read-held',
            expectedRows: 64,
            writerRows: { [fixture.dataset]: firstRows },
            snapshots: [{ ...fixture.snapshot, snapshotId: first.head.snapshotId }],
            nativeRef: first.head.metadataLocation,
          }),
        },
        stdio: ['pipe', 'pipe', 'pipe'],
      },
    );
    let out = '',
      errors = '',
      settled = false;
    let readyResolve!: (v: any) => void, readyReject!: (v: any) => void;
    const ready = new Promise<any>((resolve, reject) => {
      readyResolve = resolve;
      readyReject = reject;
    });
    void ready.catch(() => {});
    child.stdout.on('data', (part) => {
      out = (out + part).slice(-65536);
      if (!settled) {
        const line = out.split('\n').find((line) => line.includes('"event":"held_read_ready"'));
        if (line) {
          settled = true;
          readyResolve(JSON.parse(line.slice(line.indexOf('{'))));
        }
      }
    });
    child.stderr.on('data', (part) => (errors = (errors + part).slice(-65536)));
    const exited = new Promise<number | null>((resolve) =>
      child.once('exit', (code) => {
        if (!settled)
          readyReject(new Error('Held reader exited ' + code + ' ' + errors + ' ' + out));
        resolve(code);
      }),
    );
    try {
      const held = await ready;
      const pin = (
        await fixture.db.pool.query(
          "SELECT root_key,moria.runtime_dataset_execution_pin_live(owner_id) AS active FROM moria.runtime_dataset_root_pins WHERE owner_kind='execution' AND owner_id=$1",
          [held.execution],
        )
      ).rows;
      assert.equal(pin.length, 1);
      assert.equal(pin[0].active, true);
      const live = (
        await fixture.db.pool.query(
          "SELECT e.status,t.lease_until>clock_timestamp() AS leased FROM moria.runtime_executions e JOIN moria.runtime_execution_tasks t ON t.execution=e.id AND t.status='running' WHERE e.id=$1",
          [held.execution],
        )
      ).rows[0];
      assert.equal(live.status, 'running');
      assert.equal(live.leased, true);
      const expired = await fixture.command(
        ['dataset-retention', 'expire'],
        {
          policy: policy.policy,
          requestKey: 'metadata-only',
          owner: randomUUID(),
          beforeRevision: second.head.revision,
          maximumRoots: 64,
          timeSeconds: 60,
        },
        fixture.operatorEnv,
      );
      assert.equal(expired.headChanged, true);
      assert.deepEqual(expired.expiredRoots, []);
      const current = (await fixture.db.catalog.heads(fixture.environment, [fixture.dataset]))[0];
      assert.equal(current.snapshotId, second.head.snapshotId);
      assert.equal(BigInt(current.revision), BigInt(second.head.revision) + 1n);
      const after = await metadata(current.metadataLocation!);
      assert.equal(after.snapshots.length, 1);
      assert.equal(String(after['current-snapshot-id']), second.head.snapshotId);
      assert.deepEqual(after.schemas, before.schemas);
      assert.deepEqual(after.refs, before.refs);
      assert.deepEqual(after['metadata-log'], []);
      assert.equal(
        (await fixture.db.pool.query('SELECT count(*) FROM moria.runtime_dataset_expired_roots'))
          .rows[0].count,
        '0',
      );
      assert.equal(
        (
          await fixture.db.pool.query('SELECT moria.runtime_dataset_root_pinned($1) AS pinned', [
            pin[0].root_key,
          ])
        ).rows[0].pinned,
        true,
      );
      const replay = await fixture.command(
        ['dataset-retention', 'expire'],
        {
          policy: policy.policy,
          requestKey: 'already-pruned',
          owner: randomUUID(),
          beforeRevision: second.head.revision,
          maximumRoots: 64,
          timeSeconds: 60,
        },
        fixture.operatorEnv,
      );
      assert.equal(replay.headChanged, false);
      assert.equal(
        (await fixture.db.catalog.heads(fixture.environment, [fixture.dataset]))[0].revision,
        current.revision,
      );
      child.stdin.end('\n');
      assert.equal(await exited, 0, errors + ' ' + out);
      assert.match(
        out,
        /"event":"read","revision":"1","rows":64|"event":"read","rows":64,"revision":"1"/,
      );
      await fixture.command(['execution', 'cancel'], { id: held.execution });
      await fixture.read(first.head);
      await fixture.read(current);
      t.diagnostic(
        JSON.stringify({
          metadataSnapshotsBefore: before.snapshots.length,
          metadataSnapshotsAfter: after.snapshots.length,
          oldRootUnexpired: true,
          liveReaderRows: 64,
          currentRows: 64,
          idempotentNoop: true,
          indexingIntent: 'normal validated publication path retained',
        }),
      );
    } finally {
      child.kill('SIGKILL');
      await exited;
    }
  },
);
