import assert from 'node:assert/strict';
import { randomUUID, createHash } from 'node:crypto';
import test from 'node:test';
import {
  GetObjectCommand,
  PutObjectCommand,
  HeadObjectCommand,
  DeleteObjectCommand,
  S3ServiceException,
} from '@aws-sdk/client-s3';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';
import { bootstrapGraph } from './fixtures/native-retention/graph.js';
import { versionTrace } from './fixtures/native-retention/version-trace.js';

type Fixture = Awaited<ReturnType<typeof managedRetentionDataset>>;

async function currentWrapperCopy(
  fixture: Fixture,
  metadataLocation: string | null,
  kind: 'root' | 'page',
) {
  const member = (
    await fixture.db.pool.query(
      "SELECT m.content,m.physical_version FROM moria.runtime_dataset_roots r JOIN moria.runtime_dataset_root_members m ON m.root=r.key AND m.object_id=r.root->'root'->'object'->>'objectId' WHERE r.environment=$1 AND r.dataset=$2 AND r.metadata_location=$3",
      [fixture.environment, fixture.dataset, metadataLocation],
    )
  ).rows;
  assert.equal(member.length, 1);
  if (kind === 'page') {
    const root = member[0];
    const [, rootVersion] = JSON.parse(root.physical_version.token.slice('s3v1/'.length));
    const rootKey = `${fixture.datasetStorage.prefix}/${root.content.object.warehouseId}/${root.content.object.tableId}/objects/${root.content.object.objectId}`;
    const fetched = await fixture.s3.client.send(
      new GetObjectCommand({ Bucket: fixture.s3.bucket, Key: rootKey, VersionId: rootVersion }),
    );
    assert(fetched.Body);
    const rootBytes = await fetched.Body.transformToByteArray();
    assert.equal(
      'sha256:' + createHash('sha256').update(rootBytes).digest('hex'),
      root.content.sha256,
    );
    const page = JSON.parse(Buffer.from(rootBytes).toString('utf8')).lastPage;
    assert(page?.object?.objectId);
    const located = (
      await fixture.db.pool.query(
        'SELECT m.content,m.physical_version FROM moria.runtime_dataset_roots r JOIN moria.runtime_dataset_root_members m ON m.root=r.key WHERE r.environment=$1 AND r.dataset=$2 AND r.metadata_location=$3 AND m.object_id=$4',
        [fixture.environment, fixture.dataset, metadataLocation, page.object.objectId],
      )
    ).rows;
    assert.equal(located.length, 1);
    member[0] = located[0];
  }
  const { content, physical_version: version } = member[0];
  assert.match(version.token, /^s3v1\//);
  const [, pinned] = JSON.parse(version.token.slice('s3v1/'.length));
  assert(pinned && pinned !== 'null');
  const object = {
    Bucket: fixture.s3.bucket,
    Key: `${fixture.datasetStorage.prefix}/${content.object.warehouseId}/${content.object.tableId}/objects/${content.object.objectId}`,
  };
  const prior = await fixture.s3.client.send(
    new GetObjectCommand({ ...object, VersionId: pinned }),
  );
  assert(prior.Body);
  const bytes = await prior.Body.transformToByteArray();
  assert.equal(bytes.length, Number(content.size));
  assert.equal('sha256:' + createHash('sha256').update(bytes).digest('hex'), content.sha256);
  const copied = await fixture.s3.client.send(
    new PutObjectCommand({ ...object, Body: bytes, Metadata: prior.Metadata }),
  );
  const current = copied.VersionId;
  assert(current && current !== 'null');
  assert.notEqual(current, pinned);
  assert.equal((await fixture.s3.client.send(new HeadObjectCommand(object))).VersionId, current);
  return { object, pinned, current };
}

for (const kind of ['root', 'page'] as const) {
  for (const present of [false, true]) {
    test(
      `a qualified native ${kind} ${present ? 'reads a still-live' : 'rejects a missing'} pinned version despite a current copy`,
      { timeout: 120000 },
      async (t) => {
        const trace = versionTrace();
        const fixture = await managedRetentionDataset({ primaryKey: ['id'], wrapS3: trace.wrap });
        t.after(fixture.close);
        const rows = Array.from({ length: 8 }, (_, i) => ({
          id: randomUUID(),
          name: 'wrapper-' + i,
          tags: ['pinned'],
        }));
        const first = await fixture.publish(rows);
        const { object, pinned, current } = await currentWrapperCopy(
          fixture,
          first.head.metadataLocation,
          kind,
        );
        if (present) {
          assert.equal(
            (await fixture.s3.client.send(new HeadObjectCommand({ ...object, VersionId: pinned })))
              .VersionId,
            pinned,
          );
        } else {
          await fixture.s3.client.send(new DeleteObjectCommand({ ...object, VersionId: pinned }));
          await assert.rejects(
            fixture.s3.client.send(new HeadObjectCommand({ ...object, VersionId: pinned })),
            (error: unknown) =>
              error instanceof S3ServiceException && error.$metadata.httpStatusCode === 404,
          );
        }
        trace.select(`/${object.Bucket}/${object.Key}`);
        try {
          if (present) {
            await bootstrapGraph(fixture, first.head, rows);
            assert(
              trace.entries.some(
                (entry) =>
                  entry.method === 'GET' &&
                  entry.requestVersion === pinned &&
                  entry.responseVersion === pinned &&
                  entry.status === 206,
              ),
            );
          } else {
            await assert.rejects(bootstrapGraph(fixture, first.head, rows));
            assert(
              trace.entries.some(
                (entry) =>
                  entry.method === 'GET' && entry.requestVersion === pinned && entry.status === 404,
              ),
            );
            assert.equal(
              (
                await fixture.db.pool.query(
                  'SELECT count(*) FROM moria.runtime_dataset_graph_roots',
                )
              ).rows[0].count,
              '0',
            );
          }
          assert(
            !trace.entries.some(
              (entry) => entry.method === 'GET' && entry.requestVersion === current,
            ),
          );
          assert.deepEqual(
            (await fixture.db.catalog.heads(fixture.environment, [fixture.dataset]))[0],
            first.head,
          );
        } finally {
          t.diagnostic(
            JSON.stringify({
              objectClass: kind === 'root' ? 'nativeWrapperRoot' : 'nativeWrapperPage',
              pinnedVersion: pinned,
              currentVersion: current,
              pinnedPresent: present,
              requests: trace.entries,
            }),
          );
        }
      },
    );
  }
}
