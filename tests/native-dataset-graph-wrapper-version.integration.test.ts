import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { HeadObjectCommand, DeleteObjectCommand, S3ServiceException } from '@aws-sdk/client-s3';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';
import { bootstrapGraph } from './fixtures/native-retention/graph.js';
import { versionTrace, currentWrapperCopy } from './fixtures/native-retention/version-trace.js';

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
