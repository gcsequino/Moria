import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { GetObjectCommand, PutObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';
import type { managedRetentionDataset } from './dataset.js';
import { createServer, request } from 'node:http';
import type { Socket } from 'node:net';
import type { startS3 } from '../storage-s3/server.js';

type Provider = Awaited<ReturnType<typeof startS3>>;
export function versionTrace() {
  let selected = '';
  const entries: {
    method: string;
    requestVersion: string | null;
    status?: number;
    responseVersion?: string;
  }[] = [];
  return {
    entries,
    select(path: string) {
      assert(path.startsWith('/'));
      selected = path;
      entries.length = 0;
    },
    wrap: async (source: Provider): Promise<Provider> => {
      const target = new URL(source.endpoint);
      const sockets = new Set<Socket>();
      const proxy = createServer((incoming, outgoing) => {
        const url = new URL(incoming.url!, source.endpoint);
        const entry =
          url.pathname === selected
            ? { method: incoming.method!, requestVersion: url.searchParams.get('versionId') }
            : undefined;
        if (entry) {
          assert(entries.length < 128);
          entries.push(entry);
        }
        const upstream = request(
          {
            hostname: target.hostname,
            port: target.port,
            path: incoming.url,
            method: incoming.method,
            headers: incoming.headers,
          },
          (response) => {
            if (entry) {
              Object.assign(entry, {
                status: response.statusCode,
                responseVersion: response.headers['x-amz-version-id'],
              });
            }
            outgoing.writeHead(response.statusCode!, response.headers);
            response.pipe(outgoing);
          },
        );
        upstream.on('error', () => {
          outgoing.writeHead(502);
          outgoing.end();
        });
        incoming.on('error', () => upstream.destroy());
        incoming.pipe(upstream);
      });
      proxy.on('connection', (socket) => {
        sockets.add(socket);
        socket.on('close', () => sockets.delete(socket));
      });
      await new Promise<void>((resolve) => proxy.listen(0, '127.0.0.1', resolve));
      return {
        ...source,
        endpoint: `http://127.0.0.1:${(proxy.address() as { port: number }).port}`,
        close: async () => {
          for (const socket of sockets) socket.destroy();
          await new Promise<void>((resolve) => proxy.close(() => resolve()));
          await source.close();
        },
      };
    },
  };
}

type Fixture = Awaited<ReturnType<typeof managedRetentionDataset>>;

export async function currentWrapperCopy(
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
