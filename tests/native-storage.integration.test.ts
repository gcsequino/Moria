import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { execFile, spawn, type ChildProcess } from 'node:child_process';
import { chmod, mkdir, readFile, stat, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import {
  PutObjectCommand,
  HeadObjectCommand,
  GetObjectCommand,
  CreateBucketCommand,
  PutBucketVersioningCommand,
} from '@aws-sdk/client-s3';
import { completeProxy } from './fixtures/native-storage/complete-proxy.js';
import { database } from './fixtures/catalog-storage/database.js';
import { startS3 } from './fixtures/storage-s3/server.js';
import { iceberg } from './iceberg-helper.js';
import { icebergSchema } from '../packages/contracts/iceberg.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { IcebergIndexer } from '../services/ontology/src/indexer.js';
import { ObjectStore, type ObjectSchema } from '../services/ontology/src/store.js';

test(
  'native storage uses real scoped authority, immutable POSIX writes, MinIO ranges and durable paged closure state',
  { timeout: 300000 },
  async (t) => {
    const db = await database();
    let s3: Awaited<ReturnType<typeof startS3>> | undefined;
    const children = new Set<ChildProcess>();
    t.after(async () => {
      for (const child of children) child.kill('SIGKILL');
      await Promise.all(
        [...children].map((child) =>
          child.pid === undefined || child.exitCode !== null || child.signalCode !== null
            ? Promise.resolve()
            : new Promise<void>((resolve) => child.once('exit', () => resolve())),
        ),
      );
      await s3?.close();
      await db.close();
    });
    s3 = await startS3();
    const environment = randomUUID(),
      targetId = randomUUID(),
      owner = randomUUID();
    const warehouse = join(db.root, 'source'),
      root = join(db.root, 'native-objects');
    await mkdir(root);
    await db.catalog.ensureEnvironment(environment);
    await db.catalog.ensureDataset(environment, 'source');
    const authority = new Authority(db.catalog);
    const operations = ['source:read', 'objects:index'];
    await authority.syncPrincipal(environment, 'fixture', 'pipeline', {
      enabled: true,
      kind: 'workload',
      attributes: {},
      markings: ['private'],
    });
    await authority.setApplication(environment, 'fixture', 'indexing', operations);
    await authority.setGrants(environment, 'fixture', 'pipeline', 'indexing', operations);
    const schema: ObjectSchema = {
      primaryKey: 'uid',
      properties: {
        uid: { type: 'uid', nullable: false },
        name: { type: 'string', nullable: false },
      },
    };
    const context = { environment, principal: 'pipeline', application: 'indexing' };
    const credential = await new WorkloadSessions(authority).mint(context, 'fixture');
    const session = {
      ...context,
      sessionHash: createHash('sha256').update(credential.token).digest('hex'),
    };
    for (const [warehouseId, tableId, resource, purpose] of [
      ['sourcewarehouse', 'sourcetable', 'source', 'dataset-source'],
      ['artifactwarehouse', 'artifacttable', 'objects', 'index-artifact'],
    ])
      await db.pool.query(
        "INSERT INTO moria.native_storage_namespaces(environment,warehouse_id,table_id,resource,revision,purpose,markings) VALUES($1,$2,$3,$4,$5,$6,ARRAY['private'])",
        [environment, warehouseId, tableId, resource, 'v1', purpose],
      );
    await new IcebergIndexer(new ObjectStore(db.catalog), authority, {
      source: { kind: 'dataset', resource: 'source', markings: ['private'] },
      target: { resource: 'objects', markings: ['private'], schema },
    }).registerDistributed(session, { targetId, warehouse });
    const snapshot = await iceberg({
      operation: 'stage',
      warehouse,
      schema: icebergSchema(schema.properties),
      rows: [{ uid: await db.catalog.resolveIdentity(environment, 'one'), name: 'one' }],
    });
    const execution = randomUUID(),
      worker = 'publisher';
    const fence = await db.catalog.claim(environment, execution, worker, 300);
    await db.catalog.publish({
      environment,
      execution,
      worker,
      fence,
      securityEpoch: (await authority.current(environment, 'pipeline', 'indexing')).epoch,
      outputs: [
        {
          dataset: 'source',
          expectedRevision: '0',
          metadataLocation: snapshot.metadataLocation,
          snapshotId: snapshot.snapshotId,
        },
      ],
      lineage: {},
    });
    const build = (
      await db.pool.query('SELECT id FROM moria.indexing_builds WHERE environment=$1', [
        environment,
      ])
    ).rows[0].id;
    await db.pool.query(
      "UPDATE moria.indexing_builds SET owner=$2,fence=1,lease_until=clock_timestamp()+interval '10 minutes' WHERE id=$1",
      [build, owner],
    );
    await db.pool.query(
      "INSERT INTO moria.native_storage_namespaces(environment,warehouse_id,table_id,resource,revision,purpose) VALUES($1,'underclassified','artifacttable','objects','v1','index-artifact')",
      [environment],
    );
    const role = `storage_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role}; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    const connection = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const payload = Buffer.alloc(3 * 1024 * 1024 + 137);
    for (let i = 0; i < payload.length; i++) payload[i] = (i * 37) % 251;
    const key = 'native/sourcewarehouse/sourcetable/objects/payload';
    await s3.client.send(new PutObjectCommand({ Bucket: s3.bucket, Key: key, Body: payload }));
    const snapshotRoot = join(db.root, 'native-snapshot');
    await mkdir(snapshotRoot);
    const generated = await promisify(execFile)(resolve('.venv/bin/python'), [
      'tests/fixtures/iceberg-scoped-storage/generate.py',
      snapshotRoot,
      '--snapshot',
      'moria://artifactwarehouse/artifacttable',
    ]);
    const nativeSnapshot = JSON.parse(generated.stdout);
    const retiredRoot = join(db.root, 'native-retired-snapshot');
    await mkdir(retiredRoot);
    const nativeRetiredSnapshot = JSON.parse(
      (
        await promisify(execFile)(resolve('.venv/bin/python'), [
          'tests/fixtures/iceberg-scoped-storage/generate.py',
          retiredRoot,
          '--deleted-snapshot',
          'moria://artifactwarehouse/artifacttable',
        ])
      ).stdout,
    );
    const base = {
      root,
      build,
      owner,
      fence: 1,
      revision: 'v1',
      endpoint: s3.endpoint,
      bucket: s3.bucket,
    };
    const binary = process.env.MORIA_STORAGE_TEST_BINARY ?? '/usr/local/bin/moria-storage-tests';
    const start = (mode: string, changes: Record<string, unknown> = {}) => {
      const child = spawn(
        binary,
        ['--ignored', '--exact', 'provider_and_authority', '--nocapture', '--test-threads=1'],
        {
          env: {
            PATH: process.env.PATH,
            MORIA_INDEXING_DATABASE_SECRET_REF: 'NATIVE_STORAGE_DATABASE',
            NATIVE_STORAGE_DATABASE: connection,
            MORIA_INDEXING_DATABASE_TLS: 'disable',
            NATIVE_STORAGE_ACCESS_KEY: s3!.credentials.accessKeyId,
            NATIVE_STORAGE_SECRET_KEY: s3!.credentials.secretAccessKey,
            AWS_ACCESS_KEY_ID: 'wrong-ambient-key',
            AWS_SECRET_ACCESS_KEY: 'wrong-ambient-secret',
            AWS_ENDPOINT_URL: 'http://127.0.0.1:1',
            MORIA_NATIVE_STORAGE_FIXTURE: JSON.stringify({ ...base, mode, ...changes }),
          },
          stdio: ['pipe', 'pipe', 'pipe'],
        },
      );
      children.add(child);
      let output = '',
        errors = '';
      child.stdout.on('data', (bytes) => {
        output += bytes.toString();
      });
      child.stderr.on('data', (bytes) => {
        errors += bytes.toString();
      });
      const done = new Promise<string>((resolve, reject) => {
        child.once('error', (error) => {
          children.delete(child);
          reject(error);
        });
        child.once('exit', (code, signal) => {
          children.delete(child);
          code === 0
            ? resolve(output)
            : reject(
                new Error(`Native storage ${mode} exited ${code ?? signal}: ${errors}\n${output}`),
              );
        });
      });
      void done.catch(() => {});
      return {
        child,
        done,
        async event(name: string) {
          for (let i = 0; i < 1500; i++) {
            const line = output.split('\n').find((line) => line.includes(`"event":"${name}"`));
            if (line) return JSON.parse(line.slice(line.indexOf('{')));
            if (child.exitCode !== null || child.signalCode !== null)
              throw new Error(`Native storage exited before ${name}: ${errors}\n${output}`);
            await delay(10);
          }
          throw new Error(`Native storage did not reach ${name}`);
        },
      };
    };
    const run = async (mode: string, changes: Record<string, unknown> = {}) => {
      const output = await start(mode, changes).done;
      assert.match(output, /test result: ok\. 1 passed; 0 failed; 0 ignored;/);
      assert.match(output, /"event":"verified"/);
      return output;
    };
    await t.test(
      'real POSIX and S3 paths preserve bounds, integrity, namespace grants and immutable sealing',
      async (t) => {
        await run('exercise');
        assert.equal(
          await readFile(join(root, 'artifactwarehouse/artifacttable/objects/small'), 'utf8'),
          'abc',
        );
        await t.test(
          'native Iceberg semantic closure checks parse actual retained metadata, Avro and Parquet',
          async (t) => {
            for (const variant of [
              'valid',
              'wrong-table',
              'wrong-snapshot',
              'missing',
              'extra',
              'malformed-manifest',
              'malformed-parquet',
              'nullable-array',
              'historical-schema',
              'cycle',
              'duplicate-schema',
              'current-projection',
              'missing-sort-orders',
              'foreign-reference',
            ]) {
              await t.test(variant, async () => {
                await run(`snapshot-${variant}`, { snapshot: nativeSnapshot });
              });
            }
            for (const variant of ['deleted-valid', 'deleted-corrupt'])
              await t.test(variant, async () => {
                await run(`snapshot-${variant}`, { snapshot: nativeRetiredSnapshot });
              });
          },
        );
        await t.test(
          'valid same-size Parquet mutation between full closure hashing and semantic proof is denied on POSIX and MinIO',
          async (t) => {
            for (const provider of ['posix', 's3'])
              await t.test(provider, async () => {
                const attempt = start('snapshot-version-hold', {
                  snapshot: nativeSnapshot,
                  snapshotProvider: provider,
                });
                const sealed = (await attempt.event('snapshot-hashed')).content;
                assert.equal(sealed.object.warehouseId, 'artifactwarehouse');
                assert.equal(sealed.object.tableId, 'artifacttable');
                assert.match(sealed.object.objectId, /^[a-f0-9]{64}$/);
                const path = join(
                  root,
                  'artifactwarehouse',
                  'artifacttable',
                  'objects',
                  sealed.object.objectId,
                );
                const key = `native/artifactwarehouse/artifacttable/objects/${sealed.object.objectId}`;
                const bytes =
                  provider === 'posix'
                    ? await readFile(path)
                    : Buffer.from(
                        await (
                          await s3!.client.send(
                            new GetObjectCommand({ Bucket: s3!.bucket, Key: key }),
                          )
                        ).Body!.transformToByteArray(),
                      );
                const changed = Buffer.from(bytes),
                  marker = Buffer.from('parquet-cpp-arrow version '),
                  offset = changed.indexOf(marker) + marker.length;
                assert.ok(offset >= marker.length, 'real Parquet writer marker');
                changed[offset] = changed[offset] === 0x32 ? 0x33 : 0x32;
                assert.equal(changed.length, bytes.length);
                assert.notDeepEqual(changed, bytes);
                const originalPath = join(root, `original-${provider}.parquet`),
                  changedPath = join(root, `changed-${provider}.parquet`);
                await writeFile(originalPath, bytes);
                await writeFile(changedPath, changed);
                await promisify(execFile)(resolve('.venv/bin/python'), [
                  '-c',
                  'import pyarrow.parquet as p,sys; assert p.read_table(sys.argv[1]).equals(p.read_table(sys.argv[2]))',
                  originalPath,
                  changedPath,
                ]);
                if (provider === 'posix') {
                  // Model a provider administrator corrupting a sealed immutable object.
                  await chmod(path, 0o600);
                  await writeFile(path, changed);
                } else
                  await s3!.client.send(
                    new PutObjectCommand({ Bucket: s3!.bucket, Key: key, Body: changed }),
                  );
                attempt.child.stdin!.write('\n');
                const output = await attempt.done;
                assert.match(output, /"event":"version-denied"/);
                assert.match(output, /test result: ok\. 1 passed; 0 failed; 0 ignored;/);
              });
          },
        );
        assert.equal(
          (await stat(join(root, 'artifactwarehouse/artifacttable/objects/large'))).size,
          70 * 1024 * 1024,
        );
      },
    );
    await t.test(
      'qualified versioned MinIO reads and removal preserve a newer object version',
      async () => {
        const bucket = `moria-versioned-${randomUUID()}`;
        await s3!.client.send(new CreateBucketCommand({ Bucket: bucket }));
        await s3!.client.send(
          new PutBucketVersioningCommand({
            Bucket: bucket,
            VersioningConfiguration: { Status: 'Enabled' },
          }),
        );
        const attempt = start('versioned-delete', { bucket });
        const { content, version } = await attempt.event('version-ready');
        assert.match(version.token, /^s3v1\//);
        const key = `native/artifactwarehouse/artifacttable/objects/${content.object.objectId}`;
        const replacement = await s3!.client.send(
          new PutObjectCommand({ Bucket: bucket, Key: key, Body: 'newer retained payload' }),
        );
        assert.ok(replacement.VersionId && replacement.VersionId !== 'null');
        attempt.child.stdin!.write('\n');
        assert.match(await attempt.done, /test result: ok\. 1 passed; 0 failed; 0 ignored;/);
        assert.equal(
          (await s3!.client.send(new HeadObjectCommand({ Bucket: bucket, Key: key }))).VersionId,
          replacement.VersionId,
        );
      },
    );
    let spoolId: string;
    await t.test(
      'SIGKILL after a durable page commit reopens exact bounded closure state',
      async () => {
        const attempt = start('crash');
        spoolId = (await attempt.event('spool-committed')).id;
        attempt.child.kill('SIGKILL');
        await assert.rejects(attempt.done);
        await run('resume', { spoolId });
      },
    );
    await t.test(
      'a provider version change between chunks denies subsequent range release',
      async () => {
        const attempt = start('provider-change');
        await attempt.event('provider-read-started');
        payload[0] ^= 1;
        await s3!.client.send(
          new PutObjectCommand({ Bucket: s3!.bucket, Key: key, Body: payload }),
        );
        attempt.child.stdin!.write('\n');
        assert.match(await attempt.done, /test result: ok\. 1 passed; 0 failed; 0 ignored;/);
      },
    );
    await t.test(
      'an actually completed multipart upload with a lost acknowledgement recovers only after full integrity and current authority checks',
      async () => {
        const proxy = await completeProxy(s3!.endpoint);
        t.after(proxy.close);
        const pending = start('s3-uncertain', { endpoint: proxy.endpoint });
        await pending.event('s3-before-complete');
        pending.child.stdin!.write('\n');
        await Promise.race([
          proxy.committed,
          pending.done.then(() => {
            throw new Error('S3 acknowledgement was not intercepted');
          }),
        ]);
        pending.child.kill('SIGKILL');
        await assert.rejects(pending.done);
        await run('s3-recover');
      },
    );
    const replacement = randomUUID();
    await t.test(
      'expired owners cannot read or seal, and replacements cannot mutate prior-attempt spools',
      async () => {
        await db.pool.query(
          "UPDATE moria.indexing_builds SET owner=$2,fence=2,lease_until=clock_timestamp()+interval '10 minutes' WHERE id=$1",
          [build, replacement],
        );
        await run('denied');
        await run('takeover', { owner: replacement, fence: 2, spoolId });
      },
    );
    await t.test(
      'disabled namespace bindings deny both source reads and artifact writes',
      async () => {
        const reading = start('read-revoke', { owner: replacement, fence: 2 });
        await reading.event('posix-read-started');
        await db.pool.query(
          'UPDATE moria.native_storage_namespaces SET enabled=false,revision=$2 WHERE environment=$1',
          [environment, 'v2'],
        );
        reading.child.stdin!.write('\n');
        assert.match(await reading.done, /test result: ok\. 1 passed; 0 failed; 0 ignored;/);
        await run('denied', { owner: replacement, fence: 2, revision: 'v2' });
        await db.pool.query(
          'UPDATE moria.native_storage_namespaces SET enabled=true,revision=$2 WHERE environment=$1',
          [environment, 'v3'],
        );
        await run('denied', { owner: replacement, fence: 2, revision: 'v2' });
      },
    );
    await t.test(
      'current authority revocation denies native I/O without publishing any object generation',
      async () => {
        const changes = { owner: replacement, fence: 2, revision: 'v3' };
        const reading = start('read-revoke', changes),
          writing = start('write-revoke', changes),
          s3Writing = start('s3-write-revoke', changes),
          recovering = start('recover-revoke', changes),
          s3Recovering = start('recover-revoke-s3', changes);
        await Promise.all([
          reading.event('posix-read-started'),
          writing.event('posix-write-started'),
          s3Writing.event('s3-write-started'),
          recovering.event('recovery-checkpoint'),
          s3Recovering.event('recovery-checkpoint'),
        ]);
        await authority.setGrants(environment, 'fixture', 'pipeline', 'indexing', ['source:read']);
        reading.child.stdin!.write('\n');
        writing.child.stdin!.write('\n');
        s3Writing.child.stdin!.write('\n');
        recovering.child.stdin!.write('\n');
        s3Recovering.child.stdin!.write('\n');
        for (const result of await Promise.all([
          reading.done,
          writing.done,
          s3Writing.done,
          recovering.done,
          s3Recovering.done,
        ]))
          assert.match(result, /test result: ok\. 1 passed; 0 failed; 0 ignored;/);
        await assert.rejects(
          s3!.client.send(
            new HeadObjectCommand({
              Bucket: s3!.bucket,
              Key: 'native/artifactwarehouse/artifacttable/objects/s3-revoked-write',
            }),
          ),
        );
        await run('denied', { owner: replacement, fence: 2, revision: 'v3' });
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.source_generations WHERE environment=$1',
              [environment],
            )
          ).rows[0].count,
          '0',
        );
        assert.equal(
          (
            await db.pool.query(
              'SELECT revision::text FROM moria.dataset_heads WHERE environment=$1 AND dataset=$2',
              [environment, 'source'],
            )
          ).rows[0].revision,
          '1',
        );
      },
    );
  },
);
