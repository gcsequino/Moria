import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { spawn, type ChildProcess } from 'node:child_process';
import test from 'node:test';
import pg from 'pg';
import { setTimeout as delay } from 'node:timers/promises';
import {
  PutObjectCommand,
  HeadObjectCommand,
  PutBucketVersioningCommand,
} from '@aws-sdk/client-s3';
import { database } from './fixtures/catalog-storage/database.js';
import { startS3 } from './fixtures/storage-s3/server.js';
import { heldHeadProxy } from './fixtures/native-retention/held-head.js';
import { assertNativeArtifacts } from './fixtures/native-common/artifact.js';
import { fingerprint } from '../packages/compiler/src/index.js';

test(
  'explicit retention controls exact-issued versioned backup reclamation and real claim fences',
  { timeout: 180000 },
  async (t) => {
    const runtime = process.env.MORIA_RUNTIME_BINARY ?? '/usr/local/bin/moria-runtime';
    const harness =
      process.env.MORIA_NATIVE_RETENTION_TEST_BINARY ??
      '/usr/local/bin/moria-native-retention-test';
    const metadata = await assertNativeArtifacts(runtime, [harness]);
    const db = await database(),
      s3 = await startS3(),
      proxy = await heldHeadProxy(s3.endpoint);
    const children = new Set<ChildProcess>();
    t.after(async () => {
      const live = [...children];
      for (const child of live) child.kill('SIGKILL');
      proxy.release();
      await Promise.all(
        live.map((child) =>
          child.exitCode !== null || child.signalCode !== null
            ? Promise.resolve()
            : new Promise<void>((resolve) => child.once('exit', () => resolve())),
        ),
      );
      await proxy.close();
      await s3.close();
      await db.close();
    });
    await s3.client.send(
      new PutBucketVersioningCommand({
        Bucket: s3.bucket,
        VersioningConfiguration: { Status: 'Enabled' },
      }),
    );
    const namespace = { warehouseId: 'gcwarehouse', tableId: 'backups', revision: 'v1' };
    const physical = fingerprint({
      backend: 's3',
      endpoint: new URL(proxy.endpoint).href,
      bucket: s3.bucket,
      region: 'us-east-1',
      prefix: 'retention',
    });
    const storage = JSON.stringify([
      {
        namespace,
        provider: {
          kind: 's3',
          binding: {
            warehouseId: namespace.warehouseId,
            tableId: namespace.tableId,
            endpoint: proxy.endpoint,
            bucket: s3.bucket,
            prefix: 'retention',
            region: 'us-east-1',
            accessKeySecretRef: 'N18_ACCESS_KEY',
            secretKeySecretRef: 'N18_SECRET_KEY',
            limits: { maxObjectBytes: 128 * 1024 * 1024, chunkBytes: 1024 * 1024 },
          },
        },
      },
    ]);
    const sourceConnection = `postgresql://moria_local:${db.pool.options.password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const role = `n18_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role};GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role};GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    const producerConnection = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const env = {
      PATH: process.env.PATH,
      MORIA_INDEXING_DATABASE_SECRET_REF: 'N18_DATABASE',
      MORIA_INDEXING_DATABASE_TLS: 'disable',
      MORIA_NATIVE_STORAGE_BINDINGS: storage,
      N18_ACCESS_KEY: s3.credentials.accessKeyId,
      N18_SECRET_KEY: s3.credentials.secretAccessKey,
    };
    const start = (
      binary: string,
      args: string[],
      input: unknown,
      connection = sourceConnection,
      extra: Record<string, string> = {},
    ) => {
      const child = spawn(binary, args, {
        env: { ...env, ...extra, N18_DATABASE: connection },
        stdio: ['pipe', 'pipe', 'pipe'],
      });
      children.add(child);
      child.stdin!.end(JSON.stringify(input));
      let output = '',
        errors = '';
      child.stdout!.on('data', (b) => {
        output += b.toString();
      });
      child.stderr!.on('data', (b) => {
        errors += b.toString();
      });
      const done = new Promise<string>((resolve, reject) => {
        child.once('error', reject);
        child.once('exit', (code, signal) => {
          children.delete(child);
          code === 0
            ? resolve(output)
            : reject(new Error(`Native command failed ${code ?? signal}: ${errors}\n${output}`));
        });
      });
      void done.catch(() => {});
      return { child, done };
    };
    const run = async (args: string[], input: unknown) =>
      JSON.parse((await start(runtime, args, input).done).trim());
    const issue = async (lease: unknown, mode = 'issue') => {
      if (mode === 'issue') {
        // The actual lease controller chooses quotas; ordinary backup workers only consume that envelope.
        await db.pool.query(
          'INSERT INTO moria.runtime_recovery_content_counters(recovery_set,max_roots,max_members,max_bytes,max_dump_bytes,max_state_bytes) VALUES($1,10,10,16777216,16777216,4194304) ON CONFLICT DO NOTHING',
          [(lease as { set: string }).set],
        );
      }

      const output = await start(
        harness,
        ['--ignored', '--exact', 'issued_backup_fixture', '--nocapture', '--test-threads=1'],
        {},
        producerConnection,
        { MORIA_NATIVE_RETENTION_FIXTURE: JSON.stringify({ lease, namespace, mode }) },
      ).done;
      assert.match(output, /test result: ok\. 1 passed; 0 failed; 0 ignored;/);
      const record = output.split('\n').find((line) => line.includes('"event":"verified"'));
      assert.ok(record, `Missing fixture result: ${output}`);
      return JSON.parse(record.slice(record.indexOf('{')));
    };
    const policy = async (key: string) =>
      run(['retention', 'approve'], {
        requestKey: key,
        namespace,
        minimumAgeSeconds: 86400,
        recheckSeconds: 30,
      });
    const keyFor = (object: string) =>
      `retention/${namespace.warehouseId}/${namespace.tableId}/objects/${object}`;
    const head = async (object: string) =>
      s3.client.send(new HeadObjectCommand({ Bucket: s3.bucket, Key: keyFor(object) }));
    const mature = async (qualified = true) => {
      // Historical closed DATA fixture; no live producer lease is fabricated or accepted.
      // Actual native capture/retention deadlines are exercised independently below.
      const set = randomUUID(),
        owner = randomUUID();
      await db.pool.query(
        "INSERT INTO moria.runtime_recovery_sets(id,request_key,owner,fence,incarnation,lease_until,status,schema_checksum,key_manifest_hash,created_at,retained_until) SELECT $1,$2,$3,1,incarnation,clock_timestamp()-interval '2 days','failed',$4,$5,clock_timestamp()-interval '2 days',clock_timestamp()-interval '1 day' FROM moria.indexing_instance",
        [set, `historical-${set}`, owner, metadata.schema.checksum, 'a'.repeat(64)],
      );
      await db.pool.query(
        'INSERT INTO moria.runtime_recovery_backends(recovery_set,namespace,physical_fingerprint) VALUES($1,$2,$3)',
        [set, namespace, physical],
      );
      const object = `recovery_${set.replaceAll('-', '')}_metadata_part_0`,
        body = Buffer.from(`historical closed bytes ${set}`);
      const put = await s3.client.send(
        new PutObjectCommand({ Bucket: s3.bucket, Key: keyFor(object), Body: body }),
      );
      if (qualified) assert.ok(put.VersionId && put.VersionId !== 'null');
      else assert.ok(put.VersionId === undefined || put.VersionId === 'null');
      await db.pool.query(
        "INSERT INTO moria.runtime_recovery_outputs(recovery_set,warehouse_id,table_id,object_id,kind) VALUES($1,$2,$3,$4,'metadata')",
        [set, namespace.warehouseId, namespace.tableId, object],
      );
      return {
        set,
        object,
        content: {
          object: {
            version: 1,
            warehouseId: namespace.warehouseId,
            tableId: namespace.tableId,
            objectId: object,
          },
          size: body.length,
          sha256: `sha256:${createHash('sha256').update(body).digest('hex')}`,
        },
      };
    };
    const approveRetirePlan = async (item: Awaited<ReturnType<typeof mature>>, key: string) => {
      const approved = await policy(key);
      await run(['retention', 'retire'], { policy: approved.policy, set: item.set });
      await run(['retention', 'plan'], {
        policy: approved.policy,
        set: item.set,
        after: null,
        limit: 16,
      });
      return approved;
    };
    await t.test(
      'fresh actual issued writes remain retained, and their namespace cannot be exposed as Dataset bytes',
      async () => {
        const lease = await run(['recovery', 'create'], {
          requestKey: `fresh-${randomUUID()}`,
          owner: randomUUID(),
          keyManifestHash: 'b'.repeat(64),
          retentionSeconds: 86400,
        });
        const raw = new pg.Client({ connectionString: producerConnection });
        await raw.connect();
        try {
          await assert.rejects(
            raw.query(
              'INSERT INTO moria.runtime_recovery_content_counters(recovery_set,max_roots,max_members,max_bytes,max_dump_bytes,max_state_bytes) VALUES($1,10,10,16777216,16777216,4194304)',
              [lease.set],
            ),
            /RECOVERY_OPERATOR_REQUIRED/,
          );
          await assert.rejects(
            raw.query(
              'INSERT INTO moria.runtime_recovery_backends(recovery_set,namespace,physical_fingerprint) VALUES($1,$2,$3)',
              [lease.set, namespace, physical],
            ),
            /RECOVERY_OPERATOR_REQUIRED/,
          );
          await assert.rejects(
            raw.query(
              "INSERT INTO moria.runtime_recovery_outputs(recovery_set,warehouse_id,table_id,object_id,kind) VALUES($1,$2,$3,$4,'metadata')",
              [lease.set, namespace.warehouseId, namespace.tableId, 'forged-output'],
            ),
            /RECOVERY_OPERATOR_REQUIRED/,
          );
        } finally {
          await raw.end();
        }
        const issued = await issue(lease),
          approved = await policy(`fresh-policy-${randomUUID()}`);
        assert.ok(issued.version.token.startsWith('s3v1/'));
        await db.pool.query(
          "UPDATE moria.runtime_recovery_sets SET status='failed',error='fixture' WHERE id=$1",
          [lease.set],
        );
        await assert.rejects(
          run(['retention', 'retire'], { policy: approved.policy, set: lease.set }),
          /GC_RETENTION_PINNED/,
        );
        await issue(lease, 'denied');
        const environment = randomUUID();
        await db.catalog.ensureEnvironment(environment);
        await db.catalog.ensureDataset(environment, 'blocked');
        await assert.rejects(
          db.pool.query(
            "INSERT INTO moria.native_storage_namespaces(environment,warehouse_id,table_id,resource,revision,purpose) VALUES($1,$2,$3,'blocked','v1','dataset-source')",
            [environment, namespace.warehouseId, namespace.tableId],
          ),
          /RECOVERY_NAMESPACE_RESERVED/,
        );
        assert.ok((await head(issued.content.object.objectId)).VersionId);
      },
    );
    await t.test(
      'no policy means no collection, and explicit historical retirement removes only its qualified version',
      async () => {
        const item = await mature();
        assert.equal(
          (
            await run(['retention', 'once'], {
              policy: randomUUID(),
              owner: randomUUID(),
              timeSeconds: 30,
            })
          ).claimed,
          false,
        );
        assert.ok((await head(item.object)).VersionId);
        const approved = await approveRetirePlan(item, `mature-${randomUUID()}`);
        const result = await run(['retention', 'once'], {
          policy: approved.policy,
          owner: randomUUID(),
          timeSeconds: 30,
        });
        assert.equal(result.removedVersion, true);
        assert.equal(result.reconciliationRequired, true);
        await assert.rejects(
          head(item.object),
          (e) =>
            (e as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode === 404,
        );
      },
    );
    await t.test(
      'a retained foreign root blocks planning even for an expired retired producer',
      async () => {
        const item = await mature(),
          other = await run(['recovery', 'create'], {
            requestKey: `pin-${randomUUID()}`,
            owner: randomUUID(),
            keyManifestHash: 'c'.repeat(64),
            retentionSeconds: 86400,
          });
        // A conservative retained opaque reference is not treated as a successfully captured archive.
        const object = {
          version: 1,
          warehouseId: namespace.warehouseId,
          tableId: namespace.tableId,
          objectId: item.object,
        };
        await db.pool.query('UPDATE moria.runtime_recovery_sets SET metadata_dump=$2 WHERE id=$1', [
          other.set,
          { ...item.content, object },
        ]);
        const approved = await policy(`pinned-${randomUUID()}`);
        await run(['retention', 'retire'], { policy: approved.policy, set: item.set });
        await assert.rejects(
          run(['retention', 'plan'], {
            policy: approved.policy,
            set: item.set,
            after: null,
            limit: 16,
          }),
          /GC_REFERENCE_PRESENT/,
        );
        assert.ok((await head(item.object)).VersionId);
      },
    );
    await t.test('policy revocation after a held HEAD prevents the removal effect', async () => {
      const item = await mature(),
        approved = await approveRetirePlan(item, `revoke-${randomUUID()}`);
      proxy.arm(`/${s3.bucket}/${keyFor(item.object)}`);
      const before = proxy.deletes.length;
      const pending = start(runtime, ['retention', 'once'], {
        policy: approved.policy,
        owner: randomUUID(),
        timeSeconds: 30,
      });
      await proxy.wait();
      await run(['retention', 'disable'], { policy: approved.policy });
      proxy.release();
      await assert.rejects(pending.done, /GC_POLICY_CHANGED/);
      assert.equal(proxy.deletes.length, before);
      assert.ok((await head(item.object)).VersionId);
    });
    await t.test(
      'real expired claim ownership is fenced before a replacement removes the version',
      async () => {
        const item = await mature(),
          approved = await approveRetirePlan(item, `takeover-${randomUUID()}`),
          owner = randomUUID();
        proxy.arm(`/${s3.bucket}/${keyFor(item.object)}`);
        const pending = start(runtime, ['retention', 'once'], {
          policy: approved.policy,
          owner,
          timeSeconds: 60,
        });
        await proxy.wait();
        const old = (
          await db.pool.query(
            'SELECT id,fence::text,incarnation FROM moria.runtime_gc_candidates WHERE policy=$1',
            [approved.policy],
          )
        ).rows[0];
        pending.child.kill('SIGKILL');
        await assert.rejects(pending.done);
        proxy.release();
        const until = performance.now() + 40000;
        while (performance.now() < until) {
          if (
            (
              await db.pool.query(
                'SELECT lease_until<=clock_timestamp() AS expired FROM moria.runtime_gc_candidates WHERE id=$1',
                [old.id],
              )
            ).rows[0].expired
          )
            break;
          await delay(100);
        }
        assert.equal(
          (
            await db.pool.query(
              'SELECT lease_until<=clock_timestamp() AS expired FROM moria.runtime_gc_candidates WHERE id=$1',
              [old.id],
            )
          ).rows[0].expired,
          true,
        );
        proxy.arm(`/${s3.bucket}/${keyFor(item.object)}`);
        const replacing = start(runtime, ['retention', 'once'], {
          policy: approved.policy,
          owner: randomUUID(),
          timeSeconds: 30,
        });
        await proxy.wait();
        assert.ok(
          BigInt(
            (
              await db.pool.query(
                'SELECT fence::text FROM moria.runtime_gc_candidates WHERE id=$1',
                [old.id],
              )
            ).rows[0].fence,
          ) > BigInt(old.fence),
        );
        await assert.rejects(
          db.pool.query('SELECT moria.runtime_gc_claim_check($1,$2,$3,$4)', [
            old.id,
            owner,
            old.fence,
            old.incarnation,
          ]),
          /GC_LEASE_LOST/,
        );
        proxy.release();
        const replacement = JSON.parse((await replacing.done).trim());
        assert.equal(replacement.removedVersion, true);
      },
    );
    await t.test(
      'unqualified null-version objects stay unsupported and are never deleted',
      async () => {
        await s3.client.send(
          new PutBucketVersioningCommand({
            Bucket: s3.bucket,
            VersioningConfiguration: { Status: 'Suspended' },
          }),
        );
        try {
          const item = await mature(false),
            approved = await approveRetirePlan(item, `unsupported-${randomUUID()}`);
          const before = proxy.deletes.length;
          const result = await run(['retention', 'once'], {
            policy: approved.policy,
            owner: randomUUID(),
            timeSeconds: 30,
          });
          assert.equal(result.result, 'unsupported');
          assert.equal(result.removedVersion, false);
          assert.equal(proxy.deletes.length, before);
          const survivor = await head(item.object);
          assert.ok(survivor.VersionId === undefined || survivor.VersionId === 'null');
          assert.equal(survivor.ContentLength, item.content.size);
        } finally {
          await s3.client.send(
            new PutBucketVersioningCommand({
              Bucket: s3.bucket,
              VersioningConfiguration: { Status: 'Enabled' },
            }),
          );
        }
      },
    );
  },
);
