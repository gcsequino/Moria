import assert from 'node:assert/strict';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { execFile, type ChildProcess } from 'node:child_process';
import { mkdir, readFile, readdir } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { database } from './fixtures/catalog-storage/database.js';
import { startS3 } from './fixtures/storage-s3/server.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { ObjectStore, type ObjectSchema } from '../services/ontology/src/store.js';
import { IcebergIndexer } from '../services/ontology/src/indexer.js';

test(
  'native indexing consumes complete V2 snapshots from trusted POSIX and MinIO bindings under read-only indexing grants',
  { timeout: 300000 },
  async (t) => {
    const db = await database();
    let s3: Awaited<ReturnType<typeof startS3>> | undefined;
    t.after(async () => {
      await s3?.close();
      await db.close();
    });
    const environment = randomUUID(),
      source = randomUUID(),
      target = randomUUID(),
      root = join(db.root, 'objects'),
      scratch = join(db.root, 'scratch'),
      physical = join(db.root, 'fixture');
    for (const path of [root, scratch, physical]) await mkdir(path);
    await db.catalog.ensureEnvironment(environment);
    await db.catalog.ensureDataset(environment, source);
    const authority = new Authority(db.catalog),
      sessions = new WorkloadSessions(authority),
      operations = [`${source}:read`, `${source}:write`, `${target}:index`, `${target}:read`];
    await authority.syncPrincipal(environment, 'fixture', 'pipeline', {
      enabled: true,
      kind: 'workload',
      attributes: {},
      markings: ['private'],
    });
    await authority.setApplication(environment, 'fixture', 'native-indexing', operations);
    await authority.setGrants(environment, 'fixture', 'pipeline', 'native-indexing', operations);
    const context = { environment, principal: 'pipeline', application: 'native-indexing' },
      credential = await sessions.mint(context, 'fixture'),
      session = {
        ...context,
        sessionHash: createHash('sha256').update(credential.token).digest('hex'),
      };
    const generated = JSON.parse(
      (
        await promisify(execFile)(resolve('.venv/bin/python'), [
          'tests/fixtures/iceberg-scoped-storage/generate.py',
          physical,
          '--index-snapshot',
          'moria://warehouse/table',
        ])
      ).stdout,
    );
    const metadata = 'moria://warehouse/table/metadata/root.metadata.json';
    const snapshot = {
      ...generated,
      metadata,
      objects: { ...generated.objects, [metadata]: generated.metadata },
      resource: source,
      warehouseId: 'warehouse',
      tableId: 'table',
    };
    await db.pool.query(
      "INSERT INTO moria.native_storage_namespaces(environment,warehouse_id,table_id,resource,revision,purpose,iceberg_uuid,markings) VALUES($1,'warehouse','table',$2,'v1','dataset-source',$3,ARRAY['private'])",
      [environment, source, generated.tableUuid],
    );
    const role = `snapshot_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role}; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    const connection = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const namespace = { warehouseId: 'warehouse', tableId: 'table', revision: 'v1' },
      limits = { maxObjectBytes: 128 * 1024 * 1024, chunkBytes: 1024 * 1024 };
    const posix = { namespace, provider: { kind: 'posix', root, limits } };
    let binding = randomUUID();
    const native = async (
      mode: string,
      bindings: unknown[] = [posix],
      observe?: (child: ChildProcess, chunk: string) => void,
    ) => {
      const invocation = promisify(execFile)(
        process.env.MORIA_CATALOG_TEST_BINARY ?? '/usr/local/bin/moria-catalog-tests',
        ['--ignored', '--exact', 'catalog_publication', '--nocapture', '--test-threads=1'],
        {
          timeout: 120000,
          maxBuffer: 1024 * 1024,
          env: {
            PATH: process.env.PATH,
            MORIA_INDEXING_DATABASE_SECRET_REF: 'SNAPSHOT_DATABASE',
            SNAPSHOT_DATABASE: connection,
            MORIA_INDEXING_DATABASE_TLS: 'disable',
            MORIA_NATIVE_CATALOG_FIXTURE: JSON.stringify({
              root,
              token: credential.token,
              binding,
              mode,
              snapshots: [snapshot],
            }),
            MORIA_NATIVE_STORAGE_BINDINGS: JSON.stringify(bindings),
            MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT: scratch,
            MORIA_NATIVE_CLOSURE_LIMITS: JSON.stringify({
              maxEntries: 10000,
              maxReferencedBytes: 1024 * 1024 * 1024,
              maxSpoolBytes: 32 * 1024 * 1024,
            }),
            SNAPSHOT_S3_ACCESS: s3?.credentials.accessKeyId,
            SNAPSHOT_S3_SECRET: s3?.credentials.secretAccessKey,
            AWS_ACCESS_KEY_ID: 'wrong-ambient-key',
            AWS_SECRET_ACCESS_KEY: 'wrong-ambient-secret',
            AWS_ENDPOINT_URL: 'http://127.0.0.1:1',
          },
        },
      );
      if (observe)
        invocation.child.stdout!.on('data', (bytes) => observe(invocation.child, bytes.toString()));
      const result = await invocation;
      assert.match(result.stdout, /test result: ok\. 1 passed; 0 failed; 0 ignored;/);
      return result.stdout;
    };
    const publish = async () => {
      const head = (await db.catalog.heads(environment, [source]))[0],
        epoch = (await authority.current(environment, session.principal, session.application))
          .epoch;
      const spec = {
        version: 1,
        domain: 'sync',
        release: 'a'.repeat(64),
        resourceClass: 'standard',
        timeoutSeconds: 240,
        retryPolicy: 'idempotent',
        inputSchema: { type: 'object', properties: {} },
        pinnedInputs: [],
        expectedHeads: { [source]: head.revision },
      };
      binding = (
        await db.pool.query(
          `INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation)
      SELECT $1,$2,$3,$4,$5,ARRAY['private'],$6,i.security_epoch,c.incarnation FROM moria.instance_state i,moria.indexing_instance c RETURNING id`,
          [environment, source, spec, session, [`${source}:read`, `${source}:write`], epoch],
        )
      ).rows[0].id;
      await native('valid');
      return (await db.catalog.heads(environment, [source]))[0];
    };
    await publish();
    const schema: ObjectSchema = {
      primaryKey: 'uid',
      properties: {
        uid: { type: 'uid', nullable: false },
        value: { type: 'string', nullable: true },
      },
    };
    const store = new ObjectStore(db.catalog),
      indexer = new IcebergIndexer(store, authority, {
        source: { kind: 'dataset', resource: source, markings: ['private'] },
        target: { resource: target, markings: ['private'], schema },
      });
    for (let i = 0; i < 300; i++)
      await db.pool.query(
        'INSERT INTO moria.identities(domain,canonical_key,uid) VALUES($1,$2,$3)',
        [environment, String(i), `${i.toString(16).padStart(8, '0')}-0000-4000-8000-000000000000`],
      );
    await indexer.registerDistributed(session, {
      targetId: target,
      warehouse: root,
      partitions: 4,
    });
    const generations = async () =>
      Number(
        (
          await db.pool.query(
            'SELECT count(*) FROM moria.source_generations WHERE environment=$1',
            [environment],
          )
        ).rows[0].count,
      );
    await t.test(
      'POSIX publication indexes 300 canonical objects using current namespace read authority',
      async () => {
        await Promise.all([native('index'), native('index')]);
        assert.equal(await generations(), 1);
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.source_generations g JOIN moria.staged_source_rows r ON r.stage=g.stage WHERE g.environment=$1 AND g.object_type=$2',
              [environment, target],
            )
          ).rows[0].count,
          '300',
        );
        assert.deepEqual(
          (
            await db.pool.query(
              "SELECT count(*)::text AS roots,sum(verification_count)::text AS verifies FROM moria.runtime_snapshot_verifications WHERE environment=$1 AND status='ready'",
              [environment],
            )
          ).rows,
          [{ roots: '1', verifies: '1' }],
        );
      },
    );
    s3 = await startS3();
    const s3Binding = {
      namespace,
      provider: {
        kind: 's3',
        binding: {
          warehouseId: 'warehouse',
          tableId: 'table',
          endpoint: s3.endpoint,
          bucket: s3.bucket,
          prefix: 'native',
          region: 'us-east-1',
          accessKeySecretRef: 'SNAPSHOT_S3_ACCESS',
          secretKeySecretRef: 'SNAPSHOT_S3_SECRET',
          sessionTokenSecretRef: null,
          limits,
        },
      },
    };
    const mirror = async () => {
      for (const object of await readdir(join(root, 'warehouse', 'table', 'objects')))
        await s3!.client.send(
          new PutObjectCommand({
            Bucket: s3!.bucket,
            Key: `native/warehouse/table/objects/${object}`,
            Body: await readFile(join(root, 'warehouse', 'table', 'objects', object)),
          }),
        );
    };
    await t.test(
      'MinIO snapshot closure and native Arrow indexing use only provisioned S3 credentials',
      async () => {
        await publish();
        await mirror();
        await Promise.all([native('index', [s3Binding]), native('index', [s3Binding])]);
        assert.equal(await generations(), 2);
        assert.deepEqual(
          (
            await db.pool.query(
              "SELECT count(*)::text AS roots,sum(verification_count)::text AS verifies FROM moria.runtime_snapshot_verifications WHERE environment=$1 AND status='ready'",
              [environment],
            )
          ).rows,
          [{ roots: '2', verifies: '2' }],
        );
      },
    );
    await t.test(
      'an expired verifier is replaced with a new fence and the stopped old process cannot publish its attempt',
      async () => {
        const before = await generations(),
          head = await publish();
        await mirror();
        let resolveClaim!: (value: { child: ChildProcess; claim: any }) => void;
        const claimed = new Promise<{ child: ChildProcess; claim: any }>(
          (resolve) => (resolveClaim = resolve),
        );
        let text = '',
          seen = false;
        const old = native('index-plan', [s3Binding], (child, chunk) => {
          text += chunk;
          const line = text
            .split('\n')
            .find((line) => line.includes('"event":"snapshot_verification_claimed"'));
          if (line && !seen) {
            seen = true;
            child.kill('SIGSTOP');
            resolveClaim({ child, claim: JSON.parse(line.slice(line.indexOf('{'))) });
          }
        });
        void old.catch(() => {});
        const paused = await Promise.race([
          claimed,
          old.then(() => {
            throw new Error('Verifier completed before its claim was observable');
          }),
        ]);
        t.after(() => paused.child.kill('SIGKILL'));
        assert.equal(
          (
            await db.pool.query(
              'SELECT status FROM moria.runtime_snapshot_verifications WHERE key=$1',
              [paused.claim.key],
            )
          ).rows[0].status,
          'verifying',
        );
        await db.pool.query(
          "UPDATE moria.runtime_snapshot_verifications SET lease_until=clock_timestamp()-interval '1 second' WHERE key=$1",
          [paused.claim.key],
        );
        await db.pool.query(
          "UPDATE moria.indexing_builds SET lease_until=clock_timestamp()-interval '1 second' WHERE metadata_location=$1 AND status='planning'",
          [head.metadataLocation],
        );
        await native('index', [s3Binding]);
        paused.child.kill('SIGCONT');
        await old.catch(() => undefined);
        const ready = (
          await db.pool.query(
            'SELECT status,fence::text,verification_count::text FROM moria.runtime_snapshot_verifications WHERE key=$1',
            [paused.claim.key],
          )
        ).rows[0];
        assert.deepEqual(ready, {
          status: 'ready',
          fence: (BigInt(paused.claim.fence) + 1n).toString(),
          verification_count: '1',
        });
        assert.equal(await generations(), before + 1);
        await assert.rejects(
          db.pool.query('SELECT moria.runtime_snapshot_verifier_check($1,$2,$3)', [
            paused.claim.key,
            paused.claim.owner,
            paused.claim.fence,
          ]),
          /VERIFIER_FENCE_LOST/,
        );
      },
    );
    const denied = async (bindings: unknown[]) => {
      const before = await generations();
      await publish();
      await mirror();
      await assert.rejects(native('index', bindings));
      assert.equal(await generations(), before);
    };
    await t.test(
      'stale and ambiguous provider bindings cannot activate an object generation',
      async () => {
        await denied([{ ...posix, namespace: { ...namespace, revision: 'v0' } }]);
        await denied([posix, posix]);
      },
    );
    await t.test(
      'a pinned physical object with changed bytes fails semantic verification before activation',
      async () => {
        const before = await generations(),
          head = await publish();
        await mirror();
        const ref = JSON.parse(head.metadataLocation!.slice('moria-snapshot-v2:'.length)),
          rootObject = JSON.parse(
            await readFile(
              join(root, 'warehouse', 'table', 'objects', ref.root.object.objectId),
              'utf8',
            ),
          );
        await s3!.client.send(
          new PutObjectCommand({
            Bucket: s3!.bucket,
            Key: `native/warehouse/table/objects/${rootObject.proof.metadata.object.objectId}`,
            Body: Buffer.from('corrupt'),
          }),
        );
        await assert.rejects(native('index', [s3Binding]));
        assert.equal(await generations(), before);
      },
    );
    await t.test(
      'cached verification is immutable and physical file versions are still checked by concurrent task reads',
      async () => {
        const before = await generations(),
          head = await publish();
        await mirror();
        await native('index-plan', [s3Binding]);
        const reference = JSON.parse(head.metadataLocation!.slice('moria-snapshot-v2:'.length));
        const attestation = (
          await db.pool.query(
            "SELECT key,fence,verification_count::text FROM moria.runtime_snapshot_verifications WHERE environment=$1 AND reference=$2::jsonb AND status='ready'",
            [environment, reference],
          )
        ).rows[0];
        assert.equal(attestation.verification_count, '1');
        await assert.rejects(
          db.pool.query('UPDATE moria.runtime_snapshot_verifications SET owner=$2 WHERE key=$1', [
            attestation.key,
            randomUUID(),
          ]),
          /IMMUTABLE_SNAPSHOT_ATTESTATION/,
        );
        const file = (
          await db.pool.query(
            "SELECT content FROM moria.runtime_snapshot_verified_entries WHERE verification=$1 AND fence=$2 AND name LIKE '%.parquet' ORDER BY name LIMIT 1",
            [attestation.key, attestation.fence],
          )
        ).rows[0].content;
        await s3!.client.send(
          new PutObjectCommand({
            Bucket: s3!.bucket,
            Key: `native/warehouse/table/objects/${file.object.objectId}`,
            Body: Buffer.alloc(file.size),
          }),
        );
        await assert.rejects(native('index', [s3Binding]));
        assert.equal(await generations(), before);
        assert.equal(
          (
            await db.pool.query(
              'SELECT verification_count::text FROM moria.runtime_snapshot_verifications WHERE key=$1',
              [attestation.key],
            )
          ).rows[0].verification_count,
          '1',
        );
      },
    );
    await t.test(
      'a ready verification never caches permission across current authority revocation',
      async () => {
        const before = await generations();
        await publish();
        await mirror();
        await native('index-plan', [s3Binding]);
        await authority.setGrants(
          environment,
          'fixture',
          'pipeline',
          'native-indexing',
          operations.filter((op) => op !== `${target}:index`),
        );
        await assert.rejects(native('index', [s3Binding]));
        assert.equal(await generations(), before);
      },
    );
    await t.test(
      'an accepted native indexing attempt crosses ordinary TTL under minimal private authority and current revocation guards',
      async () => {
        await authority.setGrants(
          environment,
          'fixture',
          'pipeline',
          'native-indexing',
          operations,
        );
        await indexer.registerDistributed(session, {
          targetId: target,
          warehouse: root,
          partitions: 4,
        });
        await publish();
        const short = await sessions.mint(context, 'fixture');
        const shortHash = createHash('sha256').update(short.token).digest('hex');
        // Expiry is fixed before acceptance; the test crosses the actual wall clock boundary.
        await db.pool.query(
          "UPDATE moria.workload_sessions SET expires_at=clock_timestamp()+interval '4 seconds' WHERE token_hash=$1",
          [shortHash],
        );
        await indexer.registerDistributed(
          { ...context, sessionHash: shortHash },
          { targetId: target, warehouse: root, partitions: 4 },
        );
        await db.pool.query(
          `UPDATE moria.indexing_builds b SET status='cancelled' WHERE environment=$1
        AND status IN ('pending','planning','running','preparing','ready')
        AND binding_revision<>(SELECT revision FROM moria.indexing_bindings WHERE environment=$1 AND target=$2)`,
          [environment, target],
        );
        const before = await generations();
        let acceptClaim!: (value: { build: string; worker: string; fence: string }) => void;
        const claimed = new Promise<{ build: string; worker: string; fence: string }>((resolve) => {
          acceptClaim = resolve;
        });
        const run = promisify(execFile)(
          process.env.MORIA_RUNTIME_BINARY ?? '/usr/local/bin/moria-runtime',
          ['drain'],
          {
            timeout: 120000,
            maxBuffer: 1024 * 1024,
            env: {
              PATH: process.env.PATH,
              MORIA_INDEXING_DATABASE_SECRET_REF: 'SNAPSHOT_DATABASE',
              SNAPSHOT_DATABASE: connection,
              MORIA_INDEXING_DATABASE_TLS: 'disable',
              MORIA_NATIVE_STORAGE_BINDINGS: JSON.stringify([posix]),
              MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT: scratch,
              MORIA_NATIVE_CLOSURE_LIMITS: JSON.stringify({
                maxEntries: 10000,
                maxReferencedBytes: 1024 * 1024 * 1024,
                maxSpoolBytes: 32 * 1024 * 1024,
              }),
            },
          },
        );
        void run.catch(() => {});
        let buffer = '',
          stopped = false;
        run.child.stdout!.on('data', (bytes) => {
          buffer += bytes.toString();
          for (;;) {
            const end = buffer.indexOf('\n');
            if (end < 0) break;
            const line = buffer.slice(0, end);
            buffer = buffer.slice(end + 1);
            try {
              const event = JSON.parse(line);
              if (!stopped && event.event === 'planning') {
                stopped = true;
                run.child.kill('SIGSTOP');
                acceptClaim(event);
              }
            } catch {}
          }
        });
        try {
          const claim = await Promise.race([
            claimed,
            run.then(() => {
              throw new Error('No live native indexing claim observed');
            }),
          ]);
          const privateToken = randomBytes(32).toString('base64url');
          const privateHash = createHash('sha256').update(privateToken).digest('hex');
          const issued = (
            await db.pool.query(
              'SELECT moria.runtime_indexing_session_issue($1,NULL,$2,$3,$4) AS hash',
              [claim.build, claim.worker, claim.fence, privateHash],
            )
          ).rows[0].hash;
          assert.equal(issued, privateHash);
          const privateState = async () =>
            (await db.pool.query('SELECT moria.native_serving_session($1) AS state', [privateHash]))
              .rows[0].state;
          const storageCheck = () =>
            db.pool.query(
              "SELECT moria.native_storage_check($1,NULL,$2,$3,'warehouse','table','v1',false)",
              [claim.build, claim.worker, claim.fence],
            );
          assert.deepEqual(
            (await privateState()).identity.grantedOperations.sort(),
            [`${source}:read`, `${target}:index`].sort(),
          );
          await assert.rejects(
            sessions.authenticate(`Bearer ${privateToken}`),
            /expired or was revoked/,
          );
          const expires = (
            await db.pool.query(
              'SELECT expires_at FROM moria.workload_sessions WHERE token_hash=$1',
              [shortHash],
            )
          ).rows[0].expires_at as Date;
          await delay(Math.max(0, expires.getTime() - Date.now()) + 100);
          await assert.rejects(
            sessions.authenticate(`Bearer ${short.token}`),
            /expired or was revoked/,
          );
          await db.pool.query('SELECT moria.indexing_check($1)', [claim.build]);
          await storageCheck();
          await privateState();
          await assert.rejects(
            db.pool.query('SELECT moria.runtime_indexing_session_issue($1,NULL,$2,$3,$4)', [
              claim.build,
              claim.worker,
              (BigInt(claim.fence) + 1n).toString(),
              createHash('sha256').update('wrong-fence').digest('hex'),
            ]),
            /LEASE_LOST/,
          );
          await assert.rejects(
            db.pool.query("SELECT moria.native_serving_resource_access($1,$2,'read')", [
              privateHash,
              target,
            ]),
            /FORBIDDEN/,
          );
          const control = async (sql: string, parameters: unknown[]) => {
            const client = await db.pool.connect();
            try {
              await client.query('BEGIN');
              await client.query(sql, parameters);
              await assert.rejects(
                client.query('SELECT moria.native_serving_session($1)', [privateHash]),
                /UNAUTHENTICATED|AUTHORIZATION_CHANGED|FORBIDDEN|JOB_LEASE_LOST|INDEXING_DELEGATION_INACTIVE/,
              );
            } finally {
              await client.query('ROLLBACK');
              client.release();
            }
          };
          await control('UPDATE moria.workload_sessions SET disabled=true WHERE token_hash=$1', [
            shortHash,
          ]);
          await control(
            "UPDATE moria.application_scopes SET operations='{}' WHERE environment=$1 AND application='native-indexing'",
            [environment],
          );
          await control(
            "UPDATE moria.permissions SET operations='{}' WHERE environment=$1 AND principal='pipeline'",
            [environment],
          );
          await control(
            "UPDATE moria.principals SET markings='{}' WHERE environment=$1 AND principal='pipeline'",
            [environment],
          );
          await control("UPDATE moria.indexing_builds SET status='cancelled' WHERE id=$1", [
            claim.build,
          ]);
          await control('UPDATE moria.indexing_builds SET fence=fence+1 WHERE id=$1', [
            claim.build,
          ]);
          await control('UPDATE moria.indexing_builds SET lease_until=NULL WHERE id=$1', [
            claim.build,
          ]);
          await control('UPDATE moria.indexing_instance SET incarnation=gen_random_uuid()', []);
          await control(
            'UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id=$1',
            [environment],
          );
          await assert.rejects(
            indexer.registerDistributed(
              { ...context, sessionHash: shortHash },
              { targetId: target, warehouse: root, partitions: 4 },
            ),
            /expired or was revoked/,
          );
          run.child.kill('SIGCONT');
          await run;
          assert.equal(await generations(), before + 1);
          await assert.rejects(privateState(), /JOB_LEASE_LOST/);
        } finally {
          if (run.child.exitCode === null && run.child.signalCode === null) {
            run.child.kill('SIGCONT');
            run.child.kill('SIGKILL');
          }
          await run.catch(() => {});
        }
      },
    );
    await t.test(
      'a published native index retains every enabled Dataset namespace classification after later disable',
      async () => {
        const before = (await authority.current(environment, 'pipeline', 'native-indexing')).epoch;
        await db.pool.query(
          `INSERT INTO moria.native_storage_namespaces(environment,warehouse_id,table_id,resource,revision,purpose,enabled,markings)
        VALUES($1,'new-classification','secret',$2,'v1','dataset-source',true,ARRAY['secret'])`,
          [environment, source],
        );
        const labels = async (hash: string) =>
          (
            await db.pool.query(
              "SELECT moria.native_serving_resource_access($1,$2,'read') AS labels",
              [hash, target],
            )
          ).rows[0].labels;
        assert.ok(
          BigInt((await authority.current(environment, 'pipeline', 'native-indexing')).epoch) >
            BigInt(before),
        );
        await assert.rejects(labels(session.sessionHash), /FORBIDDEN/);
        await assert.rejects(
          db.catalog.transaction((c) => authority.resourceAccess(c, session, target), environment),
          /classification/,
        );
        await db.pool.query(
          "UPDATE moria.native_storage_namespaces SET enabled=false WHERE warehouse_id='new-classification' AND table_id='secret'",
        );
        await assert.rejects(labels(session.sessionHash), /FORBIDDEN/);
        await authority.syncPrincipal(environment, 'fixture', 'pipeline', {
          enabled: true,
          kind: 'workload',
          attributes: {},
          markings: ['private', 'secret'],
        });
        assert.deepEqual(await labels(session.sessionHash), ['private', 'secret']);
        assert.deepEqual(
          await db.catalog.transaction(
            (c) => authority.resourceAccess(c, session, target),
            environment,
          ),
          ['private', 'secret'],
        );
      },
    );
  },
);
