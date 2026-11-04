import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { mkdir, readFile, writeFile, rename } from 'node:fs/promises';
import { join } from 'node:path';
import pg from 'pg';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { startS3 } from './fixtures/storage-s3/server.js';
import { configuredFilesystem } from '../services/filesystem/src/local.js';
import { createServer, type ServerResponse } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { ObjectStore, type ObjectSchema } from '../services/ontology/src/store.js';
import { database } from './fixtures/catalog-storage/database.js';
import { commitProxy } from './fixtures/indexing/commit-proxy.js';
import { assertNativeArtifacts } from './fixtures/native-common/artifact.js';

test(
  'native disabled restore fences real work and external uncertainty without losing permanent state',
  { timeout: 90000 },
  async (t) => {
    const runtime = process.env.MORIA_NATIVE_BINARY ?? '/usr/local/bin/moria-runtime';
    await assertNativeArtifacts(runtime, []);
    const db = await database(),
      children: ChildProcess[] = [],
      exits: Promise<unknown>[] = [];
    t.after(async () => {
      for (const child of children) child.kill('SIGKILL');
      await Promise.allSettled(exits);
      await db.close();
    });
    const environment = randomUUID(),
      resource = randomUUID(),
      context = { environment, principal: 'recovery-user', application: 'recovery-client' };
    const authority = new Authority(db.catalog),
      store = new ObjectStore(db.catalog);
    await db.catalog.ensureEnvironment(environment);
    const operations = [`${resource}:write`, 'objects:read', 'work:execute', 'notify:invoke'];
    await authority.syncPrincipal(environment, 'fixture', context.principal, {
      kind: 'workload',
      enabled: true,
      attributes: {},
      markings: [],
    });
    await authority.setApplication(environment, 'fixture', context.application, operations);
    await authority.setGrants(
      environment,
      'fixture',
      context.principal,
      context.application,
      operations,
    );
    const epoch = (await authority.current(environment, context.principal, context.application))
      .epoch;
    const issued = await new WorkloadSessions(authority).mint(context, 'fixture');
    const uid = await db.catalog.resolveIdentity(`recovery-${environment}`, 'permanent-key');
    const schema: ObjectSchema = {
      primaryKey: 'uid',
      properties: {
        uid: { type: 'uid', nullable: false },
        name: { type: 'string', nullable: false },
      },
    };
    await store.publishStage(
      await store.stage(environment, 'objects', schema, [{ uid, name: 'source' }], epoch, '0'),
    );
    const actionRequest = {
      ...context,
      action: 'notify',
      key: 'permanent-action-key',
      input: { uid },
      actionRevision: 'v1',
      securityEpoch: epoch,
    };
    const receipt = await store.edit(
      actionRequest,
      'objects',
      uid,
      schema,
      [{ op: 'set', property: 'name', value: 'overlay' }],
      () => {},
      (client) =>
        authority.assertCurrent(client, environment, context.principal, context.application, epoch),
    );
    const originalIncarnation = (
      await db.pool.query('SELECT incarnation FROM moria.indexing_instance')
    ).rows[0].incarnation;
    const role = `recovery_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role};GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role};GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    const url = (port = db.pool.options.port) =>
      `postgresql://${role}:${password}@127.0.0.1:${port}/postgres`;
    const operatorConnection = (port = db.pool.options.port) =>
      `postgresql://${db.pool.options.user}:${encodeURIComponent(db.pool.options.password as string)}@127.0.0.1:${port}/postgres`;
    const start = (
      domain: string,
      command: string,
      input: unknown,
      connection = domain === 'recovery' || domain === 'retention' ? operatorConnection() : url(),
    ) => {
      const child = spawn(runtime, [domain, command], {
        env: {
          PATH: process.env.PATH,
          MORIA_INDEXING_DATABASE_SECRET_REF: 'RECOVERY_DATABASE',
          RECOVERY_DATABASE: connection,
          MORIA_INDEXING_DATABASE_TLS: 'disable',
          MORIA_ISOLATED_RESTORE: '1',
          MORIA_DELIVERY_ALLOW_LOOPBACK_HTTP: '1',
        },
        stdio: ['pipe', 'pipe', 'pipe'],
      });
      children.push(child);
      let output = '',
        error = '';
      child.stdout!.on('data', (b) => (output += b));
      child.stderr!.on('data', (b) => (error = (error + b).slice(-65536)));
      child.stdin!.on('error', () => {});
      child.stdin!.end(JSON.stringify(input));
      const done = new Promise<any>((resolve, reject) => {
        child.once('error', reject);
        child.once('exit', (code) =>
          code === 0
            ? resolve(JSON.parse(output))
            : reject(new Error(`${domain}/${command}: ${error}`)),
        );
      });
      exits.push(done);
      void done.catch(() => {});
      return { child, done };
    };
    const native = (domain: string, command: string, input: unknown) =>
      start(domain, command, input).done;
    await assert.rejects(
      start(
        'recovery',
        'create',
        {
          requestKey: 'ordinary-role-denied',
          owner: randomUUID(),
          keyManifestHash: 'a'.repeat(64),
          retentionSeconds: 86400,
        },
        url(),
      ).done,
      /RECOVERY_OPERATOR_REQUIRED/,
    );
    const ordinary = new pg.Pool({ connectionString: url() });
    try {
      await assert.rejects(
        ordinary.query('UPDATE moria.runtime_recovery_control SET dispatch_enabled=false'),
        /RECOVERY_OPERATOR_REQUIRED/,
      );
      assert.equal(
        (await db.pool.query('SELECT dispatch_enabled FROM moria.runtime_recovery_control')).rows[0]
          .dispatch_enabled,
        true,
      );
    } finally {
      await ordinary.end();
    }

    const spec = {
      version: 1,
      domain: 'transform',
      release: 'a'.repeat(64),
      resourceClass: 'standard',
      timeoutSeconds: 120,
      retryPolicy: 'idempotent',
      inputSchema: { type: 'object', properties: {} },
      pinnedInputs: [],
      expectedHeads: {},
    };
    const binding = async (domain: string) =>
      (
        await db.pool.query(
          'INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation) SELECT $1,$2,$3,$4,$5,$6,$7,s.security_epoch,i.incarnation FROM moria.instance_state s,moria.indexing_instance i RETURNING id',
          [environment, resource, { ...spec, domain }, context, operations, [], epoch],
        )
      ).rows[0].id;
    const workBinding = await binding('transform');
    const execution = await native('execution', 'request', {
      binding: workBinding,
      requestKey: 'restore-active-work',
      input: {},
      tasks: [{ key: 'partition-0', partition: 0 }],
    });
    const claim = await native('execution', 'claim', {
      owner: randomUUID(),
      domains: ['transform'],
    });
    assert.equal(claim.lease.execution, execution.id);
    let held: ServerResponse | undefined,
      providerEffects = 0,
      providerReceipt: any;
    const sink = createServer(async (request, response) => {
      for await (const _ of request) {
      }
      providerEffects++;
      held = response;
      providerReceipt = {
        version: 1,
        deliveryId: request.headers['moria-delivery-id'],
        payloadHash: request.headers['moria-payload-sha256'],
        receiptId: 'applied-before-restore',
      };
    });
    await new Promise<void>((resolve) => sink.listen(0, '127.0.0.1', resolve));
    t.after(async () => {
      sink.closeAllConnections();
      await new Promise<void>((resolve) => sink.close(() => resolve()));
    });
    const deliveryBinding = await binding('delivery');
    const destination = (
      await db.pool.query(
        'INSERT INTO moria.runtime_delivery_destinations(binding,endpoint,idempotent,allowed_markings) VALUES($1,$2,false,$3) RETURNING id',
        [
          deliveryBinding,
          `http://127.0.0.1:${(sink.address() as { port: number }).port}/effect`,
          [],
        ],
      )
    ).rows[0].id;
    const delivery = (
      await db.pool.query('SELECT moria.runtime_delivery_enqueue($1,$2,$3,$4,$5) AS id', [
        destination,
        'restore-notification',
        { version: 1, result: receipt },
        [],
        { kind: 'action', ...context, action: 'notify', key: actionRequest.key },
      ])
    ).rows[0].id;
    const dispatch = start('delivery', 'once', { owner: randomUUID() });
    for (let i = 0; i < 500 && !held; i++) await delay(10);
    assert.ok(held, 'actual external effect must precede restore');
    assert.equal(providerEffects, 1);
    let lease: any, restore: any;
    await t.test('maintenance leases are actual fenced, bounded and idempotent', async () => {
      const request = {
        requestKey: 'isolated-recovery',
        owner: randomUUID(),
        keyManifestHash: 'b'.repeat(64),
        retentionSeconds: 86400,
      };
      lease = await native('recovery', 'create', request);
      assert.deepEqual(await native('recovery', 'create', request), lease);
      await native('recovery', 'renew', lease);
      await assert.rejects(
        native('recovery', 'create', { ...request, retentionSeconds: 172800 }),
        /RECOVERY_MANIFEST_CONFLICT/,
      );
      await assert.rejects(
        native('recovery', 'create', { ...request, keyManifestHash: 'c'.repeat(64) }),
      );
      await assert.rejects(
        native('recovery', 'renew', { ...lease, fence: String(Number(lease.fence) + 1) }),
      );
    });
    await t.test(
      'lost pause COMMIT reply leaves dispatch disabled and exact retry finishes fencing',
      async () => {
        const proxy = await commitProxy(
          Number(db.pool.options.port),
          'moria.runtime_restore_pause',
        );
        let proxyClosed = false;
        t.after(async () => {
          if (!proxyClosed) await proxy.close();
        });
        const input = {
          restore: randomUUID(),
          set: lease.set,
          manifestHash: 'd'.repeat(64),
          expectedIncarnation: originalIncarnation,
        };
        let pausing: ReturnType<typeof start>;
        const barrier = await db.pool.connect();
        let barrierReleased = false;
        try {
          await barrier.query('BEGIN');
          await barrier.query('SELECT moria.runtime_environment_cell_check($1)', [environment]);
          pausing = start('recovery', 'restore-disabled', input, operatorConnection(proxy.port));
          for (let i = 0; i < 500; i++) {
            const waiting = (
              await db.pool.query(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE usename=$1 AND wait_event_type='Lock' AND query LIKE '%runtime_restore_pause%') AS waiting",
                [db.pool.options.user],
              )
            ).rows[0].waiting;
            if (waiting) break;
            if (i === 499) assert.fail('restore must wait on actual admitted transaction');
            await delay(10);
          }
          assert.equal(
            (await db.pool.query('SELECT dispatch_enabled FROM moria.runtime_recovery_control'))
              .rows[0].dispatch_enabled,
            true,
          );
          await barrier.query('COMMIT');
          barrier.release();
          barrierReleased = true;
        } finally {
          if (!barrierReleased) {
            await barrier.query('ROLLBACK');
            barrier.release();
          }
        }
        await proxy.commit;
        const paused = (
          await db.pool.query(
            'SELECT c.dispatch_enabled,r.new_incarnation,r.leases_fenced FROM moria.runtime_recovery_control c JOIN moria.runtime_restores r ON r.id=c.restore_id',
          )
        ).rows[0];
        assert.equal(paused.dispatch_enabled, false);
        assert.equal(paused.leases_fenced, false);
        assert.notEqual(paused.new_incarnation, originalIncarnation);
        pausing.child.kill('SIGKILL');
        await proxy.close();
        proxyClosed = true;
        await assert.rejects(pausing.done);
        restore = await native('recovery', 'restore-disabled', input);
        assert.equal(restore.newIncarnation, paused.new_incarnation);
        assert.equal(restore.leasesFenced, true);
        assert.equal(restore.dispatchEnabled, false);
        assert.deepEqual(await native('recovery', 'restore-disabled', input), restore);
        await assert.rejects(
          native('recovery', 'restore-disabled', { ...input, manifestHash: 'e'.repeat(64) }),
        );
      },
    );
    await t.test(
      'old active leases and credentials stay fenced while permanent state survives',
      async () => {
        await assert.rejects(native('execution', 'renew', claim.lease));
        await assert.rejects(native('recovery', 'renew', lease));
        assert.equal(
          (
            await db.pool.query('SELECT status,error FROM moria.runtime_executions WHERE id=$1', [
              execution.id,
            ])
          ).rows[0].status,
          'failed',
        );
        assert.equal(
          (
            await db.pool.query(
              'SELECT disabled FROM moria.workload_sessions WHERE token_hash=$1',
              [createHash('sha256').update(issued.token).digest('hex')],
            )
          ).rows[0].disabled,
          true,
        );
        assert.equal(
          (
            await db.pool.query(
              'SELECT uid FROM moria.identities WHERE domain=$1 AND canonical_key=$2',
              [`recovery-${environment}`, 'permanent-key'],
            )
          ).rows[0].uid,
          uid,
        );
        assert.equal(
          (
            await db.pool.query(
              'SELECT result FROM moria.action_keys WHERE environment=$1 AND key=$2',
              [environment, actionRequest.key],
            )
          ).rows[0].result.revision,
          receipt.revision,
        );
        assert.equal(
          (
            await db.pool.query(
              'SELECT overrides FROM moria.overlay_versions WHERE environment=$1 AND uid=$2 ORDER BY revision DESC LIMIT 1',
              [environment, uid],
            )
          ).rows[0].overrides.name,
          'overlay',
        );
        await assert.rejects(
          db.pool.query('SELECT moria.runtime_environment_cell_check($1)', [environment]),
          /RESTORE_DISPATCH_DISABLED/,
        );
      },
    );
    await t.test(
      'a real applied external effect remains uncertain and cannot silently dispatch again',
      async () => {
        held!
          .writeHead(200, { 'content-type': 'application/json' })
          .end(JSON.stringify(providerReceipt));
        await dispatch.done;
        const row = (
          await db.pool.query('SELECT status,error FROM moria.runtime_deliveries WHERE id=$1', [
            delivery,
          ])
        ).rows[0];
        assert.equal(row.status, 'uncertain');
        assert.equal(row.error, 'RESTORE_RECONCILIATION_REQUIRED');
        assert.equal((await native('delivery', 'once', { owner: randomUUID() })).worked, false);
        assert.equal(providerEffects, 1);
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.runtime_delivery_attempts WHERE delivery=$1',
              [delivery],
            )
          ).rows[0].count,
          '1',
        );
      },
    );
  },
);

for (const provider of ['posix', 's3'] as const)
  test(
    `native clean-host restore verifies multipart metadata, content and keys before enabling dispatch (${provider})`,
    { timeout: 180000 },
    async (t) => {
      const runtime = process.env.MORIA_NATIVE_BINARY ?? '/usr/local/bin/moria-runtime';
      await assertNativeArtifacts(runtime, []);
      const db = await database();
      const children: ChildProcess[] = [];
      const exits: Promise<unknown>[] = [];
      let target: pg.Pool | undefined;
      let s3: Awaited<ReturnType<typeof startS3>> | undefined;
      t.after(async () => {
        for (const child of children) child.kill('SIGKILL');
        await Promise.allSettled(exits);
        await target?.end();
        await s3?.close();
        await db.close();
      });
      if (provider === 's3') s3 = await startS3();
      const runtimeRole = `restore_runtime_${randomUUID().replaceAll('-', '')}`,
        runtimePassword = randomUUID();
      await db.pool.query(`CREATE ROLE ${runtimeRole} LOGIN PASSWORD '${runtimePassword}'`);
      const environment = randomUUID(),
        application = 'backup-app',
        principal = 'backup-user';
      await db.catalog.ensureEnvironment(environment);
      const authority = new Authority(db.catalog);
      const operations = [
        'filesystem:read',
        'filesystem:list',
        'filesystem:create',
        'filesystem:write',
        'instance:manage',
      ];
      await authority.syncPrincipal(environment, 'fixture', principal, {
        kind: 'workload',
        enabled: true,
        attributes: {},
        markings: [],
      });
      await authority.setApplication(environment, 'fixture', application, operations);
      await authority.setGrants(environment, 'fixture', principal, application, operations);
      const session = await new WorkloadSessions(authority).mint(
        { environment, application, principal },
        'fixture',
      );
      const context = {
        session: await new WorkloadSessions(authority).authenticate(`Bearer ${session.token}`),
      };
      const sourceRoot = join(db.root, 'source-files'),
        backupRoot = join(db.root, 'backup-files'),
        restoredRoot = join(db.root, 'restored-files');
      const local = join(db.root, 'local'),
        restoredLocal = join(db.root, 'restored-local'),
        restoreKeys = join(restoredLocal, 'auth'),
        staging = join(db.root, 'restore-staging'),
        scratch = join(db.root, 'restore-scratch');
      for (const path of [
        backupRoot,
        restoredRoot,
        join(local, 'auth'),
        restoreKeys,
        staging,
        scratch,
      ])
        await mkdir(path, { recursive: true, mode: 0o700 });
      const key = randomBytes(32),
        keyPath = join(local, 'auth/session-key');
      await writeFile(keyPath, key, { mode: 0o600 });
      const fs = await configuredFilesystem(
        db.catalog,
        sourceRoot,
        s3
          ? {
              MORIA_FILESYSTEM_BACKEND: 's3',
              MORIA_FILESYSTEM_S3_ENDPOINT: s3.endpoint,
              MORIA_FILESYSTEM_S3_BUCKET: s3.bucket,
              MORIA_FILESYSTEM_S3_REGION: 'us-east-1',
              MORIA_FILESYSTEM_S3_PREFIX: 'source',
              MORIA_FILESYSTEM_S3_ACCESS_KEY_ID: s3.credentials.accessKeyId,
              MORIA_FILESYSTEM_S3_SECRET_ACCESS_KEY: s3.credentials.secretAccessKey,
            }
          : {},
      );
      const home = await fs.management.ensureHome(context),
        parent = await fs.filesystem.store.get(home.home.id);
      const body = randomBytes(2 * 1024 * 1024);
      const file = await fs.filesystem.writeFile(
        context,
        'retained-file',
        {
          op: 'create',
          parentId: parent.id,
          expectedParentRevision: parent.revision,
          name: 'retained.bin',
          content: {
            size: body.length,
            digest: `sha256:${createHash('sha256').update(body).digest('hex')}`,
            mediaType: 'application/octet-stream',
          },
        },
        new ReadableStream({
          start(controller) {
            for (let i = 0; i < body.length; i += 1024 * 1024)
              controller.enqueue(body.subarray(i, i + 1024 * 1024));
            controller.close();
          },
        }),
      );
      const fileId = file.entries.find((entry) => entry.name === 'retained.bin')!.id;
      const uid = await db.catalog.resolveIdentity(`backup-${environment}`, 'retained-key');
      await db.pool.query(
        'CREATE TABLE moria.backup_stream_probe(id integer PRIMARY KEY,body bytea NOT NULL)',
      );
      await db.pool.query(
        'ALTER TABLE moria.backup_stream_probe ALTER COLUMN body SET STORAGE EXTERNAL',
      );
      for (let i = 0; i < 12; i++)
        await db.pool.query('INSERT INTO moria.backup_stream_probe VALUES($1,$2)', [
          i,
          randomBytes(2 * 1024 * 1024),
        ]);
      await db.pool.query(
        `GRANT USAGE ON SCHEMA moria TO ${runtimeRole}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${runtimeRole}; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${runtimeRole}`,
      );
      const namespace = { warehouseId: 'filesystem', tableId: 'content', revision: 'v1' },
        backupNamespace = { warehouseId: 'backup', tableId: 'sets', revision: 'backup-v1' };
      const storage = (root: string, ns = namespace) => ({
        namespace: ns,
        provider: s3
          ? {
              kind: 's3',
              binding: {
                warehouseId: ns.warehouseId,
                tableId: ns.tableId,
                endpoint: s3.endpoint,
                bucket: s3.bucket,
                prefix:
                  root === backupRoot ? 'backup' : root === sourceRoot ? 'source' : 'restored',
                region: 'us-east-1',
                accessKeySecretRef: 'RECOVERY_S3_ACCESS',
                secretKeySecretRef: 'RECOVERY_S3_SECRET',
                sessionTokenSecretRef: null,
                limits: { maxObjectBytes: 64 * 1024 * 1024, chunkBytes: 1024 * 1024 },
              },
            }
          : {
              kind: 'posix',
              root,
              limits: { maxObjectBytes: 64 * 1024 * 1024, chunkBytes: 1024 * 1024 },
            },
      });
      const backupStorage = storage(backupRoot, backupNamespace);
      const storedBytes = async (root: string, warehouse: string, table: string, id: string) => {
        if (!s3) return readFile(join(root, warehouse, table, 'objects', id));
        const result = await s3.client.send(
          new GetObjectCommand({
            Bucket: s3.bucket,
            Key: `${root === backupRoot ? 'backup' : 'restored'}/${warehouse}/${table}/objects/${id}`,
          }),
        );
        assert.ok(result.Body);
        return Buffer.from(await result.Body.transformToByteArray());
      };
      const connection = (database = 'postgres') =>
        `postgresql://${db.pool.options.user}:${encodeURIComponent(db.pool.options.password as string)}@127.0.0.1:${db.pool.options.port}/${database}`;
      const common = {
        ...(s3
          ? {
              RECOVERY_S3_ACCESS: s3.credentials.accessKeyId,
              RECOVERY_S3_SECRET: s3.credentials.secretAccessKey,
            }
          : {}),
        PATH: process.env.PATH,
        MORIA_INDEXING_DATABASE_SECRET_REF: 'RECOVERY_DATABASE',
        MORIA_INDEXING_DATABASE_TLS: 'disable',
        MORIA_ISOLATED_RESTORE: '1',
        MORIA_RECOVERY_PG_DUMP: '/usr/local/bin/pg_dump',
        MORIA_RECOVERY_PG_RESTORE: '/usr/local/bin/pg_restore',
        MORIA_LOCAL_DIR: local,
        MORIA_RECOVERY_KEYS: JSON.stringify({ 'session-encryption': keyPath }),
        MORIA_NATIVE_RECOVERY_NAMESPACE: JSON.stringify(backupNamespace),
        MORIA_NATIVE_RECOVERY_LIMITS: JSON.stringify({
          roots: 1000,
          members: 10000,
          bytes: 128 * 1024 * 1024,
          spillBytes: 16 * 1024 * 1024,
          dumpBytes: 128 * 1024 * 1024,
          timeSeconds: 120,
        }),
        MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT: scratch,
        MORIA_RECOVERY_STAGING_ROOT: staging,
      };
      const native = (args: string[], input: unknown, env: Record<string, string | undefined>) => {
        const selected: Record<string, string | undefined> = { ...common, ...env };
        if (!['recovery', 'restore-from'].includes(args[0])) {
          const url = new URL(selected.RECOVERY_DATABASE!);
          url.username = runtimeRole;
          url.password = runtimePassword;
          selected.RECOVERY_DATABASE = url.href;
        }
        const child = spawn(runtime, args, {
          env: selected,
          stdio: ['pipe', 'pipe', 'pipe'],
        });
        children.push(child);
        let out = '',
          error = '';
        child.stdout!.on('data', (b) => (out += b));
        child.stderr!.on('data', (b) => (error = (error + b).slice(-65536)));
        child.stdin!.on('error', () => {});
        child.stdin!.end(JSON.stringify(input));
        const done = new Promise<any>((resolve, reject) => {
          child.once('error', reject);
          child.once('exit', (code) =>
            code === 0
              ? resolve(JSON.parse(out))
              : reject(new Error(`${args.join('/')}: ${error}`)),
          );
        });
        exits.push(done);
        void done.catch(() => {});
        return done;
      };
      const sourceEnv = {
        RECOVERY_DATABASE: connection(),
        MORIA_NATIVE_STORAGE_BINDINGS: JSON.stringify([storage(sourceRoot), backupStorage]),
      };
      const oldCell = randomUUID(),
        domain = `backup-${environment}`;
      await native(
        ['identity', 'register-cell'],
        { cell: oldCell, endpointRef: 'SOURCE_CELL_ENDPOINT' },
        sourceEnv,
      );
      const identityOwner = await native(
        ['identity', 'register-domain'],
        { domain, cell: oldCell },
        sourceEnv,
      );
      assert.equal(
        (
          await native(
            ['identity', 'resolve'],
            { domain, owner: identityOwner, keys: ['retained-key'] },
            sourceEnv,
          )
        ).uids[0],
        uid,
      );
      const placement = await native(
        ['directory', 'place'],
        { environment, cell: oldCell },
        sourceEnv,
      );
      await native(['directory', 'activate'], { environment, owner: placement.owner }, sourceEnv);
      const oldCliKey = randomUUID(),
        profiles = JSON.stringify({ operator: { environment, principal, application } });
      await native(
        ['recovery', 'approve-cli'],
        {},
        { ...sourceEnv, MORIA_NATIVE_CLI_KEY: oldCliKey, MORIA_NATIVE_CLI_PROFILES: profiles },
      );
      const wrongKeyPath = join(local, 'auth/wrong-key');
      await writeFile(wrongKeyPath, randomBytes(32), { mode: 0o600 });
      await assert.rejects(
        native(
          ['recovery', 'capture'],
          { requestKey: 'wrong-key', owner: randomUUID(), retentionSeconds: 86400 },
          {
            ...sourceEnv,
            MORIA_RECOVERY_KEYS: JSON.stringify({ 'session-encryption': wrongKeyPath }),
          },
        ),
        /RECOVERY_KEY_CHANGED/,
      );
      const backup = await native(
        ['recovery', 'capture'],
        { requestKey: 'clean-host', owner: randomUUID(), retentionSeconds: 86400 },
        sourceEnv,
      );
      assert.equal(backup.retention.status, 'ready');
      assert.equal(backup.retention.manifestHash, backup.manifestHash);
      const metadataRoot = backup.retention.metadataDump;
      const metadataBytes = await storedBytes(
        backupRoot,
        'backup',
        'sets',
        metadataRoot.object.objectId,
      );
      const archive = JSON.parse(metadataBytes.toString());
      assert.ok(
        archive.parts >= 3,
        'incompressible PostgreSQL archive must span real bounded parts',
      );
      assert.ok(archive.bytes > 24 * 1024 * 1024);
      await db.pool.query('CREATE DATABASE restored_fixture');
      target = new pg.Pool({ connectionString: connection('restored_fixture') });
      const targetEnv = {
        RECOVERY_DATABASE: connection('restored_fixture'),
        MORIA_LOCAL_DIR: restoredLocal,
        MORIA_NATIVE_STORAGE_BINDINGS: JSON.stringify([storage(restoredRoot), backupStorage]),
        MORIA_RECOVERY_RESTORE_KEYS: JSON.stringify({
          'session-encryption': join(restoreKeys, 'session-key'),
        }),
      };
      const input = {
        restore: randomUUID(),
        manifest: backup.manifest,
        targetNamespaces: [namespace],
      };
      await assert.rejects(
        native(
          ['restore-from'],
          { ...input, manifest: { ...backup.manifest, sha256: `sha256:${'0'.repeat(64)}` } },
          targetEnv,
        ),
        /RECOVERY_MANIFEST_CONFLICT/,
      );
      assert.equal(
        (await target.query("SELECT to_regnamespace('moria') IS NULL AS absent")).rows[0].absent,
        true,
      );
      await assert.rejects(
        native(['restore-from'], input, { ...targetEnv, MORIA_RECOVERY_RESTORE_KEYS: '{}' }),
        /RECOVERY_KEY/,
      );
      assert.equal(
        (await target.query('SELECT dispatch_approved FROM public.moria_restore_bootstrap')).rows[0]
          .dispatch_approved,
        false,
      );
      assert.equal(
        (await target.query('SELECT dispatch_enabled FROM moria.runtime_recovery_control')).rows[0]
          .dispatch_enabled,
        false,
      );
      await assert.rejects(
        native(
          ['identity', 'resolve'],
          { domain, owner: identityOwner, keys: ['retained-key'] },
          targetEnv,
        ),
        /RESTORE_DISPATCH_DISABLED/,
      );
      const restored = await native(['restore-from'], input, targetEnv);
      t.diagnostic(`verified ${provider} restore approval completed`);
      await assert.rejects(
        native(['restore-from'], { ...input, targetNamespaces: [] }, targetEnv),
        /RECOVERY_MANIFEST_CONFLICT/,
      );
      assert.deepEqual(
        await native(
          ['recovery', 'restore-disabled'],
          {
            restore: input.restore,
            set: backup.recoverySet,
            manifestHash: backup.manifestHash,
            expectedIncarnation: restored.previousIncarnation,
          },
          targetEnv,
        ),
        restored,
      );
      const retainedSet = await native(
        ['recovery', 'inspect'],
        { set: backup.recoverySet },
        targetEnv,
      );
      assert.equal(retainedSet.status, 'ready');
      assert.deepEqual(retainedSet.sealedManifest, backup.manifest);
      assert.deepEqual(
        await native(['restore-from'], input, targetEnv),
        restored,
        'lost final COMMIT acknowledgement reconciles the exact completed restore',
      );
      await assert.rejects(
        native(
          ['recovery', 'approve-cli'],
          {},
          { ...targetEnv, MORIA_NATIVE_CLI_KEY: oldCliKey, MORIA_NATIVE_CLI_PROFILES: profiles },
        ),
        /CLI_KEY_REUSE_FORBIDDEN/,
      );
      await target.query('UPDATE moria.principals SET enabled=true WHERE environment=$1;', [
        environment,
      ]);
      await target.query('UPDATE moria.application_scopes SET enabled=true WHERE environment=$1', [
        environment,
      ]);
      await target.query('UPDATE moria.instance_principals SET enabled=true');
      const newCell = randomUUID();
      await native(
        ['identity', 'register-cell'],
        { cell: newCell, endpointRef: 'RESTORED_CELL_ENDPOINT' },
        targetEnv,
      );
      const beforePlacement = await native(['directory', 'inspect'], { environment }, targetEnv);
      assert.equal(beforePlacement.serving, false);
      assert.equal(beforePlacement.leasesFenced, false);
      const assignment = {
        restore: input.restore,
        environment,
        owner: beforePlacement.owner,
        cell: newCell,
      };
      await assert.rejects(
        native(
          ['recovery', 'assign-cell'],
          {
            ...assignment,
            owner: { ...assignment.owner, fence: String(BigInt(assignment.owner.fence) + 1n) },
          },
          targetEnv,
        ),
        /CELL_OWNERSHIP_LOST/,
      );
      const assigned = await native(['recovery', 'assign-cell'], assignment, targetEnv);
      assert.equal(assigned.serving, false);
      assert.equal(assigned.leasesFenced, true);
      assert.equal(assigned.owner.cell, newCell);
      assert.deepEqual(await native(['recovery', 'assign-cell'], assignment, targetEnv), assigned);
      await native(['directory', 'activate'], { environment, owner: assigned.owner }, targetEnv);
      const domainOwner = await native(['identity', 'inspect'], { domain }, targetEnv);
      const nextOwner = await native(
        ['identity', 'transfer'],
        { domain, owner: domainOwner, next: newCell },
        targetEnv,
      );
      assert.equal(
        (
          await native(
            ['identity', 'resolve'],
            { domain, owner: nextOwner, keys: ['retained-key'] },
            targetEnv,
          )
        ).uids[0],
        uid,
      );
      const newCliKey = randomUUID();
      assert.equal(
        (
          await native(
            ['recovery', 'approve-cli'],
            {},
            { ...targetEnv, MORIA_NATIVE_CLI_KEY: newCliKey, MORIA_NATIVE_CLI_PROFILES: profiles },
          )
        ).approved,
        true,
      );
      assert.equal(restored.status, 'enabled');
      assert.equal(restored.dispatchEnabled, true);
      assert.equal(restored.leasesFenced, true);
      assert.notEqual(restored.previousIncarnation, restored.newIncarnation);
      assert.equal(
        (
          await target.query(
            'SELECT uid FROM moria.identities WHERE domain=$1 AND canonical_key=$2',
            [`backup-${environment}`, 'retained-key'],
          )
        ).rows[0].uid,
        uid,
      );
      assert.equal(
        (
          await target.query(
            'SELECT count(*),sum(octet_length(body)) AS bytes FROM moria.backup_stream_probe',
          )
        ).rows[0].bytes,
        String(24 * 1024 * 1024),
      );
      assert.equal(
        (
          await target.query('SELECT disabled FROM moria.workload_sessions WHERE token_hash=$1', [
            createHash('sha256').update(session.token).digest('hex'),
          ])
        ).rows[0].disabled,
        true,
      );
      assert.deepEqual(await readFile(join(restoreKeys, 'session-key')), key);
      const restoredBinding = (
        await target.query('SELECT binding_hash FROM moria.fs_storage_bindings')
      ).rows[0].binding_hash;
      const sourceBinding = (
        await db.pool.query('SELECT binding_hash FROM moria.fs_storage_bindings')
      ).rows[0].binding_hash;
      assert.notEqual(restoredBinding, sourceBinding);
      const contentRows = (
        await target.query(
          "SELECT snapshot->'content' AS content FROM moria.fs_entry_versions WHERE entry_id=$1 AND snapshot->'content' IS NOT NULL ORDER BY revision DESC LIMIT 1",
          [fileId],
        )
      ).rows;
      assert.equal(contentRows.length, 1);
      const location = contentRows[0].content.location;
      assert.deepEqual(
        await storedBytes(restoredRoot, 'filesystem', 'content', location.objectId),
        body,
      );
      if (s3) await s3.close();
      else await rename(backupRoot, `${backupRoot}-offline`);
      assert.deepEqual(
        await native(['restore-from'], input, targetEnv),
        restored,
        'completed restore reconciles after the actual backup provider becomes unavailable',
      );
    },
  );
