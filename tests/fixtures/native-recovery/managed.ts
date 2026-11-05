import assert from 'node:assert/strict';
import { randomBytes, randomUUID, createHash } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import pg from 'pg';
import { Catalog } from '../../../services/catalog/src/catalog.js';
import { Authority } from '../../../services/security/src/authority.js';
import { WorkloadSessions } from '../../../services/security/src/sessions.js';
import { fingerprint } from '../../../packages/compiler/src/index.js';
import type { database } from '../catalog-storage/database.js';

type Input = {
  db: Awaited<ReturnType<typeof database>>;
  runtime: string;
  harness: string;
  env: Record<string, string | undefined>;
  connection: string;
  environment: string;
  principal: string;
  application: string;
  operations: string[];
  source: string;
  snapshot: Record<string, any>;
  bindings: any[];
  rows: Record<string, unknown>[];
  markings: string[];
  sourceStore: { target: string; stage: string; revision: string };
};
export async function managedDatasetRecovery(input: Input) {
  const {
    db,
    runtime,
    harness,
    environment,
    principal,
    application,
    operations,
    source,
    snapshot,
  } = input;
  const fsNamespace = { warehouseId: 'filesystem', tableId: 'content', revision: 'v1' },
    datasetNamespace = input.bindings[0].namespace;
  const backupNamespace = { warehouseId: 'recovery', tableId: 'sets', revision: 'v1' },
    backupRoot = join(db.root, 'managed-recovery-backup'),
    staging = join(db.root, 'managed-recovery-staging'),
    scratch = join(db.root, 'managed-recovery-scratch'),
    local = join(db.root, 'managed-recovery-local');
  for (const path of [backupRoot, staging, scratch, join(local, 'auth')])
    await mkdir(path, { recursive: true, mode: 0o700 });
  const keyPath = join(local, 'auth/session-key');
  await writeFile(keyPath, randomBytes(32), { mode: 0o600 });
  const posix = (namespace: any, root: string) => ({
    namespace,
    provider: {
      kind: 'posix',
      root,
      limits: { maxObjectBytes: 128 * 1024 * 1024, chunkBytes: 1024 * 1024 },
    },
  });
  const backup = posix(backupNamespace, backupRoot);
  const admin = (name = 'postgres') =>
    `postgresql://${db.pool.options.user}:${encodeURIComponent(db.pool.options.password as string)}@127.0.0.1:${db.pool.options.port}/${name}`;
  const reference = input.env.MORIA_INDEXING_DATABASE_SECRET_REF!;
  const common = {
    ...input.env,
    MORIA_LOCAL_DIR: local,
    MORIA_RECOVERY_KEYS: JSON.stringify({ 'session-encryption': keyPath }),
    MORIA_ISOLATED_RESTORE: '1',
    MORIA_RECOVERY_PG_DUMP: '/usr/local/bin/pg_dump',
    MORIA_RECOVERY_PG_RESTORE: '/usr/local/bin/pg_restore',
    MORIA_NATIVE_RECOVERY_NAMESPACE: JSON.stringify(backupNamespace),
    MORIA_NATIVE_RECOVERY_LIMITS: JSON.stringify({
      roots: 5000,
      members: 50000,
      bytes: 1024 * 1024 * 1024,
      spillBytes: 32 * 1024 * 1024,
      dumpBytes: 512 * 1024 * 1024,
      timeSeconds: 120,
    }),
    MORIA_RECOVERY_STAGING_ROOT: staging,
    MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT: scratch,
  };
  const run = async (args: string[], value: unknown, env: Record<string, string | undefined>) => {
    const { spawn } = await import('node:child_process');
    return await new Promise<any>((resolve, reject) => {
      const child = spawn(runtime, args, { env, stdio: ['pipe', 'pipe', 'pipe'] });
      let out = '',
        error = '';
      const timer = setTimeout(() => {
        child.kill('SIGKILL');
        reject(new Error('managed recovery command deadline'));
      }, 150000);
      child.stdout.on('data', (b) => (out += b));
      child.stderr.on('data', (b) => (error = (error + b).slice(-65536)));
      child.stdin.on('error', () => {});
      child.once('error', (error) => {
        clearTimeout(timer);
        reject(error);
      });
      child.once('exit', (code) => {
        clearTimeout(timer);
        code === 0 ? resolve(JSON.parse(out)) : reject(new Error(`${args.join('/')}: ${error}`));
      });
      child.stdin.end(JSON.stringify(value));
    });
  };
  const pools: pg.Pool[] = [];
  try {
    let producer = db.pool,
      producerEnv: Record<string, string | undefined> = {
        ...common,
        [reference]: admin(),
        MORIA_NATIVE_STORAGE_BINDINGS: JSON.stringify([
          ...input.bindings,
          posix(fsNamespace, join(db.root, 'filesystem')),
          backup,
        ]),
      };
    let priorHead: any, priorBinding: any;
    const read = async (
      pool: pg.Pool,
      env: Record<string, string | undefined>,
      head: any,
      token: string,
    ) => {
      const auth = new Authority(new Catalog(pool, 'verify'));
      const state = await auth.current(environment, principal, application);
      const tokenHash = createHash('sha256').update(token).digest('hex');
      const spec = {
        version: 1,
        domain: 'sync',
        release: 'a'.repeat(64),
        resourceClass: 'standard',
        timeoutSeconds: 120,
        retryPolicy: 'idempotent',
        inputSchema: { type: 'object', properties: {} },
        pinnedInputs: [{ resource: source, revision: head.revision, snapshot: head.snapshotId }],
        expectedHeads: {},
      };
      const binding = (
        await pool.query(
          'INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation) SELECT $1,$2,$3,$4,$5,$7,$6,s.security_epoch,i.incarnation FROM moria.instance_state s,moria.indexing_instance i RETURNING id',
          [
            environment,
            source,
            spec,
            { environment, principal, application, sessionHash: tokenHash },
            [`${source}:read`, 'filesystem:read'],
            state.epoch,
            input.markings.slice(0, 1),
          ],
        )
      ).rows[0].id;
      const fixture = {
        root: db.root,
        token,
        binding,
        mode: 'read',
        snapshots: [
          { ...snapshot, revision: datasetNamespace.revision, snapshotId: head.snapshotId },
        ],
        nativeRef: head.metadataLocation,
        expectedMarkings: input.markings.slice(0, 1),
      };
      const output = await promisify(execFile)(
        harness,
        ['--ignored', '--exact', 'catalog_publication', '--nocapture', '--test-threads=1'],
        {
          env: { ...env, MORIA_NATIVE_CATALOG_FIXTURE: JSON.stringify(fixture) },
          timeout: 120000,
          maxBuffer: 1024 * 1024,
        },
      );
      assert.match(output.stdout, /1 passed; 0 failed; 0 ignored;/);
      assert.match(output.stdout, /"event":"read"/);
      const execution = (
        await pool.query('SELECT id FROM moria.runtime_executions WHERE binding=$1', [binding])
      ).rows[0].id;
      await run(['execution', 'cancel'], { id: execution }, env);
    };
    for (let round = 0; round < 2; round++) {
      const catalog = new Catalog(producer, 'verify');
      priorHead = (await catalog.heads(environment, [source]))[0];
      priorBinding = (
        await producer.query(
          'SELECT revision,columns,iceberg_schema,namespace_revision,source_digest,physical_binding_fingerprint FROM moria.native_dataset_bindings WHERE environment=$1 AND resource=$2',
          [environment, source],
        )
      ).rows[0];
      const captureConnection = new URL(producerEnv[reference]!);
      captureConnection.username = db.pool.options.user!;
      captureConnection.password = db.pool.options.password as string;
      const capture = await run(
        ['recovery', 'capture'],
        { requestKey: `managed-${round}`, owner: randomUUID(), retentionSeconds: 86400 },
        { ...producerEnv, [reference]: captureConnection.href },
      );
      const name = `recovery_${randomUUID().replaceAll('-', '')}`;
      await db.pool.query(`CREATE DATABASE ${name}`);
      const pool = new pg.Pool({ connectionString: admin(name) });
      pools.push(pool);
      const dataRoot = join(db.root, `recovery-data-${round}`),
        fsRoot = join(db.root, `recovery-fs-${round}`),
        targetLocal = join(db.root, `recovery-local-${round}`);
      for (const path of [dataRoot, fsRoot, join(targetLocal, 'auth')])
        await mkdir(path, { recursive: true, mode: 0o700 });
      const targetEnv = {
        ...common,
        [reference]: admin(name),
        MORIA_LOCAL_DIR: targetLocal,
        MORIA_RECOVERY_KEYS: JSON.stringify({
          'session-encryption': join(targetLocal, 'auth/session-key'),
        }),
        MORIA_RECOVERY_RESTORE_KEYS: JSON.stringify({
          'session-encryption': join(targetLocal, 'auth/session-key'),
        }),
        MORIA_NATIVE_STORAGE_BINDINGS: JSON.stringify([
          posix(datasetNamespace, dataRoot),
          posix(fsNamespace, fsRoot),
          backup,
        ]),
      };
      const restored = await run(
        ['restore-from'],
        {
          restore: randomUUID(),
          manifest: capture.manifest,
          targetNamespaces: [datasetNamespace, fsNamespace],
        },
        targetEnv,
      );
      assert.equal(restored.dispatchEnabled, true);
      const sourceStore = (
        await pool.query(
          'SELECT s.status,s.row_count::text,s.key_count::text,a.store FROM moria.native_source_row_stores s JOIN moria.native_source_generation_aliases a ON a.store=s.id WHERE a.environment=$1 AND a.object_type=$2 AND a.revision=$3',
          [environment, input.sourceStore.target, input.sourceStore.revision],
        )
      ).rows[0];
      assert.equal(sourceStore.store, input.sourceStore.stage);
      assert.equal(sourceStore.status, 'published');
      assert.equal(sourceStore.row_count, String(input.rows.length));
      assert.equal(sourceStore.key_count, String(input.rows.length));
      const restoredSource = (
        await pool.query(
          'SELECT source_value FROM moria.runtime_object_base($1,$2,$3) ORDER BY uid',
          [environment, input.sourceStore.target, input.sourceStore.revision],
        )
      ).rows.map((row) => row.source_value);
      assert.deepEqual(restoredSource, input.rows);

      const binding = (
        await pool.query(
          'SELECT revision,columns,iceberg_schema,namespace_revision,source_digest,physical_binding_fingerprint FROM moria.native_dataset_bindings WHERE environment=$1 AND resource=$2',
          [environment, source],
        )
      ).rows[0];
      assert.notEqual(binding.revision, priorBinding.revision);
      assert.equal(
        binding.physical_binding_fingerprint,
        fingerprint({ backend: 'local', root: dataRoot }),
      );
      for (const field of ['columns', 'iceberg_schema', 'namespace_revision', 'source_digest'])
        assert.deepEqual(binding[field], priorBinding[field]);
      assert.equal(
        (
          await pool.query(
            'SELECT count(*) FROM moria.snapshot_history WHERE environment=$1 AND dataset=$2',
            [environment, source],
          )
        ).rows[0].count,
        String(round + 1),
      );
      const cloneAuthority = new Authority(new Catalog(pool, 'verify'));
      await cloneAuthority.syncPrincipal(environment, 'recovery-operator', principal, {
        kind: 'workload',
        enabled: true,
        attributes: {},
        markings: input.markings,
      });
      await cloneAuthority.setApplication(
        environment,
        'recovery-operator',
        application,
        operations,
      );
      await cloneAuthority.setGrants(
        environment,
        'recovery-operator',
        principal,
        application,
        operations,
      );
      await pool.query('UPDATE moria.instance_principals SET enabled=true');
      const issued = await new WorkloadSessions(cloneAuthority).mint(
        { environment, principal, application },
        'recovery-operator',
      );
      const ordinary = new URL(input.connection);
      ordinary.pathname = `/${name}`;
      const workerEnv = { ...targetEnv, [reference]: ordinary.href };
      await read(pool, workerEnv, priorHead, issued.token);
      if (round === 1) {
        const old = (
          await pool.query(
            'SELECT revision::text AS revision,snapshot_id AS "snapshotId",metadata_location AS "metadataLocation" FROM moria.snapshot_history WHERE environment=$1 AND dataset=$2 ORDER BY moria.snapshot_history.revision LIMIT 1',
            [environment, source],
          )
        ).rows[0];
        await read(pool, workerEnv, old, issued.token);
        break;
      }
      const head = (await new Catalog(pool, 'verify').heads(environment, [source]))[0],
        state = await cloneAuthority.current(environment, principal, application),
        tokenHash = createHash('sha256').update(issued.token).digest('hex');
      const spec = {
        version: 1,
        domain: 'sync',
        release: 'a'.repeat(64),
        resourceClass: 'standard',
        timeoutSeconds: 120,
        retryPolicy: 'idempotent',
        inputSchema: { type: 'object', properties: {} },
        pinnedInputs: [],
        expectedHeads: { [source]: head.revision },
      };
      const bindingId = (
        await pool.query(
          'INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation) SELECT $1,$2,$3,$4,$5,$7,$6,s.security_epoch,i.incarnation FROM moria.instance_state s,moria.indexing_instance i RETURNING id',
          [
            environment,
            source,
            spec,
            { environment, principal, application, sessionHash: tokenHash },
            [`${source}:read`, `${source}:write`, 'filesystem:read'],
            state.epoch,
            input.markings.slice(0, 1),
          ],
        )
      ).rows[0].id;
      const output = await promisify(execFile)(
        harness,
        ['--ignored', '--exact', 'catalog_publication', '--nocapture', '--test-threads=1'],
        {
          env: {
            ...workerEnv,
            MORIA_NATIVE_CATALOG_FIXTURE: JSON.stringify({
              root: dataRoot,
              token: issued.token,
              binding: bindingId,
              mode: 'writer-supplied',
              snapshots: [{ ...snapshot, revision: datasetNamespace.revision }],
              writerRows: { [source]: input.rows },
            }),
          },
          timeout: 120000,
          maxBuffer: 1024 * 1024,
        },
      );
      assert.match(output.stdout, /1 passed; 0 failed; 0 ignored;/);
      const next = (await new Catalog(pool, 'verify').heads(environment, [source]))[0];
      assert.ok(BigInt(next.revision) > BigInt(head.revision));
      assert.notEqual(next.snapshotId, head.snapshotId);
      producer = pool;
      producerEnv = targetEnv;
    }
  } finally {
    await Promise.all(pools.map((pool) => pool.end()));
  }
}
