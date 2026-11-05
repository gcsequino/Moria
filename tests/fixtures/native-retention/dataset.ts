import assert from 'node:assert/strict';
import { spawn, execFile, type ChildProcess } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { mkdir, mkdtemp } from 'node:fs/promises';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import { PutBucketVersioningCommand } from '@aws-sdk/client-s3';
import { database } from '../catalog-storage/database.js';
import { startS3 } from '../storage-s3/server.js';
import { assertNativeArtifacts } from '../native-common/artifact.js';
import { publishNativeSource } from '../native-source/setup.js';
import { Authority } from '../../../services/security/src/authority.js';
import { WorkloadSessions } from '../../../services/security/src/sessions.js';
import { configuredFilesystem } from '../../../services/filesystem/src/local.js';
import { filesystemClient } from '../../../packages/platform/src/filesystem.js';
import { createTransport } from '../../../packages/platform/src/transport.js';

export async function managedRetentionDataset(
  options: {
    primaryKey?: string[];
    columns?: Record<string, unknown>;
    writeModes?: string[];
    wrapS3?: (
      source: Awaited<ReturnType<typeof startS3>>,
    ) => Promise<Awaited<ReturnType<typeof startS3>>>;
  } = {},
) {
  const runtime = process.env.MORIA_RUNTIME_BINARY ?? '/usr/local/bin/moria-runtime';
  const harness =
    process.env.MORIA_CATALOG_TEST_BINARY ?? '/usr/local/bin/moria-native-catalog-test';
  await assertNativeArtifacts(runtime, [harness]);
  const db = await database();
  let s3 = await startS3();
  try {
    if (options.wrapS3) s3 = await options.wrapS3(s3);
  } catch (error) {
    await s3.close();
    await db.close();
    throw error;
  }
  const diagnostics: string[] = [];
  const children = new Set<ChildProcess>(),
    exits: Promise<unknown>[] = [];
  const close = async () => {
    for (const child of children) child.kill('SIGKILL');
    await Promise.allSettled(exits);
    await s3.close();
    await db.close();
  };
  try {
    await s3.client.send(
      new PutBucketVersioningCommand({
        Bucket: s3.bucket,
        VersioningConfiguration: { Status: 'Enabled' },
      }),
    );
    const environment = randomUUID(),
      cell = randomUUID(),
      principal = 'retention-operator',
      application = 'retention-fixture';
    db.pool.options.options = `-c moria.cell_id=${cell}`;
    await db.pool.query("SELECT set_config('moria.cell_id',$1,false)", [cell]);
    await db.catalog.ensureEnvironment(environment);
    const authority = new Authority(db.catalog),
      sessions = new WorkloadSessions(authority);
    const operations = [
      'read',
      'list',
      'create',
      'write',
      'move',
      'delete',
      'export',
      'administer',
      'register',
      'publish',
    ]
      .map((op) => 'filesystem:' + op)
      .concat('instance:manage');
    await authority.syncPrincipal(environment, 'fixture', principal, {
      kind: 'workload',
      enabled: true,
      markings: ['private'],
      attributes: {},
    });
    await authority.setApplication(environment, 'fixture', application, operations);
    await authority.setGrants(environment, 'fixture', principal, application, operations);
    const token = (await sessions.mint({ environment, principal, application }, 'fixture')).token;
    const fsRoot = join(db.root, 'filesystem'),
      scratch = join(db.root, 'scratch');
    await mkdir(fsRoot, { recursive: true });
    await mkdir(scratch, { recursive: true, mode: 0o700 });
    const fs = await configuredFilesystem(db.catalog, fsRoot);
    await fs.management.ensureHome({ session: await sessions.authenticate('Bearer ' + token) });
    await db.pool.query(
      'INSERT INTO moria.runtime_cells(id,endpoint_ref,incarnation) SELECT $1,$2,incarnation FROM moria.indexing_instance',
      [cell, 'RETENTION_CELL'],
    );
    await db.pool.query(
      'INSERT INTO moria.runtime_placements(environment,cell,incarnation,serving,leases_fenced) SELECT $1,$2,incarnation,true,true FROM moria.indexing_instance',
      [environment, cell],
    );
    const role = 'retention_' + randomUUID().replaceAll('-', ''),
      password = randomUUID();
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(`ALTER ROLE ${role} SET temp_file_limit='256MB'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role};GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role};GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    const connection = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const namespace = { warehouseId: 'retention-data', tableId: 'records', revision: 'v1' },
      backupNamespace = { warehouseId: 'retention-backup', tableId: 'sets', revision: 'v1' },
      icebergUuid = randomUUID();
    const s3Binding = (space: typeof namespace, prefix: string) => ({
      warehouseId: space.warehouseId,
      tableId: space.tableId,
      endpoint: s3.endpoint,
      bucket: s3.bucket,
      prefix,
      region: 'us-east-1',
      accessKeySecretRef: 'RETENTION_S3_ACCESS',
      secretKeySecretRef: 'RETENTION_S3_SECRET',
      sessionTokenSecretRef: null,
      limits: { maxObjectBytes: 128 * 1024 * 1024, chunkBytes: 1024 * 1024 },
    });
    const datasetStorage = s3Binding(namespace, 'datasets'),
      backupStorage = s3Binding(backupNamespace, 'backups');
    const storageBindings = [
      { namespace, provider: { kind: 's3', binding: datasetStorage } },
      { namespace: backupNamespace, provider: { kind: 's3', binding: backupStorage } },
      {
        namespace: { warehouseId: 'filesystem', tableId: 'content', revision: 'v1' },
        provider: {
          kind: 'posix',
          root: fsRoot,
          limits: { maxObjectBytes: 128 * 1024 * 1024, chunkBytes: 1024 * 1024 },
        },
      },
    ];
    const nativeEnv: NodeJS.ProcessEnv = {
      PATH: process.env.PATH,
      MORIA_INDEXING_DATABASE_SECRET_REF: 'RETENTION_DATABASE',
      RETENTION_DATABASE: connection,
      MORIA_INDEXING_DATABASE_TLS: 'disable',
      MORIA_NATIVE_CELL_ID: cell,
      MORIA_NATIVE_FILESYSTEM_STORAGE_REVISION: 'v1',
      MORIA_NATIVE_STORAGE_BINDINGS: JSON.stringify(storageBindings),
      MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT: scratch,
      MORIA_NATIVE_CLOSURE_LIMITS: JSON.stringify({
        maxEntries: 10000,
        maxReferencedBytes: 1024 * 1024 * 1024,
        maxSpoolBytes: 32 * 1024 * 1024,
      }),
      RETENTION_S3_ACCESS: s3.credentials.accessKeyId,
      RETENTION_S3_SECRET: s3.credentials.secretAccessKey,
    };
    const operatorEnv = {
      ...nativeEnv,
      RETENTION_DATABASE: `postgresql://${db.pool.options.user}:${encodeURIComponent(db.pool.options.password as string)}@127.0.0.1:${db.pool.options.port}/postgres`,
    };
    const command = async (args: string[], input: unknown, env: NodeJS.ProcessEnv = nativeEnv) => {
      const child = spawn(runtime, args, { env, stdio: ['pipe', 'pipe', 'pipe'] });
      children.add(child);
      const done = new Promise<any>((resolve, reject) => {
        let out = '',
          error = '',
          overflow = false;
        const deadline = setTimeout(() => child.kill('SIGKILL'), 150000);
        child.stdout.on('data', (part) => {
          out += part;
          if (out.length > 1024 * 1024) {
            overflow = true;
            child.kill('SIGKILL');
          }
        });
        child.stderr.on('data', (part) => {
          error = (error + part).slice(-65536);
        });
        child.stdin.on('error', () => {});
        child.once('error', (failure) => {
          clearTimeout(deadline);
          reject(failure);
        });
        child.once('exit', (code) => {
          clearTimeout(deadline);
          children.delete(child);
          if (overflow) reject(new Error('native fixture output limit'));
          else if (code !== 0) reject(new Error(args.join('/') + ': ' + error));
          else {
            try {
              resolve(JSON.parse(out));
            } catch (failure) {
              reject(failure);
            }
          }
        });
      });
      exits.push(done);
      void done.catch(() => {});
      child.stdin.end(JSON.stringify(input));
      return await done;
    };
    const start = async (profiles: unknown[] = []) => {
      const child = spawn(runtime, ['serve'], {
        env: {
          ...nativeEnv,
          MORIA_NATIVE_SOURCE_PROFILES: JSON.stringify(profiles),
          MORIA_NATIVE_LISTEN: '127.0.0.1:0',
        },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      children.add(child);
      const exited = new Promise<void>((resolve) =>
        child.once('exit', () => {
          children.delete(child);
          resolve();
        }),
      );
      exits.push(exited);
      let out = '',
        error = '';
      child.stdout.on('data', (part) => {
        out = (out + part).slice(-65536);
      });
      child.stderr.on('data', (part) => {
        error = (error + part).slice(-65536);
        diagnostics.push(String(part).slice(-4096));
        if (diagnostics.length > 16) diagnostics.shift();
      });
      for (let i = 0; i < 500; i++) {
        for (const line of out.split('\n')) {
          try {
            const value = JSON.parse(line);
            if (value.event === 'serving') return { url: 'http://' + value.address, child, exited };
          } catch {}
        }
        if (child.exitCode !== null) throw new Error(error);
        await delay(10);
      }
      throw new Error('native retention Source startup deadline');
    };
    await command(['filesystem', 'configure-storage'], {});
    const initial = await start();
    const compilerContext = await fetch(initial.url + '/api/management/v1/fs/compiler-context', {
      method: 'POST',
      headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json' },
      body: JSON.stringify({ principalAttributes: {} }),
    });
    assert.equal(compilerContext.status, 200, await compilerContext.text());
    const columns = options.columns ?? {
      id: { type: 'uid', nullable: false },
      name: { type: 'string', nullable: false },
      tags: { type: 'array', nullable: false, items: { type: 'string', nullable: false } },
    };
    const published = await publishNativeSource(initial.url, token, [
      {
        path: '.ontology.json',
        text: JSON.stringify({ version: 1, name: 'Retention Dataset', namespace: 'retention' }),
      },
      {
        path: 'records.resource.json',
        text: JSON.stringify({
          apiVersion: 'moria/v1alpha2',
          kind: 'Dataset',
          metadata: { namespace: 'retention', name: 'records' },
          spec: {
            columns,
            writeModes: options.writeModes ?? ['replace'],
            ...(options.primaryKey ? { primaryKey: options.primaryKey } : {}),
          },
        }),
      },
    ]);
    const dataset = published.resource('retention/Dataset/records');
    const finalOperations = [...operations, `${dataset}:read`, `${dataset}:write`];
    await authority.setApplication(environment, 'fixture', application, finalOperations);
    await authority.setGrants(environment, 'fixture', principal, application, finalOperations);
    initial.child.kill('SIGTERM');
    await initial.exited;
    const serving = await start([
      {
        environment,
        projectId: published.role.projectId,
        reference: 'retention/Dataset/records',
        configuration: { namespace, icebergUuid, markings: ['private'] },
      },
    ]);
    const client = filesystemClient(
      createTransport({
        baseUrl: serving.url,
        fetch: (url, init) =>
          fetch(url, { ...init, headers: { ...init?.headers, authorization: 'Bearer ' + token } }),
      }),
    );
    const plan = await client.plan(published.root.id, published.publication.digest),
      applied = (await client.apply(plan.id)) as any;
    assert.equal(applied.runtimeConfigured, true);
    assert.equal((await db.catalog.heads(environment, [dataset]))[0].revision, '0');
    const context = {
      environment,
      principal,
      application,
      sessionHash: createHash('sha256').update(token).digest('hex'),
    };
    const bind = async (pinnedInputs: unknown[], expectedHeads: Record<string, string>) => {
      const state = await authority.current(environment, principal, application);
      const spec = {
        version: 1,
        domain: 'sync',
        release: 'a'.repeat(64),
        resourceClass: 'standard',
        timeoutSeconds: 120,
        retryPolicy: 'idempotent',
        inputSchema: { type: 'object', properties: {} },
        pinnedInputs,
        expectedHeads,
      };
      return (
        await db.pool.query(
          "INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation) SELECT $1,$2,$3,$4,$5,ARRAY['private'],$6,s.security_epoch,i.incarnation FROM moria.instance_state s,moria.indexing_instance i RETURNING id",
          [
            environment,
            dataset,
            spec,
            context,
            [`${dataset}:read`, `${dataset}:write`, 'filesystem:read'],
            state.epoch,
          ],
        )
      ).rows[0].id as string;
    };
    const snapshot = {
      resource: dataset,
      warehouseId: namespace.warehouseId,
      tableId: namespace.tableId,
      revision: namespace.revision,
      tableUuid: icebergUuid,
      metadata: 'unused',
      snapshotId: '0',
    };
    const invoke = async (fixture: unknown) => {
      const output = await promisify(execFile)(
        harness,
        ['--ignored', '--exact', 'catalog_publication', '--nocapture', '--test-threads=1'],
        {
          env: {
            ...nativeEnv,
            // Independent harness processes must not share the privileged engine's private cache.
            MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT: await mkdtemp(join(db.root, 'catalog-scratch-')),
            MORIA_NATIVE_CATALOG_FIXTURE: JSON.stringify(fixture),
          },
          timeout: 120000,
          maxBuffer: 1024 * 1024,
        },
      );
      assert.match(output.stdout, /1 passed; 0 failed; 0 ignored;/);
      return output.stdout;
    };
    const writtenRows = new Map<string, Record<string, unknown>[]>();
    const publish = async (rows: Record<string, unknown>[], format: { graph?: boolean } = {}) => {
      const prior = (await db.catalog.heads(environment, [dataset]))[0],
        binding = await bind([], { [dataset]: prior.revision });
      await invoke({
        root: db.root,
        token,
        binding,
        mode: 'writer-supplied',
        graphOutput: format.graph ?? false,
        snapshots: [snapshot],
        writerRows: { [dataset]: rows },
        s3Bindings: [datasetStorage],
      });
      const head = (await db.catalog.heads(environment, [dataset]))[0];
      assert.ok(BigInt(head.revision) > BigInt(prior.revision));
      writtenRows.set(head.snapshotId!, structuredClone(rows));
      return {
        head,
        binding,
        execution: (
          await db.pool.query('SELECT id FROM moria.runtime_executions WHERE binding=$1', [binding])
        ).rows[0].id as string,
      };
    };
    const read = async (
      head: { revision: string; snapshotId: string | null; metadataLocation: string | null },
      cancel = true,
      expected = writtenRows.get(head.snapshotId!),
    ) => {
      const binding = await bind(
        [{ resource: dataset, revision: head.revision, snapshot: head.snapshotId }],
        {},
      );
      const out = await invoke({
        root: db.root,
        token,
        binding,
        mode: 'read',
        expectedRows: expected?.length,
        writerRows: { [dataset]: expected },
        snapshots: [{ ...snapshot, snapshotId: head.snapshotId }],
        nativeRef: head.metadataLocation,
        s3Bindings: [datasetStorage],
      });
      assert.match(out, /"event":"read"/);
      const execution = (
        await db.pool.query('SELECT id FROM moria.runtime_executions WHERE binding=$1', [binding])
      ).rows[0].id as string;
      if (cancel) await command(['execution', 'cancel'], { id: execution });
      return { binding, execution, out };
    };
    return {
      baseUrl: serving.url,
      db,
      s3,
      environment,
      principal,
      application,
      cell,
      token,
      namespace,
      backupNamespace,
      datasetStorage,
      backupStorage,
      storageBindings,
      nativeEnv,
      operatorEnv,
      runtime,
      harness,
      columns,
      icebergUuid,
      dataset,
      fsRoot,
      scratch,
      published,
      command,
      bind,
      invoke,
      snapshot,
      publish,
      read,
      close,
    };
  } catch (error) {
    await close();
    throw new Error(
      `${error instanceof Error ? error.message : String(error)}; native diagnostics: ${diagnostics.join('')}`,
      { cause: error },
    );
  }
}
