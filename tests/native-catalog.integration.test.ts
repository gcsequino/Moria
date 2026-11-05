import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { spawn, execFile, type ChildProcess } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { database } from './fixtures/catalog-storage/database.js';
import { requestCommitProxy } from './fixtures/runtime/postgres-proxy.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { startS3 } from './fixtures/storage-s3/server.js';
import { HeadObjectCommand } from '@aws-sdk/client-s3';
import { assertNativeArtifacts } from './fixtures/native-common/artifact.js';

test(
  'native Dataset publication prepares protected Iceberg closure pages and atomically commits domain execution',
  { timeout: 300000 },
  async (t) => {
    await assertNativeArtifacts(
      process.env.MORIA_RUNTIME_BINARY ??
        process.env.MORIA_NATIVE_BINARY ??
        '/usr/local/bin/moria-runtime',
      [process.env.MORIA_CATALOG_TEST_BINARY ?? '/usr/local/bin/moria-catalog-tests'],
    );
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
    const environment = randomUUID(),
      outputs = [randomUUID(), randomUUID()],
      input = randomUUID();
    t.afterEach(async () => {
      await db.pool.query(
        "UPDATE moria.runtime_executions SET status='cancelled' WHERE environment=$1 AND status IN ('queued','running','preparing')",
        [environment],
      );
    });
    const root = join(db.root, 'native'),
      readScratch = join(db.root, 'read-scratch');
    await mkdir(root);
    await mkdir(readScratch);
    await db.catalog.ensureEnvironment(environment);
    const authority = new Authority(db.catalog),
      sessions = new WorkloadSessions(authority);
    const operations = [...outputs.flatMap((id) => [`${id}:read`, `${id}:write`]), `${input}:read`];
    await authority.syncPrincipal(environment, 'fixture', 'pipeline', {
      enabled: true,
      kind: 'workload',
      attributes: {},
      markings: ['private'],
    });
    await authority.setApplication(environment, 'fixture', 'native-catalog', operations);
    await authority.setGrants(environment, 'fixture', 'pipeline', 'native-catalog', operations);
    const context = { environment, principal: 'pipeline', application: 'native-catalog' };
    const credential = await sessions.mint(context, 'fixture');
    const sessionHash = createHash('sha256').update(credential.token).digest('hex');
    const snapshots: any[] = [];
    for (const [index, resource] of outputs.entries()) {
      await db.catalog.ensureDataset(environment, resource);
      const tableId = `table${index}`,
        physical = join(db.root, tableId);
      await mkdir(physical);
      const logical = `moria://catalogwarehouse/${tableId}`;
      const generated = JSON.parse(
        (
          await promisify(execFile)(resolve('.venv/bin/python'), [
            'tests/fixtures/iceberg-scoped-storage/generate.py',
            physical,
            '--snapshot',
            logical,
          ])
        ).stdout,
      );
      const metadata = `${logical}/metadata/root.metadata.json`;
      snapshots.push({
        ...generated,
        metadata,
        objects: { ...generated.objects, [metadata]: generated.metadata },
        resource,
        warehouseId: 'catalogwarehouse',
        tableId,
      });
      await db.pool.query(
        "INSERT INTO moria.native_storage_namespaces(environment,warehouse_id,table_id,resource,revision,purpose,iceberg_uuid,markings) VALUES($1,'catalogwarehouse',$2,$3,'v1','dataset-source',$4,ARRAY['private'])",
        [environment, tableId, resource, generated.tableUuid],
      );
    }
    await db.catalog.ensureDataset(environment, input);
    await db.pool.query(
      "INSERT INTO moria.snapshot_history VALUES($1,$2,1,'fixture','input-snapshot','fixture','{}')",
      [environment, input],
    );
    await db.pool.query(
      "INSERT INTO moria.native_storage_namespaces(environment,warehouse_id,table_id,resource,revision,purpose,iceberg_uuid,markings) VALUES($1,'inputwarehouse','inputtable',$2,'v1','dataset-source',$3,ARRAY['private'])",
      [environment, input, randomUUID()],
    );
    const role = `catalog_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(`ALTER ROLE ${role} SET track_functions='all'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role}; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    const connection = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const binding = async (
      changes: Record<string, unknown> = {},
      boundOperations = [`${input}:read`],
    ) => {
      const heads = await db.catalog.heads(environment, outputs);
      const spec = {
        version: 1,
        domain: 'sync',
        release: 'a'.repeat(64),
        resourceClass: 'standard',
        timeoutSeconds: 240,
        retryPolicy: 'idempotent',
        inputSchema: { type: 'object', properties: {} },
        pinnedInputs: [{ resource: input, revision: '1', snapshot: 'input-snapshot' }],
        expectedHeads: Object.fromEntries(heads.map((h) => [h.dataset, h.revision])),
        ...changes,
      };
      const epoch = (await authority.current(environment, 'pipeline', 'native-catalog')).epoch;
      const id = (
        await db.pool.query(
          `INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation)
      SELECT $1,$2,$3,$4,$5,ARRAY['private'],$6,i.security_epoch,c.incarnation FROM moria.instance_state i,moria.indexing_instance c RETURNING id`,
          [environment, outputs[0], spec, { ...context, sessionHash }, boundOperations, epoch],
        )
      ).rows[0].id;
      await db.pool.query(
        "INSERT INTO moria.runtime_snapshot_checkpoint_bindings VALUES($1,'connection','stream','v1')",
        [id],
      );
      return id;
    };
    const checkpoint = {
      connection: 'connection',
      stream: 'stream',
      configRevision: 'v1',
      expectedVersion: '0',
      value: { offset: '42' },
    };
    const start = (fixture: Record<string, unknown>, url = connection) => {
      const selected = (fixture.snapshots ?? snapshots) as typeof snapshots;
      const s3Bindings = (fixture.s3Bindings ?? []) as any[];
      const nativeBindings = selected.map((snapshot) => ({
        namespace: {
          warehouseId: snapshot.warehouseId,
          tableId: snapshot.tableId,
          revision: snapshot.revision ?? 'v1',
        },
        provider: s3Bindings.some(
          (binding) =>
            binding.warehouseId === snapshot.warehouseId && binding.tableId === snapshot.tableId,
        )
          ? {
              kind: 's3',
              binding: s3Bindings.find(
                (binding) =>
                  binding.warehouseId === snapshot.warehouseId &&
                  binding.tableId === snapshot.tableId,
              ),
            }
          : {
              kind: 'posix',
              root,
              limits: { maxObjectBytes: 128 * 1024 * 1024, chunkBytes: 1024 * 1024 },
            },
      }));
      const child = spawn(
        process.env.MORIA_CATALOG_TEST_BINARY ?? '/usr/local/bin/moria-catalog-tests',
        ['--ignored', '--exact', 'catalog_publication', '--nocapture', '--test-threads=1'],
        {
          env: {
            PATH: '/usr/local/bin:/usr/bin:/bin',
            MORIA_INDEXING_DATABASE_SECRET_REF: 'CATALOG_FIXTURE_DATABASE',
            CATALOG_FIXTURE_DATABASE: url,
            MORIA_INDEXING_DATABASE_TLS: 'disable',
            MORIA_DATABASE_POOL_MAX: '4',
            CATALOG_S3_ACCESS: s3?.credentials.accessKeyId,
            CATALOG_S3_SECRET: s3?.credentials.secretAccessKey,
            MORIA_NATIVE_STORAGE_BINDINGS: JSON.stringify(nativeBindings),
            MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT: readScratch,
            MORIA_NATIVE_CLOSURE_LIMITS: JSON.stringify({
              maxEntries: 10000,
              maxReferencedBytes: 1024 * 1024 * 1024,
              maxSpoolBytes: 32 * 1024 * 1024,
            }),
            MORIA_NATIVE_CATALOG_FIXTURE: JSON.stringify({
              root,
              token: credential.token,
              snapshots,
              ...fixture,
            }),
          },
          stdio: ['pipe', 'pipe', 'pipe'],
        },
      );
      children.add(child);
      let output = '',
        errors = '';
      child.stdout.on('data', (bytes) => (output += bytes.toString()));
      child.stderr.on('data', (bytes) => (errors += bytes.toString()));
      child.stdin!.on('error', () => {});
      const done = new Promise<string>((resolve, reject) => {
        child.once('error', (error) => {
          children.delete(child);
          reject(error);
        });
        child.once('exit', (code, signal) => {
          children.delete(child);
          if (code !== 0)
            reject(new Error(`Native catalog exited ${code ?? signal}: ${errors}\n${output}`));
          else if (!/test result: ok\. 1 passed; 0 failed; 0 ignored;/.test(output))
            reject(new Error(`Native catalog did not execute exactly one test: ${output}`));
          else resolve(output);
        });
      });
      void done.catch(() => {});
      const event = async (name: string): Promise<any> => {
        const attempts = process.env.MORIA_CATALOG_DIAGNOSTIC_WAIT_MS === '60000' ? 1200 : 300;
        for (let attempt = 0; attempt < attempts; attempt++) {
          const line = output.split('\n').find((line) => line.includes(`"event":"${name}"`));
          if (line) return JSON.parse(line.slice(line.indexOf('{')));
          if (child.exitCode !== null || child.signalCode !== null)
            throw new Error(`Native catalog exited before ${name}: ${errors}\n${output}`);
          await delay(50);
        }
        const activity = await db.pool.query(
          `SELECT state,wait_event_type,wait_event,pg_blocking_pids(pid) AS blockers
           FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()`,
        );
        throw new Error(
          `Native catalog event ${name} timed out: ${JSON.stringify(activity.rows)}\n${errors}\n${output}`,
        );
      };
      return {
        child,
        done,
        event,
        phases: () =>
          output.split('\n').flatMap((line) => {
            if (
              !line.includes('"event":"closure_hashed"') &&
              !line.includes('"event":"candidate_registered"')
            )
              return [];
            return [JSON.parse(line.slice(line.indexOf('{')))];
          }),
      };
    };
    const heads = () => db.catalog.heads(environment, outputs);
    const assertUnchanged = async (before: unknown, execution: string) => {
      assert.deepEqual(await heads(), before);
      assert.equal(
        (
          await db.pool.query(
            'SELECT count(*) FROM moria.publications WHERE environment=$1 AND execution=$2',
            [environment, execution],
          )
        ).rows[0].count,
        '0',
      );
      assert.notEqual(
        (
          await db.pool.query('SELECT status FROM moria.runtime_executions WHERE id=$1', [
            execution,
          ])
        ).rows[0].status,
        'succeeded',
      );
    };
    await t.test(
      'reader-local byte windows bound actual SQL guard and provider reads and cannot seal after revocation',
      async () => {
        const before = await heads();
        const run = start({
          binding: await binding(),
          mode: 'reader-window',
          snapshots: [snapshots[0]],
        });
        const ready = await run.event('reader_window_ready');
        const reset = async () =>
          db.pool.query(`SELECT pg_stat_reset_single_function_counters(p.oid)
          FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
          WHERE n.nspname='moria' AND p.proname IN ('runtime_dataset_io_check','runtime_dataset_object_read')`);
        const counts = async () => {
          await db.pool.query('SELECT pg_stat_clear_snapshot()');
          return (
            await db.pool.query(`SELECT funcname,calls::text FROM pg_stat_user_functions
            WHERE schemaname='moria' AND funcname IN ('runtime_dataset_io_check','runtime_dataset_object_read') ORDER BY funcname`)
          ).rows;
        };
        await reset();
        run.child.stdin!.write('continue\n');
        const baseline = await run.event('reader_window_baseline');
        const baselineGuards = await counts();
        await reset();
        run.child.stdin!.write('continue\n');
        const buffered = await run.event('reader_window_buffered');
        const windowGuards = await counts();
        assert.equal(baseline.counts.providerRanges, ready.decoderReads);
        assert.equal(baseline.counts.providerVersions, ready.decoderReads);
        assert.equal(buffered.counts.providerRanges, 1);
        assert.equal(buffered.counts.providerVersions, 1);
        for (const functionName of ['runtime_dataset_io_check', 'runtime_dataset_object_read']) {
          const raw = Number(
            baselineGuards.find((row) => row.funcname === functionName)?.calls ?? 0,
          );
          const bounded = Number(
            windowGuards.find((row) => row.funcname === functionName)?.calls ?? 0,
          );
          assert.ok(raw >= ready.decoderReads, JSON.stringify({ baselineGuards, functionName }));
          assert.ok(bounded > 0 && bounded < 32, JSON.stringify({ windowGuards, functionName }));
        }
        t.diagnostic(
          JSON.stringify({
            event: 'reader_window_actual_counts',
            decoderReads: ready.decoderReads,
            baseline,
            buffered,
            baselineGuards,
            windowGuards,
          }),
        );
        await db.pool.query(
          "UPDATE moria.native_storage_namespaces SET enabled=false WHERE environment=$1 AND warehouse_id='catalogwarehouse' AND table_id=$2",
          [environment, snapshots[0].tableId],
        );
        run.child.stdin!.write('continue\n');
        const denied = await run.event('reader_window_denied');
        await run.done;
        assert.equal(denied.candidateCount, 0);
        await assertUnchanged(before, ready.execution);
        await db.pool.query(
          "UPDATE moria.native_storage_namespaces SET enabled=true,revision='reader-window-restored' WHERE environment=$1 AND warehouse_id='catalogwarehouse' AND table_id=$2",
          [environment, snapshots[0].tableId],
        );
        snapshots[0].revision = 'reader-window-restored';
      },
    );
    await t.test(
      'real protected writes and semantic preparation publish grouped heads, lineage, checkpoint, receipt and completion once',
      async () => {
        const before = await heads(),
          run = start({ binding: await binding(), mode: 'valid', checkpoint });
        const prepared = await run.event('prepared'),
          receipt = await run.event('receipt');
        await run.done;
        assert.equal(receipt.receipt.outputs.length, 2);
        for (const head of await heads()) {
          assert.equal(
            BigInt(head.revision),
            BigInt((before as any[]).find((h) => h.dataset === head.dataset).revision) + 1n,
          );
          assert.match(head.metadataLocation!, /^moria-snapshot-v2:/);
        }
        assert.equal(
          (
            await db.pool.query('SELECT status FROM moria.runtime_executions WHERE id=$1', [
              prepared.lease.execution,
            ])
          ).rows[0].status,
          'succeeded',
        );
        assert.deepEqual(
          (
            await db.pool.query(
              'SELECT lineage FROM moria.snapshot_history WHERE environment=$1 AND execution=$2',
              [environment, prepared.lease.execution],
            )
          ).rows.map((r) => r.lineage),
          [{ [input]: '1' }, { [input]: '1' }],
        );
        assert.deepEqual(
          (
            await db.pool.query(
              'SELECT version::text,value FROM moria.checkpoints WHERE environment=$1',
              [environment],
            )
          ).rows,
          [{ version: '1', value: { offset: '42' } }],
        );
        assert.equal(
          (
            await db.pool.query(
              "SELECT count(*) FROM moria.runtime_snapshot_candidates WHERE execution=$1 AND namespace_state->'inheritedMarkings'='[\"private\"]'::jsonb",
              [prepared.lease.execution],
            )
          ).rows[0].count,
          '2',
        );
      },
    );
    await t.test(
      'a read-only producer consumes the persisted native page chain through private scratch and actual Arrow scan',
      async () => {
        const head = (await heads()).find((h) => h.dataset === outputs[0])!;
        await authority.setGrants(
          environment,
          'fixture',
          'pipeline',
          'native-catalog',
          operations.filter((op) => !op.endsWith(':write')),
        );
        const id = await binding(
          {
            expectedHeads: {},
            pinnedInputs: [
              { resource: outputs[0], revision: head.revision, snapshot: head.snapshotId },
            ],
          },
          [`${outputs[0]}:read`],
        );
        const run = start({
          binding: id,
          mode: 'read',
          snapshots: [snapshots[0]],
          nativeRef: head.metadataLocation,
        });
        assert.equal((await run.event('read')).rows, 300);
        await run.done;
        await authority.setGrants(environment, 'fixture', 'pipeline', 'native-catalog', operations);
      },
    );
    await t.test(
      'pre-admission resolves the current pin while a real leased reader retains an older declared immutable snapshot',
      async () => {
        const old = (await heads()).find((h) => h.dataset === outputs[0])!;
        const publication = start({ binding: await binding(), mode: 'valid' });
        await publication.done;
        const current = (await heads()).find((h) => h.dataset === outputs[0])!;
        assert.ok(BigInt(current.revision) > BigInt(old.revision));
        const resolved = start({
          binding: await binding(),
          mode: 'resolve-pin',
          snapshots: [snapshots[0]],
        });
        const pin = (await resolved.event('resolved')).pin;
        assert.equal(pin.revision, current.revision);
        assert.equal(pin.metadataLocation, current.metadataLocation);
        await resolved.done;
        const id = await binding(
          {
            expectedHeads: {},
            pinnedInputs: [
              { resource: outputs[0], revision: old.revision, snapshot: old.snapshotId },
            ],
          },
          [`${outputs[0]}:read`],
        );
        const read = start({
          binding: id,
          mode: 'read-old',
          snapshots: [snapshots[0]],
          nativeRef: old.metadataLocation,
        });
        assert.deepEqual(await read.event('read'), {
          event: 'read',
          rows: 300,
          revision: old.revision,
        });
        await read.done;
      },
    );
    await t.test('a live producer lease cannot read an undeclared Dataset', async () => {
      const head = (await heads()).find((h) => h.dataset === outputs[0])!;
      const id = await binding({ expectedHeads: {} }, [`${input}:read`]);
      const run = start({
        binding: id,
        mode: 'read-undeclared',
        snapshots: [snapshots[0]],
        nativeRef: head.metadataLocation,
      });
      await assert.rejects(run.done, /UNDECLARED_DATASET/);
    });
    await t.test(
      'Dataset IO validates a matching private execution once and keeps ordinary and mismatched checks',
      async () => {
        const before = await heads();
        const owner = randomUUID(),
          host = randomUUID();
        await db.pool.query(
          `INSERT INTO moria.runtime_workers(owner,host_id,incarnation,resource_classes,domains,expires_at)
         SELECT $1,$2,incarnation,ARRAY['standard'],ARRAY['sync'],clock_timestamp()+interval '1 hour' FROM moria.indexing_instance`,
          [owner, host],
        );
        const run = start({
          binding: await binding(),
          mode: 'hold',
          registeredOwner: owner,
          registeredHost: host,
        });
        const prepared = await run.event('prepared');
        try {
          const privateHash = (
            await db.pool.query(
              'SELECT token_hash FROM moria.runtime_job_sessions WHERE execution=$1 AND task IS NULL',
              [prepared.lease.execution],
            )
          ).rows[0].token_hash;
          const current = (
            await db.pool.query('SELECT moria.native_serving_session($1) AS state', [privateHash])
          ).rows[0].state;
          const ordinary = (
            await db.pool.query('SELECT moria.native_serving_session($1) AS state', [sessionHash])
          ).rows[0].state;
          assert.equal(ordinary.privateValidatedExecution, undefined);
          const args = [
            prepared.lease.execution,
            prepared.lease.owner,
            prepared.lease.fence,
            prepared.lease.incarnation,
            snapshots[0].warehouseId,
            snapshots[0].tableId,
            (
              await db.pool.query(
                'SELECT revision FROM moria.native_storage_namespaces WHERE warehouse_id=$1 AND table_id=$2',
                [snapshots[0].warehouseId, snapshots[0].tableId],
              )
            ).rows[0].revision,
          ];
          const measure = async (hash: string) => {
            const client = await db.pool.connect();
            try {
              await client.query('BEGIN');
              await client.query("SET LOCAL track_functions='all'");
              const calls = async () =>
                Number(
                  (
                    await client.query(
                      "SELECT COALESCE(sum(calls),0)::text AS calls FROM pg_stat_xact_user_functions WHERE funcid='moria.runtime_execution_check(uuid)'::regprocedure::oid",
                    )
                  ).rows[0].calls,
                );
              const before = await calls();
              const state = (
                await client.query(
                  'SELECT moria.runtime_dataset_io_check($1,$2,NULL,$3,$4,$5,$6,$7,$8,false) AS state',
                  [hash, ...args],
                )
              ).rows[0].state;
              assert.equal(
                (await calls()) - before,
                1,
                'one fresh execution validation per IO check',
              );
              if (hash === privateHash) {
                assert.equal(
                  (
                    await db.pool.query(
                      'SELECT id FROM moria.runtime_executions WHERE id=$1 FOR SHARE NOWAIT',
                      [args[0]],
                    )
                  ).rowCount,
                  1,
                  'the live publication IO check permits a concurrent shared reader',
                );
                await assert.rejects(
                  db.pool.query(
                    'SELECT id FROM moria.runtime_executions WHERE id=$1 FOR UPDATE NOWAIT',
                    [args[0]],
                  ),
                  { code: '55P03' },
                );
              }
              return state;
            } finally {
              await client.query('ROLLBACK');
              client.release();
            }
          };
          assert.deepEqual(await measure(privateHash), await measure(sessionHash));
          assert.equal(current.privateValidatedExecution, prepared.lease.execution);
          await assert.rejects(
            db.pool.query(
              'SELECT moria.runtime_dataset_io_check($1,$2,$3,$4,$5,$6,$7,$8,$9,false)',
              [privateHash, args[0], randomUUID(), ...args.slice(1)],
            ),
            /AUTHORIZATION_CHANGED/,
          );
        } finally {
          run.child.kill('SIGKILL');
          await assert.rejects(run.done);
        }
        await assertUnchanged(before, prepared.lease.execution);
      },
    );
    await t.test(
      'a registered Sync producer replays only its exact completed receipt while private IO and caller-forged metadata state stay denied',
      async (t) => {
        const owner = randomUUID(),
          host = randomUUID();
        await db.pool.query(
          `INSERT INTO moria.runtime_workers(owner,host_id,incarnation,resource_classes,domains,expires_at)
          SELECT $1,$2,incarnation,ARRAY['standard'],ARRAY['sync'],clock_timestamp()+interval '1 hour' FROM moria.indexing_instance`,
          [owner, host],
        );
        const run = start({
          binding: await binding(),
          mode: 'valid',
          registeredOwner: owner,
          registeredHost: host,
        });
        const prepared = await run.event('prepared');
        const receipt = await run.event('receipt');
        await run.done;
        t.diagnostic(
          JSON.stringify({ event: 'registered_publication_phases', phases: run.phases() }),
        );
        t.diagnostic(
          JSON.stringify({
            event: 'native_guard_function_counts',
            rows: (
              await db.pool.query(
                `SELECT funcname,calls,total_time,self_time FROM pg_stat_user_functions WHERE schemaname='moria' AND calls>0 ORDER BY total_time DESC LIMIT 24`,
              )
            ).rows,
          }),
        );
        assert.equal(receipt.receipt.outputs.length, 2);
        const privateHash = (
          await db.pool.query(
            'SELECT token_hash FROM moria.runtime_job_sessions WHERE execution=$1 AND task IS NULL',
            [prepared.lease.execution],
          )
        ).rows[0].token_hash;
        await assert.rejects(
          db.pool.query('SELECT moria.native_serving_session($1)', [privateHash]),
          /JOB_LEASE_LOST/,
        );
        await assert.rejects(
          db.pool.query("SELECT moria.native_dataset_receipt_access($1,$2,$3,$4,$5,'read')", [
            privateHash,
            prepared.lease.execution,
            owner,
            prepared.lease.fence,
            input,
          ]),
          /JOB_RECEIPT_AUTHORITY_REQUIRED/,
        );
        const client = await db.pool.connect();
        try {
          for (const query of [
            "SELECT moria.native_serving_resource_access_state('{}'::jsonb,$1,'read')",
            "SELECT moria.runtime_dataset_labels_state('{}'::jsonb,$1)",
          ]) {
            await client.query('BEGIN');
            await client.query(`SET LOCAL ROLE ${role}`);
            await assert.rejects(client.query(query, [outputs[0]]), { code: '42501' });
            await client.query('ROLLBACK');
          }
        } finally {
          await client.query('ROLLBACK');
          client.release();
        }
        const control = await db.pool.connect();
        try {
          await control.query('BEGIN');
          await control.query(
            'UPDATE moria.workload_sessions SET disabled=true WHERE token_hash=$1',
            [sessionHash],
          );
          await assert.rejects(
            control.query('SELECT moria.runtime_snapshot_publish($1,$2,$3,$4,$5,$6,NULL)', [
              privateHash,
              prepared.lease.execution,
              owner,
              prepared.lease.fence,
              prepared.lease.incarnation,
              prepared.candidates,
            ]),
            /UNAUTHENTICATED/,
          );
        } finally {
          await control.query('ROLLBACK');
          control.release();
        }
      },
    );
    await t.test(
      'SIGKILL before authority commit leaves immutable candidates invisible',
      async () => {
        const before = await heads(),
          run = start({ binding: await binding(), mode: 'hold' }),
          prepared = await run.event('prepared');
        run.child.kill('SIGKILL');
        await assert.rejects(run.done);
        await assertUnchanged(before, prepared.lease.execution);
      },
    );
    await t.test(
      'one output conflict rolls back the whole group and execution completion',
      async () => {
        const run = start({ binding: await binding(), mode: 'denied' }),
          prepared = await run.event('prepared');
        const concurrent = start({ binding: await binding(), mode: 'valid' });
        await concurrent.event('receipt');
        await concurrent.done;
        const before = await heads();
        run.child.stdin!.write('\n');
        assert.match((await run.event('denied')).error, /HEAD_CONFLICT/);
        await run.done;
        await assertUnchanged(before, prepared.lease.execution);
      },
    );
    await t.test('checkpoint CAS failure rolls back both output heads and completion', async () => {
      const before = await heads(),
        run = start({ binding: await binding(), mode: 'denied', checkpoint }),
        prepared = await run.event('prepared');
      run.child.stdin!.write('\n');
      assert.match((await run.event('denied')).error, /CHECKPOINT_CONFLICT/);
      await run.done;
      await assertUnchanged(before, prepared.lease.execution);
    });
    await t.test('namespace revision changes revoke already prepared candidates', async () => {
      const before = await heads(),
        run = start({ binding: await binding(), mode: 'denied' }),
        prepared = await run.event('prepared');
      await db.pool.query(
        "UPDATE moria.native_storage_namespaces SET revision='v2' WHERE environment=$1 AND table_id='table0'",
        [environment],
      );
      run.child.stdin!.write('\n');
      assert.match((await run.event('denied')).error, /STORAGE_NAMESPACE_REVOKED/);
      await run.done;
      await assertUnchanged(before, prepared.lease.execution);
      await db.pool.query(
        "UPDATE moria.native_storage_namespaces SET revision='v3' WHERE environment=$1 AND table_id='table0'",
        [environment],
      );
      snapshots[0].revision = 'v3';
    });
    await t.test('stale coordinator fences cannot publish prepared candidates', async () => {
      const before = await heads(),
        run = start({ binding: await binding(), mode: 'denied' }),
        prepared = await run.event('prepared');
      await db.pool.query(
        'UPDATE moria.runtime_executions SET owner=$2,fence=fence+1 WHERE id=$1',
        [prepared.lease.execution, randomUUID()],
      );
      run.child.stdin!.write('\n');
      assert.match((await run.event('denied')).error, /LEASE_LOST/);
      await run.done;
      await assertUnchanged(before, prepared.lease.execution);
    });
    await t.test(
      'loss of a commit acknowledgement replays the durable receipt without duplicate publication',
      async () => {
        const proxy = await requestCommitProxy(
          Number(db.pool.options.port),
          'runtime_snapshot_publish',
        );
        t.after(proxy.close);
        const proxied = `postgresql://${role}:${password}@127.0.0.1:${proxy.port}/postgres`,
          before = await heads(),
          run = start({ binding: await binding(), mode: 'hold' }),
          prepared = await run.event('prepared');
        run.child.kill('SIGKILL');
        await assert.rejects(run.done);
        const committing = start(
          {
            binding: await binding(),
            mode: 'replay',
            lease: prepared.lease,
            candidates: prepared.candidates,
          },
          proxied,
        );
        await Promise.race([
          proxy.committed,
          committing.done.then(() => {
            throw new Error('Commit acknowledgement was not intercepted');
          }),
        ]);
        committing.child.kill('SIGKILL');
        await assert.rejects(committing.done);
        const replay = start({
          binding: await binding(),
          mode: 'replay',
          lease: prepared.lease,
          candidates: prepared.candidates,
        });
        await replay.done;
        for (const head of await heads())
          assert.equal(
            BigInt(head.revision),
            BigInt((before as any[]).find((h) => h.dataset === head.dataset).revision) + 1n,
          );
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.publications WHERE environment=$1 AND execution=$2',
              [environment, prepared.lease.execution],
            )
          ).rows[0].count,
          '1',
        );
      },
    );
    await t.test(
      'output writes require explicit read and write and complete inherited marking coverage',
      async () => {
        const before = await heads();
        await authority.setGrants(
          environment,
          'fixture',
          'pipeline',
          'native-catalog',
          operations.filter((op) => op !== `${outputs[0]}:read`),
        );
        await assert.rejects(start({ binding: await binding(), mode: 'valid' }).done, /FORBIDDEN/);
        assert.deepEqual(await heads(), before);
        await authority.setGrants(environment, 'fixture', 'pipeline', 'native-catalog', operations);
        await db.pool.query(
          "UPDATE moria.native_storage_namespaces SET markings='{}',revision='v4' WHERE environment=$1 AND table_id='table0'",
          [environment],
        );
        snapshots[0].revision = 'v4';
        await assert.rejects(start({ binding: await binding(), mode: 'valid' }).done, /FORBIDDEN/);
        assert.deepEqual(await heads(), before);
        await db.pool.query(
          "UPDATE moria.native_storage_namespaces SET markings=ARRAY['private'],revision='v5' WHERE environment=$1 AND table_id='table0'",
          [environment],
        );
        snapshots[0].revision = 'v5';
      },
    );
    await t.test(
      'native S3 multipart source writes, immutable closure publication and read-only native scans use real MinIO',
      async () => {
        s3 = await startS3();
        const s3Bindings = snapshots.map((snapshot) => ({
          warehouseId: snapshot.warehouseId,
          tableId: snapshot.tableId,
          endpoint: s3!.endpoint,
          bucket: s3!.bucket,
          prefix: 'catalog',
          region: 'us-east-1',
          accessKeySecretRef: 'CATALOG_S3_ACCESS',
          secretKeySecretRef: 'CATALOG_S3_SECRET',
          sessionTokenSecretRef: null,
          limits: { maxObjectBytes: 128 * 1024 * 1024, chunkBytes: 1024 * 1024 },
        }));
        const run = start({ binding: await binding(), mode: 'valid-s3', s3Bindings });
        await run.done;
        const head = (await heads()).find((h) => h.dataset === outputs[0])!,
          reference = JSON.parse(head.metadataLocation!.slice('moria-snapshot-v2:'.length));
        assert.ok(
          (
            await s3.client.send(
              new HeadObjectCommand({
                Bucket: s3.bucket,
                Key: `catalog/catalogwarehouse/table0/objects/${reference.root.object.objectId}`,
              }),
            )
          ).ContentLength! > 0,
        );
        await authority.setGrants(
          environment,
          'fixture',
          'pipeline',
          'native-catalog',
          operations.filter((op) => !op.endsWith(':write')),
        );
        const readBinding = await binding(
          {
            expectedHeads: {},
            pinnedInputs: [
              { resource: outputs[0], revision: head.revision, snapshot: head.snapshotId },
            ],
          },
          [`${outputs[0]}:read`],
        );
        const read = start({
          binding: readBinding,
          mode: 'read',
          snapshots: [snapshots[0]],
          nativeRef: head.metadataLocation,
          s3Bindings,
        });
        assert.equal((await read.event('read')).rows, 300);
        await read.done;
        await authority.setGrants(environment, 'fixture', 'pipeline', 'native-catalog', operations);
      },
    );
    await t.test(
      'native Arrow replace outputs stream into protected Parquet and Iceberg on POSIX and MinIO',
      async (t) => {
        for (const provider of ['posix', 's3']) {
          await t.test(provider, async () => {
            const writerSnapshots = snapshots.map((snapshot, index) => ({
              ...snapshot,
              resource: randomUUID(),
              warehouseId: `writer${provider}`,
              tableId: `table${index}`,
              revision: 'v1',
              tableUuid: randomUUID(),
            }));
            for (const snapshot of writerSnapshots) {
              await db.catalog.ensureDataset(environment, snapshot.resource);
              operations.push(`${snapshot.resource}:read`, `${snapshot.resource}:write`);
              await db.pool.query(
                "INSERT INTO moria.native_storage_namespaces(environment,warehouse_id,table_id,resource,revision,purpose,iceberg_uuid,markings) VALUES($1,$2,$3,$4,'v1','dataset-source',$5,ARRAY['private'])",
                [
                  environment,
                  snapshot.warehouseId,
                  snapshot.tableId,
                  snapshot.resource,
                  snapshot.tableUuid,
                ],
              );
            }
            await authority.setApplication(environment, 'fixture', 'native-catalog', operations);
            await authority.setGrants(
              environment,
              'fixture',
              'pipeline',
              'native-catalog',
              operations,
            );
            const s3Bindings =
              provider === 's3'
                ? writerSnapshots.map((snapshot) => ({
                    warehouseId: snapshot.warehouseId,
                    tableId: snapshot.tableId,
                    endpoint: s3!.endpoint,
                    bucket: s3!.bucket,
                    prefix: 'catalog',
                    region: 'us-east-1',
                    accessKeySecretRef: 'CATALOG_S3_ACCESS',
                    secretKeySecretRef: 'CATALOG_S3_SECRET',
                    sessionTokenSecretRef: null,
                    limits: { maxObjectBytes: 128 * 1024 * 1024, chunkBytes: 1024 * 1024 },
                  }))
                : [];
            let retainedSnapshot: string | undefined;
            for (const generation of [1, 2]) {
              const writerHeads = await db.catalog.heads(
                environment,
                writerSnapshots.map((s) => s.resource),
              );
              const run = start({
                binding: await binding({
                  expectedHeads: Object.fromEntries(
                    writerHeads.map((h) => [h.dataset, h.revision]),
                  ),
                }),
                mode: 'writer',
                snapshots: writerSnapshots,
                s3Bindings,
              });
              const prepared = await run.event('prepared');
              const receipt = await run.event('receipt');
              await run.done;
              assert.equal(receipt.receipt.outputs.length, 2);
              const proof = (
                await db.pool.query(
                  'SELECT proof FROM moria.runtime_snapshot_candidates WHERE execution=$1',
                  [prepared.lease.execution],
                )
              ).rows;
              assert.equal(proof.length, 2);
              for (const row of proof) {
                assert.equal(row.proof.physicalRows, 300 * generation);
                assert.equal(row.proof.dataFiles, generation);
              }
              const head = (
                await db.catalog.heads(
                  environment,
                  writerSnapshots.map((s) => s.resource),
                )
              )[0]!;
              const readBinding = await binding(
                {
                  expectedHeads: {},
                  pinnedInputs: [
                    { resource: head.dataset, revision: head.revision, snapshot: head.snapshotId },
                  ],
                },
                [`${head.dataset}:read`],
              );
              const read = start({
                binding: readBinding,
                mode: 'read',
                snapshots: [
                  {
                    ...writerSnapshots.find((s) => s.resource === head.dataset),
                    snapshotId: head.snapshotId,
                  },
                ],
                nativeRef: head.metadataLocation,
                retainedSnapshot,
                expectedNativeRows: true,
                s3Bindings,
              });
              const observed = await read.event('read');
              assert.equal(observed.rows, 300);
              if (generation === 2) assert.equal(observed.historicalRows, 300);
              await read.done;
              retainedSnapshot = head.snapshotId!;
            }
          });
        }
        for (const mode of ['writer-fail', 'writer-schema-deny']) {
          const before = await heads(),
            run = start({ binding: await binding(), mode });
          const denied = await run.event('denied');
          await run.done;
          await assertUnchanged(before, denied.execution);
        }
      },
    );
    await t.test(
      'current session and security revocation deny commit after preparation',
      async () => {
        const before = await heads(),
          run = start({ binding: await binding(), mode: 'denied' }),
          prepared = await run.event('prepared');
        await sessions.revoke({ ...context, sessionHash }, 'fixture');
        run.child.stdin!.write('\n');
        assert.match((await run.event('denied')).error, /UNAUTHENTICATED|AUTHORIZATION_CHANGED/);
        await run.done;
        await assertUnchanged(before, prepared.lease.execution);
      },
    );
    await t.test(
      'namespace policy revisions cannot be reused after disable, replacement or deletion',
      async () => {
        const insert = (revision: string) =>
          db.pool.query(
            "INSERT INTO moria.native_storage_namespaces(environment,warehouse_id,table_id,resource,revision,purpose,iceberg_uuid,markings) VALUES($1,'historywarehouse','historytable',$2,$3,'dataset-source',$4,ARRAY['private'])",
            [environment, outputs[0], revision, randomUUID()],
          );
        await insert('v1');
        await assert.rejects(
          db.pool.query(
            "UPDATE moria.native_storage_namespaces SET markings='{}' WHERE warehouse_id='historywarehouse'",
          ),
          /FRESH_NAMESPACE_REVISION_REQUIRED/,
        );
        await db.pool.query(
          "UPDATE moria.native_storage_namespaces SET enabled=false WHERE warehouse_id='historywarehouse'",
        );
        await assert.rejects(
          db.pool.query(
            "UPDATE moria.native_storage_namespaces SET enabled=true WHERE warehouse_id='historywarehouse'",
          ),
          /FRESH_NAMESPACE_REVISION_REQUIRED/,
        );
        await db.pool.query(
          "UPDATE moria.native_storage_namespaces SET enabled=true,revision='v2' WHERE warehouse_id='historywarehouse'",
        );
        await assert.rejects(
          db.pool.query(
            "UPDATE moria.native_storage_namespaces SET revision='v1' WHERE warehouse_id='historywarehouse'",
          ),
          /NAMESPACE_REVISION_REUSED/,
        );
        await db.pool.query(
          "DELETE FROM moria.native_storage_namespaces WHERE warehouse_id='historywarehouse'",
        );
        await assert.rejects(insert('v1'), /NAMESPACE_REVISION_REUSED/);
        await assert.rejects(insert('v2'), /NAMESPACE_REVISION_REUSED/);
        await insert('v3');
        const upsert =
          "INSERT INTO moria.native_storage_namespaces(environment,warehouse_id,table_id,resource,revision,purpose,iceberg_uuid,markings) SELECT environment,warehouse_id,table_id,resource,'v4',purpose,iceberg_uuid,markings FROM moria.native_storage_namespaces WHERE warehouse_id='historywarehouse' ON CONFLICT(warehouse_id,table_id) DO UPDATE SET revision=EXCLUDED.revision";
        await db.pool.query(upsert);
        await db.pool.query(upsert);
        assert.equal(
          (
            await db.pool.query(
              "SELECT count(*) FROM moria.native_storage_namespace_versions WHERE warehouse_id='historywarehouse'",
            )
          ).rows[0].count,
          '4',
        );
        await assert.rejects(
          db.pool.query(
            "UPDATE moria.native_storage_namespaces SET revision='v3' WHERE warehouse_id='historywarehouse'",
          ),
          /NAMESPACE_REVISION_REUSED/,
        );
        await assert.rejects(
          db.pool.query(
            "DELETE FROM moria.native_storage_namespace_versions WHERE warehouse_id='historywarehouse'",
          ),
          /IMMUTABLE_NAMESPACE_HISTORY/,
        );
      },
    );
  },
);
