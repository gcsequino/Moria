import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { database } from './fixtures/catalog-storage/database.js';
import { iceberg } from './iceberg-helper.js';
import { icebergSchema } from '../packages/contracts/iceberg.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { ObjectStore, type ObjectSchema } from '../services/ontology/src/store.js';
import { IcebergIndexer } from '../services/ontology/src/indexer.js';
import { ObjectService } from '../services/ontology/src/service.js';
import { commitProxy } from './fixtures/indexing/commit-proxy.js';
import { createWorkspace } from '../apps/local/src/workspace.js';
import { assetSchema, builtinObjectTypes } from '../packages/contracts/bundled-objects.js';
import { chmod } from 'node:fs/promises';

test(
  'Rust indexing publishes complete native Iceberg partitions through the shared authority',
  { timeout: 300000 },
  async (t) => {
    const db = await database();
    t.after(db.close);
    const environment = randomUUID(),
      targetId = randomUUID(),
      warehouse = join(db.root, 'warehouse');
    const context = { environment, principal: 'pipeline', application: 'indexing' };
    const schema: ObjectSchema = {
      primaryKey: 'uid',
      properties: {
        uid: { type: 'uid', nullable: false },
        name: { type: 'string', nullable: false },
        amount: { type: 'decimal', precision: 12, scale: 2, nullable: false },
        number: { type: 'int64', nullable: false },
        day: { type: 'date', nullable: true },
        time: { type: 'timestamp', nullable: false },
        tags: { type: 'array', items: { type: 'string', nullable: false }, nullable: false },
      },
    };
    await db.catalog.ensureEnvironment(environment);
    await db.catalog.ensureDataset(environment, 'source');
    const authority = new Authority(db.catalog),
      store = new ObjectStore(db.catalog);
    await authority.syncPrincipal(environment, 'fixture', 'pipeline', {
      enabled: true,
      kind: 'workload',
      attributes: {},
      markings: ['private'],
    });
    const operations = ['source:read', 'objects:index', 'objects:read', 'edit:invoke'];
    await authority.setApplication(environment, 'fixture', 'indexing', operations);
    await authority.setGrants(environment, 'fixture', 'pipeline', 'indexing', operations);
    const credential = await new WorkloadSessions(authority).mint(context, 'fixture');
    const session = {
      ...context,
      sessionHash: createHash('sha256').update(credential.token).digest('hex'),
    };
    const indexer = new IcebergIndexer(store, authority, {
      source: { kind: 'dataset', resource: 'source', markings: ['private'] },
      target: { resource: 'objects', markings: ['private'], schema },
    });
    await indexer.registerDistributed(session, { targetId, warehouse, partitions: 4 });
    const rows = await Promise.all(
      Array.from({ length: 60 }, async (_, i) => ({
        uid: await db.catalog.resolveIdentity(`indexing-${environment}`, String(i)),
        name: `item-${i}`,
        amount: '12.30',
        number: '9223372036854775807',
        day: i % 2 ? '2000-02-29' : null,
        time: '2026-10-30T00:00:00.123456Z',
        tags: ['first', 'first', 'last'],
      })),
    );
    rows[0].uid = '00000000-0000-0000-0000-000000000000';
    await db.pool.query('INSERT INTO moria.identities(domain,canonical_key,uid) VALUES($1,$2,$3)', [
      `indexing-${environment}`,
      'nil',
      rows[0].uid,
    ]);
    const first = await iceberg({
      operation: 'stage',
      warehouse,
      schema: icebergSchema(schema.properties),
      rows: rows.slice(0, 30),
    });
    const snapshot = await iceberg({
      operation: 'stage',
      warehouse,
      baseMetadataLocation: first.metadataLocation,
      mode: 'append',
      rows: rows.slice(30),
    });
    const execution = randomUUID(),
      worker = 'publisher';
    const fence = await db.catalog.claim(environment, execution, worker, 300);
    const epoch = (await authority.current(environment, 'pipeline', 'indexing')).epoch;
    const publication = {
      environment,
      execution,
      worker,
      fence,
      securityEpoch: epoch,
      outputs: [
        {
          dataset: 'source',
          expectedRevision: '0',
          metadataLocation: snapshot.metadataLocation,
          snapshotId: snapshot.snapshotId,
        },
      ],
      lineage: {},
    };
    await db.catalog.publish(publication);
    const build = (
      await db.pool.query('SELECT * FROM moria.indexing_builds WHERE environment=$1', [environment])
    ).rows[0];
    assert.equal(build.status, 'pending');
    assert.equal(
      (
        await db.pool.query('SELECT count(*) FROM moria.source_generations WHERE environment=$1', [
          environment,
        ])
      ).rows[0].count,
      '0',
    );
    await db.catalog.publish(publication);
    assert.equal(
      (
        await db.pool.query('SELECT count(*) FROM moria.indexing_builds WHERE environment=$1', [
          environment,
        ])
      ).rows[0].count,
      '1',
    );

    const role = `index_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role}; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    const connection = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const binary = process.env.MORIA_INDEXING_BINARY ?? '/usr/local/bin/moria-indexing';
    const start = (
      command: string,
      url = connection,
      claimed?: () => void,
      args: string[] = [],
    ) => {
      const process = spawn(binary, [command, ...args], {
        env: {
          PATH: '/usr/local/bin:/usr/bin:/bin',
          MORIA_INDEXING_DATABASE_SECRET_REF: 'INDEXING_FIXTURE_DATABASE',
          INDEXING_FIXTURE_DATABASE: url,
          MORIA_INDEXING_DATABASE_TLS: 'disable',
        },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      let output = '',
        errors = '';
      process.stdout.on('data', (bytes) => {
        output += bytes.toString();
        if (output.includes('claimed')) claimed?.();
      });
      process.stderr.on('data', (bytes) => {
        errors += bytes.toString();
      });
      const done = new Promise<string>((resolve, reject) => {
        process.once('error', reject);
        process.once('exit', (code, signal) =>
          code === 0
            ? resolve(output)
            : reject(new Error(`Rust ${command} failed (${code ?? signal}): ${errors}`)),
        );
      });
      void done.catch(() => {});
      return { process, done, output: () => output };
    };
    await start('plan').done;
    assert.equal(
      (await db.pool.query('SELECT count(*) FROM moria.indexing_tasks WHERE build=$1', [build.id]))
        .rows[0].count,
      '2',
    );
    const blocked = await db.pool.connect();
    await blocked.query('BEGIN');
    await blocked.query('LOCK moria.indexing_rows IN ACCESS EXCLUSIVE MODE');
    const a = start('task'),
      b = start('task');
    t.after(() => {
      a.process.kill('SIGKILL');
      b.process.kill('SIGKILL');
    });
    for (
      let i = 0;
      i < 200 && (!a.output().includes('claimed') || !b.output().includes('claimed'));
      i++
    )
      await delay(25);
    assert.match(a.output(), /claimed/);
    assert.match(b.output(), /claimed/);
    const active = (
      await db.pool.query(
        "SELECT DISTINCT owner FROM moria.indexing_tasks WHERE build=$1 AND status='running'",
        [build.id],
      )
    ).rows;
    assert.equal(active.length, 2, 'two actual Rust processes must own independent scan tasks');
    await blocked.query('COMMIT');
    blocked.release();
    await Promise.all([a.done, b.done]);
    await start('drain').done;
    const completed = (
      await db.pool.query('SELECT status,receipt,error FROM moria.indexing_builds WHERE id=$1', [
        build.id,
      ])
    ).rows[0];
    assert.equal(completed.status, 'active', JSON.stringify(completed));
    const service = new ObjectService(store, authority, {
      schema,
      objectTypeId: targetId,
      policies: {
        resource: 'objects',
        markings: ['private'],
        properties: schema.properties,
        sourceColumns: schema.properties,
        attributes: {},
        row: { literal: true },
      },
    });
    for (const expected of [rows[0], rows[30], rows[59]]) {
      const result = await service.read(session, expected.uid);
      assert.deepEqual(result.object, expected);
    }
    assert.equal(
      (
        await db.pool.query('SELECT count(*) FROM moria.staged_source_rows WHERE stage=$1', [
          build.id,
        ])
      ).rows[0].count,
      '60',
    );
    const generations = await db.pool.query(
      'SELECT revision FROM moria.source_generations WHERE stage=$1',
      [build.id],
    );
    assert.equal(generations.rowCount, 1);
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.indexing_changes WHERE generation=$1 AND transaction_end',
          [build.id],
        )
      ).rows[0].count,
      '1',
    );

    const publish = async (metadata = snapshot) => {
      const execution = randomUUID(),
        worker = 'publisher';
      const fence = await db.catalog.claim(environment, execution, worker, 300);
      const epoch = (await authority.current(environment, 'pipeline', 'indexing')).epoch;
      const head = (await db.catalog.heads(environment, ['source']))[0];
      await db.catalog.publish({
        environment,
        execution,
        worker,
        fence,
        securityEpoch: epoch,
        outputs: [
          {
            dataset: 'source',
            expectedRevision: head.revision,
            metadataLocation: metadata.metadataLocation,
            snapshotId: metadata.snapshotId,
          },
        ],
        lineage: {},
      });
      return (
        await db.pool.query(
          'SELECT * FROM moria.indexing_builds WHERE environment=$1 ORDER BY source_revision DESC LIMIT 1',
          [environment],
        )
      ).rows[0];
    };

    await t.test(
      'source publication rollback leaves neither a new head nor indexing intent',
      async () => {
        await db.catalog.ensureDataset(environment, 'conflict');
        const execution = randomUUID(),
          worker = 'publisher',
          fence = await db.catalog.claim(environment, execution, worker, 300);
        const epoch = (await authority.current(environment, 'pipeline', 'indexing')).epoch;
        await assert.rejects(
          db.catalog.publish({
            environment,
            execution,
            worker,
            fence,
            securityEpoch: epoch,
            outputs: [
              {
                dataset: 'source',
                expectedRevision: '1',
                metadataLocation: snapshot.metadataLocation,
                snapshotId: snapshot.snapshotId,
              },
              {
                dataset: 'conflict',
                expectedRevision: '99',
                metadataLocation: snapshot.metadataLocation,
                snapshotId: snapshot.snapshotId,
              },
            ],
            lineage: {},
          }),
          { code: 'HEAD_CONFLICT' },
        );
        assert.equal((await db.catalog.heads(environment, ['source']))[0].revision, '1');
        assert.equal(
          (
            await db.pool.query('SELECT count(*) FROM moria.indexing_builds WHERE environment=$1', [
              environment,
            ])
          ).rows[0].count,
          '1',
        );
      },
    );

    await t.test('replacement fences a paused Rust executor without a second upload', async () => {
      const next = await publish();
      await service.invoke(
        session,
        {
          resource: 'edit',
          revision: '1',
          markings: ['private'],
          targetParameter: 'uid',
          inputs: { uid: schema.properties.uid, name: schema.properties.name },
          condition: { literal: true },
          edits: [{ op: 'set', property: 'name', value: { parameter: 'name' } }],
        },
        randomUUID(),
        { uid: rows[1].uid, name: 'Preserved override' },
      );
      await start('plan').done;
      let stopped = false;
      const old = start('task', connection, () => {
        if (!stopped) {
          stopped = true;
          old.process.kill('SIGSTOP');
        }
      });
      t.after(() => old.process.kill('SIGKILL'));
      for (let i = 0; i < 200 && !stopped; i++) await delay(10);
      assert.equal(stopped, true);
      const task = (
        await db.pool.query(
          "SELECT * FROM moria.indexing_tasks WHERE build=$1 AND status='running'",
          [next.id],
        )
      ).rows[0];
      assert.ok(task);
      // Explicit authority-side lease expiry models an operator's failed-host takeover.
      await db.pool.query(
        "UPDATE moria.indexing_tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
        [task.id],
      );
      await start('drain').done;
      const replacement = (
        await db.pool.query('SELECT owner,fence,status FROM moria.indexing_tasks WHERE id=$1', [
          task.id,
        ])
      ).rows[0];
      assert.equal(replacement.status, 'sealed');
      assert.equal(replacement.fence, '2');
      assert.notEqual(replacement.owner, task.owner);
      old.process.kill('SIGCONT');
      await old.done;
      assert.equal(
        (await db.pool.query('SELECT status FROM moria.indexing_builds WHERE id=$1', [next.id]))
          .rows[0].status,
        'active',
      );
      assert.equal(
        (
          await db.pool.query('SELECT count(*) FROM moria.source_generations WHERE stage=$1', [
            next.id,
          ])
        ).rows[0].count,
        '1',
      );
      assert.equal((await service.read(session, rows[1].uid)).object.name, 'Preserved override');
      const attempts = (
        await db.pool.query(
          "SELECT fence,outcome FROM moria.indexing_attempts WHERE build=$1 AND kind='task' AND unit=$2 ORDER BY fence",
          [next.id, task.id],
        )
      ).rows;
      assert.deepEqual(attempts, [
        { fence: '1', outcome: 'interrupted' },
        { fence: '2', outcome: 'sealed' },
      ]);
    });

    await t.test('a newer accepted snapshot supersedes an older prepared attempt', async () => {
      const old = await publish();
      await start('plan').done;
      await Promise.all([start('task').done, start('task').done]);
      const next = await publish();
      await start('drain').done;
      assert.equal(
        (await db.pool.query('SELECT status FROM moria.indexing_builds WHERE id=$1', [old.id]))
          .rows[0].status,
        'superseded',
      );
      assert.equal(
        (
          await db.pool.query('SELECT count(*) FROM moria.source_generations WHERE stage=$1', [
            old.id,
          ])
        ).rows[0].count,
        '0',
      );
      assert.equal(
        (await db.pool.query('SELECT status FROM moria.indexing_builds WHERE id=$1', [next.id]))
          .rows[0].status,
        'active',
      );
    });

    await t.test(
      'actual COMMIT survives killing Rust before its acknowledgment arrives',
      async () => {
        const next = await publish();
        await start('plan').done;
        await Promise.all([start('task').done, start('task').done]);
        await start('advance').done;
        await Promise.all(Array.from({ length: 4 }, () => start('task').done));
        const proxy = await commitProxy(Number(db.pool.options.port));
        t.after(proxy.close);
        await db.pool.query(
          "UPDATE moria.indexing_builds SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
          [next.id],
        );
        const publication = start(
          'advance',
          connection.replace(`:${db.pool.options.port}/`, `:${proxy.port}/`),
        );
        t.after(() => publication.process.kill('SIGKILL'));
        await Promise.race([
          proxy.commit,
          delay(15000).then(() => {
            throw new Error('Activation did not reach the real COMMIT barrier');
          }),
        ]);
        const committed = (
          await db.pool.query('SELECT * FROM moria.indexing_builds WHERE id=$1', [next.id])
        ).rows[0];
        assert.equal(committed.status, 'active');
        publication.process.kill('SIGKILL');
        await assert.rejects(publication.done);
        const receipt = (
          await db.pool.query('SELECT moria.indexing_activate($1,$2,$3) AS receipt', [
            next.id,
            committed.owner,
            committed.fence,
          ])
        ).rows[0].receipt;
        assert.deepEqual(receipt, committed.receipt);
        assert.equal(
          (
            await db.pool.query('SELECT count(*) FROM moria.source_generations WHERE stage=$1', [
              next.id,
            ])
          ).rows[0].count,
          '1',
        );
      },
    );

    await t.test('duplicate UIDs across different Iceberg files prevent activation', async () => {
      const duplicated = await iceberg({
        operation: 'stage',
        warehouse,
        baseMetadataLocation: snapshot.metadataLocation,
        mode: 'append',
        rows: [rows[0]],
      });
      const next = await publish(duplicated);
      const head = (
        await db.pool.query(
          'SELECT max(revision)::text AS revision FROM moria.source_generations WHERE environment=$1',
          [environment],
        )
      ).rows[0].revision;
      await start('drain').done;
      const failed = (
        await db.pool.query('SELECT status,error FROM moria.indexing_builds WHERE id=$1', [next.id])
      ).rows[0];
      assert.equal(failed.status, 'failed');
      assert.equal(failed.error, 'DUPLICATE_UID');
      assert.equal(
        (
          await db.pool.query(
            'SELECT max(revision)::text AS revision FROM moria.source_generations WHERE environment=$1',
            [environment],
          )
        ).rows[0].revision,
        head,
      );
    });

    await t.test(
      'operator retry reuses pinned input after a real file access failure',
      async () => {
        const next = await publish();
        await chmod(snapshot.metadataLocation, 0);
        try {
          await start('drain').done;
        } finally {
          await chmod(snapshot.metadataLocation, 0o644);
        }
        assert.equal(
          (await db.pool.query('SELECT status FROM moria.indexing_builds WHERE id=$1', [next.id]))
            .rows[0].status,
          'failed',
        );
        await start('retry', connection, undefined, [next.id]).done;
        await start('drain').done;
        assert.equal(
          (await db.pool.query('SELECT status FROM moria.indexing_builds WHERE id=$1', [next.id]))
            .rows[0].status,
          'active',
        );
        assert.equal((await service.read(session, rows[1].uid)).object.name, 'Preserved override');
        assert.equal(
          (
            await db.pool.query('SELECT count(*) FROM moria.source_generations WHERE stage=$1', [
              next.id,
            ])
          ).rows[0].count,
          '1',
        );
      },
    );

    await t.test(
      'cancellation prevents activation and cannot revoke a committed success',
      async () => {
        const next = await publish();
        await start('cancel', connection, undefined, [next.id]).done;
        await start('drain').done;
        assert.equal(
          (await db.pool.query('SELECT status FROM moria.indexing_builds WHERE id=$1', [next.id]))
            .rows[0].status,
          'cancelled',
        );
        assert.equal(
          (
            await db.pool.query('SELECT count(*) FROM moria.source_generations WHERE stage=$1', [
              next.id,
            ])
          ).rows[0].count,
          '0',
        );
        await start('cancel', connection, undefined, [build.id]).done;
        assert.equal(
          (await db.pool.query('SELECT status FROM moria.indexing_builds WHERE id=$1', [build.id]))
            .rows[0].status,
          'active',
        );
      },
    );

    await t.test(
      'corrupt binary task plans fail before any object generation is exposed',
      async () => {
        const next = await publish();
        await start('plan').done;
        await db.pool.query(
          "UPDATE moria.indexing_tasks SET wire=decode('00','hex') WHERE build=$1",
          [next.id],
        );
        await start('drain').done;
        assert.equal(
          (await db.pool.query('SELECT status FROM moria.indexing_builds WHERE id=$1', [next.id]))
            .rows[0].status,
          'failed',
        );
        assert.equal(
          (
            await db.pool.query('SELECT count(*) FROM moria.source_generations WHERE stage=$1', [
              next.id,
            ])
          ).rows[0].count,
          '0',
        );
      },
    );

    await t.test(
      'loss of the lifetime schema lease stops admission while checked-out connections fence migration',
      async (t) => {
        const next = await publish();
        await start('plan').done;
        const blocker = await db.pool.connect();
        await blocker.query('BEGIN');
        await blocker.query('LOCK moria.instance_state IN ACCESS EXCLUSIVE MODE');
        const blockerPid = (await blocker.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
        const running = start('worker');
        t.after(() => running.process.kill('SIGKILL'));
        t.after(async () => {
          await blocker.query('ROLLBACK');
          blocker.release();
        });
        let checkedOut = false;
        for (let i = 0; i < 200 && !checkedOut; i++) {
          checkedOut = (
            await db.pool.query(
              "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='moria-indexing' AND usename=$1 AND wait_event_type='Lock' AND $2=ANY(pg_blocking_pids(pid))) AS waiting",
              [role, blockerPid],
            )
          ).rows[0].waiting;
          if (!checkedOut) await delay(10);
        }
        if (!checkedOut)
          t.diagnostic(
            JSON.stringify(
              (
                await db.pool.query(
                  "SELECT pid,state,wait_event_type,query,pg_blocking_pids(pid) AS blockers FROM pg_stat_activity WHERE application_name='moria-indexing' AND usename=$1",
                  [role],
                )
              ).rows,
            ),
          );
        assert.equal(
          checkedOut,
          true,
          'Rust must hold a checked-out connection at the SQL barrier',
        );
        running.process.kill('SIGSTOP');
        const lifetime = (
          await db.pool.query(
            "SELECT pid FROM pg_stat_activity WHERE application_name='moria-indexing' AND usename=$1 AND query LIKE 'SELECT rolsuper%'",
            [role],
          )
        ).rows;
        assert.equal(lifetime.length, 1);
        assert.equal(
          (await db.pool.query('SELECT pg_terminate_backend($1) AS stopped', [lifetime[0].pid]))
            .rows[0].stopped,
          true,
        );
        const probe = await db.pool.connect();
        try {
          const allowed = (
            await probe.query('SELECT pg_try_advisory_lock(731804135219) AS acquired')
          ).rows[0].acquired;
          if (allowed) await probe.query('SELECT pg_advisory_unlock(731804135219)');
          assert.equal(
            allowed,
            false,
            'a checked-out Rust connection must still exclude migration',
          );
          await blocker.query('ROLLBACK');
          running.process.kill('SIGCONT');
          await assert.rejects(running.done);
          let acquired = false;
          for (let i = 0; i < 100 && !acquired; i++) {
            acquired = (await probe.query('SELECT pg_try_advisory_lock(731804135219) AS acquired'))
              .rows[0].acquired;
            if (!acquired) await delay(10);
          }
          assert.equal(
            acquired,
            true,
            'stopped Rust workers must not leave orphaned migration exclusion',
          );
          await probe.query('SELECT pg_advisory_unlock(731804135219)');
        } finally {
          probe.release();
        }
        assert.equal(
          (
            await db.pool.query('SELECT count(*) FROM moria.source_generations WHERE stage=$1', [
              next.id,
            ])
          ).rows[0].count,
          '0',
        );
      },
    );

    await t.test('revocation before release preserves the previous object head', async () => {
      const next = await publish();
      await start('plan').done;
      const head = (
        await db.pool.query(
          'SELECT max(revision)::text AS revision FROM moria.source_generations WHERE environment=$1',
          [environment],
        )
      ).rows[0].revision;
      await authority.setGrants(environment, 'fixture', 'pipeline', 'indexing', [
        'source:read',
        'objects:read',
      ]);
      await start('drain').done;
      const failed = (
        await db.pool.query('SELECT status,error FROM moria.indexing_builds WHERE id=$1', [next.id])
      ).rows[0];
      assert.equal(failed.status, 'failed');
      assert.equal(
        (
          await db.pool.query(
            'SELECT max(revision)::text AS revision FROM moria.source_generations WHERE environment=$1',
            [environment],
          )
        ).rows[0].revision,
        head,
      );
    });

    await t.test(
      'generic imports persist source/served status and recover in a new workspace instance',
      async () => {
        const warehouse = join(db.root, 'generic');
        const workspace = await createWorkspace(db.catalog, warehouse, {
          distributedIndexing: true,
        });
        await assert.rejects(
          workspace.ingest(
            { environment: 'local-workspace', principal: 'operator', application: 'local-ui' },
            [],
          ),
          (error: unknown) => (error as { code?: string }).code === 'UNAUTHENTICATED',
        );
        const imported = await workspace.ingest('operator', [
          { id: 'one', title: 'Original', region: 'West', status: 'Open', notes: 'Durable' },
        ]);
        assert.equal(imported.indexing, 'pending');
        const before = await workspace.status();
        assert.equal(before.ready, false);
        assert.equal(before.importing, true);
        assert.equal(before.sourceRevision, '1');
        assert.equal(before.servedSourceRevision, '0');
        const restarted = await createWorkspace(db.catalog, warehouse, {
          distributedIndexing: true,
        });
        await start('drain').done;
        const after = await restarted.status();
        assert.equal(after.ready, true);
        assert.equal(after.importing, false);
        assert.equal(after.indexingStatus, 'active');
        assert.equal(after.servedSourceRevision, '1');
        const page = await restarted.service.query(
          { environment: 'local-workspace', principal: 'operator', application: 'local-ui' },
          { pageSize: 10 },
        );
        assert.equal(page.objects.length, 1);
        assert.equal(page.objects[0].title, 'Original');
        const local = {
          environment: 'local-workspace',
          principal: 'operator',
          application: 'local-ui',
        };
        await db.pool.query(
          `UPDATE moria.workload_sessions SET expires_at=clock_timestamp()-interval '1 second'
           WHERE token_hash=(SELECT context->>'sessionHash' FROM moria.indexing_bindings WHERE environment=$1 AND target='assets')`,
          [local.environment],
        );
        const sourceHead = (await db.catalog.heads(local.environment, ['assets-source']))[0];
        const replacement = await iceberg({
          operation: 'stage',
          warehouse,
          baseMetadataLocation: sourceHead.metadataLocation,
          rows: [
            {
              uid: page.objects[0].uid,
              id: 'one',
              title: 'Saved while indexer expired',
              region: 'West',
              status: 'Open',
              notes: 'Durable',
            },
          ],
        });
        const publisher = await restarted.sessions.authenticate(
          `Bearer ${(await restarted.sessions.mint(local, 'fixture')).token}`,
        );
        const publishing = randomUUID();
        const publishingFence = await db.catalog.claim(
          local.environment,
          publishing,
          'workspace-publisher',
          300,
        );
        const epoch = (
          await restarted.authority.current(local.environment, local.principal, local.application)
        ).epoch;
        await db.catalog.publish(
          {
            environment: local.environment,
            execution: publishing,
            worker: 'workspace-publisher',
            fence: publishingFence,
            securityEpoch: epoch,
            outputs: [
              {
                dataset: 'assets-source',
                expectedRevision: sourceHead.revision,
                metadataLocation: replacement.metadataLocation,
                snapshotId: replacement.snapshotId,
              },
            ],
            lineage: {},
          },
          (client) =>
            restarted.authority.assertCurrent(
              client,
              local.environment,
              local.principal,
              local.application,
              epoch,
              publisher.sessionHash,
            ),
        );
        const blocked = await restarted.status();
        assert.equal(blocked.ready, true);
        assert.equal(blocked.importing, false);
        assert.equal(blocked.sourceRevision, '2');
        assert.equal(blocked.servedSourceRevision, '1');
        assert.equal(blocked.indexingStatus, 'blocked');
        assert.equal(blocked.indexingError, 'UNAUTHENTICATED');
        assert.equal(
          (await restarted.service.query(publisher, { pageSize: 10 })).objects[0].title,
          'Original',
        );
        const recoveredIndexer = new IcebergIndexer(
          new ObjectStore(db.catalog),
          restarted.authority,
          {
            source: { kind: 'dataset', resource: 'assets-source', markings: ['workspace'] },
            target: { resource: 'assets', markings: ['workspace'], schema: assetSchema },
          },
        );
        await recoveredIndexer.registerDistributed(publisher, {
          targetId: builtinObjectTypes.assets,
          warehouse,
        });
        assert.equal((await restarted.status()).indexingStatus, 'pending');
        await start('drain').done;
        const recovered = await restarted.status();
        assert.equal(recovered.indexingStatus, 'active');
        assert.equal(recovered.sourceRevision, '2');
        assert.equal(recovered.servedSourceRevision, '2');
        assert.equal(
          (await restarted.service.query(publisher, { pageSize: 10 })).objects[0].title,
          'Saved while indexer expired',
        );
      },
    );

    await t.test('revoked preclaim work cannot poison an independent build drain', async (t) => {
      const workspace = await createWorkspace(db.catalog, join(db.root, 'generic'), {
        distributedIndexing: true,
      });
      for (const phase of ['plan', 'task', 'advance'] as const)
        await t.test(phase, async () => {
          await authority.setGrants(environment, 'fixture', 'pipeline', 'indexing', operations);
          await indexer.registerDistributed(session, { targetId, warehouse, partitions: 4 });
          const rejected = await publish();
          if (phase !== 'plan') await start('plan').done;
          if (phase === 'advance') {
            let pending = 0;
            for (let attempt = 0; attempt < 16; attempt++) {
              pending = Number(
                (
                  await db.pool.query(
                    "SELECT count(*) FROM moria.indexing_tasks WHERE build=$1 AND status<>'sealed'",
                    [rejected.id],
                  )
                ).rows[0].count,
              );
              if (!pending) break;
              await start('task').done;
            }
            assert.equal(pending, 0, 'all actual tasks must seal before coordinator admission');
          }
          const head = (
            await db.pool.query(
              'SELECT max(revision)::text AS revision FROM moria.source_generations WHERE environment=$1',
              [environment],
            )
          ).rows[0].revision;
          await authority.setGrants(environment, 'fixture', 'pipeline', 'indexing', [
            'source:read',
            'objects:read',
          ]);
          const accepted = await workspace.ingest('operator', [
            {
              id: 'independent-' + phase,
              title: 'Independent ' + phase,
              region: 'West',
              status: 'Open',
              notes: 'Valid work behind a revoked candidate',
            },
          ]);
          assert.equal(accepted.indexing, 'pending');
          await start('drain').done;
          assert.deepEqual(
            (
              await db.pool.query('SELECT status,error FROM moria.indexing_builds WHERE id=$1', [
                rejected.id,
              ])
            ).rows[0],
            { status: 'failed', error: 'AUTHORIZATION_CHANGED' },
          );
          assert.equal(
            (
              await db.pool.query(
                'SELECT max(revision)::text AS revision FROM moria.source_generations WHERE environment=$1',
                [environment],
              )
            ).rows[0].revision,
            head,
          );
          assert.equal(
            (
              await db.pool.query('SELECT count(*) FROM moria.source_generations WHERE stage=$1', [
                rejected.id,
              ])
            ).rows[0].count,
            '0',
          );
          const status = await workspace.status();
          assert.equal(status.indexingStatus, 'active');
          assert.equal(status.servedSourceRevision, accepted.sourceRevision);
          const page = await workspace.service.query(
            { environment: 'local-workspace', principal: 'operator', application: 'local-ui' },
            { pageSize: 10 },
          );
          assert.equal(page.objects[0].title, 'Independent ' + phase);
        });
    });
  },
);
