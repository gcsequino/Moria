import assert from 'node:assert/strict';
import { execFile, spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import { Pool } from 'pg';
import type { database } from '../catalog-storage/database.js';

type Input = {
  db: Awaited<ReturnType<typeof database>>;
  runtime: string;
  env: Record<string, string | undefined>;
  environment: string;
  target: string;
  source: string;
  publishedBuild: string;
  publish: () => Promise<unknown>;
  queryCount: () => Promise<number>;
};

/** Real native stages, policies and competing SQL locks; only fixture-owned clock fields are aged. */
export async function sourceStagingCollection(input: Input) {
  const { db, runtime, env, environment, target, publishedBuild } = input;
  const ref = env.MORIA_INDEXING_DATABASE_SECRET_REF!;
  const operator = {
    ...env,
    [ref]: `postgresql://${db.pool.options.user}:${encodeURIComponent(db.pool.options.password as string)}@127.0.0.1:${db.pool.options.port}/postgres`,
  };
  const cli = async (args: string[], value?: unknown, op = false) => {
    if (value === undefined) {
      return (
        await promisify(execFile)(runtime, args, {
          env: op ? operator : env,
          timeout: 90000,
          maxBuffer: 1024 * 1024,
        })
      ).stdout;
    }
    return await new Promise<string>((resolve, reject) => {
      const child = spawn(runtime, args, {
        env: op ? operator : env,
        stdio: ['pipe', 'pipe', 'pipe'],
      });
      let out = '',
        error = '';
      const timer = setTimeout(() => {
        child.kill('SIGKILL');
        reject(new Error('staging command deadline'));
      }, 30000);
      child.stdout.on('data', (chunk) => (out += chunk));
      child.stderr.on('data', (chunk) => (error = (error + chunk).slice(-65536)));
      child.stdin.on('error', () => {});
      child.once('error', (error) => {
        clearTimeout(timer);
        reject(error);
      });
      child.once('exit', (code) => {
        clearTimeout(timer);
        code === 0 ? resolve(out) : reject(new Error(`${args.join('/')}: ${error}`));
      });
      child.stdin.end(JSON.stringify(value));
    });
  };
  const history = async (command: string, id: string) =>
    JSON.parse(await cli(['history-retention', command], { id }, true));
  const approve = async (key: string) =>
    JSON.parse(
      await cli(
        ['history-retention', 'approve'],
        { environment, requestKey: key, retainSeconds: 86400, keepGenerations: 2 },
        true,
      ),
    ).policy as string;
  const counts = async (build: string) =>
    (
      await db.pool.query(
        'SELECT (SELECT count(*)::text FROM moria.indexing_rows WHERE build=$1) AS spills,(SELECT count(*)::text FROM moria.native_source_key_map WHERE store=$1) AS keys,(SELECT count(*)::text FROM moria.staged_source_rows WHERE stage=$1) AS rows',
        [build],
      )
    ).rows[0];
  const published = await counts(publishedBuild);
  assert.equal(published.spills, '300');
  assert.equal(published.keys, '300');
  const policyA = await approve('staging-a-' + randomUUID());
  await history('plan', policyA);
  await assert.rejects(
    cli(['history-retention', 'once'], { id: policyA }),
    /RECOVERY_OPERATOR_REQUIRED/,
  );
  const ordinary = new Pool({ connectionString: env[ref], ssl: false });
  try {
    await assert.rejects(
      ordinary.query('SELECT moria.runtime_indexing_staging_once($1::uuid)', [policyA]),
      /HISTORY_OPERATOR_REQUIRED/,
    );
  } finally {
    await ordinary.end();
  }
  await assert.rejects(
    db.pool.query(
      'UPDATE moria.runtime_indexing_stage_closures SET observed_at=observed_at WHERE build=$1',
      [publishedBuild],
    ),
    /IMMUTABLE_INDEXING_CLOSURE/,
  );
  // A pre-trigger legacy closed build has no historical close time. It must be observed now.
  const fault = await db.pool.connect();
  try {
    await fault.query('BEGIN');
    await fault.query('LOCK TABLE moria.runtime_indexing_stage_closures IN ACCESS EXCLUSIVE MODE');
    await fault.query(
      'ALTER TABLE moria.runtime_indexing_stage_closures DISABLE TRIGGER runtime_indexing_closure_immutable',
    );
    await fault.query('DELETE FROM moria.runtime_indexing_stage_closures WHERE build=$1', [
      publishedBuild,
    ]);
    await fault.query(
      "UPDATE moria.indexing_builds SET created_at=created_at-interval '3 days' WHERE id=$1",
      [publishedBuild],
    );
    await fault.query(
      'ALTER TABLE moria.runtime_indexing_stage_closures ENABLE TRIGGER runtime_indexing_closure_immutable',
    );
    await fault.query('COMMIT');
  } catch (error) {
    await fault.query('ROLLBACK');
    throw error;
  } finally {
    fault.release();
  }
  const observed = await history('once', policyA);
  assert.equal(observed.indexingStaging.state, 'observed');
  const observation = (
    await db.pool.query(
      'SELECT kind,observed_at,owner,fence::text FROM moria.runtime_indexing_stage_closures WHERE build=$1 ORDER BY sequence DESC LIMIT 1',
      [publishedBuild],
    )
  ).rows[0];
  assert.equal(observation.kind, 'observed');
  assert.ok(Math.abs(new Date(observation.observed_at).getTime() - Date.now()) < 10000);
  assert.deepEqual(await counts(publishedBuild), published);
  assert.equal((await history('once', policyA)).indexingStaging.state, 'idle');

  await input.publish();
  const build = (
    await db.pool.query(
      "SELECT id FROM moria.indexing_builds WHERE environment=$1 AND target=$2 AND status='pending' ORDER BY created_at DESC LIMIT 1",
      [environment, target],
    )
  ).rows[0].id as string;
  await cli(['plan']);
  for (let i = 0; i < 20; i++) {
    const left = (
      await db.pool.query(
        "SELECT count(*)::text FROM moria.indexing_tasks WHERE build=$1 AND plan->>'kind'='scan' AND status<>'sealed'",
        [build],
      )
    ).rows[0].count;
    if (left === '0') break;
    await cli(['task']);
  }
  await cli(['advance']);
  await cli(['task']);
  assert.deepEqual(await counts(build), { spills: '300', keys: '300', rows: '300' });
  const gate = await db.pool.connect(),
    tag = 'staging_' + randomUUID().replaceAll('-', '');
  const gateName = 'moria/fixture/staging-projection/' + tag;
  let child: ReturnType<typeof spawn> | undefined, done: Promise<number | null> | undefined;
  try {
    await gate.query('SELECT pg_advisory_lock(hashtextextended($1,0))', [gateName]);
    await db.pool.query(
      // Try admission without a blocking lock: native lock/statement deadlines remain enabled.
      `CREATE FUNCTION moria.${tag}() RETURNS event_trigger LANGUAGE plpgsql AS $$ DECLARE previous_name text:=current_setting('application_name'); BEGIN IF position('runtime_projection_build_index' in current_query())=0 THEN RETURN; END IF; PERFORM set_config('application_name','${tag}',true); WHILE NOT pg_try_advisory_xact_lock(hashtextextended('${gateName}',0)) LOOP PERFORM pg_sleep(0.005); END LOOP; PERFORM set_config('application_name',previous_name,true); END $$`,
    );
    await db.pool.query(
      `CREATE EVENT TRIGGER ${tag} ON ddl_command_end WHEN TAG IN ('CREATE INDEX') EXECUTE FUNCTION moria.${tag}()`,
    );
    child = spawn(runtime, ['advance'], { env, stdio: ['ignore', 'pipe', 'pipe'] });
    let tail = '';
    child.stdout!.on('data', (chunk) => (tail = (tail + chunk).slice(-65536)));
    child.stderr!.on('data', (chunk) => (tail = (tail + chunk).slice(-65536)));
    done = new Promise<number | null>((resolve, reject) => {
      child!.once('error', reject);
      child!.once('exit', resolve);
    });
    void done.catch(() => {});
    let writerPid: number | undefined;
    for (let i = 0; i < 2000; i++) {
      writerPid = (
        await db.pool.query(
          "SELECT a.pid FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid WHERE a.application_name=$1 AND a.query LIKE '%runtime_projection_build_index%' AND l.locktype='advisory' AND l.mode='ShareLock' AND l.granted AND l.classid=((hashtextextended('moria/indexing-staging/v1/'||$2::text,0)>>32)&4294967295)::oid AND l.objid=(hashtextextended('moria/indexing-staging/v1/'||$2::text,0)&4294967295)::oid AND l.objsubid=1",
          [tag, build],
        )
      ).rows[0]?.pid;
      if (writerPid !== undefined) break;
      if (child.exitCode !== null) throw new Error('advance exited before real DDL gate: ' + tail);
      await delay(5);
    }
    if (!writerPid) {
      const activity = await db.pool.query(
        "SELECT a.pid,a.application_name,a.state,a.wait_event,position('runtime_projection_build_index' in a.query)>0 AS index_step,COALESCE((SELECT jsonb_agg(jsonb_build_object('classid',l.classid::text,'objid',l.objid::text,'subid',l.objsubid,'mode',l.mode,'granted',l.granted)) FROM pg_locks l WHERE l.pid=a.pid AND l.locktype='advisory'),'[]'::jsonb) AS locks,((hashtextextended('moria/indexing-staging/v1/'||$2::text,0)>>32)&4294967295)::text AS expected_classid,(hashtextextended('moria/indexing-staging/v1/'||$2::text,0)&4294967295)::text AS expected_objid FROM pg_stat_activity a WHERE a.pid<>pg_backend_pid() AND a.datname=current_database() AND (a.application_name=$1 OR position('runtime_projection_' in a.query)>0) LIMIT 8",
        [tag, build],
      );
      throw new Error(
        'actual index barrier not observed: ' + JSON.stringify({ activity: activity.rows, tail }),
      );
    }
    const runningRows = await counts(build);
    const ineligible = await history('once', policyA);
    assert.equal(ineligible.indexingStaging.state, 'idle');
    assert.equal(ineligible.indexingStaging.worked, false);
    assert.deepEqual(await counts(build), runningRows);
    // Roll back only this verified fixture backend. Earlier native begin/page commits survive.
    child.kill('SIGKILL');
    await db.pool.query(
      "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid=$1 AND application_name=$2 AND query LIKE '%runtime_projection_build_index%'",
      [writerPid, tag],
    );
    await done;
    await gate.query('SELECT pg_advisory_unlock(hashtextextended($1,0))', [gateName]);
    await cli(['cancel', build]);
    assert.equal(
      (await db.pool.query('SELECT status FROM moria.indexing_builds WHERE id=$1', [build])).rows[0]
        .status,
      'cancelled',
    );
    // Move only historical clock fields and their derived hash consistently; actual owner/fence stay intact.
    const age = await db.pool.connect();
    try {
      await age.query('BEGIN');
      await age.query('LOCK TABLE moria.runtime_indexing_stage_closures IN ACCESS EXCLUSIVE MODE');
      await age.query(
        'ALTER TABLE moria.runtime_indexing_stage_closures DISABLE TRIGGER runtime_indexing_closure_immutable',
      );
      await age.query(
        "UPDATE moria.indexing_builds SET created_at=created_at-interval '3 days',deadline=deadline-interval '3 days' WHERE id=$1",
        [build],
      );
      await age.query(
        "UPDATE moria.runtime_indexing_stage_closures c SET observed_at=c.observed_at-interval '3 days',scope_hash=moria.runtime_indexing_staging_scope(b) FROM moria.indexing_builds b WHERE c.build=b.id AND b.id=$1",
        [build],
      );
      await age.query(
        'ALTER TABLE moria.runtime_indexing_stage_closures ENABLE TRIGGER runtime_indexing_closure_immutable',
      );
      await age.query('COMMIT');
    } catch (error) {
      await age.query('ROLLBACK');
      throw error;
    } finally {
      age.release();
    }
    // A separate fixture actor now holds the same real barrier on the truly closed build.
    await gate.query('SELECT pg_advisory_lock_shared(hashtextextended($1,0))', [
      'moria/indexing-staging/v1/' + build,
    ]);
    const before = await counts(build),
      busy = await history('once', policyA);
    assert.equal(busy.indexingStaging.state, 'busy');
    assert.equal(busy.indexingStaging.worked, false);
    assert.deepEqual(await counts(build), before);
    await gate.query('SELECT pg_advisory_unlock_shared(hashtextextended($1,0))', [
      'moria/indexing-staging/v1/' + build,
    ]);
    const projection = (
      await db.pool.query(
        'SELECT id,moria.runtime_projection_relation(id) AS relation FROM moria.runtime_projections WHERE stage=$1',
        [build],
      )
    ).rows[0];
    assert.ok(
      projection,
      'committed native projection remains private after its later index step is cancelled',
    );
    assert.equal(
      (
        await db.pool.query('SELECT count(*)::text FROM moria.source_generations WHERE stage=$1', [
          build,
        ])
      ).rows[0].count,
      '0',
    );
    const first = await history('once', policyA);
    assert.equal(first.indexingStaging.mode, 'unpublished');
    assert.equal(first.indexingStaging.removedSpills, '300');
    await history('disable', policyA);
    await assert.rejects(history('once', policyA), /HISTORY_POLICY_REVOKED/);
    const policyB = await approve('staging-b-' + randomUUID());
    await history('plan', policyB);
    await db.pool.query(
      "UPDATE moria.runtime_history_collections SET observed_at=clock_timestamp()-interval '3 days',not_before=clock_timestamp()-interval '1 day' WHERE policy=$1",
      [policyB],
    );
    const aging = await db.pool.connect();
    try {
      await aging.query('BEGIN');
      await aging.query(
        'LOCK TABLE moria.runtime_indexing_stage_closures IN ACCESS EXCLUSIVE MODE',
      );
      await aging.query(
        'ALTER TABLE moria.runtime_indexing_stage_closures DISABLE TRIGGER runtime_indexing_closure_immutable',
      );
      await aging.query(
        "UPDATE moria.runtime_indexing_stage_closures SET observed_at=observed_at-interval '3 days' WHERE build=$1",
        [publishedBuild],
      );
      await aging.query(
        'ALTER TABLE moria.runtime_indexing_stage_closures ENABLE TRIGGER runtime_indexing_closure_immutable',
      );
      await aging.query('COMMIT');
    } catch (error) {
      await aging.query('ROLLBACK');
      throw error;
    } finally {
      aging.release();
    }
    let atProjection = false;
    for (let i = 0; i < 30; i++) {
      const row = (
        await db.pool.query(
          'SELECT phase FROM moria.runtime_indexing_staging_work WHERE build=$1',
          [build],
        )
      ).rows[0];
      if (row?.phase === 'projection') {
        atProjection = true;
        break;
      }
      const step = await history('once', policyB);
      const bytes = Number(step.indexingStaging.storageBytes ?? 0);
      assert.ok(bytes <= 16 * 1024 * 1024);
    }
    assert.ok(atProjection, 'successor policy must continue committed private staging work');
    assert.equal(
      (
        await db.pool.query(
          'SELECT origin_policy FROM moria.runtime_indexing_staging_work WHERE build=$1',
          [build],
        )
      ).rows[0].origin_policy,
      policyA,
    );
    await gate.query(
      "SELECT pg_advisory_lock_shared(hashtextextended('moria/recovery-retention/v1',0))",
    );
    let publishedComplete = false;
    for (let i = 0; i < 12; i++) {
      const held = await history('once', policyB);
      assert.equal(held.indexingStaging.state, 'backupBusy');
      publishedComplete =
        (
          await db.pool.query(
            'SELECT status FROM moria.runtime_history_collections WHERE policy=$1 ORDER BY observed_at DESC LIMIT 1',
            [policyB],
          )
        ).rows[0].status === 'complete';
      if (publishedComplete) break;
    }
    assert.ok(publishedComplete, 'published history lane must complete independently');
    const held = await history('once', policyB);
    assert.equal(held.indexingStaging.state, 'backupBusy');
    assert.equal(held.indexingStaging.pending, true);
    assert.equal(held.state, 'busy', 'finished published work must not hide pending staging work');
    assert.ok(
      (await db.pool.query('SELECT to_regclass($1) AS relation', [projection.relation])).rows[0]
        .relation,
    );
    await gate.query(
      "SELECT pg_advisory_unlock_shared(hashtextextended('moria/recovery-retention/v1',0))",
    );
    let last: any;
    for (let i = 0; i < 30; i++) {
      last = await history('once', policyB);
      if (['complete', 'planRequired'].includes(last.state) && !last.indexingStaging.pending) break;
    }
    assert.ok(['complete', 'planRequired'].includes(last.state), JSON.stringify(last));
    assert.equal(last.indexingStaging.pending, false);
    assert.equal(
      (
        await db.pool.query(
          'SELECT status FROM moria.runtime_history_collections WHERE policy=$1 ORDER BY observed_at DESC LIMIT 1',
          [policyB],
        )
      ).rows[0].status,
      'complete',
    );
    assert.deepEqual(
      (
        await db.pool.query(
          'SELECT phase FROM moria.runtime_indexing_staging_work WHERE build=ANY($1::uuid[]) ORDER BY build',
          [[build, publishedBuild]],
        )
      ).rows.map((row) => row.phase),
      ['complete', 'complete'],
    );
    assert.deepEqual(await counts(build), { spills: '0', keys: '0', rows: '0' });
    assert.equal(
      (await db.pool.query('SELECT to_regclass($1) AS relation', [projection.relation])).rows[0]
        .relation,
      null,
    );
    assert.equal(
      (
        await db.pool.query('SELECT status FROM moria.native_source_row_stores WHERE id=$1', [
          build,
        ])
      ).rows[0].status,
      'retired',
    );
    assert.equal(
      (await db.pool.query('SELECT status FROM moria.source_stages WHERE id=$1', [build])).rows[0]
        .status,
      'failed',
    );
    assert.deepEqual(await counts(publishedBuild), { spills: '0', keys: '300', rows: '300' });
    assert.equal(await input.queryCount(), 150);
    const b = (
      await db.pool.query(
        'SELECT owner,fence::text,status FROM moria.indexing_builds WHERE id=$1',
        [build],
      )
    ).rows[0];
    const c = (
      await db.pool.query(
        'SELECT owner,fence::text,status FROM moria.runtime_indexing_stage_closures WHERE build=$1 ORDER BY sequence DESC LIMIT 1',
        [build],
      )
    ).rows[0];
    assert.deepEqual(b, c);
  } finally {
    await gate.query('SELECT pg_advisory_unlock_all()').catch(() => {});
    gate.release();
    if (child && child.exitCode === null && child.signalCode === null) {
      child.kill('SIGKILL');
      await done?.catch(() => {});
    }
    await db.pool.query(`DROP EVENT TRIGGER IF EXISTS ${tag}`).catch(() => {});
    await db.pool.query(`DROP FUNCTION IF EXISTS moria.${tag}()`).catch(() => {});
  }
}
