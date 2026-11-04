import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { requestCommitProxy } from './fixtures/runtime/postgres-proxy.js';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { database } from './fixtures/catalog-storage/database.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';

test(
  'native execution authority schedules, fences and composes completion with real publication',
  { timeout: 180000 },
  async (t) => {
    const db = await database();
    t.after(db.close);
    const environment = randomUUID(),
      resource = randomUUID();
    const authority = new Authority(db.catalog);
    await db.catalog.ensureEnvironment(environment);
    const operations = ['work:execute'];
    await authority.syncPrincipal(environment, 'fixture', 'pipeline', {
      enabled: true,
      kind: 'workload',
      attributes: {},
      markings: ['private'],
    });
    await authority.setApplication(environment, 'fixture', 'native-runtime', operations);
    await authority.setGrants(environment, 'fixture', 'pipeline', 'native-runtime', operations);
    const context: {
      environment: string;
      principal: string;
      application: string;
      sessionHash?: string;
    } = { environment, principal: 'pipeline', application: 'native-runtime' };
    const epoch = (await authority.current(environment, 'pipeline', 'native-runtime')).epoch;
    const role = `execution_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role}; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    await db.pool.query(
      "UPDATE moria.runtime_resource_classes SET maximum_tasks=2,maximum_environment_tasks=2 WHERE name='standard'",
    );
    const url = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const start = (
      command: string,
      input: unknown = {},
      connection = url,
      keepInputOpen = false,
    ) => {
      const child = spawn(
        process.env.MORIA_INDEXING_BINARY ?? '/usr/local/bin/moria-indexing',
        ['execution', command],
        {
          env: {
            PATH: '/usr/local/bin:/usr/bin:/bin',
            MORIA_INDEXING_DATABASE_SECRET_REF: 'EXECUTION_FIXTURE_DATABASE',
            EXECUTION_FIXTURE_DATABASE: connection,
            MORIA_INDEXING_DATABASE_TLS: 'disable',
          },
          stdio: ['pipe', 'pipe', 'pipe'],
        },
      );
      let output = '',
        errors = '';
      child.stdout.on('data', (bytes) => (output += bytes.toString()));
      child.stderr.on('data', (bytes) => (errors += bytes.toString()));
      child.stdin.on('error', () => {});
      if (!keepInputOpen) child.stdin.end(JSON.stringify(input));
      const done = new Promise<any>((resolve, reject) => {
        child.once('error', reject);
        child.once('exit', (code, signal) => {
          if (code !== 0)
            reject(new Error(`Native execution ${command} failed (${code ?? signal}): ${errors}`));
          else {
            try {
              resolve(JSON.parse(output));
            } catch (error) {
              reject(error);
            }
          }
        });
      });
      void done.catch(() => {});
      t.after(() => child.kill('SIGKILL'));
      return { child, done, output: () => output };
    };
    const native = (command: string, input: unknown = {}) => start(command, input).done;
    const taskPlans = (count = 1) =>
      Array.from({ length: count }, (_, partition) => ({
        key: `partition-${partition}`,
        partition,
      }));
    const baseSpec = {
      version: 1,
      domain: 'transform',
      release: 'a'.repeat(64),
      resourceClass: 'standard',
      timeoutSeconds: 120,
      retryPolicy: 'idempotent',
      inputSchema: {
        type: 'object',
        properties: { amount: { type: 'decimal', precision: 8, scale: 2, nullable: false } },
      },
      pinnedInputs: [
        { resource: randomUUID(), revision: '9007199254740993', snapshot: 'snapshot-1' },
      ],
      expectedHeads: {},
    };
    // Scheduler metadata fixture; physical Dataset reads are exercised by native catalog tests.
    const pinned = baseSpec.pinnedInputs[0];
    await db.pool.query(
      'INSERT INTO moria.dataset_heads(environment,dataset,revision) VALUES($1,$2,$3)',
      [environment, pinned.resource, pinned.revision],
    );
    await db.pool.query(
      'INSERT INTO moria.snapshot_history(environment,dataset,revision,metadata_location,snapshot_id,execution,lineage) VALUES($1,$2,$3,$4,$5,$6,$7)',
      [
        environment,
        pinned.resource,
        pinned.revision,
        `fixture://execution/${pinned.resource}/metadata.json`,
        pinned.snapshot,
        randomUUID(),
        {},
      ],
    );
    const binding = async (changes: Record<string, unknown> = {}, actor = context) =>
      (
        await db.pool.query(
          `INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation)
      SELECT $1,$2,$3,$4,$5,$6,$7,i.security_epoch,c.incarnation FROM moria.instance_state i,moria.indexing_instance c RETURNING id`,
          [
            environment,
            resource,
            { ...baseSpec, ...changes },
            actor,
            operations,
            ['private'],
            epoch,
          ],
        )
      ).rows[0].id;
    const registered = await binding();
    const request = (requestKey: string, count = 1, bindingId = registered) => ({
      binding: bindingId,
      requestKey,
      input: { amount: '1.2' },
      tasks: taskPlans(count),
    });
    const claim = (owner = randomUUID(), domains = ['transform']) =>
      native('claim', { owner, domains });
    const sealed = (lease: any) =>
      native('seal', { lease, result: { staged: true }, digest: 'b'.repeat(64) });

    await t.test(
      'typed logical requests deduplicate and their pinned inputs cannot mutate',
      async () => {
        const first = await native('request', request('typed-replay', 2));
        assert.equal(
          (
            await db.pool.query(
              "SELECT count(*) FROM moria.runtime_dataset_root_pins WHERE owner_kind='execution' AND owner_id=$1",
              [first.id],
            )
          ).rows[0].count,
          '1',
        );
        assert.equal(
          (await native('request', { ...request('typed-replay', 2), input: { amount: '1.20' } }))
            .id,
          first.id,
        );
        await assert.rejects(
          native('request', { ...request('typed-replay', 2), input: { amount: '2.00' } }),
        );
        await assert.rejects(native('request', request('typed-replay', 1)));
        await assert.rejects(
          db.pool.query(
            "UPDATE moria.runtime_executions SET spec=jsonb_set(spec,'{release}','\"changed\"') WHERE id=$1",
            [first.id],
          ),
          /IMMUTABLE_EXECUTION/,
        );
        await assert.rejects(
          db.pool.query(
            "UPDATE moria.runtime_execution_bindings SET spec=jsonb_set(spec,'{release}','\"changed\"') WHERE id=$1",
            [registered],
          ),
          /IMMUTABLE_EXECUTION_BINDING/,
        );
        assert.equal(
          (await native('status', { id: first.id })).spec.pinnedInputs[0].revision,
          '9007199254740993',
        );
      },
    );

    await t.test(
      'independent native claims obey class admission and stale owners cannot renew or seal',
      async () => {
        const owners = [randomUUID(), randomUUID()];
        const claims = await Promise.all(owners.map((owner) => claim(owner)));
        for (let index = 0; index < claims.length; index++) {
          for (let attempt = 0; claims[index] === null && attempt < 20; attempt++)
            claims[index] = await claim(owners[index]);
          assert.ok(claims[index]);
        }
        const [first, second] = claims;
        assert.notEqual(first.lease.task, second.lease.task);
        assert.notEqual(first.lease.owner, second.lease.owner);
        assert.equal(typeof first.lease.fence, 'string');
        const queued = await native('request', request('cancel-queued'));
        assert.equal(await claim(), null, 'a third task cannot exceed the class reservation');
        await db.pool.query(
          "UPDATE moria.runtime_execution_tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
          [first.lease.task],
        );
        const replacement = await claim();
        assert.equal(replacement.lease.task, first.lease.task);
        assert.ok(BigInt(replacement.lease.fence) > BigInt(first.lease.fence));
        await assert.rejects(native('renew', first.lease));
        await assert.rejects(sealed(first.lease));
        await native('renew', replacement.lease);
        await sealed(replacement.lease);
        await sealed(replacement.lease);
        await sealed(second.lease);
        const events = await native('events', { id: first.lease.execution, after: '0' });
        assert.deepEqual(
          events.map((event: any) => BigInt(event.sequence)),
          events.map((_: unknown, i: number) => BigInt(i + 1)),
        );
        assert.ok(events.some((event: any) => event.kind === 'retryScheduled'));
        await native('cancel', { id: queued.id });
        assert.equal((await native('status', { id: queued.id })).status, 'cancelled');
        assert.equal(await claim(), null);
      },
    );

    await t.test(
      'task sealing is private; publication and business completion commit or roll back together',
      async () => {
        const prepared = await native('prepare', { owner: randomUUID(), domains: ['transform'] });
        assert.ok(prepared);
        assert.equal(prepared.lease.task, null);
        const id = prepared.lease.execution;
        assert.equal((await native('status', { id })).status, 'preparing');
        await assert.rejects(
          native('complete', { lease: prepared.lease, result: { published: true } }),
        );
        await db.pool.query(
          'CREATE TABLE public.native_execution_publications(id uuid PRIMARY KEY,result jsonb NOT NULL)',
        );
        const client = await db.pool.connect();
        const complete = () =>
          client.query('SELECT moria.runtime_execution_complete($1,$2,$3,$4)', [
            id,
            prepared.lease.owner,
            prepared.lease.fence,
            { published: true },
          ]);
        try {
          await client.query('BEGIN');
          await client.query('INSERT INTO public.native_execution_publications VALUES($1,$2)', [
            id,
            { published: true },
          ]);
          await complete();
          assert.equal(
            (await db.pool.query('SELECT status FROM moria.runtime_executions WHERE id=$1', [id]))
              .rows[0].status,
            'preparing',
          );
          await client.query('ROLLBACK');
          assert.equal(
            (await db.pool.query('SELECT count(*) FROM public.native_execution_publications'))
              .rows[0].count,
            '0',
          );
          assert.equal((await native('status', { id })).status, 'preparing');
          await client.query('BEGIN');
          await client.query('INSERT INTO public.native_execution_publications VALUES($1,$2)', [
            id,
            { published: true },
          ]);
          await complete();
          await client.query('COMMIT');
          await complete();
          await native('cancel', { id });
          const result = await native('status', { id });
          assert.equal(result.status, 'succeeded');
          assert.deepEqual(result.result, { published: true });
          assert.equal(
            (await db.pool.query('SELECT count(*) FROM public.native_execution_publications'))
              .rows[0].count,
            '1',
          );
        } finally {
          await client.query('ROLLBACK');
          client.release();
        }
      },
    );

    await t.test(
      'accepted work survives killing Rust before the real commit response arrives',
      async () => {
        const proxy = await requestCommitProxy(Number(db.pool.options.port));
        t.after(proxy.close);
        const pending = start(
          'request',
          request('lost-response'),
          `postgresql://${role}:${password}@127.0.0.1:${proxy.port}/postgres`,
        );
        await Promise.race([
          proxy.committed,
          delay(10000, undefined, { ref: false }).then(() => {
            throw new Error('No native request commit reached proxy');
          }),
        ]);
        const accepted = (
          await db.pool.query(
            "SELECT id FROM moria.runtime_executions WHERE request_key='lost-response'",
          )
        ).rows[0].id;
        pending.child.kill('SIGKILL');
        await assert.rejects(pending.done);
        assert.equal((await native('request', request('lost-response'))).id, accepted);
        assert.equal(
          (
            await db.pool.query(
              "SELECT count(*) FROM moria.runtime_executions WHERE request_key='lost-response'",
            )
          ).rows[0].count,
          '1',
        );
        await native('cancel', { id: accepted });
      },
    );

    await t.test('single-attempt function work is never re-executed after lease loss', async () => {
      const host = randomUUID();
      const sandboxClaim = async () => {
        const owner = randomUUID();
        await db.pool.query(
          "INSERT INTO moria.runtime_function_workers(owner,host_id,incarnation,resource_classes,expires_at) SELECT $1,$2,incarnation,ARRAY['standard'],clock_timestamp()+interval '1 hour' FROM moria.indexing_instance",
          [owner, host],
        );
        const child = spawn(
          process.env.MORIA_NATIVE_SANDBOX_TEST_BINARY ??
            '/usr/local/bin/moria-native-sandbox-test',
          ['--ignored', '--exact', 'native_sandbox_driver', '--nocapture'],
          {
            env: {
              PATH: '/usr/local/bin:/usr/bin:/bin',
              MORIA_INDEXING_DATABASE_SECRET_REF: 'EXECUTION_FIXTURE_DATABASE',
              EXECUTION_FIXTURE_DATABASE: url,
              MORIA_INDEXING_DATABASE_TLS: 'disable',
              MORIA_SANDBOX_TEST_HOST: host,
              MORIA_SANDBOX_TEST_CLAIM_OWNER: owner,
            },
            stdio: ['ignore', 'pipe', 'pipe'],
          },
        );
        let output = '',
          error = '';
        child.stdout.on('data', (b) => (output += b));
        child.stderr.on('data', (b) => (error += b));
        await new Promise<void>((resolve, reject) => {
          child.once('error', reject);
          child.once('exit', (code) => (code === 0 ? resolve() : reject(new Error(error))));
        });
        const line = output.split('\n').find((line) => line.startsWith('SANDBOX_RESULT='));
        assert.ok(line);
        return JSON.parse(line.slice('SANDBOX_RESULT='.length));
      };
      const functionBinding = await binding({
        domain: 'function',
        retryPolicy: 'never',
        timeoutSeconds: 30,
      });
      await assert.rejects(
        native('request', request('invalid-function-partitions', 2, functionBinding)),
      );
      const accepted = await native('request', request('single-attempt', 1, functionBinding));
      await assert.rejects(claim(randomUUID(), ['function']));
      const work = await sandboxClaim();
      assert.equal(work.lease.execution, accepted.id);
      await db.pool.query(
        "UPDATE moria.runtime_execution_tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
        [work.lease.task],
      );
      assert.equal(await sandboxClaim(), null);
      assert.equal((await native('status', { id: accepted.id })).status, 'failed');
      assert.equal(
        (
          await db.pool.query(
            'SELECT count(*) FROM moria.runtime_execution_attempts a JOIN moria.runtime_execution_tasks t ON t.id=a.task WHERE t.execution=$1',
            [accepted.id],
          )
        ).rows[0].count,
        '1',
      );
      await assert.rejects(sealed(work.lease));
    });

    await t.test('database deadlines and event bounds remain authoritative', async () => {
      const limited = await binding({ timeoutSeconds: 3 });
      const accepted = await native('request', request('short-deadline', 1, limited));
      const work = await claim();
      assert.equal(work.lease.execution, accepted.id);
      const first = await native('event', { lease: work.lease, payload: { phase: 'reading' } });
      assert.equal(typeof first.sequence, 'string');
      await db.pool.query('UPDATE moria.runtime_executions SET custom_events=256 WHERE id=$1', [
        accepted.id,
      ]);
      await assert.rejects(native('event', { lease: work.lease, payload: { phase: 'overflow' } }));
      await delay(3100);
      await native('tick');
      const result = await native('status', { id: accepted.id });
      assert.equal(result.status, 'failed');
      assert.equal(result.error, 'DEADLINE_EXCEEDED');
      await assert.rejects(native('renew', work.lease));
    });

    await t.test('disabled sessions fail dispatch before any attempt is recorded', async () => {
      const sessions = new WorkloadSessions(authority);
      const credentials = await sessions.mint(context, 'fixture');
      const session = await sessions.authenticate(`Bearer ${credentials.token}`);
      const sessionBinding = await binding({}, { ...context, ...session });
      const accepted = await native('request', request('session-revoked', 1, sessionBinding));
      await db.pool.query('UPDATE moria.workload_sessions SET disabled=true WHERE token_hash=$1', [
        session.sessionHash,
      ]);
      assert.equal(await claim(), null);
      const result = (
        await db.pool.query('SELECT status,error FROM moria.runtime_executions WHERE id=$1', [
          accepted.id,
        ])
      ).rows[0];
      assert.equal(result.status, 'failed');
      assert.equal(result.error, 'UNAUTHENTICATED');
      assert.equal(
        (
          await db.pool.query(
            'SELECT count(*) FROM moria.runtime_execution_attempts a JOIN moria.runtime_execution_tasks t ON t.id=a.task WHERE t.execution=$1',
            [accepted.id],
          )
        ).rows[0].count,
        '0',
      );
      await assert.rejects(native('status', { id: accepted.id }));
    });

    await t.test('persistent native scheduler starts before inherited stdin closes', async () => {
      const automation = randomUUID();
      await native('schedule', {
        id: automation,
        binding: registered,
        interval: 60,
        enabled: true,
        input: { amount: '1.20' },
        tasks: taskPlans(),
      });
      const scheduler = start('scheduler', {}, url, true);
      try {
        const deadline = Date.now() + 5000;
        let fired = false;
        while (Date.now() < deadline) {
          fired =
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.runtime_automation_firings WHERE automation=$1',
                [automation],
              )
            ).rows[0].count === '1' && scheduler.output().includes('"event":"scheduled"');
          if (fired) break;
          await delay(20);
        }
        assert.equal(
          fired,
          true,
          'Native scheduler must run while its inherited stdin remains open',
        );
        assert.equal(scheduler.child.stdin.writableEnded, false);
      } finally {
        scheduler.child.kill('SIGTERM');
      }
      assert.deepEqual(await scheduler.done, { event: 'scheduled', count: 1 });
      await db.pool.query('UPDATE moria.runtime_automations SET enabled=false WHERE id=$1', [
        automation,
      ]);
    });
    await t.test(
      'parallel schedulers commit one firing, coalesce missed intervals and audit overlap/revocation',
      async () => {
        const automation = randomUUID();
        const schedule = {
          id: automation,
          binding: registered,
          interval: 60,
          enabled: true,
          input: { amount: '1.20' },
          tasks: taskPlans(),
        };
        await native('schedule', schedule);
        const results = await Promise.all([native('tick'), native('tick')]);
        assert.equal(
          results.reduce((sum, result) => sum + result.scheduled, 0),
          1,
        );
        assert.deepEqual(
          (
            await db.pool.query(
              'SELECT status FROM moria.runtime_automation_firings WHERE automation=$1',
              [automation],
            )
          ).rows,
          [{ status: 'enqueued' }],
        );
        await db.pool.query(
          "UPDATE moria.runtime_automations SET next_run=clock_timestamp()-interval '10 days' WHERE id=$1",
          [automation],
        );
        await native('tick');
        assert.equal((await native('tick')).scheduled, 0);
        assert.deepEqual(
          (
            await db.pool.query(
              'SELECT status FROM moria.runtime_automation_firings WHERE automation=$1 ORDER BY created_at',
              [automation],
            )
          ).rows.map((row) => row.status),
          ['enqueued', 'overlap'],
        );
        assert.equal(
          (
            await db.pool.query(
              'SELECT next_run>clock_timestamp() AS future FROM moria.runtime_automations WHERE id=$1',
              [automation],
            )
          ).rows[0].future,
          true,
        );
        const lost = randomUUID();
        await native('schedule', { ...schedule, id: lost });
        const proxy = await requestCommitProxy(
          Number(db.pool.options.port),
          'runtime_schedule_tick',
        );
        t.after(proxy.close);
        const firing = start(
          'tick',
          {},
          `postgresql://${role}:${password}@127.0.0.1:${proxy.port}/postgres`,
        );
        await Promise.race([
          proxy.committed,
          delay(10000, undefined, { ref: false }).then(() => {
            throw new Error('No native scheduler commit reached proxy');
          }),
        ]);
        firing.child.kill('SIGKILL');
        await assert.rejects(firing.done);
        assert.equal((await native('tick')).scheduled, 0);
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.runtime_automation_firings WHERE automation=$1',
              [lost],
            )
          ).rows[0].count,
          '1',
        );
        await authority.setGrants(environment, 'fixture', 'pipeline', 'native-runtime', []);
        await db.pool.query(
          "UPDATE moria.runtime_automations SET next_run=clock_timestamp()-interval '1 minute' WHERE id=$1",
          [automation],
        );
        await native('tick');
        assert.equal(
          (
            await db.pool.query(
              "SELECT count(*) FROM moria.runtime_automation_firings WHERE automation=$1 AND status='denied'",
              [automation],
            )
          ).rows[0].count,
          '1',
        );
        await assert.rejects(native('request', request('revoked')));
      },
    );
  },
);
