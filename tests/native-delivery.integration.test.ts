import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { createServer, type ServerResponse } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { database } from './fixtures/catalog-storage/database.js';
import { Authority } from '../services/security/src/authority.js';

test(
  'native delivery preserves action commits while recovering external outcomes explicitly',
  { timeout: 180000 },
  async (t) => {
    const db = await database();
    t.after(db.close);
    const environment = randomUUID(),
      resource = randomUUID(),
      providerSecret = randomUUID();
    const context = { environment, principal: 'pipeline', application: 'native-delivery' };
    const authority = new Authority(db.catalog);
    await db.catalog.ensureEnvironment(environment);
    const operations = [`${resource}:write`, 'work:execute', 'notify:invoke'];
    await authority.syncPrincipal(environment, 'fixture', 'pipeline', {
      enabled: true,
      kind: 'workload',
      attributes: {},
      markings: ['private', 'secret'],
    });
    await authority.setApplication(environment, 'fixture', 'native-delivery', operations);
    await authority.setGrants(environment, 'fixture', 'pipeline', 'native-delivery', operations);
    let epoch = (await authority.current(environment, 'pipeline', 'native-delivery')).epoch;
    let mode: 'normal' | 'dropAfterApply' | 'holdAfterApply' | 'reject' = 'normal';
    const effects = new Map<string, number>(),
      receipts = new Map<string, unknown>();
    const received = new Set<string>(),
      waiting = new Map<string, () => void>();
    const held = new Map<string, { response: ServerResponse; receipt: unknown }>();
    const observations: { id: string; hash: string; body: string; fence: string }[] = [];
    const sinkErrors: unknown[] = [];
    const sink = createServer(async (request, response) => {
      try {
        const id = String(request.headers['moria-delivery-id']);
        const hash = String(request.headers['moria-payload-sha256']);
        assert.equal(request.headers['idempotency-key'], id);
        assert.equal(request.headers.authorization, `Bearer ${providerSecret}`);
        assert.equal(request.method, 'POST');
        const chunks: Buffer[] = [];
        let size = 0;
        for await (const chunk of request) {
          const bytes = Buffer.from(chunk);
          size += bytes.length;
          assert.ok(size <= 65536);
          chunks.push(bytes);
        }
        const body = Buffer.concat(chunks).toString('utf8');
        assert.equal(createHash('sha256').update(body).digest('hex'), hash);
        observations.push({
          id,
          hash,
          body,
          fence: String(request.headers['moria-delivery-attempt']),
        });
        if (mode === 'reject') {
          response.writeHead(503).end();
          return;
        }
        const idempotent = request.url === '/idempotent';
        if (!idempotent || !receipts.has(id)) {
          effects.set(id, (effects.get(id) ?? 0) + 1);
          receipts.set(id, {
            version: 1,
            deliveryId: id,
            payloadHash: hash,
            receiptId: `fixture-${randomUUID()}`,
          });
        }
        const receipt = receipts.get(id);
        received.add(id);
        waiting.get(id)?.();
        waiting.delete(id);
        if (mode === 'dropAfterApply') {
          response.destroy();
          return;
        }
        if (mode === 'holdAfterApply') {
          held.set(id, { response, receipt });
          return;
        }
        response
          .writeHead(200, { 'content-type': 'application/json' })
          .end(JSON.stringify(receipt));
      } catch (error) {
        sinkErrors.push(error);
        response.destroy();
      }
    });
    await new Promise<void>((resolve) => sink.listen(0, '127.0.0.1', resolve));
    t.after(async () => {
      sink.closeAllConnections();
      await new Promise<void>((resolve) => sink.close(() => resolve()));
    });
    const sinkPort = (sink.address() as { port: number }).port;
    const awaitReceived = async (id: string) => {
      if (received.has(id)) return;
      await Promise.race([
        new Promise<void>((resolve) => waiting.set(id, resolve)),
        delay(10000, undefined, { ref: false }).then(() => {
          throw new Error('No native delivery reached destination');
        }),
      ]);
    };
    const release = (id: string) => {
      const pending = held.get(id);
      assert.ok(pending);
      pending.response
        .writeHead(200, { 'content-type': 'application/json' })
        .end(JSON.stringify(pending.receipt));
      held.delete(id);
    };
    const role = `delivery_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role}; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    const url = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const start = (domain: string, command: string, input: unknown = {}) => {
      const child = spawn(
        process.env.MORIA_INDEXING_BINARY ?? '/usr/local/bin/moria-indexing',
        [domain, command],
        {
          env: {
            PATH: '/usr/local/bin:/usr/bin:/bin',
            MORIA_INDEXING_DATABASE_SECRET_REF: 'DELIVERY_FIXTURE_DATABASE',
            DELIVERY_FIXTURE_DATABASE: url,
            MORIA_INDEXING_DATABASE_TLS: 'disable',
            MORIA_DELIVERY_ALLOW_LOOPBACK_HTTP: '1',
            DELIVERY_FIXTURE_AUTH: providerSecret,
          },
          stdio: ['pipe', 'pipe', 'pipe'],
        },
      );
      let output = '',
        errors = '';
      child.stdout.on('data', (bytes) => (output += bytes.toString()));
      child.stderr.on('data', (bytes) => (errors += bytes.toString()));
      child.stdin.on('error', () => {});
      child.stdin.end(JSON.stringify(input));
      const done = new Promise<any>((resolve, reject) => {
        child.once('error', reject);
        child.once('exit', (code, signal) => {
          if (code !== 0)
            reject(new Error(`Native ${domain} ${command} failed (${code ?? signal}): ${errors}`));
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
      return { child, done };
    };
    const native = (command: string, input: unknown = {}) => start('delivery', command, input).done;
    const once = () => native('once', { owner: randomUUID() });
    const state = (id: string) => native('status', { id });
    const spec = {
      version: 1,
      domain: 'delivery',
      release: 'a'.repeat(64),
      resourceClass: 'standard',
      timeoutSeconds: 120,
      retryPolicy: 'idempotent',
      inputSchema: { type: 'object', properties: {} },
      pinnedInputs: [],
      expectedHeads: {},
    };
    const binding = (domain = 'delivery') =>
      db.pool
        .query(
          `INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation)
    SELECT $1,$2,$3,$4,$5,$6,$7,i.security_epoch,c.incarnation FROM moria.instance_state i,moria.indexing_instance c RETURNING id`,
          [environment, resource, { ...spec, domain }, context, operations, ['private'], epoch],
        )
        .then((result) => result.rows[0].id);
    let registered = await binding();
    const destination = async (idempotent: boolean, allowed = ['private'], maximum = 3) =>
      (
        await db.pool.query(
          `INSERT INTO moria.runtime_delivery_destinations(binding,endpoint,secret_ref,idempotent,allowed_markings,maximum_attempts,retry_seconds)
      VALUES($1,$2,'DELIVERY_FIXTURE_AUTH',$3,$4,$5,1) RETURNING id`,
          [
            registered,
            `http://127.0.0.1:${sinkPort}/${idempotent ? 'idempotent' : 'non-idempotent'}`,
            idempotent,
            allowed,
            maximum,
          ],
        )
      ).rows[0].id;
    const idem = await destination(true),
      nonIdem = await destination(false);
    await db.pool.query('CREATE TABLE public.delivery_business_effects(key text PRIMARY KEY)');
    const action = async (
      key: string,
      dest = idem,
      options: {
        rollback?: boolean;
        labels?: string[];
        beforeCommit?: (id: string) => Promise<void>;
        payload?: unknown;
      } = {},
    ) => {
      let deliveryId = '';
      await db.catalog.transaction(async (client) => {
        const result = await db.catalog.action(
          {
            ...context,
            action: 'notify',
            key,
            input: { key },
            actionRevision: 'r1',
            securityEpoch: epoch,
          },
          async (c) => {
            await c.query('INSERT INTO public.delivery_business_effects(key) VALUES($1)', [key]);
            return { changed: true };
          },
          (c) =>
            authority.assertCurrent(c, environment, context.principal, context.application, epoch),
          client,
        );
        deliveryId = (
          await client.query('SELECT moria.runtime_delivery_enqueue($1,$2,$3,$4,$5)', [
            dest,
            'notification',
            options.payload ?? { version: 1, key, result },
            options.labels ?? ['private'],
            { kind: 'action', ...context, action: 'notify', key },
          ])
        ).rows[0].runtime_delivery_enqueue;
        await options.beforeCommit?.(deliveryId);
        if (options.rollback) throw new Error('Injected action rollback');
      }, environment);
      return deliveryId;
    };
    const available = async (id: string) =>
      db.pool.query(
        'UPDATE moria.runtime_deliveries SET available_at=clock_timestamp() WHERE id=$1',
        [id],
      );
    const expire = async (id: string) => {
      await db.pool.query(
        "UPDATE moria.runtime_deliveries SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
        [id],
      );
      await db.pool.query('SELECT moria.runtime_delivery_reap()');
      await available(id);
    };

    await t.test(
      'action effects, receipt and outbox intent roll back together and cannot dispatch early',
      async () => {
        const before = observations.length;
        await assert.rejects(
          action('rolled-back', idem, {
            rollback: true,
            beforeCommit: async () => {
              assert.equal((await once()).worked, false);
              assert.equal(observations.length, before);
            },
          }),
          /Injected action rollback/,
        );
        assert.equal(
          (await db.pool.query("SELECT count(*) FROM moria.action_keys WHERE key='rolled-back'"))
            .rows[0].count,
          '0',
        );
        assert.equal(
          (await db.pool.query('SELECT count(*) FROM public.delivery_business_effects')).rows[0]
            .count,
          '0',
        );
        assert.equal(
          (await db.pool.query('SELECT count(*) FROM moria.runtime_deliveries')).rows[0].count,
          '0',
        );
        const id = await action('published');
        assert.equal(await action('published'), id);
        await assert.rejects(
          action('published', idem, { payload: { changed: 'payload' } }),
          /IDEMPOTENCY_CONFLICT/,
        );
        assert.equal((await once()).worked, true);
        assert.equal((await state(id)).status, 'delivered');
        assert.equal(effects.get(id), 1);
        await native('cancel', { id });
        assert.equal((await state(id)).status, 'delivered');
        assert.equal(
          (await db.pool.query('SELECT count(*) FROM public.delivery_business_effects')).rows[0]
            .count,
          '1',
        );
      },
    );

    await t.test(
      'execution and delivery claims share one global/environment resource budget',
      async () => {
        await db.pool.query(
          "UPDATE moria.runtime_resource_classes SET maximum_tasks=1,maximum_environment_tasks=1 WHERE name='standard'",
        );
        const executionBinding = await binding('transform');
        const execution = await start('execution', 'request', {
          binding: executionBinding,
          requestKey: 'shared-budget',
          input: {},
          tasks: [{ key: 'work', partition: null }],
        }).done;
        const id = await action('budget');
        const contenders = await Promise.all([
          start('execution', 'claim', { owner: randomUUID(), domains: ['transform'] }).done,
          native('claim', { owner: randomUUID() }),
        ]);
        assert.equal(contenders.filter(Boolean).length, 1);
        assert.equal(
          (await db.pool.query("SELECT moria.runtime_resource_in_use('standard')::text AS active"))
            .rows[0].active,
          '1',
        );
        assert.equal(await native('claim', { owner: randomUUID() }), null);
        assert.equal(
          await start('execution', 'claim', { owner: randomUUID(), domains: ['transform'] }).done,
          null,
        );
        await start('execution', 'cancel', { id: execution.id }).done;
        await native('cancel', { id });
        assert.equal((await state(id)).status, 'cancelled');
        assert.equal(
          (await db.pool.query("SELECT moria.runtime_resource_in_use('standard')::text AS active"))
            .rows[0].active,
          '0',
        );
        await db.pool.query(
          "UPDATE moria.runtime_resource_classes SET maximum_tasks=2,maximum_environment_tasks=2 WHERE name='standard'",
        );
      },
    );

    await t.test(
      'response loss reuses identical bytes and one stable key at an idempotent destination',
      async () => {
        const id = await action('idempotent-response-loss');
        mode = 'dropAfterApply';
        await once();
        assert.equal((await state(id)).status, 'retryScheduled');
        assert.equal(effects.get(id), 1);
        mode = 'normal';
        await available(id);
        await once();
        assert.equal((await state(id)).status, 'delivered');
        assert.equal(effects.get(id), 1);
        const sent = observations.filter((item) => item.id === id);
        assert.equal(sent.length, 2);
        assert.equal(sent[0].body, sent[1].body);
        assert.equal(sent[0].hash, sent[1].hash);
        assert.notEqual(sent[0].fence, sent[1].fence);
        assert.equal(
          (
            await db.pool.query(
              "SELECT result FROM moria.action_keys WHERE key='idempotent-response-loss'",
            )
          ).rows[0].result.changed,
          true,
        );
      },
    );

    await t.test(
      'a real killed dispatcher is reclaimed without a duplicate idempotent effect',
      async () => {
        const id = await action('dispatcher-crash');
        mode = 'holdAfterApply';
        const running = start('delivery', 'once', { owner: randomUUID() });
        await awaitReceived(id);
        running.child.kill('SIGKILL');
        await assert.rejects(running.done);
        release(id);
        assert.equal((await state(id)).status, 'delivering');
        await expire(id);
        mode = 'normal';
        await once();
        assert.equal((await state(id)).status, 'delivered');
        assert.equal(effects.get(id), 1);
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.runtime_delivery_attempts WHERE delivery=$1',
              [id],
            )
          ).rows[0].count,
          '2',
        );
      },
    );

    await t.test(
      'non-idempotent uncertainty cannot retry until explicit provider evidence is reconciled',
      async () => {
        const id = await action('uncertain-effect', nonIdem);
        mode = 'dropAfterApply';
        await once();
        assert.equal((await state(id)).status, 'uncertain');
        assert.equal(effects.get(id), 1);
        assert.equal((await once()).worked, false);
        const current = await state(id);
        await assert.rejects(native('retry', { id, expectedFence: current.fence }));
        await assert.rejects(
          native('reconcile', {
            id,
            expectedFence: '999',
            resolution: 'delivered',
            evidence: 'fixture:confirmed',
          }),
        );
        await native('reconcile', {
          id,
          expectedFence: current.fence,
          resolution: 'delivered',
          evidence: `fixture:terminal-receipt/${id}`,
        });
        assert.equal((await state(id)).status, 'delivered');
        mode = 'normal';
        assert.equal((await once()).worked, false);
        assert.equal(effects.get(id), 1);
      },
    );

    await t.test(
      'a killed non-idempotent dispatcher remains uncertain without another send',
      async () => {
        const id = await action('non-idempotent-crash', nonIdem);
        mode = 'holdAfterApply';
        const running = start('delivery', 'once', { owner: randomUUID() });
        await awaitReceived(id);
        running.child.kill('SIGKILL');
        await assert.rejects(running.done);
        release(id);
        await expire(id);
        assert.equal((await state(id)).status, 'uncertain');
        mode = 'normal';
        assert.equal((await once()).worked, false);
        assert.equal(observations.filter((entry) => entry.id === id).length, 1);
        const uncertain = await state(id);
        await native('reconcile', {
          id,
          expectedFence: uncertain.fence,
          resolution: 'delivered',
          evidence: `fixture:crashed-after-terminal-receipt/${id}`,
        });
        assert.equal((await state(id)).status, 'delivered');
        assert.equal(effects.get(id), 1);
      },
    );

    await t.test(
      'cancellation after dispatch reports uncertainty and cannot undo an external effect',
      async () => {
        const id = await action('cancel-during-send', nonIdem);
        mode = 'holdAfterApply';
        const running = start('delivery', 'once', { owner: randomUUID() });
        await awaitReceived(id);
        await native('cancel', { id });
        assert.equal((await state(id)).status, 'uncertain');
        release(id);
        await running.done;
        const current = await state(id);
        assert.equal(current.status, 'uncertain');
        assert.equal(effects.get(id), 1);
        await native('reconcile', {
          id,
          expectedFence: current.fence,
          resolution: 'delivered',
          evidence: `fixture:cancelled-after-terminal-receipt/${id}`,
        });
        assert.equal((await state(id)).status, 'delivered');
        mode = 'normal';
      },
    );

    await t.test(
      'bounded dead letters retain attempts and permit explicit same-payload retry',
      async () => {
        const limited = await destination(true, ['private'], 2);
        const id = await action('dead-letter', limited);
        mode = 'reject';
        await once();
        await available(id);
        await once();
        const failed = await state(id);
        assert.equal(failed.status, 'failed');
        assert.equal(failed.attempts, 2);
        assert.equal(effects.get(id), undefined);
        await native('retry', { id, expectedFence: failed.fence });
        mode = 'normal';
        await once();
        const delivered = await state(id);
        assert.equal(delivered.status, 'delivered');
        assert.equal(delivered.attempts, 3);
        assert.equal(effects.get(id), 1);
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.runtime_delivery_attempts WHERE delivery=$1',
              [id],
            )
          ).rows[0].count,
          '3',
        );
      },
    );

    await t.test(
      'a proven terminal non-delivery can resume, while an abandoned unsent claim is safely fenced',
      async () => {
        const id = await action('terminal-non-delivery', nonIdem);
        mode = 'reject';
        await once();
        const uncertain = await state(id);
        assert.equal(uncertain.status, 'uncertain');
        assert.equal(effects.get(id), undefined);
        await native('reconcile', {
          id,
          expectedFence: uncertain.fence,
          resolution: 'notDelivered',
          evidence: `fixture:request-drained-no-effect/${id}`,
        });
        mode = 'normal';
        await once();
        assert.equal((await state(id)).status, 'delivered');
        assert.equal(effects.get(id), 1);
        const unsent = await action('unsent-claim', nonIdem);
        const old = await native('claim', { owner: randomUUID() });
        assert.equal(old.lease.delivery, unsent);
        await expire(unsent);
        const replacement = await native('claim', { owner: randomUUID() });
        assert.equal(replacement.lease.delivery, unsent);
        assert.ok(BigInt(replacement.lease.fence) > BigInt(old.lease.fence));
        await assert.rejects(native('renew', old.lease));
        await native('cancel', { id: unsent });
        assert.equal((await state(unsent)).status, 'cancelled');
      },
    );

    await t.test(
      'destination marking limits fail the action transaction and revocation prevents dispatch',
      async () => {
        await assert.rejects(
          action('marking-denied', idem, { labels: ['private', 'secret'] }),
          /DELIVERY_FORBIDDEN/,
        );
        assert.equal(
          (await db.pool.query("SELECT count(*) FROM moria.action_keys WHERE key='marking-denied'"))
            .rows[0].count,
          '0',
        );
        const id = await action('revoked-delivery');
        const before = observations.length;
        await authority.setGrants(environment, 'fixture', 'pipeline', 'native-delivery', [
          'notify:invoke',
        ]);
        await once();
        assert.equal(observations.length, before);
        assert.equal(
          (await db.pool.query('SELECT status FROM moria.runtime_deliveries WHERE id=$1', [id]))
            .rows[0].status,
          'failed',
        );
        assert.equal(
          (await db.pool.query("SELECT result FROM moria.action_keys WHERE key='revoked-delivery'"))
            .rows[0].result.changed,
          true,
        );
        await assert.rejects(state(id));
        assert.deepEqual(sinkErrors, []);
      },
    );
    await t.test(
      'an unobserved destination disable and restore never revives queued delivery',
      async () => {
        await authority.setGrants(
          environment,
          'fixture',
          'pipeline',
          'native-delivery',
          operations,
        );
        epoch = (await authority.current(environment, 'pipeline', 'native-delivery')).epoch;
        registered = await binding();
        const target = await destination(true),
          id = await action('destination-disable-restore', target),
          before = observations.length;
        const client = await db.pool.connect();
        try {
          await client.query('BEGIN');
          await client.query(
            'UPDATE moria.runtime_delivery_destinations SET enabled=false WHERE id=$1',
            [target],
          );
          await client.query(
            'UPDATE moria.runtime_delivery_destinations SET enabled=true WHERE id=$1',
            [target],
          );
          await client.query('COMMIT');
        } finally {
          client.release();
        }
        assert.equal(
          BigInt((await authority.current(environment, 'pipeline', 'native-delivery')).epoch),
          BigInt(epoch) + 2n,
        );
        await once();
        assert.equal(observations.length, before);
        assert.equal(
          (await db.pool.query('SELECT status FROM moria.runtime_deliveries WHERE id=$1', [id]))
            .rows[0].status,
          'failed',
        );
        assert.equal(
          (
            await db.pool.query('SELECT result FROM moria.action_keys WHERE key=$1', [
              'destination-disable-restore',
            ])
          ).rows[0].result.changed,
          true,
        );
        await assert.rejects(state(id), /AUTHORIZATION_CHANGED/);
      },
    );
  },
);
