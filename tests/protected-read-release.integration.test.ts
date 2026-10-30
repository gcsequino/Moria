import assert from 'node:assert/strict';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { randomUUID } from 'node:crypto';
import { database } from './fixtures/catalog-storage/database.js';
import { ObjectStore, type ObjectSchema } from '../services/ontology/src/store.js';
import { ObjectService, type SessionContext } from '../services/ontology/src/service.js';
import { Authority } from '../services/security/src/authority.js';
import { createApi } from '../apps/api/src/server.js';
import { MoriaError } from '../packages/contracts/types.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';

test(
  'protected object reads recheck session expiry after blocked evaluation',
  { timeout: 60000 },
  async (t) => {
    const db = await database();
    t.after(db.close);
    const { catalog, pool } = db;
    const environment = 'read-release';
    await catalog.ensureEnvironment(environment);
    const authority = new Authority(catalog);
    await authority.syncPrincipal(environment, 'operator', 'reader', {
      enabled: true,
      kind: 'workload',
      attributes: {},
      markings: [],
    });
    await authority.setApplication(environment, 'operator', 'app', ['assets:read']);
    await authority.setGrants(environment, 'operator', 'reader', 'app', ['assets:read']);
    const state = await authority.current(environment, 'reader', 'app');
    const store = new ObjectStore(catalog);
    const uid = await catalog.resolveIdentity('read-release', 'one');
    const schema: ObjectSchema = {
      primaryKey: 'uid',
      properties: {
        uid: { type: 'uid', nullable: false },
        value: { type: 'string', nullable: false },
      },
    };
    await store.activate(
      environment,
      'assets',
      schema,
      [{ uid, value: 'protected' }],
      state.epoch,
      '0',
    );
    const service = new ObjectService(store, authority, {
      schema,
      policies: {
        resource: 'assets',
        markings: [],
        properties: schema.properties,
        sourceColumns: schema.properties,
        attributes: {},
      },
    });
    const sessions = new WorkloadSessions(authority);
    const tokens = new Map<string, string>();
    const session = async () => {
      const minted = await sessions.mint(
        { environment, principal: 'reader', application: 'app' },
        'operator',
      );
      const context = await sessions.authenticate(`Bearer ${minted.token}`);
      tokens.set(context.sessionHash!, minted.token);
      return context;
    };
    const api = createApi(sessions, {
      objects: new Map([['assets', service]]),
      actions: new Map(),
    });
    await new Promise<void>((ready) => api.listen(0, '127.0.0.1', ready));
    t.after(
      () =>
        new Promise<void>((done, reject) => api.close((error) => (error ? reject(error) : done()))),
    );
    const base = `http://127.0.0.1:${(api.address() as { port: number }).port}`;
    const routed = async (context: SessionContext, query: boolean) => {
      const response = await fetch(`${base}/api/v1/objects/assets/${query ? 'query' : uid}`, {
        method: query ? 'POST' : 'GET',
        headers: {
          authorization: `Bearer ${tokens.get(context.sessionHash!)}`,
          'content-type': 'application/json',
        },
        ...(query ? { body: '{}' } : {}),
      });
      const result = await response.json();
      if (!response.ok) throw new MoriaError(result.error.code, result.error.message);
      return result;
    };
    const operations: [string, (context: SessionContext) => Promise<unknown>][] = [
      ['point read', (context) => service.read(context, uid)],
      [
        'transaction point read',
        (context) =>
          catalog.transaction(
            (client) => service.read(context, uid, undefined, client),
            context.environment,
            'repeatable-read',
            'shared',
          ),
      ],
      ['query control', (context) => service.query(context, {})],
      ['HTTP point read', (context) => routed(context, false)],
      ['HTTP query', (context) => routed(context, true)],
    ];
    await t.test('composition cannot expose stores or weaken existing gates', async () => {
      const current = await session();
      assert.equal('store' in service, false);
      assert.equal('authority' in service, false);
      const restricted = service.requiring([{ resource: 'instance', operation: 'manage' }]);
      const declaration = restricted.binding;
      declaration.requiredOperations = [];
      declaration.policies.markings.push('changed-outside');
      await assert.rejects(restricted.read(current, uid), { code: 'FORBIDDEN' });
      assert.deepEqual(restricted.binding.requiredOperations, [
        { resource: 'instance', operation: 'manage' },
      ]);
      assert.deepEqual(service.binding.policies.markings, []);
      assert.equal((await service.read(current, uid)).object.value, 'protected');
    });
    for (const [name, operation] of operations) {
      await t.test(name, async () => {
        const context = await session();
        const blocker = await pool.connect();
        let pending: Promise<{ error?: { code?: string }; value?: unknown }> | undefined;
        try {
          await blocker.query('BEGIN');
          await blocker.query('LOCK moria.staged_source_rows IN ACCESS EXCLUSIVE MODE');
          const pid = (await blocker.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
          await pool.query(
            "UPDATE moria.workload_sessions SET expires_at=clock_timestamp()+interval '5 seconds' WHERE token_hash=$1",
            [context.sessionHash],
          );
          pending = operation(context).then(
            (value) => ({ value }),
            (error) => ({ error }),
          );
          const deadline = Date.now() + 5000;
          for (;;) {
            const blocked = await pool.query(
              "SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(pid)) AND query LIKE '%staged_source_rows%'",
              [pid],
            );
            if (blocked.rowCount) break;
            assert.ok(Date.now() < deadline, 'operation must reach the database barrier');
            await delay(10);
          }
          // Expiry is pinned before the read snapshot; wait for that actual deadline.
          await pool.query(
            'SELECT pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM (expires_at-clock_timestamp())))+0.01) FROM moria.workload_sessions WHERE token_hash=$1',
            [context.sessionHash],
          );
          await blocker.query('COMMIT');
          const result = await pending;
          assert.equal(result.error?.code, 'UNAUTHENTICATED');
          assert.equal(result.value, undefined);
        } finally {
          await blocker.query('ROLLBACK');
          blocker.release();
          await pending;
        }
        const current = await session();
        assert.equal((await service.read(current, uid)).object.value, 'protected');
        await assert.rejects(service.read(current, randomUUID()), { code: 'NOT_FOUND' });
      });
    }
  },
);
