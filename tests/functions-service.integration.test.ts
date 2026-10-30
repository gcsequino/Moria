import assert from 'node:assert/strict';
import test from 'node:test';
import { randomUUID } from 'node:crypto';
import { functionPlatform } from './fixtures/functions/platform.js';
import type { FunctionWorker } from '../services/functions/src/worker.js';
import { createApi } from '../apps/api/src/server.js';
import { createTransport, ApiError } from '../packages/platform/src/transport.js';
import { functionsClient } from '../packages/platform/src/functions.js';
import { functionsCommand } from '../apps/cli/src/functions.js';
import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { Functions } from '../services/functions/src/service.js';
import { MoriaError } from '../packages/contracts/types.js';
import { ObjectService } from '../services/ontology/src/service.js';
import { createScopedAppServer } from '../services/app-host/src/scoped.js';
import { localAppHost } from '../services/app-host/src/bootstrap.js';

test(
  'function acceptance, HTTP clients and publication use real transactional authority',
  { timeout: 60000 },
  async (t) => {
    let executions = 0;
    const worker: FunctionWorker = {
      remove: async () => {},
      execute: async (_id, _artifact, _limits, start: any, _rpc, log) => {
        executions++;
        await log('test computation');
        return { value: start.input.value + 1 };
      },
    };
    const p = await functionPlatform(
      { uri: 'oci://fixture/functions', digest: `sha256:${'a'.repeat(64)}` },
      worker,
    );
    t.after(p.close);
    const context = p.owner.context;
    const server = createApi(p.sessions, {
      objects: new Map(),
      actions: new Map(),
      functions: p.functions,
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    t.after(() => new Promise<void>((resolve) => server.close(() => resolve())));
    const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
    const client = functionsClient(
      createTransport({
        baseUrl: origin,
        fetch: (url, init) =>
          fetch(url, {
            ...init,
            headers: { ...init?.headers, authorization: `Bearer ${p.owner.token}` },
          }),
      }),
    );
    await t.test(
      'default synchronous invocation and explicit asynchronous inspection preserve one logical call',
      async () => {
        const originalGet = p.functions.get.bind(p.functions);
        let contentions = 1;
        p.functions.get = async (...args: Parameters<typeof originalGet>) => {
          if (contentions-- > 0)
            throw new MoriaError('CONCURRENT_MODIFICATION', 'Transient inspection contention');
          return originalGet(...args);
        };
        t.after(() => {
          p.functions.get = originalGet;
        });
        const key = randomUUID(),
          result = await client.invoke(
            p.resourceId,
            { mode: 'normal', value: 1 },
            { idempotencyKey: key },
          );
        assert.equal(result.status, 'succeeded');
        assert.deepEqual(result.result, { value: 2 });
        assert.equal(
          (await client.invoke(p.resourceId, { mode: 'normal', value: 1 }, { idempotencyKey: key }))
            .id,
          result.id,
        );
        assert.equal(executions, 1);
        const events = await client.events(result.id);
        assert.equal(events.events.at(-1)?.message, 'succeeded');
        assert.deepEqual(
          (await client.events(result.id, events.events.at(-1)!.sequence)).events,
          [],
        );
        await assert.rejects(
          client.invoke(p.resourceId, { mode: 'normal', value: 2 }, { idempotencyKey: key }),
          (error) => error instanceof ApiError && error.status === 409,
        );
        const payload = join(p.directory, 'input.json');
        await writeFile(payload, JSON.stringify({ mode: 'normal', value: 4 }));
        process.env.MORIA_API_TOKEN = p.owner.token;
        try {
          const invocation: any = await functionsCommand(['invoke', p.resourceId], {
            url: origin,
            config: payload,
            key: randomUUID(),
          });
          assert.deepEqual(invocation.result, { value: 5 });
        } finally {
          delete process.env.MORIA_API_TOKEN;
        }
      },
    );
    await t.test(
      'strict transport boundaries and verified sessions reject malformed or unauthorized requests',
      async () => {
        const route = `${origin}/api/v1/functions/${p.resourceId}/invoke`;
        assert.equal(
          (
            await fetch(route, {
              method: 'POST',
              headers: { 'content-type': 'application/json' },
              body: '{}',
            })
          ).status,
          401,
        );
        for (const value of [{ input: {}, principal: 'other' }, { input: {}, wait: 'yes' }, {}]) {
          const response = await fetch(route, {
            method: 'POST',
            headers: {
              authorization: `Bearer ${p.owner.token}`,
              'content-type': 'application/json',
              'idempotency-key': randomUUID(),
            },
            body: JSON.stringify(value),
          });
          assert.equal(response.status, 400);
        }
        const other = functionsClient(
          createTransport({
            baseUrl: origin,
            fetch: (url, init) =>
              fetch(url, {
                ...init,
                headers: { ...init?.headers, authorization: `Bearer ${p.other.token}` },
              }),
          }),
        );
        const result = await client.invoke(
          p.resourceId,
          { mode: 'normal', value: 1 },
          { idempotencyKey: randomUUID() },
        );
        await assert.rejects(
          other.get(result.id),
          (error) => error instanceof ApiError && error.status === 404,
        );
        await p.authority.setGrants(
          p.environment,
          'operator',
          'author',
          p.application,
          p.operations.filter((operation) => operation !== 'functions:invoke'),
        );
        await assert.rejects(
          client.events(result.id),
          (error) => error instanceof ApiError && error.status === 403,
        );
        await p.authority.setGrants(
          p.environment,
          'operator',
          'author',
          p.application,
          p.operations,
        );
      },
    );
    await t.test(
      'hosted apps expose functions only through an explicit binding and verified app identity',
      async () => {
        for (const enabled of [false, true]) {
          const app = createScopedAppServer({
            app: 'function-fixture',
            host: localAppHost(p.directory),
            key: 'synthetic-app-key',
            sessions: p.sessions,
            token: async () => p.owner.token,
            objects: new Map(),
            status: async () => ({}),
            ...(enabled ? { functions: p.functions } : {}),
          });
          await new Promise<void>((resolve) => app.listen(0, '127.0.0.1', resolve));
          try {
            const url = `http://127.0.0.1:${(app.address() as { port: number }).port}`;
            const response = await fetch(`${url}/api/v1/functions/${p.resourceId}/invoke`, {
              method: 'POST',
              headers: {
                'x-moria-app-key': 'synthetic-app-key',
                authorization: `Bearer ${p.other.token}`,
                'x-moria-profile': 'other',
                'content-type': 'application/json',
                'idempotency-key': randomUUID(),
              },
              body: JSON.stringify({ input: { mode: 'normal', value: 41 } }),
            });
            assert.equal(response.status, enabled ? 200 : 404);
            if (enabled) {
              const result = (await response.json()) as any;
              assert.deepEqual(result.result, { value: 42 });
              assert.equal((await p.functions.get(context, result.id)).id, result.id);
            }
          } finally {
            await new Promise<void>((resolve) => app.close(() => resolve()));
          }
        }
      },
    );
    await t.test(
      'replayed acceptance stays pinned when a new release changes input types',
      async () => {
        const key = randomUUID(),
          result = await client.invoke(
            p.resourceId,
            { mode: 'normal', value: 3 },
            { idempotencyKey: key },
          );
        await p.publish({
          ...p.declaration,
          spec: {
            ...p.declaration.spec,
            inputs: { ...p.declaration.spec.inputs, value: { type: 'string', nullable: false } },
          },
        });
        const replay = await client.invoke(
          p.resourceId,
          { mode: 'normal', value: 3 },
          { idempotencyKey: key },
        );
        assert.equal(replay.id, result.id);
        assert.equal(replay.release, result.release);
        await p.publish();
      },
    );
  },
);

test(
  'function publication fences cancellation, worker loss and changing OSDK read evidence',
  { timeout: 60000 },
  async (t) => {
    let ready: () => void = () => {},
      release: () => void = () => {};
    let reading = new Promise<void>((resolve) => {
        ready = resolve;
      }),
      held = new Promise<void>((resolve) => {
        release = resolve;
      });
    const worker: FunctionWorker = {
      remove: async () => {},
      execute: async (_id, _artifact, _limits, start: any, rpc, _log, signal) => {
        if (start.input.mode === 'forbidden')
          return rpc({ path: '/api/v1/actions/edit/invoke', method: 'POST', input: {} });
        let result = { value: start.input.value };
        if (start.input.mode.startsWith('read')) {
          const response: any = await rpc({
            path: `/api/v1/objects/records/${start.input.uid}?properties=uid,value`,
            method: 'GET',
            input: null,
          });
          result = { value: response.object.value };
        }
        if (start.input.mode.endsWith('wait')) {
          ready();
          await Promise.race([
            held,
            new Promise((_, reject) => {
              if (signal.aborted) reject(signal.reason);
              else signal.addEventListener('abort', () => reject(signal.reason), { once: true });
            }),
          ]);
        }
        return result;
      },
    };
    const p = await functionPlatform(
      { uri: 'oci://fixture/functions', digest: `sha256:${'b'.repeat(64)}` },
      worker,
    );
    t.after(p.close);
    const context = p.owner.context;
    async function data(value: number) {
      const state = await p.authority.current(p.environment, 'author', p.application),
        revision = (
          await p.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [p.environment])
        ).rows[0].revision;
      const uid = await p.catalog.resolveIdentity('function-records', 'record');
      const stage = await p.objectStore.stage(
        p.environment,
        'records',
        p.records.binding.schema,
        [{ uid, value }],
        state.epoch,
        revision,
      );
      await p.objectStore.publishStage(stage);
      return String((await p.records.query(context.session, {})).objects[0].uid);
    }
    let uid = await data(42);
    await t.test('brokered reads label results and deny internal or mutation routes', async () => {
      const accepted = await p.functions.invoke(context, p.resourceId, randomUUID(), {
          mode: 'read',
          uid,
          value: 0,
        }),
        result = await p.functions.wait(context, accepted.id);
      assert.equal(result.status, 'succeeded');
      assert.deepEqual(result.result, { value: 42 });
      const blocked = await p.functions.invoke(context, p.resourceId, randomUUID(), {
        mode: 'forbidden',
        value: 0,
      });
      assert.equal((await p.functions.wait(context, blocked.id)).status, 'failed');
      await p.authority.setGrants(
        p.environment,
        'operator',
        'author',
        p.application,
        p.operations.filter((operation) => operation !== 'records:read'),
      );
      await assert.rejects(p.functions.get(context, result.id), { code: 'FORBIDDEN' });
      await assert.rejects(p.functions.events(context, result.id), { code: 'FORBIDDEN' });
      await p.authority.setGrants(p.environment, 'operator', 'author', p.application, p.operations);
    });
    await t.test(
      'new input classifications accompany retained results after trusted binding reload',
      async () => {
        const accepted = await p.functions.invoke(context, p.resourceId, randomUUID(), {
          mode: 'read',
          uid,
          value: 0,
        });
        const result = await p.functions.wait(context, accepted.id);
        assert.deepEqual(result.markings, []);
        const restricted = new ObjectService(p.objectStore, p.authority, {
          ...p.records.binding,
          policies: { ...p.records.binding.policies, markings: ['protected'] },
        });
        const reader = new Functions(p.runtime, new Map([['records', restricted]]));
        try {
          assert.ok(
            (await reader.get(context, result.id)).markings.some(
              (label) => label.marking === 'protected',
            ),
          );
        } finally {
          await reader.close();
        }
        await p.runtime.filesystem.security.setPolicy('operator', p.root.id, '0', {
          markings: [{ environment: p.environment, marking: 'protected' }],
          grants: [],
        });
        assert.ok(
          (await p.functions.get(context, result.id)).markings.some(
            (label) => label.marking === 'protected',
          ),
        );
      },
    );
    await t.test(
      'progress polling checks authority without repeating unchanged object reads',
      async () => {
        const original = p.records.read;
        let count = 0;
        p.records.read = async (...args) => {
          count++;
          return original.apply(p.records, args);
        };
        const accepted = await p.functions.invoke(context, p.resourceId, randomUUID(), {
          mode: 'read-wait',
          uid,
          value: 0,
        });
        try {
          await reading;
          assert.equal(count, 1);
          for (let i = 0; i < 3; i++) {
            assert.equal((await p.functions.get(context, accepted.id)).status, 'running');
            await p.functions.events(context, accepted.id);
          }
          assert.equal(count, 1);
        } finally {
          p.records.read = original;
          release();
          await p.functions.wait(context, accepted.id);
          reading = new Promise((resolve) => {
            ready = resolve;
          });
          held = new Promise((resolve) => {
            release = resolve;
          });
        }
      },
    );
    await t.test('changed data cannot publish a stale computation or expose its logs', async () => {
      const accepted = await p.functions.invoke(context, p.resourceId, randomUUID(), {
        mode: 'read-wait',
        uid,
        value: 0,
      });
      await reading;
      uid = await data(43);
      release();
      for (let i = 0; i < 100; i++) {
        const row = (
          await p.pool.query('SELECT status,result FROM moria.function_invocations WHERE id=$1', [
            accepted.id,
          ])
        ).rows[0];
        if (row.status === 'failed') {
          assert.equal(row.result, null);
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
      assert.equal(
        (
          await p.pool.query('SELECT status FROM moria.function_invocations WHERE id=$1', [
            accepted.id,
          ])
        ).rows[0].status,
        'failed',
      );
      await assert.rejects(p.functions.get(context, accepted.id), {
        code: 'FUNCTION_INPUT_CHANGED',
      });
      await assert.rejects(p.functions.events(context, accepted.id), {
        code: 'FUNCTION_INPUT_CHANGED',
      });
    });
    await t.test('a cancellation from another supervisor prevents late success', async () => {
      reading = new Promise((resolve) => {
        ready = resolve;
      });
      held = new Promise((resolve) => {
        release = resolve;
      });
      const accepted = await p.functions.invoke(context, p.resourceId, randomUUID(), {
        mode: 'normal-wait',
        value: 9,
      });
      await reading;
      const second = new Functions(p.runtime, new Map(), worker);
      assert.equal((await second.cancel(context, accepted.id)).status, 'cancelled');
      release();
      assert.equal((await p.functions.wait(context, accepted.id)).status, 'cancelled');
      assert.equal((await p.functions.get(context, accepted.id)).result, undefined);
      await second.close();
    });
    await t.test(
      'lost leases never publish, and request keys cannot be reclaimed by retry',
      async () => {
        reading = new Promise((resolve) => {
          ready = resolve;
        });
        held = new Promise((resolve) => {
          release = resolve;
        });
        const key = randomUUID(),
          accepted = await p.functions.invoke(context, p.resourceId, key, {
            mode: 'normal-wait',
            value: 0,
          });
        await reading;
        await p.pool.query(
          "UPDATE moria.function_invocations SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
          [accepted.id],
        );
        assert.equal(
          (await p.functions.get(context, accepted.id)).error?.code,
          'FUNCTION_WORKER_LOST',
        );
        release();
        const replay = await p.functions.invoke(context, p.resourceId, key, {
          mode: 'normal-wait',
          value: 0,
        });
        assert.equal(replay.id, accepted.id);
        assert.equal(replay.status, 'failed');
      },
    );
    await t.test(
      'API-only acceptance, cancellation and expired-lease inspection never call the container backend',
      async () => {
        await p.functions.close();
        const unconfigured = new Functions(p.runtime);
        assert.throws(() => unconfigured.startWorker(), { code: 'FUNCTION_WORKER_UNAVAILABLE' });
        await unconfigured.close();
        const api = new Functions(p.runtime, new Map(), {
          execute: async () => {
            throw new Error('API launched code');
          },
          remove: async () => {
            throw new Error('API touched Docker');
          },
        });
        const accepted = await api.invoke(context, p.resourceId, randomUUID(), {
          mode: 'normal',
          value: 1,
        });
        assert.equal(accepted.status, 'accepted');
        assert.equal((await api.cancel(context, accepted.id)).status, 'cancelled');
        const abandoned = await api.invoke(context, p.resourceId, randomUUID(), {
          mode: 'normal',
          value: 2,
        });
        await p.pool.query(
          "UPDATE moria.function_invocations SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
          [abandoned.id],
        );
        assert.equal((await api.get(context, abandoned.id)).error?.code, 'FUNCTION_WORKER_LOST');
        const expired = await api.invoke(context, p.resourceId, randomUUID(), {
          mode: 'normal',
          value: 3,
        });
        await p.pool.query(
          "UPDATE moria.function_invocations SET deadline=clock_timestamp()-interval '1 second' WHERE id=$1",
          [expired.id],
        );
        assert.equal((await api.get(context, expired.id)).error?.code, 'FUNCTION_TIMEOUT');
        let executions = 0;
        const competing: FunctionWorker = {
          remove: async () => {},
          execute: async () => {
            executions++;
            await new Promise((resolve) => setTimeout(resolve, 100));
            return { value: 8 };
          },
        };
        const first = new Functions(p.runtime, new Map(), competing),
          second = new Functions(p.runtime, new Map(), competing);
        const queued = await api.invoke(context, p.resourceId, randomUUID(), {
          mode: 'normal',
          value: 8,
        });
        first.startWorker();
        second.startWorker();
        try {
          assert.deepEqual((await api.wait(context, queued.id)).result, { value: 8 });
          assert.equal(executions, 1);
        } finally {
          await first.close();
          await second.close();
          await api.close();
        }
      },
    );
  },
);
