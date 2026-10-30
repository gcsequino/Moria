import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { functionPlatform } from './fixtures/functions/platform.js';
import type { FunctionWorker } from '../services/functions/src/worker.js';
import { objectClient } from '../packages/osdk/src/runtime.js';
import { createTransport } from '../packages/platform/src/transport.js';
import { createApi } from '../apps/api/src/server.js';

const objectType = '00000000-0000-4000-8000-000000000004';
const customerType = { type: 'ontologyObject' as const, objectType, nullable: false };
test(
  'ontology function values retain identity, current authority and result evidence',
  { timeout: 60000 },
  async (t) => {
    let executions = 0,
      output: any;
    const worker: FunctionWorker = {
      remove: async () => {},
      execute: async (_id, _artifact, _limits, start: any) => {
        executions++;
        return output ?? { customer: start.input.customer, customers: start.input.customers };
      },
    };
    const p = await functionPlatform(
      { uri: 'oci://fixture/objects', digest: 'sha256:' + 'a'.repeat(64) },
      worker,
    );
    t.after(p.close);
    const context = p.owner.context;
    const uid = await p.catalog.resolveIdentity('object-reference-tests', 'customer');
    const state = await p.authority.current(
      p.environment,
      context.session.principal,
      p.application,
    );
    await p.objectStore.activate(
      p.environment,
      'records',
      p.records.binding.schema,
      [{ uid, value: 41 }],
      state.epoch,
      (await p.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [p.environment]))
        .rows[0].revision,
    );
    p.configureRecords((binding) => {
      binding.policies.markings = ['protected'];
    });
    const declaration = {
      ...p.declaration,
      spec: {
        ...p.declaration.spec,
        inputs: { customer: customerType, customers: { type: 'list', items: customerType } },
        outputs: { customer: customerType, customers: { type: 'list', items: customerType } },
      },
    };
    await p.publish(declaration);
    const identity = { objectType, uid };
    const input = { customer: identity, customers: [identity, identity] };
    const invoke = async (value: unknown = input) =>
      p.functions.wait(
        context,
        (await p.functions.invoke(context, p.resourceId, randomUUID(), value)).id,
      );
    let retained: string;
    await t.test(
      'reference parameters do not request or snapshot protected properties',
      async () => {
        p.configureRecords((binding) => {
          binding.policies.propertyRead = { value: { literal: false } };
        });
        const key = randomUUID();
        const accepted = await p.functions.invoke(context, p.resourceId, key, input);
        const result = await p.functions.wait(context, accepted.id);
        retained = result.id;
        assert.equal(result.status, 'succeeded', JSON.stringify(result));
        assert.deepEqual(result.result, input);
        assert(result.markings.some((label) => label.marking === 'protected'));
        assert.equal((await p.functions.invoke(context, p.resourceId, key, input)).id, result.id);
        assert.equal(executions, 1);
        const stored = (
          await p.pool.query(
            'SELECT input,reads,read_grants FROM moria.function_invocations WHERE id=$1',
            [result.id],
          )
        ).rows[0];
        assert.deepEqual(stored.input, input);
        assert(stored.reads.every((read: any) => read.objectType === objectType));
        assert(
          stored.read_grants.some(
            (read: any) => read.resource === 'records' && read.operation === 'read',
          ),
        );
      },
    );
    await t.test(
      'wrong types, forged snapshots, foreign environments and absent objects never execute',
      async () => {
        const before = executions;
        for (const customer of [
          { ...identity, objectType: randomUUID() },
          { ...identity, value: 99 },
          { ...identity, environment: 'foreign' },
        ])
          await assert.rejects(
            p.functions.invoke(context, p.resourceId, randomUUID(), { ...input, customer }),
            { code: 'VALUE_INVALID' },
          );
        await assert.rejects(
          p.functions.invoke(context, p.resourceId, randomUUID(), {
            ...input,
            customer: { ...identity, uid: randomUUID() },
          }),
          { code: 'NOT_FOUND' },
        );
        p.configureRecords((binding) => {
          binding.policies.objectRead = { literal: false };
        });
        await assert.rejects(p.functions.invoke(context, p.resourceId, randomUUID(), input), {
          code: 'NOT_FOUND',
        });
        p.configureRecords((binding) => {
          delete binding.policies.objectRead;
        });
        assert.equal(executions, before);
      },
    );
    await t.test(
      'current grant revocation blocks input acceptance and retained reference results',
      async () => {
        await p.authority.setGrants(
          p.environment,
          'operator',
          context.session.principal,
          p.application,
          p.operations.filter((value) => value !== 'records:read'),
        );
        await assert.rejects(p.functions.invoke(context, p.resourceId, randomUUID(), input), {
          code: 'FORBIDDEN',
        });
        await assert.rejects(p.functions.get(context, retained), { code: 'FORBIDDEN' });
        await p.authority.setGrants(
          p.environment,
          'operator',
          context.session.principal,
          p.application,
          p.operations,
        );
      },
    );
    await t.test('declared types must be bound even when the value would be null', async () => {
      await p.publish({
        ...declaration,
        spec: {
          ...declaration.spec,
          inputs: { customer: { ...customerType, objectType: randomUUID(), nullable: true } },
        },
      });
      await assert.rejects(
        p.functions.invoke(context, p.resourceId, randomUUID(), { customer: null }),
        { code: 'OBJECT_TYPE_UNBOUND' },
      );
      await p.publish(declaration);
    });
    await t.test(
      'returning a reference validates type, existence and row policy before publication',
      async () => {
        output = { ...input, customer: { ...identity, objectType: randomUUID() } };
        const wrong = await invoke();
        assert.equal(wrong.status, 'failed');
        assert.equal(wrong.error?.code, 'VALUE_INVALID');
        assert.equal(wrong.result, undefined);
        output = { ...input, customer: { ...identity, uid: randomUUID() } };
        const missing = await invoke();
        assert.equal(missing.status, 'failed');
        assert.equal(missing.error?.code, 'NOT_FOUND');
        assert.equal(missing.result, undefined);
        await p.publish({ ...declaration, spec: { ...declaration.spec, inputs: {} } });
        output = input;
        p.configureRecords((binding) => {
          binding.policies.objectRead = { literal: false };
        });
        const hidden = await invoke({});
        assert.equal(hidden.status, 'failed');
        assert.equal(hidden.error?.code, 'NOT_FOUND');
        assert.equal(hidden.result, undefined);
        p.configureRecords((binding) => {
          delete binding.policies.objectRead;
        });
        await p.publish(declaration);
        output = undefined;
      },
    );
    await t.test(
      'UUID OSDK routes preserve projection policy and never fall back to a name binding',
      async () => {
        const server = createApi(p.sessions, {
          objects: p.objects,
          actions: new Map(),
        });
        await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
        t.after(() => new Promise<void>((resolve) => server.close(() => resolve())));
        const transport = createTransport({
          baseUrl: `http://127.0.0.1:${(server.address() as any).port}`,
          fetch: (url, init) =>
            fetch(url, {
              ...init,
              headers: { ...init?.headers, authorization: `Bearer ${p.owner.token}` },
            }),
        });
        const records = objectClient<{ uid: string; value: number }>(
          transport,
          'records',
          p.records.binding.schema.properties,
          objectType,
        );
        const customer = records.object(uid);
        assert.deepEqual((await customer.get([])).object, {});
        await assert.rejects(customer.get(['value']), { code: 'PROPERTY_FORBIDDEN' });
        p.configureRecords((binding) => {
          binding.objectTypeId = randomUUID();
        });
        await assert.rejects(customer.get([]), { code: 'NOT_FOUND' });
        await assert.rejects(p.functions.get(context, retained));
        p.configureRecords((binding) => {
          binding.objectTypeId = objectType;
        });
      },
    );
    await t.test(
      'deletion withholds a previously returned identity even without property reads',
      async () => {
        const current = await p.authority.current(
          p.environment,
          context.session.principal,
          p.application,
        );
        await p.objectStore.activate(
          p.environment,
          'records',
          p.records.binding.schema,
          [],
          current.epoch,
          (
            await p.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [
              p.environment,
            ])
          ).rows[0].revision,
        );
        await assert.rejects(p.functions.get(context, retained), {
          code: 'FUNCTION_INPUT_CHANGED',
        });
      },
    );
  },
);
