import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { generate, writeSdk } from '../packages/osdk/src/generate.js';
import { managementContract, snowContract } from '../packages/osdk/src/bundled.js';
import { encodeValue, canonicalJson } from '../packages/contracts/values.js';
import { builtinObjectTypes } from '../packages/contracts/bundled-objects.js';
import { bindObject, typedRequest, actionClient } from '../packages/osdk/src/runtime.js';
import { createTransport } from '../packages/platform/src/transport.js';
import { createClient, ApiError } from '../apps/snotel/src/generated/index.js';
import {
  createClient as management,
  ApiError as ManagementApiError,
} from '../apps/management/src/generated/index.js';
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status });

test('native action contracts retain public names while routing permanent IDs and decoding creation results', async () => {
  const id = '00000000-0000-4000-8000-000000000088',
    uid = '00000000-0000-4000-8000-000000000089';
  const result = {
    revision: '7',
    created: [{ objectType: id, uid, version: '7' }],
    changed: [{ objectType: id, uid, version: '7', deleted: false }],
  };
  const invoke = actionClient<{ name: string }>(
    createTransport({
      fetch: async (url) => {
        assert.equal(
          new URL(String(url), 'http://localhost').pathname,
          `/api/v1/actions/${id}/invoke`,
        );
        return json(result);
      },
    }),
    id,
    { name: { type: 'string', nullable: false } },
  );
  assert.deepEqual(
    await invoke({ name: 'created' }, { idempotencyKey: 'stable-operation' }),
    result,
  );
  const contract = {
    version: 1 as const,
    app: 'native',
    types: {},
    objects: {},
    actions: { create: { id, inputs: { name: { type: 'string' as const, nullable: false } } } },
    endpoints: {},
  };
  const files = await generate(contract);
  assert(files['index.ts'].includes(id));
  await assert.rejects(
    generate({
      ...contract,
      actions: { create: { ...contract.actions.create, id: 'not-a-uuid' } },
    }),
    /action ID/,
  );
});

test('generated ontology names denote identity handles and serialize without properties or transport', async () => {
  const uid = '00000000-0000-4000-8000-000000000006';
  let calls = 0;
  const sdk = management({
    fetch: async (url) => {
      calls++;
      assert(String(url).includes(builtinObjectTypes.assets));
      return json({ revision: '1', object: { status: 'Current' } });
    },
  });
  const asset = sdk.objects.assets.object(uid);
  assert.equal(calls, 0);
  assert(Object.isFrozen(asset));
  const wire = { objectType: builtinObjectTypes.assets, uid };
  const schema = {
    type: 'ontologyObject' as const,
    objectType: builtinObjectTypes.assets,
    nullable: false,
  };
  assert.deepEqual(encodeValue(asset, schema), wire);
  assert.deepEqual(JSON.parse(canonicalJson({ asset })), { asset: wire });
  assert.deepEqual((await asset.get(['status'])).object, { status: 'Current' });
  assert.equal(calls, 1);
  assert.equal((bindObject(sdk, wire) as typeof asset).uid, uid);
  assert.throws(() => bindObject(sdk, { ...wire, objectType: builtinObjectTypes.stations }), {
    code: 'OBJECT_TYPE_UNBOUND',
  });
  assert.throws(() => encodeValue(Promise.resolve(asset), schema), { code: 'VALUE_INVALID' });
  const files = await generate(managementContract);
  assert.match(files['index.ts'], /export type Asset = OntologyObject</);
  assert(!files['index.ts'].includes('AssetRef'));
  const transport = createTransport({
    fetch: async (_url, init) => {
      assert.deepEqual(JSON.parse(String(init?.body)), wire);
      return json(wire);
    },
  });
  const returned = await typedRequest<typeof asset>(
    transport,
    '/api/echo',
    'POST',
    asset,
    schema,
    schema,
    {},
    undefined,
    (identity) => bindObject(sdk, identity),
  );
  assert.deepEqual((await returned.get(['status'])).object, { status: 'Current' });
  const custom = {
    ...managementContract,
    types: { Envelope: { type: 'object' as const, properties: { customer: schema } } },
    endpoints: {
      echo: {
        path: '/api/echo',
        method: 'POST' as const,
        input: { ref: 'Envelope' },
        output: { ref: 'Envelope' },
      },
    },
  };
  const generated = await generate(custom);
  assert.match(generated['index.ts'], /customer: Asset/);
});
test('SDK generation is deterministic, standalone and scoped; stale artifacts fail check', async () => {
  const c = JSON.parse(JSON.stringify(snowContract));
  const files = await generate(c);
  const reversed = Object.fromEntries(Object.entries(c).reverse());
  assert.deepEqual(await generate(reversed), files);
  assert.ok(!files['index.ts'].includes('edit-status'));
  assert.ok(!files['runtime.ts'].includes("from 'node:"));
  assert.ok(!files['index.ts'].includes('filesystem'));
  assert.ok(!Object.hasOwn(createClient(), 'filesystem'));
  assert.ok(!Object.hasOwn(files, 'filesystem.ts'));
  assert.ok(!Object.hasOwn(files, 'filesystem-contracts.ts'));
  assert.ok(!Object.hasOwn(files, 'storage.ts'));
  assert.ok(!files['index.ts'].includes('functions'));
  assert.ok(!Object.hasOwn(createClient(), 'functions'));
  assert.ok(!Object.hasOwn(files, 'functions.ts'));
  assert.ok(!Object.hasOwn(files, 'functions-contracts.ts'));
  assert.ok(!Object.hasOwn(files, 'function-values.ts'));
  await assert.rejects(
    generate({ ...c, objects: { bad: { type: 'Bad;export', properties: {} } } }),
  );
  await assert.rejects(
    generate({
      ...c,
      endpoints: { bad: { method: 'GET', path: 'https://evil.test', output: { type: 'string' } } },
    }),
  );
  await assert.rejects(generate({ ...c, types: { Loop: { ref: 'Loop' } } }));
  const dir = await mkdtemp('.local/osdk-');
  await writeSdk(c, dir);
  await writeSdk(c, dir, true);
  for (const name of [
    'filesystem.ts',
    'functions.ts',
    'functions-contracts.ts',
    'function-values.ts',
  ]) {
    await writeFile(dir + '/' + name, '// obsolete generated client');
    await assert.rejects(writeSdk(c, dir, true), /Obsolete generated SDK file/);
    await writeSdk(c, dir);
    await assert.rejects(readFile(dir + '/' + name), { code: 'ENOENT' });
  }
  await writeSdk(c, dir, true);
  await writeFile(dir + '/index.ts', (await readFile(dir + '/index.ts', 'utf8')) + '// stale');
  await assert.rejects(writeSdk(c, dir, true), /stale/);
  const numeric = await generate({
    ...c,
    types: {
      ...c.types,
      Precise: {
        type: 'object',
        properties: {
          count: { type: 'int64', nullable: false },
          amount: { type: 'decimal', nullable: true, precision: 20, scale: 2 },
        },
      },
    },
  });
  assert.match(numeric['index.ts'], /count: string/);
  assert.match(numeric['index.ts'], /amount: string \| null/);
});
test('SDK pagination retains typed filter/projection and abort signal without switching identity', async () => {
  const calls: { url: string; init: RequestInit }[] = [];
  const controller = new AbortController();
  const sdk = createClient({
    fetch: async (url, init) => {
      calls.push({ url: String(url), init: init! });
      return json({
        revision: '4',
        objects: [{ triplet: calls.length === 1 ? 'A' : 'B' }],
        ...(calls.length === 1 ? { nextCursor: 'next' } : {}),
      });
    },
  });
  const rows = await sdk.objects['snow-stations'].all(
    {
      projection: ['triplet'],
      filter: { op: 'eq', args: [{ field: 'state' }, { literal: 'CO' }] },
      pageSize: 1,
    },
    { signal: controller.signal },
  );
  assert.deepEqual(rows, [{ triplet: 'A' }, { triplet: 'B' }]);
  assert.equal(calls[0].init.credentials, 'same-origin');
  assert.equal(calls[0].init.redirect, 'error');
  assert.equal(calls[1].init.signal, controller.signal);
  const first = JSON.parse(calls[0].init.body as string),
    second = JSON.parse(calls[1].init.body as string);
  assert.deepEqual(second, { ...first, cursor: 'next' });
  assert.equal(Object.hasOwn(calls[0].init.headers!, 'authorization'), false);
  const bad = createClient({
    fetch: async () => json({ revision: '1', objects: [{}], nextCursor: 'same' }),
  });
  await assert.rejects(
    bad.objects['snow-stations'].all({ projection: [] }, { maxRows: 1 }),
    (e) => e instanceof ApiError && e.code === 'CLIENT_ROW_LIMIT',
  );
  await assert.rejects(
    bad.objects['snow-stations'].all({ projection: [] }),
    (e) => e instanceof ApiError && e.code === 'CURSOR_LOOP',
  );
});
test('SDK errors retain server diagnostics and actions never retry or invent idempotency keys', async () => {
  let calls = 0,
    expired = 0;
  const sdk = management({
    onUnauthenticated: () => expired++,
    fetch: async (_url, init) => {
      calls++;
      assert.equal((init?.headers as Record<string, string>)['idempotency-key'], 'same-key');
      return json(
        {
          error: {
            code: 'UNAUTHENTICATED',
            message: 'expired',
            correlationId: 'trace',
            retryable: false,
            details: {},
          },
        },
        401,
      );
    },
  });
  await assert.rejects(
    sdk.actions['edit-status'](
      { uid: '00000000-0000-0000-0000-000000000000', status: 'x' },
      { idempotencyKey: 'same-key' },
    ),
    (e) => e instanceof ManagementApiError && e.status === 401 && e.correlationId === 'trace',
  );
  assert.equal(calls, 1);
  assert.equal(expired, 1);
  assert.throws(() =>
    sdk.actions['edit-status']({ uid: 'a', status: 'x' }, { idempotencyKey: '' }),
  );
  const malformed = createClient({
    fetch: async () => new Response('<html>gateway failure</html>', { status: 502 }),
  });
  await assert.rejects(
    malformed.endpoints.status(),
    (e) => e instanceof ApiError && e.code === 'INVALID_RESPONSE',
  );
  assert.throws(() => createClient({ baseUrl: 'https://example.test/path' }));
});

test('generated clients reject invalid typed inputs and malformed typed responses', async () => {
  let calls = 0;
  const sdk = management({
    fetch: async () => {
      calls++;
      return json({ revision: '1' });
    },
  });
  assert.throws(
    () =>
      sdk.actions['edit-status']({ uid: 'invalid', status: 'x' }, { idempotencyKey: 'invalid' }),
    { code: 'VALUE_INVALID' },
  );
  assert.equal(calls, 0);
  const malformed = createClient({
    fetch: async () => json({ revision: '1', objects: [{ latitude: '1.2' }] }),
  });
  await assert.rejects(malformed.objects['snow-stations'].query({ projection: ['latitude'] }), {
    code: 'VALUE_INVALID',
  });
});
