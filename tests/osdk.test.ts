import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { generate, writeSdk } from '../packages/osdk/src/generate.js';
import { managementContract, snowContract } from '../packages/osdk/src/bundled.js';
import { createClient, ApiError } from '../apps/snotel/src/generated/index.js';
import {
  createClient as management,
  ApiError as ManagementApiError,
} from '../apps/management/src/generated/index.js';
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status });
test('SDK generation is deterministic, standalone and scoped; stale artifacts fail check', async () => {
  const c = JSON.parse(JSON.stringify(snowContract));
  const files = await generate(c);
  const reversed = Object.fromEntries(Object.entries(c).reverse());
  assert.deepEqual(await generate(reversed), files);
  assert.ok(!files['index.ts'].includes('edit-status'));
  assert.ok(!files['runtime.ts'].includes("from 'node:"));
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
    bad.objects['snow-stations'].all({}, { maxRows: 1 }),
    (e) => e instanceof ApiError && e.code === 'CLIENT_ROW_LIMIT',
  );
  await assert.rejects(
    bad.objects['snow-stations'].all(),
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
    sdk.actions['edit-status']({ uid: 'a', status: 'x' }, { idempotencyKey: 'same-key' }),
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
