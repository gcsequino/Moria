import assert from 'node:assert/strict';
import test from 'node:test';
import { createTransport } from '../packages/platform/src/transport.js';
import { functionsClient } from '../packages/platform/src/functions.js';
import { agentsClient } from '../packages/platform/src/agents.js';
import { evaluationsClient } from '../packages/platform/src/evaluations.js';
import { pipelinesClient } from '../packages/platform/src/pipelines.js';

test('domain cancellation clients accept only the uniform control receipt', async () => {
  const id = '781d11a2-6a51-4647-942a-8e16944bd5f0';
  for (const factory of [functionsClient, agentsClient, evaluationsClient, pipelinesClient]) {
    const client = (body: unknown) =>
      factory(
        createTransport({
          baseUrl: 'http://control.test',
          fetch: async (_url, options) => {
            assert.equal(options?.method, 'POST');
            assert.equal(options?.body, '{}');
            return new Response(JSON.stringify(body), {
              headers: { 'content-type': 'application/json' },
            });
          },
        }),
      );
    assert.deepEqual(await client({ id, cancelRequested: true }).cancel(id), {
      id,
      cancelRequested: true,
    });
    for (const body of [
      null,
      { id, cancelRequested: false },
      { id, cancelRequested: true, result: { value: 'private' } },
      { id, cancelRequested: true, status: 'succeeded' },
      { id: 'other', cancelRequested: true },
    ])
      await assert.rejects(client(body).cancel(id), { code: 'CANCELLATION_PROTOCOL' });
  }
});
