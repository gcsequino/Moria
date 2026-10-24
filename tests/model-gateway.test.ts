import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { ModelGateway } from '../services/model-gateway/src/gateway.js';

test('gateway refuses forbidden egress before provider call and rechecks before output', async () => {
  let calls = 0,
    disabled = false;
  const server = createServer((req, res) => {
    calls++;
    req.resume();
    res.setHeader('content-type', 'application/json');
    res.end(
      JSON.stringify({ choices: [{ message: { content: 'fixture' }, finish_reason: 'stop' }] }),
    );
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = (server.address() as { port: number }).port;
  let allowed: string[] = [],
    checks = 0,
    revokeOnResponse = false;
  const gateway = new ModelGateway(
    {
      resource: 'model',
      model: 'fixture',
      endpoint: `http://127.0.0.1:${port}/v1`,
      destination: 'local',
    },
    async () => {
      checks++;
      if (revokeOnResponse && checks % 2 === 0) disabled = true;
      return {
        identity: {
          principal: 'user',
          application: 'app',
          environment: 'dev',
          disabled,
          grantedOperations: new Set(['model:invoke']),
          applicationScopes: new Set(['model:invoke']),
          markings: new Set(['secret']),
        },
        policy: { allowedDestinations: ['local'], allowedMarkings: allowed },
      };
    },
  );
  const request = { messages: [{ role: 'user' as const, content: 'test' }], maxTokens: 20 };
  try {
    await assert.rejects(gateway.chat(request, new Set(['secret'])), {
      code: 'DESTINATION_FORBIDDEN',
    });
    assert.equal(calls, 0);
    allowed = ['secret'];
    checks = 0;
    assert.equal((await gateway.chat(request, new Set(['secret']))).content, 'fixture');
    assert.equal(calls, 1);
    checks = 0;
    revokeOnResponse = true;
    await assert.rejects(gateway.chat(request, new Set(['secret'])), { code: 'FORBIDDEN' });
    assert.equal(calls, 2);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});
