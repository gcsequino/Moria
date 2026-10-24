import { readFile } from 'node:fs/promises';
import { ModelGateway } from '../services/model-gateway/src/gateway.js';

if (!process.env.MORIA_LOCAL_KEY_FILE)
  throw new Error('Set MORIA_LOCAL_KEY_FILE to the existing external key file');
const secret = (await readFile(process.env.MORIA_LOCAL_KEY_FILE, 'utf8')).trim();
const gateway = new ModelGateway(
  {
    resource: 'local-model',
    model: 'local-chat',
    endpoint: 'http://127.0.0.1:8080/v1',
    destination: 'local',
    secret,
  },
  async () => ({
    identity: {
      principal: 'smoke',
      application: 'smoke',
      environment: 'test',
      grantedOperations: new Set(['local-model:invoke']),
      applicationScopes: new Set(['local-model:invoke']),
      markings: new Set(),
    },
    policy: { allowedDestinations: ['local'], allowedMarkings: [] },
  }),
);
const start = Date.now();
const result = await gateway.chat(
  { messages: [{ role: 'user', content: 'Reply with exactly the word Moria.' }], maxTokens: 2048 },
  new Set(),
);
console.log(
  JSON.stringify({
    model: 'local-chat',
    elapsedMs: Date.now() - start,
    finishReason: result.finishReason,
    content: result.content,
  }),
);
