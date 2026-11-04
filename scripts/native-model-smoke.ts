import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import { promisify } from 'node:util';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { database } from '../tests/fixtures/catalog-storage/database.js';

const endpoint = process.env.MORIA_LOCAL_MODEL_ENDPOINT;
assert.ok(
  endpoint,
  'MORIA_LOCAL_MODEL_ENDPOINT must name the explicitly configured local Ollama /v1/ endpoint',
);
const url = new URL(endpoint);
assert.ok(
  ['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname),
  'The local inference smoke test requires loopback',
);
const tags = await (await fetch(new URL('/api/tags', url))).json();
const version = await (await fetch(new URL('/api/version', url))).json();
const chat = tags.models.find((m: any) => m.name === 'qwen3:4b'),
  embed = tags.models.find((m: any) => m.name === 'nomic-embed-text:latest');
assert.ok(
  chat?.digest && embed?.digest,
  'Preload the local Qwen3 and Nomic models before this test',
);
const db = await database();
try {
  const environment = randomUUID(),
    principal = 'local-model-smoke',
    application = 'local-model-smoke';
  await db.catalog.ensureEnvironment(environment);
  const authority = new Authority(db.catalog),
    sessions = new WorkloadSessions(authority),
    operations = ['chat:invoke', 'embedding:invoke'];
  await authority.syncPrincipal(environment, 'fixture', principal, {
    kind: 'workload',
    enabled: true,
    markings: ['synthetic'],
    attributes: {},
  });
  await authority.setApplication(environment, 'fixture', application, operations);
  await authority.setGrants(environment, 'fixture', principal, application, operations);
  const issued = await sessions.mint({ environment, principal, application }, 'fixture');
  for (const [resource, model, dimensions, capabilities] of [
    ['chat', chat, undefined, ['chat', 'tools', 'structured', 'stream']],
    ['embedding', embed, 768, ['embed']],
  ] as const) {
    await db.pool.query(
      'INSERT INTO moria.native_model_bindings(environment,resource,revision,definition) VALUES($1,$2,$3,$4)',
      [
        environment,
        resource,
        model.digest,
        {
          endpointRef: 'MORIA_LOCAL_MODEL_ENDPOINT',
          model: model.name,
          modelRevision: model.digest,
          resourceClass: 'models',
          destination: 'local-inference',
          markings: ['synthetic'],
          destinationPolicy: {
            allowedDestinations: ['local-inference'],
            allowedMarkings: ['synthetic'],
          },
          dimensions,
          capabilities,
          embeddingProtocol: resource === 'embedding' ? 'ollama-v1' : 'openai-compatible-v1',
        },
      ],
    );
  }
  const role = `modelsmoke_${randomUUID().replaceAll('-', '')}`,
    password = randomUUID();
  await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
  await db.pool.query(
    `GRANT USAGE ON SCHEMA moria TO ${role}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role}`,
  );
  const binary = process.env.MORIA_NATIVE_BINARY ?? '/usr/local/bin/moria-runtime';
  const run = async (command: string, resource: string, input: unknown = {}) => {
    const { spawn } = await import('node:child_process');
    const child = spawn(binary, ['models', command], {
      env: {
        PATH: '/usr/local/bin:/usr/bin:/bin',
        MORIA_INDEXING_DATABASE_SECRET_REF: 'MODEL_SMOKE_DATABASE',
        MODEL_SMOKE_DATABASE: `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`,
        MORIA_INDEXING_DATABASE_TLS: 'disable',
        MORIA_LOCAL_MODEL_ENDPOINT: endpoint,
      },
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    let output = '',
      errors = '';
    child.stdout.on('data', (v) => (output += v));
    child.stderr.on('data', (v) => (errors += v));
    child.stdin.end(JSON.stringify({ token: issued.token, resource, key: randomUUID(), input }));
    return await new Promise<any>((resolve, reject) => {
      child.once('error', reject);
      child.once('exit', (code) =>
        code === 0
          ? resolve(JSON.parse(output))
          : reject(new Error(`Native model ${command} failed: ${errors}`)),
      );
    });
  };
  const started = Date.now();
  console.log('Local inference capability probes started against a fresh synthetic database.');
  const chatProbe = await run('probe', 'chat');
  assert.deepEqual(chatProbe.capabilities, [
    'chat',
    'stream',
    'streamStructured',
    'streamTools',
    'structured',
    'tools',
  ]);
  assert.deepEqual(chatProbe.unsupported, {});
  console.log('Chat, stream, structured output and tool probes passed against local Qwen3.');
  const embeddingProbe = await run('probe', 'embedding');
  assert.deepEqual(embeddingProbe.capabilities, ['embed']);
  const vectors = await run('embed', 'embedding', {
    input: ['search_document: Moria stores protected objects.', 'search_query: protected objects'],
  });
  assert.equal(vectors.vectors.length, 2);
  assert.equal(vectors.vectors[0].length, 768);
  await assert.rejects(
    run('embed', 'embedding', { input: ['a '.repeat(8192)] }),
    /MODEL_CONTEXT_LIMIT/,
  );
  const afterTags = await (await fetch(new URL('/api/tags', url))).json();
  for (const model of [chat, embed])
    assert.equal(
      afterTags.models.find((m: any) => m.name === model.name)?.digest,
      model.digest,
      'provider model revisions must remain unchanged through the smoke',
    );
  const loadedModels = await (await fetch(new URL('/api/ps', url))).json();
  const native = JSON.parse(
    (await promisify(execFile)(binary, ['schema'], { encoding: 'utf8' })).stdout,
  );
  const calls = (
    await db.pool.query(
      "SELECT resource,state,error,result->'usage' AS usage,extract(epoch FROM (finished_at-created_at))*1000 AS duration_ms FROM moria.native_model_calls WHERE environment=$1 ORDER BY created_at",
      [environment],
    )
  ).rows;
  const evidence = {
    kind: 'local-functional-model-smoke',
    recordedAt: new Date().toISOString(),
    elapsedMs: Date.now() - started,
    native,
    engine: {
      implementation: 'Ollama',
      version: version.version,
      image:
        'ollama/ollama@sha256:b2f0122c476bc9fe7ce679d8bb4bd555351e25e2554eefb156f61cb0a89cf429',
      cloudDisabled: true,
      cpus: 4,
      memoryBytes: 6 * 1024 ** 3,
      loadedModels,
    },
    models: [
      { name: chat.name, digest: chat.digest },
      { name: embed.name, digest: embed.digest },
    ],
    chatProbe,
    embeddingProbe,
    calls,
    limitations: [
      'Functional capability proof only; no inference quality, latency SLO, fleet capacity or HA certification.',
    ],
  };
  const path = process.env.MORIA_MODEL_SMOKE_REPORT;
  assert.ok(path, 'MORIA_MODEL_SMOKE_REPORT must name an explicit evidence file');
  await writeFile(path, JSON.stringify(evidence, null, 2) + '\n', { flag: 'wx' });
  console.log(`Local model smoke passed; evidence written to ${path}`);
} finally {
  await db.close();
}
