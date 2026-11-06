import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { createServer, request as httpRequest } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { database } from './fixtures/catalog-storage/database.js';
import { transactionCommitProxy } from './fixtures/runtime/postgres-proxy.js';

test(
  'native model gateway protects real HTTP chat, embeddings, schemas and streams',
  { timeout: 180000 },
  async (t) => {
    const db = await database();
    const processes: ReturnType<typeof spawn>[] = [];
    const exits: Promise<unknown>[] = [];
    const environment = randomUUID(),
      principal = 'model-user',
      application = 'model-app';
    await db.catalog.ensureEnvironment(environment);
    const authority = new Authority(db.catalog),
      sessions = new WorkloadSessions(authority),
      operations = ['model:invoke', 'work:execute'];
    await authority.syncPrincipal(environment, 'fixture', principal, {
      kind: 'workload',
      enabled: true,
      markings: ['private'],
      attributes: {},
    });
    await authority.setApplication(environment, 'fixture', application, operations);
    await authority.setGrants(environment, 'fixture', principal, application, operations);
    const issued = await sessions.mint({ environment, principal, application }, 'fixture');
    let calls = 0,
      mode = 'text',
      embeddingWidth = 3;
    let probeSocketClosed = false;
    let signal = () => {},
      release = () => {};
    let barrier = new Promise<void>((resolve) => (signal = resolve)),
      held = new Promise<void>((resolve) => (release = resolve));
    const secret = randomUUID();
    const server = createServer(async (req, res) => {
      calls++;
      try {
        assert.equal(req.headers.authorization, `Bearer ${secret}`);
        let bytes = '';
        for await (const part of req) bytes += part;
        const body = JSON.parse(bytes);
        assert.equal(body.model, 'fixture-model');
        if (mode === 'hold') {
          signal();
          await held;
        }
        if (req.url === '/v1/embeddings' || req.url === '/api/embed') {
          const native = req.url === '/api/embed';
          if (native) assert.equal(body.truncate, false);
          res.setHeader('content-type', 'application/json');
          if (native && mode === 'contextLimit') {
            res.statusCode = 400;
            res.end(JSON.stringify({ error: 'the input length exceeds the context length' }));
            return;
          }
          const embeddings = body.input.map((_: unknown, i: number) =>
            mode === 'zero'
              ? [0, 0, 0]
              : mode === 'dimension'
                ? [1, 0]
                : embeddingWidth === 768
                  ? Array.from({ length: 768 }, () => 0.123456789)
                  : i === 0
                    ? [1, 0, 0]
                    : [0, 1, 0],
          );
          res.end(
            JSON.stringify(
              native
                ? { model: 'fixture-model', embeddings, prompt_eval_count: 4, total_duration: 100 }
                : {
                    data: embeddings
                      .map((embedding: number[], index: number) => ({ index, embedding }))
                      .reverse(),
                    usage: { prompt_tokens: 4, total_tokens: 4 },
                  },
            ),
          );
          return;
        }

        if (body.stream) {
          res.setHeader('content-type', 'text/event-stream');
          const chunk = (delta: unknown, finish_reason: unknown = null) =>
            `data: ${JSON.stringify({ id: 'provider-stream', choices: [{ index: 0, delta, finish_reason }] })}\n\n`;
          if (mode === 'probeHold') {
            probeSocketClosed = false;
            res.once('close', () => {
              if (!res.writableEnded) probeSocketClosed = true;
            });
            res.write(chunk({ role: 'assistant', content: 'held probe' }));
            signal();
            await held;
            res.write(chunk({}, 'stop'));
            res.end('data: [DONE]\n\n');
            return;
          }
          if (body.tools && mode !== 'streamNoTools') {
            res.write(
              chunk({
                role: 'assistant',
                tool_calls: [
                  {
                    index: 0,
                    id: 'stream-tool',
                    type: 'function',
                    function: { name: body.tools[0].function.name, arguments: '{"value":' },
                  },
                ],
                vendor_reason: 'retained',
              }),
            );
            res.write(chunk({ tool_calls: [{ index: 0, function: { arguments: '7}' } }] }));
            res.write(chunk({}, 'tool_calls'));
            res.end('data: [DONE]\n\n');
            return;
          }
          const content = body.response_format ? '{"answer":7}' : 'λ';
          res.write(chunk({ role: 'assistant', content, vendor_reason: 'retained' }));
          if (mode === 'streamHold') {
            signal();
            await held;
          }
          if (mode === 'broken') {
            res.end();
            return;
          }
          if (!body.response_format) res.write(chunk({ content: ' done' }));
          res.write(chunk({}, 'stop'));
          res.end('data: [DONE]\n\n');
          return;
        }
        res.setHeader('content-type', 'application/json');
        if (mode === 'oversize') {
          res.end(JSON.stringify({ data: 'x'.repeat(1024 * 1024) }));
          return;
        }
        const tool = body.tools?.[0]?.function;
        const content = body.response_format
          ? mode === 'invalidSchema'
            ? '{}'
            : '{"answer":7}'
          : 'native response';
        const message = tool
          ? {
              role: 'assistant',
              content: null,
              tool_calls: [
                {
                  id: 'tool-1',
                  type: 'function',
                  function: {
                    name: tool.name,
                    arguments: mode === 'invalidTool' ? '{}' : '{"value":7}',
                  },
                },
              ],
            }
          : { role: 'assistant', content };
        res.end(
          JSON.stringify({
            id: 'provider-id',
            choices: [{ index: 0, message, finish_reason: tool ? 'tool_calls' : 'stop' }],
            usage: { prompt_tokens: 5, completion_tokens: 3, total_tokens: 8 },
            vendor_field: { verified: true },
          }),
        );
      } catch (error) {
        res.destroy(error instanceof Error ? error : undefined);
      }
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    t.after(async () => {
      release();
      for (const child of processes) child.kill('SIGKILL');
      await Promise.allSettled(exits);
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await db.close();
    });
    const endpoint = `http://127.0.0.1:${(server.address() as { port: number }).port}/v1/`;
    const definition = {
      endpointRef: 'FIXTURE_MODEL_ENDPOINT',
      secretRef: 'FIXTURE_MODEL_SECRET',
      model: 'fixture-model',
      modelRevision: 'fixture-v1',
      resourceClass: 'models',
      destination: 'local-model',
      markings: ['private'],
      destinationPolicy: { allowedDestinations: ['local-model'], allowedMarkings: ['private'] },
      capabilities: ['chat', 'embed', 'structured', 'tools', 'stream'],
      dimensions: 3,
      embeddingProtocol: 'openai-compatible-v1',
      timeoutSeconds: 120,
    };
    const binding = () =>
      db.pool.query(
        'INSERT INTO moria.native_model_bindings(environment,resource,revision,definition) VALUES($1,$2,$3,$4) ON CONFLICT(environment,resource) DO UPDATE SET definition=EXCLUDED.definition',
        [environment, 'model', 'v1', definition],
      );
    await binding();
    const role = `models_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role}`,
    );
    const connection = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const input = { messages: [{ role: 'user', content: 'hello' }], maxTokens: 100 };
    const nativeEnvironment = {
      PATH: '/usr/local/bin:/usr/bin:/bin',
      MORIA_INDEXING_DATABASE_SECRET_REF: 'MODEL_DATABASE',
      MODEL_DATABASE: connection,
      MORIA_INDEXING_DATABASE_TLS: 'disable',
      FIXTURE_MODEL_ENDPOINT: endpoint,
      FIXTURE_MODEL_SECRET: secret,
    };
    const launch = (
      domain: string,
      command: string,
      value: unknown,
      overrides: Record<string, string> = {},
    ) => {
      const child = spawn(
        process.env.MORIA_NATIVE_BINARY ?? '/usr/local/bin/moria-runtime',
        [domain, command],
        { env: { ...nativeEnvironment, ...overrides }, stdio: ['pipe', 'pipe', 'pipe'] },
      );
      exits.push(new Promise((resolve) => child.once('exit', resolve)));
      processes.push(child);
      let stdout = '',
        stderr = '';
      child.stdout.on('data', (chunk) => (stdout += chunk));
      child.stderr.on('data', (chunk) => (stderr += chunk));
      child.stdin.end(JSON.stringify(value));
      const done = new Promise<any>((resolve, reject) => {
        child.once('error', reject);
        child.once('exit', (code) => {
          assert.ok(!stderr.includes(secret));
          code === 0
            ? resolve(
                stdout
                  .trim()
                  .split('\n')
                  .map((line) => JSON.parse(line)),
              )
            : reject(new Error(stderr));
        });
      });
      void done.catch(() => {});
      return { child, done };
    };
    const start = (command: string, value: unknown, key = randomUUID()) =>
      launch('models', command, { token: issued.token, resource: 'model', key, input: value });
    const reset = (next: string) => {
      mode = next;
      barrier = new Promise<void>((resolve) => (signal = resolve));
      held = new Promise<void>((resolve) => (release = resolve));
    };
    await t.test(
      'configured capabilities remain unavailable until actual probes pass',
      async () => {
        await assert.rejects(start('chat', input).done);
        assert.equal(calls, 0);
        const result = (await start('probe', {}).done)[0];
        assert.deepEqual(result.capabilities, [
          'chat',
          'embed',
          'stream',
          'streamStructured',
          'streamTools',
          'structured',
          'tools',
        ]);
        assert.deepEqual(result.unsupported, {});
      },
    );
    await t.test(
      'stable calls replay exactly without another provider request; conflicts fail',
      async () => {
        const key = randomUUID(),
          before = calls;
        const first = await start('chat', input, key).done;
        assert.equal(first[0].content, 'native response');
        assert.deepEqual(
          (
            await db.pool.query(
              'SELECT state,uncertain FROM moria.native_model_calls WHERE id=$1',
              [first[0].id],
            )
          ).rows[0],
          { state: 'succeeded', uncertain: false },
        );
        assert.match(first[0].modelBindingFingerprint, /^sha256:[0-9a-f]{64}$/);
        assert.equal(
          (await start('capabilities', {}).done)[0].modelBindingFingerprint,
          first[0].modelBindingFingerprint,
        );
        assert.equal(first[0].usage.inputTokens, '5');
        assert.deepEqual(first[0].metadata.provider.vendor_field, { verified: true });
        assert.equal(first[0].metadata.provider.choices[0].message.content, undefined);
        assert.deepEqual(
          (
            await db.pool.query('SELECT usage FROM moria.native_model_attempts WHERE call_id=$1', [
              first[0].id,
            ])
          ).rows[0].usage,
          first[0].usage,
        );
        assert.deepEqual(await start('chat', input, key).done, first);
        assert.equal(calls, before + 1);
        await assert.rejects(start('chat', { ...input, maxTokens: 99 }, key).done);
        assert.equal(calls, before + 1);
      },
    );
    await t.test(
      'structured output and tool arguments use a real bounded JSON Schema validator',
      async () => {
        const structured = {
          ...input,
          responseFormat: {
            name: 'answer',
            schema: {
              type: 'object',
              properties: { answer: { type: 'integer' } },
              required: ['answer'],
              additionalProperties: false,
            },
          },
        };
        assert.equal((await start('chat', structured).done)[0].structured.answer, 7);
        mode = 'invalidSchema';
        await assert.rejects(start('chat', structured).done);
        const tool = {
          ...input,
          tools: [
            {
              name: 'moria_tool',
              parameters: {
                type: 'object',
                properties: { value: { type: 'integer' } },
                required: ['value'],
                additionalProperties: false,
              },
            },
          ],
        };
        mode = 'text';
        assert.equal((await start('chat', tool).done)[0].toolCalls[0].arguments.value, 7);
        mode = 'invalidTool';
        await assert.rejects(start('chat', tool).done);
        const before = calls;
        await assert.rejects(
          start('chat', {
            ...input,
            responseFormat: { name: 'bad', schema: { $ref: 'http://127.0.0.1/schema' } },
          }).done,
        );
        assert.equal(calls, before);
        mode = 'text';
      },
    );
    await t.test(
      'embeddings preserve input order and exact generation dimension, rejecting zero and malformed vectors',
      async () => {
        const result = (await start('embed', { input: ['a', 'b'] }).done)[0];
        assert.deepEqual(result.vectors, [
          [1, 0, 0],
          [0, 1, 0],
        ]);
        assert.equal(result.dimensions, 3);
        assert.equal(result.metadata.provider.data[0].embedding, undefined);
        mode = 'zero';
        await assert.rejects(start('embed', { input: ['a'] }).done);
        mode = 'dimension';
        await assert.rejects(start('embed', { input: ['a'] }).done);
        mode = 'text';
      },
    );
    await t.test(
      'streams retain ordered text and terminal result; broken streams never restart',
      async () => {
        const output = await start('stream', input).done;
        assert.deepEqual(
          output.filter((v: any) => v.kind === 'text').map((v: any) => v.text),
          ['λ', ' done'],
        );
        assert.equal(output.at(-1).result.content, 'λ done');
        mode = 'broken';
        const before = calls;
        await assert.rejects(start('stream', input).done);
        assert.equal(calls, before + 1);
        mode = 'text';
      },
    );
    await t.test(
      'stream combinations require their own probe and preserve provider extensions',
      async () => {
        const tools = {
          ...input,
          tools: [
            {
              name: 'moria_tool',
              parameters: {
                type: 'object',
                properties: { value: { type: 'integer' } },
                required: ['value'],
                additionalProperties: false,
              },
            },
          ],
        };
        const structured = {
          ...input,
          responseFormat: {
            name: 'answer',
            schema: {
              type: 'object',
              properties: { answer: { type: 'integer' } },
              required: ['answer'],
              additionalProperties: false,
            },
          },
        };
        const output = await start('stream', tools).done;
        assert.equal(output.at(-1).result.toolCalls[0].arguments.value, 7);
        assert.equal(
          output.at(-1).result.metadata.provider.streamExtensions[0].choices[0].delta.vendor_reason,
          'retained',
        );
        assert.equal((await start('stream', structured).done).at(-1).result.structured.answer, 7);
        await db.pool.query(
          "DELETE FROM moria.native_model_probes WHERE environment=$1 AND capability='streamTools'",
          [environment],
        );
        const before = calls;
        await assert.rejects(start('stream', tools).done);
        assert.equal(calls, before);
        mode = 'streamNoTools';
        const probes = (await start('probe', {}).done)[0];
        assert.equal(probes.unsupported.streamTools, 'PROBE_FAILED');
        await assert.rejects(start('stream', tools).done);
        mode = 'text';
        assert.deepEqual((await start('probe', {}).done)[0].unsupported, {});
      },
    );
    await t.test(
      'revocation during inference blocks result release and preserves the failed attempt',
      async () => {
        reset('hold');
        const call = start('chat', input);
        await barrier;
        await authority.setGrants(environment, 'fixture', principal, application, []);
        release();
        await assert.rejects(call.done);
        assert.equal(
          (
            await db.pool.query(
              "SELECT count(*) FROM moria.native_model_calls WHERE environment=$1 AND state='failed'",
              [environment],
            )
          ).rows[0].count !== '0',
          true,
        );
        await authority.setGrants(environment, 'fixture', principal, application, operations);
        mode = 'text';
      },
    );
    await t.test(
      'an unobserved binding disable and re-enable cannot revive an admitted call',
      async () => {
        reset('hold');
        const key = randomUUID(),
          call = start('chat', input, key);
        await barrier;
        const client = await db.pool.connect();
        try {
          await client.query('BEGIN');
          await client.query(
            "UPDATE moria.native_model_bindings SET enabled=false WHERE environment=$1 AND resource='model'",
            [environment],
          );
          await client.query(
            "UPDATE moria.native_model_bindings SET enabled=true WHERE environment=$1 AND resource='model'",
            [environment],
          );
          await client.query('COMMIT');
        } finally {
          await client.query('ROLLBACK');
          client.release();
        }
        release();
        await assert.rejects(call.done);
        const row = (
          await db.pool.query(
            'SELECT state,error FROM moria.native_model_calls WHERE environment=$1 AND call_key=$2',
            [environment, key],
          )
        ).rows[0];
        assert.equal(row.state, 'failed');
        assert.equal(row.error, 'AUTHORIZATION_CHANGED');
        mode = 'text';
        assert.ok(await start('chat', input).done);
      },
    );
    await t.test('stream revocation blocks later events and its terminal result', async () => {
      reset('streamHold');
      const call = start('stream', input);
      const first = new Promise<void>((resolve) => call.child.stdout.once('data', () => resolve()));
      await barrier;
      await first;
      await authority.setGrants(environment, 'fixture', principal, application, []);
      release();
      await assert.rejects(call.done);
      await authority.setGrants(environment, 'fixture', principal, application, operations);
      mode = 'text';
    });
    await t.test(
      'cross-process cancellation interrupts waiting inference and retains uncertain admission',
      { timeout: 15000 },
      async () => {
        reset('hold');
        const key = randomUUID(),
          before = calls;
        const call = start('chat', input, key);
        await barrier;
        assert.equal((await start('status', {}, key).done)[0].state, 'running');
        const started = Date.now();
        const cancelled = (await start('cancel', {}, key).done)[0];
        assert.equal(cancelled.cancelled, true);
        assert.equal(cancelled.uncertain, true);
        await assert.rejects(call.done);
        assert.ok(Date.now() - started < 10000);
        const retained = (await start('status', {}, key).done)[0];
        assert.equal(retained.error, 'MODEL_CANCELLED');
        assert.equal(retained.uncertain, true);
        await assert.rejects(
          db.pool.query(
            "INSERT INTO moria.native_model_attempts(call_id,ordinal,status) SELECT c.id,COALESCE(max(a.ordinal),0)+1,'started' FROM moria.native_model_calls c LEFT JOIN moria.native_model_attempts a ON a.call_id=c.id WHERE c.environment=$1 AND c.call_key=$2 GROUP BY c.id",
            [environment, key],
          ),
          /MODEL_CALL_UNAVAILABLE/,
        );
        assert.equal(
          (await db.pool.query("SELECT moria.runtime_resource_in_use('models') AS used")).rows[0]
            .used !== '0',
          true,
        );
        await assert.rejects(start('chat', input, key).done);
        assert.equal(calls, before + 1);
        release();
        mode = 'text';
      },
    );
    await t.test(
      'provider bodies and caller-selected endpoints remain bounded and protected',
      async () => {
        mode = 'oversize';
        await assert.rejects(start('chat', input).done);
        mode = 'text';
        const before = calls;
        await assert.rejects(
          start('chat', { ...input, endpoint: 'http://untrusted.invalid' }).done,
        );
        assert.equal(calls, before);
        definition.destinationPolicy.allowedMarkings = [];
        await binding();
        await assert.rejects(start('chat', input).done);
        assert.equal(calls, before);
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.native_model_attempts a JOIN moria.native_model_calls c ON c.id=a.call_id WHERE c.environment=$1',
              [environment],
            )
          ).rows[0].count,
          String(calls),
        );
      },
    );
    await t.test(
      'the persistent native HTTP role serves authenticated APIs and bounds concurrent inference',
      async () => {
        assert.ok(
          Number(
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.native_model_calls WHERE environment=$1 AND uncertain AND deadline>clock_timestamp()',
                [environment],
              )
            ).rows[0].count,
          ) > 0,
          'uncertain provider outcomes retain their original admission through the deadline',
        );
        await db.pool.query(
          "INSERT INTO moria.runtime_resource_classes VALUES('httpmodels',true,8,8)",
        );
        definition.resourceClass = 'httpmodels';
        definition.destinationPolicy.allowedMarkings = ['private'];
        await binding();
        assert.deepEqual((await start('probe', {}).done)[0].unsupported, {});
        const child = spawn(
          process.env.MORIA_NATIVE_BINARY ?? '/usr/local/bin/moria-runtime',
          ['models', 'serve'],
          {
            env: { ...nativeEnvironment, MORIA_NATIVE_MODEL_LISTEN: '127.0.0.1:0' },
            stdio: ['ignore', 'pipe', 'pipe'],
          },
        );
        processes.push(child);
        exits.push(new Promise((resolve) => child.once('exit', resolve)));
        const address = await new Promise<string>((resolve, reject) => {
          let output = '';
          child.stdout.on('data', (chunk) => {
            output += chunk;
            for (const line of output.split('\n').filter(Boolean)) {
              const value = JSON.parse(line);
              if (value.event === 'modelsServing') resolve(value.address);
            }
          });
          child.once('error', reject);
          child.once('exit', () => reject(new Error('model HTTP role exited before readiness')));
        });
        const request = (operation: string, value: unknown, headers: Record<string, string> = {}) =>
          fetch(`http://${address}/api/v1/models/model/${operation}`, {
            method: 'POST',
            headers: {
              authorization: `Bearer ${issued.token}`,
              'content-type': 'application/json',
              'idempotency-key': randomUUID(),
              ...headers,
            },
            body: JSON.stringify(value),
          });
        assert.equal(
          (await request('chat', input, { authorization: 'Bearer invalid' })).status,
          401,
        );
        const foreignHost = await new Promise<number>((resolve, reject) => {
          const req = httpRequest(
            `http://${address}/api/v1/models/model/chat`,
            {
              method: 'POST',
              headers: {
                host: 'unexpected.invalid',
                authorization: `Bearer ${issued.token}`,
                'content-type': 'application/json',
                'idempotency-key': randomUUID(),
              },
            },
            (response) => {
              response.resume();
              resolve(response.statusCode!);
            },
          );
          req.once('error', reject);
          req.end(JSON.stringify(input));
        });
        assert.equal(foreignHost, 403);
        const response = await request('chat', input);
        assert.equal(response.status, 200);
        assert.equal(response.headers.get('cache-control'), 'no-store');
        assert.equal((await response.json()).content, 'native response');
        const retainedKey = randomUUID();
        assert.equal(
          (await request('chat', input, { 'idempotency-key': retainedKey })).status,
          200,
        );
        await authority.setGrants(environment, 'fixture', principal, application, []);
        assert.equal(
          (await request('chat', { ...input, maxTokens: 99 }, { 'idempotency-key': retainedKey }))
            .status,
          403,
        );
        await authority.setGrants(environment, 'fixture', principal, application, operations);
        const stream = await request('stream', input);
        assert.equal(stream.status, 200);
        assert.match(stream.headers.get('content-type') ?? '', /^text\/event-stream/);
        const text = await stream.text();
        assert.match(text, /event: result/);
        assert.match(text, /λ/);
        reset('hold');
        const before = calls;
        const pending = Array.from({ length: 8 }, () => request('chat', input));
        for (let i = 0; i < 500 && calls < before + 8; i++) await delay(10);
        assert.equal(
          calls,
          before + 8,
          'all admitted calls must reach the explicit upstream barrier',
        );
        assert.equal((await request('chat', input)).status, 429);
        release();
        for (const response of await Promise.all(pending)) {
          assert.equal(response.status, 200);
          await response.arrayBuffer();
        }
        mode = 'text';
      },
    );
    await t.test(
      'one shared class budget covers native execution leases and model calls in both directions',
      async () => {
        definition.resourceClass = 'standard';
        await binding();
        assert.deepEqual((await start('probe', {}).done)[0].unsupported, {});
        await db.pool.query(
          "UPDATE moria.runtime_resource_classes SET maximum_tasks=1,maximum_environment_tasks=1 WHERE name='standard'",
        );
        const epoch = (await authority.current(environment, principal, application)).epoch;
        const bindingId = (
          await db.pool.query(
            `INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation)
      SELECT $1,$2,$3,$4,$5,$6,$7,i.security_epoch,c.incarnation FROM moria.instance_state i,moria.indexing_instance c RETURNING id`,
            [
              environment,
              randomUUID(),
              {
                version: 1,
                domain: 'transform',
                release: 'a'.repeat(64),
                resourceClass: 'standard',
                timeoutSeconds: 120,
                retryPolicy: 'idempotent',
                inputSchema: { type: 'object', properties: {} },
                pinnedInputs: [],
                expectedHeads: {},
              },
              { environment, principal, application },
              ['work:execute'],
              ['private'],
              epoch,
            ],
          )
        ).rows[0].id;
        const execution = async (command: string, value: unknown) =>
          (await launch('execution', command, value).done)[0];
        const request = async () =>
          (
            await execution('request', {
              binding: bindingId,
              requestKey: randomUUID(),
              input: {},
              tasks: [{ key: 'one' }],
            })
          ).id;
        const first = await request();
        assert.ok(
          (await execution('claim', { owner: randomUUID(), domains: ['transform'] })).lease,
        );
        const before = calls;
        await assert.rejects(start('chat', input).done);
        assert.equal(calls, before);
        await execution('cancel', { id: first });
        reset('hold');
        const call = start('chat', input);
        await barrier;
        const second = await request();
        assert.equal(
          await execution('claim', { owner: randomUUID(), domains: ['transform'] }),
          null,
        );
        release();
        await call.done;
        assert.ok(
          (await execution('claim', { owner: randomUUID(), domains: ['transform'] })).lease,
        );
        await execution('cancel', { id: second });
        mode = 'text';
      },
    );
    await t.test(
      'bounded embedding batches retain one vector copy and terminal validation failures are recorded',
      async () => {
        embeddingWidth = 768;
        definition.dimensions = 768;
        await binding();
        assert.deepEqual((await start('probe', {}).done)[0].unsupported, {});
        const result = (
          await start('embed', { input: Array.from({ length: 64 }, (_, i) => `text ${i}`) }).done
        )[0];
        assert.equal(result.vectors.length, 64);
        assert.equal(result.vectors[63].length, 768);
        assert.ok(Buffer.byteLength(JSON.stringify(result)) < 1024 * 1024);
        assert.equal(result.metadata.provider.data[0].embedding, undefined);
        embeddingWidth = 3;
        let probeSocketClosed = false;
        definition.dimensions = 3;
        await binding();
        assert.deepEqual((await start('probe', {}).done)[0].unsupported, {});
        const before = calls;
        await assert.rejects(
          start('chat', {
            ...input,
            tools: [{ name: 'tool', parameters: { type: 'object' } }],
            responseFormat: { name: 'format', schema: { type: 'object' } },
          }).done,
        );
        assert.equal(calls, before);
        assert.equal(
          (
            await db.pool.query(
              "SELECT count(*) FROM moria.native_model_calls WHERE environment=$1 AND state='running'",
              [environment],
            )
          ).rows[0].count,
          '0',
        );
      },
    );
    await t.test(
      'a permanently lost COMMIT acknowledgment releases admission and replays the committed receipt',
      { timeout: 15000 },
      async () => {
        const previous = {
          resourceClass: definition.resourceClass,
          timeoutSeconds: definition.timeoutSeconds,
        };
        await db.pool.query(
          "INSERT INTO moria.runtime_resource_classes VALUES('commitmodels',true,2,2)",
        );
        definition.resourceClass = 'commitmodels';
        definition.timeoutSeconds = 5;
        await binding();
        assert.deepEqual((await start('probe', {}).done)[0].unsupported, {});
        const proxy = await transactionCommitProxy(
          Number(db.pool.options.port),
          "UPDATE moria.native_model_calls SET state='succeeded'",
        );
        try {
          const key = randomUUID(),
            before = calls;
          const call = launch(
            'models',
            'chat',
            { token: issued.token, resource: 'model', key, input },
            {
              MODEL_DATABASE: nativeEnvironment.MODEL_DATABASE.replace(
                `:${db.pool.options.port}/`,
                `:${proxy.port}/`,
              ),
            },
          );
          await proxy.committed;
          let changed = false;
          const mutation = authority
            .setGrants(environment, 'fixture', principal, application, operations)
            .then(() => (changed = true));
          await delay(100);
          assert.equal(
            changed,
            false,
            'the actual lost COMMIT acknowledgment still holds protected admission',
          );
          const result = await call.done;
          await mutation;
          assert.equal(result[0].content, 'native response');
          assert.equal(calls, before + 1);
          assert.deepEqual(await start('chat', input, key).done, result);
          assert.equal(calls, before + 1);
          assert.equal(proxy.checkpoints.commitResponses, 1);
        } finally {
          await proxy.close();
          Object.assign(definition, previous);
          await binding();
        }
      },
    );
    await t.test(
      'Ollama embedding transport disables truncation and records terminal context failure',
      async () => {
        definition.embeddingProtocol = 'ollama-v1';
        await binding();
        assert.deepEqual((await start('probe', {}).done)[0].unsupported, {});
        const result = (await start('embed', { input: ['a', 'b'] }).done)[0];
        assert.deepEqual(result.vectors, [
          [1, 0, 0],
          [0, 1, 0],
        ]);
        assert.equal(result.metadata.provider.providerProtocol, 'ollama-v1');
        assert.equal(result.usage.inputTokens, '4');
        assert.equal(result.metadata.provider.embeddings, undefined);
        mode = 'contextLimit';
        const key = randomUUID();
        await assert.rejects(
          start('embed', { input: ['oversized'] }, key).done,
          /MODEL_CONTEXT_LIMIT/,
        );
        const state = (
          await db.pool.query(
            'SELECT state,error,uncertain FROM moria.native_model_calls WHERE environment=$1 AND call_key=$2',
            [environment, key],
          )
        ).rows[0];
        assert.deepEqual(state, {
          state: 'failed',
          error: 'MODEL_CONTEXT_LIMIT',
          uncertain: false,
        });
        const before = calls;
        await assert.rejects(start('embed', { input: ['oversized'] }, key).done);
        assert.equal(calls, before);
        mode = 'text';
        definition.embeddingProtocol = 'openai-compatible-v1';
        await binding();
      },
    );
    await t.test('expired capability proofs fail before contacting the provider', async () => {
      await db.pool.query(
        "UPDATE moria.native_model_probes SET expires_at=clock_timestamp()-interval '1 second' WHERE environment=$1",
        [environment],
      );
      assert.deepEqual((await start('capabilities', {}).done)[0].capabilities, []);
      const before = calls;
      await assert.rejects(start('chat', input).done);
      assert.equal(calls, before);
    });
    await t.test('a buffered capability probe remains fenced while upstream is held', async () => {
      reset('probeHold');
      const call = start('probe', {});
      await barrier;
      const before = calls;
      await authority.setGrants(environment, 'fixture', principal, application, []);
      try {
        await assert.rejects(call.done);
        const deadline = Date.now() + 10000;
        while (!probeSocketClosed && Date.now() < deadline) await delay(25);
        assert.equal(probeSocketClosed, true, 'Revocation must close the held provider stream');
        assert.equal(calls, before, 'No further probe may dispatch after revocation');
      } finally {
        release();
        await authority.setGrants(environment, 'fixture', principal, application, operations);
        mode = 'text';
      }
    });
  },
);
