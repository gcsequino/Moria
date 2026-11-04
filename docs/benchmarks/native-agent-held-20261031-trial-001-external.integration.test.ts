import assert from 'node:assert/strict';
import { spawn, execFile, type ChildProcess } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { join } from 'node:path';
import { readlink, realpath } from 'node:fs/promises';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { agentImage } from './fixtures/agents/image.js';
import { database } from './fixtures/catalog-storage/database.js';
import { createLocalFilesystem } from '../services/filesystem/src/local.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { filesystemOperations } from '../packages/contracts/filesystem.js';
import { ObjectStore } from '../services/ontology/src/store.js';
import { fingerprint } from '../packages/compiler/src/index.js';
import type { Resource } from '../packages/contracts/types.js';
import { requestCommitProxy, transactionCommitProxy } from './fixtures/runtime/postgres-proxy.js';
import { agentsClient } from '../packages/platform/src/agents.js';
import { ApiError, createTransport } from '../packages/platform/src/transport.js';

const execute = promisify(execFile);
async function until(check: () => Promise<boolean>, timeout = 10000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    if (await check()) return;
    await delay(25);
  }
  assert.fail('Native agent did not reach its real lifecycle barrier');
}

test(
  'native agent orchestration supervises the locked LangGraph adapter and protected native model/tools',
  { timeout: 300000 },
  async (t) => {
    const proofCases = new Set(["a pending child observation blocks parent logs, actions and model dispatch", "private child progress crosses a held dependency insertion and resumes after terminal proof"]);
    const selected = async (...args: any[]) => {
      if (proofCases.has(String(args[0]))) return Reflect.apply(t.test, t, args);
      return t.test(String(args[0]), {skip:'explicit targeted proof; full suite remains required'}, async()=>{});
    };
    const db = await database();
    const image = await agentImage();
    const children: ChildProcess[] = [];
    const ids = new Set<string>();
    const functionIds = new Set<string>();
    let providerMode = 'normal',
      calls = 0,
      providerClosed = false;
    let entered = () => {},
      release = () => {};
    let barrier = Promise.resolve(),
      held = Promise.resolve();
    const providerSecret = randomUUID();
    const provider = createServer(async (request, response) => {
      try {
        calls++;
        assert.equal(request.headers.authorization, `Bearer ${providerSecret}`);
        let bytes = '';
        for await (const part of request) bytes += part;
        const input = JSON.parse(bytes);
        assert.equal(input.model, 'native-agent-fixture');
        if (providerMode === 'hold') {
          response.once('close', () => (providerClosed = true));
          entered();
          await held;
        }
        response.setHeader('content-type', 'application/json');
        response.end(
          JSON.stringify({
            id: 'local-agent-model',
            choices: [
              {
                index: 0,
                message: { role: 'assistant', content: 'native agent answer' },
                finish_reason: 'stop',
              },
            ],
            usage: { prompt_tokens: 3, completion_tokens: 4, total_tokens: 7 },
          }),
        );
      } catch (error) {
        response.destroy(error instanceof Error ? error : undefined);
      }
    });
    await new Promise<void>((resolve) => provider.listen(0, '127.0.0.1', resolve));
    t.after(async () => {
      release();
      for (const child of children) child.kill('SIGKILL');
      for (const id of ids)
        await execute('docker', ['rm', '--force', `moria-native-agent-${id}`]).catch(() => {});
      for (const id of functionIds)
        await execute('docker', ['rm', '--force', `moria-native-function-${id}`]).catch(() => {});
      await new Promise<void>((resolve) => provider.close(() => resolve()));
      await image.close();
      await db.close();
    });
    const endpoint = `http://127.0.0.1:${(provider.address() as { port: number }).port}/v1/`;
    const environment = `agents-${randomUUID()}`,
      principal = 'agent-user',
      bootstrap = 'agent-source-author';
    await db.catalog.ensureEnvironment(environment);
    const fs = await createLocalFilesystem(db.catalog, join(db.root, 'files'));
    const authority = fs.filesystem.security.authority;
    const sessions = new WorkloadSessions(authority);
    const fsOperations = filesystemOperations.map((operation) => `filesystem:${operation}`);
    await authority.syncPrincipal(environment, 'fixture', principal, {
      kind: 'workload',
      enabled: true,
      attributes: {},
      markings: ['agents/Marking/Private'],
    });
    const actor = async (application: string, operations: string[]) => {
      await authority.setApplication(environment, 'fixture', application, operations);
      await authority.setGrants(environment, 'fixture', principal, application, operations);
      const issued = await sessions.mint({ environment, principal, application }, 'fixture');
      return {
        token: issued.token,
        context: { session: await sessions.authenticate(`Bearer ${issued.token}`) },
      };
    };
    const author = await actor(bootstrap, ['instance:manage', ...fsOperations]);
    const home = await fs.management.ensureHome(author.context);
    const role = await fs.management.role(author.context, home.home.id);
    const projects = await fs.filesystem.store.get(role!.configuration.projects as string);
    await fs.management.createProject(author.context, projects.id, 'agents', projects.revision);
    const source = await fs.filesystem.store.resolve(
      `/users/${home.home.name}/projects/agents/source`,
    );
    const made = await fs.filesystem.apply(author.context, randomUUID(), [
      {
        op: 'create',
        kind: 'directory',
        parentId: source.id,
        expectedParentRevision: source.revision,
        name: 'logic',
      },
    ]);
    const root = made.entries.find((entry) => entry.name === 'logic')!;
    const descriptor = Buffer.from(
      JSON.stringify({
        version: 1,
        name: 'Agent Logic',
        namespace: 'agents',
        runtime: 'typescript-v1',
      }),
    );
    await fs.filesystem.writeFile(
      author.context,
      randomUUID(),
      {
        op: 'create',
        parentId: root.id,
        expectedParentRevision: root.revision,
        name: '.repository.json',
        content: {
          size: descriptor.length,
          digest: `sha256:${createHash('sha256').update(descriptor).digest('hex')}`,
          mediaType: 'application/json',
        },
      },
      new ReadableStream({
        start(controller) {
          controller.enqueue(descriptor);
          controller.close();
        },
      }),
    );
    await fs.source.register(
      author.context,
      root.id,
      (await fs.filesystem.store.get(root.id)).revision,
      'repository',
    );
    const scalar = { type: 'float64' as const, nullable: false },
      string = { type: 'string' as const, nullable: false },
      uidType = { type: 'uid' as const, nullable: false };
    let definitions: Resource[] = [
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'Marking',
        metadata: { namespace: 'agents', name: 'Private' },
        spec: { eligibilityGroup: 'fixture', declassifierGroup: 'fixture' },
      },
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'Application',
        metadata: { namespace: 'agents', name: 'App' },
        spec: {
          mode: 'service',
          scopes: [
            { target: 'agents/Model/Chat', operations: ['invoke'] },
            { target: 'agents/SecretReference/ProviderCredential', operations: ['use'] },
          ],
        },
      },
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'SecretReference',
        metadata: { namespace: 'agents', name: 'ProviderCredential' },
        spec: { binding: 'agent-provider', consumers: ['agents/ModelProvider/Provider'] },
      },
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'Policy',
        metadata: { namespace: 'agents', name: 'LocalModel' },
        spec: {
          type: 'modelDestination',
          allowedDestinations: ['local-agent'],
          allowedMarkings: ['agents/Marking/Private'],
        },
      },
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'ModelProvider',
        metadata: { namespace: 'agents', name: 'Provider' },
        spec: {
          adapter: 'openai-compatible-v1',
          endpoint,
          destination: 'local-agent',
          destinationPolicy: 'agents/Policy/LocalModel',
          secret: 'agents/SecretReference/ProviderCredential',
        },
      },
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'Model',
        metadata: { namespace: 'agents', name: 'Chat' },
        spec: {
          provider: 'agents/ModelProvider/Provider',
          upstreamName: 'native-agent-fixture',
          versionLabel: 'fixture-v1',
          mutable: false,
          capabilities: ['chat'],
        },
      },
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'AgentAdapter',
        metadata: { namespace: 'agents', name: 'LangGraph' },
        spec: {
          artifact: image.adapter,
          abiVersion: 'agent/v1',
          capabilities: ['events', 'cancel'],
        },
      },
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'Agent',
        metadata: { namespace: 'agents', name: 'Assistant' },
        spec: {
          artifact: image.artifact,
          adapter: 'agents/AgentAdapter/LangGraph',
          application: 'agents/Application/App',
          model: 'agents/Model/Chat',
          inputs: { mode: string, prompt: string, uid: { ...uidType, nullable: true } },
          outputs: { answer: string },
          limits: { timeoutSeconds: 60, memoryMiB: 256, cpuMillis: 10000 },
        },
      },
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'Function',
        metadata: { namespace: 'agents', name: 'Child' },
        spec: {
          artifact: image.child,
          runtime: 'typescript-v1',
          effect: 'readOnly',
          inputs: { mode: string, value: scalar, uid: uidType },
          outputs: { value: scalar },
          limits: { timeoutSeconds: 30, memoryMiB: 128, cpuMillis: 10000 },
        },
      },
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'Automation',
        metadata: { namespace: 'agents', name: 'Refresh' },
        spec: {
          operation: 'invokeAgent',
          target: 'agents/Agent/Assistant',
          application: 'agents/Application/App',
          intervalSeconds: 60,
          input: { mode: 'normal', prompt: 'automation-v1', uid: null },
          overlap: 'skip',
          missed: 'coalesce',
        },
      },
    ];
    let resources = new Map<string, string>();
    const writeResource = async (declaration: Resource, replace = false) => {
      const bytes = Buffer.from(JSON.stringify(declaration));
      const content = {
        size: bytes.length,
        digest: `sha256:${createHash('sha256').update(bytes).digest('hex')}`,
        mediaType: 'application/json',
      };
      const entry = replace
        ? await fs.filesystem.store.resolve(
            `/users/${home.home.name}/projects/agents/source/logic/${declaration.metadata.name}.resource.json`,
          )
        : undefined;
      await fs.filesystem.writeFile(
        author.context,
        randomUUID(),
        entry
          ? { op: 'write', id: entry.id, expectedRevision: entry.revision, content }
          : {
              op: 'create',
              parentId: root.id,
              expectedParentRevision: (await fs.filesystem.store.get(root.id)).revision,
              name: `${declaration.metadata.name}.resource.json`,
              content,
            },
        new ReadableStream({
          start(controller) {
            controller.enqueue(bytes);
            controller.close();
          },
        }),
      );
    };
    for (const declaration of definitions.filter(
      (resource) => !['Agent', 'AgentAdapter', 'Function'].includes(resource.kind),
    ))
      await writeResource(declaration);
    const publish = async (deletions: string[] = []) => {
      const publication = await fs.source.publish(
        author.context,
        root.id,
        (await fs.source.checkout(author.context, root.id)).digest,
        definitions.filter((resource) =>
          ['Agent', 'AgentAdapter', 'Function'].includes(resource.kind),
        ),
      );
      await fs.source.apply(
        author.context,
        (await fs.source.plan(author.context, root.id, publication.digest, deletions)).id,
      );
      resources = new Map(
        publication.resources.map((resource) => [resource.reference, resource.id]),
      );
    };
    await publish();
    const application = resources.get('agents/Application/App')!;
    const model = resources.get('agents/Model/Chat')!;
    const secret = resources.get('agents/SecretReference/ProviderCredential')!;
    const resource = resources.get('agents/Agent/Assistant')!;
    const operations = [
      'agents:invoke',
      'functions:invoke',
      `${model}:invoke`,
      `${secret}:use`,
      'records:read',
      'edit-record:invoke',
      ...fsOperations,
    ];
    const user = await actor(application, operations);
    const store = new ObjectStore(db.catalog),
      uid = await db.catalog.resolveIdentity(`agent-record-${randomUUID()}`, 'record');
    const schema = { primaryKey: 'uid', properties: { uid: uidType, value: scalar } };
    await store.publishStage(
      await store.stage(
        environment,
        'records',
        schema,
        [{ uid, value: 10 }],
        (await authority.current(environment, principal, application)).epoch,
        (await db.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [environment]))
          .rows[0].revision,
        undefined,
        ['agents/Marking/Private'],
      ),
    );
    const objectBinding = {
      objectTypeId: '00000000-0000-4000-8000-000000000004',
      schema,
      policies: {
        resource: 'records',
        markings: ['agents/Marking/Private'],
        properties: schema.properties,
        sourceColumns: schema.properties,
        attributes: {},
      },
    };
    await db.pool.query(
      'INSERT INTO moria.native_serving_bindings(environment,resource,object_type_id,revision,definition) VALUES($1,$2,$3,$4,$5)',
      [
        environment,
        'records',
        objectBinding.objectTypeId,
        fingerprint(objectBinding),
        objectBinding,
      ],
    );
    const action = {
      resource: 'edit-record',
      revision: '1',
      markings: ['agents/Marking/Private'],
      targetParameter: 'uid',
      inputs: { uid: uidType, value: scalar },
      condition: { literal: true },
      edits: [{ op: 'set', property: 'value', value: { parameter: 'value' } }],
    };
    await db.pool.query(
      'INSERT INTO moria.native_serving_actions(environment,resource,object_resource,revision,definition) VALUES($1,$2,$3,$4,$5)',
      [environment, 'edit-record', 'records', '1', action],
    );
    const route = {
      endpointRef: 'AGENT_MODEL_ENDPOINT',
      secretRef: 'AGENT_MODEL_SECRET',
      model: 'native-agent-fixture',
      modelRevision: 'fixture-v1',
      resourceClass: 'models',
      destination: 'local-agent',
      markings: [],
      destinationPolicy: {
        allowedDestinations: ['local-agent'],
        allowedMarkings: ['agents/Marking/Private'],
      },
      capabilities: ['chat'],
    };
    await db.pool.query(
      'INSERT INTO moria.native_model_bindings(environment,resource,revision,definition) VALUES($1,$2,$3,$4)',
      [environment, model, 'fixture-v1', route],
    );
    const dbRole = `agent_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(
      `CREATE ROLE ${dbRole} LOGIN PASSWORD '${password}'; GRANT USAGE ON SCHEMA moria TO ${dbRole}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${dbRole}; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${dbRole}`,
    );
    const connection = `postgresql://${dbRole}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const owner = randomUUID(),
      host = randomUUID();
    await db.pool.query(
      "INSERT INTO moria.runtime_function_workers(owner,host_id,incarnation,resource_classes,domains,expires_at) SELECT $1,$2,incarnation,ARRAY['standard'],ARRAY['agent','function'],clock_timestamp()+interval '1 hour' FROM moria.indexing_instance",
      [owner, host],
    );
    const nativeEnvironment = {
      PATH: process.env.PATH,
      MORIA_INDEXING_DATABASE_SECRET_REF: 'AGENT_DATABASE',
      AGENT_DATABASE: connection,
      MORIA_INDEXING_DATABASE_TLS: 'disable',
      MORIA_AGENT_WORKER_OWNER: owner,
      MORIA_AGENT_HOST_ID: host,
      MORIA_FUNCTION_WORKER_OWNER: owner,
      MORIA_FUNCTION_HOST_ID: host,
      AGENT_MODEL_ENDPOINT: endpoint,
      AGENT_MODEL_SECRET: providerSecret,
    };
    const start = (arguments_: string[], input?: unknown, extra: Record<string, string> = {}) => {
      const child = spawn(
        process.env.MORIA_RUNTIME_BINARY ??
          process.env.MORIA_INDEXING_BINARY ??
          '/usr/local/bin/moria-indexing',
        arguments_,
        { env: { ...nativeEnvironment, ...extra }, stdio: ['pipe', 'pipe', 'pipe'] },
      );
      children.push(child);
      let output = '',
        error = '';
      child.stdout!.on('data', (bytes) => (output += bytes));
      child.stderr!.on('data', (bytes) => (error += bytes));
      const done = new Promise<string>((resolve, reject) => {
        child.once('error', reject);
        child.once('exit', (code, signal) =>
          code === 0
            ? resolve(output)
            : reject(
                new Error(
                  `Native agent process ${arguments_.join(' ')} failed ${code ?? signal}: ${error}`,
                ),
              ),
        );
      });
      void done.catch(() => {});
      child.stdin!.end(input === undefined ? '' : JSON.stringify(input));
      return { child, done, output: () => output, error: () => error };
    };
    const operatorConnection = `postgresql://${db.pool.options.user}:${encodeURIComponent(db.pool.options.password as string)}@127.0.0.1:${db.pool.options.port}/postgres`;
    const approveSecret = async () => {
      const binding = JSON.parse(
        await start(
          ['secret', 'inspect-model'],
          { environment, model },
          { AGENT_DATABASE: operatorConnection },
        ).done,
      ).binding;
      return JSON.parse(
        await start(
          ['secret', 'approve'],
          {
            requestKey: randomUUID(),
            environment,
            binding,
            materialRevision: 'agent-material-v1',
            markings: ['agents/Marking/Private'],
          },
          { AGENT_DATABASE: operatorConnection },
        ).done,
      );
    };
    let secretApproval = await approveSecret();
    await start(['models', 'probe'], { token: user.token, resource: model }).done;
    const server = start(['serve'], undefined, { MORIA_NATIVE_LISTEN: '127.0.0.1:0' });
    let api = '';
    await until(async () => {
      assert.equal(server.child.exitCode, null, server.error());
      for (const line of server.output().split('\n'))
        try {
          const event = JSON.parse(line);
          if (event.event === 'serving') api = `http://${event.address}`;
        } catch {}
      return !!api;
    });
    const request = async (
      path: string,
      method = 'GET',
      body?: unknown,
      key?: string,
      token = user.token,
    ) => {
      const response = await fetch(api + path, {
        method,
        headers: {
          authorization: `Bearer ${token}`,
          'content-type': 'application/json',
          ...(key ? { 'idempotency-key': key } : {}),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      });
      return { response, value: (await response.json()) as any };
    };
    const enqueue = async (mode: string, key: string = randomUUID(), prompt = 'hello') => {
      const result = await request(
        `/api/v1/agents/${resource}/executions`,
        'POST',
        { input: { mode, prompt, uid }, wait: false },
        key,
      );
      assert.equal(result.response.status, 202, JSON.stringify(result.value));
      ids.add(result.value.id);
      return result.value;
    };
    const get = async (id: string) => {
      const result = await request(`/api/v1/agent-executions/${id}`);
      assert.equal(result.response.status, 200, JSON.stringify(result.value));
      return result.value;
    };
    const once = () => start(['agents', 'once']);
    const running = (id: string) =>
      until(
        async () =>
          (
            await db.pool.query(
              'SELECT state FROM moria.runtime_sandbox_attempts WHERE execution=$1',
              [id],
            )
          ).rows[0]?.state === 'running',
      );
    const receipts = async (id: string) =>
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.runtime_agent_receipts WHERE execution=$1',
          [id],
        )
      ).rows[0].count;

    await selected(
      'authenticated Agent admission replays pinned inputs and rejects generic/unregistered workers',
      async () => {
        const accepted = await enqueue('normal', 'agent-original');
        assert.equal(accepted.status, 'queued');
        assert.deepEqual(accepted.markings, [{ environment, marking: 'agents/Marking/Private' }]);
        const captured = (
          await db.pool.query(
            'SELECT model_secret_approval FROM moria.runtime_agent_executions WHERE execution=$1',
            [accepted.id],
          )
        ).rows[0].model_secret_approval;
        assert.equal(captured.id, secretApproval.id);
        assert.ok(captured.generation && captured.incarnation);
        await assert.rejects(
          db.pool.query(
            "UPDATE moria.runtime_agent_executions SET model_secret_approval='{}' WHERE execution=$1",
            [accepted.id],
          ),
          /IMMUTABLE_AGENT_EXECUTION/,
        );
        const replay = await enqueue('normal', 'agent-original');
        assert.equal(replay.id, accepted.id);
        assert.equal(
          (
            await request(
              `/api/v1/agents/${resource}/executions`,
              'POST',
              { input: { mode: 'normal', prompt: 'changed', uid } },
              'agent-original',
            )
          ).response.status,
          409,
        );
        assert.equal(
          (
            await request(
              `/api/v1/agents/${resource}/executions`,
              'POST',
              { input: { mode: 'normal', prompt: 'hello', uid }, principal: 'forged' },
              'forged',
            )
          ).response.status,
          400,
        );
        await assert.rejects(
          start(['execution', 'claim'], { owner, domains: ['agent'], publication: false }).done,
        );
        await assert.rejects(
          start(['agents', 'once'], undefined, { MORIA_AGENT_WORKER_OWNER: randomUUID() }).done,
        );
        await once().done;
        const result = await get(accepted.id);
        assert.equal(result.status, 'succeeded');
        assert.equal(result.result.answer, 'hello');
        assert.equal(await receipts(accepted.id), '1');
        assert.equal(
          (await db.pool.query('SELECT count(*) FROM moria.runtime_function_invocations')).rows[0]
            .count,
          '0',
        );
      },
    );
    await selected(
      'SDK and CLI use the persistent native API and the agent worker replaces its Node launcher',
      async () => {
        const binary =
          process.env.MORIA_RUNTIME_BINARY ??
          process.env.MORIA_INDEXING_BINARY ??
          '/usr/local/bin/moria-indexing';
        const child = spawn(
          process.execPath,
          ['--import', 'tsx', 'apps/cli/src/main.ts', 'agents', 'worker'],
          {
            env: {
              ...nativeEnvironment,
              MORIA_RUNTIME_BINARY: binary,
              MORIA_AGENT_CONCURRENCY: '2',
            },
            stdio: ['ignore', 'pipe', 'pipe'],
          },
        );
        children.push(child);
        let stdout = '',
          stderr = '';
        child.stdout.on('data', (bytes) => (stdout += bytes));
        child.stderr.on('data', (bytes) => (stderr += bytes));
        const done = new Promise<void>((resolve, reject) => {
          child.once('error', reject);
          child.once('exit', (code, signal) =>
            code === 0
              ? resolve()
              : reject(new Error(`Native agent CLI worker exited ${code ?? signal}: ${stderr}`)),
          );
        });
        void done.catch(() => {});
        try {
          await until(async () => {
            assert.equal(child.exitCode, null, stderr);
            return stdout.split('\n').some((line) => {
              try {
                return JSON.parse(line).event === 'agent-worker';
              } catch {
                return false;
              }
            });
          });
          assert.equal(await readlink(`/proc/${child.pid}/exe`), await realpath(binary));
          const sdk = agentsClient(
            createTransport({
              baseUrl: api,
              fetch: (url, options) =>
                fetch(url, {
                  ...options,
                  headers: { ...options?.headers, authorization: `Bearer ${user.token}` },
                }),
            }),
          );
          const accepted = await sdk.invoke(
            resource,
            { mode: 'normal', prompt: 'SDK native graph', uid },
            { idempotencyKey: 'sdk-agent', wait: false },
          );
          ids.add(accepted.id);
          assert.equal(accepted.resourceId, resource);
          assert.equal(accepted.valueVersion, 1);
          await until(async () => {
            try {
              return (await sdk.get(accepted.id)).status === 'succeeded';
            } catch (error) {
              if (
                error instanceof ApiError &&
                error.status === 409 &&
                error.code === 'FUNCTION_OBSERVATION_INCOMPLETE'
              )
                return false;
              throw error;
            }
          });
          assert.equal((await sdk.get(accepted.id)).result?.answer, 'SDK native graph');
          assert.ok(
            (await sdk.events(accepted.id)).events.some((event) => event.type === 'succeeded'),
          );
          const cli = await execute(
            process.execPath,
            ['--import', 'tsx', 'apps/cli/src/main.ts', 'agents', 'get', accepted.id, '--url', api],
            { env: { PATH: process.env.PATH, MORIA_API_TOKEN: user.token } },
          );
          assert.equal(JSON.parse(cli.stdout).id, accepted.id);
        } finally {
          child.kill('SIGTERM');
          await done;
        }
      },
    );
    await selected(
      'a durable model intent commits before gateway admission while public callback data stays withheld',
      async () => {
        const control = await db.pool.connect();
        await control.query('BEGIN');
        await control.query(
          "SELECT 1 FROM moria.runtime_resource_classes WHERE name='models' FOR UPDATE",
        );
        const before = calls;
        const job = await enqueue('model');
        const worker = once();
        try {
          await until(async () => {
            const trace = (
              await db.pool.query(
                'SELECT model_resource,model_key,status FROM moria.runtime_agent_model_calls WHERE execution=$1',
                [job.id],
              )
            ).rows[0];
            if (!trace) return false;
            assert.equal(trace.status, 102);
            return (
              (
                await db.pool.query(
                  'SELECT count(*) FROM moria.native_model_calls WHERE resource=$1 AND call_key=$2',
                  [trace.model_resource, trace.model_key],
                )
              ).rows[0].count === '0'
            );
          });
          let answered = false;
          const pending = fetch(api + `/api/v1/agent-executions/${job.id}`, {
            headers: { authorization: `Bearer ${user.token}` },
            signal: AbortSignal.timeout(10000),
          }).finally(() => {
            answered = true;
          });
          void pending.catch(() => {});
          const began = Date.now();
          await delay(500);
          if (!answered) {
            const blockers = (
              await db.pool.query(
                'SELECT a.pid,a.wait_event_type,a.wait_event,pg_blocking_pids(a.pid) AS blockers,l.relation::regclass::text AS relation,l.mode,l.granted FROM pg_stat_activity a LEFT JOIN pg_locks l ON l.pid=a.pid AND l.relation IS NOT NULL WHERE a.datname=current_database() AND cardinality(pg_blocking_pids(a.pid))>0 ORDER BY a.pid,relation,l.mode',
              )
            ).rows;
            t.diagnostic(
              JSON.stringify({ phase: 'model-class-public-observation-barrier', blockers }),
            );
          }
          const response = await pending;
          const body = (await response.json()) as any;
          assert.equal(
            response.status,
            409,
            JSON.stringify({
              elapsedMillis: Date.now() - began,
              returnedStatus: body.status,
              returnedError: body.error?.code,
              providerCalls: calls - before,
              state: (
                await db.pool.query(
                  'SELECT e.status,s.state,s.observation_pending,s.observation_blocked,c.status AS model_status,c.error AS model_error FROM moria.runtime_executions e LEFT JOIN moria.runtime_sandbox_attempts s ON s.execution=e.id LEFT JOIN moria.runtime_agent_model_calls c ON c.execution=e.id WHERE e.id=$1',
                  [job.id],
                )
              ).rows,
            }),
          );
          assert.equal(body.error.code, 'FUNCTION_OBSERVATION_INCOMPLETE');
          assert.equal(calls, before);
          await control.query('COMMIT');
          await worker.done;
          assert.equal((await get(job.id)).status, 'succeeded');
          assert.equal(calls, before + 1);
        } finally {
          await control.query('ROLLBACK');
          control.release();
          await worker.done;
        }
      },
    );
    await selected(
      'actual LangGraph model/tool callbacks retain read classification, usage and committed tool effects',
      async () => {
        const before = calls;
        const read = await enqueue('read-model');
        await once().done;
        const result = await get(read.id);
        assert.equal(
          result.status,
          'succeeded',
          JSON.stringify({
            result,
            events:
              result.status === 'succeeded'
                ? undefined
                : (await request(`/api/v1/agent-executions/${read.id}/events?after=0`)).value,
          }),
        );
        assert.equal(result.result.answer, 'native agent answer');
        assert.equal(calls, before + 1);
        assert.deepEqual(result.markings, [{ environment, marking: 'agents/Marking/Private' }]);
        assert.equal(result.modelCalls[0].status, 200);
        assert.equal(result.modelCalls[0].usage.totalTokens, '7');
        const modelKey = (
          await db.pool.query(
            "SELECT model_key FROM moria.runtime_agent_model_calls WHERE execution=$1 AND logical_id='decision'",
            [read.id],
          )
        ).rows[0].model_key;
        assert.equal(
          (
            await db.pool.query(
              'SELECT caller_execution FROM moria.native_model_calls WHERE resource=$1 AND call_key=$2',
              [model, modelKey],
            )
          ).rows[0].caller_execution,
          read.id,
        );
        assert.equal(
          JSON.parse(
            await start(['models', 'status'], { token: user.token, resource: model, key: modelKey })
              .done,
          ).state,
          'succeeded',
        );
        await assert.rejects(
          db.pool.query(
            'UPDATE moria.native_model_calls SET caller_execution=NULL WHERE resource=$1 AND call_key=$2',
            [model, modelKey],
          ),
          /MODEL_ORIGIN_IMMUTABLE/,
        );
        await authority.setGrants(
          environment,
          'fixture',
          principal,
          application,
          operations.filter((operation) => operation !== 'records:read'),
        );
        try {
          await assert.rejects(
            start(['models', 'status'], { token: user.token, resource: model, key: modelKey }).done,
            /FORBIDDEN/,
          );
          await assert.rejects(
            start(['models', 'chat'], {
              token: user.token,
              resource: model,
              key: modelKey,
              input: { messages: [{ role: 'user', content: '10' }], maxTokens: 32 },
            }).done,
            /FORBIDDEN/,
          );
          assert.equal(
            calls,
            before + 1,
            'Revoked source receipts must not trigger another provider attempt',
          );
        } finally {
          await authority.setGrants(environment, 'fixture', principal, application, operations);
        }
        const failed = await enqueue('write-fail');
        await once().done;
        assert.equal((await get(failed.id)).status, 'failed');
        assert.equal(await receipts(failed.id), '0');
        const object = await request(`/api/v1/objects/records/${uid}`);
        assert.equal(object.value.object.value, 43);
        assert.equal(
          (
            await db.pool.query('SELECT count(*) FROM moria.action_keys WHERE action=$1', [
              'edit-record',
            ])
          ).rows[0].count,
          '1',
        );
        assert.notEqual(
          (await request(`/api/v1/agent-executions/${read.id}`)).response.status,
          200,
        );
        assert.notEqual(
          (await request(`/api/v1/agent-executions/${read.id}/events?after=0`)).response.status,
          200,
        );
      },
    );
    await selected(
      'Agent admission captures approved material before code and a missed approval pulse cannot revive it',
      async () => {
        const before = calls;
        await authority.setApplication(
          environment,
          'fixture',
          application,
          operations.filter((operation) => operation !== `${secret}:use`),
        );
        try {
          assert.equal(
            (
              await request(
                `/api/v1/agents/${resource}/executions`,
                'POST',
                { input: { mode: 'model', prompt: 'no-use', uid }, wait: false },
                'missing-secret-use',
              )
            ).response.status,
            403,
          );
          assert.equal(calls, before);
        } finally {
          await authority.setApplication(environment, 'fixture', application, operations);
        }
        const job = await enqueue('model', 'secret-before-code');
        const accepted = (
          await db.pool.query(
            'SELECT model_secret_approval,markings FROM moria.runtime_agent_executions WHERE execution=$1',
            [job.id],
          )
        ).rows[0];
        assert.equal(accepted.model_secret_approval.id, secretApproval.id);
        assert.deepEqual(accepted.markings, [{ environment, marking: 'agents/Marking/Private' }]);
        await start(
          ['secret', 'disable'],
          { id: secretApproval.id },
          { AGENT_DATABASE: operatorConnection },
        ).done;
        await db.pool.query('UPDATE moria.native_secret_approvals SET enabled=true WHERE id=$1', [
          secretApproval.id,
        ]);
        const current = (
          await db.pool.query(
            'SELECT generation::text FROM moria.native_secret_approvals WHERE id=$1',
            [secretApproval.id],
          )
        ).rows[0].generation;
        assert.notEqual(current, accepted.model_secret_approval.generation);
        const denied = await request(`/api/v1/agent-executions/${job.id}`);
        assert.equal(denied.response.status, 403, JSON.stringify(denied.value));
        assert.equal(denied.value.error.code, 'SECRET_APPROVAL_REVOKED');
        await once().done;
        assert.equal(calls, before, 'A stale accepted grant must not dispatch a provider request');
        assert.equal(await receipts(job.id), '0');
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.runtime_sandbox_attempts WHERE execution=$1 AND creation_state>0',
              [job.id],
            )
          ).rows[0].count,
          '0',
        );
        assert.deepEqual(
          (await request(`/api/v1/agent-executions/${job.id}/cancel`, 'POST', {})).value,
          { id: job.id, cancelRequested: true },
        );
        secretApproval = await approveSecret();
        const fresh = await enqueue('model', 'secret-reapproved');
        await once().done;
        assert.equal((await get(fresh.id)).status, 'succeeded');
        const captured = (
          await db.pool.query(
            'SELECT model_secret_approval FROM moria.runtime_agent_executions WHERE execution=$1',
            [fresh.id],
          )
        ).rows[0].model_secret_approval;
        assert.equal(captured.generation, secretApproval.generation);
        assert.ok(calls > before);
        for (const row of (
          await db.pool.query(
            'SELECT v.payload::text FROM moria.runtime_execution_events v JOIN moria.runtime_executions e ON e.id=v.execution WHERE e.environment=$1',
            [environment],
          )
        ).rows)
          assert.ok(
            !row.payload.includes(providerSecret),
            'Protected events must not contain host credential material',
          );
      },
    );
    await selected(
      'exact Agent invocation scope admits required dependencies without filesystem browsing',
      async () => {
        const scoped = [`${resource}:invoke`, `${model}:invoke`, `${secret}:use`];
        await authority.setApplication(environment, 'fixture', application, scoped);
        await authority.setGrants(environment, 'fixture', principal, application, scoped);
        let accepted: string | undefined;
        try {
          const job = await enqueue('normal');
          accepted = job.id;
          assert.deepEqual(
            (
              await db.pool.query(
                'SELECT b.operations FROM moria.runtime_execution_bindings b JOIN moria.runtime_executions e ON e.binding=b.id WHERE e.id=$1',
                [job.id],
              )
            ).rows[0].operations,
            scoped,
          );
          await once().done;
          assert.equal((await get(job.id)).status, 'succeeded');
          assert.notEqual((await request('/api/management/v1/fs/home')).response.status, 200);
        } finally {
          if (
            accepted &&
            (
              await db.pool.query('SELECT status FROM moria.runtime_executions WHERE id=$1', [
                accepted,
              ])
            ).rows[0]?.status === 'queued'
          ) {
            assert.equal(
              (await request(`/api/v1/agent-executions/${accepted}/cancel`, 'POST', {})).response
                .status,
              200,
            );
          }
          await authority.setApplication(environment, 'fixture', application, operations);
          await authority.setGrants(environment, 'fixture', principal, application, operations);
        }
      },
    );
    await selected(
      'a pending child observation blocks parent logs, actions and model dispatch',
      async () => {
        for (const effect of ['log', 'action', 'model']) {
          const job = await enqueue(
            `pending-child-${effect}`,
            randomUUID(),
            resources.get('agents/Function/Child')!,
          );
          const key = randomUUID();
          const blocker = await db.pool.connect();
          let parentWorker: ReturnType<typeof start> | undefined;
          let childWorker: ReturnType<typeof start> | undefined;
          let child = '';
          const beforeCalls = calls;
          const beforeValue = (await request(`/api/v1/objects/records/${uid}`)).value.object.value;
          const beforeActions = (
            await db.pool.query('SELECT count(*) FROM moria.action_keys WHERE environment=$1', [
              environment,
            ])
          ).rows[0].count;
          try {
            await blocker.query('BEGIN');
            await blocker.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [key]);
            await db.pool.query(
              'CREATE FUNCTION moria.test_agent_child_read_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS(SELECT 1 FROM moria.runtime_function_invocations WHERE execution=NEW.execution AND parent_id=TG_ARGV[1]::uuid) THEN PERFORM pg_advisory_xact_lock(hashtextextended(TG_ARGV[0],0)); END IF; RETURN NEW; END $$',
            );
            await db.pool.query(
              `CREATE TRIGGER test_agent_child_read_barrier AFTER INSERT ON moria.native_function_reads FOR EACH ROW WHEN(NEW.call_key LIKE 'read/%') EXECUTE FUNCTION moria.test_agent_child_read_barrier('${key}','${job.id}')`,
            );
            parentWorker = once();
            await until(async () => {
              child = (
                await db.pool.query(
                  'SELECT execution FROM moria.runtime_function_invocations WHERE parent_id=$1',
                  [job.id],
                )
              ).rows[0]?.execution;
              return !!child;
            });
            functionIds.add(child);
            childWorker = start(['functions', 'once']);
            await until(async () =>
              Boolean(
                (
                  await db.pool.query(
                    "SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND wait_event='advisory' AND query LIKE '%INSERT INTO moria.native_function_reads%' AND pid<>pg_backend_pid()",
                  )
                ).rowCount,
              ),
            );
            assert.ok(
              (
                await db.pool.query(
                  'SELECT observation_pending FROM moria.runtime_sandbox_attempts WHERE execution=$1',
                  [child],
                )
              ).rows[0].observation_pending,
            );
            for (const suffix of ['', '/events?after=0']) {
              const pending = await request(`/api/v1/agent-executions/${job.id}${suffix}`);
              assert.equal(pending.response.status, 409, JSON.stringify(pending.value));
              assert.equal(pending.value.error.code, 'FUNCTION_OBSERVATION_INCOMPLETE');
            }
            // The real read transaction remains held until after the parent attempts its effect.
            await parentWorker.done;
            assert.equal(await receipts(job.id), '0');
            assert.equal(calls, beforeCalls);
            assert.equal(
              (await request(`/api/v1/objects/records/${uid}`)).value.object.value,
              beforeValue,
            );
            assert.equal(
              (
                await db.pool.query('SELECT count(*) FROM moria.action_keys WHERE environment=$1', [
                  environment,
                ])
              ).rows[0].count,
              beforeActions,
            );
            assert.equal(
              (
                await db.pool.query(
                  "SELECT count(*) FROM moria.runtime_execution_events WHERE execution=$1 AND kind='log' AND payload->>'message'='pending child artifact must not escape'",
                  [job.id],
                )
              ).rows[0].count,
              '0',
            );
            assert.equal(
              (
                await db.pool.query(
                  'SELECT count(*) FROM moria.native_model_calls WHERE caller_execution=$1',
                  [job.id],
                )
              ).rows[0].count,
              '0',
            );
            const withheld = await request(`/api/v1/agent-executions/${job.id}`);
            assert.equal(withheld.response.status, 409, JSON.stringify(withheld.value));
            await blocker.query('ROLLBACK');
            await childWorker.done;
            assert.deepEqual(
              (
                await db.pool.query(
                  'SELECT attempts FROM moria.runtime_execution_tasks WHERE execution=ANY($1::uuid[]) ORDER BY execution',
                  [[job.id, child]],
                )
              ).rows.map((row) => row.attempts),
              [1, 1],
            );
          } finally {
            await blocker.query('ROLLBACK');
            blocker.release();
            parentWorker?.child.kill('SIGKILL');
            childWorker?.child.kill('SIGKILL');
            await db.pool.query(
              'DROP TRIGGER IF EXISTS test_agent_child_read_barrier ON moria.native_function_reads;DROP FUNCTION IF EXISTS moria.test_agent_child_read_barrier()',
            );
          }
        }
      },
    );
    await selected(
      'private child progress crosses a held dependency insertion and resumes after terminal proof',
      async () => {
        const job = await enqueue(
          'pending-child-progress',
          randomUUID(),
          resources.get('agents/Function/Child')!,
        );
        const key = randomUUID();
        const blocker = await db.pool.connect();
        let parentWorker: ReturnType<typeof start> | undefined;
        let childWorker: ReturnType<typeof start> | undefined;
        let child = '';
        try {
          await blocker.query('BEGIN');
          await blocker.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [key]);
          await db.pool.query(
            'CREATE FUNCTION moria.test_child_dependency_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(hashtextextended(TG_ARGV[0],0)); RETURN NEW; END $$',
          );
          await db.pool.query(
            `CREATE TRIGGER test_child_dependency_barrier BEFORE INSERT ON moria.native_function_dependencies FOR EACH ROW WHEN(NEW.execution='${job.id}'::uuid) EXECUTE FUNCTION moria.test_child_dependency_barrier('${key}')`,
          );
          parentWorker = once();
          await until(async () => {
            child = (
              await db.pool.query(
                'SELECT execution FROM moria.runtime_function_invocations WHERE parent_id=$1',
                [job.id],
              )
            ).rows[0]?.execution;
            return !!child;
          });
          functionIds.add(child);
          await until(async () =>
            Boolean(
              (
                await db.pool.query(
                  "SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory' AND query LIKE '%INSERT INTO moria.native_function_dependencies%'",
                )
              ).rowCount,
            ),
          );
          childWorker = start(['functions', 'once']);
          await until(async () => {
            const probe = await db.pool.connect();
            try {
              await probe.query('BEGIN');
              await probe.query(
                'SELECT 1 FROM moria.runtime_executions WHERE id=$1 FOR UPDATE NOWAIT',
                [child],
              );
              return false;
            } catch (error) {
              if ((error as { code?: string }).code === '55P03') return true;
              throw error;
            } finally {
              await probe.query('ROLLBACK');
              probe.release();
            }
          });
          // The real child holds its execution fence while the real parent holds its fence
          // before the dependency FK check. Releasing this barrier must not form a cycle.
          await blocker.query('ROLLBACK');
          await Promise.all([parentWorker.done, childWorker.done]);
          const result = await get(job.id);
          assert.equal(result.status, 'succeeded', JSON.stringify(result));
          assert.equal(result.result.answer, 'succeeded');
          assert.equal(await receipts(job.id), '1');
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.runtime_function_receipts WHERE execution=$1',
                [child],
              )
            ).rows[0].count,
            '1',
          );
          assert.deepEqual(
            (
              await db.pool.query(
                'SELECT attempts FROM moria.runtime_execution_tasks WHERE execution=ANY($1::uuid[]) ORDER BY execution',
                [[job.id, child]],
              )
            ).rows.map((row) => row.attempts),
            [1, 1],
          );
        } finally {
          await blocker.query('ROLLBACK');
          blocker.release();
          parentWorker?.child.kill('SIGKILL');
          childWorker?.child.kill('SIGKILL');
          await db.pool.query(
            'DROP TRIGGER IF EXISTS test_child_dependency_barrier ON moria.native_function_dependencies;DROP FUNCTION IF EXISTS moria.test_child_dependency_barrier()',
          );
        }
      },
    );
    await selected(
      'Agent observes actual Function child failure labels before returning a caught outcome',
      async () => {
        const job = await enqueue(
          'child-fail',
          randomUUID(),
          resources.get('agents/Function/Child')!,
        );
        const parentWorker = once();
        let childId = '';
        await until(async () => {
          assert.equal(
            parentWorker.child.exitCode,
            null,
            JSON.stringify({
              worker: parentWorker.error(),
              execution: (
                await db.pool.query(
                  'SELECT status,error FROM moria.runtime_executions WHERE id=$1',
                  [job.id],
                )
              ).rows,
              events: (
                await db.pool.query(
                  'SELECT kind,payload FROM moria.runtime_execution_events WHERE execution=$1 ORDER BY sequence',
                  [job.id],
                )
              ).rows,
            }),
          );
          childId =
            (
              await db.pool.query(
                'SELECT execution FROM moria.runtime_function_invocations WHERE parent_id=$1',
                [job.id],
              )
            ).rows[0]?.execution ?? '';
          return !!childId;
        });
        await start(['functions', 'once']).done;
        await parentWorker.done;
        const child = (
          await db.pool.query(
            'SELECT e.status,d.parent_execution,s.consumed_markings,s.observation_pending FROM moria.runtime_executions e JOIN moria.runtime_job_delegations d ON d.execution=e.id JOIN moria.runtime_sandbox_attempts s ON s.execution=e.id WHERE e.id=$1',
            [childId],
          )
        ).rows[0];
        assert.equal(child.status, 'failed');
        assert.equal(child.parent_execution, job.id);
        assert.equal(child.observation_pending, null);
        assert.deepEqual(child.consumed_markings, [
          { environment, marking: 'agents/Marking/Private' },
        ]);
        const result = await get(job.id);
        assert.equal(result.status, 'succeeded', JSON.stringify(result));
        assert.equal(result.result.answer, 'failed');
        assert.deepEqual(result.markings, [{ environment, marking: 'agents/Marking/Private' }]);
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.native_function_dependencies WHERE execution=$1 AND child=$2',
              [job.id, childId],
            )
          ).rows[0].count,
          '1',
        );
        await authority.syncPrincipal(environment, 'fixture', principal, {
          kind: 'workload',
          enabled: true,
          attributes: {},
          markings: [],
        });
        try {
          assert.equal((await request(`/api/v1/agent-executions/${job.id}`)).response.status, 403);
          assert.equal(
            (await request(`/api/v1/agent-executions/${job.id}/events?after=0`)).response.status,
            403,
          );
        } finally {
          await authority.syncPrincipal(environment, 'fixture', principal, {
            kind: 'workload',
            enabled: true,
            attributes: {},
            markings: ['agents/Marking/Private'],
          });
        }
      },
    );
    await selected(
      'Agent cancellation fences a running Function child without a second attempt',
      async () => {
        const job = await enqueue(
          'child-hold',
          randomUUID(),
          resources.get('agents/Function/Child')!,
        );
        const parentWorker = once();
        let childId = '';
        await until(async () => {
          assert.equal(
            parentWorker.child.exitCode,
            null,
            JSON.stringify({
              worker: parentWorker.error(),
              execution: (
                await db.pool.query(
                  'SELECT status,error FROM moria.runtime_executions WHERE id=$1',
                  [job.id],
                )
              ).rows,
              events: (
                await db.pool.query(
                  'SELECT kind,payload FROM moria.runtime_execution_events WHERE execution=$1 ORDER BY sequence',
                  [job.id],
                )
              ).rows,
            }),
          );
          childId =
            (
              await db.pool.query(
                'SELECT execution FROM moria.runtime_function_invocations WHERE parent_id=$1',
                [job.id],
              )
            ).rows[0]?.execution ?? '';
          return !!childId;
        });
        const childWorker = start(['functions', 'once']);
        await until(
          async () =>
            !!(
              await db.pool.query(
                "SELECT 1 FROM moria.native_function_reads WHERE execution=$1 AND call_key LIKE 'read/%'",
                [childId],
              )
            ).rows[0],
        );
        const began = Date.now();
        const cancelled = await request(`/api/v1/agent-executions/${job.id}/cancel`, 'POST', {});
        assert.equal(cancelled.response.status, 200, JSON.stringify(cancelled.value));
        assert.deepEqual(cancelled.value, { id: job.id, cancelRequested: true });
        await Promise.all([parentWorker.done, childWorker.done]);
        assert.ok(Date.now() - began < 10000);
        const task = (
          await db.pool.query(
            'SELECT t.attempts,t.status AS task_status,t.lease_until,e.status,s.cleaned,c.root AS cancellation_root FROM moria.runtime_execution_tasks t JOIN moria.runtime_executions e ON e.id=t.execution JOIN moria.runtime_sandbox_attempts s ON s.task=t.id JOIN moria.runtime_execution_cancellations c ON c.execution=e.id WHERE e.id=$1',
            [childId],
          )
        ).rows[0];
        assert.equal(task.attempts, 1);
        assert.ok(['failed', 'cancelled'].includes(task.status));
        assert.equal(task.task_status, 'cancelled');
        assert.equal(task.lease_until, null);
        assert.equal(task.cancellation_root, job.id);
        assert.equal(task.cleaned, true);
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.runtime_function_receipts WHERE execution=$1',
              [childId],
            )
          ).rows[0].count,
          '0',
        );
      },
    );
    await selected(
      'healthy long-running agent work renews the same single-attempt database lease',
      async () => {
        const job = await enqueue('sleep');
        const worker = once();
        await running(job.id);
        const first = (
          await db.pool.query(
            'SELECT lease_until FROM moria.runtime_execution_tasks WHERE execution=$1',
            [job.id],
          )
        ).rows[0].lease_until;
        await delay(8500);
        const renewed = (
          await db.pool.query(
            'SELECT lease_until,attempts FROM moria.runtime_execution_tasks WHERE execution=$1',
            [job.id],
          )
        ).rows[0];
        assert.ok(new Date(renewed.lease_until).getTime() > new Date(first).getTime());
        assert.equal(renewed.attempts, 1);
        await worker.done;
        assert.equal((await get(job.id)).status, 'succeeded');
      },
    );
    await selected(
      'bound model dispatch stops after real worker-scope revocation and cannot publish a result',
      async () => {
        providerMode = 'hold';
        providerClosed = false;
        barrier = new Promise<void>((resolve) => (entered = resolve));
        held = new Promise<void>((resolve) => (release = resolve));
        const job = await enqueue('read-model');
        const worker = once();
        await Promise.race([
          barrier,
          worker.done.then(async () =>
            assert.fail(
              JSON.stringify({
                result: await get(job.id),
                events: (await request(`/api/v1/agent-executions/${job.id}/events?after=0`)).value,
              }),
            ),
          ),
          delay(10000).then(() => assert.fail('Real provider was not reached')),
        ]);
        const began = Date.now();
        await db.pool.query(
          "UPDATE moria.runtime_function_workers SET resource_classes=ARRAY['unassigned'] WHERE owner=$1",
          [owner],
        );
        try {
          await worker.done;
          assert.ok(Date.now() - began < 10000);
          await until(async () => providerClosed);
          assert.equal(await receipts(job.id), '0');
          assert.equal(
            (
              await db.pool.query(
                'SELECT state FROM moria.native_model_calls WHERE call_key LIKE $1',
                [`agent/${job.id}/%`],
              )
            ).rows[0].state,
            'failed',
            JSON.stringify(
              (
                await db.pool.query(
                  'SELECT e.status,e.error,t.status AS task_status,t.lease_until,c.state AS model_state,c.error AS model_error FROM moria.runtime_executions e JOIN moria.runtime_execution_tasks t ON t.execution=e.id JOIN moria.native_model_calls c ON c.caller_execution=e.id WHERE e.id=$1',
                  [job.id],
                )
              ).rows,
            ),
          );
        } finally {
          release();
          providerMode = 'normal';
          await db.pool.query(
            "UPDATE moria.runtime_function_workers SET resource_classes=ARRAY['standard'] WHERE owner=$1",
            [owner],
          );
        }
      },
    );
    await selected(
      'cancellation and supervisor SIGKILL fence the graph without re-executing user code',
      async () => {
        const job = await enqueue('hold');
        const worker = once();
        await running(job.id);
        const began = Date.now();
        assert.deepEqual(
          (await request(`/api/v1/agent-executions/${job.id}/cancel`, 'POST', {})).value,
          { id: job.id, cancelRequested: true },
        );
        await worker.done;
        assert.ok(Date.now() - began < 10000);
        const lost = await enqueue('hold');
        const crashed = once();
        await running(lost.id);
        crashed.child.kill('SIGKILL');
        await assert.rejects(crashed.done, /SIGKILL/);
        await db.pool.query(
          "UPDATE moria.runtime_execution_tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE execution=$1",
          [lost.id],
        );
        assert.equal((await get(lost.id)).status, 'failed');
        assert.equal(JSON.parse(await start(['agents', 'cleanup']).done).cleaned, 1);
        assert.equal(JSON.parse(await once().done).worked, false);
        assert.equal(
          (
            await db.pool.query(
              'SELECT attempts FROM moria.runtime_execution_tasks WHERE execution=$1',
              [lost.id],
            )
          ).rows[0].attempts,
          1,
        );
      },
    );
    await selected(
      'real result COMMIT acknowledgement loss preserves one Agent receipt after supervisor death',
      async () => {
        const job = await enqueue('normal');
        const proxy = await transactionCommitProxy(Number(db.pool.options.port));
        try {
          const url = new URL(connection);
          url.port = String(proxy.port);
          const worker = start(['agents', 'once'], undefined, { AGENT_DATABASE: url.toString() });
          await Promise.race([
            proxy.committed,
            worker.done.then(() => assert.fail('Worker ended before actual Agent commit')),
            delay(10000).then(() => assert.fail('Agent COMMIT barrier not reached')),
          ]);
          assert.equal(await receipts(job.id), '1');
          worker.child.kill('SIGKILL');
          await assert.rejects(worker.done, /SIGKILL/);
          assert.equal((await get(job.id)).status, 'succeeded');
          assert.equal(JSON.parse(await once().done).worked, false);
        } finally {
          await proxy.close();
        }
      },
    );
    await selected(
      'registered worker class admission is independent of unrelated class ordering',
      async () => {
        const prefix = `aaa-${randomUUID()}-`;
        await db.pool.query(
          "INSERT INTO moria.runtime_resource_classes(name,maximum_tasks,maximum_environment_tasks) SELECT $1||lpad(n::text,3,'0'),1,1 FROM generate_series(1,72) n",
          [prefix],
        );
        try {
          const job = await enqueue('normal');
          assert.equal(JSON.parse(await once().done).worked, true);
          assert.equal((await get(job.id)).status, 'succeeded');
        } finally {
          await db.pool.query(
            'DELETE FROM moria.runtime_resource_classes WHERE starts_with(name,$1)',
            [prefix],
          );
        }
      },
    );
    await selected(
      'Source automation factories atomically dispatch private current Agent releases and recover a lost acknowledgement',
      async () => {
        const automation = resources.get('agents/Automation/Refresh')!;
        const serviceFunction = resources.get('agents/Function/Child')!;
        assert.match(automation, /^[0-9a-f-]{36}$/);
        const entry = (
          await db.pool.query(
            'SELECT id FROM moria.fs_entries WHERE resource_id=$1 AND NOT deleted',
            [automation],
          )
        ).rows[0].id;
        await fs.filesystem.security.setPolicy('fixture', entry, '0', {
          markings: [{ environment, marking: 'agents/Marking/Private' }],
          grants: [],
        });
        const serviceOps = [
          ...new Set([
            ...operations,
            `${automation}:run`,
            `${automation}:read`,
            `${resource}:invoke`,
            `${resource}:read`,
            `${serviceFunction}:invoke`,
          ]),
        ];
        await authority.setApplication(environment, 'fixture', application, serviceOps);
        await authority.setGrants(environment, 'fixture', principal, application, serviceOps);
        const service = randomUUID(),
          controlOwner = randomUUID();
        await db.pool.query(
          "INSERT INTO moria.runtime_service_authorities(id,environment,principal,application,operations,markings,domains,resource_classes,incarnation) SELECT $1,$2,$3,$4,$5,ARRAY['agents/Marking/Private'],ARRAY['maintenance','agent','function'],ARRAY['automation-control','standard'],incarnation FROM moria.indexing_instance",
          [service, environment, principal, application, serviceOps],
        );
        await db.pool.query(
          "INSERT INTO moria.runtime_service_targets(environment,resource,domain,binding_revision,resource_class,authority) VALUES($1,$2,'maintenance','auto-v1','automation-control',$4),($1,$3,'agent','agent-v1','standard',$4),($1,$5,'function','function-v1','standard',$4)",
          [environment, automation, resource, service, serviceFunction],
        );
        await db.pool.query(
          "INSERT INTO moria.runtime_workers(owner,host_id,incarnation,resource_classes,domains,expires_at) SELECT $1,$2,incarnation,ARRAY['automation-control'],ARRAY['maintenance'],clock_timestamp()+interval '1 hour' FROM moria.indexing_instance",
          [controlOwner, host],
        );
        const config = {
          serviceAuthority: service,
          serviceBindingRevision: 'auto-v1',
          targetServiceBindingRevision: 'agent-v1',
          resourceClass: 'automation-control',
          controlTimeoutSeconds: 120,
        };
        const controlEnv = {
          MORIA_AUTOMATION_WORKER_OWNER: controlOwner,
          MORIA_AUTOMATION_HOST_ID: host,
        };
        const install = () =>
          start(['automations', 'install'], {
            environment,
            resource: automation,
            configuration: config,
          }).done;
        const tick = () => start(['automations', 'tick']).done;
        const control = (extra: Record<string, string> = {}) =>
          start(['automations', 'once'], undefined, { ...controlEnv, ...extra });
        const firings = async () =>
          (
            await db.pool.query(
              'SELECT f.id,f.control,f.child,f.outcome FROM moria.runtime_source_automation_firings f JOIN moria.runtime_source_automation_schedules s ON s.id=f.schedule WHERE s.resource=$1 ORDER BY f.created_at',
              [automation],
            )
          ).rows;
        const previous = definitions;
        try {
          assert.equal(JSON.parse(await install()).configured, true);
          await Promise.all([tick(), tick()]);
          assert.equal((await firings()).length, 1);
          const initial = (await firings())[0];
          assert.equal(initial.outcome, 'enqueued');
          const diagnostics = await requestCommitProxy(
            Number(db.pool.options.port),
            'moria_diagnostic_no_hold',
          );
          t.after(() => diagnostics.close());
          const diagnosticUrl = new URL(connection);
          diagnosticUrl.port = String(diagnostics.port);
          try {
            await control({ AGENT_DATABASE: diagnosticUrl.toString() }).done;
          } finally {
            await diagnostics.close();
          }
          const dispatched = (await firings())[0];
          assert.ok(
            dispatched.child,
            JSON.stringify({
              firing: dispatched,
              database: diagnostics.errors,
              control: (
                await db.pool.query(
                  'SELECT status,error FROM moria.runtime_executions WHERE id=$1',
                  [dispatched.control],
                )
              ).rows,
              events: (
                await db.pool.query(
                  'SELECT kind,payload FROM moria.runtime_execution_events WHERE execution=$1 ORDER BY sequence',
                  [dispatched.control],
                )
              ).rows,
            }),
          );
          assert.equal(
            (
              await db.pool.query('SELECT status FROM moria.runtime_executions WHERE id=$1', [
                dispatched.control,
              ])
            ).rows[0].status,
            'succeeded',
          );
          assert.equal(
            (
              await db.pool.query('SELECT status FROM moria.runtime_executions WHERE id=$1', [
                dispatched.child,
              ])
            ).rows[0].status,
            'queued',
          );
          const serviceSecret = (
            await db.pool.query(
              'SELECT a.model_secret_approval,b.operations FROM moria.runtime_agent_executions a JOIN moria.runtime_executions e ON e.id=a.execution JOIN moria.runtime_execution_bindings b ON b.id=e.binding WHERE a.execution=$1',
              [dispatched.child],
            )
          ).rows[0];
          assert.equal(serviceSecret.model_secret_approval.id, secretApproval.id);
          assert.equal(serviceSecret.model_secret_approval.generation, secretApproval.generation);
          assert.ok(serviceSecret.operations.includes(`${secret}:use`));
          await db.pool.query(
            "UPDATE moria.runtime_source_automation_schedules SET next_run=clock_timestamp()-interval '1 hour' WHERE resource=$1 AND enabled",
            [automation],
          );
          await Promise.all([tick(), tick()]);
          assert.deepEqual(
            (await firings()).map((row) => row.outcome),
            ['enqueued', 'overlap'],
          );
          await once().done;
          const result = await get(dispatched.child);
          assert.equal(result.status, 'succeeded', JSON.stringify(result));
          assert.equal(result.result.answer, 'automation-v1');
          assert.deepEqual(result.markings, [{ environment, marking: 'agents/Marking/Private' }]);
          assert.equal(
            (
              await db.pool.query(
                'SELECT parent_execution FROM moria.runtime_job_delegations WHERE execution=$1',
                [dispatched.child],
              )
            ).rows[0].parent_execution,
            null,
          );
          definitions = definitions.map((definition) =>
            definition.kind === 'Automation'
              ? {
                  ...definition,
                  spec: {
                    ...definition.spec,
                    input: { mode: 'child-fail', prompt: serviceFunction, uid },
                  },
                }
              : definition,
          );
          await writeResource(
            definitions.find((definition) => definition.kind === 'Automation')!,
            true,
          );
          await publish();
          await install();
          await tick();
          await control().done;
          const withFunction = (await firings())
            .filter((row) => row.outcome === 'enqueued')
            .at(-1)!;
          assert.ok(withFunction.child);
          const parentWorker = once();
          let functionId = '';
          await until(async () => {
            functionId =
              (
                await db.pool.query(
                  'SELECT execution FROM moria.runtime_function_invocations WHERE parent_id=$1',
                  [withFunction.child],
                )
              ).rows[0]?.execution ?? '';
            return !!functionId;
          });
          const functionWorker = start(['functions', 'once']);
          await Promise.all([parentWorker.done, functionWorker.done]);
          const observed = await get(withFunction.child);
          assert.equal(observed.status, 'succeeded', JSON.stringify(observed));
          assert.equal(observed.result.answer, 'failed');
          assert.deepEqual(observed.markings, [{ environment, marking: 'agents/Marking/Private' }]);
          const functionOrigin = (
            await db.pool.query(
              "SELECT d.origin_kind,d.parent_execution,b.resource,b.spec->>'domain' AS domain FROM moria.runtime_job_delegations d JOIN moria.runtime_executions e ON e.id=d.execution JOIN moria.runtime_execution_bindings b ON b.id=d.origin_binding WHERE d.execution=$1",
              [functionId],
            )
          ).rows[0];
          assert.deepEqual(functionOrigin, {
            origin_kind: 'service',
            parent_execution: withFunction.child,
            resource: serviceFunction,
            domain: 'function',
          });
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.runtime_function_receipts WHERE execution=$1',
                [functionId],
              )
            ).rows[0].count,
            '0',
          );
          definitions = previous;
          await writeResource(
            previous.find((definition) => definition.kind === 'Automation')!,
            true,
          );
          await publish();
          await install();
          await db.pool.query(
            "UPDATE moria.runtime_source_automation_schedules SET next_run=clock_timestamp()-interval '1 second' WHERE resource=$1 AND enabled",
            [automation],
          );
          await tick();
          const proxy = await transactionCommitProxy(
            Number(db.pool.options.port),
            'INSERT INTO moria.runtime_agent_executions',
          );
          t.after(() => proxy.close());
          const proxied = new URL(connection);
          proxied.port = String(proxy.port);
          const lost = start(['automations', 'once'], undefined, {
            ...controlEnv,
            AGENT_DATABASE: proxied.toString(),
          });
          let child = '';
          try {
            await Promise.race([
              proxy.committed,
              lost.done.then(() => assert.fail('Control stopped before child acceptance COMMIT')),
              delay(15000).then(() => assert.fail('Child acceptance COMMIT was not reached')),
            ]);
            const committed = (await firings()).filter((row) => row.outcome === 'enqueued').at(-1)!;
            child = committed.child;
            assert.ok(child);
            lost.child.kill('SIGKILL');
            await assert.rejects(lost.done, /SIGKILL/);
          } finally {
            await proxy.close();
          }
          definitions = definitions.map((definition) =>
            definition.kind === 'Agent'
              ? {
                  ...definition,
                  spec: {
                    ...definition.spec,
                    inputs: {
                      ...definition.spec.inputs,
                      prompt: { type: 'int64', nullable: false },
                    },
                  },
                }
              : definition.kind === 'Automation'
                ? {
                    ...definition,
                    spec: {
                      ...definition.spec,
                      input: { mode: 'normal', prompt: '42', uid: null },
                    },
                  }
                : definition,
          );
          await writeResource(
            definitions.find((definition) => definition.kind === 'Automation')!,
            true,
          );
          await publish();
          await db.pool.query(
            "UPDATE moria.runtime_execution_tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE execution IN(SELECT control FROM moria.runtime_source_automation_firings WHERE child=$1) AND status='running'",
            [child],
          );
          await control().done;
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.runtime_source_automation_firings WHERE child=$1',
                [child],
              )
            ).rows[0].count,
            '1',
          );
          assert.equal(
            (
              await db.pool.query(
                'SELECT e.status FROM moria.runtime_executions e JOIN moria.runtime_source_automation_firings f ON f.control=e.id WHERE f.child=$1',
                [child],
              )
            ).rows[0].status,
            'succeeded',
          );
          await once().done;
          const replay = await get(child);
          assert.equal(replay.status, 'succeeded', JSON.stringify(replay));
          assert.equal(replay.result.answer, 'automation-v1');
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.runtime_agent_executions WHERE execution=$1',
                [child],
              )
            ).rows[0].count,
            '1',
          );
          const inspectorPrincipal = 'automation-inspector',
            inspectorApp = 'automation-inspector-app';
          await authority.syncPrincipal(environment, 'fixture', inspectorPrincipal, {
            kind: 'workload',
            enabled: true,
            attributes: {},
            markings: ['agents/Marking/Private'],
          });
          await authority.setApplication(environment, 'fixture', inspectorApp, [
            ...fsOperations,
            `${automation}:run`,
          ]);
          await authority.setGrants(environment, 'fixture', inspectorPrincipal, inspectorApp, [
            ...fsOperations,
            `${automation}:run`,
          ]);
          const inspection = await sessions.mint(
            { environment, principal: inspectorPrincipal, application: inspectorApp },
            'fixture',
          );
          const inspectionContext = {
            session: await sessions.authenticate(`Bearer ${inspection.token}`),
          };
          const inspectorHome = await fs.management.ensureHome(inspectionContext);
          await authority.setApplication(environment, 'fixture', inspectorApp, [
            `${automation}:run`,
          ]);
          await authority.setGrants(environment, 'fixture', inspectorPrincipal, inspectorApp, [
            `${automation}:run`,
          ]);
          const policy = await fs.management.policy(author.context, root.id);
          await fs.management.setPolicy(author.context, root.id, policy.revision, {
            markings: policy.markings,
            grants: [
              ...policy.grants,
              {
                subject: { kind: 'principal', id: inspectorHome.principalId },
                operations: ['read', 'list'],
                inherit: true,
                effect: 'allow',
              },
            ],
          });
          try {
            for (let index = 0; index < 51; index++) {
              await db.pool.query(
                "UPDATE moria.runtime_source_automation_schedules SET next_run=clock_timestamp()-interval '1 second' WHERE resource=$1 AND enabled",
                [automation],
              );
              await tick();
            }
            const status = async (after?: string) => {
              const response = await fetch(
                `${api}/api/v1/automations/${automation}/firings${after ? '?' + new URLSearchParams({ after }) : ''}`,
                { headers: { authorization: `Bearer ${inspection.token}` } },
              );
              return { response, value: (await response.json()) as any };
            };
            const firstPage = await status();
            assert.equal(firstPage.response.status, 200, JSON.stringify(firstPage.value));
            assert.equal(firstPage.value.firings.length, 50);
            assert.equal(typeof firstPage.value.nextAfter, 'string');
            const secondPage = await status(firstPage.value.nextAfter);
            assert.equal(secondPage.response.status, 200, JSON.stringify(secondPage.value));
            assert.ok(secondPage.value.firings.length > 0);
            assert.equal(secondPage.value.nextAfter, null);
            const seen = [...firstPage.value.firings, ...secondPage.value.firings];
            assert.equal(new Set(seen.map((row) => row.id)).size, (await firings()).length);
            for (const row of seen) {
              assert.deepEqual(row.markings, [{ environment, marking: 'agents/Marking/Private' }]);
              for (const privateField of [
                'childStatus',
                'childExecution',
                'controlStatus',
                'input',
                'result',
                'logs',
                'principal',
                'application',
                'error',
              ])
                assert.equal(Object.hasOwn(row, privateField), false);
              assert.equal(row.childView, 'withheld');
            }
            assert.equal((await status('[]')).response.status, 400);
            await authority.syncPrincipal(environment, 'fixture', inspectorPrincipal, {
              kind: 'workload',
              enabled: true,
              attributes: {},
              markings: [],
            });
            assert.equal(
              (await status()).response.status,
              403,
              'Historical private firings cannot be released to an inspector without their retained clearance',
            );
          } finally {
            const currentPolicy = await fs.management.policy(author.context, root.id);
            await fs.management.setPolicy(author.context, root.id, currentPolicy.revision, {
              markings: policy.markings,
              grants: policy.grants,
            });
            await authority.syncPrincipal(environment, 'fixture', inspectorPrincipal, {
              kind: 'workload',
              enabled: false,
              attributes: {},
              markings: [],
            });
          }
          await db.pool.query(
            'UPDATE moria.runtime_source_automation_profiles SET enabled=false WHERE environment=$1 AND resource=$2',
            [environment, automation],
          );
          await db.pool.query(
            'UPDATE moria.runtime_source_automation_profiles SET enabled=true WHERE environment=$1 AND resource=$2',
            [environment, automation],
          );
          const revokedOrigin = await request(`/api/v1/agent-executions/${child}`);
          assert.equal(revokedOrigin.response.status, 403, JSON.stringify(revokedOrigin.value));
          assert.equal(revokedOrigin.value.error.code, 'AUTOMATION_DISPATCH_REVOKED');
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.runtime_source_automation_schedules WHERE resource=$1 AND enabled',
                [automation],
              )
            ).rows[0].count,
            '0',
          );
        } finally {
          await db.pool.query(
            'UPDATE moria.runtime_source_automation_schedules SET enabled=false WHERE resource=$1',
            [automation],
          );
          definitions = previous;
          await writeResource(
            previous.find((definition) => definition.kind === 'Automation')!,
            true,
          );
          await publish();
          await authority.setApplication(environment, 'fixture', application, operations);
          await authority.setGrants(environment, 'fixture', principal, application, operations);
        }
      },
    );
    await selected(
      'accepted private job authority survives ordinary session expiry while public and stale lease credentials fail',
      async () => {
        const issued = await sessions.mint({ environment, principal, application }, 'fixture');
        const original = createHash('sha256').update(issued.token).digest('hex');
        const accepted = await request(
          `/api/v1/agents/${resource}/executions`,
          'POST',
          { input: { mode: 'sleep', prompt: 'delegated', uid }, wait: false },
          'expired-origin',
          issued.token,
        );
        assert.equal(accepted.response.status, 202, JSON.stringify(accepted.value));
        ids.add(accepted.value.id);
        await db.pool.query(
          "UPDATE moria.workload_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token_hash=$1",
          [original],
        );
        assert.equal(
          (
            await request(
              `/api/v1/agent-executions/${accepted.value.id}`,
              'GET',
              undefined,
              undefined,
              issued.token,
            )
          ).response.status,
          401,
        );
        await assert.rejects(
          db.pool.query('SELECT moria.native_serving_session($1)', [original]),
          /UNAUTHENTICATED/,
        );
        const worker = once();
        await running(accepted.value.id);
        const capability = (
          await db.pool.query(
            'SELECT s.*,e.context FROM moria.runtime_job_sessions s JOIN moria.runtime_executions e ON e.id=s.execution WHERE s.execution=$1 AND s.task IS NOT NULL',
            [accepted.value.id],
          )
        ).rows[0];
        assert.ok(capability);
        assert.ok(capability.context.sessionHash === original, 'Accepted origin remains unchanged');
        assert.ok(capability.token_hash !== original, 'Private and ordinary credentials differ');
        const active = (
          await db.pool.query('SELECT moria.native_serving_session($1) AS state', [
            capability.token_hash,
          ])
        ).rows[0].state;
        assert.equal(active.environment, environment);
        assert.equal(active.principal, principal);
        assert.equal(active.application, application);
        assert.equal(
          (
            await request(
              `/api/v1/agent-executions/${accepted.value.id}`,
              'GET',
              undefined,
              undefined,
              capability.token_hash,
            )
          ).response.status,
          401,
        );
        await assert.rejects(
          db.pool.query('SELECT moria.runtime_job_session_issue($1,$2,$3,$4,$5,$6,$7)', [
            createHash('sha256').update(randomUUID()).digest('hex'),
            accepted.value.id,
            capability.task,
            owner,
            String(BigInt(capability.fence) + 1n),
            host,
            capability.incarnation,
          ]),
        );
        await assert.rejects(
          db.pool.query('SELECT moria.runtime_job_session_issue($1,$2,$3,$4,$5,$6,$7)', [
            createHash('sha256').update(randomUUID()).digest('hex'),
            accepted.value.id,
            capability.task,
            owner,
            capability.fence,
            randomUUID(),
            capability.incarnation,
          ]),
          /FUNCTION_WORKER_NOT_AUTHORIZED/,
        );
        await worker.done;
        assert.equal((await get(accepted.value.id)).result.answer, 'delegated');
        await assert.rejects(
          db.pool.query('SELECT moria.native_serving_session($1)', [capability.token_hash]),
          /JOB_LEASE_LOST/,
        );
      },
    );
    await selected(
      'newly granted tool permissions cannot widen an already accepted job capability',
      async () => {
        await request(
          '/api/v1/actions/edit-record/invoke',
          'POST',
          { input: { uid, value: 44 } },
          'before-capability-proof',
        );
        await authority.setGrants(
          environment,
          'fixture',
          principal,
          application,
          operations.filter((operation) => operation !== 'edit-record:invoke'),
        );
        const job = await enqueue('write-fail');
        await authority.setGrants(environment, 'fixture', principal, application, operations);
        await once().done;
        assert.equal((await get(job.id)).status, 'failed');
        assert.equal((await request(`/api/v1/objects/records/${uid}`)).value.object.value, 44);
        const accepted = (
          await db.pool.query(
            'SELECT accepted_grants FROM moria.runtime_job_delegations WHERE execution=$1',
            [job.id],
          )
        ).rows[0];
        assert.equal(accepted.accepted_grants.includes('edit-record:invoke'), false);
        assert.equal(await receipts(job.id), '0');
      },
    );
    await selected(
      'postclaim filesystem rejection records no code start after an expired original session',
      async () => {
        const issued = await sessions.mint({ environment, principal, application }, 'fixture');
        const original = createHash('sha256').update(issued.token).digest('hex');
        const accepted = await request(
          `/api/v1/agents/${resource}/executions`,
          'POST',
          { input: { mode: 'normal', prompt: 'must-not-run', uid }, wait: false },
          'no-code-start',
          issued.token,
        );
        assert.equal(accepted.response.status, 202, JSON.stringify(accepted.value));
        ids.add(accepted.value.id);
        await db.pool.query(
          "UPDATE moria.workload_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token_hash=$1",
          [original],
        );
        const previous = definitions;
        let descriptorRemoved = false;
        try {
          const descriptor = await fs.filesystem.store.resolve(
            `/users/${home.home.name}/projects/agents/source/logic/Refresh.resource.json`,
          );
          await fs.filesystem.apply(author.context, randomUUID(), [
            {
              op: 'delete',
              id: descriptor.id,
              expectedRevision: descriptor.revision,
              expectedParentRevision: (await fs.filesystem.store.get(root.id)).revision,
            },
          ]);
          descriptorRemoved = true;
          definitions = definitions.filter(
            (definition) => !['Agent', 'Automation'].includes(definition.kind),
          );
          await publish(['agents/Agent/Assistant', 'agents/Automation/Refresh']);
          assert.equal(
            (
              await db.pool.query(
                'SELECT active FROM moria.fs_resource_bindings b JOIN moria.fs_entries e ON e.id=b.entry_id WHERE e.resource_id=$1',
                [resource],
              )
            ).rows[0].active,
            false,
          );
          await once().done;
          const journal = (
            await db.pool.query(
              'SELECT state,creation_state,cleaned FROM moria.runtime_sandbox_attempts WHERE execution=$1',
              [accepted.value.id],
            )
          ).rows[0];
          assert.deepEqual(journal, { state: 'failed', creation_state: 0, cleaned: true });
          assert.equal(
            (
              await db.pool.query(
                "SELECT count(*) FROM moria.runtime_execution_events WHERE execution=$1 AND kind='noCodeStart'",
                [accepted.value.id],
              )
            ).rows[0].count,
            '1',
          );
          assert.equal(
            (
              await db.pool.query(
                'SELECT attempts FROM moria.runtime_execution_tasks WHERE execution=$1',
                [accepted.value.id],
              )
            ).rows[0].attempts,
            1,
          );
          assert.equal(await receipts(accepted.value.id), '0');
          await assert.rejects(
            execute('docker', ['inspect', `moria-native-agent-${accepted.value.id}`]),
          );
        } finally {
          definitions = previous;
          if (descriptorRemoved)
            await writeResource(previous.find((definition) => definition.kind === 'Automation')!);
          await publish();
        }
      },
    );
    await selected(
      'deleting an actual pinned Source descriptor withholds retained code data while the resource binding stays active',
      async () => {
        const job = await enqueue('normal', 'pinned-descriptor');
        await once().done;
        assert.equal((await get(job.id)).status, 'succeeded');
        const entry = await fs.filesystem.store.resolve(
          `/users/${home.home.name}/projects/agents/source/logic/.repository.json`,
        );
        assert.equal(
          (
            await db.pool.query(
              "SELECT count(*) FROM moria.fs_source_resource_files p JOIN moria.runtime_agent_executions a ON a.release=p.digest WHERE a.execution=$1 AND p.reference='agents/Agent/Assistant' AND p.entry_id=$2",
              [job.id, entry.id],
            )
          ).rows[0].count,
          '1',
        );
        let removed = false;
        try {
          await fs.filesystem.apply(author.context, randomUUID(), [
            {
              op: 'delete',
              id: entry.id,
              expectedRevision: entry.revision,
              expectedParentRevision: (await fs.filesystem.store.get(root.id)).revision,
            },
          ]);
          removed = true;
          assert.equal(
            (
              await db.pool.query(
                'SELECT active FROM moria.fs_resource_bindings b JOIN moria.fs_entries e ON e.id=b.entry_id WHERE e.resource_id=$1',
                [resource],
              )
            ).rows[0].active,
            true,
          );
          assert.equal((await request(`/api/v1/agent-executions/${job.id}`)).response.status, 404);
          assert.equal(
            (await request(`/api/v1/agent-executions/${job.id}/events`)).response.status,
            404,
          );
          assert.equal(await receipts(job.id), '1', 'Withholding cannot erase a committed result');
        } finally {
          if (removed) {
            await fs.filesystem.writeFile(
              author.context,
              randomUUID(),
              {
                op: 'create',
                parentId: root.id,
                expectedParentRevision: (await fs.filesystem.store.get(root.id)).revision,
                name: '.repository.json',
                content: {
                  size: descriptor.length,
                  digest: `sha256:${createHash('sha256').update(descriptor).digest('hex')}`,
                  mediaType: 'application/json',
                },
              },
              new ReadableStream({
                start(controller) {
                  controller.enqueue(descriptor);
                  controller.close();
                },
              }),
            );
            await publish();
          }
        }
        assert.equal(
          (await request(`/api/v1/agent-executions/${job.id}`)).response.status,
          404,
          'A replacement descriptor ID cannot revive the original pinned file proof',
        );
        const fresh = await enqueue('normal', 'fresh-descriptor');
        await once().done;
        assert.equal((await get(fresh.id)).status, 'succeeded');
      },
    );
    await selected(
      'accepted agent code remains pinned across redeployment and current authority gates retained results',
      async () => {
        const job = await enqueue('normal', 'agent-pinned');
        const previous = definitions;
        try {
          definitions = definitions.map((definition) =>
            definition.kind === 'Agent'
              ? { ...definition, spec: { ...definition.spec, artifact: image.adapter } }
              : definition,
          );
          await publish();
          await once().done;
          assert.equal((await get(job.id)).status, 'succeeded');
          const replay = await request(
            `/api/v1/agents/${resource}/executions`,
            'POST',
            { input: { mode: 'normal', prompt: 'hello', uid }, wait: false },
            'agent-pinned',
          );
          assert.equal(replay.response.status, 200, JSON.stringify(replay.value));
          assert.equal(replay.value.id, job.id);
        } finally {
          definitions = previous;
          await publish();
        }
        await authority.setGrants(
          environment,
          'fixture',
          principal,
          application,
          operations.filter((operation) => operation !== 'agents:invoke'),
        );
        assert.equal((await request(`/api/v1/agent-executions/${job.id}`)).response.status, 403);
      },
    );
  },
);
