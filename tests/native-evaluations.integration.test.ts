import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { spawn, execFile, type ChildProcess } from 'node:child_process';
import { createServer } from 'node:http';
import { mkdir, writeFile, readFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { agentImage } from './fixtures/agents/image.js';
import { database } from './fixtures/catalog-storage/database.js';
import { createLocalFilesystem } from '../services/filesystem/src/local.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { filesystemOperations } from '../packages/contracts/filesystem.js';
import type { Resource } from '../packages/contracts/types.js';
import { nativeInputs, nativeRoot as nativeRuntimeRoot } from '../scripts/native-inputs.js';
import { evaluationsClient } from '../packages/platform/src/evaluations.js';
import { createTransport } from '../packages/platform/src/transport.js';
import { transactionCommitProxy } from './fixtures/runtime/postgres-proxy.js';
import { fingerprint } from '../packages/compiler/src/index.js';
import { evaluationResumeFixture } from './fixtures/native-evaluations/resume.js';
import { evaluationPurposeFixture } from './fixtures/native-evaluations/purpose.js';
import { childProgressLockFixture } from './fixtures/native-evaluations/child-progress-lock.js';
import { evaluationProgressRetryFixture } from './fixtures/native-evaluations/progress-retry.js';
import { evaluationReplaySnapshot } from './fixtures/native-evaluations/replay-snapshot.js';

const execute = promisify(execFile);
async function until(check: () => Promise<boolean>, timeout = 90000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    if (await check()) return;
    await delay(50);
  }
  assert.fail('Native evaluation did not reach its real lifecycle barrier');
}

test(
  'native evaluations stream a pinned Dataset once and supervise immutable Agent and evaluator attempts',
  // Several independent accepted runs and fault barriers share this fixture watchdog.
  { timeout: 600000 },
  async (t) => {
    const nativeDigest = createHash('sha256');
    for (const path of nativeInputs())
      for (const bytes of [Buffer.from(path), await readFile(new URL(path, nativeRuntimeRoot))]) {
        const length = Buffer.alloc(8);
        length.writeBigUInt64BE(BigInt(bytes.length));
        nativeDigest.update(length);
        nativeDigest.update(bytes);
      }
    const expectedNativeDigest = nativeDigest.digest('hex');
    assert.equal(
      JSON.parse(
        (
          await execute(
            process.env.MORIA_RUNTIME_BINARY ??
              process.env.MORIA_INDEXING_BINARY ??
              '/usr/local/bin/moria-indexing',
            ['schema'],
          )
        ).stdout,
      ).sourceDigest,
      expectedNativeDigest,
      'Native runtime must come from this exact source snapshot',
    );
    const catalogMetadata = await execute(
      process.env.MORIA_CATALOG_TEST_BINARY ?? '/usr/local/bin/moria-catalog-tests',
      ['--ignored', '--exact', 'native_artifact_metadata', '--nocapture', '--test-threads=1'],
      { env: { ...process.env, MORIA_INDEXING_EXPECTED_SOURCE_SHA256: expectedNativeDigest } },
    );
    assert.match(catalogMetadata.stdout, /test result: ok\. 1 passed/);
    const db = await database(),
      image = await agentImage({
        childProgram:
          "export default async function(input,context){context.log('evaluation child consumed its accepted input');return{value:input.value};}\n",
      }),
      children: ChildProcess[] = [];
    const imageRepositories: string[] = [];
    const providerSecret = randomUUID();
    let modelCalls = 0;
    let holdModels = false;
    let heldModelCloses = 0;
    const heldModels = new Set<() => void>();
    const provider = createServer(async (request, response) => {
      assert.equal(request.headers.authorization, `Bearer ${providerSecret}`);
      let bytes = '';
      for await (const part of request) bytes += part;
      assert.equal(JSON.parse(bytes).model, 'native-evaluation-model');
      modelCalls++;
      if (holdModels) {
        await new Promise<void>((resolve) => {
          const release = () => {
            heldModels.delete(release);
            resolve();
          };
          heldModels.add(release);
          response.once('close', () => {
            heldModelCloses++;
            release();
          });
        });
        if (response.destroyed) return;
      }
      response.setHeader('content-type', 'application/json');
      response.end(
        JSON.stringify({
          id: 'evaluation-model',
          choices: [
            {
              index: 0,
              message: { role: 'assistant', content: 'native evaluation answer' },
              finish_reason: 'stop',
            },
          ],
          usage: { prompt_tokens: 3, completion_tokens: 4, total_tokens: 7 },
        }),
      );
    });
    await new Promise<void>((resolve) => provider.listen(0, '127.0.0.1', resolve));
    const endpoint = `http://127.0.0.1:${(provider.address() as { port: number }).port}/v1/`;
    t.after(async () => {
      for (const release of heldModels) release();
      for (const child of children) child.kill('SIGKILL');
      for (const repository of imageRepositories)
        await execute('docker', ['image', 'rm', '--force', repository]).catch(() => {});
      await image.close();
      await new Promise<void>((resolve) => provider.close(() => resolve()));
      await db.close();
    });
    const environment = randomUUID(),
      principal = 'evaluation-user',
      bootstrap = 'evaluation-author';
    await db.catalog.ensureEnvironment(environment);
    const fs = await createLocalFilesystem(db.catalog, join(db.root, 'files')),
      authority = fs.filesystem.security.authority,
      sessions = new WorkloadSessions(authority);
    const fsOperations = filesystemOperations.map((operation) => `filesystem:${operation}`);
    await authority.syncPrincipal(environment, 'fixture', principal, {
      kind: 'workload',
      enabled: true,
      attributes: {},
      markings: ['evaluations/Marking/Private'],
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
    const home = await fs.management.ensureHome(author.context),
      role = await fs.management.role(author.context, home.home.id),
      projects = await fs.filesystem.store.get(role!.configuration.projects as string);
    await fs.management.createProject(
      author.context,
      projects.id,
      'evaluations',
      projects.revision,
    );
    const source = await fs.filesystem.store.resolve(
      `/users/${home.home.name}/projects/evaluations/source`,
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
    const file = async (name: string, content: unknown) => {
      const bytes = Buffer.from(JSON.stringify(content));
      await fs.filesystem.writeFile(
        author.context,
        randomUUID(),
        {
          op: 'create',
          parentId: root.id,
          expectedParentRevision: (await fs.filesystem.store.get(root.id)).revision,
          name,
          content: {
            size: bytes.length,
            digest: `sha256:${createHash('sha256').update(bytes).digest('hex')}`,
            mediaType: 'application/json',
          },
        },
        new ReadableStream({
          start(controller) {
            controller.enqueue(bytes);
            controller.close();
          },
        }),
      );
    };
    await file('.repository.json', {
      version: 1,
      name: 'Native evaluations',
      namespace: 'evaluations',
      runtime: 'typescript-v1',
    });
    await fs.source.register(
      author.context,
      root.id,
      (await fs.filesystem.store.get(root.id)).revision,
      'repository',
    );
    const evaluatorDirectory = join(db.root, 'evaluator-image');
    await mkdir(evaluatorDirectory);
    const evaluator = async (suffix: string) => {
      const repository = image.artifact.uri.slice(6).replace(/\/[^/]+$/, `/evaluator-${suffix}`);
      imageRepositories.push(repository);
      await writeFile(
        join(evaluatorDirectory, 'evaluator.mjs'),
        `export default async function(input,context){const actual=JSON.parse(input.actualJson),expected=JSON.parse(input.expectedJson);if(expected.__denyPlatform)await context.platform.fetch('/api/v1/agent/model',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({messages:[{role:'user',content:'forbidden evaluator inference'}]})});context.log('deterministic evaluator completed');const passed=JSON.stringify(actual)===JSON.stringify(expected);return{passed,score:passed?1:0,message:passed?'matched':'different'};}\n// evaluator version ${suffix}\n`,
      );
      await writeFile(
        join(evaluatorDirectory, 'Dockerfile'),
        `FROM ${image.artifact.uri.slice(6)}@${image.artifact.digest}\nCOPY evaluator.mjs /evaluator.mjs\nCMD ["node","/function/runtime/runner.mjs","/evaluator.mjs"]\n`,
      );
      await execute('docker', ['build', '--pull=false', '--tag', repository, evaluatorDirectory]);
      const push = await execute('docker', ['push', repository]);
      const digest = push.stdout.match(/digest: (sha256:[0-9a-f]{64})/)?.[1];
      assert.ok(digest);
      return { uri: `oci://${repository}`, digest };
    };
    const evaluator1 = await evaluator('v1'),
      evaluator2 = await evaluator('v2');
    const string = { type: 'string' as const, nullable: false };
    let definitions: Resource[] = [
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'AgentAdapter',
        metadata: { namespace: 'evaluations', name: 'LangGraph' },
        spec: {
          artifact: image.adapter,
          abiVersion: 'agent/v1',
          capabilities: ['events', 'cancel'],
        },
      },
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'Agent',
        metadata: { namespace: 'evaluations', name: 'Assistant' },
        spec: {
          artifact: image.artifact,
          adapter: 'evaluations/AgentAdapter/LangGraph',
          application: 'evaluations/Application/App',
          model: 'evaluations/Model/Chat',
          inputs: { mode: string, prompt: string, uid: { type: 'uid', nullable: true } },
          outputs: { answer: string },
          limits: { timeoutSeconds: 60, memoryMiB: 256, cpuMillis: 10000 },
        },
      },
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'Function',
        metadata: { namespace: 'evaluations', name: 'Tool' },
        spec: {
          artifact: image.child,
          runtime: 'typescript-v1',
          effect: 'readOnly',
          inputs: {
            mode: string,
            value: { type: 'float64', nullable: false },
            uid: { type: 'uid', nullable: true },
          },
          outputs: { value: { type: 'float64', nullable: false } },
          limits: { timeoutSeconds: 30, memoryMiB: 128, cpuMillis: 10000 },
        },
      },
    ];
    for (const declaration of [
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'Marking',
        metadata: { namespace: 'evaluations', name: 'Private' },
        spec: { eligibilityGroup: 'fixture', declassifierGroup: 'fixture' },
      },
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'Application',
        metadata: { namespace: 'evaluations', name: 'App' },
        spec: {
          mode: 'service',
          scopes: [{ target: 'evaluations/Model/Chat', operations: ['invoke'] }],
        },
      },
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'Policy',
        metadata: { namespace: 'evaluations', name: 'Local' },
        spec: {
          type: 'modelDestination',
          allowedDestinations: ['local-evaluation'],
          allowedMarkings: ['evaluations/Marking/Private'],
        },
      },
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'ModelProvider',
        metadata: { namespace: 'evaluations', name: 'Provider' },
        spec: {
          adapter: 'openai-compatible-v1',
          endpoint,
          destination: 'local-evaluation',
          destinationPolicy: 'evaluations/Policy/Local',
        },
      },
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'Model',
        metadata: { namespace: 'evaluations', name: 'Chat' },
        spec: {
          provider: 'evaluations/ModelProvider/Provider',
          upstreamName: 'native-evaluation-model',
          versionLabel: 'v1',
          mutable: false,
          capabilities: ['chat'],
        },
      },
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'Dataset',
        metadata: { namespace: 'evaluations', name: 'Cases' },
        spec: {
          columns: { caseId: string, inputJson: string, expectedJson: string },
          primaryKey: ['caseId'],
          writeModes: ['replace'],
        },
      },
      ...['Check', 'Other'].map((name, index) => ({
        apiVersion: 'moria/v1alpha2',
        kind: 'EvaluationSuite',
        metadata: { namespace: 'evaluations', name },
        spec: {
          agent: 'evaluations/Agent/Assistant',
          fixtures: 'evaluations/Dataset/Cases',
          evaluator: index ? evaluator2 : evaluator1,
          repeats: 2,
        },
      })),
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'Automation',
        metadata: { namespace: 'evaluations', name: 'Nightly' },
        spec: {
          operation: 'evaluate',
          target: 'evaluations/EvaluationSuite/Check',
          application: 'evaluations/Application/App',
          intervalSeconds: 60,
          input: {},
          overlap: 'skip',
          missed: 'coalesce',
        },
      },
    ])
      await file(`${declaration.metadata.name}.resource.json`, declaration);
    let resources = new Map<string, string>();
    let release = '';
    const publish = async () => {
      const publication = await fs.source
        .publish(
          author.context,
          root.id,
          (await fs.source.checkout(author.context, root.id)).digest,
          definitions,
        )
        .catch((error: unknown) => {
          assert.fail(
            JSON.stringify({
              message: error instanceof Error ? error.message : String(error),
              details: (error as { details?: unknown }).details,
            }),
          );
        });
      await fs.source.apply(
        author.context,
        (await fs.source.plan(author.context, root.id, publication.digest)).id,
      );
      resources = new Map(
        publication.resources.map((resource) => [resource.reference, resource.id]),
      );
      release = publication.digest;
    };
    await publish();
    const application = resources.get('evaluations/Application/App')!,
      model = resources.get('evaluations/Model/Chat')!,
      fixture = resources.get('evaluations/Dataset/Cases')!,
      suite = resources.get('evaluations/EvaluationSuite/Check')!,
      tool = resources.get('evaluations/Function/Tool')!;
    const operations = [
      'evaluations:run',
      'agents:invoke',
      `${model}:invoke`,
      `${fixture}:read`,
      `${fixture}:write`,
      `${tool}:invoke`,
      ...fsOperations,
    ];
    const user = await actor(application, operations);
    const dbRole = `eval_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(
      `CREATE ROLE ${dbRole} LOGIN PASSWORD '${password}'; GRANT USAGE ON SCHEMA moria TO ${dbRole}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${dbRole}; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${dbRole}`,
    );
    const connection = `postgresql://${dbRole}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const nativeRoot = join(db.root, 'native'),
      scratch = join(db.root, 'scratch'),
      physical = join(db.root, 'iceberg');
    await Promise.all([mkdir(nativeRoot), mkdir(scratch), mkdir(physical)]);
    const rows = [
      {
        caseId: 'pass',
        inputJson: JSON.stringify({ mode: 'child', prompt: tool, uid: null }),
        expectedJson: JSON.stringify({ answer: 'succeeded' }),
      },
      {
        caseId: 'fail',
        inputJson: JSON.stringify({ mode: 'normal', prompt: 'beta', uid: null }),
        expectedJson: JSON.stringify({ answer: 'different' }),
      },
      {
        caseId: 'error',
        inputJson: JSON.stringify({
          mode: 'fail',
          prompt: 'invalid missing object',
          uid: null,
        }),
        expectedJson: '{}',
      },
      {
        caseId: 'model',
        inputJson: JSON.stringify({ mode: 'model', prompt: 'real native model', uid: null }),
        expectedJson: JSON.stringify({ answer: 'native evaluation answer' }),
      },
      {
        caseId: 'denied-evaluator',
        inputJson: JSON.stringify({ mode: 'normal', prompt: 'no evaluator privilege', uid: null }),
        expectedJson: JSON.stringify({ __denyPlatform: true }),
      },
    ];
    const rowsPath = join(db.root, 'cases.json');
    await writeFile(rowsPath, JSON.stringify(rows));
    const logical = 'moria://evaluationwarehouse/cases';
    const generated = JSON.parse(
      (
        await execute(resolve('.venv/bin/python'), [
          'tests/fixtures/agents/evaluation-dataset.py',
          physical,
          logical,
          rowsPath,
        ])
      ).stdout,
    );
    await db.catalog.ensureDataset(environment, fixture);
    await db.pool.query(
      "INSERT INTO moria.native_storage_namespaces(environment,warehouse_id,table_id,resource,revision,purpose,iceberg_uuid,markings) VALUES($1,'evaluationwarehouse','cases',$2,'v1','dataset-source',$3,ARRAY['evaluations/Marking/Private'])",
      [environment, fixture, generated.tableUuid],
    );
    const metadata = JSON.parse(await readFile(generated.objects[generated.metadata], 'utf8'));
    const fixtureSchema = metadata.schemas.find(
      (schema: any) => schema['schema-id'] === metadata['current-schema-id'],
    );
    assert.ok(fixtureSchema);
    const fixtureColumns = { caseId: string, inputJson: string, expectedJson: string };
    await db.pool.query(
      "INSERT INTO moria.native_dataset_bindings(environment,resource,reference,revision,columns,iceberg_schema,warehouse_id,table_id,namespace_revision,physical_binding_fingerprint,source_digest,primary_key,write_modes,key_schema_hash) VALUES($1,$2,'evaluations/Dataset/Cases',$3,$4,$5,'evaluationwarehouse','cases','v1',$6,$7,ARRAY['caseId'],ARRAY['replace'],$8)",
      [
        environment,
        fixture,
        fingerprint({
          fixture,
          columns: fixtureColumns,
          schema: fixtureSchema,
          primaryKey: ['caseId'],
          writeModes: ['replace'],
        }),
        fixtureColumns,
        fixtureSchema,
        fingerprint({ backend: 'local', root: nativeRoot }),
        release,
        fingerprint({ version: 1, keys: [{ name: 'caseId', descriptor: string }] }),
      ],
    );
    await db.pool.query(
      'INSERT INTO moria.native_model_bindings(environment,resource,revision,definition) VALUES($1,$2,$3,$4)',
      [
        environment,
        model,
        'v1',
        {
          endpointRef: 'EVAL_MODEL_ENDPOINT',
          secretRef: 'EVAL_MODEL_SECRET',
          model: 'native-evaluation-model',
          modelRevision: 'v1',
          resourceClass: 'models',
          destination: 'local-evaluation',
          markings: [],
          destinationPolicy: {
            allowedDestinations: ['local-evaluation'],
            allowedMarkings: ['evaluations/Marking/Private'],
          },
          capabilities: ['chat'],
        },
      ],
    );
    const owner = randomUUID(),
      host = randomUUID();
    await db.pool.query(
      "INSERT INTO moria.runtime_workers(owner,host_id,incarnation,resource_classes,domains,expires_at) SELECT $1,$2,incarnation,ARRAY['standard','evaluation-control'],ARRAY['agent','function','evaluation','evaluator','sync'],clock_timestamp()+interval '1 hour' FROM moria.indexing_instance",
      [owner, host],
    );
    const nativeEnvironment = {
      PATH: process.env.PATH,
      MORIA_INDEXING_DATABASE_SECRET_REF: 'EVALUATION_DATABASE',
      EVALUATION_DATABASE: connection,
      MORIA_INDEXING_DATABASE_TLS: 'disable',
      MORIA_AGENT_WORKER_OWNER: owner,
      MORIA_AGENT_HOST_ID: host,
      MORIA_FUNCTION_WORKER_OWNER: owner,
      MORIA_FUNCTION_HOST_ID: host,
      MORIA_EVALUATION_WORKER_OWNER: owner,
      MORIA_EVALUATION_HOST_ID: host,
      EVAL_MODEL_ENDPOINT: endpoint,
      EVAL_MODEL_SECRET: providerSecret,
      MORIA_NATIVE_STORAGE_BINDINGS: JSON.stringify([
        {
          namespace: { warehouseId: 'evaluationwarehouse', tableId: 'cases', revision: 'v1' },
          provider: {
            kind: 'posix',
            root: nativeRoot,
            limits: { maxObjectBytes: 134217728, chunkBytes: 1048576 },
          },
        },
      ]),
      MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT: scratch,
      MORIA_NATIVE_CLOSURE_LIMITS: JSON.stringify({
        maxEntries: 1000,
        maxReferencedBytes: 134217728,
        maxSpoolBytes: 8388608,
      }),
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
      child.stdout.on('data', (bytes) => (output += bytes));
      child.stderr.on('data', (bytes) => (error += bytes));
      child.stdin.on('error', () => {});
      child.stdin.end(input === undefined ? '' : JSON.stringify(input));
      const done = new Promise<string>((resolve, reject) => {
        child.once('error', reject);
        child.once('exit', (code, signal) =>
          code === 0
            ? resolve(output)
            : reject(
                new Error(
                  `Native evaluation ${arguments_.join(' ')} failed ${code ?? signal}: ${error}`,
                ),
              ),
        );
      });
      void done.catch(() => {});
      return { child, done, output: () => output, error: () => error };
    };
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
    let publicToken = user.token;
    const native = async (command: string, input: Record<string, unknown>, token = publicToken) => {
      let path = `/api/v1/evaluation-runs/${input.id}`,
        method = 'GET',
        body: unknown;
      if (command === 'invoke') {
        path = `/api/v1/evaluations/${input.resource}/runs`;
        method = 'POST';
        body = { wait: false };
      } else if (command === 'cancel') {
        path += '/cancel';
        method = 'POST';
        body = {};
      } else if (command === 'compare')
        path += `/compare?${new URLSearchParams({ other: String(input.other) })}`;
      else if (['cases', 'events'].includes(command))
        path += `/${command}?${new URLSearchParams(input.after === undefined ? {} : { after: String(input.after) })}`;
      const response = await fetch(api + path, {
        method,
        headers: {
          authorization: `Bearer ${token}`,
          'content-type': 'application/json',
          ...(input.key ? { 'idempotency-key': String(input.key) } : {}),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      });
      const value = (await response.json()) as any;
      if (!response.ok) throw new Error(JSON.stringify({ status: response.status, ...value }));
      return value;
    };
    const sourceEpoch = (await authority.current(environment, principal, application)).epoch;
    const sourceBinding = (
      await db.pool.query(
        `INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation) SELECT $1,$2,$3,$4,$5,ARRAY['evaluations/Marking/Private'],$6,i.security_epoch,c.incarnation FROM moria.instance_state i,moria.indexing_instance c RETURNING id`,
        [
          environment,
          fixture,
          {
            version: 1,
            domain: 'sync',
            release: 'a'.repeat(64),
            resourceClass: 'standard',
            timeoutSeconds: 120,
            retryPolicy: 'idempotent',
            inputSchema: { type: 'object', properties: {} },
            pinnedInputs: [],
            expectedHeads: { [fixture]: '0' },
          },
          {
            environment,
            principal,
            application,
            sessionHash: createHash('sha256').update(user.token).digest('hex'),
          },
          [`${fixture}:read`, `${fixture}:write`],
          sourceEpoch,
        ],
      )
    ).rows[0].id;
    const producer = spawn(
      process.env.MORIA_CATALOG_TEST_BINARY ?? '/usr/local/bin/moria-catalog-tests',
      ['--ignored', '--exact', 'catalog_publication', '--nocapture', '--test-threads=1'],
      {
        env: {
          ...nativeEnvironment,
          MORIA_NATIVE_CATALOG_FIXTURE: JSON.stringify({
            root: nativeRoot,
            token: user.token,
            binding: sourceBinding,
            mode: 'valid',
            registeredHost: host,
            registeredOwner: owner,
            snapshots: [
              {
                ...generated,
                resource: fixture,
                warehouseId: 'evaluationwarehouse',
                tableId: 'cases',
              },
            ],
          }),
        },
        stdio: ['ignore', 'pipe', 'pipe'],
      },
    );
    children.push(producer);
    let producerOutput = '',
      producerError = '';
    producer.stdout.on('data', (bytes) => (producerOutput += bytes));
    producer.stderr.on('data', (bytes) => (producerError += bytes));
    await new Promise<void>((resolve, reject) => {
      producer.once('error', reject);
      producer.once('exit', (code) =>
        code === 0
          ? resolve()
          : reject(
              new Error(
                `Actual native fixture publication failed: ${producerOutput}\n${producerError}`,
              ),
            ),
      );
    });
    assert.match(producerOutput, /"event":"receipt"/);
    await start(['models', 'probe'], { token: user.token, resource: model }).done;
    const beforeModels = modelCalls;
    let evaluationWorker: ReturnType<typeof start> | undefined,
      agentWorker: ReturnType<typeof start> | undefined,
      functionWorker: ReturnType<typeof start> | undefined;
    t.after(() => {
      evaluationWorker?.child.kill('SIGTERM');
      agentWorker?.child.kill('SIGTERM');
      functionWorker?.child.kill('SIGTERM');
    });
    const diagnostics = async (id: string) => ({
      states: (
        await db.pool.query(
          "WITH RECURSIVE jobs AS(SELECT id,context,spec,status,error FROM moria.runtime_executions WHERE id=$1 UNION ALL SELECT e.id,e.context,e.spec,e.status,e.error FROM jobs p JOIN moria.runtime_job_delegations d ON d.parent_execution=p.id JOIN moria.runtime_executions e ON e.id=d.execution) SELECT id,spec->>'domain' AS domain,status,error FROM jobs",
          [id],
        )
      ).rows,
      failures: (
        await db.pool.query(
          "SELECT e.id,e.spec->>'domain' AS domain,v.kind,v.payload FROM moria.runtime_execution_events v JOIN moria.runtime_executions e ON e.id=v.execution WHERE e.environment=$1 AND v.kind IN ('failed','brokerFailure','agentModelBrokerFailure')",
          [environment],
        )
      ).rows,
      agentModelCalls: (
        await db.pool.query('SELECT logical_id,status,error FROM moria.runtime_agent_model_calls')
      ).rows,
      gatewayCalls: (
        await db.pool.query(
          'SELECT state,error FROM moria.native_model_calls WHERE environment=$1',
          [environment],
        )
      ).rows,
      sandboxObservations: (
        await db.pool.query(
          "SELECT e.id,e.spec->>'domain' AS domain,e.status,c.case_id,c.repeat,s.state,s.error,s.observation_pending,s.observation_blocked FROM moria.runtime_executions e JOIN moria.runtime_sandbox_attempts s ON s.execution=e.id LEFT JOIN moria.runtime_evaluation_cases c ON c.agent_execution=e.id OR c.evaluator_execution=e.id WHERE e.environment=$1 ORDER BY e.created_at",
          [environment],
        )
      ).rows,
    });
    const terminal = async (id: string) => {
      let result: any;
      await until(async () => {
        if (evaluationWorker?.child.exitCode !== null && evaluationWorker)
          assert.fail(
            JSON.stringify({ worker: evaluationWorker.error(), ...(await diagnostics(id)) }),
          );
        if (agentWorker?.child.exitCode !== null && agentWorker)
          assert.fail(JSON.stringify({ worker: agentWorker.error(), ...(await diagnostics(id)) }));
        if (functionWorker?.child.exitCode !== null && functionWorker)
          assert.fail(
            JSON.stringify({ worker: functionWorker.error(), ...(await diagnostics(id)) }),
          );
        result = await native('get', { id }).catch(async (error: unknown) => {
          const active = (
            await db.pool.query(
              "SELECT status IN('queued','running','preparing') AS active FROM moria.runtime_executions WHERE id=$1",
              [id],
            )
          ).rows[0]?.active;
          if (active && /FUNCTION_OBSERVATION_INCOMPLETE/.test(String(error))) return null;
          assert.fail(
            JSON.stringify({
              error: error instanceof Error ? error.message : String(error),
              ...(await diagnostics(id)),
            }),
          );
        });
        if (!result) return false;
        return !['queued', 'running', 'preparing'].includes(result.status);
      }).catch(async (error: unknown) => {
        assert.fail(
          JSON.stringify({
            error: error instanceof Error ? error.message : String(error),
            ...(await diagnostics(id)),
          }),
        );
      });
      return result;
    };
    let first = '';
    await t.test(
      'admission requires a dedicated environment and pins the suite, Agent and real native Dataset',
      async () => {
        await assert.rejects(
          native('invoke', { resource: suite, key: 'not-approved' }),
          /EVALUATION_ENVIRONMENT_REQUIRED/,
        );
        await db.pool.query(
          'INSERT INTO moria.runtime_evaluation_environments(environment,application,maximum_cases,maximum_repeats,maximum_parallel) VALUES($1,$2,12,4,2)',
          [environment, application],
        );
        const accepted = await native('invoke', { resource: suite, key: 'run-original' });
        first = accepted.id;
        assert.equal(accepted.status, 'queued');
        assert.equal((await native('invoke', { resource: suite, key: 'run-original' })).id, first);
        assert.deepEqual(accepted.markings, [
          { environment, marking: 'evaluations/Marking/Private' },
        ]);
        const observer = await sessions.mint({ environment, principal, application }, 'fixture');
        publicToken = observer.token;
        await db.pool.query(
          "UPDATE moria.workload_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token_hash=$1",
          [createHash('sha256').update(user.token).digest('hex')],
        );
        await assert.rejects(native('get', { id: first }, user.token), /401/);
        const pin = (
          await db.pool.query(
            'SELECT agent_pin,fixture_pin FROM moria.runtime_evaluations WHERE execution=$1',
            [first],
          )
        ).rows[0];
        assert.equal(pin.fixture_pin.snapshot, generated.snapshotId);
        assert.equal(pin.agent_pin.definition.spec.artifact.digest, image.artifact.digest);
        await assert.rejects(
          start(['execution', 'claim'], { owner, domains: ['evaluator'] }).done,
          /FUNCTION_SANDBOX_REQUIRED/,
        );
        const previous = definitions;
        definitions = definitions.map((definition) =>
          definition.kind === 'Agent'
            ? { ...definition, spec: { ...definition.spec, artifact: image.adapter } }
            : definition,
        );
        await publish();
        try {
          evaluationWorker = start(['evaluations', 'worker']);
          agentWorker = start(['agents', 'worker']);
          functionWorker = start(['functions', 'worker']);
          const result = await terminal(first);
          assert.equal(
            result.status,
            'completed',
            JSON.stringify({
              result,
              cases: (
                await db.pool.query(
                  'SELECT case_id,repeat,outcome,error FROM moria.runtime_evaluation_cases WHERE evaluation=$1',
                  [first],
                )
              ).rows,
              worker: evaluationWorker.error(),
              diagnostics: await diagnostics(first),
            }),
          );
          const { scoreStandardDeviation, ...summary } = result.summary;
          assert.deepEqual(
            summary,
            {
              total: '10',
              passed: '4',
              failed: '2',
              errors: '4',
              meanScore: 2 / 3,
            },
            JSON.stringify({
              diagnostics: await diagnostics(first),
              cases: (
                await db.pool.query(
                  'SELECT case_id,repeat,outcome,error,agent_execution,evaluator_execution FROM moria.runtime_evaluation_cases WHERE evaluation=$1',
                  [first],
                )
              ).rows,
            }),
          );
          assert.ok(Math.abs(scoreStandardDeviation - Math.sqrt(4 / 15)) < 1e-12);
          assert.equal(
            modelCalls - beforeModels,
            2,
            'Only Agent model cases may dispatch inference',
          );
          assert.deepEqual(
            (
              await db.pool.query(
                "SELECT DISTINCT markings FROM moria.native_model_calls WHERE call_key LIKE 'agent/%'",
              )
            ).rows.map((row) => row.markings),
            [['evaluations/Marking/Private']],
            'Private Dataset input labels must reach the model even when the model route is public',
          );
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.runtime_agent_executions a JOIN moria.runtime_evaluation_cases c ON c.agent_execution=a.execution WHERE c.evaluation=$1 AND NOT a.markings @> $2::jsonb',
                [first, JSON.stringify([{ environment, marking: 'evaluations/Marking/Private' }])],
              )
            ).rows[0].count,
            '0',
          );
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.runtime_evaluation_receipts WHERE execution=$1',
                [first],
              )
            ).rows[0].count,
            '1',
          );
          const cases = await native('cases', { id: first });
          assert.equal(cases.cases.length, 10);
          assert.deepEqual(cases.cases.map((value: any) => value.status).sort(), [
            'error',
            'error',
            'error',
            'error',
            'fail',
            'fail',
            'pass',
            'pass',
            'pass',
            'pass',
          ]);
          const attempts = (
            await db.pool.query(
              "SELECT e.spec->>'domain' AS domain,t.attempts FROM moria.runtime_execution_tasks t JOIN moria.runtime_executions e ON e.id=t.execution WHERE e.id IN(SELECT agent_execution FROM moria.runtime_evaluation_cases WHERE evaluation=$1 UNION ALL SELECT evaluator_execution FROM moria.runtime_evaluation_cases WHERE evaluation=$1)",
              [first],
            )
          ).rows;
          assert.equal(attempts.length, 18);
          assert.ok(attempts.every((row) => row.attempts === 1));
          const tools = (
            await db.pool.query(
              "SELECT f.execution,f.parent_id,f.markings,d.parent_execution,e.status,b.resource FROM moria.runtime_function_invocations f JOIN moria.runtime_executions e ON e.id=f.execution JOIN moria.runtime_job_delegations d ON d.execution=f.execution JOIN moria.runtime_execution_bindings b ON b.id=e.binding JOIN moria.runtime_evaluation_cases c ON c.agent_execution=f.parent_id WHERE c.evaluation=$1 AND c.case_id='pass' ORDER BY c.repeat",
              [first],
            )
          ).rows;
          assert.equal(tools.length, 2, 'Each repeat must execute its actual Function tool');
          assert.equal(
            (await db.pool.query('SELECT count(*) FROM moria.runtime_function_invocations')).rows[0]
              .count,
            '2',
          );
          for (const child of tools) {
            assert.equal(child.parent_execution, child.parent_id);
            assert.equal(child.status, 'succeeded');
            assert.equal(child.resource, tool);
            assert.deepEqual(child.markings, [
              { environment, marking: 'evaluations/Marking/Private' },
            ]);
          }
          assert.equal(
            (
              await db.pool.query(
                "SELECT count(*) FROM moria.runtime_execution_events WHERE execution=$1 AND kind='fixturesLoaded'",
                [first],
              )
            ).rows[0].count,
            '1',
          );
        } finally {
          definitions = previous;
          await publish();
        }
      },
    );
    await t.test(
      'repeats stay separate and comparison rejects a different evaluator version',
      async () => {
        const scoped = [
          `${suite}:run`,
          `${resources.get('evaluations/Agent/Assistant')}:invoke`,
          `${model}:invoke`,
          `${tool}:invoke`,
          `${fixture}:read`,
        ];
        await authority.setApplication(environment, 'fixture', application, scoped);
        await authority.setGrants(environment, 'fixture', principal, application, scoped);
        try {
          const second = await native('invoke', { resource: suite, key: 'run-comparable' });
          assert.deepEqual(
            (
              await db.pool.query(
                'SELECT b.operations FROM moria.runtime_execution_bindings b JOIN moria.runtime_executions e ON e.binding=b.id WHERE e.id=$1',
                [second.id],
              )
            ).rows[0].operations,
            [`${suite}:run`, `${model}:invoke`],
          );
          assert.equal(
            (await terminal(second.id)).status,
            'completed',
            JSON.stringify(await diagnostics(second.id)),
          );
          assert.equal((await native('compare', { id: first, other: second.id })).comparable, true);
          assert.notEqual(
            (
              await fetch(api + '/api/management/v1/fs/home', {
                headers: { authorization: `Bearer ${publicToken}` },
              })
            ).status,
            200,
          );
        } finally {
          await authority.setApplication(environment, 'fixture', application, operations);
          await authority.setGrants(environment, 'fixture', principal, application, operations);
        }
        const other = await native('invoke', {
          resource: resources.get('evaluations/EvaluationSuite/Other')!,
          key: 'run-incompatible',
        });
        assert.equal((await terminal(other.id)).status, 'completed');
        assert.equal((await native('compare', { id: first, other: other.id })).comparable, false);
      },
    );
    await t.test(
      'public SDK and CLI inspect the same native receipt and preserve logical replay',
      async () => {
        const sdk = evaluationsClient(
          createTransport({
            baseUrl: api,
            fetch: (input, init) =>
              fetch(input, {
                ...init,
                headers: { ...init?.headers, authorization: `Bearer ${publicToken}` },
              }),
          }),
        );
        assert.equal((await sdk.get(first)).status, 'completed');
        assert.equal((await sdk.cases(first)).cases.length, 10);
        assert.equal(
          (await sdk.invoke(suite, { idempotencyKey: 'run-original', wait: false })).id,
          first,
        );
        assert.equal((await sdk.compare(first, first)).comparable, true);
        const result = await execute(
          process.execPath,
          ['--import', 'tsx', 'apps/cli/src/main.ts', 'evaluations', 'get', first, '--url', api],
          { env: { PATH: process.env.PATH, MORIA_API_TOKEN: publicToken } },
        );
        assert.equal(JSON.parse(result.stdout).id, first);
        assert.deepEqual(JSON.parse(result.stdout).summary, (await sdk.get(first)).summary);
        const cliReplay = await execute(
          process.execPath,
          [
            '--import',
            'tsx',
            'apps/cli/src/main.ts',
            'evaluations',
            'invoke',
            suite,
            '--key',
            'run-original',
            '--async',
            '--url',
            api,
          ],
          { env: { PATH: process.env.PATH, MORIA_API_TOKEN: publicToken } },
        );
        assert.equal(JSON.parse(cliReplay.stdout).id, first);
      },
    );
    await t.test(
      'cancelling a run fences every accepted descendant and stops live model and OCI work',
      async () => {
        holdModels = true;
        try {
          const cancelled = await native('invoke', { resource: suite, key: 'run-cancelled' });
          await until(async () => heldModels.size > 0);
          await assert.rejects(
            native('get', { id: cancelled.id }),
            /FUNCTION_OBSERVATION_INCOMPLETE/,
          );
          const beforeCloses = heldModelCloses;
          const acceptedChildren = (
            await db.pool.query(
              "WITH RECURSIVE jobs AS(SELECT $1::uuid AS id UNION ALL SELECT d.execution FROM jobs p JOIN moria.runtime_job_delegations d ON d.parent_execution=p.id) SELECT e.id FROM jobs JOIN moria.runtime_executions e USING(id) WHERE e.spec->>'domain'='agent' AND e.status='running'",
              [cancelled.id],
            )
          ).rows.map((row) => row.id);
          assert.ok(acceptedChildren.length > 0);
          const startCancel = Date.now();
          assert.deepEqual(await native('cancel', { id: cancelled.id }), {
            id: cancelled.id,
            cancelRequested: true,
          });
          await until(async () => heldModelCloses > beforeCloses, 10000);
          await until(async () => {
            const containers = (
              await execute('docker', ['ps', '-a', '--format', '{{.Names}}'])
            ).stdout.split('\n');
            return acceptedChildren.every((id) => !containers.includes(`moria-native-agent-${id}`));
          }, 10000);
          assert.ok(Date.now() - startCancel < 10000);
          assert.equal(
            (
              await db.pool.query(
                "WITH RECURSIVE jobs AS(SELECT $1::uuid AS id UNION ALL SELECT d.execution FROM jobs p JOIN moria.runtime_job_delegations d ON d.parent_execution=p.id) SELECT count(*) FROM jobs JOIN moria.runtime_executions e USING(id) WHERE status IN ('queued','running','preparing')",
                [cancelled.id],
              )
            ).rows[0].count,
            '0',
          );
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.runtime_evaluation_receipts WHERE execution=$1',
                [cancelled.id],
              )
            ).rows[0].count,
            '0',
          );
          // Cancellation can fence an unfinished private observation without releasing it.
          await assert.rejects(
            native('get', { id: cancelled.id }),
            /FUNCTION_OBSERVATION_INCOMPLETE/,
          );
          await assert.rejects(
            native('events', { id: cancelled.id }),
            /FUNCTION_OBSERVATION_INCOMPLETE/,
          );
          assert.equal(
            (
              await db.pool.query(
                "SELECT count(*) FROM moria.runtime_execution_events WHERE execution=$1 AND kind='cancelled'",
                [cancelled.id],
              )
            ).rows[0].count,
            '1',
          );
        } finally {
          holdModels = false;
          for (const release of heldModels) release();
        }
      },
    );
    await t.test(
      'a real root receipt COMMIT with a lost acknowledgement replays once after worker SIGKILL',
      async () => {
        assert.ok(evaluationWorker);
        evaluationWorker.child.kill('SIGTERM');
        await evaluationWorker.done;
        const proxy = await transactionCommitProxy(
          Number(db.pool.options.port),
          'INSERT INTO moria.runtime_evaluation_receipts',
        );
        t.after(() => proxy.close());
        const url = new URL(connection);
        url.port = String(proxy.port);
        const accepted = await native('invoke', { resource: suite, key: 'run-lost-commit' });
        const replaySession = await sessions.mint(
          { environment, principal, application },
          'fixture',
        );
        const snapshotGate = await evaluationReplaySnapshot(
          db.pool,
          accepted.id,
          suite,
          createHash('sha256').update(replaySession.token).digest('hex'),
        );
        const worker = start(['evaluations', 'worker'], undefined, {
          EVALUATION_DATABASE: url.toString(),
        });
        try {
          const replay = snapshotGate.replay(
            () =>
              native('invoke', { resource: suite, key: 'run-lost-commit' }, replaySession.token),
            proxy.committed,
            AbortSignal.timeout(90000),
          );
          const duringCommit = await Promise.race([
            replay,
            worker.done.then(() => assert.fail('Worker stopped before root COMMIT')),
          ]).catch(async (error: unknown) =>
            assert.fail(
              JSON.stringify({
                message: error instanceof Error ? error.message : String(error),
                checkpoints: proxy.checkpoints,
                worker: worker.error(),
                agentWorker: agentWorker?.error(),
                agentExit: agentWorker?.child.exitCode,
                result: await native('get', { id: accepted.id }).catch((error: unknown) => ({
                  error: error instanceof Error ? error.message : String(error),
                })),
                diagnostics: await diagnostics(accepted.id),
              }),
            ),
          );
          assert.equal(duringCommit.id, accepted.id);
          assert.equal(duringCommit.status, 'running');
          assert.equal(duringCommit.summary, null);
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.runtime_evaluation_receipts WHERE execution=$1',
                [accepted.id],
              )
            ).rows[0].count,
            '1',
          );
          const control = await db.pool.connect();
          try {
            await until(async () => {
              const { rows } = await control.query(
                'SELECT pg_try_advisory_lock(hashtextextended($1,0)) AS acquired',
                [`moria/environment/${environment}`],
              );
              if (!rows[0].acquired) return false;
              await control.query('SELECT pg_advisory_unlock(hashtextextended($1,0))', [
                `moria/environment/${environment}`,
              ]);
              return true;
            }, 10000);
          } finally {
            control.release();
          }
          const before = modelCalls;
          worker.child.kill('SIGKILL');
          await assert.rejects(worker.done, /SIGKILL/);
          const recovered = await native('get', { id: accepted.id });
          assert.equal(recovered.status, 'completed');
          assert.equal(
            recovered.summary.passed,
            '4',
            JSON.stringify(await diagnostics(accepted.id)),
          );
          assert.equal(recovered.summary.failed, '2');
          assert.equal(recovered.summary.errors, '4');
          assert.equal(
            (await native('invoke', { resource: suite, key: 'run-lost-commit' })).id,
            accepted.id,
          );
          assert.equal(JSON.parse(await start(['evaluations', 'once']).done).worked, false);
          assert.equal(modelCalls, before);
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.runtime_evaluation_receipts WHERE execution=$1',
                [accepted.id],
              )
            ).rows[0].count,
            '1',
          );
        } finally {
          worker.child.kill('SIGKILL');
          try {
            await proxy.close();
          } finally {
            await snapshotGate.close();
          }
          evaluationWorker = start(['evaluations', 'worker']);
        }
      },
    );
    await t.test(
      'direct Agent results retain the actual private fixture input authority',
      async () => {
        const agent = (
          await db.pool.query(
            "SELECT agent_execution FROM moria.runtime_evaluation_cases WHERE evaluation=$1 AND case_id='model' AND repeat=0",
            [first],
          )
        ).rows[0].agent_execution;
        const modelKey = (
          await db.pool.query(
            "SELECT model_key FROM moria.runtime_agent_model_calls WHERE execution=$1 AND logical_id='decision'",
            [agent],
          )
        ).rows[0].model_key;
        assert.equal(
          (
            await db.pool.query(
              'SELECT caller_execution FROM moria.native_model_calls WHERE resource=$1 AND call_key=$2',
              [model, modelKey],
            )
          ).rows[0].caller_execution,
          agent,
        );
        const before = modelCalls;
        await authority.setGrants(
          environment,
          'fixture',
          principal,
          application,
          operations.filter((operation) => operation !== `${fixture}:read`),
        );
        try {
          for (const suffix of ['', '/events?after=0']) {
            const response = await fetch(`${api}/api/v1/agent-executions/${agent}${suffix}`, {
              headers: { authorization: `Bearer ${publicToken}` },
            });
            assert.equal(response.status, 403, JSON.stringify(await response.json()));
          }
          await assert.rejects(
            start(['models', 'status'], { token: publicToken, resource: model, key: modelKey })
              .done,
            /FORBIDDEN/,
          );
          await assert.rejects(
            start(['models', 'chat'], {
              token: publicToken,
              resource: model,
              key: modelKey,
              input: { messages: [{ role: 'user', content: 'real native model' }], maxTokens: 32 },
            }).done,
            /FORBIDDEN/,
          );
          assert.equal(modelCalls, before);
        } finally {
          await authority.setGrants(environment, 'fixture', principal, application, operations);
        }
      },
    );
    await t.test('ready case publication takes its suite slot before new case work', async () => {
      await db.pool.query(
        'INSERT INTO moria.runtime_evaluation_environments(environment,application,maximum_cases,maximum_repeats,maximum_parallel) VALUES($1,$2,12,4,2) ON CONFLICT(environment) DO NOTHING',
        [environment, application],
      );
      const ownedWorkers = [
        agentWorker ? null : start(['agents', 'worker']),
        functionWorker ? null : start(['functions', 'worker']),
      ];
      const previous = evaluationWorker;
      if (previous?.child.exitCode === null) {
        previous.child.kill('SIGTERM');
        await previous.done;
      }
      let id: string | undefined;
      try {
        const accepted = await native('invoke', {
          resource: suite,
          key: 'case-publication-priority',
        });
        id = accepted.id;
        assert.equal(accepted.status, 'queued');
        const once = (operation = 'once') =>
          start(['evaluations', operation]).done.then(JSON.parse);
        assert.deepEqual(await once(), { worked: true });
        const caseRows = async () =>
          (
            await db.pool.query(
              'SELECT c.execution,c.evaluator_execution,e.status,t.status AS task FROM moria.runtime_evaluation_cases c JOIN moria.runtime_executions e ON e.id=c.execution JOIN moria.runtime_execution_tasks t ON t.execution=e.id WHERE c.evaluation=$1 ORDER BY e.created_at,e.id',
              [id],
            )
          ).rows;
        const control = start(['evaluations', 'once']);
        await until(async () => {
          assert.equal(control.child.exitCode, null, control.error());
          return Boolean((await caseRows())[0]?.evaluator_execution);
        });
        assert.deepEqual(await once('evaluator-once'), { worked: true });
        assert.deepEqual(await once('evaluator-once'), { worked: true });
        assert.deepEqual(JSON.parse(await control.done), { worked: true });
        const [sealed, pending] = await caseRows();
        assert.deepEqual(
          [sealed.status, sealed.task, pending.status, pending.task],
          ['running', 'sealed', 'queued', 'pending'],
        );
        const targetedClaim = () =>
          start(
            ['execution', 'worker-claim'],
            { domains: ['evaluation'], target: pending.execution },
            {
              MORIA_EXECUTION_WORKER_OWNER: owner,
              MORIA_EXECUTION_HOST_ID: host,
            },
          ).done.then(JSON.parse);
        assert.equal(
          await targetedClaim(),
          null,
          'A real pending claim must defer to the sealed case',
        );

        const checkAdmission = async (client: typeof db.pool | import('pg').PoolClient) =>
          (
            await client.query('SELECT moria.runtime_evaluation_case_admission($1) AS accepted', [
              pending.execution,
            ])
          ).rows[0].accepted;
        const probe = await db.pool.connect();
        try {
          await probe.query('BEGIN');
          await probe.query('SELECT 1 FROM moria.runtime_executions WHERE id=$1 FOR UPDATE', [
            sealed.execution,
          ]);
          assert.equal(
            await checkAdmission(db.pool),
            true,
            'A currently locked sibling cannot claim the freed slot',
          );
          await probe.query('ROLLBACK');
          await probe.query('BEGIN');
          await probe.query(
            'INSERT INTO moria.runtime_execution_cancellations(execution,root,requested_by) VALUES($1,$1,$2)',
            [sealed.execution, { environment, principal, application }],
          );
          assert.equal(
            await checkAdmission(probe),
            true,
            'A cancelled sibling cannot block pending work',
          );
        } finally {
          await probe.query('ROLLBACK');
          probe.release();
        }

        assert.deepEqual(await once(), { worked: true });
        const afterPublication = await caseRows();
        assert.equal(afterPublication[0].status, 'succeeded');
        assert.equal(afterPublication[1].task, 'pending');
        const next = await targetedClaim();
        assert.equal(next.lease.execution, pending.execution);
      } finally {
        try {
          if (id) assert.deepEqual(await native('cancel', { id }), { id, cancelRequested: true });
        } finally {
          if (previous) evaluationWorker = start(['evaluations', 'worker']);
          await Promise.all(
            ownedWorkers.map(async (worker) => {
              if (worker?.child.exitCode === null) {
                worker.child.kill('SIGTERM');
                await worker.done;
              }
            }),
          );
        }
      }
    });
    await t.test(
      'a Source automation dispatches a service evaluation with exact case-bound Agent and evaluator purposes',
      async () => {
        const automation = resources.get('evaluations/Automation/Nightly')!;
        const agent = resources.get('evaluations/Agent/Assistant')!;
        const serviceOps = [
          ...new Set([
            ...operations.filter(
              (operation) =>
                !operation.startsWith('filesystem:') &&
                !['agents:invoke', 'evaluations:run'].includes(operation),
            ),
            `${automation}:run`,
            `${automation}:read`,
            `${suite}:run`,
            `${suite}:read`,
            `${agent}:invoke`,
            `${agent}:read`,
          ]),
        ];
        await authority.setApplication(environment, 'fixture', application, serviceOps);
        await authority.setGrants(environment, 'fixture', principal, application, serviceOps);
        const service = randomUUID(),
          controlOwner = randomUUID();
        await db.pool.query(
          "INSERT INTO moria.runtime_service_authorities(id,environment,principal,application,operations,markings,domains,resource_classes,incarnation) SELECT $1,$2,$3,$4,$5,ARRAY['evaluations/Marking/Private'],ARRAY['maintenance','evaluation','agent','function','evaluator'],ARRAY['automation-control','evaluation-control','standard'],incarnation FROM moria.indexing_instance",
          [service, environment, principal, application, serviceOps],
        );
        await db.pool.query(
          "INSERT INTO moria.runtime_service_targets(environment,resource,domain,binding_revision,resource_class,authority) VALUES($1,$2,'maintenance','auto-v1','automation-control',$5),($1,$3,'evaluation','suite-v1','evaluation-control',$5),($1,$3,'evaluator','evaluator-v1','standard',$5),($1,$4,'agent','agent-v1','standard',$5),($1,$6,'function','tool-v1','standard',$5)",
          [environment, automation, suite, agent, service, tool],
        );
        await db.pool.query(
          "INSERT INTO moria.runtime_workers(owner,host_id,incarnation,resource_classes,domains,expires_at) SELECT $1,$2,incarnation,ARRAY['automation-control'],ARRAY['maintenance'],clock_timestamp()+interval '1 hour' FROM moria.indexing_instance",
          [controlOwner, host],
        );
        const controlEnv = {
          MORIA_AUTOMATION_WORKER_OWNER: controlOwner,
          MORIA_AUTOMATION_HOST_ID: host,
        };
        let resume: Awaited<ReturnType<typeof evaluationResumeFixture>> | undefined;
        const progressRetries = await evaluationProgressRetryFixture(db.pool, agent);
        let progressLock: Awaited<ReturnType<typeof childProgressLockFixture>> | undefined;
        try {
          progressLock = await childProgressLockFixture(db.pool, tool);
          const configuration = {
            serviceAuthority: service,
            suiteServiceRevision: 'suite-v1',
            agentServiceRevision: 'agent-v1',
            evaluatorServiceRevision: 'evaluator-v1',
          };
          await assert.rejects(
            start(['evaluations', 'install-service'], {
              environment,
              resource: suite,
              configuration: { ...configuration, agentServiceRevision: 'unregistered' },
            }).done,
            /SERVICE_PURPOSE_REVOKED/,
          );
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.runtime_evaluation_service_profiles WHERE environment=$1',
                [environment],
              )
            ).rows[0].count,
            '0',
          );
          assert.equal(
            JSON.parse(
              await start(['evaluations', 'install-service'], {
                environment,
                resource: suite,
                configuration,
              }).done,
            ).configured,
            true,
          );
          const autoConfiguration = {
            serviceAuthority: service,
            serviceBindingRevision: 'auto-v1',
            targetServiceBindingRevision: 'suite-v1',
            resourceClass: 'automation-control',
            controlTimeoutSeconds: 120,
          };
          await start(['automations', 'install'], {
            environment,
            resource: automation,
            configuration: autoConfiguration,
          }).done;
          await start(['automations', 'tick']).done;
          await start(['automations', 'once'], undefined, controlEnv).done;
          const firing = (
            await db.pool.query(
              'SELECT f.child,f.control FROM moria.runtime_source_automation_firings f JOIN moria.runtime_source_automation_schedules s ON s.id=f.schedule WHERE s.resource=$1 AND f.child IS NOT NULL',
              [automation],
            )
          ).rows[0];
          assert.ok(firing);
          assert.equal(
            (
              await db.pool.query('SELECT status FROM moria.runtime_executions WHERE id=$1', [
                firing.control,
              ])
            ).rows[0].status,
            'succeeded',
          );
          progressRetries.arm(firing.child);
          resume = await evaluationResumeFixture(db.pool, firing.child, api, publicToken);
          const [result] = await Promise.all([
            terminal(firing.child),
            resume.done,
            progressLock.done,
            progressRetries.done,
          ]);
          assert.equal(
            result.status,
            'completed',
            JSON.stringify({ result, ...(await diagnostics(firing.child)) }),
          );
          await evaluationPurposeFixture(db.pool, firing.child);
          const cases = (
            await db.pool.query(
              `SELECT c.execution,c.agent_execution,c.evaluator_execution,e.binding,t.id AS task
               FROM moria.runtime_evaluation_cases c JOIN moria.runtime_executions e ON e.id=c.execution
               JOIN moria.runtime_execution_tasks t ON t.execution=e.id
               JOIN moria.runtime_job_delegations d ON d.execution=e.id AND d.parent_execution=c.evaluation
               WHERE c.evaluation=$1`,
              [firing.child],
            )
          ).rows;
          assert.equal(cases.length, 10);
          assert.equal(new Set(cases.map((row) => row.binding)).size, 1);
          assert.equal(new Set(cases.map((row) => row.execution)).size, 10);
          assert.equal(new Set(cases.map((row) => row.task)).size, 10);
          const tools = (
            await db.pool.query(
              "SELECT f.execution,f.parent_id,d.origin_kind,d.parent_execution,e.status,b.resource,s.authority FROM moria.runtime_function_invocations f JOIN moria.runtime_executions e ON e.id=f.execution JOIN moria.runtime_job_delegations d ON d.execution=f.execution JOIN moria.runtime_execution_bindings b ON b.id=e.binding JOIN moria.runtime_execution_services s ON s.binding=e.binding JOIN moria.runtime_evaluation_cases c ON c.agent_execution=f.parent_id WHERE c.evaluation=$1 AND c.case_id='pass' ORDER BY c.repeat",
              [firing.child],
            )
          ).rows;
          assert.equal(
            tools.length,
            2,
            'Each service evaluation repeat must execute its actual Function tool',
          );
          for (const child of tools) {
            assert.equal(child.origin_kind, 'service');
            assert.equal(child.parent_execution, child.parent_id);
            assert.equal(child.status, 'succeeded');
            assert.equal(child.resource, tool);
            assert.equal(child.authority, service);
            assert.deepEqual(
              (
                await db.pool.query(
                  'SELECT markings FROM moria.runtime_function_invocations WHERE execution=$1',
                  [child.execution],
                )
              ).rows[0].markings,
              [{ environment, marking: 'evaluations/Marking/Private' }],
            );
          }
          for (const item of cases) {
            const origin = (
              await db.pool.query(
                "SELECT d.origin_kind,d.parent_execution,b.resource,b.spec->>'domain' AS domain FROM moria.runtime_job_delegations d JOIN moria.runtime_executions e ON e.id=d.execution JOIN moria.runtime_execution_bindings b ON b.id=d.origin_binding WHERE d.execution=$1",
                [item.agent_execution],
              )
            ).rows[0];
            assert.deepEqual(origin, {
              origin_kind: 'service',
              parent_execution: item.execution,
              resource: agent,
              domain: 'agent',
            });
            if (item.evaluator_execution) {
              const evaluator = (
                await db.pool.query(
                  "SELECT d.origin_kind,d.parent_execution,b.resource,b.spec->>'domain' AS domain FROM moria.runtime_job_delegations d JOIN moria.runtime_execution_bindings b ON b.id=d.origin_binding WHERE d.execution=$1",
                  [item.evaluator_execution],
                )
              ).rows[0];
              assert.deepEqual(evaluator, {
                origin_kind: 'service',
                parent_execution: item.execution,
                resource: suite,
                domain: 'evaluator',
              });
            }
          }
          await db.pool.query(
            "UPDATE moria.runtime_service_targets SET enabled=false WHERE authority=$1 AND resource=$2 AND domain='agent'",
            [service, agent],
          );
          await db.pool.query(
            "UPDATE moria.runtime_service_targets SET enabled=true WHERE authority=$1 AND resource=$2 AND domain='agent'",
            [service, agent],
          );
          await assert.rejects(native('get', { id: firing.child }), /SERVICE_PURPOSE_REVOKED/);
          await db.pool.query(
            "UPDATE moria.runtime_source_automation_schedules SET next_run=clock_timestamp()-interval '1 second' WHERE resource=$1 AND enabled",
            [automation],
          );
          await start(['automations', 'tick']).done;
          await start(['automations', 'once'], undefined, controlEnv).done;
          const refreshed = (
            await db.pool.query(
              'SELECT f.child FROM moria.runtime_source_automation_firings f JOIN moria.runtime_source_automation_schedules s ON s.id=f.schedule WHERE s.resource=$1 AND f.child IS NOT NULL ORDER BY f.created_at DESC LIMIT 1',
              [automation],
            )
          ).rows[0]?.child;
          assert.ok(refreshed);
          assert.notEqual(refreshed, firing.child);
          assert.equal((await terminal(refreshed)).status, 'completed');
          await db.pool.query(
            "UPDATE moria.runtime_service_targets SET enabled=false WHERE authority=$1 AND resource=$2 AND domain='evaluator'",
            [service, suite],
          );
          await db.pool.query(
            "UPDATE moria.runtime_service_targets SET enabled=true WHERE authority=$1 AND resource=$2 AND domain='evaluator'",
            [service, suite],
          );
          await assert.rejects(native('get', { id: refreshed }), /SERVICE_PURPOSE_REVOKED/);
          await assert.rejects(native('cases', { id: refreshed }), /SERVICE_PURPOSE_REVOKED/);
          await db.pool.query(
            'UPDATE moria.runtime_evaluation_service_profiles SET enabled=false WHERE environment=$1 AND resource=$2',
            [environment, suite],
          );
          await db.pool.query(
            'UPDATE moria.runtime_evaluation_service_profiles SET enabled=true WHERE environment=$1 AND resource=$2',
            [environment, suite],
          );
          await assert.rejects(native('get', { id: firing.child }), /EVALUATION_SERVICE_REVOKED/);
        } finally {
          await progressLock?.close();
          await progressRetries.close();
          await resume?.close();
          await db.pool.query(
            'UPDATE moria.runtime_source_automation_schedules SET enabled=false WHERE resource=$1',
            [automation],
          );
          await authority.setApplication(environment, 'fixture', application, operations);
          await authority.setGrants(environment, 'fixture', principal, application, operations);
        }
      },
    );
    await t.test('unready roots leave two control workers available for actual cases', async () => {
      await db.pool.query(
        'INSERT INTO moria.runtime_evaluation_environments(environment,application,maximum_cases,maximum_repeats,maximum_parallel) VALUES($1,$2,12,4,2) ON CONFLICT(environment) DO NOTHING',
        [environment, application],
      );
      const previousWorker = evaluationWorker;
      if (previousWorker?.child.exitCode === null) {
        previousWorker.child.kill('SIGTERM');
        await previousWorker.done;
      }
      const ids: string[] = [];
      let worker: ReturnType<typeof start> | undefined;
      try {
        for (const key of ['concurrent-first', 'concurrent-second']) {
          const accepted = await native('invoke', { resource: suite, key });
          assert.equal(accepted.status, 'queued');
          ids.push(accepted.id);
        }
        const activeWorker = start(['evaluations', 'worker'], undefined, {
          MORIA_EVALUATION_CONCURRENCY: '2',
        });
        worker = activeWorker;
        await until(async () => {
          assert.equal(activeWorker.child.exitCode, null, activeWorker.error());
          const { rows } = await db.pool.query(
            "SELECT root.status,fixture.status AS fixture_status,EXISTS(SELECT 1 FROM moria.runtime_evaluation_cases c JOIN moria.runtime_executions child ON child.id=c.execution WHERE c.evaluation=root.id AND c.outcome IN ('pending','running') AND child.status IN ('queued','running','preparing')) AS has_unfinished_cases,EXISTS(SELECT 1 FROM moria.runtime_evaluation_cases c JOIN moria.runtime_job_delegations d ON d.execution=c.agent_execution AND d.parent_execution=c.execution WHERE c.evaluation=root.id) AS accepted_case_agent FROM moria.runtime_executions root JOIN moria.runtime_execution_tasks fixture ON fixture.execution=root.id WHERE root.id=ANY($1::uuid[])",
            [ids],
          );
          assert.equal(rows.length, 2);
          for (const row of rows) {
            if (row.has_unfinished_cases) {
              assert.equal(row.status, 'running', JSON.stringify(rows));
            }
          }
          return (
            rows.every((row) => row.fixture_status === 'sealed') &&
            rows.some((row) => row.accepted_case_agent)
          );
        });
      } finally {
        try {
          for (const id of ids) {
            assert.deepEqual(await native('cancel', { id }), { id, cancelRequested: true });
          }
        } finally {
          if (worker?.child.exitCode === null) {
            worker.child.kill('SIGTERM');
            await worker.done;
          }
          if (previousWorker) evaluationWorker = start(['evaluations', 'worker']);
        }
      }
    });
    await t.test(
      'current clearance and registration protect retained case scores, summaries and events',
      async () => {
        await authority.syncPrincipal(environment, 'fixture', principal, {
          kind: 'workload',
          enabled: true,
          attributes: {},
          markings: [],
        });
        await assert.rejects(native('get', { id: first }));
        await assert.rejects(native('cases', { id: first }));
        await assert.rejects(native('events', { id: first }));
        await authority.syncPrincipal(environment, 'fixture', principal, {
          kind: 'workload',
          enabled: true,
          attributes: {},
          markings: ['evaluations/Marking/Private'],
        });
        await db.pool.query(
          'UPDATE moria.runtime_evaluation_environments SET enabled=false WHERE environment=$1',
          [environment],
        );
        await assert.rejects(native('get', { id: first }), /EVALUATION_ENVIRONMENT_REQUIRED/);
      },
    );
  },
);
