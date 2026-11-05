import assert from 'node:assert/strict';
import { spawn, execFile, type ChildProcess } from 'node:child_process';
import { promisify } from 'node:util';
import { pipelinesClient } from '../../../packages/platform/src/pipelines.js';
import { createTransport } from '../../../packages/platform/src/transport.js';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import type { TestContext } from 'node:test';
import { managedRetentionDataset } from '../native-retention/dataset.js';
import { publishNativeSource } from '../native-source/setup.js';
import { engineImage } from '../data-engine/image.js';
import { startInstanceEngine } from '../native-instance/engine.js';
import { Authority } from '../../../services/security/src/authority.js';
import { WorkloadSessions } from '../../../services/security/src/sessions.js';
import { transactionCommitProxy } from '../runtime/postgres-proxy.js';
import { FilesystemAuthority } from '../../../services/security/src/filesystem.js';
import type { Resource } from '../../../packages/contracts/types.js';

const program = `import pyarrow as pa,uuid,os
from protocol import json_request
def discover(ctx):
 assert ctx.metadata['capability']=='schemaDiscovery'
 assert ctx.metadata['stream']=='rows'
 assert 'CONNECTOR_ENDPOINT' not in os.environ
 assert not any(key.startswith('CONNECTOR_MATERIAL_') for key in os.environ)
 response=ctx.http('GET','/rows')
 assert response['status']==200 and set(response)=={'status','body'}
 body=response['body']
 probe=body.get('_probe')
 if probe=='write': ctx.write('retention/Dataset/records',pa.table({'id':[]}))
 if probe=='checkpoint': ctx.checkpoint({'forbidden':True})
 if probe=='post': ctx.http('POST','/rows',{})
 if probe=='wrongStream': json_request('/api/v1/engine/connector/schema','POST',{'version':1,'stream':'guest-selected','columns':body['columns'],'primaryKey':body['primaryKey']})
 return {'columns':body['columns'],'primaryKey':body['primaryKey']}
def sync(ctx):
 body=ctx.http('GET','/rows')['body']
 rows=body['rows']
 ctx.write('retention/Dataset/records',pa.table({'id':pa.array([uuid.UUID(row['id']).bytes for row in rows],type=pa.uuid()),'name':pa.array([row['name'] for row in rows],type=pa.string()),'tags':pa.array([row['tags'] for row in rows],type=pa.list_(pa.string()))}))
 ctx.checkpoint(body['cursor'])
`;

async function until(check: () => Promise<boolean>, timeout = 120000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    if (await check()) return;
    await delay(100);
  }
  assert.fail('Native Connector lifecycle barrier was not reached');
}

export async function connectorDiscovery(t: TestContext) {
  const fixture = await managedRetentionDataset();
  const { db, environment, principal, dataset, nativeEnv, operatorEnv } = fixture;
  const image = await engineImage(program).catch(async (error) => {
    await fixture.close();
    throw error;
  });
  const materials = Array.from({ length: 1 }, () => randomUUID());
  const owner = randomUUID(),
    host = randomUUID(),
    service = randomUUID(),
    destination = randomUUID();
  const application = 'connector-client',
    serviceApplication = 'connector-service';
  const authority = new Authority(db.catalog),
    sessions = new WorkloadSessions(authority);
  let engine: Awaited<ReturnType<typeof startInstanceEngine>> | undefined;
  let calls = 0,
    hold = false,
    entered = false,
    socketClosed = false;
  let phase: 'schema' | 'rows' = 'schema';
  let probe:
    | 'write'
    | 'checkpoint'
    | 'post'
    | 'wrongStream'
    | 'invalidType'
    | 'oversized'
    | 'reflection'
    | undefined;
  let api: { url: string; child: ChildProcess; exited: Promise<void> } | undefined;
  let release = () => {};
  let held = Promise.resolve();
  const row = {
    id: randomUUID(),
    name: 'approved Connector row',
    tags: ['typed', 'private'],
  };
  const proposal = {
    version: 1,
    stream: 'rows',
    columns: {
      id: { type: 'uid', nullable: false },
      discoveredOnly: { type: 'string', nullable: true },
      tags: { type: 'array', nullable: true, items: { type: 'string', nullable: false } },
    },
    primaryKey: ['id'],
  };
  const provider = createServer(async (request, response) => {
    calls++;
    try {
      assert.equal(request.method, 'GET');
      assert.equal(request.url, '/rows');
      assert.equal(request.headers.authorization, `Bearer ${materials[0]}`);
      response.once('close', () => {
        if (!response.writableEnded) socketClosed = true;
      });
      if (hold) {
        entered = true;
        await held;
      }
      response.setHeader('content-type', 'application/json');
      response.setHeader('set-cookie', 'must-not-reach-guest');
      if (phase === 'rows') {
        response.end(JSON.stringify({ rows: [row], cursor: { offset: 1 } }));
        return;
      }
      const value: any = structuredClone(proposal);
      value._probe = probe;
      if (probe === 'invalidType') value.columns.id.type = 'untyped';
      if (probe === 'oversized') value.columns.id.extra = 'x'.repeat(40000);
      if (probe === 'reflection')
        value.columns[`field_${materials[0]}`] = { type: 'string', nullable: false };
      response.end(JSON.stringify(value));
    } catch (error) {
      response.destroy(error instanceof Error ? error : undefined);
    }
  });
  t.after(async () => {
    const errors: unknown[] = [];
    const cleanup = async (action: () => Promise<unknown>) => {
      try {
        await action();
      } catch (error) {
        errors.push(error);
      }
    };
    release();
    if (engine) {
      await cleanup(engine.close);
      await cleanup(() => engine!.restoreOwnership(db.root));
    }
    provider.closeAllConnections();
    await cleanup(() => new Promise<void>((resolve) => provider.close(() => resolve())));
    if (api) {
      api.child.kill('SIGTERM');
      await Promise.race([api.exited, delay(2000)]);
      if (api.child.exitCode === null && api.child.signalCode === null) api.child.kill('SIGKILL');
      await cleanup(() => api!.exited);
    }
    await cleanup(image.close);
    await cleanup(fixture.close);
    assert.deepEqual(errors, [], 'Every owned Connector fixture resource must close');
  });
  await new Promise<void>((resolve, reject) => {
    provider.once('error', reject);
    provider.listen(0, '127.0.0.1', resolve);
  });
  const implementation: Resource = {
    apiVersion: 'moria/v1alpha2',
    kind: 'ConnectorImplementation',
    metadata: { namespace: 'connector', name: 'Implementation' },
    spec: {
      artifact: image.artifact,
      abiVersion: 'connector/v1',
      capabilities: ['schemaDiscovery', 'batchRead'],
      configurationSchema: { type: 'object', additionalProperties: false },
    },
  };
  const connection: Resource = {
    apiVersion: 'moria/v1alpha2',
    kind: 'Connection',
    metadata: { namespace: 'connector', name: 'Rows' },
    spec: {
      implementation: 'connector/ConnectorImplementation/Implementation',
      configuration: {},
      secrets: materials.map((_, i) => `connector/SecretReference/Credential${i}`),
      output: 'retention/Dataset/records',
      mode: 'replace',
      coverage: 'full',
      stream: 'rows',
    },
  };
  const batchImplementation: Resource = {
    ...implementation,
    metadata: { namespace: 'connector', name: 'BatchOnly' },
    spec: { ...implementation.spec, capabilities: ['batchRead'] },
  };
  const batchConnection: Resource = {
    ...connection,
    metadata: { namespace: 'connector', name: 'BatchOnly' },
    spec: { ...connection.spec, implementation: 'connector/ConnectorImplementation/BatchOnly' },
  };
  const secrets: Resource[] = materials.map((_, i) => ({
    apiVersion: 'moria/v1alpha2',
    kind: 'SecretReference',
    metadata: { namespace: 'connector', name: `Credential${i}` },
    spec: {
      binding: `logical-credential-${i}`,
      consumers: ['connector/Connection/Rows', 'connector/Connection/BatchOnly'],
    },
  }));
  const published = await publishNativeSource(
    fixture.baseUrl,
    fixture.token,
    [
      {
        path: '.repository.json',
        text: JSON.stringify({
          version: 1,
          name: 'Connector custody',
          namespace: 'connector',
          runtime: 'python-duckdb-v1',
          dependencies: [
            {
              rootId: fixture.published.root.id,
              digest: fixture.published.publication.digest,
            },
          ],
        }),
      },
      { path: 'main.py', text: program },
      ...[connection, batchConnection, ...secrets].map((resource) => ({
        path: `${resource.metadata.name}.resource.json`,
        text: JSON.stringify(resource),
      })),
    ],
    'repository',
    [implementation, batchImplementation],
  );
  await published.client.apply(
    (await published.client.plan(published.root.id, published.publication.digest)).id,
  );
  const resource = published.resource('connector/Connection/Rows');
  const secretIds = secrets.map((secret) =>
    published.resource(`connector/SecretReference/${secret.metadata.name}`),
  );
  const batchResource = published.resource('connector/Connection/BatchOnly');
  const batchImplementationResource = published.resource(
    'connector/ConnectorImplementation/BatchOnly',
  );
  const operations = [
    `${resource}:read`,
    `${resource}:sync`,
    `${batchResource}:read`,
    `${batchResource}:sync`,
    `${batchImplementationResource}:read`,
    `${published.resource('connector/ConnectorImplementation/Implementation')}:read`,
    `${dataset}:read`,
    `${dataset}:write`,
    ...secretIds.map((secret) => `${secret}:use`),
  ];
  const mint = async (app: string, granted: string[]) => {
    await authority.setApplication(environment, 'fixture', app, granted);
    await authority.setGrants(environment, 'fixture', principal, app, granted);
    return (await sessions.mint({ environment, principal, application: app }, 'fixture')).token;
  };
  const discoveryOperations = operations.filter(
    (operation) =>
      !operation.startsWith(`${dataset}:`) &&
      !operation.startsWith(`${batchResource}:`) &&
      !operation.startsWith(`${batchImplementationResource}:`),
  );
  const token = await mint(application, discoveryOperations);
  const writerToken = await mint('connector-batch-writer', operations);
  const batchToken = await mint('connector-batch-only', operations);
  await mint(serviceApplication, operations);
  await db.pool.query(
    "INSERT INTO moria.runtime_resource_classes(name,maximum_tasks,maximum_environment_tasks) VALUES('engine-spill',2,2)",
  );
  await db.pool.query(
    "INSERT INTO moria.runtime_workers(owner,host_id,incarnation,resource_classes,domains,expires_at) SELECT $1,$2,incarnation,ARRAY['engine-spill'],ARRAY['sync'],clock_timestamp()+interval '1 hour' FROM moria.indexing_instance",
    [owner, host],
  );
  await db.pool.query(
    "INSERT INTO moria.runtime_service_authorities(id,environment,principal,application,operations,markings,domains,resource_classes,incarnation) SELECT $1,$2,$3,$4,$5,ARRAY['private'],ARRAY['sync'],ARRAY['engine-spill'],incarnation FROM moria.indexing_instance",
    [service, environment, principal, serviceApplication, operations],
  );
  await db.pool.query(
    "INSERT INTO moria.runtime_service_targets(environment,resource,domain,binding_revision,resource_class,authority) VALUES($1,$2,'sync','v1','engine-spill',$3)",
    [environment, resource, service],
  );
  await db.pool.query(
    "INSERT INTO moria.runtime_service_targets(environment,resource,domain,binding_revision,resource_class,authority) VALUES($1,$2,'sync','v1','engine-spill',$3)",
    [environment, batchResource, service],
  );
  const credentials = secretIds.map((secret, index) => ({
    secret,
    materialRef: `CONNECTOR_MATERIAL_${index}`,
    header: index === 0 ? 'authorization' : `x-credential-${index}`,
    scheme: index === 0 ? 'bearer' : 'raw',
  }));
  await db.pool.query(
    "INSERT INTO moria.runtime_pipeline_destinations(id,environment,resource,revision,endpoint_ref,secret_ref,credentials,methods,paths,markings,allowed_markings) VALUES($1,$2,$3,'v1','CONNECTOR_ENDPOINT','CONNECTOR_MATERIAL_0',$4,ARRAY['GET','POST'],ARRAY['/rows'],ARRAY['private'],ARRAY['private'])",
    [destination, environment, resource, JSON.stringify(credentials)],
  );
  const physical = (
    await db.pool.query(
      'SELECT iceberg_schema FROM moria.native_dataset_bindings WHERE environment=$1 AND resource=$2 AND enabled',
      [environment, dataset],
    )
  ).rows[0];
  const profile = {
    serviceAuthority: service,
    serviceBindingRevision: 'v1',
    resourceClass: 'engine-spill',
    hardSpill: true,
    adapter: image.adapter,
    outputs: {
      'retention/Dataset/records': {
        namespace: fixture.namespace,
        schema: physical.iceberg_schema,
      },
    },
    limits: {
      timeoutSeconds: 120,
      memoryMiB: 512,
      cpuMillis: 60000,
      inputBytes: 134217728,
      outputBytes: 134217728,
    },
    connector: destination,
  };
  await db.pool.query(
    "INSERT INTO moria.runtime_pipeline_profiles(environment,resource,revision,domain,service_authority,service_revision,resource_class,profile) VALUES($1,$2,'v1','sync',$3,'v1','engine-spill',$4)",
    [environment, resource, service, profile],
  );
  await db.pool.query(
    "INSERT INTO moria.runtime_pipeline_profiles(environment,resource,revision,domain,service_authority,service_revision,resource_class,profile) VALUES($1,$2,'v1','sync',$3,'v1','engine-spill',$4)",
    [environment, batchResource, service, profile],
  );
  const environmentValues = {
    CONNECTOR_ENDPOINT: `http://127.0.0.1:${(provider.address() as { port: number }).port}/`,
    ...Object.fromEntries(
      materials.map((material, index) => [`CONNECTOR_MATERIAL_${index}`, material]),
    ),
  };
  const runtimeEnvironment = {
    ...nativeEnv,
    ...environmentValues,
    MORIA_PIPELINE_WORKER_OWNER: owner,
    MORIA_PIPELINE_HOST_ID: host,
  };
  const operatorEnvironment = { ...operatorEnv, ...environmentValues };
  const invoke = (key: string, caller = token) =>
    fixture.command(
      ['pipelines', 'discover'],
      { token: caller, resource, key },
      runtimeEnvironment,
    );
  const inspect = await fixture.command(
    ['secret', 'inspect-connector'],
    { environment, connection: resource },
    operatorEnvironment,
  );
  assert.equal(inspect.bindings.length, 1);
  const approve = async (index: number, requestKey: string) =>
    fixture.command(
      ['secret', 'approve'],
      {
        requestKey,
        environment,
        binding: inspect.bindings[index],
        materialRevision: `material-${index}-v1`,
        markings: ['private'],
      },
      operatorEnvironment,
    );
  const terminal = async (id: string) => {
    let row: any;
    await until(async () => {
      engine?.check();
      row = (
        await db.pool.query(
          'SELECT status,error,result FROM moria.runtime_executions WHERE id=$1',
          [id],
        )
      ).rows[0];
      return ['succeeded', 'failed', 'cancelled'].includes(row.status);
    });
    return row;
  };
  const snapshot = async () => ({
    heads: (
      await db.pool.query(
        'SELECT to_jsonb(h) value FROM moria.dataset_heads h WHERE environment=$1 AND dataset=$2',
        [environment, dataset],
      )
    ).rows,
    binding: (
      await db.pool.query(
        'SELECT to_jsonb(b) value FROM moria.native_dataset_bindings b WHERE environment=$1 AND resource=$2',
        [environment, dataset],
      )
    ).rows,
    checkpoint: (
      await db.pool.query(
        'SELECT to_jsonb(c) value FROM moria.checkpoints c WHERE environment=$1 AND connection=$2',
        [environment, resource],
      )
    ).rows,
    publications: (
      await db.pool.query('SELECT count(*) FROM moria.publications WHERE environment=$1', [
        environment,
      ])
    ).rows,
  });
  const startApi = async () => {
    const child = spawn(fixture.runtime, ['serve'], {
      env: { ...runtimeEnvironment, MORIA_NATIVE_LISTEN: '127.0.0.1:0' },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    const exited = new Promise<void>((resolve) => child.once('exit', () => resolve()));
    let output = '',
      error = '';
    child.stdout.on('data', (part) => {
      output = (output + part).slice(-65536);
    });
    child.stderr.on('data', (part) => {
      error = (error + part).slice(-65536);
    });
    for (let n = 0; n < 500; n++) {
      for (const line of output.split('\n')) {
        try {
          const value = JSON.parse(line);
          if (value.event === 'serving') {
            api = { url: 'http://' + value.address, child, exited };
            return;
          }
        } catch {}
      }
      if (child.exitCode !== null) throw new Error(error);
      await delay(10);
    }
    child.kill('SIGKILL');
    await exited;
    throw new Error('Native discovery API startup deadline');
  };
  const before = await snapshot();
  let approval: { id: string };
  await t.test(
    'discovery requires actual declared capability and independent current Secret use before code',
    async () => {
      await assert.rejects(invoke('without-custody'), /SECRET_APPROVAL_REQUIRED/);
      approval = await approve(0, 'discovery-approval');
      const missing = await mint(
        'discovery-without-use',
        discoveryOperations.filter((operation) => operation !== `${secretIds[0]}:use`),
      );
      await assert.rejects(invoke('without-use', missing));
      await assert.rejects(
        fixture.command(
          ['pipelines', 'discover'],
          { token: batchToken, resource: batchResource, key: 'not-declared' },
          runtimeEnvironment,
        ),
        /PIPELINE_CAPABILITY_UNSUPPORTED/,
      );
      await assert.rejects(
        fixture.command(
          ['secret', 'inspect-connector'],
          { environment, connection: resource },
          runtimeEnvironment,
        ),
        /RECOVERY_OPERATOR_REQUIRED/,
      );
      assert.equal(calls, 0);
      assert.equal(
        (await db.pool.query('SELECT count(*) FROM moria.runtime_sandbox_attempts')).rows[0].count,
        '0',
      );
    },
  );
  await t.test('current Secret file ACL prevents discovery before guest execution', async () => {
    const accepted = await invoke('file-denied');
    const entry = (
      await db.pool.query(
        "SELECT id FROM moria.fs_entries WHERE parent_id=$1 AND name='Credential0.resource.json' AND NOT deleted",
        [published.root.id],
      )
    ).rows[0].id;
    const identity = (
      await db.pool.query(
        'SELECT instance_id FROM moria.instance_principal_bindings WHERE environment=$1 AND principal=$2',
        [environment, principal],
      )
    ).rows[0].instance_id;
    const security = new FilesystemAuthority(authority);
    await security.setPolicy('fixture', entry, '0', {
      markings: [],
      grants: [
        {
          subject: { kind: 'principal', id: identity },
          operations: ['read'],
          inherit: false,
          effect: 'deny',
        },
      ],
    });
    const pool = JSON.parse(process.env.MORIA_CONNECTOR_SCRATCH_POOL_JSON ?? 'null');
    assert.equal(pool?.slots.length, 2);
    engine = await startInstanceEngine({ host, runtimeEnvironment, pool });
    assert.equal((await terminal(accepted.id)).status, 'failed');
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.runtime_sandbox_attempts WHERE execution=$1',
          [accepted.id],
        )
      ).rows[0].count,
      '0',
    );
    assert.equal(calls, 0);
    await security.setPolicy('fixture', entry, '1', { markings: [], grants: [] });
  });
  await t.test(
    'SDK and CLI return typed proposed metadata without Dataset write scope or target mutation',
    async () => {
      await startApi();
      const client = pipelinesClient(
        createTransport({
          baseUrl: api!.url,
          fetch: (input, init) => {
            const headers = new Headers(init?.headers);
            headers.set('authorization', `Bearer ${token}`);
            return fetch(input, { ...init, headers });
          },
        }),
      );
      const accepted = await client.discover(resource, {
        idempotencyKey: 'schema-only',
        wait: false,
      });
      assert.equal((await terminal(accepted.id)).status, 'succeeded');
      const result = await client.get(accepted.id);
      assert.deepEqual(result.result, { schemaDiscovery: proposal });
      const replay = await promisify(execFile)(
        process.execPath,
        [
          '--import',
          'tsx',
          'apps/cli/src/main.ts',
          'pipelines',
          'discover',
          resource,
          '--key',
          'schema-only',
          '--async',
          '--url',
          api!.url,
        ],
        { env: { PATH: process.env.PATH, MORIA_API_TOKEN: token } },
      );
      assert.equal(JSON.parse(replay.stdout).id, accepted.id);
      assert.equal(calls, 1);
      const row = (
        await db.pool.query(
          'SELECT b.operations,s.effect,p.capability,p.inputs,p.outputs,p.checkpoint FROM moria.runtime_executions e JOIN moria.runtime_execution_bindings b ON b.id=e.binding JOIN moria.runtime_sandbox_bindings s ON s.binding=e.binding JOIN moria.runtime_pipeline_executions p ON p.execution=e.id WHERE e.id=$1',
          [accepted.id],
        )
      ).rows[0];
      assert.equal(row.effect, 'readOnly');
      assert.equal(row.capability, 'schemaDiscovery');
      assert.deepEqual(row.inputs, {});
      assert.deepEqual(row.outputs, {});
      assert.equal(row.checkpoint, null);
      assert.ok(!row.operations.some((operation: string) => operation.startsWith(`${dataset}:`)));
      await assert.rejects(
        fixture.command(
          ['pipelines', 'invoke'],
          { token, resource, key: 'no-dataset-write' },
          runtimeEnvironment,
        ),
        /FORBIDDEN/,
      );
      for (const body of [
        { capability: 'writeback' },
        { capability: 'schemaDiscovery', stream: 'guest-selected' },
      ]) {
        const response = await fetch(api!.url + `/api/v1/pipelines/${resource}/executions`, {
          method: 'POST',
          headers: {
            authorization: `Bearer ${token}`,
            'content-type': 'application/json',
            'idempotency-key': randomUUID(),
          },
          body: JSON.stringify(body),
        });
        assert.equal(response.status, 400);
      }
      assert.equal(calls, 1);
      assert.deepEqual(await snapshot(), before);
      const events = await client.events(accepted.id);
      assert.ok(!JSON.stringify(events).includes(materials[0]));
    },
  );
  await t.test(
    'cancellation closes an in-flight read-only discovery without a second attempt',
    async () => {
      hold = true;
      entered = false;
      socketClosed = false;
      held = new Promise<void>((resolve) => (release = resolve));
      const accepted = await invoke('cancel-discovery');
      try {
        await until(async () => entered);
        await fixture.command(
          ['pipelines', 'cancel'],
          { token, id: accepted.id },
          runtimeEnvironment,
        );
        await until(async () => socketClosed, 10000);
      } finally {
        hold = false;
        release();
      }
      assert.equal((await terminal(accepted.id)).status, 'cancelled');
      assert.deepEqual(
        (
          await db.pool.query(
            'SELECT attempts FROM moria.runtime_execution_tasks WHERE execution=$1',
            [accepted.id],
          )
        ).rows,
        [{ attempts: 1 }],
      );
      assert.equal(
        (
          await db.pool.query(
            'SELECT count(*) FROM moria.runtime_pipeline_discoveries WHERE execution=$1',
            [accepted.id],
          )
        ).rows[0].count,
        '0',
      );
      assert.deepEqual(await snapshot(), before);
    },
  );
  for (const fault of [
    'write',
    'checkpoint',
    'post',
    'wrongStream',
    'invalidType',
    'oversized',
    'reflection',
  ] as const) {
    await t.test(
      `native discovery denies ${fault} without publishing a proposal or Dataset state`,
      async () => {
        probe = fault;
        const priorCalls = calls;
        try {
          const accepted = await invoke('deny-' + fault);
          assert.equal((await terminal(accepted.id)).status, 'failed');
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.runtime_pipeline_discoveries WHERE execution=$1',
                [accepted.id],
              )
            ).rows[0].count,
            '0',
          );
          assert.deepEqual(await snapshot(), before);
          if (fault === 'post')
            assert.equal(
              calls,
              priorCalls + 1,
              'Discovery narrows an operator profile that permits POST',
            );
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.runtime_pipeline_outputs WHERE execution=$1',
                [accepted.id],
              )
            ).rows[0].count,
            '0',
          );
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.runtime_pipeline_checkpoints WHERE execution=$1',
                [accepted.id],
              )
            ).rows[0].count,
            '0',
          );
          const events = (
            await db.pool.query(
              'SELECT kind,payload FROM moria.runtime_execution_events WHERE execution=$1',
              [accepted.id],
            )
          ).rows;
          assert.ok(!JSON.stringify(events).includes(materials[0]));
        } finally {
          probe = undefined;
        }
      },
    );
  }
  await t.test(
    'in-flight Secret revocation closes discovery and a later enable cannot revive it',
    async () => {
      hold = true;
      entered = false;
      socketClosed = false;
      held = new Promise<void>((resolve) => (release = resolve));
      const accepted = await invoke('revoke-inflight');
      try {
        await until(async () => entered);
        await fixture.command(['secret', 'disable'], { id: approval!.id }, operatorEnvironment);
        await db.pool.query('UPDATE moria.native_secret_approvals SET enabled=true WHERE id=$1', [
          approval!.id,
        ]);
        await until(async () => socketClosed, 10000);
      } finally {
        hold = false;
        release();
      }
      assert.equal((await terminal(accepted.id)).status, 'failed');
      assert.deepEqual(await snapshot(), before);
      const next = await invoke('after-approval-pulse');
      assert.equal((await terminal(next.id)).status, 'succeeded');
      assert.deepEqual(await snapshot(), before);
    },
  );
  await t.test(
    'ordinary batch Sync still uses its original typed Dataset and checkpoint publication',
    async () => {
      const discovery = await fixture.command(
        ['pipelines', 'discover'],
        { token: writerToken, resource, key: 'original-batch' },
        runtimeEnvironment,
      );
      assert.equal((await terminal(discovery.id)).status, 'succeeded');
      assert.deepEqual(await snapshot(), before);
      phase = 'rows';
      const accepted = await fixture.command(
        ['pipelines', 'invoke'],
        { token: writerToken, resource, key: 'original-batch' },
        runtimeEnvironment,
      );
      assert.notEqual(
        accepted.id,
        discovery.id,
        'Capability identity separates the same actor/resource/key',
      );
      const result = await terminal(accepted.id);
      assert.equal(result.status, 'succeeded', JSON.stringify(result));
      assert.equal((await db.catalog.heads(environment, [dataset]))[0].revision, '1');
      assert.deepEqual(
        (
          await db.pool.query(
            'SELECT version::text,value FROM moria.checkpoints WHERE environment=$1 AND connection=$2',
            [environment, resource],
          )
        ).rows,
        [{ version: '1', value: { offset: 1 } }],
      );
      const head = (await db.catalog.heads(environment, [dataset]))[0];
      await fixture.read(head, true, [row]);
    },
  );
  await t.test(
    'lost discovery COMMIT acknowledgement retains one exact report and replay cannot restart code',
    async () => {
      phase = 'schema';
      const prior = await snapshot();
      const priorCalls = calls;
      await engine!.close();
      await engine!.restoreOwnership(db.root);
      engine = undefined;
      const url = new URL(nativeEnv.RETENTION_DATABASE as string);
      const proxy = await transactionCommitProxy(Number(url.port));
      url.port = String(proxy.port);
      try {
        const pool = JSON.parse(process.env.MORIA_NATIVE_INSTANCE_SCRATCH_POOL_JSON ?? 'null');
        engine = await startInstanceEngine({
          host,
          runtimeEnvironment: { ...runtimeEnvironment, RETENTION_DATABASE: url.href },
          pool,
        });
        const accepted = await invoke('lost-discovery-ack');
        await until(async () => {
          engine!.check();
          const row = (
            await db.pool.query('SELECT status,error FROM moria.runtime_executions WHERE id=$1', [
              accepted.id,
            ])
          ).rows[0];
          assert.notEqual(row.status, 'failed', JSON.stringify(row));
          return proxy.checkpoints.commitResponses === 1;
        });
        await proxy.committed;
        assert.equal(
          (
            await db.pool.query('SELECT status FROM moria.runtime_executions WHERE id=$1', [
              accepted.id,
            ])
          ).rows[0].status,
          'succeeded',
        );
        await engine!.close();
        await engine!.restoreOwnership(db.root);
        engine = undefined;
        const replay = await invoke('lost-discovery-ack');
        assert.equal(replay.id, accepted.id);
        assert.deepEqual(replay.result, { schemaDiscovery: proposal });
        assert.equal(calls, priorCalls + 1);
        assert.deepEqual(
          (
            await db.pool.query(
              'SELECT attempts FROM moria.runtime_execution_tasks WHERE execution=$1',
              [accepted.id],
            )
          ).rows,
          [{ attempts: 1 }],
        );
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.runtime_pipeline_discoveries WHERE execution=$1',
              [accepted.id],
            )
          ).rows[0].count,
          '1',
        );
        assert.deepEqual(await snapshot(), prior);
        await fixture.command(['secret', 'disable'], { id: approval!.id }, operatorEnvironment);
        await assert.rejects(
          fixture.command(['pipelines', 'get'], { token, id: accepted.id }, runtimeEnvironment),
          /SECRET_APPROVAL_REQUIRED/,
        );
        approval = await approve(0, 'discovery-approval-after-completion');
        await assert.rejects(
          fixture.command(['pipelines', 'get'], { token, id: accepted.id }, runtimeEnvironment),
          /SECRET_APPROVAL_REVOKED/,
        );
      } finally {
        await proxy.close();
      }
    },
  );
}
