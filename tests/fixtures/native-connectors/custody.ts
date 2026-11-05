import assert from 'node:assert/strict';
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
import { FilesystemAuthority } from '../../../services/security/src/filesystem.js';
import { filesystemOperations } from '../../../packages/contracts/filesystem.js';
import type { Resource } from '../../../packages/contracts/types.js';

const program = `import pyarrow as pa,uuid,os
def sync(ctx):
 assert 'CONNECTOR_ENDPOINT' not in os.environ
 assert not any(key.startswith('CONNECTOR_MATERIAL_') for key in os.environ)
 previous=ctx.metadata['checkpoint']
 response=ctx.http('GET','/rows')
 assert response['status']==200
 assert set(response)=={'status','body'}
 body=response['body']
 assert body['previous']==previous
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

export async function connectorCustody(t: TestContext) {
  const fixture = await managedRetentionDataset();
  const { db, environment, principal, dataset, nativeEnv, operatorEnv } = fixture;
  const image = await engineImage(program).catch(async (error) => {
    await fixture.close();
    throw error;
  });
  const materials = Array.from({ length: 8 }, () => randomUUID());
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
    previous: unknown = null,
    hold = false,
    reflect = false,
    entered = false,
    socketClosed = false;
  let release = () => {};
  let held = Promise.resolve();
  const row = { id: randomUUID(), name: 'approved Connector row', tags: ['typed', 'private'] };
  const provider = createServer(async (request, response) => {
    calls++;
    try {
      assert.equal(request.method, 'GET');
      assert.equal(request.url, '/rows');
      assert.equal(request.headers.authorization, `Bearer ${materials[0]}`);
      for (let i = 1; i < materials.length; i++)
        assert.equal(request.headers[`x-credential-${i}`], materials[i]);
      assert.equal(request.headers.cookie, undefined);
      response.once('close', () => {
        if (!response.writableEnded) socketClosed = true;
      });
      if (hold) {
        entered = true;
        await held;
      }
      response.setHeader('content-type', 'application/json');
      response.setHeader('set-cookie', 'must-not-reach-guest');
      response.end(
        JSON.stringify(
          reflect
            ? { reflected: materials[0] }
            : { rows: [row], previous, cursor: { offset: calls } },
        ),
      );
    } catch (error) {
      response.destroy(error instanceof Error ? error : undefined);
    }
  });
  t.after(async () => {
    release();
    await engine?.close();
    if (engine) await engine.restoreOwnership(db.root);
    provider.closeAllConnections();
    await new Promise<void>((resolve) => provider.close(() => resolve()));
    await image.close();
    await fixture.close();
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
      capabilities: ['batchRead'],
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
  const secrets: Resource[] = materials.map((_, i) => ({
    apiVersion: 'moria/v1alpha2',
    kind: 'SecretReference',
    metadata: { namespace: 'connector', name: `Credential${i}` },
    spec: { binding: `logical-credential-${i}`, consumers: ['connector/Connection/Rows'] },
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
            { rootId: fixture.published.root.id, digest: fixture.published.publication.digest },
          ],
        }),
      },
      { path: 'main.py', text: program },
      ...[connection, ...secrets].map((resource) => ({
        path: `${resource.metadata.name}.resource.json`,
        text: JSON.stringify(resource),
      })),
    ],
    'repository',
    [implementation],
  );
  await published.client.apply(
    (await published.client.plan(published.root.id, published.publication.digest)).id,
  );
  const resource = published.resource('connector/Connection/Rows');
  const secretIds = secrets.map((secret) =>
    published.resource(`connector/SecretReference/${secret.metadata.name}`),
  );
  const operations = [
    `${resource}:read`,
    `${resource}:sync`,
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
  const token = await mint(application, [
    ...operations,
    ...filesystemOperations.map((operation) => `filesystem:${operation}`),
  ]);
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
  const credentials = secretIds.map((secret, index) => ({
    secret,
    materialRef: `CONNECTOR_MATERIAL_${index}`,
    header: index === 0 ? 'authorization' : `x-credential-${index}`,
    scheme: index === 0 ? 'bearer' : 'raw',
  }));
  await db.pool.query(
    "INSERT INTO moria.runtime_pipeline_destinations(id,environment,resource,revision,endpoint_ref,secret_ref,credentials,methods,paths,markings,allowed_markings) VALUES($1,$2,$3,'v1','CONNECTOR_ENDPOINT','CONNECTOR_MATERIAL_0',$4,ARRAY['GET'],ARRAY['/rows'],ARRAY['private'],ARRAY['private'])",
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
    fixture.command(['pipelines', 'invoke'], { token: caller, resource, key }, runtimeEnvironment);
  const inspect = await fixture.command(
    ['secret', 'inspect-connector'],
    { environment, connection: resource },
    operatorEnvironment,
  );
  assert.equal(inspect.bindings.length, 8);
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
  const approvals: { id: string }[] = [];
  const succeeded = async (execution: string) => {
    engine?.check();
    const row = (
      await db.pool.query('SELECT status,error FROM moria.runtime_executions WHERE id=$1', [
        execution,
      ])
    ).rows[0];
    if (['failed', 'cancelled'].includes(row.status)) {
      const events = (
        await db.pool.query(
          'SELECT kind,payload FROM moria.runtime_execution_events WHERE execution=$1 ORDER BY sequence DESC LIMIT 16',
          [execution],
        )
      ).rows;
      assert.fail(JSON.stringify({ execution, ...row, events }));
    }
    return row.status === 'succeeded';
  };

  await t.test(
    'every declared Secret needs exact operator custody and independent caller/service use before code',
    async () => {
      await assert.rejects(invoke('no-custody'), /SECRET_APPROVAL_REQUIRED/);
      assert.equal(calls, 0);
      for (let i = 0; i < 8; i++) approvals.push(await approve(i, `connector-approval-${i}`));
      const missingUse = await mint(
        'connector-no-use',
        operations.filter((operation) => operation !== `${secretIds[7]}:use`),
      );
      await assert.rejects(invoke('missing-use', missingUse));
      assert.equal(calls, 0);
      assert.equal(
        (await db.pool.query('SELECT count(*) FROM moria.runtime_sandbox_attempts')).rows[0].count,
        '0',
      );
      await assert.rejects(
        fixture.command(
          ['secret', 'inspect-connector'],
          { environment, connection: resource },
          runtimeEnvironment,
        ),
        /RECOVERY_OPERATOR_REQUIRED/,
      );
      const forged = structuredClone(inspect.bindings[0]);
      forged.materialRef = 'UNAPPROVED_SOURCE_BINDING';
      await assert.rejects(
        fixture.command(
          ['secret', 'approve'],
          {
            requestKey: 'forged-material',
            environment,
            binding: forged,
            materialRevision: 'v1',
            markings: ['private'],
          },
          operatorEnvironment,
        ),
        /SECRET_APPROVAL_REVOKED/,
      );
    },
  );
  let stale: { id: string }, accepted: { id: string };
  await t.test(
    'a missed approval disable/reenable pulse permanently fences an accepted window',
    async () => {
      stale = await invoke('before-approval-pulse');
      await fixture.command(['secret', 'disable'], { id: approvals[0].id }, operatorEnvironment);
      await db.pool.query('UPDATE moria.native_secret_approvals SET enabled=true WHERE id=$1', [
        approvals[0].id,
      ]);
      await assert.rejects(
        db.pool.query('SELECT moria.runtime_pipeline_current($1)', [stale.id]),
        /SECRET_APPROVAL_REVOKED/,
      );
      accepted = await invoke('after-approval-pulse');
      await db.pool.query('SELECT moria.runtime_pipeline_current($1)', [accepted.id]);
      const rows = (
        await db.pool.query(
          'SELECT execution,checkpoint,secret_approvals FROM moria.runtime_pipeline_executions WHERE execution=ANY($1::uuid[])',
          [[stale.id, accepted.id]],
        )
      ).rows;
      assert.equal(rows[0].checkpoint.configRevision, rows[1].checkpoint.configRevision);
      assert.notDeepEqual(rows[0].secret_approvals, rows[1].secret_approvals);
    },
  );
  await t.test(
    'a Secret file denial fences a queued window without stopping its worker',
    async () => {
      await fixture.command(
        ['pipelines', 'cancel'],
        { token, id: accepted.id },
        runtimeEnvironment,
      );
      const denied = await invoke('secret-file-revoked-before-claim');
      const entry = (
        await db.pool.query(
          "SELECT id FROM moria.fs_entries WHERE parent_id=$1 AND name='Credential0.resource.json' AND NOT deleted",
          [published.root.id],
        )
      ).rows[0].id;
      const security = new FilesystemAuthority(authority);
      await security.setPolicy('fixture', entry, '0', {
        markings: [{ environment, marking: 'connector-denied' }],
        grants: [],
      });
      const pool = JSON.parse(process.env.MORIA_ENGINE_SCRATCH_POOL_JSON ?? 'null');
      assert.ok(
        pool?.slots.length === 2,
        'Connector proof requires the actual dedicated ext4 pool',
      );
      engine = await startInstanceEngine({ host, runtimeEnvironment, pool });
      await until(async () => {
        engine!.check();
        return (
          (
            await db.pool.query('SELECT status FROM moria.runtime_executions WHERE id=$1', [
              denied.id,
            ])
          ).rows[0].status === 'failed'
        );
      });
      assert.equal(
        (await db.pool.query('SELECT error FROM moria.runtime_executions WHERE id=$1', [denied.id]))
          .rows[0].error,
        'FORBIDDEN',
      );
      assert.equal(
        (
          await db.pool.query(
            'SELECT count(*) FROM moria.runtime_sandbox_attempts WHERE execution=$1',
            [denied.id],
          )
        ).rows[0].count,
        '0',
      );
      assert.equal(calls, 0);
      await security.setPolicy('fixture', entry, '1', { markings: [], grants: [] });
      accepted = await invoke('after-file-restored');
    },
  );
  await t.test(
    'the actual isolated Sync guest publishes typed rows and its durable checkpoint atomically',
    async () => {
      assert.ok(engine, 'The native engine must have started');
      await until(() => succeeded(accepted.id));
      assert.equal(
        (await db.pool.query('SELECT status FROM moria.runtime_executions WHERE id=$1', [stale.id]))
          .rows[0].status,
        'failed',
      );
      assert.equal(calls, 1);
      assert.equal((await invoke('after-file-restored')).id, accepted.id);
      assert.equal(calls, 1);
      assert.deepEqual(
        (
          await db.pool.query(
            'SELECT version::text,value FROM moria.checkpoints WHERE environment=$1 AND connection=$2',
            [environment, resource],
          )
        ).rows,
        [{ version: '1', value: { offset: 1 } }],
      );
      assert.equal((await db.catalog.heads(environment, [dataset]))[0].revision, '1');
      assert.equal(
        (
          await db.pool.query('SELECT count(*) FROM moria.publications WHERE execution=$1', [
            accepted.id,
          ])
        ).rows[0].count,
        '1',
      );
      const events = await fixture.command(
        ['pipelines', 'events'],
        { token, id: accepted.id },
        runtimeEnvironment,
      );
      for (const material of materials) assert.ok(!JSON.stringify(events).includes(material));
    },
  );
  await t.test(
    'a next window reuses its semantic cursor despite a new execution and head revision',
    async () => {
      assert.ok(engine, 'The native engine must have started');
      previous = { offset: 1 };
      const next = await invoke('next-window');
      await until(() => succeeded(next.id));
      assert.equal(calls, 2);
      assert.deepEqual(
        (
          await db.pool.query(
            'SELECT version::text,value FROM moria.checkpoints WHERE environment=$1 AND connection=$2',
            [environment, resource],
          )
        ).rows,
        [{ version: '2', value: { offset: 2 } }],
      );
      assert.equal((await db.catalog.heads(environment, [dataset]))[0].revision, '2');
    },
  );
  await t.test('reflected material never reaches guest frames or observable events', async () => {
    assert.ok(engine, 'The native engine must have started');
    engine!.check();
    reflect = true;
    const reflected = await invoke('reflected-material');
    await until(
      async () =>
        (
          await db.pool.query('SELECT status FROM moria.runtime_executions WHERE id=$1', [
            reflected.id,
          ])
        ).rows[0].status === 'failed',
    );
    reflect = false;
    assert.equal((await db.catalog.heads(environment, [dataset]))[0].revision, '2');
    const events = await fixture.command(
      ['pipelines', 'events'],
      { token, id: reflected.id },
      runtimeEnvironment,
    );
    const stored = (
      await db.pool.query('SELECT payload FROM moria.runtime_execution_events WHERE execution=$1', [
        reflected.id,
      ])
    ).rows;
    for (const material of materials) {
      assert.ok(!JSON.stringify(events).includes(material));
      assert.ok(!JSON.stringify(stored).includes(material));
    }
  });
  await t.test(
    'revocation closes an in-flight Connector socket and preserves the prior head and cursor',
    async () => {
      assert.ok(engine, 'The native engine must have started');
      engine!.check();
      previous = { offset: 2 };
      hold = true;
      held = new Promise<void>((resolve) => {
        release = resolve;
      });
      const inFlight = await invoke('in-flight-revocation');
      await until(async () => entered, 10000);
      await fixture.command(['secret', 'disable'], { id: approvals[1].id }, operatorEnvironment);
      await until(async () => socketClosed, 10000);
      release();
      await until(
        async () =>
          (
            await db.pool.query('SELECT status FROM moria.runtime_executions WHERE id=$1', [
              inFlight.id,
            ])
          ).rows[0].status === 'failed',
      );
      assert.equal((await db.catalog.heads(environment, [dataset]))[0].revision, '2');
      assert.deepEqual(
        (
          await db.pool.query(
            'SELECT version::text,value FROM moria.checkpoints WHERE environment=$1 AND connection=$2',
            [environment, resource],
          )
        ).rows,
        [{ version: '2', value: { offset: 2 } }],
      );
      assert.equal(
        (
          await db.pool.query('SELECT count(*) FROM moria.publications WHERE execution=$1', [
            inFlight.id,
          ])
        ).rows[0].count,
        '0',
      );
    },
  );
}
