import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { Client } from 'pg';
import type { Resource } from '../packages/contracts/types.js';
import { filesystemClient } from '../packages/platform/src/filesystem.js';
import { createTransport } from '../packages/platform/src/transport.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';
import { publishNativeSource } from './fixtures/native-source/setup.js';

test(
  'native Action writeback commits one guarded HTTP intent and reconciles uncertain sends',
  { timeout: 180000 },
  async (t) => {
    const fixture = await managedRetentionDataset();
    t.after(fixture.close);
    const { db, environment, principal, cell, dataset, nativeEnv, operatorEnv } = fixture;
    const authority = new Authority(db.catalog),
      sessions = new WorkloadSessions(authority);
    const destination = randomUUID(),
      connectorDestination = randomUUID(),
      service = randomUUID();
    const material = randomUUID();
    let mode: 'normal' | 'hold' | 'reject' | 'reflect' = 'normal';
    const observed: { id: string; fence: string; hash: string; body: string }[] = [];
    const effects = new Set<string>(),
      errors: unknown[] = [];
    const provider = createServer(async (request, response) => {
      try {
        assert.equal(request.method, 'POST');
        assert.equal(request.url, '/writeback');
        assert.equal(request.headers['x-fixture-material'], material);
        const id = String(request.headers['moria-delivery-id']);
        assert.equal(request.headers['idempotency-key'], id);
        const fence = String(request.headers['moria-delivery-attempt']),
          hash = String(request.headers['moria-payload-sha256']);
        const chunks: Buffer[] = [];
        for await (const chunk of request) chunks.push(Buffer.from(chunk));
        const body = Buffer.concat(chunks).toString();
        assert.equal(createHash('sha256').update(body).digest('hex'), hash);
        assert.ok(!body.includes(material));
        const sent = (
          await db.pool.query(
            'SELECT sent_at IS NOT NULL AS sent FROM moria.runtime_delivery_attempts WHERE delivery=$1 AND fence=$2',
            [id, fence],
          )
        ).rows;
        assert.deepEqual(sent, [{ sent: true }]);
        observed.push({ id, fence, hash, body });
        if (mode === 'reject') {
          response.writeHead(503).end();
          return;
        }
        effects.add(id);
        if (mode === 'hold') return;
        const receipt = JSON.stringify({
          version: 1,
          deliveryId: id,
          payloadHash: hash,
          receiptId: mode === 'reflect' ? material : 'provider:' + id,
        });
        response.setHeader('content-type', 'application/json');
        response.end(
          mode === 'reflect'
            ? receipt.replace(
                material,
                String.fromCharCode(92) +
                  'u' +
                  material.charCodeAt(0).toString(16).padStart(4, '0') +
                  material.slice(1),
              )
            : receipt,
        );
      } catch (error) {
        errors.push(error);
        response.destroy();
      }
    });
    t.after(async () => {
      provider.closeAllConnections();
      await new Promise<void>((done) => provider.close(() => done()));
    });
    await new Promise<void>((resolve, reject) => {
      provider.once('error', reject);
      provider.listen(0, '127.0.0.1', resolve);
    });
    const endpoint = `http://127.0.0.1:${(provider.address() as { port: number }).port}/writeback`;
    const connectorEnv = { WRITEBACK_ENDPOINT: endpoint, MORIA_DELIVERY_ALLOW_LOOPBACK_HTTP: '1' };
    const operator = { ...operatorEnv, ...connectorEnv };
    const worker = { ...nativeEnv, ...connectorEnv, WRITEBACK_MATERIAL: material };
    const source = (path: string, content: unknown) => ({ path, text: JSON.stringify(content) });
    const references = [
      { rootId: fixture.published.root.id, digest: fixture.published.publication.digest },
    ];
    const uid = { type: 'uid', nullable: false },
      name = { type: 'string', nullable: false };
    const resource = (kind: string, name: string, spec: unknown) => ({
      apiVersion: 'moria/v1alpha2',
      kind,
      metadata: { namespace: 'writeback', name },
      spec,
    });
    const actions = await publishNativeSource(fixture.baseUrl, fixture.token, [
      source('.ontology.json', {
        version: 1,
        name: 'Writeback Action',
        namespace: 'writeback',
        dependencies: references,
      }),
      source(
        'record.object.json',
        resource('ObjectType', 'Record', {
          primaryKey: 'uid',
          properties: { uid, name },
          source: { resource: 'retention/Dataset/records', mapping: { uid: 'id', name: 'name' } },
        }),
      ),
      source(
        'create.action.json',
        resource('Action', 'Create', {
          inputs: { name, key: name },
          targetType: 'writeback/ObjectType/Record',
          condition: { literal: true },
          edits: [
            {
              op: 'create',
              targetType: 'writeback/ObjectType/Record',
              identityKey: { parameter: 'key' },
              values: { name: { parameter: 'name' } },
            },
          ],
        }),
      ),
    ]);
    const object = actions.resource('writeback/ObjectType/Record'),
      action = actions.resource('writeback/Action/Create');
    const connector = await publishNativeSource(
      fixture.baseUrl,
      fixture.token,
      [
        source('.repository.json', {
          version: 1,
          name: 'Writeback Connector',
          namespace: 'writeback',
          runtime: 'python-duckdb-v1',
          dependencies: references,
        }),
        {
          path: 'main.py',
          text: 'raise RuntimeError("http-outbox-v1 must never start guest code")',
        },
        source(
          'outbox.resource.json',
          resource('Connection', 'Outbox', {
            implementation: 'writeback/ConnectorImplementation/Native',
            configuration: {},
            secrets: ['writeback/SecretReference/Credential'],
            output: 'retention/Dataset/records',
            mode: 'replace',
            coverage: 'full',
            stream: 'events',
          }),
        ),
        source(
          'secret.resource.json',
          resource('SecretReference', 'Credential', {
            binding: 'fixture-writeback-material',
            consumers: ['writeback/Connection/Outbox'],
          }),
        ),
      ],
      'repository',
      [
        resource('ConnectorImplementation', 'Native', {
          artifact: { uri: 'oci://example.invalid/writeback', digest: 'sha256:' + 'a'.repeat(64) },
          abiVersion: 'connector/v1',
          capabilities: ['writeback'],
          configurationSchema: { type: 'object', additionalProperties: false },
        }) as Resource,
      ],
    );
    await connector.client.apply(
      (await connector.client.plan(connector.root.id, connector.publication.digest)).id,
    );
    const connection = connector.resource('writeback/Connection/Outbox');
    const implementation = connector.resource('writeback/ConnectorImplementation/Native');
    const secret = connector.resource('writeback/SecretReference/Credential');
    const identityDomain = `actions/${environment}`;
    const serviceOperations = [`${dataset}:read`, `${object}:index`, 'filesystem:read'];
    await db.pool.query(
      'INSERT INTO moria.runtime_identity_domains(domain,cell,incarnation) SELECT $1,$2,incarnation FROM moria.indexing_instance',
      [identityDomain, cell],
    );
    await db.pool.query(
      "INSERT INTO moria.runtime_service_authorities(id,environment,principal,application,operations,markings,domains,resource_classes,incarnation) SELECT $1,$2,$3,$4,$5,ARRAY['private'],ARRAY['indexing'],ARRAY['standard'],incarnation FROM moria.indexing_instance",
      [service, environment, principal, fixture.application, serviceOperations],
    );
    await db.pool.query(
      "INSERT INTO moria.runtime_service_targets(environment,resource,domain,binding_revision,resource_class,authority) VALUES($1,$2,'indexing','writeback-v1','standard',$3)",
      [environment, object, service],
    );
    const sourceOperations = [
      ...[
        'read',
        'list',
        'create',
        'write',
        'move',
        'delete',
        'export',
        'administer',
        'register',
        'publish',
      ].map((op) => `filesystem:${op}`),
      'instance:manage',
      ...serviceOperations,
      `${dataset}:write`,
      `${object}:read`,
      `${object}:write`,
      `${action}:invoke`,
    ];
    await authority.setApplication(environment, 'fixture', fixture.application, sourceOperations);
    await authority.setGrants(
      environment,
      'fixture',
      principal,
      fixture.application,
      sourceOperations,
    );
    const workerRole = new URL(nativeEnv.RETENTION_DATABASE!).username;
    const writerRole = 'writeback_' + randomUUID().replaceAll('-', ''),
      password = randomUUID();
    await db.pool.query(
      `CREATE ROLE ${writerRole} LOGIN INHERIT PASSWORD '${password}'; GRANT ${workerRole} TO ${writerRole}`,
    );
    const writerUrl = `postgresql://${writerRole}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const artifactNamespace = { warehouseId: 'writeback', tableId: 'objects', revision: 'v1' };
    const artifactRoot = join(db.root, 'writeback-objects');
    await mkdir(artifactRoot, { recursive: true });
    const writer = await fixture.start(
      [
        {
          environment,
          projectId: actions.role.projectId,
          reference: 'writeback/ObjectType/Record',
          configuration: {
            projection: {
              version: 1,
              sourceFields: ['id'],
              effectiveFields: ['uid', 'name'],
              layouts: [
                { name: 'name', sort: [{ property: 'name', direction: 'asc' }], keyBytes: 128 },
              ],
              readerRoles: [writerRole],
              budgets: {
                rowBytes: 1048576,
                batchRows: 100,
                batchBytes: 1048576,
                commitReplayRows: 100,
                ddlTimeMs: 5000,
              },
            },
            artifactNamespace,
            markings: ['private'],
            partitions: 2,
            timeoutSeconds: 1800,
            serviceBindingRevision: 'writeback-v1',
            identityDomain,
          },
        },
      ],
      {
        ...connectorEnv,
        RETENTION_DATABASE: writerUrl,
        MORIA_NATIVE_STORAGE_BINDINGS: JSON.stringify([
          ...fixture.storageBindings,
          {
            namespace: artifactNamespace,
            provider: {
              kind: 'posix',
              root: artifactRoot,
              limits: { maxObjectBytes: 64 * 1024 * 1024, chunkBytes: 1024 * 1024 },
            },
          },
        ]),
      },
    );
    const sourceClient = filesystemClient(
      createTransport({
        baseUrl: writer.url,
        fetch: (url, init) =>
          fetch(url, {
            ...init,
            headers: { ...init?.headers, authorization: 'Bearer ' + fixture.token },
          }),
      }),
    );
    const actionPlan = await sourceClient.plan(actions.root.id, actions.publication.digest);
    assert.equal(actionPlan.runtime?.configurationReady, true, JSON.stringify(actionPlan.runtime));
    assert.deepEqual(
      (
        await db.pool.query(
          'SELECT target FROM moria.native_action_identity_domains WHERE domain=$1',
          [identityDomain],
        )
      ).rows,
      [],
    );
    const appliedAction = await sourceClient.apply(actionPlan.id);
    assert.equal(appliedAction.runtimeConfigured, true);
    await db.pool.query(
      "INSERT INTO moria.runtime_pipeline_destinations(id,environment,resource,revision,endpoint_ref,secret_ref,credentials,methods,paths,markings,allowed_markings) VALUES($1,$2,$3,'v1','WRITEBACK_ENDPOINT','WRITEBACK_MATERIAL',$4,ARRAY['POST'],ARRAY['/writeback'],ARRAY['private'],ARRAY['private'])",
      [
        connectorDestination,
        environment,
        connection,
        JSON.stringify([
          {
            secret,
            materialRef: 'WRITEBACK_MATERIAL',
            header: 'x-fixture-material',
            scheme: 'raw',
          },
        ]),
      ],
    );
    const inspected = await fixture.command(
      ['secret', 'inspect-connector'],
      { environment, connection, destination: connectorDestination },
      operator,
    );
    assert.equal(inspected.bindings.length, 1);
    const approval = await fixture.command(
      ['secret', 'approve'],
      {
        environment,
        requestKey: 'writeback-fixture',
        binding: inspected.bindings[0],
        materialRevision: 'v1',
        markings: ['private'],
      },
      operator,
    );
    const operations = [
      'filesystem:read',
      `${object}:read`,
      `${object}:write`,
      `${dataset}:read`,
      `${action}:read`,
      `${action}:invoke`,
      `${implementation}:read`,
      `${connection}:writeback`,
      `${destination}:write`,
      `${secret}:use`,
    ];
    const mint = async (application: string, scope: string[]) => {
      await authority.setApplication(environment, 'fixture', application, scope);
      await authority.setGrants(environment, 'fixture', principal, application, scope);
      return (await sessions.mint({ environment, principal, application }, 'fixture')).token;
    };
    const application = 'writeback-origin';
    const noWriteback = await mint(
      'writeback-denied',
      operations.filter((op) => !op.endsWith(':writeback')),
    );
    const token = await mint(application, operations);
    const epoch = (await authority.current(environment, principal, application)).epoch;
    const deliveryBinding = (
      await db.pool.query(
        "INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation) SELECT $1,$2,$3,$4,$5,ARRAY['private'],$6,s.security_epoch,i.incarnation FROM moria.instance_state s,moria.indexing_instance i RETURNING id",
        [
          environment,
          destination,
          {
            version: 1,
            domain: 'delivery',
            release: 'a'.repeat(64),
            resourceClass: 'standard',
            timeoutSeconds: 120,
            retryPolicy: 'idempotent',
            inputSchema: { type: 'object', properties: {} },
            pinnedInputs: [],
            expectedHeads: {},
          },
          { environment, principal, application },
          [`${destination}:write`],
          epoch,
        ],
      )
    ).rows[0].id as string;
    const registration = {
      environment,
      action,
      connection,
      connectorDestination,
      deliveryBinding,
      writerRole,
      effectKey: 'notify',
    };
    await assert.rejects(
      fixture.command(['delivery', 'register-writeback'], registration, worker),
      /RECOVERY_OPERATOR_REQUIRED/,
    );
    const registered = await fixture.command(
      ['delivery', 'register-writeback'],
      registration,
      operator,
    );
    assert.equal(registered.profile, 'http-outbox-v1');
    assert.deepEqual(
      await fixture.command(['delivery', 'register-writeback'], registration, operator),
      registered,
    );
    const invoke = (key: string, bearer = token, base = writer.url) =>
      fetch(`${base}/api/v1/actions/${action}/invoke`, {
        method: 'POST',
        headers: {
          authorization: 'Bearer ' + bearer,
          'content-type': 'application/json',
          'idempotency-key': key,
        },
        body: JSON.stringify({ input: { name: 'row-' + key, key } }),
      });
    const record = async (key: string) =>
      (
        await db.pool.query(
          "SELECT id,status,fence::text AS fence,payload,body,payload_hash AS hash,receipt FROM moria.runtime_deliveries WHERE destination=$1 AND origin->>'key'=$2",
          [registered.destination, key],
        )
      ).rows;
    const call = (command: string, input: unknown, env: NodeJS.ProcessEnv = worker) =>
      fixture.command(['delivery', command], input, env);
    const once = () => call('once', { owner: randomUUID() });
    const apply = async (key: string) => {
      const response = await invoke(key);
      assert.equal(response.status, 200, await response.clone().text());
      const result = await response.json();
      const [intent] = await record(key);
      assert.ok(intent);
      assert.deepEqual(intent.payload, { version: 1, action, key, result });
      return intent;
    };

    await t.test(
      'an actual outbox insert rolls back when the later Action audit fails',
      async () => {
        const key = randomUUID();
        await db.pool.query(`
        CREATE SEQUENCE moria.fixture_writeback_rollback;
        CREATE FUNCTION moria.fixture_writeback_rollback() RETURNS trigger LANGUAGE plpgsql
          SECURITY DEFINER SET search_path=pg_catalog,moria AS $body$
        BEGIN
          IF NEW.key='${key}' THEN
            IF EXISTS(SELECT 1 FROM moria.runtime_deliveries WHERE origin->>'key'=NEW.key AND writeback IS NOT NULL)
              THEN PERFORM nextval('moria.fixture_writeback_rollback'); END IF;
            RAISE EXCEPTION 'FIXTURE_ACTION_ROLLBACK';
          END IF;
          RETURN NEW;
        END $body$;
        CREATE TRIGGER fixture_writeback_rollback BEFORE INSERT ON moria.native_serving_audit
          FOR EACH ROW EXECUTE FUNCTION moria.fixture_writeback_rollback();
      `);
        try {
          const failed = await invoke(key);
          assert.equal(failed.status, 500, await failed.text());
          assert.deepEqual(
            (
              await db.pool.query(
                'SELECT last_value::int AS observed,is_called FROM moria.fixture_writeback_rollback',
              )
            ).rows,
            [{ observed: 1, is_called: true }],
          );
          assert.deepEqual(await record(key), []);
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*)::int n FROM moria.action_keys WHERE environment=$1 AND key=$2',
                [environment, key],
              )
            ).rows[0].n,
            0,
          );
          assert.equal(observed.length, 0);
        } finally {
          await db.pool.query(
            'DROP TRIGGER fixture_writeback_rollback ON moria.native_serving_audit; DROP FUNCTION moria.fixture_writeback_rollback(); DROP SEQUENCE moria.fixture_writeback_rollback',
          );
        }
      },
    );
    await t.test(
      'role, grant, legacy enqueue and raw SQL cannot fabricate mapped intents',
      async () => {
        for (const [bearer, base] of [
          [noWriteback, writer.url],
          [token, fixture.baseUrl],
        ]) {
          const key = randomUUID();
          const denied = await invoke(key, bearer, base);
          assert.equal(denied.status, 403, await denied.text());
          assert.deepEqual(await record(key), []);
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*)::int n FROM moria.action_keys WHERE environment=$1 AND key=$2',
                [environment, key],
              )
            ).rows[0].n,
            0,
          );
        }
        const key = randomUUID(),
          intent = await apply(key);
        assert.deepEqual(
          (await record(key)).map((row) => row.id),
          [intent.id],
        );
        const retry = await invoke(key);
        assert.equal(retry.status, 200, await retry.text());
        assert.deepEqual(
          (await record(key)).map((row) => row.id),
          [intent.id],
        );
        const untrusted = new Client({ connectionString: nativeEnv.RETENTION_DATABASE! });
        await untrusted.connect();
        try {
          await assert.rejects(
            untrusted.query(
              "SELECT moria.runtime_delivery_enqueue($1,'forged','{}'::jsonb,ARRAY['private'],$2)",
              [
                registered.destination,
                { kind: 'action', environment, principal, application, action, key },
              ],
            ),
            /DELIVERY_WRITEBACK_ORIGIN_INVALID/,
          );
          await assert.rejects(
            untrusted.query(
              "INSERT INTO moria.runtime_deliveries(destination,destination_resource,environment,resource_class,logical_key,origin_hash,payload,body,payload_hash,source_markings,origin,writeback) SELECT destination,destination_resource,environment,resource_class,'forged',origin_hash,payload,body,payload_hash,source_markings,origin,writeback FROM moria.runtime_deliveries WHERE id=$1",
              [intent.id],
            ),
            /DELIVERY_WRITEBACK_ORIGIN_INVALID/,
          );
          await assert.rejects(
            untrusted.query(
              "INSERT INTO moria.action_keys SELECT environment,application,principal,action,'forged',input_hash,action_revision,result,expires_at,writeback FROM moria.action_keys WHERE environment=$1 AND key=$2",
              [environment, key],
            ),
            /DELIVERY_WRITEBACK_ORIGIN_INVALID/,
          );
          await assert.rejects(
            untrusted.query(
              "INSERT INTO moria.runtime_delivery_destinations(binding,endpoint,secret_ref,idempotent,allowed_markings) VALUES($1,$2,'WRITEBACK_MATERIAL',false,ARRAY['private'])",
              [deliveryBinding, endpoint],
            ),
            /RECOVERY_OPERATOR_REQUIRED/,
          );
        } finally {
          await untrusted.end();
        }
        const detached = new Client({ connectionString: writerUrl });
        await detached.connect();
        try {
          await assert.rejects(
            detached.query(
              'INSERT INTO moria.runtime_deliveries(destination,destination_resource,environment,resource_class,logical_key,origin_hash,payload,body,payload_hash,source_markings,origin,writeback) SELECT destination,destination_resource,environment,resource_class,logical_key,origin_hash,payload,body,payload_hash,source_markings,origin,writeback FROM moria.runtime_deliveries WHERE id=$1',
              [intent.id],
            ),
            /DELIVERY_WRITEBACK_ORIGIN_INVALID/,
          );
          await assert.rejects(
            detached.query('DELETE FROM moria.runtime_deliveries WHERE id=$1', [intent.id]),
            /IMMUTABLE_DELIVERY/,
          );
        } finally {
          await detached.end();
        }
        assert.equal(observed.length, 0);
        assert.equal((await once()).worked, true);
        assert.equal((await record(key))[0].status, 'delivered');
        assert.deepEqual(
          observed.map((item) => item.id),
          [intent.id],
        );
      },
    );

    await t.test('SIGKILL after the actual sent marker never causes another POST', async () => {
      const key = randomUUID(),
        intent = await apply(key);
      mode = 'hold';
      const child = spawn(fixture.runtime, ['delivery', 'once'], {
        env: worker,
        stdio: ['pipe', 'ignore', 'pipe'],
      });
      const exited = new Promise<number | null>((resolve, reject) => {
        child.once('error', reject);
        child.once('exit', resolve);
      });
      void exited.catch(() => {});
      t.after(async () => {
        child.kill('SIGKILL');
        await exited.catch(() => {});
      });
      child.stdin.on('error', () => {});
      child.stderr.resume();
      child.stdin.end(JSON.stringify({ owner: randomUUID() }));
      const end = Date.now() + 10000;
      while (!effects.has(intent.id) && child.exitCode === null && Date.now() < end)
        await delay(20);
      assert.equal(effects.has(intent.id), true);
      child.kill('SIGKILL');
      await exited;
      await db.pool.query(
        "UPDATE moria.runtime_deliveries SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
        [intent.id],
      );
      await db.pool.query('SELECT moria.runtime_delivery_reap()');
      const [state] = await record(key);
      assert.equal(state.status, 'uncertain');
      assert.equal((await once()).worked, false);
      const resolution = {
        id: intent.id,
        expectedFence: state.fence,
        resolution: 'delivered',
        evidence: 'fixture:actual-provider-receipt',
      };
      await assert.rejects(call('reconcile', resolution), /RECOVERY_OPERATOR_REQUIRED/);
      await call('reconcile', resolution, operator);
      assert.equal((await record(key))[0].status, 'delivered');
      assert.equal(observed.filter((item) => item.id === intent.id).length, 1);
      mode = 'normal';
    });

    await t.test(
      'provider non-delivery requires exact operator evidence before stable-byte resend',
      async () => {
        const key = randomUUID(),
          intent = await apply(key);
        mode = 'reject';
        await once();
        const [uncertain] = await record(key);
        assert.equal(uncertain.status, 'uncertain');
        assert.equal((await once()).worked, false);
        assert.equal(effects.has(intent.id), false);
        const resolution = {
          id: intent.id,
          expectedFence: uncertain.fence,
          resolution: 'notDelivered',
          evidence: 'fixture:terminal-rejected-with-no-effect',
        };
        await assert.rejects(
          call('reconcile', { ...resolution, expectedFence: '999' }, operator),
          /DELIVERY_RECONCILIATION_STALE/,
        );
        await call('reconcile', resolution, operator);
        mode = 'normal';
        await once();
        assert.equal((await record(key))[0].status, 'delivered');
        const attempts = observed.filter((item) => item.id === intent.id);
        assert.equal(attempts.length, 2);
        assert.equal(attempts[0].body, attempts[1].body);
        assert.equal(attempts[0].hash, attempts[1].hash);
        assert.notEqual(attempts[0].fence, attempts[1].fence);
      },
    );

    await t.test(
      'escaped credential reflection and a missed approval pulse fail closed',
      async () => {
        const first = randomUUID(),
          reflected = await apply(first);
        mode = 'reflect';
        await once();
        const [uncertain] = await record(first);
        assert.equal(uncertain.status, 'uncertain');
        assert.equal(uncertain.receipt, null);
        assert.equal(effects.has(reflected.id), true);
        assert.equal((await once()).worked, false);
        mode = 'normal';
        const second = randomUUID(),
          pending = await apply(second);
        await db.pool.query('UPDATE moria.native_secret_approvals SET enabled=false WHERE id=$1', [
          approval.id,
        ]);
        await db.pool.query('UPDATE moria.native_secret_approvals SET enabled=true WHERE id=$1', [
          approval.id,
        ]);
        await once();
        assert.equal((await record(second))[0].status, 'failed');
        assert.equal(
          observed.some((item) => item.id === pending.id),
          false,
        );
        await assert.rejects(
          call(
            'reconcile',
            {
              id: reflected.id,
              expectedFence: uncertain.fence,
              resolution: 'notDelivered',
              evidence: 'fixture:stale-authority',
            },
            operator,
          ),
          /AUTHORIZATION_CHANGED/,
        );
        assert.equal((await record(first))[0].status, 'uncertain');
        const renewed = randomUUID(),
          current = await apply(renewed);
        assert.equal((await once()).worked, true);
        assert.equal((await record(renewed))[0].status, 'delivered');
        assert.equal(observed.filter((item) => item.id === current.id).length, 1);
        assert.equal((await record(second))[0].status, 'failed');
      },
    );
    assert.deepEqual(errors, []);
  },
);
