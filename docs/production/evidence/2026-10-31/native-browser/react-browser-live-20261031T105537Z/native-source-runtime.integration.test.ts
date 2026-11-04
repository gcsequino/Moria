import assert from 'node:assert/strict';
import test from 'node:test';
import { randomUUID, createHash } from 'node:crypto';
import { spawn, execFile, type ChildProcess } from 'node:child_process';
import { createServer as createNetServer } from 'node:net';
import { promisify } from 'node:util';
import { mkdir, writeFile, readFile, access } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { database } from './fixtures/catalog-storage/database.js';
import { publishNativeSource } from './fixtures/native-source/setup.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { configuredFilesystem } from '../services/filesystem/src/local.js';
import { filesystemClient } from '../packages/platform/src/filesystem.js';
import { createTransport } from '../packages/platform/src/transport.js';
import { actionClient, objectClient } from '../packages/osdk/src/runtime.js';
import { AppHost } from '../services/app-host/src/store.js';
import { build } from 'vite';
import { assertNativeArtifacts } from './fixtures/native-common/artifact.js';
import { publishBrowserSource } from '../packages/platform/src/source.js';
import { buildNativeManagement } from './fixtures/native-common/management.js';
import { functionImage } from './fixtures/functions/image.js';

async function unusedPort() {
  const server = createNetServer();
  await new Promise<void>((done) => server.listen(0, '127.0.0.1', done));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>((done) => server.close(() => done()));
  return port;
}

test(
  'native source composition requires explicit current Dataset/index authority and installs all bindings atomically',
  { timeout: process.env.MORIA_NATIVE_BROWSER_PROOF_FILE ? 1800000 : 180000 },
  async (t) => {
    await assertNativeArtifacts(
      process.env.MORIA_RUNTIME_BINARY ?? '/usr/local/bin/moria-indexing',
      [process.env.MORIA_CATALOG_TEST_BINARY ?? '/usr/local/bin/moria-catalog-tests'],
    );
    const db = await database();
    const processes: ChildProcess[] = [];
    let browserImage: Awaited<ReturnType<typeof functionImage>> | undefined;
    let browserCreates = 0;
    const exits: Promise<unknown>[] = [];
    t.after(async () => {
      for (const process of processes) process.kill('SIGTERM');
      await Promise.race([Promise.allSettled(exits), delay(40000, undefined, { ref: false })]);
      for (const process of processes) if (process.exitCode === null) process.kill('SIGKILL');
      await Promise.allSettled(exits);
      await browserImage?.close();
      await db.close();
    });
    const environment = randomUUID(),
      application = 'source-runtime',
      principal = 'publisher',
      cell = randomUUID(),
      service = randomUUID();
    // The retained administration pool uses the same operator-selected cell as
    // native services; placement never comes from an HTTP request.
    db.pool.options.options = `-c moria.cell_id=${cell}`;
    await db.pool.query("SELECT set_config('moria.cell_id',$1,false)", [cell]);
    await db.catalog.ensureEnvironment(environment);
    const authority = new Authority(db.catalog),
      sessions = new WorkloadSessions(authority);
    const operations = [
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
    ]
      .map((op) => `filesystem:${op}`)
      .concat('instance:manage');
    await authority.syncPrincipal(environment, 'fixture', principal, {
      kind: 'workload',
      enabled: true,
      markings: ['private'],
      attributes: {},
    });
    await authority.setApplication(environment, 'fixture', application, operations);
    await authority.setGrants(environment, 'fixture', principal, application, operations);
    const token = (await sessions.mint({ environment, principal, application }, 'fixture')).token;
    const fs = await configuredFilesystem(db.catalog, join(db.root, 'filesystem'));
    await fs.management.ensureHome({ session: await sessions.authenticate(`Bearer ${token}`) });
    const role = `source_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(`ALTER ROLE ${role} SET temp_file_limit='256MB'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role};GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role};GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    const connection = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const namespace = { warehouseId: 'source-runtime', tableId: 'records', revision: 'v1' },
      artifacts = { warehouseId: 'source-runtime', tableId: 'objects', revision: 'v1' };
    const bindings = [
      {
        namespace: { warehouseId: 'filesystem', tableId: 'content', revision: 'v1' },
        root: join(db.root, 'filesystem'),
      },
      { namespace, root: join(db.root, 'records') },
      { namespace: artifacts, root: join(db.root, 'objects') },
    ];
    for (const binding of bindings) await mkdir(binding.root, { recursive: true });
    const start = async (
      profiles: unknown[] = [],
      extraEnvironment: Record<string, string> = {},
      launcher = false,
    ) => {
      const nativeBinary = process.env.MORIA_RUNTIME_BINARY ?? '/usr/local/bin/moria-indexing';
      const localEnvironment = launcher
        ? {
            NODE_ENV: 'test',
            MORIA_LOCAL_RUNTIME: 'native',
            MORIA_DATABASE_MODE: 'external',
            MORIA_DATABASE_SECRET_REF: 'SOURCE_RUNTIME_DATABASE',
            MORIA_DATABASE_TLS: 'disable',
            MORIA_RUNTIME_BINARY: nativeBinary,
            MORIA_LOCAL_DIR: db.root,
            MORIA_PORT: String(await unusedPort()),
            MORIA_AUTH_PORT: String(await unusedPort()),
            MORIA_NATIVE_DRAIN_SECONDS: '2',
            MORIA_NATIVE_RECOVERY_PRINCIPAL: principal,
            MORIA_NATIVE_BROWSER_BINDING: JSON.stringify({ environment, application }),
            MORIA_NATIVE_APP_HOST: JSON.stringify({
              directory: managementHost.root,
              app: 'management',
              entryPath: '/',
              environment,
              application,
              publicShell: true,
            }),
            MORIA_NATIVE_WORKERS: '[]',
          }
        : {};
      const child = spawn(
        launcher ? process.execPath : nativeBinary,
        launcher ? ['--import', 'tsx', 'tests/fixtures/native-local-process.ts'] : ['serve'],
        {
          env: {
            PATH: '/usr/local/bin:/usr/bin:/bin',
            MORIA_INDEXING_DATABASE_SECRET_REF: 'SOURCE_RUNTIME_DATABASE',
            SOURCE_RUNTIME_DATABASE: connection,
            MORIA_INDEXING_DATABASE_TLS: 'disable',
            MORIA_NATIVE_LISTEN: '127.0.0.1:0',
            MORIA_NATIVE_FILESYSTEM_STORAGE_REVISION: 'v1',
            MORIA_NATIVE_CELL_ID: cell,
            MORIA_NATIVE_SOURCE_PROFILES: JSON.stringify(profiles),
            MORIA_NATIVE_STORAGE_BINDINGS: JSON.stringify(
              bindings.map(({ namespace, root }) => ({
                namespace,
                provider: {
                  kind: 'posix',
                  root,
                  limits: { maxObjectBytes: 64 * 1024 * 1024, chunkBytes: 1024 * 1024 },
                },
              })),
            ),
            ...localEnvironment,
            ...extraEnvironment,
          },
          stdio: ['ignore', 'pipe', 'pipe'],
        },
      );
      processes.push(child);
      exits.push(new Promise((resolve) => child.once('exit', resolve)));
      let output = '',
        errors = '';
      child.stdout!.on('data', (chunk) => (output += chunk));
      child.stderr!.on('data', (chunk) => (errors += chunk));
      for (let attempt = 0; attempt < (launcher ? 3000 : 500); attempt++) {
        const line = output.split('\n').find((line) => line.includes('"event":"serving"'));
        if (line && (!launcher || output.includes('native-local-ready'))) {
          const value = JSON.parse(line);
          return {
            url: `http://${value.address}`,
            process: child,
            diagnostics: () => ({
              exitCode: child.exitCode,
              errors: errors.replace(/[A-Za-z0-9_-]{43,}/g, '[opaque]').slice(-4096),
            }),
          };
        }
        if (child.exitCode !== null) throw new Error(errors);
        await delay(10);
      }
      throw new Error(`Native server startup timeout: ${errors}`);
    };
    const managementBuild = join(db.root, 'management-preload');
    await buildNativeManagement(managementBuild);
    const managementHost = new AppHost(join(db.root, 'apps'));
    const managementArtifact = await managementHost.publish('management', managementBuild);
    await managementHost.promote('management', managementArtifact.version);
    const initial = await start([], {}, true);
    const headers = { authorization: `Bearer ${token}`, 'content-type': 'application/json' };
    const frontendSource = join(db.root, 'frontend-source'),
      frontendBuild = join(db.root, 'frontend-build');
    await mkdir(frontendSource);
    await writeFile(
      join(frontendSource, 'index.html'),
      '<!doctype html><html><head><meta charset="utf-8"><title>Native source application</title><script type="module" src="/main.ts"></script></head><body><button id="create">Create record</button><label>Management origin <input id="management-origin"></label><button id="cross">Test cross-app isolation</button><pre id="result">Loading session</pre><pre id="isolation"></pre></body></html>',
    );
    await writeFile(
      join(frontendSource, 'main.ts'),
      `import {createTransport} from ${JSON.stringify(resolve('packages/platform/src/transport.ts'))};import {objectClient,actionClient} from ${JSON.stringify(resolve('packages/osdk/src/runtime.ts'))};
const transport=createTransport();const output=document.querySelector('#result');
const managementOrigin=${JSON.stringify(initial.url)};document.querySelector('#management-origin').value=managementOrigin;
document.querySelector('#cross').addEventListener('click',async()=>{const output=document.querySelector('#isolation');try{const response=await fetch(document.querySelector('#management-origin').value+'/api/management/v1/runtime',{credentials:'include'});output.textContent='Cross-app unexpectedly readable: '+response.status;}catch{output.textContent='Cross-app request blocked';}});
async function boot(){const ticket=new URLSearchParams(location.hash.slice(1)).get('recovery');if(ticket){history.replaceState(null,'',location.pathname);await transport.auth.recover(ticket);}const session=await transport.auth.session();if(!session.user){output.textContent='Sign in required';return;}const contract=await transport.request('/api/v1/contract','GET');output.textContent='Native session ready';document.querySelector('#create').addEventListener('click',async()=>{try{const changed=await actionClient(transport,contract.actions.create.id,contract.actions.create.inputs)({name:'browser-app-created',key:'browser-app-created'},{idempotencyKey:crypto.randomUUID()});const uid=changed.created[0].uid;const row=await objectClient(transport,contract.objects.Record.id,contract.objects.Record.properties).get(uid);output.textContent=JSON.stringify({uid,object:row.object});}catch(error){output.textContent=String(error);}});}boot().catch(error=>{output.textContent=String(error)});`,
    );
    await build({
      root: frontendSource,
      base: './',
      configFile: false,
      logLevel: 'silent',
      build: { outDir: frontendBuild },
    });
    const appHost = new AppHost(join(db.root, 'app-artifacts'));
    const frontend = await appHost.publish('source-browser', frontendBuild);
    await appHost.promote('source-browser', frontend.version);
    const context = await fetch(`${initial.url}/api/management/v1/fs/compiler-context`, {
      method: 'POST',
      headers,
      body: JSON.stringify({ principalAttributes: {} }),
    });
    assert.equal(context.status, 200, await context.text());
    const uid = { type: 'uid', nullable: false },
      name = { type: 'string', nullable: false };
    const published = await publishNativeSource(initial.url, token, [
      {
        path: '.ontology.json',
        text: JSON.stringify({
          version: 1,
          name: 'Native runtime source',
          namespace: 'source_runtime',
        }),
      },
      {
        path: 'records.resource.json',
        text: JSON.stringify({
          apiVersion: 'moria/v1alpha2',
          kind: 'Dataset',
          metadata: { namespace: 'source_runtime', name: 'records' },
          spec: { columns: { id: uid, name }, writeModes: ['replace'] },
        }),
      },
      {
        path: 'records.object.json',
        text: JSON.stringify({
          apiVersion: 'moria/v1alpha2',
          kind: 'ObjectType',
          metadata: { namespace: 'source_runtime', name: 'Record' },
          spec: {
            primaryKey: 'uid',
            properties: { uid, name },
            source: {
              resource: 'source_runtime/Dataset/records',
              mapping: { uid: 'id', name: 'name' },
            },
          },
        }),
      },
      {
        path: 'create.action.json',
        text: JSON.stringify({
          apiVersion: 'moria/v1alpha2',
          kind: 'Action',
          metadata: { namespace: 'source_runtime', name: 'create' },
          spec: {
            inputs: { name, key: name },
            targetType: 'source_runtime/ObjectType/Record',
            condition: { literal: true },
            edits: [
              {
                op: 'create',
                targetType: 'source_runtime/ObjectType/Record',
                identityKey: { parameter: 'key' },
                values: { name: { parameter: 'name' } },
              },
            ],
          },
        }),
      },
      {
        path: 'clear.action.json',
        text: JSON.stringify({
          apiVersion: 'moria/v1alpha2',
          kind: 'Action',
          metadata: { namespace: 'source_runtime', name: 'clear' },
          spec: {
            inputs: { uid },
            targetType: 'source_runtime/ObjectType/Record',
            targetParameter: 'uid',
            condition: { literal: true },
            edits: [
              {
                op: 'clearOverride',
                targetType: 'source_runtime/ObjectType/Record',
                uid: { parameter: 'uid' },
                property: 'name',
              },
            ],
          },
        }),
      },
      {
        path: 'browser.resource.json',
        text: JSON.stringify({
          apiVersion: 'moria/v1alpha2',
          kind: 'Application',
          metadata: { namespace: 'source_runtime', name: 'browser' },
          spec: {
            mode: 'browser',
            frontend: {
              uri: `moria-app://source-browser/${frontend.version}`,
              digest: `sha256:${frontend.version}`,
            },
            scopes: [
              { target: 'source_runtime/ObjectType/Record', operations: ['read', 'write'] },
              { target: 'source_runtime/Dataset/records', operations: ['read'] },
              { target: 'source_runtime/Action/create', operations: ['invoke'] },
            ],
          },
        }),
      },
      ...[
        {
          apiVersion: 'moria/v1alpha2',
          kind: 'LinkType',
          metadata: { namespace: 'source_runtime', name: 'managed' },
          spec: {
            sourceType: 'source_runtime/ObjectType/Record',
            targetType: 'source_runtime/ObjectType/Record',
            mode: 'actionManaged',
            cardinality: 'manyToOne',
          },
        },
        {
          apiVersion: 'moria/v1alpha2',
          kind: 'LinkType',
          metadata: { namespace: 'source_runtime', name: 'mapped' },
          spec: {
            sourceType: 'source_runtime/ObjectType/Record',
            targetType: 'source_runtime/ObjectType/Record',
            mode: 'mapped',
            cardinality: 'manyToOne',
            sourceProperty: 'uid',
            targetProperty: 'uid',
          },
        },
        ...['link', 'unlink'].map((op) => ({
          apiVersion: 'moria/v1alpha2',
          kind: 'Action',
          metadata: { namespace: 'source_runtime', name: op },
          spec: {
            inputs: { uid, target: uid },
            targetType: 'source_runtime/ObjectType/Record',
            targetParameter: 'uid',
            condition: { literal: true },
            edits: [
              {
                op,
                linkType: 'source_runtime/LinkType/managed',
                sourceUid: { parameter: 'uid' },
                targetUid: { parameter: 'target' },
              },
            ],
          },
        })),
        ...['delete', 'restore'].map((op) => ({
          apiVersion: 'moria/v1alpha2',
          kind: 'Action',
          metadata: { namespace: 'source_runtime', name: op },
          spec: {
            inputs: { uid },
            targetType: 'source_runtime/ObjectType/Record',
            targetParameter: 'uid',
            condition: { literal: true },
            edits: [
              { op, targetType: 'source_runtime/ObjectType/Record', uid: { parameter: 'uid' } },
            ],
          },
        })),
      ].map((resource) => ({
        path: `${resource.metadata.name}.${resource.kind === 'LinkType' ? 'link' : 'action'}.json`,
        text: JSON.stringify(resource),
      })),
    ]);
    const dataset = published.resource('source_runtime/Dataset/records'),
      object = published.resource('source_runtime/ObjectType/Record'),
      createAction = published.resource('source_runtime/Action/create'),
      clearAction = published.resource('source_runtime/Action/clear'),
      managedLink = published.resource('source_runtime/LinkType/managed'),
      mappedLink = published.resource('source_runtime/LinkType/mapped'),
      linkAction = published.resource('source_runtime/Action/link'),
      unlinkAction = published.resource('source_runtime/Action/unlink');
    const deleteAction = published.resource('source_runtime/Action/delete'),
      restoreAction = published.resource('source_runtime/Action/restore');
    const browserApplication = published.resource('source_runtime/Application/browser');
    const browserOperations = [
      `${object}:read`,
      `${object}:write`,
      `${dataset}:read`,
      `${createAction}:invoke`,
    ];
    await authority.setApplication(environment, 'fixture', browserApplication, browserOperations);
    await authority.setGrants(
      environment,
      'fixture',
      principal,
      browserApplication,
      browserOperations,
    );
    const incarnation = (await db.pool.query('SELECT incarnation FROM moria.indexing_instance'))
      .rows[0].incarnation;
    await db.pool.query(
      'INSERT INTO moria.runtime_cells(id,endpoint_ref,incarnation) VALUES($1,$2,$3) ON CONFLICT DO NOTHING',
      [cell, 'SOURCE_RUNTIME_CELL', incarnation],
    );
    await db.pool.query(
      'INSERT INTO moria.runtime_placements(environment,cell,incarnation,serving,leases_fenced) VALUES($1,$2,$3,true,true) ON CONFLICT DO NOTHING',
      [environment, cell, incarnation],
    );
    const identityDomain = `actions/${environment}`;
    await db.pool.query(
      'INSERT INTO moria.runtime_identity_domains(domain,cell,incarnation) VALUES($1,$2,$3)',
      [identityDomain, cell, incarnation],
    );
    const serviceOps = [`${dataset}:read`, `${object}:index`, 'filesystem:read'];
    await db.pool.query(
      'INSERT INTO moria.runtime_service_authorities(id,environment,principal,application,operations,markings,domains,resource_classes,incarnation) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)',
      [
        service,
        environment,
        principal,
        application,
        serviceOps,
        ['private'],
        ['indexing'],
        ['standard'],
        incarnation,
      ],
    );
    await db.pool.query(
      'INSERT INTO moria.runtime_service_targets(environment,resource,domain,binding_revision,resource_class,authority) VALUES($1,$2,$3,$4,$5,$6)',
      [environment, object, 'indexing', 'source-profile-v1', 'standard', service],
    );
    const profiles = [
      {
        environment,
        projectId: published.role.projectId,
        reference: 'source_runtime/Dataset/records',
        configuration: { namespace, icebergUuid: randomUUID(), markings: ['private'] },
      },
      {
        environment,
        projectId: published.role.projectId,
        reference: 'source_runtime/ObjectType/Record',
        configuration: {
          projection: {
            version: 1,
            sourceFields: ['id'],
            effectiveFields: ['uid', 'name'],
            layouts: [
              { name: 'name', sort: [{ property: 'name', direction: 'asc' }], keyBytes: 128 },
            ],
            readerRoles: [role],
            budgets: {
              rowBytes: 1024 * 1024,
              batchRows: 100,
              batchBytes: 1024 * 1024,
              commitReplayRows: 100,
              commitReplayBytes: 1024 * 1024,
              ddlTimeMs: 5000,
            },
          },
          artifactNamespace: artifacts,
          markings: ['private'],
          partitions: 2,
          timeoutSeconds: 1800,
          serviceBindingRevision: 'source-profile-v1',
          identityDomain,
        },
      },
      ...['managed', 'mapped'].map((name) => ({
        environment,
        projectId: published.role.projectId,
        reference: `source_runtime/LinkType/${name}`,
        configuration: { markings: ['private'] },
      })),
      {
        environment,
        projectId: published.role.projectId,
        reference: 'source_runtime/Application/browser',
        configuration: {},
      },
    ];
    const configured = await start(profiles, {}, true),
      client = filesystemClient(
        createTransport({
          baseUrl: configured.url,
          fetch: (url, init) =>
            fetch(url, {
              ...init,
              headers: { ...init?.headers, authorization: `Bearer ${token}` },
            }),
        }),
      );
    const denied = await client.plan(published.root.id, published.publication.digest);
    await assert.rejects(client.apply(denied.id), { status: 403 });
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*)::int AS n FROM moria.native_serving_bindings WHERE environment=$1',
          [environment],
        )
      ).rows[0].n,
      0,
    );
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*)::int AS n FROM moria.fs_active_releases WHERE environment=$1',
          [environment],
        )
      ).rows[0].n,
      0,
    );
    // This is an explicit administration change; filesystem publication did not create these grants.
    await authority.setApplication(environment, 'fixture', application, [
      ...operations,
      ...serviceOps,
      `${object}:read`,
      `${object}:write`,
      `${dataset}:write`,
      `${createAction}:invoke`,
      `${clearAction}:invoke`,
      `${managedLink}:read`,
      `${managedLink}:write`,
      `${mappedLink}:read`,
      `${mappedLink}:write`,
      `${linkAction}:invoke`,
      `${unlinkAction}:invoke`,
      `${deleteAction}:invoke`,
      `${restoreAction}:invoke`,
    ]);
    await authority.setGrants(environment, 'fixture', principal, application, [
      ...operations,
      ...serviceOps,
      `${object}:read`,
      `${object}:write`,
      `${dataset}:write`,
      `${createAction}:invoke`,
      `${clearAction}:invoke`,
      `${managedLink}:read`,
      `${managedLink}:write`,
      `${mappedLink}:read`,
      `${mappedLink}:write`,
      `${linkAction}:invoke`,
      `${unlinkAction}:invoke`,
      `${deleteAction}:invoke`,
      `${restoreAction}:invoke`,
    ]);
    const plan = await client.plan(published.root.id, published.publication.digest),
      result = (await client.apply(plan.id)) as any;
    assert.equal(result.runtimeConfigured, true);
    assert.deepEqual(await client.apply(plan.id), result);
    const rows = (
      await db.pool.query(
        'SELECT b.definition,b.instance_epoch::text,b.security_epoch::text,i.security_epoch::text AS current_instance,e.security_epoch::text AS current_environment FROM moria.indexing_bindings b CROSS JOIN moria.instance_state i JOIN moria.environments e ON e.id=$1 WHERE b.environment=$1 AND b.target=$2',
        [environment, object],
      )
    ).rows;
    assert.equal(rows.length, 1);
    assert.equal(rows[0].instance_epoch, rows[0].current_instance);
    assert.equal(rows[0].security_epoch, rows[0].current_environment);
    assert.equal(rows[0].definition.sourceMapping.uid, 'id');
    assert(rows[0].definition.servingDefinition);
    const datasetBinding = (
      await db.pool.query(
        'SELECT iceberg_schema FROM moria.native_dataset_bindings WHERE environment=$1 AND resource=$2 AND enabled',
        [environment, dataset],
      )
    ).rows[0];
    assert.equal(datasetBinding.iceberg_schema.fields[0].type, 'uuid');
    const serving = (
      await db.pool.query(
        'SELECT definition FROM moria.native_serving_bindings WHERE environment=$1 AND resource=$2',
        [environment, object],
      )
    ).rows[0].definition;
    assert.equal(serving.sourceResource, dataset);
    assert.equal(serving.policies.sourceColumns.id.type, 'uid');
    assert.equal(serving.schema.primaryKey, 'uid');
    const runtimeStatus = await client.runtimeStatus(published.root.id);
    assert.equal(runtimeStatus.configured, true);
    assert.equal(runtimeStatus.managedCellReady, true);
    await t.test(
      'action creation allocates once and enforces source attachment and required overrides',
      async () => {
        const second = await start(profiles),
          key = randomUUID();
        const invoke = (url: string, resource: string, actionKey: string, input: unknown) =>
          fetch(`${url}/api/v1/actions/${resource}/invoke`, {
            method: 'POST',
            headers: { ...headers, 'idempotency-key': actionKey },
            body: JSON.stringify({ input }),
          });
        const requests = await Promise.all(
          [configured.url, second.url].map((url) =>
            invoke(url, createAction, key, { name: 'created', key: 'identity' }),
          ),
        );
        const results = await Promise.all(
          requests.map(async (response) => {
            assert.equal(response.status, 200, await response.clone().text());
            return response.json();
          }),
        );
        assert.deepEqual(results[0], results[1]);
        const sdkCreate = actionClient<{ name: string; key: string }>(
          published.transport,
          createAction,
          { name: { type: 'string', nullable: false }, key: { type: 'string', nullable: false } },
        );
        assert.deepEqual(
          await sdkCreate({ name: 'created', key: 'identity' }, { idempotencyKey: key }),
          results[0],
        );
        const created = results[0].created[0].uid;
        const get = () =>
          fetch(`${configured.url}/api/v1/objects/${object}/${created}`, { headers });
        assert.equal((await (await get()).json()).object.name, 'created');
        const queried = await fetch(`${second.url}/api/v1/objects/${object}/query`, {
          method: 'POST',
          headers,
          body: '{}',
        });
        assert.equal(queried.status, 200, await queried.clone().text());
        assert.equal((await queried.json()).objects[0].name, 'created');
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*)::int n FROM moria.native_object_creations WHERE environment=$1',
              [environment],
            )
          ).rows[0].n,
          1,
        );
        assert.equal(
          (await invoke(second.url, createAction, key, { name: 'different', key: 'identity' }))
            .status,
          409,
        );
        assert.equal(
          (await invoke(second.url, clearAction, randomUUID(), { uid: created })).status,
          400,
        );
        assert.equal((await (await get()).json()).object.name, 'created');
        await assert.rejects(
          db.pool.query('SELECT moria.runtime_object_import_check($1,$2,$3,$4)', [
            environment,
            object,
            [created],
            dataset,
          ]),
          /ACTION_SOURCE_ATTACHMENT_REQUIRED/,
        );
        const candidate = await db.pool.query(
          'SELECT uid,action_created,source_value FROM moria.runtime_object_base_stage(NULL,$1,$2,$3::bigint)',
          [environment, object, results[0].revision],
        );
        assert.deepEqual(candidate.rows, [
          { uid: created, action_created: true, source_value: { id: created, name: 'created' } },
        ]);
        assert.equal((await (await get()).json()).object.name, 'created');
        const otherResponse = await invoke(second.url, createAction, randomUUID(), {
          name: 'other',
          key: 'other',
        });
        assert.equal(otherResponse.status, 200, await otherResponse.clone().text());
        const other = (await otherResponse.json()).created[0].uid;
        const thirdResponse = await invoke(second.url, createAction, randomUUID(), {
          name: 'third',
          key: 'third',
        });
        assert.equal(thirdResponse.status, 200, await thirdResponse.clone().text());
        const third = (await thirdResponse.json()).created[0].uid;
        const linkKey = randomUUID();
        const linked = await invoke(second.url, linkAction, linkKey, {
          uid: created,
          target: other,
        });
        assert.equal(linked.status, 200, await linked.clone().text());
        const edgeReceipt = await linked.json();
        assert.deepEqual(
          await (
            await invoke(configured.url, linkAction, linkKey, { uid: created, target: other })
          ).json(),
          edgeReceipt,
        );
        const frame = (
          await db.pool.query(
            'SELECT t.complete,t.commit_revision::text,array_agg(i.uid::text ORDER BY i.uid) ids FROM moria.runtime_change_transactions t JOIN moria.runtime_change_items i ON i.transaction_id=t.id WHERE t.environment=$1 AND t.commit_revision=$2 GROUP BY t.id',
            [environment, edgeReceipt.revision],
          )
        ).rows[0];
        assert.equal(frame.complete, true);
        assert.equal(frame.commit_revision, edgeReceipt.revision);
        assert.deepEqual(frame.ids, [created, other].sort());
        const mutationCounts = async () =>
          (
            await db.pool.query(
              'SELECT (SELECT count(*)::int FROM moria.native_link_edge_versions WHERE environment=$1) edges,(SELECT count(*)::int FROM moria.runtime_change_transactions WHERE environment=$1) frames',
              [environment],
            )
          ).rows[0];
        const beforeFailedLink = await mutationCounts();
        const failedKey = randomUUID();
        const cardinality = await invoke(second.url, linkAction, failedKey, {
          uid: created,
          target: third,
        });
        assert.equal(cardinality.status, 409, await cardinality.clone().text());
        assert.deepEqual(await mutationCounts(), beforeFailedLink);
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*)::int n FROM moria.action_keys WHERE environment=$1 AND key=$2',
              [environment, failedKey],
            )
          ).rows[0].n,
          0,
        );
        const traverse = (link: string, uid: string, fanout = 10, direction = 'out') =>
          fetch(`${configured.url}/api/v1/links/${link}/traverse`, {
            method: 'POST',
            headers,
            body: JSON.stringify({
              uid,
              direction,
              projection: ['uid', 'name'],
              fanout,
              budget: { scanRows: 100, responseBytes: 100000, timeMs: 3000 },
            }),
          });
        const managed = await traverse(managedLink, created);
        assert.equal(managed.status, 200, await managed.clone().text());
        assert.deepEqual(
          (await managed.json()).objects.map((row: any) => row.uid),
          [other],
        );
        const reverse = await traverse(managedLink, other, 10, 'in');
        assert.equal(reverse.status, 200, await reverse.clone().text());
        assert.deepEqual(
          (await reverse.json()).objects.map((row: any) => row.uid),
          [created],
        );
        const anotherLink = await invoke(second.url, linkAction, randomUUID(), {
          uid: third,
          target: other,
        });
        assert.equal(anotherLink.status, 200, await anotherLink.clone().text());
        const overflow = await traverse(managedLink, other, 1, 'in');
        assert.equal(overflow.status, 429, await overflow.clone().text());
        assert.equal((await overflow.json()).error.code, 'RESOURCE_LIMIT');
        const mapped = await traverse(mappedLink, created);
        assert.equal(mapped.status, 200, await mapped.clone().text());
        assert.deepEqual((await mapped.json()).objects.map((row: any) => row.uid).sort(), [
          created,
        ]);
        assert.equal((await traverse(mappedLink, created, 1)).status, 200);
        assert.equal(
          (await invoke(second.url, unlinkAction, randomUUID(), { uid: created, target: other }))
            .status,
          200,
        );
        assert.deepEqual((await (await traverse(managedLink, created)).json()).objects, []);
        const createProducer = async () => {
          const publisher = await authority.current(environment, principal, application);
          const head = (await db.catalog.heads(environment, [dataset]))[0];
          return (
            await db.pool.query(
              'INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation) SELECT $1,$2,$3,$4,$5,$6,$7,s.security_epoch,i.incarnation FROM moria.instance_state s,moria.indexing_instance i RETURNING id',
              [
                environment,
                dataset,
                {
                  version: 1,
                  domain: 'sync',
                  release: published.publication.digest.slice(7),
                  resourceClass: 'standard',
                  timeoutSeconds: 180,
                  retryPolicy: 'idempotent',
                  inputSchema: { type: 'object', properties: {} },
                  pinnedInputs: [],
                  expectedHeads: { [dataset]: head.revision },
                },
                {
                  environment,
                  principal,
                  application,
                  sessionHash: createHash('sha256').update(token).digest('hex'),
                },
                [`${dataset}:read`, `${dataset}:write`, 'filesystem:read'],
                ['private'],
                publisher.epoch,
              ],
            )
          ).rows[0].id;
        };
        const producer = await createProducer();
        const scratch = join(db.root, 'snapshot-scratch');
        await mkdir(scratch, { recursive: true });
        const native = async (fixture: unknown) => {
          const result = await promisify(execFile)(
            process.env.MORIA_CATALOG_TEST_BINARY ?? '/usr/local/bin/moria-catalog-tests',
            ['--ignored', '--exact', 'catalog_publication', '--nocapture', '--test-threads=1'],
            {
              env: {
                PATH: process.env.PATH,
                MORIA_INDEXING_DATABASE_SECRET_REF: 'SOURCE_RUNTIME_DATABASE',
                SOURCE_RUNTIME_DATABASE: connection,
                MORIA_INDEXING_DATABASE_TLS: 'disable',
                MORIA_NATIVE_CELL_ID: cell,
                MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT: scratch,
                MORIA_NATIVE_CLOSURE_LIMITS: JSON.stringify({
                  maxEntries: 10000,
                  maxReferencedBytes: 1024 * 1024 * 1024,
                  maxSpoolBytes: 32 * 1024 * 1024,
                }),
                MORIA_NATIVE_STORAGE_BINDINGS: JSON.stringify(
                  bindings.map(({ namespace, root }) => ({
                    namespace,
                    provider: {
                      kind: 'posix',
                      root,
                      limits: { maxObjectBytes: 64 * 1024 * 1024, chunkBytes: 1024 * 1024 },
                    },
                  })),
                ),
                MORIA_NATIVE_CATALOG_FIXTURE: JSON.stringify({
                  root: join(db.root, 'records'),
                  token,
                  binding: producer,
                  snapshots: [],
                  ...(fixture as object),
                }),
              },
              timeout: 90000,
              maxBuffer: 1024 * 1024,
            },
          );
          assert.match(result.stdout, /1 passed; 0 failed; 0 ignored;/);
          return result.stdout;
        };
        const nativeTable = (
          await db.pool.query(
            'SELECT iceberg_uuid FROM moria.native_storage_namespaces WHERE environment=$1 AND resource=$2',
            [environment, dataset],
          )
        ).rows[0].iceberg_uuid;
        await native({
          mode: 'writer-supplied',
          snapshots: [
            {
              resource: dataset,
              ...namespace,
              metadata: 'unused',
              snapshotId: '1',
              tableUuid: nativeTable,
              objects: {},
            },
          ],
          writerRows: { [dataset]: [] },
        });
        await native({ mode: 'index' });
        assert.equal((await (await get()).json()).object.name, 'created');
        const projected = await fetch(`${second.url}/api/v1/objects/${object}/query`, {
          method: 'POST',
          headers,
          body: JSON.stringify({ sort: [{ property: 'name', direction: 'asc' }] }),
        });
        assert.equal(projected.status, 200, await projected.clone().text());
        assert.deepEqual(
          (await projected.json()).objects.map((row: any) => row.uid).sort(),
          [created, other, third].sort(),
        );
        const generation = (
          await db.pool.query(
            'SELECT stage FROM moria.source_generations WHERE environment=$1 AND object_type=$2 ORDER BY revision DESC LIMIT 1',
            [environment, object],
          )
        ).rows[0];
        assert(generation.stage);
        assert.equal(
          (
            await db.pool.query(
              "SELECT row_count::int FROM moria.runtime_projections WHERE stage=$1 AND status='active'",
              [generation.stage],
            )
          ).rows[0].row_count,
          3,
        );
        const mappedFile = (await client.list(published.root.id)).entries.find(
          (entry) => entry.name === 'mapped.link.json',
        )!;
        const mappedDeclaration = {
          apiVersion: 'moria/v1alpha2',
          kind: 'LinkType',
          metadata: { namespace: 'source_runtime', name: 'mapped' },
          spec: {
            sourceType: 'source_runtime/ObjectType/Record',
            targetType: 'source_runtime/ObjectType/Record',
            mode: 'mapped',
            cardinality: 'oneToOne',
            sourceProperty: 'name',
            targetProperty: 'name',
          },
        };
        const mappedBytes = new TextEncoder().encode(JSON.stringify(mappedDeclaration));
        await client.writeFile(
          {
            op: 'write',
            id: mappedFile.id,
            expectedRevision: mappedFile.revision,
            content: {
              size: mappedBytes.length,
              digest: `sha256:${createHash('sha256').update(mappedBytes).digest('hex')}`,
              mediaType: 'application/json',
            },
          },
          mappedBytes,
          { idempotencyKey: randomUUID() },
        );
        const republished = await publishBrowserSource(client, published.root.id);
        const mappedPlan = await client.plan(published.root.id, republished.digest);
        const mappedConfiguration = (await client.apply(mappedPlan.id)) as any;
        assert.equal(mappedConfiguration.runtimeConfigured, true);
        assert.equal((await traverse(mappedLink, created)).status, 409);
        await native({ mode: 'index' });
        const mappedReady = await traverse(mappedLink, created);
        assert.equal(mappedReady.status, 200, await mappedReady.clone().text());
        assert.deepEqual(
          (await mappedReady.json()).objects.map((row: any) => row.uid),
          [created],
        );
        const beforeDuplicate = (
          await db.pool.query(
            'SELECT count(*)::int n FROM moria.native_object_creations WHERE environment=$1',
            [environment],
          )
        ).rows[0].n;
        const duplicate = await invoke(second.url, createAction, randomUUID(), {
          name: 'other',
          key: 'duplicate-name',
        });
        assert.equal(duplicate.status, 409, await duplicate.clone().text());
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*)::int n FROM moria.native_object_creations WHERE environment=$1',
              [environment],
            )
          ).rows[0].n,
          beforeDuplicate,
        );
        const removed = await invoke(second.url, deleteAction, randomUUID(), { uid: third });
        assert.equal(removed.status, 200, await removed.clone().text());
        const replacement = await invoke(second.url, createAction, randomUUID(), {
          name: 'third',
          key: 'temporary-replacement',
        });
        assert.equal(replacement.status, 200, await replacement.clone().text());
        const replacementId = (await replacement.json()).created[0].uid;
        const blockedRestore = await invoke(second.url, restoreAction, randomUUID(), {
          uid: third,
        });
        assert.equal(blockedRestore.status, 409, await blockedRestore.clone().text());
        assert.equal(
          (await invoke(second.url, deleteAction, randomUUID(), { uid: replacementId })).status,
          200,
        );
        assert.equal(
          (await invoke(second.url, restoreAction, randomUUID(), { uid: third })).status,
          200,
        );
        const wideName = 'wide-key-'.repeat(1024);
        const wide = await invoke(second.url, createAction, randomUUID(), {
          name: wideName,
          key: 'wide-key',
        });
        assert.equal(wide.status, 200, await wide.clone().text());
        const wideId = (await wide.json()).created[0].uid;
        const wideTraversal = await traverse(mappedLink, wideId);
        assert.equal(wideTraversal.status, 200, await wideTraversal.clone().text());
        assert.deepEqual(
          (await wideTraversal.json()).objects.map((row: any) => row.uid),
          [wideId],
        );
        const wideDuplicate = await invoke(second.url, createAction, randomUUID(), {
          name: wideName,
          key: 'another-wide-key',
        });
        assert.equal(wideDuplicate.status, 409, await wideDuplicate.clone().text());
        const attach = (key: string, sourceDataset: string, expectedRevision: string) =>
          fetch(`${configured.url}/api/v1/objects/${object}/${created}/attach-source`, {
            method: 'POST',
            headers: { ...headers, 'idempotency-key': key },
            body: JSON.stringify({ sourceDataset, expectedRevision }),
          });
        const attachmentR = (await (await get()).json()).revision;
        assert.equal((await attach(randomUUID(), randomUUID(), attachmentR)).status, 409);
        assert.equal((await attach(randomUUID(), dataset, '0')).status, 409);
        const granted = (
          await db.pool.query(
            'SELECT operations FROM moria.permissions WHERE environment=$1 AND principal=$2 AND application=$3',
            [environment, principal, application],
          )
        ).rows[0].operations as string[];
        for (const denied of [`${dataset}:read`, `${object}:write`]) {
          await authority.setGrants(
            environment,
            'fixture',
            principal,
            application,
            granted.filter((operation) => operation !== denied),
          );
          assert.equal((await attach(randomUUID(), dataset, attachmentR)).status, 403);
          await authority.setGrants(environment, 'fixture', principal, application, granted);
        }
        const attachmentKey = randomUUID();
        const attached = await attach(attachmentKey, dataset, attachmentR);
        assert.equal(attached.status, 200, await attached.clone().text());
        const attachment = await attached.json();
        assert.deepEqual(
          await (await attach(attachmentKey, dataset, attachmentR)).json(),
          attachment,
        );
        assert.equal((await attach(attachmentKey, dataset, attachment.revision)).status, 409);
        await db.pool.query('SELECT moria.runtime_object_import_check($1,$2,$3,$4)', [
          environment,
          object,
          [created],
          dataset,
        ]);
        const snapshot = {
          resource: dataset,
          ...namespace,
          metadata: 'unused',
          snapshotId: '1',
          tableUuid: nativeTable,
          objects: {},
        };
        await native({
          mode: 'writer-supplied',
          binding: await createProducer(),
          snapshots: [snapshot],
          writerRows: { [dataset]: [{ id: created, name: 'from-dataset' }] },
        });
        await native({ mode: 'index' });
        assert.equal((await (await get()).json()).object.name, 'created');
        assert.equal(
          (await invoke(second.url, clearAction, randomUUID(), { uid: created })).status,
          200,
        );
        assert.equal((await (await get()).json()).object.name, 'from-dataset');
        const retainedStage = (
          await db.pool.query(
            'SELECT stage FROM moria.source_generations WHERE environment=$1 AND object_type=$2 ORDER BY revision DESC LIMIT 1',
            [environment, object],
          )
        ).rows[0].stage;
        await native({
          mode: 'writer-supplied',
          binding: await createProducer(),
          snapshots: [snapshot],
          writerRows: { [dataset]: [] },
        });
        await assert.rejects(native({ mode: 'index' }), /INDEXING_INCOMPLETE/);
        assert.equal(
          (
            await db.pool.query(
              'SELECT stage FROM moria.source_generations WHERE environment=$1 AND object_type=$2 ORDER BY revision DESC LIMIT 1',
              [environment, object],
            )
          ).rows[0].stage,
          retainedStage,
        );
        assert.equal((await (await get()).json()).object.name, 'from-dataset');
        const appKey = randomUUID() + randomUUID();
        const hosted = await start(profiles, {
          MORIA_NATIVE_BROWSER_BINDING: JSON.stringify({
            environment,
            application: browserApplication,
          }),
          MORIA_NATIVE_RECOVERY_PRINCIPAL: principal,
          MORIA_NATIVE_CLI_KEY: appKey,
          MORIA_NATIVE_CLI_PROFILES: JSON.stringify({
            operator: { environment, principal, application: browserApplication },
          }),
          MORIA_NATIVE_APP_HOST: JSON.stringify({
            directory: appHost.root,
            app: 'source-browser',
            entryPath: '/',
            environment,
            application: browserApplication,
            sourceResource: browserApplication,
          }),
        });
        assert.equal((await fetch(hosted.url)).status, 401);
        const navigation = await fetch(hosted.url, {
          headers: { accept: 'text/html' },
          redirect: 'manual',
        });
        assert.equal(navigation.status, 303);
        assert.equal(navigation.headers.get('location'), '/api/auth/bootstrap');
        const shell = await fetch(hosted.url + '/api/auth/bootstrap');
        assert.equal(shell.status, 200);
        assert((await shell.text()).includes('Sign in to Moria'));
        assert.equal(
          (
            await fetch(hosted.url + `/releases/${frontend.version}/index.html`, {
              headers: { accept: 'text/html' },
              redirect: 'manual',
            })
          ).status,
          401,
        );
        assert.equal(
          (await fetch(hosted.url, { headers: { authorization: `Bearer ${token}` } })).status,
          403,
        );
        const ticketResponse = await fetch(`${hosted.url}/api/auth/recovery-ticket`, {
          method: 'POST',
          headers: { 'x-moria-app-key': appKey },
        });
        assert.equal(ticketResponse.status, 200, await ticketResponse.clone().text());
        const recoveryURL = new URL((await ticketResponse.json()).url);
        const recovered = await fetch(`${hosted.url}/api/auth/recover`, {
          method: 'POST',
          headers: { origin: hosted.url, 'content-type': 'application/json' },
          body: JSON.stringify({
            ticket: new URLSearchParams(recoveryURL.hash.slice(1)).get('recovery'),
          }),
        });
        assert.equal(recovered.status, 200, await recovered.clone().text());
        const cookie = recovered.headers
          .getSetCookie()
          .find((value) => value.includes('Path=/;'))!
          .split(';')[0];
        assert(
          recovered.headers.getSetCookie().some((value) => value.includes('Path=/api/; Max-Age=0')),
        );
        const browserTransport = createTransport({
          baseUrl: hosted.url,
          fetch: (url, init) =>
            fetch(url, { ...init, headers: { ...init?.headers, origin: hosted.url, cookie } }),
        });
        const browserSession = await browserTransport.auth.session();
        assert.equal(browserSession.user?.principal, principal);
        const html = await fetch(hosted.url, { headers: { cookie } });
        assert.equal(html.status, 200, await html.clone().text());
        assert.equal(html.headers.get('x-moria-version'), frontend.version);
        assert(html.headers.get('content-security-policy')!.includes("script-src 'self'"));
        assert(!html.headers.get('content-security-policy')!.includes('unsafe-eval'));
        const document = await html.text();
        assert(document.includes(`<base href="/releases/${frontend.version}/">`));
        const script = Object.keys(frontend.files).find((name) => name.endsWith('.js'))!;
        assert.equal(
          (
            await fetch(`${hosted.url}/releases/${frontend.version}/${script}`, {
              headers: { cookie },
            })
          ).status,
          200,
        );
        const contract = await browserTransport.request<any>('/api/v1/contract', 'GET');
        assert.equal(contract.objects.Record.id, object);
        assert.equal(contract.actions.create.id, createAction);
        const browserCreated = await actionClient(
          browserTransport,
          contract.actions.create.id,
          contract.actions.create.inputs,
        )({ name: 'browser-created', key: 'browser-created' }, { idempotencyKey: randomUUID() });
        const browserUID = browserCreated.created![0].uid;
        const browserObject = await objectClient<any>(
          browserTransport,
          contract.objects.Record.id,
          contract.objects.Record.properties,
        ).get(browserUID);
        assert.equal(browserObject.object.name, 'browser-created');
        const assertBrowserCreated = async (name: string) => {
          const page = await objectClient<any>(
            browserTransport,
            object,
            contract.objects.Record.properties,
          ).query({
            filter: { op: 'eq', args: [{ field: 'name' }, { literal: name }] },
            projection: ['uid', 'name'],
            pageSize: 10,
          });
          assert.equal(
            page.objects.length,
            1,
            'Each browser/transport operation must create exactly one effective protected object: ' +
              name,
          );
          assert.equal(page.objects[0].name, name);
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*)::int n FROM moria.native_object_creations WHERE environment=$1 AND object_type=$2 AND uid=$3',
                [environment, object, page.objects[0].uid],
              )
            ).rows[0].n,
            1,
            'Protected readback must identify a real action-created UID',
          );
        };
        await assertBrowserCreated('browser-created');

        assert.equal(
          (
            await fetch(`${hosted.url}/api/management/v1/fs/entries/${published.root.id}`, {
              headers: { cookie },
            })
          ).status,
          404,
        );
        await assert.rejects(
          browserTransport.request(`/api/v1/objects/${randomUUID()}/${browserUID}`, 'GET'),
          { status: 404 },
        );

        const foreign = await fetch(`${hosted.url}/api/v1/actions/${createAction}/invoke`, {
          method: 'POST',
          headers: {
            origin: 'http://foreign.test',
            cookie,
            'content-type': 'application/json',
            'idempotency-key': randomUUID(),
          },
          body: JSON.stringify({ input: { name: 'denied', key: 'denied' } }),
        });
        assert.equal(foreign.status, 403);
        if (process.env.MORIA_NATIVE_BROWSER_PROOF_FILE) {
          const destination = process.env.MORIA_NATIVE_BROWSER_PROOF_FILE;
          const local = JSON.parse(await readFile(join(db.root, 'credentials.json'), 'utf8'));
          browserImage = await functionImage();
          const functionSource = await publishNativeSource(
            initial.url,
            token,
            [
              {
                path: '.repository.json',
                text: JSON.stringify({
                  version: 1,
                  name: 'Browser functions',
                  namespace: 'browser_logic',
                  runtime: 'typescript-v1',
                }),
              },
              {
                path: 'function.ts',
                text: 'export default async function(input){return {value:input.value+1};}',
              },
            ],
            'repository',
            [
              {
                apiVersion: 'moria/v1alpha2',
                kind: 'Function',
                metadata: { namespace: 'browser_logic', name: 'increment' },
                spec: {
                  artifact: browserImage.artifact,
                  runtime: 'typescript-v1',
                  inputs: { value: { type: 'float64', nullable: false } },
                  outputs: { value: { type: 'float64', nullable: false } },
                  effect: 'readOnly',
                  limits: { timeoutSeconds: 20, memoryMiB: 128, cpuMillis: 5000 },
                },
              },
            ],
          );
          const fn = functionSource.resource('browser_logic/Function/increment');
          const granted: string[] = (
            await db.pool.query(
              'SELECT operations FROM moria.permissions WHERE environment=$1 AND principal=$2 AND application=$3',
              [environment, principal, application],
            )
          ).rows[0].operations;
          const scoped = [...new Set([...granted, `${fn}:invoke`])];
          await authority.setApplication(environment, 'browser-fixture', application, scoped);
          await authority.setGrants(environment, 'browser-fixture', principal, application, scoped);
          const worker = randomUUID(),
            host = randomUUID();
          await db.pool.query(
            "INSERT INTO moria.runtime_function_workers(owner,host_id,incarnation,resource_classes,expires_at) SELECT $1,$2,incarnation,ARRAY['standard'],clock_timestamp()+interval '1 hour' FROM moria.indexing_instance",
            [worker, host],
          );
          const workerScratch = join(db.root, 'native-worker-scratch');
          await mkdir(workerScratch, { mode: 0o700 });
          const browserManagement = await start(
            [
              ...profiles,
              {
                environment,
                projectId: functionSource.role.projectId,
                reference: 'browser_logic/Function/increment',
                configuration: { resourceClass: 'standard' },
              },
            ],
            {
              MORIA_NATIVE_WORKERS: '["functions","indexing"]',
              MORIA_FUNCTION_WORKER_OWNER: worker,
              MORIA_FUNCTION_HOST_ID: host,
              MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT: workerScratch,
              MORIA_NATIVE_CLOSURE_LIMITS: JSON.stringify({
                maxEntries: 10000,
                maxReferencedBytes: 1024 * 1024 * 1024,
                maxSpoolBytes: 32 * 1024 * 1024,
              }),
            },
            true,
          );
          const functionClient = filesystemClient(
            createTransport({
              baseUrl: browserManagement.url,
              fetch: (url, init) =>
                fetch(url, {
                  ...init,
                  headers: { ...init?.headers, authorization: `Bearer ${token}` },
                }),
            }),
          );
          const functionPlan = await functionClient.plan(
            functionSource.root.id,
            functionSource.publication.digest,
          );
          await functionClient.apply(functionPlan.id);
          const functionSmoke = await fetch(
            browserManagement.url + `/api/v1/functions/${fn}/invoke`,
            {
              method: 'POST',
              headers: {
                authorization: `Bearer ${token}`,
                'content-type': 'application/json',
                'idempotency-key': randomUUID(),
              },
              body: JSON.stringify({ input: { value: 1 }, wait: true }),
            },
          );
          assert.equal(functionSmoke.status, 200, await functionSmoke.clone().text());
          assert.equal((await functionSmoke.json()).result.value, 2);

          const beforeBrowserIndex = (
            await db.pool.query(
              'SELECT stage FROM moria.source_generations WHERE environment=$1 AND object_type=$2 ORDER BY revision DESC LIMIT 1',
              [environment, object],
            )
          ).rows[0].stage;
          await native({
            mode: 'writer-supplied',
            binding: await createProducer(),
            snapshots: [
              {
                resource: dataset,
                ...namespace,
                metadata: 'unused',
                snapshotId: '1',
                tableUuid: nativeTable,
                objects: {},
              },
            ],
            writerRows: { [dataset]: [{ id: created, name: 'from-dataset' }] },
          });
          for (let attempt = 0; attempt < 3000; attempt++) {
            const current = (
              await db.pool.query(
                'SELECT stage FROM moria.source_generations WHERE environment=$1 AND object_type=$2 ORDER BY revision DESC LIMIT 1',
                [environment, object],
              )
            ).rows[0].stage;
            if (current !== beforeBrowserIndex) break;
            const failed =
              attempt % 50 === 0 &&
              (
                await db.pool.query(
                  "SELECT EXISTS(SELECT 1 FROM moria.indexing_builds WHERE environment=$1 AND target=$2 AND source_revision=(SELECT revision FROM moria.dataset_heads WHERE environment=$1 AND dataset=$3) AND status='failed') OR EXISTS(SELECT 1 FROM moria.indexing_blocked_intents WHERE environment=$1 AND target=$2 AND source_revision=(SELECT revision FROM moria.dataset_heads WHERE environment=$1 AND dataset=$3)) AS failed",
                  [environment, object, dataset],
                )
              ).rows[0].failed;
            if (attempt === 2999 || browserManagement.process.exitCode !== null || failed) {
              const builds = (
                await db.pool.query(
                  'SELECT status,error,source_revision::text FROM moria.indexing_builds WHERE environment=$1 AND target=$2 ORDER BY created_at DESC LIMIT 5',
                  [environment, object],
                )
              ).rows.map((row) => ({
                ...row,
                error:
                  typeof row.error === 'string' && /^[A-Z0-9_]{1,128}$/.test(row.error)
                    ? row.error
                    : row.error === null
                      ? null
                      : 'unclassified',
              }));
              const blocked = (
                await db.pool.query(
                  'SELECT error,source_revision::text FROM moria.indexing_blocked_intents WHERE environment=$1 AND target=$2 ORDER BY source_revision DESC LIMIT 5',
                  [environment, object],
                )
              ).rows;
              console.log(
                JSON.stringify({
                  event: 'browser-fixture-indexing-blocked',
                  builds,
                  blocked,
                  process: browserManagement.diagnostics(),
                }),
              );
              throw new Error(
                'Default native indexing child did not publish the refreshed Dataset',
              );
            }
            await delay(20);
          }
          const managementTicket = await fetch(
            browserManagement.url + '/api/auth/recovery-ticket',
            { method: 'POST', headers: { 'x-moria-app-key': local.accessKey } },
          );
          assert.equal(managementTicket.status, 200, await managementTicket.clone().text());
          const appTicket = await fetch(hosted.url + '/api/auth/recovery-ticket', {
            method: 'POST',
            headers: { 'x-moria-app-key': appKey },
          });
          assert.equal(appTicket.status, 200, await appTicket.clone().text());
          await writeFile(
            destination,
            JSON.stringify(
              {
                management: {
                  url: browserManagement.url,
                  recoveryURL: (await managementTicket.json()).url,
                },
                originalManagement: { url: initial.url },
                function: fn,
                functionSourceRoot: functionSource.root.id,
                application: { url: hosted.url, recoveryURL: (await appTicket.json()).url },
                sourceRoot: published.root.id,
                object,
                createAction,
                dataset,
                environment,
                fixture: 'native-source-runtime',
                note: 'Temporary disposable fixture credentials. Continue by creating the adjacent .continue file after browser verification.',
              },
              null,
              2,
            ),
            { mode: 0o600, flag: 'wx' },
          );
          for (let attempt = 0; attempt < 1800; attempt++) {
            const proceed = await access(destination + '.continue').then(
              () => true,
              () => false,
            );
            if (proceed) break;
            if (attempt === 1799) throw new Error('Native browser acceptance was not completed');
            await delay(1000);
          }
          for (const name of ['browser-ui-created', 'browser-created', 'browser-app-created']) {
            await assertBrowserCreated(name);
          }
          assert.equal(
            (
              await db.pool.query(
                "SELECT count(*)::int n FROM moria.runtime_function_receipts r JOIN moria.runtime_function_invocations i ON i.execution=r.execution WHERE i.entry_id=(SELECT id FROM moria.fs_entries WHERE resource_id=$1::uuid AND kind='resource' AND NOT deleted) AND r.result->>'value'='42'",
                [fn],
              )
            ).rows[0].n,
            1,
            'Management Function form must run the actual worker',
          );
          browserCreates = 2;
        }
        const currentFiles = (await client.checkout(published.root.id)).files;
        const sourceACLFiles = ['unlink.action.json', 'create.action.json'].map(
          (path) => currentFiles.find((file) => file.path === path)!.entry.id,
        );
        const instanceId = (
          await db.pool.query(
            'SELECT instance_id FROM moria.instance_principal_bindings WHERE environment=$1 AND principal=$2',
            [environment, principal],
          )
        ).rows[0].instance_id;
        const actionBatch = async () =>
          (
            await db.pool.query('SELECT moria.native_action_resource_access($1,$2) value', [
              createHash('sha256').update(token).digest('hex'),
              clearAction,
            ])
          ).rows[0].value;
        const batchBefore = await actionBatch();
        for (const demand of batchBefore.resources) {
          const scalar = (
            await db.pool.query('SELECT moria.native_serving_resource_access($1,$2,$3) labels', [
              createHash('sha256').update(token).digest('hex'),
              demand.resource,
              demand.operation,
            ])
          ).rows[0].labels;
          assert.deepEqual(
            [...demand.markings].sort(),
            [...scalar].sort(),
            'Published Action batch retains scalar current Source authority',
          );
        }
        for (const [index, file] of sourceACLFiles.entries()) {
          const prior = await client.policy(file);
          const denied = await client.setPolicy(file, prior.revision, {
            markings: prior.markings,
            grants: [
              ...prior.grants,
              {
                subject: { kind: 'principal', id: instanceId },
                operations: ['read'],
                inherit: false,
                effect: 'deny',
              },
            ],
          });
          assert.equal(
            (await fetch(hosted.url, { headers: { cookie } })).status,
            index === 0 ? 200 : 403,
          );
          assert.deepEqual(
            (await actionBatch()).resources,
            batchBefore.resources,
            'Unrelated sibling Action ACL cannot change clear Action authority',
          );
          assert.equal(
            (
              await objectClient<any>(
                browserTransport,
                object,
                contract.objects.Record.properties,
              ).get(browserUID)
            ).object.name,
            'browser-created',
          );
          if (index === 1)
            await assert.rejects(
              actionClient(
                browserTransport,
                createAction,
                contract.actions.create.inputs,
              )({ name: 'source-denied', key: 'source-denied' }, { idempotencyKey: randomUUID() }),
              { status: 403 },
            );
          await client.setPolicy(file, denied.revision, {
            markings: prior.markings,
            grants: prior.grants,
          });
        }
        const clearFile = currentFiles.find((file) => file.path === 'clear.action.json')!.entry.id;
        const clearPolicy = await client.policy(clearFile);
        const clearDenied = await client.setPolicy(clearFile, clearPolicy.revision, {
          markings: clearPolicy.markings,
          grants: [
            ...clearPolicy.grants,
            {
              subject: { kind: 'principal', id: instanceId },
              operations: ['read'],
              inherit: false,
              effect: 'deny',
            },
          ],
        });
        try {
          await assert.rejects(actionBatch(), /FORBIDDEN/);
        } finally {
          await client.setPolicy(clearFile, clearDenied.revision, {
            markings: clearPolicy.markings,
            grants: clearPolicy.grants,
          });
        }
        assert.deepEqual((await actionBatch()).resources, batchBefore.resources);
        assert.equal((await fetch(hosted.url, { headers: { cookie } })).status, 200);
        const untrusted = await db.pool.connect();
        try {
          await untrusted.query('BEGIN');
          await untrusted.query(`SET LOCAL ROLE ${role}`);
          await assert.rejects(
            untrusted.query('SELECT moria.native_filesystem_acl_state($1,$2,$3,false)', [
              {},
              published.root.id,
              'read',
            ]),
            { code: '42501' },
          );
        } finally {
          await untrusted.query('ROLLBACK');
          untrusted.release();
        }

        await authority.setGrants(
          environment,
          'fixture',
          principal,
          browserApplication,
          browserOperations.filter((value) => value !== `${object}:read`),
        );
        await assert.rejects(
          objectClient(browserTransport, object, contract.objects.Record.properties).get(
            browserUID,
          ),
          { status: 403 },
        );
        const narrowed = await browserTransport.request<any>('/api/v1/contract', 'GET');
        assert.equal(narrowed.objects.Record, undefined);
        await browserTransport.auth.logout();
        assert.equal((await fetch(hosted.url, { headers: { cookie } })).status, 401);
        await db.pool.query(
          'UPDATE moria.native_serving_bindings SET definition=jsonb_set(definition,\'{policies,sourceColumns,unmapped}\',\'{"type":"string","nullable":false}\'::jsonb) WHERE environment=$1 AND resource=$2',
          [environment, object],
        );
        const denied = await invoke(second.url, createAction, randomUUID(), {
          name: 'incomplete',
          key: 'missing-canonical',
        });
        assert.equal(denied.status, 422, await denied.clone().text());
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*)::int n FROM moria.native_object_creations WHERE environment=$1',
              [environment],
            )
          ).rows[0].n,
          6 + browserCreates,
        );
        const previousGeneration = (
          await db.pool.query(
            'SELECT authority_generation FROM moria.native_serving_bindings WHERE environment=$1 AND resource=$2',
            [environment, object],
          )
        ).rows[0].authority_generation;
        await db.pool.query(
          'UPDATE moria.native_serving_bindings SET enabled=false WHERE environment=$1 AND resource=$2',
          [environment, object],
        );
        await db.pool.query(
          'UPDATE moria.native_serving_bindings SET enabled=true WHERE environment=$1 AND resource=$2',
          [environment, object],
        );
        const nextGeneration = (
          await db.pool.query(
            'SELECT authority_generation FROM moria.native_serving_bindings WHERE environment=$1 AND resource=$2',
            [environment, object],
          )
        ).rows[0].authority_generation;
        assert.equal(BigInt(nextGeneration), BigInt(previousGeneration) + 2n);
        assert.equal((await client.runtimeStatus(published.root.id)).configured, false);
      },
    );
  },
);
