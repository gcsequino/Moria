import assert from 'node:assert/strict';
import test from 'node:test';
import { randomUUID, createHash } from 'node:crypto';
import { spawn, execFile, type ChildProcess } from 'node:child_process';
import { promisify } from 'node:util';
import { join } from 'node:path';
import { mkdir, writeFile } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { database } from './fixtures/catalog-storage/database.js';
import { assertNativeArtifacts } from './fixtures/native-common/artifact.js';
import { functionImage } from './fixtures/functions/image.js';
import { createLocalFilesystem } from '../services/filesystem/src/local.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { filesystemOperations } from '../packages/contracts/filesystem.js';
import { ObjectStore } from '../services/ontology/src/store.js';
import { fingerprint } from '../packages/compiler/src/index.js';
import type { Resource } from '../packages/contracts/types.js';
const exec = promisify(execFile);
test(
  'caught secondary link-cardinality failure retains its complete native outcome and rolls replay effects back',
  { timeout: 120000 },
  async (t) => {
    const binary = process.env.MORIA_RUNTIME_BINARY ?? '/usr/local/bin/moria-runtime';
    await assertNativeArtifacts(binary, []);
    const db = await database(),
      image = await functionImage(),
      runtime = await createLocalFilesystem(db.catalog, join(db.root, 'files')),
      authority = runtime.filesystem.security.authority;
    const processes: ChildProcess[] = [],
      exits: Promise<unknown>[] = [],
      repository = image.artifact.uri
        .slice(6)
        .replace(/\/[^/]+$/, '/moria-failed-action-' + randomUUID());
    t.after(async () => {
      for (const child of processes) child.kill('SIGKILL');
      await Promise.allSettled(exits);
      await exec('docker', ['image', 'rm', '--force', repository]).catch(() => {});
      await image.close();
      await db.close();
    });
    const environment = randomUUID(),
      application = 'function-action-outcome',
      principal = 'owner';
    await db.catalog.ensureEnvironment(environment);
    const operations = [
      'instance:manage',
      'functions:invoke',
      'records:read',
      'records:write',
      'record-link:read',
      'record-link:write',
      'link-record:invoke',
      'unlink-record:invoke',
      ...filesystemOperations.map((op) => 'filesystem:' + op),
    ];
    await authority.syncPrincipal(environment, 'fixture', principal, {
      kind: 'workload',
      enabled: true,
      attributes: {},
      markings: ['protected'],
    });
    await authority.setApplication(environment, 'fixture', application, operations);
    await authority.setGrants(environment, 'fixture', principal, application, operations);
    const sessions = new WorkloadSessions(authority),
      token = (await sessions.mint({ environment, application, principal }, 'fixture')).token,
      context = { session: await sessions.authenticate('Bearer ' + token) };
    const home = await runtime.management.ensureHome(context),
      role = await runtime.management.role(context, home.home.id),
      projects = await runtime.filesystem.get(context, role!.configuration.projects as string);
    await runtime.management.createProject(context, projects.id, 'outcome', projects.revision);
    const source = await runtime.filesystem.store.resolve(
        `/users/${home.home.name}/projects/outcome/source`,
      ),
      created = await runtime.filesystem.apply(context, randomUUID(), [
        {
          op: 'create',
          kind: 'directory',
          parentId: source.id,
          expectedParentRevision: source.revision,
          name: 'logic',
        },
      ]),
      root = created.entries.find((entry) => entry.name === 'logic')!;
    const descriptor = Buffer.from(
      JSON.stringify({ version: 1, name: 'Outcome', namespace: 'logic', runtime: 'typescript-v1' }),
    );
    await runtime.filesystem.writeFile(
      context,
      randomUUID(),
      {
        op: 'create',
        parentId: root.id,
        expectedParentRevision: (await runtime.filesystem.store.get(root.id)).revision,
        name: '.repository.json',
        content: {
          size: descriptor.length,
          digest: 'sha256:' + createHash('sha256').update(descriptor).digest('hex'),
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
    await runtime.source.register(
      context,
      root.id,
      (await runtime.filesystem.store.get(root.id)).revision,
      'repository',
    );
    await runtime.filesystem.security.setPolicy('fixture', root.id, '0', {
      markings: [{ environment, marking: 'protected' }],
      grants: [],
    });
    const code = join(db.root, 'guest');
    await mkdir(code);
    await writeFile(
      join(code, 'guest.mjs'),
      `export default async function(input,context){const response=await context.platform.fetch('/api/v1/actions/link-record/invoke',{method:'POST',headers:{'content-type':'application/json','idempotency-key':'link'},body:JSON.stringify({input:{uid:input.uid,target:input.target}})});context.log('native secondary outcome observed');return{value:response.status===409?1:0};}`,
    );
    await writeFile(
      join(code, 'Dockerfile'),
      `FROM ${image.artifact.uri.slice(6)}@${image.artifact.digest}\nCOPY guest.mjs /function/outcome.mjs\nCMD ["node","/function/runtime/runner.mjs","/function/outcome.mjs"]\n`,
    );
    await exec('docker', ['build', '--network=none', '--pull=false', '--tag', repository, code], {
      timeout: 30000,
      maxBuffer: 1048576,
    });
    const pushed = await exec('docker', ['push', repository], {
        timeout: 30000,
        maxBuffer: 1048576,
      }),
      digest = pushed.stdout.match(/digest: (sha256:[0-9a-f]{64})/)?.[1];
    assert(digest);
    const uidType = { type: 'uid' as const, nullable: false },
      scalar = { type: 'float64' as const, nullable: false };
    const definition: Resource = {
      apiVersion: 'moria/v1alpha2',
      kind: 'Function',
      metadata: { namespace: 'logic', name: 'caught' },
      spec: {
        artifact: { uri: 'oci://' + repository, digest },
        runtime: 'typescript-v1',
        inputs: { uid: uidType, target: uidType },
        outputs: { value: scalar },
        effect: 'readWrite',
        limits: { timeoutSeconds: 20, memoryMiB: 128, cpuMillis: 5000 },
      },
    };
    const published = await runtime.source.publish(
      context,
      root.id,
      (await runtime.source.checkout(context, root.id)).digest,
      [definition],
    );
    await runtime.source.apply(
      context,
      (await runtime.source.plan(context, root.id, published.digest)).id,
    );
    const fn = published.resources.find(
      (resource) => resource.reference === 'logic/Function/caught',
    )!.id;
    const [primary, other, target] = await Promise.all(
        ['primary', 'other', 'target'].map((key) =>
          db.catalog.resolveIdentity('function-outcome-' + root.id, key),
        ),
      ),
      schema = { primaryKey: 'uid', properties: { uid: uidType, value: scalar } },
      store = new ObjectStore(db.catalog);
    await store.publishStage(
      await store.stage(
        environment,
        'records',
        schema,
        [primary, other, target].map((uid, index) => ({ uid, value: index })),
        (await authority.current(environment, principal, application)).epoch,
        (await db.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [environment]))
          .rows[0].revision,
        undefined,
        ['protected'],
      ),
    );
    const binding = {
      objectTypeId: randomUUID(),
      schema,
      policies: {
        resource: 'records',
        markings: ['protected'],
        properties: schema.properties,
        sourceColumns: schema.properties,
        attributes: {},
      },
    };
    await db.pool.query(
      'INSERT INTO moria.native_serving_bindings(environment,resource,object_type_id,revision,definition) VALUES($1,$2,$3,$4,$5)',
      [environment, 'records', binding.objectTypeId, fingerprint(binding), binding],
    );
    const link = { mode: 'actionManaged', cardinality: 'oneToOne', markings: ['protected'] };
    await db.pool.query(
      'INSERT INTO moria.native_serving_links(environment,resource,revision,source_resource,target_resource,definition) VALUES($1,$2,$3,$4,$4,$5)',
      [environment, 'record-link', 'v1', 'records', link],
    );
    for (const op of ['link', 'unlink']) {
      const action = {
        resource: op + '-record',
        revision: 'v1',
        markings: ['protected'],
        targetParameter: 'uid',
        inputs: { uid: uidType, target: uidType },
        condition: { literal: true },
        edits: [],
        typedEdits: [
          {
            op,
            linkType: 'record-link',
            sourceUid: { parameter: 'uid' },
            targetUid: { parameter: 'target' },
          },
        ],
      };
      await db.pool.query(
        'INSERT INTO moria.native_serving_actions(environment,resource,object_resource,revision,definition) VALUES($1,$2,$3,$4,$5)',
        [environment, action.resource, 'records', 'v1', action],
      );
    }
    const dbRole = 'outcome_' + randomUUID().replaceAll('-', ''),
      password = randomUUID();
    await db.pool.query(
      `CREATE ROLE ${dbRole} LOGIN PASSWORD '${password}';GRANT USAGE ON SCHEMA moria TO ${dbRole};GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${dbRole};GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${dbRole}`,
    );
    const host = randomUUID(),
      owner = randomUUID();
    await db.pool.query(
      "INSERT INTO moria.runtime_function_workers(owner,host_id,incarnation,resource_classes,expires_at) SELECT $1,$2,incarnation,ARRAY['standard'],clock_timestamp()+interval '1 hour' FROM moria.indexing_instance",
      [owner, host],
    );
    const env = {
      PATH: process.env.PATH,
      MORIA_INDEXING_DATABASE_SECRET_REF: 'OUTCOME_DATABASE',
      OUTCOME_DATABASE: `postgresql://${dbRole}:${password}@127.0.0.1:${db.pool.options.port}/postgres`,
      MORIA_INDEXING_DATABASE_TLS: 'disable',
      MORIA_FUNCTION_HOST_ID: host,
      MORIA_FUNCTION_WORKER_OWNER: owner,
    };
    let address = '';
    const server = spawn(binary, ['serve'], {
      env: { ...env, MORIA_NATIVE_LISTEN: '127.0.0.1:0' },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    processes.push(server);
    exits.push(new Promise((done) => server.once('exit', done)));
    let output = '',
      errors = '';
    server.stdout!.on('data', (bytes) => (output += bytes));
    server.stderr!.on('data', (bytes) => (errors += bytes));
    for (let attempt = 0; attempt < 500 && !address; attempt++) {
      const line = output.split('\n').find((line) => line.includes('"event":"serving"'));
      if (line) address = 'http://' + JSON.parse(line).address;
      else {
        assert.equal(server.exitCode, null, errors);
        await delay(10);
      }
    }
    assert(address, errors);
    const request = async (path: string, body?: unknown) => {
      const response = await fetch(address + path, {
        method: body === undefined ? 'GET' : 'POST',
        headers: {
          authorization: 'Bearer ' + token,
          'content-type': 'application/json',
          'idempotency-key': randomUUID(),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      });
      return { response, value: await response.json() };
    };
    const initial = await request('/api/v1/actions/link-record/invoke', {
      input: { uid: other, target },
    });
    assert.equal(initial.response.status, 200, JSON.stringify(initial.value));
    const accepted = await request(`/api/v1/functions/${fn}/invoke`, {
      input: { uid: primary, target },
      wait: false,
    });
    assert.equal(accepted.response.status, 202, JSON.stringify(accepted.value));
    const worker = await exec(binary, ['functions', 'once'], {
      env,
      timeout: 30000,
      maxBuffer: 1048576,
    });
    assert.equal(JSON.parse(worker.stdout).worked, true);
    const released = await request('/api/v1/invocations/' + accepted.value.id);
    assert.equal(released.response.status, 200, JSON.stringify(released.value));
    assert.equal(released.value.status, 'succeeded', JSON.stringify(released.value));
    assert.equal(released.value.result.value, 1);
    const evidence = (
      await db.pool.query('SELECT evidence FROM moria.native_function_reads WHERE execution=$1', [
        accepted.value.id,
      ])
    ).rows;
    assert(!JSON.stringify(evidence).includes(other));
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*)::int n FROM moria.native_function_failed_actions WHERE execution=$1',
          [accepted.value.id],
        )
      ).rows[0].n,
      1,
    );
    const removed = await request('/api/v1/actions/unlink-record/invoke', {
      input: { uid: other, target },
    });
    assert.equal(removed.response.status, 200, JSON.stringify(removed.value));
    const counts = async () =>
      (
        await db.pool.query(
          'SELECT revision::text,(SELECT count(*)::int FROM moria.native_link_edge_versions) AS edges,(SELECT count(*)::int FROM moria.action_keys) AS receipts FROM moria.environments WHERE id=$1',
          [environment],
        )
      ).rows[0];
    const before = await counts();
    const withheld = await request('/api/v1/invocations/' + accepted.value.id);
    assert.equal(withheld.response.status, 409, JSON.stringify(withheld.value));
    assert.equal(
      (await request('/api/v1/invocations/' + accepted.value.id + '/events')).response.status,
      409,
    );
    assert.deepEqual(await counts(), before);
  },
);
