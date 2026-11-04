import assert from 'node:assert/strict';
import { spawn, execFile, type ChildProcess } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { database } from './fixtures/catalog-storage/database.js';
import { functionImage } from './fixtures/functions/image.js';
import { createLocalFilesystem } from '../services/filesystem/src/local.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { filesystemOperations } from '../packages/contracts/filesystem.js';
import { ObjectStore } from '../services/ontology/src/store.js';
import { fingerprint } from '../packages/compiler/src/index.js';
import type { Resource } from '../packages/contracts/types.js';

const exec = promisify(execFile);
test(
  'native function global aggregate evidence is bounded and protects retained results and logs',
  { timeout: 180000 },
  async (t) => {
    const db = await database();
    const children: ChildProcess[] = [];
    let image: Awaited<ReturnType<typeof functionImage>> | undefined;
    let repository: string | undefined;
    t.after(async () => {
      for (const child of children) child.kill('SIGKILL');
      await Promise.all(
        children.map((child) =>
          child.exitCode === null
            ? new Promise((resolve) => child.once('exit', resolve))
            : Promise.resolve(),
        ),
      );
      if (repository) await exec('docker', ['image', 'rm', '--force', repository]).catch(() => {});
      await image?.close();
      await db.close();
    });
    image = await functionImage();
    const directory = await mkdtemp(join(tmpdir(), 'native-aggregate-guest-'));
    let artifact: { uri: string; digest: string };
    try {
      repository = image.artifact.uri.slice(6).replace(/\/[^/]+$/, '/native-aggregate-guest');
      await writeFile(
        join(directory, 'aggregate.mjs'),
        `export default async function(input,context){const response=await context.platform.fetch('/api/v1/objects/records/aggregate',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({metrics:{sum:{op:'sum',property:'value'}},budget:{scanRows:1000000,groups:100,responseBytes:1000000,timeMs:30000}})});if(input.mode==='denied')return{value:response.status===403?1:0};if(!response.ok)throw new Error('aggregate denied');const result=await response.json();context.log('aggregate observed');return{value:result.groups[0].values.sum};}`,
      );
      await writeFile(
        join(directory, 'Dockerfile'),
        `FROM ${image.artifact.uri.slice(6)}@${image.artifact.digest}\nCOPY aggregate.mjs /function/aggregate.mjs\nCMD ["node","/function/runtime/runner.mjs","/function/aggregate.mjs"]\n`,
      );
      await exec(
        'docker',
        ['build', '--network=none', '--pull=false', '--tag', repository, directory],
        { timeout: 30000, maxBuffer: 1048576 },
      );
      const pushed = await exec('docker', ['push', repository], {
        timeout: 30000,
        maxBuffer: 1048576,
      });
      const digest = pushed.stdout.match(/digest: (sha256:[0-9a-f]{64})/)?.[1];
      assert.ok(digest);
      artifact = { uri: `oci://${repository}`, digest };
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
    const environment = randomUUID(),
      application = 'aggregate-app',
      principal = 'reader';
    await db.catalog.ensureEnvironment(environment);
    const runtime = await createLocalFilesystem(db.catalog, join(db.root, 'files'));
    const authority = runtime.filesystem.security.authority;
    const operations = [
      'instance:manage',
      'functions:invoke',
      'records:read',
      ...filesystemOperations.map((op) => `filesystem:${op}`),
    ];
    await authority.setApplication(environment, 'fixture', application, operations);
    await authority.syncPrincipal(environment, 'fixture', principal, {
      kind: 'workload',
      enabled: true,
      attributes: {},
      markings: ['protected'],
    });
    await authority.setGrants(environment, 'fixture', principal, application, operations);
    const sessions = new WorkloadSessions(authority);
    const issued = await sessions.mint({ environment, application, principal }, 'fixture');
    const context = { session: await sessions.authenticate(`Bearer ${issued.token}`) };
    const home = await runtime.management.ensureHome(context);
    const role = await runtime.management.role(context, home.home.id);
    const projects = await runtime.filesystem.get(context, role!.configuration.projects as string);
    await runtime.management.createProject(context, projects.id, 'aggregate', projects.revision);
    const source = await runtime.filesystem.store.resolve(
      `/users/${home.home.name}/projects/aggregate/source`,
    );
    const changes = await runtime.filesystem.apply(context, randomUUID(), [
      {
        op: 'create',
        kind: 'directory',
        parentId: source.id,
        expectedParentRevision: source.revision,
        name: 'logic',
      },
    ]);
    const root = changes.entries.find((entry) => entry.name === 'logic')!;
    const bytes = new TextEncoder().encode(
      JSON.stringify({
        version: 1,
        name: 'Aggregate',
        namespace: 'aggregate',
        runtime: 'typescript-v1',
      }),
    );
    await runtime.filesystem.writeFile(
      context,
      randomUUID(),
      {
        op: 'create',
        parentId: root.id,
        expectedParentRevision: root.revision,
        name: '.repository.json',
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
    await runtime.source.register(
      context,
      root.id,
      (await runtime.filesystem.store.get(root.id)).revision,
      'repository',
    );
    const scalar = { type: 'float64' as const, nullable: false };
    const definitions: Resource[] = ['read', 'proposal'].map((name) => ({
      apiVersion: 'moria/v1alpha2',
      kind: 'Function',
      metadata: { namespace: 'aggregate', name },
      spec: {
        artifact,
        runtime: 'typescript-v1',
        inputs: { mode: { type: 'string', nullable: false } },
        outputs: { value: scalar },
        effect: name === 'proposal' ? 'editProposal' : 'readOnly',
        limits: { timeoutSeconds: 20, memoryMiB: 128, cpuMillis: 5000 },
      },
    }));
    const publication = await runtime.source.publish(
      context,
      root.id,
      (await runtime.source.checkout(context, root.id)).digest,
      definitions,
    );
    await runtime.source.apply(
      context,
      (await runtime.source.plan(context, root.id, publication.digest)).id,
    );
    const resources = new Map(
      publication.resources.map((resource) => [resource.reference.split('/').at(-1)!, resource.id]),
    );
    const store = new ObjectStore(db.catalog),
      uid = await db.catalog.resolveIdentity(`aggregate-${environment}`, 'record');
    const schema = {
      primaryKey: 'uid',
      properties: { uid: { type: 'uid' as const, nullable: false }, value: scalar },
    };
    const publish = async (value: number) =>
      store.publishStage(
        await store.stage(
          environment,
          'records',
          schema,
          [{ uid, value }],
          (await authority.current(environment, principal, application)).epoch,
          (
            await db.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [
              environment,
            ])
          ).rows[0].revision,
          undefined,
          ['protected'],
        ),
      );
    await publish(10);
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
    const dbRole = `aggregate_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(
      `CREATE ROLE ${dbRole} LOGIN PASSWORD '${password}';GRANT USAGE ON SCHEMA moria TO ${dbRole};GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${dbRole};GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${dbRole};ALTER ROLE ${dbRole} SET temp_file_limit='256MB'`,
    );
    const host = randomUUID(),
      worker = randomUUID();
    await db.pool.query(
      "INSERT INTO moria.runtime_function_workers(owner,host_id,incarnation,resource_classes,expires_at) SELECT $1,$2,incarnation,ARRAY['standard'],clock_timestamp()+interval '1 hour' FROM moria.indexing_instance",
      [worker, host],
    );
    const env = {
      PATH: process.env.PATH,
      MORIA_INDEXING_DATABASE_SECRET_REF: 'AGGREGATE_DATABASE',
      AGGREGATE_DATABASE: `postgresql://${dbRole}:${password}@127.0.0.1:${db.pool.options.port}/postgres`,
      MORIA_INDEXING_DATABASE_TLS: 'disable',
      MORIA_FUNCTION_HOST_ID: host,
      MORIA_FUNCTION_WORKER_OWNER: worker,
    };
    const binary = process.env.MORIA_RUNTIME_BINARY ?? '/usr/local/bin/moria-indexing';
    const start = (args: string[], extra: Record<string, string> = {}) => {
      const child = spawn(binary, args, {
        env: { ...env, ...extra },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      children.push(child);
      let output = '',
        error = '';
      child.stdout!.on('data', (chunk) => (output += chunk));
      child.stderr!.on('data', (chunk) => (error += chunk));
      const done = new Promise<string>((resolve, reject) => {
        child.once('error', reject);
        child.once('exit', (code) =>
          code === 0
            ? resolve(output)
            : reject(new Error(`native ${args.join(' ')} ${code}: ${error}`)),
        );
      });
      void done.catch(() => {});
      return { child, done, output: () => output, error: () => error };
    };
    const server = start(['serve'], { MORIA_NATIVE_LISTEN: '127.0.0.1:0' });
    let endpoint = '';
    for (let i = 0; i < 1000 && !endpoint; i++) {
      assert.equal(server.child.exitCode, null, server.error());
      for (const line of server.output().split('\n'))
        try {
          const value = JSON.parse(line);
          if (value.event === 'serving') endpoint = `http://${value.address}`;
        } catch {}
      if (!endpoint) await delay(10);
    }
    assert.ok(endpoint);
    const request = async (path: string, method = 'GET', body?: unknown, key?: string) => {
      const response = await fetch(endpoint + path, {
        method,
        headers: {
          authorization: `Bearer ${issued.token}`,
          'content-type': 'application/json',
          ...(key ? { 'idempotency-key': key } : {}),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      });
      return { response, value: await response.json() };
    };
    const enqueue = async (name: string, mode = 'sum') => {
      const result = await request(
        `/api/v1/functions/${resources.get(name)}/invoke`,
        'POST',
        { input: { mode }, wait: false },
        randomUUID(),
      );
      assert.equal(result.response.status, 202, JSON.stringify(result.value));
      return result.value.id as string;
    };
    await t.test(
      'actual OCI code records a bounded summary and retained protected classification',
      async () => {
        const id = await enqueue('read');
        const processed = JSON.parse(await start(['functions', 'once']).done);
        assert.ok(processed);
        const result = await request(`/api/v1/invocations/${id}`);
        assert.equal(result.response.status, 200, JSON.stringify(result.value));
        assert.equal(result.value.status, 'succeeded');
        assert.equal(result.value.result.value, 10);
        assert.ok(
          result.value.markings.some((mark: { marking: string }) => mark.marking === 'protected'),
        );
        const ledger = (
          await db.pool.query(
            'SELECT evidence FROM moria.native_function_reads WHERE execution=$1',
            [id],
          )
        ).rows.flatMap((row) => row.evidence);
        const summary = ledger.find((read) => read.query?.aggregateSpec);
        assert.ok(summary);
        assert.deepEqual(summary.uids, []);
        assert.deepEqual(summary.fingerprints, {});
        assert.deepEqual(summary.policies, {});
        assert.match(summary.query.aggregateHash, /^sha256:[0-9a-f]{64}$/);
        assert.ok(JSON.stringify(summary).length < 4096);
        await publish(10);
        assert.equal((await request(`/api/v1/invocations/${id}`)).response.status, 200);
        await publish(11);
        const denied = await request(`/api/v1/invocations/${id}`);
        assert.equal(denied.response.status, 409, JSON.stringify(denied.value));
        assert.equal(denied.value.error.code, 'FUNCTION_INPUT_CHANGED');
        assert.equal((await request(`/api/v1/invocations/${id}/events`)).response.status, 409);
      },
    );
    await t.test('edit-proposal ancestry cannot consume an aggregate', async () => {
      const id = await enqueue('proposal', 'denied');
      await start(['functions', 'once']).done;
      const result = await request(`/api/v1/invocations/${id}`);
      assert.equal(result.response.status, 200, JSON.stringify(result.value));
      assert.equal(result.value.status, 'succeeded');
      assert.equal(result.value.result.value, 1);
      const rows = (
        await db.pool.query('SELECT evidence FROM moria.native_function_reads WHERE execution=$1', [
          id,
        ])
      ).rows;
      assert.ok(
        !rows.some((row) =>
          row.evidence.some(
            (read: { query?: { aggregateSpec?: unknown } }) => read.query?.aggregateSpec,
          ),
        ),
      );
    });
  },
);
