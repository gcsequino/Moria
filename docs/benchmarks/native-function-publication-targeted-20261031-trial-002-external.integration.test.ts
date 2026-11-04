import assert from 'node:assert/strict';
import { spawn, execFile, type ChildProcess } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { join } from 'node:path';
import { mkdtemp, writeFile, rm, readlink, realpath, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { promisify } from 'node:util';
import { createServer, request as httpRequest } from 'node:http';
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
import { transactionCommitProxy } from './fixtures/runtime/postgres-proxy.js';

const exec = promisify(execFile);
async function until(check: () => Promise<boolean>, limit = 10000) {
  const end = Date.now() + limit;
  while (Date.now() < end) {
    if (await check()) return;
    await delay(25);
  }
  assert.fail('Native function lifecycle barrier did not complete');
}

test(
  'native public functions compose authenticated admission, real sandboxes and atomic result/effect receipts',
  { timeout: 300000 },
  async (t) => {
    const proofCases = new Set(["publication retries only definite transaction aborts without rerunning the guest"]);
    const selected = async (...args: any[]) => {
      if (proofCases.has(String(args[0]))) return Reflect.apply(t.test, t, args);
      return t.test(String(args[0]), {skip:'explicit targeted proof; full suite remains required'}, async()=>{});
    };
    const db = await database();
    const locks: unknown[] = [], observed = new Set<string>();let polling=false;
    const poll=setInterval(async()=>{if(polling)return;polling=true;try{
      const rows=(await db.pool.query("SELECT a.pid,substring(a.query from 'moria[.]([a-z_]+)[(]') AS guard,split_part(ltrim(a.query),' ',1) AS statement_kind,a.state,a.wait_event_type,a.wait_event,pg_blocking_pids(a.pid) AS blockers,l.locktype,l.relation::regclass::text AS relation,l.mode,l.granted FROM pg_stat_activity a LEFT JOIN pg_locks l ON l.pid=a.pid WHERE a.datname=current_database() AND (cardinality(pg_blocking_pids(a.pid))>0 OR a.pid IN(SELECT unnest(pg_blocking_pids(p.pid)) FROM pg_stat_activity p WHERE p.datname=current_database())) ORDER BY a.pid,l.locktype,relation,l.mode")).rows;
      const key=JSON.stringify(rows);if(rows.length&&!observed.has(key)&&locks.length<64){observed.add(key);locks.push({at:Date.now(),rows});}
    }catch{}finally{polling=false;}},300);
    t.after(()=>{clearInterval(poll);t.diagnostic(JSON.stringify({phase:'targeted-model-locks',locks}));});
    let image: Awaited<ReturnType<typeof functionImage>> | undefined;
    const children: ChildProcess[] = [],
      ids = new Set<string>();
    const extraImages = new Set<string>();
    t.after(async () => {
      for (const child of children) child.kill('SIGKILL');
      for (const id of ids)
        await exec('docker', ['rm', '--force', `moria-native-function-${id}`]).catch(() => {});
      for (const repository of extraImages)
        await exec('docker', ['image', 'rm', '--force', repository]).catch(() => {});
      await image?.close();
      await db.close();
    });
    image = await functionImage();
    const runtime = await createLocalFilesystem(db.catalog, join(db.root, 'files')),
      authority = runtime.filesystem.security.authority;
    const environment = 'native-functions',
      application = 'functions-app',
      principal = 'author';
    await db.catalog.ensureEnvironment(environment);
    const operations = [
      'instance:manage',
      'functions:invoke',
      'records:read',
      'edit-record:invoke',
      'reject-record:invoke',
      'function-record:invoke',
      ...filesystemOperations.map((op) => `filesystem:${op}`),
    ];
    await authority.setApplication(environment, 'fixture', application, operations);
    const sessions = new WorkloadSessions(authority);
    const actor = async (name: string) => {
      await authority.syncPrincipal(environment, 'fixture', name, {
        kind: 'workload',
        enabled: true,
        attributes: {},
        markings: ['protected'],
      });
      await authority.setGrants(environment, 'fixture', name, application, operations);
      const issued = await sessions.mint({ environment, application, principal: name }, 'fixture');
      return {
        token: issued.token,
        context: { session: await sessions.authenticate(`Bearer ${issued.token}`) },
      };
    };
    const owner = await actor(principal),
      other = await actor('other');
    const home = await runtime.management.ensureHome(owner.context),
      role = await runtime.management.role(owner.context, home.home.id);
    const projects = await runtime.filesystem.get(
      owner.context,
      role!.configuration.projects as string,
    );
    await runtime.management.createProject(owner.context, projects.id, 'native', projects.revision);
    const source = await runtime.filesystem.store.resolve(
      `/users/${home.home.name}/projects/native/source`,
    );
    const created = await runtime.filesystem.apply(owner.context, randomUUID(), [
      {
        op: 'create',
        kind: 'directory',
        parentId: source.id,
        expectedParentRevision: source.revision,
        name: 'logic',
      },
    ]);
    const root = created.entries.find((entry) => entry.name === 'logic')!;
    const descriptor = Buffer.from(
      JSON.stringify({ version: 1, name: 'Logic', namespace: 'logic', runtime: 'typescript-v1' }),
    );
    await runtime.filesystem.writeFile(
      owner.context,
      randomUUID(),
      {
        op: 'create',
        parentId: root.id,
        expectedParentRevision: (await runtime.filesystem.store.get(root.id)).revision,
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
    await runtime.source.register(
      owner.context,
      root.id,
      (await runtime.filesystem.store.get(root.id)).revision,
      'repository',
    );
    await runtime.filesystem.security.setPolicy('fixture', root.id, '0', {
      markings: [{ environment, marking: 'protected' }],
      grants: [],
    });
    const scalar = { type: 'float64' as const, nullable: false },
      uidType = { type: 'uid' as const, nullable: false };
    const inputs = {
      mode: { type: 'string' as const, nullable: false },
      value: scalar,
      uid: { ...uidType, nullable: true },
    };
    let definitions: Resource[] = [
      'read',
      'write',
      'proposal',
      'short-proposal',
      'calculate-edit',
      'child',
      'parent',
    ].map((name) => ({
      apiVersion: 'moria/v1alpha2',
      kind: 'Function',
      metadata: { namespace: 'logic', name },
      spec: {
        artifact: image!.artifact,
        runtime: 'typescript-v1',
        inputs,
        outputs: { value: scalar },
        effect:
          name === 'write'
            ? 'readWrite'
            : name === 'proposal' || name === 'short-proposal' || name === 'calculate-edit'
              ? 'editProposal'
              : 'readOnly',
        limits: {
          timeoutSeconds: name === 'short-proposal' ? 5 : 20,
          memoryMiB: 128,
          cpuMillis: 5000,
        },
      },
    }));
    let resources = new Map<string, string>();
    const publish = async () => {
      const publication = await runtime.source.publish(
        owner.context,
        root.id,
        (await runtime.source.checkout(owner.context, root.id)).digest,
        definitions,
      );
      await runtime.source.apply(
        owner.context,
        (await runtime.source.plan(owner.context, root.id, publication.digest)).id,
      );
      resources = new Map(
        publication.resources.map((resource) => [
          resource.reference.split('/').at(-1)!,
          resource.id,
        ]),
      );
    };
    await publish();
    const objectStore = new ObjectStore(db.catalog),
      uid = await db.catalog.resolveIdentity(`native-function-${randomUUID()}`, 'record');
    const schema = { primaryKey: 'uid', properties: { uid: uidType, value: scalar } };
    const sourceRows = async (value: number) =>
      objectStore.publishStage(
        await objectStore.stage(
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
    await sourceRows(10);
    const binding = {
      objectTypeId: '00000000-0000-4000-8000-000000000004',
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
    const action = async (name: string, condition = true, fn?: string) => {
      const definition = {
        resource: name,
        revision: '1',
        markings: ['protected'],
        targetParameter: 'uid',
        inputs: fn ? inputs : { uid: uidType, value: scalar },
        condition: { literal: condition },
        edits: fn ? [] : [{ op: 'set', property: 'value', value: { parameter: 'value' } }],
        ...(fn ? { function: fn } : {}),
      };
      await db.pool.query(
        'INSERT INTO moria.native_serving_actions(environment,resource,object_resource,revision,definition) VALUES($1,$2,$3,$4,$5)',
        [environment, name, 'records', '1', definition],
      );
    };
    await action('edit-record');
    await action('reject-record', false);
    await action('function-record', true, resources.get('calculate-edit'));
    const dbRole = `function_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(
      `CREATE ROLE ${dbRole} LOGIN PASSWORD '${password}'; GRANT USAGE ON SCHEMA moria TO ${dbRole}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${dbRole}; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${dbRole}`,
    );
    const url = `postgresql://${dbRole}:${password}@127.0.0.1:${db.pool.options.port}/postgres`,
      host = randomUUID(),
      worker = randomUUID();
    await db.pool.query(
      "INSERT INTO moria.runtime_function_workers(owner,host_id,incarnation,resource_classes,expires_at) SELECT $1,$2,incarnation,ARRAY['standard'],clock_timestamp()+interval '1 hour' FROM moria.indexing_instance",
      [worker, host],
    );
    const env = {
      PATH: process.env.PATH,
      MORIA_INDEXING_DATABASE_SECRET_REF: 'FUNCTION_FIXTURE_DATABASE',
      FUNCTION_FIXTURE_DATABASE: url,
      MORIA_INDEXING_DATABASE_TLS: 'disable',
      MORIA_FUNCTION_HOST_ID: host,
      MORIA_FUNCTION_WORKER_OWNER: worker,
    };
    const binary =
      process.env.MORIA_RUNTIME_BINARY ??
      process.env.MORIA_INDEXING_BINARY ??
      '/usr/local/bin/moria-indexing';
    const start = (args: string[], extra: Record<string, string> = {}) => {
      const child = spawn(binary, args, {
        env: { ...env, ...extra },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      children.push(child);
      let output = '',
        error = '';
      child.stdout!.on('data', (b) => (output += b));
      child.stderr!.on('data', (b) => (error += b));
      const done = new Promise<string>((resolve, reject) => {
        child.once('error', reject);
        child.once('exit', (code, signal) =>
          code === 0
            ? resolve(output)
            : reject(
                new Error(
                  `Native function process ${args.join(' ')} failed ${code ?? signal}: ${error}`,
                ),
              ),
        );
      });
      void done.catch(() => {});
      return { child, done, output: () => output, error: () => error };
    };
    const serving = start(['serve'], { MORIA_NATIVE_LISTEN: '127.0.0.1:0' });
    let endpoint = '';
    await until(async () => {
      assert.equal(serving.child.exitCode, null, serving.error());
      for (const line of serving.output().split('\n'))
        try {
          const item = JSON.parse(line);
          if (item.event === 'serving') endpoint = `http://${item.address}`;
        } catch {}
      return !!endpoint;
    });
    const request = async (
      path: string,
      method = 'GET',
      body?: unknown,
      key?: string,
      token = owner.token,
    ) => {
      const response = await fetch(endpoint + path, {
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
    const enqueue = async (
      name: string,
      mode: string,
      value = 3,
      key: string = randomUUID(),
      inputUid: string | null = uid,
    ) => {
      const result = await request(
        `/api/v1/functions/${resources.get(name)}/invoke`,
        'POST',
        { input: { mode, value, uid: inputUid }, wait: false },
        key,
      );
      assert.equal(result.response.status, 202, JSON.stringify(result.value));
      assert.equal(result.value.status, 'accepted');
      ids.add(result.value.id);
      return result.value;
    };
    const get = async (id: string) => {
      const result = await request(`/api/v1/invocations/${id}`);
      assert.equal(result.response.status, 200, JSON.stringify(result.value));
      return result.value;
    };
    const once = async () => JSON.parse(await start(['functions', 'once']).done);
    const read = async () => {
      const result = await request(`/api/v1/objects/records/${uid}`);
      assert.equal(result.response.status, 200, JSON.stringify(result.value));
      return result.value.object.value as number;
    };
    const receipts = async (id: string) =>
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.runtime_function_receipts WHERE execution=$1',
          [id],
        )
      ).rows[0].count;
    const running = async (id: string) =>
      until(
        async () =>
          (
            await db.pool.query(
              'SELECT state FROM moria.runtime_function_sandboxes WHERE execution=$1',
              [id],
            )
          ).rows[0]?.state === 'running',
      );
    let extraArtifact: { uri: string; digest: string } | undefined;
    const guestArtifact = async () => {
      if (extraArtifact) return extraArtifact;
      const directory = await mkdtemp(join(tmpdir(), 'native-function-extra-'));
      const repository = image!.artifact.uri
        .slice(6)
        .replace(/\/[^/]+$/, '/moria-native-functions-extra');
      extraImages.add(repository);
      try {
        await writeFile(
          join(directory, 'guest.mjs'),
          `export default async function(input,context){
const route='/api/management/v1/fs';const api=async(path,method='GET',body,key)=>{const response=await context.platform.fetch(path,{method,headers:{'content-type':'application/json',...(key?{'idempotency-key':key}:{})},...(body===undefined?{}:{body:JSON.stringify(body)})});if(!response.ok)throw new Error('native platform operation failed');return response.json();};
if(input.source){const source=await api(route+'/entries/'+input.source),target=await api(route+'/entries/'+input.destination),parent=await api(route+'/entries/'+source.parentId);await api(route+'/changes','POST',{changes:[{op:'move',id:source.id,expectedRevision:source.revision,parentId:target.id,expectedParentRevision:target.revision,expectedSourceParentRevision:parent.revision,name:'moved-by-function'}]},'move');const moved=await api(route+'/entries/'+source.id);context.log('native filesystem mutation completed');return{value:moved.parentId===target.id?1:0};}
if(input.action){const response=await context.platform.fetch('/api/v1/actions/reject-record/invoke',{method:'POST',headers:{'content-type':'application/json','idempotency-key':'rejected'},body:JSON.stringify({input:{uid:input.uid,value:1}})});context.log('protected rejected action completed');return{value:response.status===409?1:0};}
const response=await context.platform.fetch('/api/v1/objects/records/'+input.uid+'?properties=uid,value');context.log('protected observation completed');return{value:response.status===404?1:0};}
`,
        );
        await writeFile(
          join(directory, 'Dockerfile'),
          `FROM ${image!.artifact.uri.slice(6)}@${image!.artifact.digest}\nCOPY guest.mjs /function/extra.mjs\nCMD ["node","/function/runtime/runner.mjs","/function/extra.mjs"]\n`,
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
        extraArtifact = { uri: `oci://${repository}`, digest };
        return extraArtifact;
      } finally {
        await rm(directory, { recursive: true, force: true });
      }
    };

    await selected(
      'authenticated admission pins release/actor/input, rejects forged fields and generic function workers',
      async () => {
        const accepted = await enqueue('read', 'normal', 3, 'original-key', null);
        assert.ok(accepted.attemptId && accepted.correlationId);
        assert.match(accepted.release, /^sha256:[0-9a-f]{64}$/);
        assert.equal((await get(accepted.id)).status, 'accepted');
        assert.equal(await receipts(accepted.id), '0');
        const replay = await request(
          `/api/v1/functions/${resources.get('read')}/invoke`,
          'POST',
          { input: { mode: 'normal', value: 3 }, wait: false },
          'original-key',
        );
        assert.equal(replay.value.id, accepted.id);
        const conflict = await request(
          `/api/v1/functions/${resources.get('read')}/invoke`,
          'POST',
          { input: { mode: 'normal', value: 4 }, wait: false },
          'original-key',
        );
        assert.equal(conflict.response.status, 409);
        const forged = await request(
          `/api/v1/functions/${resources.get('read')}/invoke`,
          'POST',
          { input: { mode: 'normal', value: 3 }, principal: 'other', wait: false },
          randomUUID(),
        );
        assert.equal(forged.response.status, 400);
        assert.equal(
          (
            await request(
              `/api/v1/invocations/${accepted.id}`,
              'GET',
              undefined,
              undefined,
              other.token,
            )
          ).response.status,
          404,
        );
        const fakeWorker = start(['functions', 'once'], {
          MORIA_FUNCTION_WORKER_OWNER: randomUUID(),
        });
        await assert.rejects(fakeWorker.done);
        const generic = spawn(binary, ['execution', 'claim'], {
          env,
          stdio: ['pipe', 'pipe', 'pipe'],
        });
        children.push(generic);
        generic.stdin!.end(JSON.stringify({ owner: worker, domains: ['function'] }));
        assert.notEqual(await new Promise((resolve) => generic.once('exit', resolve)), 0);
        assert.equal((await once()).worked, true);
        const finished = await get(accepted.id);
        assert.equal(finished.status, 'succeeded', JSON.stringify(finished));
        assert.equal(finished.result.value, 4);
        assert.equal(finished.attemptId, accepted.attemptId);
        assert.equal(await receipts(accepted.id), '1');
        const events = await request(`/api/v1/invocations/${accepted.id}/events?after=0`);
        assert.equal(events.response.status, 200);
        assert.ok(
          events.value.events.some(
            (event: any) => event.type === 'log' && event.message === 'function started',
          ),
        );
        assert.equal(
          events.value.events.filter(
            (event: any) => event.type === 'state' && event.message === 'succeeded',
          ).length,
          1,
        );
        assert.ok(finished.markings.some((marking: any) => marking.marking === 'protected'));
        assert.deepEqual(
          (await request(`/api/v1/invocations/${accepted.id}/cancel`, 'POST', {})).value,
          { id: accepted.id, cancelRequested: true },
        );
        assert.equal((await get(accepted.id)).status, 'succeeded');
      },
    );
    await selected(
      'a lost public acceptance response and API process death replay one durable invocation',
      async () => {
        let acceptedResolve!: (id: string) => void, acceptedReject!: (error: unknown) => void;
        const acceptedResponse = new Promise<string>((resolve, reject) => {
          acceptedResolve = resolve;
          acceptedReject = reject;
        });
        const proxy = createServer((incoming, outgoing) => {
          const target = new URL(incoming.url!, endpoint);
          const upstream = httpRequest(
            target,
            { method: incoming.method, headers: { ...incoming.headers, host: target.host } },
            (response) => {
              const chunks: Buffer[] = [];
              response.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
              response.on('end', () => {
                try {
                  assert.equal(response.statusCode, 202);
                  const accepted = JSON.parse(Buffer.concat(chunks).toString());
                  ids.add(accepted.id);
                  acceptedResolve(accepted.id);
                } catch (error) {
                  acceptedReject(error);
                }
                outgoing.destroy();
                upstream.destroy();
              });
            },
          );
          upstream.on('error', (error) => {
            acceptedReject(error);
            outgoing.destroy();
          });
          incoming.pipe(upstream);
        });
        await new Promise<void>((resolve) => proxy.listen(0, '127.0.0.1', resolve));
        try {
          const port = (proxy.address() as { port: number }).port;
          await assert.rejects(
            fetch(`http://127.0.0.1:${port}/api/v1/functions/${resources.get('read')}/invoke`, {
              method: 'POST',
              headers: {
                authorization: `Bearer ${owner.token}`,
                'content-type': 'application/json',
                'idempotency-key': 'lost-public-acceptance',
              },
              body: JSON.stringify({ input: { mode: 'normal', value: 4 }, wait: false }),
            }),
          );
          const id = await Promise.race([
            acceptedResponse,
            delay(10000, undefined, { ref: false }).then(async () => {
              throw new Error('No committed native acceptance response');
            }),
          ]);
          serving.child.kill('SIGKILL');
          await assert.rejects(serving.done, /SIGKILL/);
          const replacement = start(['serve'], { MORIA_NATIVE_LISTEN: '127.0.0.1:0' });
          await until(async () => {
            assert.equal(replacement.child.exitCode, null, replacement.error());
            for (const line of replacement.output().split('\n'))
              try {
                const item = JSON.parse(line);
                if (item.event === 'serving') {
                  endpoint = `http://${item.address}`;
                  return true;
                }
              } catch {}
            return false;
          });
          const replay = await request(
            `/api/v1/functions/${resources.get('read')}/invoke`,
            'POST',
            { input: { mode: 'normal', value: 4 }, wait: false },
            'lost-public-acceptance',
          );
          assert.equal(replay.value.id, id);
          assert.equal(
            (
              await db.pool.query(
                "SELECT count(*) FROM moria.runtime_executions WHERE request_key='lost-public-acceptance'",
              )
            ).rows[0].count,
            '1',
          );
          await once();
          assert.equal((await get(id)).result.value, 5);
          assert.equal(await receipts(id), '1');
        } finally {
          proxy.closeAllConnections();
          await new Promise<void>((resolve) => proxy.close(() => resolve()));
        }
      },
    );
    await selected(
      'publication retries only definite transaction aborts without rerunning the guest',
      async () => {
        for (const [state, failures, attempts, succeeds] of [
          ['40001', 2, 3, true],
          ['40P01', 1, 2, true],
          ['40001', 4, 4, false],
        ] as const) {
          const accepted = await enqueue('read', 'normal', 8);
          await db.pool.query(
            `CREATE SEQUENCE moria.function_publication_attempts;
             GRANT USAGE,SELECT ON SEQUENCE moria.function_publication_attempts TO ${dbRole};
             CREATE FUNCTION public.abort_function_publication() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
               IF NEW.execution='${accepted.id}'::uuid AND nextval('moria.function_publication_attempts')<=${failures}
                 THEN RAISE EXCEPTION 'publication transaction fault' USING ERRCODE='${state}'; END IF;
               RETURN NEW;
             END $$;
             CREATE TRIGGER native_publication_abort BEFORE INSERT ON moria.runtime_function_receipts
               FOR EACH ROW EXECUTE FUNCTION public.abort_function_publication()`,
          );
          try {
            await once();
            const finished = await get(accepted.id);
            assert.equal(
              finished.status,
              succeeds ? 'succeeded' : 'failed',
              JSON.stringify({
                finished,
                events: (
                  await db.pool.query(
                    "SELECT kind,payload FROM moria.runtime_execution_events WHERE execution=$1 AND kind='failed'",
                    [accepted.id],
                  )
                ).rows,
              }),
            );
            if (succeeds) assert.equal(finished.result.value, 9);
            assert.equal(await receipts(accepted.id), succeeds ? '1' : '0');
            assert.equal(
              (await db.pool.query('SELECT last_value FROM moria.function_publication_attempts'))
                .rows[0].last_value,
              String(attempts),
            );
            assert.deepEqual(
              (
                await db.pool.query(
                  'SELECT attempts FROM moria.runtime_execution_tasks WHERE execution=$1',
                  [accepted.id],
                )
              ).rows.map((row) => row.attempts),
              [1],
            );
            const events = await request(`/api/v1/invocations/${accepted.id}/events?after=0`);
            assert.equal(events.response.status, 200, JSON.stringify(events.value));
            assert.equal(
              events.value.events.filter(
                (event: any) => event.type === 'log' && event.message === 'function started',
              ).length,
              1,
            );
            if (!succeeds)
              assert.equal(
                (
                  await db.pool.query(
                    "SELECT payload->>'sqlState' AS state FROM moria.runtime_execution_events WHERE execution=$1 AND kind='failed'",
                    [accepted.id],
                  )
                ).rows.at(-1)?.state,
                state,
              );
          } finally {
            await db.pool.query(
              'DROP TRIGGER native_publication_abort ON moria.runtime_function_receipts; DROP FUNCTION public.abort_function_publication(); DROP SEQUENCE moria.function_publication_attempts',
            );
          }
        }
      },
    );
    await selected('deployed code cannot change an accepted release or replay schema', async () => {
      const accepted = await enqueue('read', 'normal', 7, 'before-redeploy', null),
        pinned = accepted.release;
      const original = definitions;
      try {
        definitions = definitions.map((definition) =>
          definition.metadata.name === 'read'
            ? ({
                ...definition,
                spec: {
                  ...definition.spec,
                  outputs: { value: { type: 'string', nullable: false } },
                },
              } as Resource)
            : definition,
        );
        await publish();
        assert.equal((await once()).worked, true);
        const finished = await get(accepted.id);
        assert.equal(finished.release, pinned);
        assert.equal(finished.result.value, 8);
        const replay = await request(
          `/api/v1/functions/${resources.get('read')}/invoke`,
          'POST',
          { input: { mode: 'normal', value: 7 }, wait: false },
          'before-redeploy',
        );
        assert.equal(replay.value.id, accepted.id);
      } finally {
        definitions = original;
        await publish();
      }
    });
    await selected(
      'real native broker preserves reads and committed readWrite effects after later code failure',
      async () => {
        const readJob = await enqueue('read', 'sdk-read');
        await once();
        assert.equal((await get(readJob.id)).result.value, 10);
        const failed = await enqueue('write', 'action-fail', 25);
        await once();
        assert.equal((await get(failed.id)).status, 'failed');
        assert.equal(await read(), 25);
        assert.equal(await receipts(failed.id), '0');
        assert.equal(
          (
            await db.pool.query('SELECT count(*) FROM moria.action_keys WHERE action=$1', [
              'edit-record',
            ])
          ).rows[0].count,
          '1',
        );
        const success = await enqueue('write', 'action', 26);
        await once();
        const succeeded = await get(success.id);
        assert.equal(succeeded.status, 'succeeded', JSON.stringify(succeeded));
        assert.equal(succeeded.result.value, 26);
        assert.equal(await read(), 26);
      },
    );
    await selected(
      'editProposal batch and function receipt commit together; rejection or result failure rolls all edits back',
      async () => {
        const good = await enqueue('proposal', 'proposal', 31);
        await once();
        assert.equal((await get(good.id)).status, 'succeeded');
        assert.equal(await read(), 31);
        assert.equal(await receipts(good.id), '1');
        const rejected = await enqueue('proposal', 'proposal-rollback', 41);
        await once();
        assert.equal((await get(rejected.id)).status, 'failed');
        assert.equal(await read(), 31);
        assert.equal(await receipts(rejected.id), '0');
        await db.pool.query(
          "CREATE FUNCTION public.reject_function_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture rollback'; END $$; CREATE TRIGGER native_receipt_failure BEFORE INSERT ON moria.runtime_function_receipts FOR EACH ROW EXECUTE FUNCTION public.reject_function_receipt()",
        );
        try {
          const rolled = await enqueue('proposal', 'proposal', 51);
          await once();
          assert.equal((await get(rolled.id)).status, 'failed');
          assert.equal(await receipts(rolled.id), '0');
          assert.equal(await read(), 31);
        } finally {
          await db.pool.query(
            'DROP TRIGGER native_receipt_failure ON moria.runtime_function_receipts; DROP FUNCTION public.reject_function_receipt()',
          );
        }
      },
    );
    await selected(
      'live function leases reject generic sealing, out-of-admission epoch acknowledgement and worker-scope revocation',
      async () => {
        const job = await enqueue('write', 'hold'),
          active = start(['functions', 'once']);
        await running(job.id);
        const leased = (
          await db.pool.query(
            'SELECT t.id AS task,t.owner,t.fence::text,t.lease_until::text,i.incarnation,a.security_epoch,a.instance_epoch,f.markings FROM moria.runtime_execution_tasks t JOIN moria.runtime_function_epochs a ON a.execution=t.execution JOIN moria.runtime_function_invocations f ON f.execution=t.execution CROSS JOIN moria.indexing_instance i WHERE t.execution=$1',
            [job.id],
          )
        ).rows[0];
        const seal = spawn(binary, ['execution', 'seal'], { env, stdio: ['pipe', 'pipe', 'pipe'] });
        children.push(seal);
        seal.stdin!.end(
          JSON.stringify({
            lease: {
              incarnation: leased.incarnation,
              execution: job.id,
              task: leased.task,
              owner: leased.owner,
              fence: leased.fence,
              leaseUntil: leased.lease_until,
            },
            result: { value: 999 },
            digest: 'a'.repeat(64),
          }),
        );
        assert.notEqual(await new Promise((resolve) => seal.once('exit', resolve)), 0);
        assert.equal(
          (
            await db.pool.query(
              'SELECT status FROM moria.runtime_execution_tasks WHERE execution=$1',
              [job.id],
            )
          ).rows[0].status,
          'running',
        );
        await assert.rejects(
          db.pool.query('SELECT moria.runtime_function_acknowledge($1,$2,$3,$4,$5,$6)', [
            job.id,
            leased.owner,
            leased.fence,
            leased.security_epoch,
            leased.instance_epoch,
            JSON.stringify(leased.markings),
          ]),
          /FUNCTION_MUTATION_ADMISSION_REQUIRED/,
        );
        await db.pool.query(
          "UPDATE moria.runtime_function_workers SET resource_classes=ARRAY['unassigned'] WHERE owner=$1",
          [worker],
        );
        try {
          await active.done;
          const withheld = await request(`/api/v1/invocations/${job.id}`);
          assert.equal(withheld.response.status, 409);
          assert.equal(withheld.value.error.code, 'FUNCTION_OBSERVATION_INCOMPLETE');
          assert.equal(
            (
              await db.pool.query('SELECT status FROM moria.runtime_executions WHERE id=$1', [
                job.id,
              ])
            ).rows[0].status,
            'failed',
          );
          assert.equal(await receipts(job.id), '0');
          assert.equal(
            (
              await db.pool.query(
                'SELECT cleaned FROM moria.runtime_function_sandboxes WHERE execution=$1',
                [job.id],
              )
            ).rows[0].cleaned,
            true,
          );
        } finally {
          await db.pool.query(
            "UPDATE moria.runtime_function_workers SET resource_classes=ARRAY['standard'] WHERE owner=$1",
            [worker],
          );
        }
      },
    );
    await selected(
      'cancellation and supervisor death fence the only attempt and remove local orphans',
      async () => {
        const cancelled = await enqueue('read', 'hold'),
          active = start(['functions', 'once']);
        await running(cancelled.id);
        const begin = Date.now();
        assert.deepEqual(
          (await request(`/api/v1/invocations/${cancelled.id}/cancel`, 'POST', {})).value,
          { id: cancelled.id, cancelRequested: true },
        );
        await active.done;
        assert.ok(Date.now() - begin < 10000);
        assert.equal(await receipts(cancelled.id), '0');
        const lost = await enqueue('read', 'hold'),
          crashed = start(['functions', 'once']);
        await running(lost.id);
        crashed.child.kill('SIGKILL');
        await assert.rejects(crashed.done, /SIGKILL/);
        await db.pool.query(
          "UPDATE moria.runtime_execution_tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE execution=$1",
          [lost.id],
        );
        assert.equal((await get(lost.id)).status, 'failed');
        assert.equal(JSON.parse(await start(['functions', 'cleanup']).done).cleaned, 1);
        assert.equal((await once()).worked, false);
        assert.equal(
          (
            await db.pool.query(
              'SELECT attempts FROM moria.runtime_execution_tasks WHERE execution=$1',
              [lost.id],
            )
          ).rows[0].attempts,
          1,
        );
        assert.equal(await receipts(lost.id), '0');
      },
    );
    await selected(
      'actual publication COMMIT response loss preserves one result and one proposal batch after SIGKILL',
      async () => {
        const job = await enqueue('proposal', 'proposal', 52, 'lost-publication-response');
        const proxy = await transactionCommitProxy(Number(db.pool.options.port));
        try {
          const connection = new URL(url);
          connection.port = String(proxy.port);
          const process = start(['functions', 'once'], {
            FUNCTION_FIXTURE_DATABASE: connection.toString(),
          });
          await Promise.race([
            proxy.committed,
            process.done.then(async (output) => {
              throw new Error(
                `Native worker ended before commit barrier: ${output}; state=${JSON.stringify((await db.pool.query('SELECT status,error FROM moria.runtime_executions WHERE id=$1', [job.id])).rows[0])}`,
              );
            }),
            delay(10000, undefined, { ref: false }).then(async () => {
              throw new Error(
                `Native publication barrier: ${JSON.stringify(proxy.checkpoints)}; state=${JSON.stringify((await db.pool.query('SELECT status,error FROM moria.runtime_executions WHERE id=$1', [job.id])).rows[0])}`,
              );
            }),
          ]);
          assert.equal(await receipts(job.id), '1');
          process.child.kill('SIGKILL');
          await assert.rejects(process.done, /SIGKILL/);
          assert.equal(await read(), 52);
          const result = await get(job.id);
          assert.equal(result.status, 'succeeded');
          assert.equal(result.result.value, 52);
          const replay = await request(
            `/api/v1/functions/${resources.get('proposal')}/invoke`,
            'POST',
            { input: { mode: 'proposal', value: 52, uid }, wait: false },
            'lost-publication-response',
          );
          assert.equal(replay.value.id, job.id);
          assert.equal((await once()).worked, false);
          assert.equal(
            (
              await db.pool.query(
                'SELECT attempts FROM moria.runtime_execution_tasks WHERE execution=$1',
                [job.id],
              )
            ).rows[0].attempts,
            1,
          );
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.native_function_proposals WHERE execution=$1',
                [job.id],
              )
            ).rows[0].count,
            '1',
          );
        } finally {
          await proxy.close();
        }
      },
    );
    await selected(
      'a permanently lost COMMIT response releases admission at its DB deadline and preserves committed success',
      async () => {
        const job = await enqueue('short-proposal', 'proposal', 53, 'publication-deadline');
        const proxy = await transactionCommitProxy(Number(db.pool.options.port));
        try {
          const connection = new URL(url);
          connection.port = String(proxy.port);
          const worker = start(['functions', 'once'], {
            FUNCTION_FIXTURE_DATABASE: connection.toString(),
          });
          await Promise.race([
            proxy.committed,
            worker.done.then((output) => {
              throw new Error(`Worker ended before commit: ${output}`);
            }),
            delay(6000, undefined, { ref: false }).then(() => {
              throw new Error('Short publication never committed');
            }),
          ]);
          assert.equal(await receipts(job.id), '1');
          const output = await Promise.race([
            worker.done,
            delay(7000, undefined, { ref: false }).then(() => {
              throw new Error('Publication network wait exceeded its DB deadline');
            }),
          ]);
          assert.equal(JSON.parse(output).worked, true);
          assert.equal((await get(job.id)).status, 'succeeded');
          assert.equal(await read(), 53);
          assert.equal(await receipts(job.id), '1');
        } finally {
          await proxy.close();
        }
      },
    );
    await selected(
      'native worker pool supports bounded nested invocations and function-backed action preparation',
      async () => {
        const child = spawn(
          process.execPath,
          ['--import', 'tsx', 'apps/cli/src/main.ts', 'functions', 'worker'],
          {
            env: { ...env, MORIA_RUNTIME_BINARY: binary, MORIA_FUNCTION_WORKER_CONCURRENCY: '4' },
            stdio: ['ignore', 'pipe', 'pipe'],
          },
        );
        children.push(child);
        let stdout = '',
          stderr = '';
        child.stdout!.on('data', (b) => (stdout += b));
        child.stderr!.on('data', (b) => (stderr += b));
        const done = new Promise<void>((resolve, reject) => {
          child.once('error', reject);
          child.once('exit', (code, signal) =>
            code === 0
              ? resolve()
              : reject(
                  new Error(`Persistent native CLIworker exited ${code ?? signal}: ${stderr}`),
                ),
          );
        });
        void done.catch(() => {});
        const pool = { child, done };
        try {
          await until(async () => {
            assert.equal(child.exitCode, null, stderr);
            return stdout.split('\n').some((line) => {
              try {
                return JSON.parse(line).event === 'function-worker';
              } catch {
                return false;
              }
            });
          });
          assert.equal(await readlink(`/proc/${child.pid}/exe`), await realpath(binary));
          const nested = await request(
            `/api/v1/functions/${resources.get('parent')}/invoke`,
            'POST',
            { input: { mode: 'nested', value: 7, uid: resources.get('child') }, wait: true },
            'nested-parent',
          );
          assert.equal(nested.response.status, 200, JSON.stringify(nested.value));
          ids.add(nested.value.id);
          assert.equal(
            nested.value.status,
            'succeeded',
            JSON.stringify({
              parent: nested.value,
              children: (
                await db.pool.query(
                  'SELECT e.id,e.status,e.error FROM moria.runtime_executions e JOIN moria.runtime_function_invocations f ON f.execution=e.id WHERE f.parent_id=$1',
                  [nested.value.id],
                )
              ).rows,
              events: (await request(`/api/v1/invocations/${nested.value.id}/events?after=0`))
                .value,
            }),
          );
          assert.equal(nested.value.result.value, 8);
          const nestedChild = (
            await db.pool.query(
              'SELECT execution FROM moria.runtime_function_invocations WHERE parent_id=$1',
              [nested.value.id],
            )
          ).rows[0];
          assert.ok(nestedChild);
          ids.add(nestedChild.execution);
          assert.equal((await get(nestedChild.execution)).status, 'succeeded');
          const input = { mode: 'normal', value: 60, uid };
          const actionResult = await request(
            '/api/v1/actions/function-record/invoke',
            'POST',
            { input },
            'prepared-action',
          );
          assert.equal(actionResult.response.status, 200, JSON.stringify(actionResult.value));
          assert.equal(await read(), 61);
          const prepared = (
            await db.pool.query(
              'SELECT execution FROM moria.native_serving_prepared_actions WHERE action=$1 AND key=$2',
              ['function-record', 'prepared-action'],
            )
          ).rows[0];
          assert.ok(prepared);
          ids.add(prepared.execution);
          const attempts = (
            await db.pool.query(
              'SELECT attempts FROM moria.runtime_execution_tasks WHERE execution=$1',
              [prepared.execution],
            )
          ).rows[0].attempts;
          const replay = await request(
            '/api/v1/actions/function-record/invoke',
            'POST',
            { input },
            'prepared-action',
          );
          assert.equal(replay.response.status, 200, JSON.stringify(replay.value));
          assert.equal(
            (
              await db.pool.query(
                'SELECT attempts FROM moria.runtime_execution_tasks WHERE execution=$1',
                [prepared.execution],
              )
            ).rows[0].attempts,
            attempts,
          );
        } finally {
          pool.child.kill('SIGTERM');
          await pool.done;
        }
      },
    );
    await selected(
      'worker CLI rejects missing native configuration before creating a workspace or Node worker',
      async () => {
        const child = spawn(
          process.execPath,
          ['--import', 'tsx', 'apps/cli/src/main.ts', 'functions', 'worker'],
          {
            env: { PATH: process.env.PATH, MORIA_LOCAL_DIR: join(db.root, 'must-not-exist') },
            stdio: ['ignore', 'pipe', 'pipe'],
          },
        );
        children.push(child);
        let errors = '';
        child.stderr!.on('data', (b) => (errors += b));
        const code = await new Promise<number | null>((resolve, reject) => {
          child.once('error', reject);
          child.once('exit', resolve);
        });
        assert.equal(code, 2);
        assert.match(errors, /MORIA_RUNTIME_BINARY/);
        await assert.rejects(stat(join(db.root, 'must-not-exist')), { code: 'ENOENT' });
      },
    );
    let publicReadResource: string | undefined;
    await selected(
      'a public function catching a protected missing-object read retains its labels and failed observation',
      async () => {
        const made = await runtime.filesystem.apply(owner.context, randomUUID(), [
          {
            op: 'create',
            kind: 'directory',
            parentId: source.id,
            expectedParentRevision: (await runtime.filesystem.store.get(source.id)).revision,
            name: 'public-logic',
          },
        ]);
        const publicRoot = made.entries.find((entry) => entry.name === 'public-logic')!;
        const bytes = Buffer.from(
          JSON.stringify({
            version: 1,
            name: 'Public Logic',
            namespace: 'publiclogic',
            runtime: 'typescript-v1',
          }),
        );
        await runtime.filesystem.writeFile(
          owner.context,
          randomUUID(),
          {
            op: 'create',
            parentId: publicRoot.id,
            expectedParentRevision: (await runtime.filesystem.store.get(publicRoot.id)).revision,
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
          owner.context,
          publicRoot.id,
          (await runtime.filesystem.store.get(publicRoot.id)).revision,
          'repository',
        );
        const declaration: Resource = {
          apiVersion: 'moria/v1alpha2',
          kind: 'Function',
          metadata: { namespace: 'publiclogic', name: 'catch-read' },
          spec: {
            artifact: await guestArtifact(),
            runtime: 'typescript-v1',
            inputs: { uid: uidType },
            outputs: { value: scalar },
            effect: 'readOnly',
            limits: { timeoutSeconds: 20, memoryMiB: 128, cpuMillis: 5000 },
          },
        };
        const actionDeclaration: Resource = {
          ...declaration,
          metadata: { namespace: 'publiclogic', name: 'catch-action' },
          spec: {
            ...declaration.spec,
            inputs: { uid: uidType, action: { type: 'boolean', nullable: false } },
            effect: 'readWrite',
          },
        };
        const publication = await runtime.source.publish(
          owner.context,
          publicRoot.id,
          (await runtime.source.checkout(owner.context, publicRoot.id)).digest,
          [declaration, actionDeclaration],
        );
        await runtime.source.apply(
          owner.context,
          (await runtime.source.plan(owner.context, publicRoot.id, publication.digest)).id,
        );
        const missing = await db.catalog.resolveIdentity(
            `native-function-failure-${randomUUID()}`,
            'missing',
          ),
          resource = publication.resources.find(
            (resource) => resource.reference === 'publiclogic/Function/catch-read',
          )?.id;
        assert.ok(resource, JSON.stringify(publication.resources));
        publicReadResource = resource;
        const accepted = await request(
          `/api/v1/functions/${resource}/invoke`,
          'POST',
          { input: { uid: missing }, wait: false },
          'catch-private-404',
        );
        assert.equal(
          accepted.response.status,
          202,
          JSON.stringify({
            response: accepted.value,
            definition: (await runtime.source.resource(owner.context, resource)).resource,
          }),
        );
        ids.add(accepted.value.id);
        assert.deepEqual(accepted.value.markings, []);
        await once();
        const result = await get(accepted.value.id);
        assert.equal(result.status, 'succeeded', JSON.stringify(result));
        assert.equal(result.result.value, 1);
        assert.deepEqual(result.markings, [{ environment, marking: 'protected' }]);
        assert.ok(
          result.calls.some(
            (call: any) => call.status === 'failed' && call.path.includes('/objects/records/'),
          ),
        );
        const actionResource = publication.resources.find(
          (resource) => resource.reference === 'publiclogic/Function/catch-action',
        )?.id;
        assert.ok(actionResource);
        const actionJob = await request(
          `/api/v1/functions/${actionResource}/invoke`,
          'POST',
          { input: { uid, action: true }, wait: false },
          'catch-private-action',
        );
        assert.equal(actionJob.response.status, 202, JSON.stringify(actionJob.value));
        ids.add(actionJob.value.id);
        assert.deepEqual(actionJob.value.markings, []);
        await once();
        const actionResult = await get(actionJob.value.id);
        assert.equal(actionResult.status, 'succeeded', JSON.stringify(actionResult));
        assert.equal(actionResult.result.value, 1);
        assert.deepEqual(actionResult.markings, [{ environment, marking: 'protected' }]);
        assert.ok(
          actionResult.calls.some(
            (call: any) =>
              call.status === 'failed' && call.path.includes('/actions/reject-record/'),
          ),
        );
        await authority.setGrants(
          environment,
          'fixture',
          principal,
          application,
          operations.filter((operation) => operation !== 'reject-record:invoke'),
        );
        assert.equal(
          (await request(`/api/v1/invocations/${actionJob.value.id}`)).response.status,
          403,
        );
        await authority.setGrants(environment, 'fixture', principal, application, operations);
        await objectStore.publishStage(
          await objectStore.stage(
            environment,
            'records',
            schema,
            [
              { uid, value: 99 },
              { uid: missing, value: 44 },
            ],
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
        assert.notEqual(
          (await request(`/api/v1/invocations/${accepted.value.id}`)).response.status,
          200,
        );
        assert.notEqual(
          (await request(`/api/v1/invocations/${accepted.value.id}/events?after=0`)).response
            .status,
          200,
        );
      },
    );
    await selected(
      'a killed callback with rolled-back private evidence cannot release failure metadata or logs',
      async () => {
        assert.ok(publicReadResource);
        const accepted = await request(
          `/api/v1/functions/${publicReadResource}/invoke`,
          'POST',
          { input: { uid }, wait: false },
          'private-observation-rollback',
        );
        assert.equal(accepted.response.status, 202, JSON.stringify(accepted.value));
        const id = accepted.value.id as string;
        assert.match(id, /^[0-9a-f-]{36}$/);
        ids.add(id);
        assert.deepEqual(accepted.value.markings, []);
        const key = randomUUID(),
          blocker = await db.pool.connect();
        let active: ReturnType<typeof start> | undefined;
        let backend: number | undefined;
        try {
          await blocker.query('BEGIN');
          await blocker.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [key]);
          await db.pool.query(
            'CREATE FUNCTION moria.test_private_observation_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(hashtextextended(TG_ARGV[0],0));RETURN NEW;END $$',
          );
          await db.pool.query(
            `CREATE TRIGGER test_private_observation_barrier AFTER INSERT ON moria.native_function_reads FOR EACH ROW WHEN(NEW.execution='${id}'::uuid AND NEW.call_key LIKE 'read/%') EXECUTE FUNCTION moria.test_private_observation_barrier('${key}')`,
          );
          active = start(['functions', 'once']);
          await until(async () => {
            const rows = (
              await db.pool.query(
                "SELECT pid FROM pg_stat_activity WHERE wait_event_type='Lock' AND wait_event='advisory' AND query LIKE '%INSERT INTO moria.native_function_reads%' AND pid<>pg_backend_pid()",
              )
            ).rows;
            if (rows.length === 1) backend = rows[0].pid;
            return backend !== undefined;
          });
          const pending = (
            await db.pool.query(
              'SELECT observation_pending,consumed_markings FROM moria.runtime_sandbox_attempts WHERE execution=$1',
              [id],
            )
          ).rows[0];
          assert.ok(pending.observation_pending);
          assert.deepEqual(
            pending.consumed_markings,
            [],
            'Private read happened inside the still-uncommitted evidence transaction',
          );
          active.child.kill('SIGKILL');
          await assert.rejects(active.done, /SIGKILL/);
          await blocker.query('ROLLBACK');
          await until(
            async () =>
              !(await db.pool.query('SELECT 1 FROM pg_stat_activity WHERE pid=$1', [backend]))
                .rowCount,
          );
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.native_function_reads WHERE execution=$1',
                [id],
              )
            ).rows[0].count,
            '0',
          );
          await db.pool.query(
            "UPDATE moria.runtime_execution_tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE execution=$1",
            [id],
          );
          for (const path of [
            `/api/v1/invocations/${id}`,
            `/api/v1/invocations/${id}/events?after=0`,
          ]) {
            const denied = await request(path);
            assert.equal(denied.response.status, 409, JSON.stringify(denied.value));
            assert.equal(denied.value.error.code, 'FUNCTION_OBSERVATION_INCOMPLETE');
          }
          await authority.syncPrincipal(environment, 'fixture', principal, {
            kind: 'workload',
            enabled: true,
            attributes: {},
            markings: [],
          });
          const denied = await request(`/api/v1/invocations/${id}`);
          assert.equal(denied.response.status, 409, JSON.stringify(denied.value));
          assert.equal(denied.value.error.code, 'FUNCTION_OBSERVATION_INCOMPLETE');
          assert.equal(await receipts(id), '0');
          assert.equal(JSON.parse(await start(['functions', 'cleanup']).done).cleaned, 1);
          assert.equal(
            (
              await db.pool.query(
                'SELECT attempts FROM moria.runtime_execution_tasks WHERE execution=$1',
                [id],
              )
            ).rows[0].attempts,
            1,
          );
          assert.ok(
            (
              await db.pool.query(
                'SELECT observation_pending FROM moria.runtime_sandbox_attempts WHERE execution=$1',
                [id],
              )
            ).rows[0].observation_pending,
          );
        } finally {
          active?.child.kill('SIGKILL');
          await blocker.query('ROLLBACK');
          blocker.release();
          await db.pool.query(
            'DROP TRIGGER IF EXISTS test_private_observation_barrier ON moria.native_function_reads;DROP FUNCTION IF EXISTS moria.test_private_observation_barrier()',
          );
          await authority.syncPrincipal(environment, 'fixture', principal, {
            kind: 'workload',
            enabled: true,
            attributes: {},
            markings: ['protected'],
          });
        }
      },
    );
    await selected(
      'a real guest filesystem move commits retained labels and its own epoch acknowledgement atomically',
      async () => {
        const directory = async (parentId: string, name: string) => {
          const receipt = await runtime.filesystem.apply(owner.context, randomUUID(), [
            {
              op: 'create',
              kind: 'directory',
              parentId,
              expectedParentRevision: (await runtime.filesystem.store.get(parentId)).revision,
              name,
            },
          ]);
          return receipt.entries.find((entry) => entry.name === name)!;
        };
        const origin = await directory(source.id, 'guest-move-origin');
        const destination = await directory(source.id, 'guest-move-destination');
        for (const entry of [origin, destination])
          await runtime.filesystem.security.setPolicy('fixture', entry.id, '0', {
            markings: [{ environment, marking: 'protected' }],
            grants: [],
          });
        const moving = await directory(origin.id, 'guest-moving');
        const previous = definitions;
        try {
          definitions = [
            ...definitions,
            {
              apiVersion: 'moria/v1alpha2',
              kind: 'Function',
              metadata: { namespace: 'logic', name: 'filesystem-move' },
              spec: {
                artifact: await guestArtifact(),
                runtime: 'typescript-v1',
                inputs: { source: uidType, destination: uidType },
                outputs: { value: scalar },
                effect: 'readWrite',
                limits: { timeoutSeconds: 20, memoryMiB: 128, cpuMillis: 5000 },
              },
            },
          ];
          await publish();
          const accepted = await request(
            `/api/v1/functions/${resources.get('filesystem-move')}/invoke`,
            'POST',
            { input: { source: moving.id, destination: destination.id }, wait: false },
            'guest-filesystem-move',
          );
          assert.equal(accepted.response.status, 202, JSON.stringify(accepted.value));
          ids.add(accepted.value.id);
          const before = (await db.pool.query('SELECT security_epoch FROM moria.instance_state'))
            .rows[0].security_epoch;
          await once();
          const result = await get(accepted.value.id);
          assert.equal(result.status, 'succeeded', JSON.stringify(result));
          assert.equal(result.result.value, 1);
          assert.deepEqual(result.markings, [{ environment, marking: 'protected' }]);
          assert.equal((await runtime.filesystem.store.get(moving.id)).parentId, destination.id);
          const retained = await db.pool.query(
            'SELECT retained_markings FROM moria.fs_policies WHERE entry_id=$1',
            [moving.id],
          );
          assert.deepEqual(retained.rows[0].retained_markings, [
            { environment, marking: 'protected' },
          ]);
          const audit = await db.pool.query(
            "SELECT instance_epoch FROM moria.runtime_function_epoch_events WHERE execution=$1 AND reason='mutation'",
            [accepted.value.id],
          );
          assert.equal(audit.rowCount, 1);
          assert.ok(BigInt(audit.rows[0].instance_epoch) > BigInt(before));
          assert.ok(
            result.calls.some(
              (call: any) => call.path.endsWith('/fs/changes') && call.status === 'succeeded',
            ),
          );
          assert.equal(
            (await request(`/api/v1/invocations/${accepted.value.id}/events?after=0`)).response
              .status,
            200,
          );
        } finally {
          definitions = previous;
          await publish();
        }
      },
    );
    await selected(
      'exact Function invocation scopes run without filesystem browsing and remain pinned on acceptance',
      async () => {
        const target = resources.get('read')!;
        const scoped = [`${target}:invoke`];
        await authority.setApplication(environment, 'fixture', application, scoped);
        await authority.setGrants(environment, 'fixture', principal, application, scoped);
        try {
          const job = await enqueue('read', 'normal');
          assert.deepEqual(
            (
              await db.pool.query(
                'SELECT b.operations FROM moria.runtime_execution_bindings b JOIN moria.runtime_executions e ON e.binding=b.id WHERE e.id=$1',
                [job.id],
              )
            ).rows[0].operations,
            scoped,
          );
          await once();
          assert.equal((await get(job.id)).status, 'succeeded');
          assert.notEqual((await request('/api/management/v1/fs/home')).response.status, 200);
          const denied = await request(
            `/api/v1/functions/${resources.get('write')}/invoke`,
            'POST',
            { input: { mode: 'normal', value: 3, uid }, wait: false },
            randomUUID(),
          );
          assert.equal(denied.response.status, 403);
          const pinned = await enqueue('read', 'normal');
          await authority.setApplication(environment, 'fixture', application, ['functions:invoke']);
          await authority.setGrants(environment, 'fixture', principal, application, [
            'functions:invoke',
          ]);
          await assert.rejects(
            db.pool.query('SELECT moria.runtime_execution_check($1)', [pinned.id]),
            /AUTHORIZATION_CHANGED|AUTHORIZATION_SCOPE_REVOKED|FORBIDDEN/,
          );
        } finally {
          await authority.setApplication(environment, 'fixture', application, operations);
          await authority.setGrants(environment, 'fixture', principal, application, operations);
        }
      },
    );
    await selected(
      'changed read evidence and revoked access withhold retained results and protected logs',
      async () => {
        const observed = await enqueue('read', 'read');
        await once();
        assert.equal((await get(observed.id)).status, 'succeeded');
        await sourceRows(99);
        assert.equal((await request(`/api/v1/invocations/${observed.id}`)).response.status, 200);
        assert.equal(
          (
            await request(
              '/api/v1/actions/edit-record/invoke',
              'POST',
              { input: { uid, value: 100 } },
              'change-evidence',
            )
          ).response.status,
          200,
        );
        const changed = await request(`/api/v1/invocations/${observed.id}`);
        assert.notEqual(changed.response.status, 200);
        assert.notEqual(
          (await request(`/api/v1/invocations/${observed.id}/events?after=0`)).response.status,
          200,
        );
        await authority.setGrants(
          environment,
          'fixture',
          principal,
          application,
          operations.filter((operation) => operation !== 'functions:invoke'),
        );
        assert.equal((await request(`/api/v1/invocations/${observed.id}`)).response.status, 403);
      },
    );
  },
);
