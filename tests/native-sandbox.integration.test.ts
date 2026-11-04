import assert from 'node:assert/strict';
import { spawn, execFile, type ChildProcess } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { promisify } from 'node:util';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { database } from './fixtures/catalog-storage/database.js';
import { functionImage } from './fixtures/functions/image.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { ObjectStore, type ObjectSchema } from '../services/ontology/src/store.js';
import { fingerprint } from '../packages/compiler/src/index.js';

const exec = promisify(execFile);
const docker = async (...args: string[]) => (await exec('docker', args, { timeout: 15000 })).stdout;
async function until(check: () => Promise<boolean>) {
  for (let attempt = 0; attempt < 400; attempt++) {
    if (await check()) return;
    await delay(25);
  }
  assert.fail('Real sandbox did not reach the required lifecycle state');
}

test(
  'native OCI supervision enforces real isolation and owner-bound single attempts',
  { timeout: 240000 },
  async (t) => {
    const db = await database();
    t.after(db.close);
    const image = await functionImage();
    const children: ChildProcess[] = [],
      executions: string[] = [];
    t.after(async () => {
      for (const child of children) child.kill('SIGKILL');
      for (const id of executions)
        await docker('rm', '--force', `moria-native-function-${id}`).catch(() => {});
      await image.close();
    });
    const environment: string = randomUUID(),
      resource = randomUUID(),
      host = randomUUID();
    const principal = 'pipeline',
      application = 'native-sandbox';
    await db.catalog.ensureEnvironment(environment);
    const authority = new Authority(db.catalog),
      sessions = new WorkloadSessions(authority);
    const operations = ['function:execute', 'records:read'];
    await authority.syncPrincipal(environment, 'fixture', principal, {
      enabled: true,
      kind: 'workload',
      attributes: {},
      markings: ['private'],
    });
    await authority.setApplication(environment, 'fixture', application, operations);
    await authority.setGrants(environment, 'fixture', principal, application, operations);
    const issued = await sessions.mint({ environment, principal, application }, 'fixture');
    const sessionHash = createHash('sha256').update(issued.token).digest('hex');
    const context = { environment, principal, application, sessionHash };
    const epoch = (await authority.current(environment, principal, application)).epoch;
    const store = new ObjectStore(db.catalog);
    const properties = {
      uid: { type: 'uid' as const, nullable: false },
      value: { type: 'float64' as const, nullable: false },
    };
    const schema: ObjectSchema = { primaryKey: 'uid', properties };
    const uid = await db.catalog.resolveIdentity(`sandbox-${environment}`, 'record');
    await store.publishStage(
      await store.stage(
        environment,
        'records',
        schema,
        [{ uid, value: 17 }],
        epoch,
        (await db.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [environment]))
          .rows[0].revision,
        undefined,
        ['private'],
      ),
    );
    const definition = {
      objectTypeId: '00000000-0000-4000-8000-000000000004',
      schema,
      policies: {
        resource: 'records',
        markings: ['private'],
        properties,
        sourceColumns: properties,
        attributes: {},
        row: { literal: true },
        propertyRead: {},
      },
    };
    await db.pool.query(
      'INSERT INTO moria.native_serving_bindings(environment,resource,object_type_id,revision,definition) VALUES($1,$2,$3,$4,$5)',
      [environment, 'records', definition.objectTypeId, fingerprint(definition), definition],
    );
    await db.pool.query(
      'CREATE TABLE public.sandbox_protected_logs(execution uuid NOT NULL,message text NOT NULL,markings jsonb NOT NULL)',
    );
    await db.pool.query(
      'CREATE TABLE public.sandbox_protected_callbacks(execution uuid NOT NULL,call_id text NOT NULL,body_hash text NOT NULL,markings jsonb NOT NULL,PRIMARY KEY(execution,call_id))',
    );
    const role = `sandbox_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(
      `CREATE ROLE ${role} LOGIN PASSWORD '${password}'; GRANT USAGE ON SCHEMA moria,public TO ${role}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria,public TO ${role}; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    const url = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const binary =
      process.env.MORIA_RUNTIME_BINARY ??
      process.env.MORIA_INDEXING_BINARY ??
      '/usr/local/bin/moria-indexing';
    const env = {
      PATH: process.env.PATH,
      MORIA_INDEXING_DATABASE_SECRET_REF: 'SANDBOX_FIXTURE_DATABASE',
      SANDBOX_FIXTURE_DATABASE: url,
      MORIA_INDEXING_DATABASE_TLS: 'disable',
    };
    const native = async (command: string, input: unknown) => {
      const child = spawn(binary, ['execution', command], { env, stdio: ['pipe', 'pipe', 'pipe'] });
      children.push(child);
      let out = '',
        err = '';
      child.stdout.on('data', (b) => (out += b));
      child.stderr.on('data', (b) => (err += b));
      child.stdin.end(JSON.stringify(input));
      await new Promise<void>((resolve, reject) => {
        child.once('error', reject);
        child.once('exit', (code) =>
          code === 0 ? resolve() : reject(new Error(`Native execution failed: ${err}`)),
        );
      });
      return JSON.parse(out);
    };
    const serving = spawn(binary, ['serve'], {
      env: { ...env, MORIA_NATIVE_LISTEN: '127.0.0.1:0' },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    children.push(serving);
    let servingOutput = '',
      endpoint = '',
      servingErrors = '';
    serving.stdout.on('data', (b) => (servingOutput += b));
    serving.stderr.on('data', (b) => (servingErrors += b));
    await until(async () => {
      assert.equal(serving.exitCode, null, servingErrors);
      for (const line of servingOutput.split('\n')) {
        try {
          const value = JSON.parse(line);
          if (value.event === 'serving') endpoint = `http://${value.address}`;
        } catch {}
      }
      return !!endpoint;
    });
    const harness = (lease?: unknown, claimOwner?: string) => {
      const child = spawn(
        process.env.MORIA_NATIVE_SANDBOX_TEST_BINARY ?? '/usr/local/bin/moria-native-sandbox-test',
        ['--ignored', '--exact', 'native_sandbox_driver', '--nocapture'],
        {
          env: {
            ...env,
            MORIA_SANDBOX_TEST_HOST: host,
            MORIA_SANDBOX_TEST_ENDPOINT: endpoint,
            MORIA_SANDBOX_TEST_TOKEN: issued.token,
            ...(claimOwner
              ? { MORIA_SANDBOX_TEST_CLAIM_OWNER: claimOwner }
              : lease
                ? { MORIA_SANDBOX_TEST_LEASE: JSON.stringify(lease) }
                : { MORIA_SANDBOX_TEST_CLEANUP: '1' }),
            MORIA_FUNCTION_TEST_SECRET: 'must-never-enter-guest',
            PGHOST: 'must-never-enter-guest',
            AWS_SECRET_ACCESS_KEY: 'must-never-enter-guest',
          },
          stdio: ['ignore', 'pipe', 'pipe'],
        },
      );
      children.push(child);
      let out = '',
        err = '';
      child.stdout.on('data', (b) => (out += b));
      child.stderr.on('data', (b) => (err += b));
      const done = new Promise<any>((resolve, reject) => {
        child.once('error', reject);
        child.once('exit', (code, signal) => {
          if (code !== 0)
            return reject(new Error(`Native sandbox driver exited ${code ?? signal}: ${err}`));
          const line = out.split('\n').find((line) => line.startsWith('SANDBOX_RESULT='));
          assert.ok(line, out);
          resolve(JSON.parse(line.slice('SANDBOX_RESULT='.length)));
        });
      });
      void done.catch(() => {});
      return { child, done };
    };
    const claim = async () => {
      const owner = randomUUID();
      await db.pool.query(
        "INSERT INTO moria.runtime_function_workers(owner,host_id,incarnation,resource_classes,expires_at) SELECT $1,$2,incarnation,ARRAY['standard'],clock_timestamp()+interval '1 hour' FROM moria.indexing_instance",
        [owner, host],
      );
      return harness(undefined, owner).done;
    };
    const binding = async (cpu = 5000, timeout = 30, artifact = image.artifact) => {
      const spec = {
        version: 1,
        domain: 'function',
        release: 'b'.repeat(64),
        resourceClass: 'standard',
        timeoutSeconds: 30,
        retryPolicy: 'never',
        inputSchema: {
          type: 'object',
          properties: {
            mode: { type: 'string', nullable: false },
            value: properties.value,
            uid: { type: 'uid', nullable: true },
          },
        },
        pinnedInputs: [],
        expectedHeads: {},
      };
      const id = (
        await db.pool.query(
          'INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation) SELECT $1,$2,$3,$4,$5,$6,$7,i.security_epoch,c.incarnation FROM moria.instance_state i,moria.indexing_instance c RETURNING id',
          [environment, resource, spec, context, operations, ['private'], epoch],
        )
      ).rows[0].id;
      await db.pool.query(
        "INSERT INTO moria.runtime_function_bindings(binding,artifact_uri,artifact_digest,outputs,effect,memory_mib,cpu_millis,timeout_seconds) VALUES($1,$2,$3,$4,'readOnly',128,$5,$6)",
        [id, artifact.uri, artifact.digest, { value: properties.value }, cpu, timeout],
      );
      return id;
    };
    const standard = await binding();
    const request = async (mode: string, registered = standard) => {
      const { id } = await native('request', {
        binding: registered,
        requestKey: randomUUID(),
        input: { mode, value: 3, uid },
        tasks: [{ key: 'function', partition: null }],
      });
      executions.push(id);
      const work = await claim();
      assert.equal(work.lease.execution, id);
      return { id: id as string, lease: work.lease };
    };
    const journal = async (id: string) =>
      (
        await db.pool.query('SELECT * FROM moria.runtime_function_sandboxes WHERE execution=$1', [
          id,
        ])
      ).rows[0];
    const running = async (id: string) =>
      until(async () => (await journal(id))?.state === 'running');
    const absent = async (id: string) =>
      assert.equal(
        (
          await docker(
            'ps',
            '--all',
            '--filter',
            `name=^moria-native-function-${id}$`,
            '--format',
            '{{.ID}}',
          )
        ).trim(),
        '',
      );
    const expire = async (id: string) => {
      await db.pool.query(
        "UPDATE moria.runtime_execution_tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE execution=$1",
        [id],
      );
      assert.equal(await claim(), null);
    };

    await t.test(
      'typed results, real native object reads and protected logs cross the guest protocol',
      async () => {
        for (const [mode, value] of [
          ['normal', 4],
          ['read', 17],
          ['sdk-read', 17],
        ] as const) {
          const { id, lease } = await request(mode),
            result = await harness(lease).done;
          assert.equal(result.ok, true, JSON.stringify(result));
          assert.equal(result.output.result.value, value);
          assert.deepEqual(result.output.consumedMarkings, [{ environment, marking: 'private' }]);
          assert.equal((await journal(id)).state, 'sealed');
          assert.equal((await journal(id)).cleaned, true);
          const task = (
            await db.pool.query(
              'SELECT status,attempts FROM moria.runtime_execution_tasks WHERE execution=$1',
              [id],
            )
          ).rows[0];
          assert.equal(task.status, 'sealed');
          assert.equal(task.attempts, 1);
          assert.ok(
            (
              await db.pool.query(
                'SELECT count(*) FROM public.sandbox_protected_logs WHERE execution=$1',
                [id],
              )
            ).rows[0].count > 0,
          );
          await absent(id);
          const replay = await harness(lease).done;
          assert.equal(replay.ok, false);
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.runtime_function_sandboxes WHERE execution=$1',
                [id],
              )
            ).rows[0].count,
            '1',
          );
        }
      },
    );
    await t.test(
      'Docker enforces UID, namespaces, credentials, root, memory, PID and CPU boundaries',
      async () => {
        for (const mode of ['isolation', 'pids']) {
          const { id, lease } = await request(mode),
            result = await harness(lease).done;
          assert.equal(result.ok, true, JSON.stringify(result));
          assert.equal(result.output.result.value, 1);
          await absent(id);
        }
        const lowCpu = await binding(300);
        for (const mode of ['oom', 'spin']) {
          const { id, lease } = await request(mode, mode === 'spin' ? lowCpu : standard),
            result = await harness(lease).done;
          assert.equal(result.ok, false);
          if (mode === 'spin') assert.equal(result.code, 'FUNCTION_CPU_LIMIT');
          assert.equal((await journal(id)).cleaned, true);
          await absent(id);
        }
      },
    );
    await t.test('malformed frames, bounded logs and invalid results fail closed', async () => {
      for (const mode of ['protocol', 'flood', 'huge', 'bad', 'nan', 'undefined', 'throw']) {
        const { id, lease } = await request(mode),
          result = await harness(lease).done;
        assert.equal(result.ok, false, mode);
        assert.equal((await journal(id)).state, 'failed');
        assert.equal((await journal(id)).cleaned, true);
        await absent(id);
      }
    });
    await t.test(
      'a result requires the completion handshake and successful exit without trailing output',
      async () => {
        const directory = await mkdtemp(join(tmpdir(), 'native-sandbox-handshake-'));
        const repository = image.artifact.uri
          .slice(6)
          .replace(/\/[^/]+$/, '/moria-sandbox-handshake');
        try {
          await writeFile(
            join(directory, 'guest.mjs'),
            `import readline from 'node:readline';import {writeFile} from 'node:fs/promises';
let mode;const send=(value=4)=>process.stdout.write(JSON.stringify({version:1,type:'result',value:{value}})+'\\n');
readline.createInterface({input:process.stdin}).on('line',async line=>{const m=JSON.parse(line);if(m.type==='start'){mode=m.input.mode;if(mode==='missing')process.exit(0);if(mode==='rootfs'){try{await writeFile('/world-writable','escape');send(0);}catch(error){send(error.code==='EROFS'?1:0);}return;}send();if(mode==='premature')process.exit(0);}else if(m.type==='complete'){if(mode==='rootfs')process.exit(0);if(mode==='nonzero')process.exit(7);send();process.exit(0);}});
`,
          );
          await writeFile(
            join(directory, 'Dockerfile'),
            `FROM ${image.artifact.uri.slice(6)}@${image.artifact.digest}\nRUN touch /world-writable && chmod 666 /world-writable\nCOPY guest.mjs /guest.mjs\nCMD ["node","/guest.mjs"]\n`,
          );
          await docker('build', '--network=none', '--pull=false', '--tag', repository, directory);
          const pushed = await docker('push', repository),
            digest = pushed.match(/digest: (sha256:[0-9a-f]{64})/)?.[1];
          assert.ok(digest);
          const bad = await binding(5000, 30, { uri: `oci://${repository}`, digest });
          const protectedRoot = await request('rootfs', bad),
            protectedResult = await harness(protectedRoot.lease).done;
          assert.equal(protectedResult.ok, true, JSON.stringify(protectedResult));
          assert.equal(protectedResult.output.result.value, 1);
          await absent(protectedRoot.id);
          for (const mode of ['missing', 'premature', 'nonzero', 'trailing']) {
            const { id, lease } = await request(mode, bad),
              result = await harness(lease).done;
            assert.equal(result.ok, false, mode);
            assert.equal((await journal(id)).state, 'failed');
            await absent(id);
          }
        } finally {
          await docker('image', 'rm', '--force', repository).catch(() => {});
          await rm(directory, { recursive: true, force: true });
        }
      },
    );
    await t.test(
      'foreign owners cannot start code and cancellation removes the container within ten seconds',
      async () => {
        const { id, lease } = await request('hold');
        assert.equal((await harness({ ...lease, owner: randomUUID() }).done).ok, false);
        assert.equal(await journal(id), undefined);
        const worker = harness(lease);
        await running(id);
        const start = Date.now();
        await native('cancel', { id });
        assert.equal((await worker.done).ok, false);
        assert.ok(Date.now() - start < 10000);
        assert.equal((await journal(id)).cleaned, true);
        await absent(id);
      },
    );
    await t.test(
      'deadlines and local cancellation signals clean up without rerunning code',
      async () => {
        const short = await binding(5000, 2),
          timed = await request('hold', short);
        assert.equal((await harness(timed.lease).done).code, 'FUNCTION_TIMEOUT');
        await absent(timed.id);
        const { id, lease } = await request('hold'),
          worker = harness(lease);
        await running(id);
        worker.child.kill('SIGTERM');
        assert.equal((await worker.done).code, 'CANCELLED');
        await absent(id);
      },
    );
    await t.test(
      'supervisor process death fences the only attempt and durable host cleanup removes the orphan',
      async () => {
        const { id, lease } = await request('hold'),
          worker = harness(lease);
        await running(id);
        worker.child.kill('SIGKILL');
        await assert.rejects(worker.done, /SIGKILL/);
        assert.equal((await journal(id)).cleaned, false);
        await expire(id);
        assert.equal((await harness().done).cleaned, 1);
        assert.equal((await harness().done).cleaned, 0);
        await absent(id);
        assert.equal(
          (
            await db.pool.query(
              'SELECT attempts FROM moria.runtime_execution_tasks WHERE execution=$1',
              [id],
            )
          ).rows[0].attempts,
          1,
        );
        assert.equal((await harness(lease).done).ok, false);
      },
    );
    await t.test('revoked sessions stop live code before protected output can seal', async () => {
      const { id, lease } = await request('hold'),
        worker = harness(lease);
      await running(id);
      await db.pool.query('UPDATE moria.workload_sessions SET disabled=true WHERE token_hash=$1', [
        sessionHash,
      ]);
      assert.equal((await worker.done).ok, false);
      await absent(id);
      assert.equal(
        (
          await db.pool.query(
            'SELECT status FROM moria.runtime_execution_tasks WHERE execution=$1',
            [id],
          )
        ).rows[0].status,
        'cancelled',
      );
    });
  },
);
