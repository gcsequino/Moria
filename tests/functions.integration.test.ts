import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { functionImage } from './fixtures/functions/image.js';
import { functionPlatform } from './fixtures/functions/platform.js';
import { DockerFunctionWorker } from '../services/functions/src/docker.js';
import { Functions } from '../services/functions/src/service.js';
import { execFile, fork } from 'node:child_process';
import { promisify } from 'node:util';
import { once } from 'node:events';
import { join } from 'node:path';
import { writeFile } from 'node:fs/promises';
import { createApi } from '../apps/api/src/server.js';
import { functionsCommand } from '../apps/cli/src/functions.js';
import { objectClient } from '../packages/osdk/src/runtime.js';
import { createTransport } from '../packages/platform/src/transport.js';

const exec = promisify(execFile);
const docker = async (args: string[]) =>
  (await exec('docker', args, { timeout: 15000, maxBuffer: 1048576 })).stdout;
async function until(check: () => Promise<boolean>, milliseconds = 30000) {
  const end = Date.now() + milliseconds;
  while (Date.now() < end) {
    if (await check()) return;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  assert.fail('Condition did not complete before deadline');
}

test(
  'published TypeScript functions execute in real isolated containers with durable authorized outcomes',
  { timeout: 180000 },
  async (t) => {
    const image = await functionImage();
    t.after(image.close);
    const p = await functionPlatform(image.artifact);
    t.after(p.close);
    const { functions, owner, resourceId } = p,
      context = owner.context;
    const invoke = async (mode: string, key = randomUUID(), value = 3, uid?: string) => {
      const invocation = await functions.invoke(context, resourceId, key, {
        mode,
        value,
        ...(uid ? { uid } : {}),
      });
      return functions.wait(context, invocation.id, 25000);
    };
    let recordUid: string;
    await t.test(
      'invalid native values fail before JSON can coerce them into nullable results',
      async () => {
        await p.publish({
          ...p.declaration,
          spec: { ...p.declaration.spec, outputs: { value: { type: 'float64', nullable: true } } },
        });
        for (const mode of ['nan', 'undefined']) {
          const result = await invoke(mode);
          assert.equal(result.status, 'failed', JSON.stringify(result));
          assert.equal(result.result, undefined);
        }
        await p.publish();
      },
    );
    await t.test(
      'versioned structured values remain exact through real function pipes and typed replay',
      async () => {
        const payloadType = {
          type: 'object',
          properties: {
            count: { type: 'int64', nullable: false },
            amount: { type: 'decimal', nullable: false, precision: 38, scale: 9 },
            instant: { type: 'timestamp', nullable: false },
            items: {
              type: 'list',
              items: {
                type: 'object',
                properties: {
                  flag: { type: 'boolean', nullable: false },
                  note: { type: 'string', nullable: true },
                },
                optional: ['note'],
              },
            },
          },
        };
        await p.publish({
          ...p.declaration,
          spec: {
            ...p.declaration.spec,
            inputs: { mode: { type: 'string', nullable: false }, payload: payloadType },
            outputs: { payload: payloadType },
          },
        });
        const input = {
          mode: 'echo',
          payload: {
            count: '9223372036854775807',
            amount: '1.2',
            instant: '2026-10-29T00:00:00Z',
            items: [{ flag: true }, { flag: false, note: null }],
          },
        };
        const key = randomUUID();
        const accepted = await functions.invoke(context, resourceId, key, input);
        const result = await functions.wait(context, accepted.id);
        assert.equal(result.status, 'succeeded', JSON.stringify(result));
        assert.equal(result.valueVersion, 1);
        assert.deepEqual(result.result, {
          payload: {
            ...input.payload,
            amount: '1.200000000',
            instant: '2026-10-29T00:00:00.000000Z',
          },
        });
        assert.equal(
          (
            await functions.invoke(context, resourceId, key, {
              ...input,
              payload: result.result!.payload,
            })
          ).id,
          result.id,
        );
        await assert.rejects(
          functions.invoke(context, resourceId, randomUUID(), {
            ...input,
            payload: { ...input.payload, count: 42 },
          }),
          { code: 'VALUE_INVALID' },
        );
        await p.pool.query('UPDATE moria.function_invocations SET value_version=2 WHERE id=$1', [
          result.id,
        ]);
        await assert.rejects(functions.get(context, result.id), {
          code: 'UNSUPPORTED_VALUE_VERSION',
        });
        await p.pool.query('UPDATE moria.function_invocations SET value_version=1 WHERE id=$1', [
          result.id,
        ]);
        await p.publish({
          ...p.declaration,
          spec: { ...p.declaration.spec, inputs: {}, outputs: {} },
        });
        const empty = await functions.invoke(context, resourceId, randomUUID(), {});
        assert.deepEqual((await functions.wait(context, empty.id)).result, {});
        await p.publish();
      },
    );
    await t.test(
      'CLI invokes, inspects and cancels real containers through authenticated HTTP',
      async () => {
        const server = createApi(p.sessions, { objects: new Map(), actions: new Map(), functions });
        await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
        const url = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
        const config = join(p.directory, 'function-cli.json');
        const previous = process.env.MORIA_API_TOKEN;
        process.env.MORIA_API_TOKEN = owner.token;
        try {
          await writeFile(config, JSON.stringify({ mode: 'normal', value: 41 }));
          const invocation: any = await functionsCommand(['invoke', resourceId], {
            url,
            config,
            key: randomUUID(),
          });
          assert.deepEqual(invocation.result, { value: 42 });
          const inspected: any = await functionsCommand(['get', invocation.id], { url });
          assert.equal(inspected.id, invocation.id);
          const page: any = await functionsCommand(['events', invocation.id], { url, cursor: '0' });
          assert.equal(page.events.at(-1).message, 'succeeded');
          await writeFile(config, JSON.stringify({ mode: 'hold', value: 0 }));
          const running: any = await functionsCommand(['invoke', resourceId], {
            url,
            config,
            key: randomUUID(),
            async: true,
          });
          await until(async () =>
            (await functions.events(context, running.id)).some(
              (event) => event.message === 'function started',
            ),
          );
          const cancelled: any = await functionsCommand(['cancel', running.id], { url });
          assert.equal(cancelled.status, 'cancelled');
          await until(
            async () =>
              (
                await p.pool.query(
                  'SELECT worker_cleaned FROM moria.function_invocations WHERE id=$1',
                  [running.id],
                )
              ).rows[0].worker_cleaned,
          );
          assert.equal(
            (
              await docker([
                'ps',
                '--all',
                '--quiet',
                '--filter',
                `name=^moria-function-${running.id}$`,
              ])
            ).trim(),
            '',
          );
        } finally {
          if (previous === undefined) delete process.env.MORIA_API_TOKEN;
          else process.env.MORIA_API_TOKEN = previous;
          server.closeAllConnections();
          await new Promise<void>((resolve) => server.close(() => resolve()));
        }
      },
    );
    await t.test(
      'typed result, retained identity, ordered events and exact-key replay',
      async () => {
        const key = randomUUID(),
          result = await invoke('normal', key);
        assert.equal(result.status, 'succeeded');
        assert.deepEqual(result.result, { value: 4 });
        assert.equal(result.artifact.digest, image.artifact.digest);
        assert.equal((await invoke('normal', key)).id, result.id);
        const events = await functions.events(context, result.id);
        assert.deepEqual(
          events.map((event) => event.type),
          ['state', 'state', 'log', 'state'],
        );
        assert.deepEqual(
          events.map((event) => event.sequence),
          ['1', '2', '3', '4'],
        );
        assert.equal((await functions.events(context, result.id, '3')).length, 1);
        await assert.rejects(
          functions.invoke(context, resourceId, key, { mode: 'normal', value: 4 }),
          { code: 'IDEMPOTENCY_CONFLICT' },
        );
        await assert.rejects(functions.get(p.other.context, result.id), { code: 'NOT_FOUND' });
        await assert.rejects(functions.events(p.other.context, result.id), { code: 'NOT_FOUND' });
      },
    );
    await t.test(
      'malformed input never reserves a key, and code/output/protocol failures never publish results',
      async () => {
        const before = (await p.pool.query('SELECT count(*) FROM moria.function_invocations'))
          .rows[0].count;
        await assert.rejects(
          functions.invoke(context, resourceId, randomUUID(), { mode: 'normal', value: 'bad' }),
          { code: 'VALUE_INVALID' },
        );
        assert.equal(
          (await p.pool.query('SELECT count(*) FROM moria.function_invocations')).rows[0].count,
          before,
        );
        for (const mode of ['bad', 'throw', 'protocol', 'huge', 'flood']) {
          const result = await invoke(mode);
          assert.equal(result.status, 'failed', mode);
          assert.equal(result.result, undefined);
          assert.ok(
            !JSON.stringify(await functions.events(context, result.id)).includes(
              'private worker exception',
            ),
          );
        }
      },
    );
    await t.test(
      'worker cannot access host credentials, root writes, daemon socket, external or host network',
      async () => {
        process.env.MORIA_FUNCTION_TEST_SECRET = 'synthetic-private-supervisor-value';
        try {
          const port = Number(new URL(image.artifact.uri.replace('oci:', 'http:')).port);
          const result = await invoke('isolation', randomUUID(), port);
          assert.equal(result.status, 'succeeded');
          assert.deepEqual(result.result, { value: 1 });
        } finally {
          delete process.env.MORIA_FUNCTION_TEST_SECRET;
        }
        assert.deepEqual((await invoke('forbidden')).result, { value: 1 });
      },
    );
    await t.test(
      'real container OSDK reads retain current classifications and cannot bypass revocation',
      async () => {
        const uid = await p.catalog.resolveIdentity('function-container-records', 'record'),
          state = await p.authority.current(p.environment, 'author', p.application),
          revision = (
            await p.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [
              p.environment,
            ])
          ).rows[0].revision;
        const stage = await p.objectStore.stage(
          p.environment,
          'records',
          p.records.binding.schema,
          [{ uid, value: 41 }],
          state.epoch,
          revision,
        );
        recordUid = uid;
        await p.objectStore.publishStage(stage);
        p.records.binding.policies.markings = ['protected'];
        const accepted = await functions.invoke(context, resourceId, randomUUID(), {
            mode: 'read',
            uid,
            value: 0,
          }),
          result = await functions.wait(context, accepted.id);
        assert.equal(result.status, 'succeeded');
        assert.deepEqual(result.result, { value: 41 });
        assert.ok(result.markings.some((label) => label.marking === 'protected'));
        await p.authority.setGrants(
          p.environment,
          'operator',
          'author',
          p.application,
          p.operations.filter((operation) => operation !== 'records:read'),
        );
        await assert.rejects(functions.get(context, result.id), { code: 'FORBIDDEN' });
        await p.authority.setGrants(
          p.environment,
          'operator',
          'author',
          p.application,
          p.operations,
        );
      },
    );
    await t.test(
      'generated ontology types hydrate references and await promised object/list results in real containers',
      async () => {
        const objectType = '00000000-0000-4000-8000-000000000004' as const;
        const customerType = { type: 'ontologyObject', objectType, nullable: false };
        await p.publish({
          ...p.declaration,
          spec: {
            ...p.declaration.spec,
            inputs: {
              mode: { type: 'string', nullable: false },
              customer: customerType,
              customers: { type: 'list', items: customerType },
            },
            outputs: {
              customer: customerType,
              customers: { type: 'list', items: customerType },
              value: { type: 'float64', nullable: false },
            },
          },
        });
        const records = objectClient<{ uid: string; value: number }, typeof objectType>(
          createTransport({
            fetch: async () => {
              throw new Error('Constructing an identity must not fetch properties');
            },
          }),
          'records',
          p.records.binding.schema.properties,
          objectType,
        );
        const customer = records.object(recordUid);
        const key = randomUUID();
        const accepted = await functions.invoke(context, resourceId, key, {
          mode: 'objects',
          customer,
          customers: [customer, customer],
        });
        const result = await functions.wait(context, accepted.id);
        assert.equal(result.status, 'succeeded', JSON.stringify(result));
        const identity = { objectType, uid: recordUid };
        assert.deepEqual(result.result, {
          customer: identity,
          customers: [identity, identity],
          value: 41,
        });
        assert(result.markings.some((label) => label.marking === 'protected'));
        assert.equal(
          (
            await functions.invoke(context, resourceId, key, {
              mode: 'objects',
              customer: identity,
              customers: [identity, identity],
            })
          ).id,
          result.id,
        );
        const stored = (
          await p.pool.query('SELECT input FROM moria.function_invocations WHERE id=$1', [
            result.id,
          ])
        ).rows[0].input;
        assert.deepEqual(stored.customer, identity);
        assert.equal(Object.hasOwn(stored.customer, 'value'), false);
        await p.publish();
      },
    );
    await t.test(
      'generated OSDK and raw platform transports run inside real containers',
      async () => {
        assert.deepEqual((await invoke('sdk-read', randomUUID(), 0, recordUid)).result, {
          value: 41,
        });
        assert.deepEqual((await invoke('platform')).result, { value: 1 });
        await p.publish({ ...p.declaration, spec: { ...p.declaration.spec, effect: 'readWrite' } });
        const action = await invoke('action', randomUUID(), 52, recordUid);
        assert.equal(action.status, 'succeeded', JSON.stringify(action));
        assert.equal(action.calls?.[0].status, 'succeeded');
        const failed = await invoke('action-fail', randomUUID(), 53, recordUid);
        assert.equal(failed.status, 'failed');
        assert.equal(failed.calls?.[0].status, 'succeeded');
        assert.equal((await p.records.read(context.session, recordUid)).object.value, 53);
        await p.publish({
          ...p.declaration,
          spec: { ...p.declaration.spec, effect: 'editProposal' },
        });
        const proposed = await invoke('proposal', randomUUID(), 60, recordUid);
        assert.equal(proposed.status, 'succeeded', JSON.stringify(proposed));
        assert.equal(proposed.calls?.[0].status, 'committed');
        assert.equal(
          (await invoke('proposal-rollback', randomUUID(), 61, recordUid)).status,
          'failed',
        );
        assert.equal((await p.records.read(context.session, recordUid)).object.value, 60);
        await p.publish();
      },
    );
    await t.test(
      'real function containers compose through the platform API with bounded parent identity',
      async () => {
        const child = { ...p.declaration, metadata: { ...p.declaration.metadata, name: 'child' } };
        const published = await p.runtime.source.publish(
          context,
          p.root.id,
          (await p.runtime.source.checkout(context, p.root.id)).digest,
          [p.declaration, child],
        );
        await p.runtime.source.apply(
          context,
          (await p.runtime.source.plan(context, p.root.id, published.digest)).id,
        );
        const childId = published.resources.find(
          (resource) => resource.reference === 'logic/Function/child',
        )!.id;
        const result = await invoke('nested', randomUUID(), 8, childId);
        assert.equal(result.status, 'succeeded', JSON.stringify(result));
        assert.deepEqual(result.result, { value: 9 });
        const record = (
          await p.pool.query(
            'SELECT id,parent_id,deadline FROM moria.function_invocations WHERE parent_id=$1',
            [result.id],
          )
        ).rows[0];
        assert.equal(record.parent_id, result.id);
        const parent = (
          await p.pool.query('SELECT deadline FROM moria.function_invocations WHERE id=$1', [
            result.id,
          ])
        ).rows[0];
        assert(record.deadline <= parent.deadline);
        await p.publish();
      },
    );
    await t.test(
      'CPU, memory and process limits terminate or reject actual resource exhaustion',
      async () => {
        await p.publish({
          ...p.declaration,
          spec: { ...p.declaration.spec, limits: { ...p.declaration.spec.limits, cpuMillis: 1 } },
        });
        const short = await invoke('normal');
        assert.equal(short.status, 'failed');
        assert.equal(short.error?.code, 'FUNCTION_CPU_LIMIT');
        await p.publish({
          ...p.declaration,
          spec: { ...p.declaration.spec, limits: { ...p.declaration.spec.limits, cpuMillis: 100 } },
        });
        const cpu = await invoke('spin');
        assert.equal(cpu.status, 'failed');
        assert.equal(cpu.error?.code, 'FUNCTION_CPU_LIMIT');
        await p.publish();
        assert.equal((await invoke('oom')).status, 'failed');
        await p.publish({
          ...p.declaration,
          spec: { ...p.declaration.spec, limits: { ...p.declaration.spec.limits, memoryMiB: 256 } },
        });
        assert.deepEqual((await invoke('pids')).result, { value: 1 });
        await p.publish();
      },
    );
    await t.test(
      'competing identical invocations accept one attempt and redeployment cannot replace its result',
      async () => {
        const key = randomUUID(),
          requests = Array.from({ length: 5 }, () =>
            functions.invoke(context, resourceId, key, { mode: 'normal', value: 8 }),
          );
        const accepted = await Promise.all(requests);
        assert.equal(new Set(accepted.map((item) => item.id)).size, 1);
        const result = await functions.wait(context, accepted[0].id);
        assert.deepEqual(result.result, { value: 9 });
        const oldRelease = result.release;
        await p.publish({
          ...p.declaration,
          spec: {
            ...p.declaration.spec,
            limits: { ...p.declaration.spec.limits, timeoutSeconds: 19 },
          },
        });
        assert.equal(
          (await functions.invoke(context, resourceId, key, { mode: 'normal', value: 8 })).release,
          oldRelease,
        );
        assert.equal(
          (
            await p.pool.query(
              'SELECT count(*) FROM moria.function_invocations WHERE request_key=$1',
              [key],
            )
          ).rows[0].count,
          '1',
        );
      },
    );
    await t.test(
      'cancellation persists, timeout kills code, and revoked authority blocks result access',
      async () => {
        const running = await functions.invoke(context, resourceId, randomUUID(), {
          mode: 'sleep',
          value: 0,
        });
        assert.equal((await functions.cancel(context, running.id)).status, 'cancelled');
        assert.equal((await functions.get(context, running.id)).result, undefined);
        await p.publish({
          ...p.declaration,
          spec: {
            ...p.declaration.spec,
            limits: { ...p.declaration.spec.limits, timeoutSeconds: 1 },
          },
        });
        const timeout = await invoke('sleep');
        assert.equal(timeout.status, 'failed');
        assert.equal(timeout.error?.code, 'FUNCTION_TIMEOUT');
        await p.publish();
        const successful = await invoke('normal');
        await p.authority.setGrants(
          p.environment,
          'operator',
          'author',
          p.application,
          p.operations.filter((operation) => operation !== 'functions:invoke'),
        );
        await assert.rejects(functions.get(context, successful.id), { code: 'FORBIDDEN' });
        await assert.rejects(functions.events(context, successful.id), { code: 'FORBIDDEN' });
        await p.authority.setGrants(
          p.environment,
          'operator',
          'author',
          p.application,
          p.operations,
        );
      },
    );
    await t.test(
      'expired worker fences cannot publish or reexecute, and expired results retain their keys',
      async () => {
        const result = await invoke('normal');
        await p.pool.query(
          "UPDATE moria.function_invocations SET status='running',result=NULL,lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
          [result.id],
        );
        const lost = await functions.get(context, result.id);
        assert.equal(lost.status, 'failed');
        assert.equal(lost.error?.code, 'FUNCTION_WORKER_LOST');
        const key = randomUUID(),
          finished = await invoke('normal', key);
        await p.pool.query(
          "UPDATE moria.function_invocations SET finished_at=clock_timestamp()-interval '31 days' WHERE id=$1",
          [finished.id],
        );
        await assert.rejects(
          functions.invoke(context, resourceId, key, { mode: 'normal', value: 3 }),
          { code: 'RESULT_EXPIRED' },
        );
        assert.equal(
          (
            await p.pool.query(
              'SELECT count(*) FROM moria.function_invocations WHERE request_key=$1',
              [key],
            )
          ).rows[0].count,
          '1',
        );
      },
    );
    await t.test(
      'a killed supervisor is fenced and a replacement removes its surviving sandbox without rerunning code',
      async () => {
        await p.publish();
        await functions.close();
        const api = new Functions(p.runtime);
        t.after(() => api.close());
        const startWorker = async () => {
          const child = fork(new URL('./fixtures/functions/worker.ts', import.meta.url), [], {
            execArgv: ['--import', 'tsx'],
            stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
          });
          child.stdout?.resume();
          child.stderr?.resume();
          const exited = new Promise<number | null>((resolve) => child.once('exit', resolve));
          t.after(async () => {
            if (child.exitCode === null && child.signalCode === null) {
              child.kill('SIGTERM');
              await exited;
            }
          });
          const ready = once(child, 'message');
          const { host, port, user, password, database } = p.pool.options;
          child.send({
            connection: { host, port, user, password, database },
            files: join(p.directory, 'files'),
          });
          await Promise.race([
            ready,
            exited.then(() => {
              throw new Error('Function test worker exited before readiness');
            }),
            new Promise((_, reject) => {
              const timer = setTimeout(
                () => reject(new Error('Function test worker startup timed out')),
                10000,
              );
              timer.unref();
              ready.finally(() => clearTimeout(timer)).catch(() => {});
            }),
          ]);
          return { child, exited };
        };
        const first = await startWorker();
        const accepted = await api.invoke(context, resourceId, randomUUID(), {
          mode: 'hold',
          value: 0,
        });
        const name = 'moria-function-' + accepted.id;
        await until(
          async () =>
            (await docker(['ps', '-q', '--filter', 'name=^/' + name + '$'])).trim() !== '',
        );
        await until(async () =>
          (await api.events(context, accepted.id)).some(
            (event) => event.message === 'function started',
          ),
        );
        first.child.kill('SIGKILL');
        await first.exited;
        assert((await docker(['ps', '-q', '--filter', 'name=^/' + name + '$'])).trim());
        const second = await startWorker();
        try {
          const lost = await api.wait(context, accepted.id, 25000);
          assert.equal(lost.status, 'failed');
          assert.equal(lost.error?.code, 'FUNCTION_WORKER_LOST');
          await until(
            async () => !(await docker(['ps', '-aq', '--filter', 'name=^/' + name + '$'])).trim(),
          );
          assert.equal(
            (await api.events(context, accepted.id)).filter(
              (event) => event.message === 'function started',
            ).length,
            1,
          );
          assert.equal(
            (
              await p.pool.query('SELECT count(*) FROM moria.function_invocations WHERE id=$1', [
                accepted.id,
              ])
            ).rows[0].count,
            '1',
          );
        } finally {
          second.child.kill('SIGTERM');
          await second.exited;
          await api.close();
        }
      },
    );
    await t.test(
      'unknown artifact schemes fail without launching alternate host commands',
      async () => {
        const worker = new DockerFunctionWorker();
        await assert.rejects(
          worker.execute(
            randomUUID(),
            { uri: 'file:///bin/sh', digest: `sha256:${'a'.repeat(64)}` },
            { timeoutSeconds: 1 },
            {},
            async () => null,
            async () => {},
            new AbortController().signal,
          ),
          { code: 'FUNCTION_ARTIFACT_INVALID' },
        );
      },
    );
  },
);
