import assert from 'node:assert/strict';
import test from 'node:test';
import { randomUUID, createHash } from 'node:crypto';
import { functionPlatform } from './fixtures/functions/platform.js';
import type { FunctionWorker } from '../services/functions/src/worker.js';
import {
  createTransport,
  objectClient,
  actionClient,
  ApiError,
} from '../packages/osdk/src/runtime.js';
import { filesystemClient } from '../packages/platform/src/filesystem.js';

test(
  'functions use permission-checked ontology and platform APIs with explicit effects',
  { timeout: 120000 },
  async (t) => {
    const worker: FunctionWorker = {
      remove: async () => {},
      execute: async (id, _artifact, _limits, start: any, rpc) => {
        let sequence = 0;
        const fetcher: typeof fetch = async (path, options = {}) => {
          const headers = Object.fromEntries(new Headers(options.headers));
          const body =
            options.body === undefined
              ? null
              : Buffer.from(await new Response(options.body).arrayBuffer()).toString('base64');
          const response: any = await rpc({
            type: 'http',
            id: String(++sequence),
            path: String(path),
            method: options.method ?? 'GET',
            headers,
            body,
          });
          return new Response(Buffer.from(response.body, 'base64'), {
            status: response.status,
            headers: response.headers,
          });
        };
        const transport = createTransport({ fetch: fetcher });
        const columns = {
          uid: { type: 'uid', nullable: false },
          value: { type: 'float64', nullable: false },
        } as const;
        const records = objectClient<{ uid: string; value: number }>(transport, 'records', columns);
        const edit = actionClient<{ uid: string; value: number }>(
          transport,
          'edit-record',
          columns,
        );
        const reject = actionClient<{ uid: string; value: number }>(
          transport,
          'reject-record',
          columns,
        );
        const functionAction = actionClient<{ mode: string; uid: string; value: number }>(
          transport,
          'function-record',
          p.declaration.spec.inputs,
        );
        const fs = filesystemClient(transport);
        const { mode, uid, value } = start.input;
        if (mode === 'unknown-action') {
          const options = {
            method: 'POST',
            headers: { 'content-type': 'application/json', 'idempotency-key': 'missing-action' },
            body: JSON.stringify({ input: { uid, value } }),
          };
          const first = await fetcher('/api/v1/actions/missing/invoke', options);
          const second = await fetcher('/api/v1/actions/missing/invoke', options);
          assert.equal(first.status, 404);
          assert.equal(second.status, 404);
          assert.deepEqual(await first.json(), await second.json());
          return { value: second.status };
        }
        if (mode === 'calculate') return { value: (await records.get(uid)).object.value + value };
        if (mode === 'function-action' || mode === 'function-action-rollback') {
          await functionAction({ mode: 'calculate', uid, value }, { idempotencyKey: 'function' });
          if (mode === 'function-action-rollback')
            await reject({ uid, value: 999 }, { idempotencyKey: 'reject' });
          return { value };
        }
        if (mode === 'home') return { value: (await fs.home()).home.kind === 'directory' ? 1 : 0 };
        if (mode === 'headers') {
          const response = await fetcher('/api/management/v1/fs/home', {
            headers: { authorization: 'Bearer forged' },
          });
          return { value: response.status };
        }
        if (mode === 'external') return { value: (await fetcher('http://127.0.0.1/admin')).status };
        if (mode === 'read') return { value: (await records.get(uid)).object.value };
        if (mode === 'read-only-write') {
          try {
            await edit({ uid, value }, { idempotencyKey: 'edit' });
            return { value: 0 };
          } catch (error) {
            assert(error instanceof ApiError);
            assert.equal(error.code, 'FUNCTION_EFFECT');
            return { value: 403 };
          }
        }
        if (mode === 'write' || mode === 'write-fail') {
          await records.get(uid);
          await edit({ uid, value }, { idempotencyKey: 'edit' });
          await edit({ uid, value }, { idempotencyKey: 'edit' });
          if (mode === 'write-fail') throw new Error('after committed write');
          return { value: (await records.get(uid)).object.value };
        }
        if (mode.startsWith('proposal')) {
          const before = (await records.get(uid)).object.value;
          await edit({ uid, value }, { idempotencyKey: 'first' });
          assert.equal((await records.get(uid)).object.value, before);
          if (mode === 'proposal-rollback')
            await reject({ uid, value: 999 }, { idempotencyKey: 'second' });
          if (mode === 'proposal-invalid') return { value: 'invalid' };
          return { value };
        }
        if (mode === 'read-error') {
          try {
            await records.get(uid);
            return { value: 0 };
          } catch (error) {
            assert(error instanceof ApiError);
            assert.equal(error.code, 'NOT_FOUND');
            return { value: 1 };
          }
        }
        if (mode === 'tainted-write' || mode === 'tainted-error') {
          try {
            await records.get(uid);
          } catch (error) {
            if (mode !== 'tainted-error') throw error;
            assert(error instanceof ApiError);
            assert.equal(error.code, 'NOT_FOUND');
          }
          const parent = await fs.home();
          const response = await fetcher('/api/management/v1/fs/changes', {
            method: 'POST',
            headers: { 'content-type': 'application/json', 'idempotency-key': 'declassify' },
            body: JSON.stringify({
              changes: [
                {
                  op: 'create',
                  kind: 'directory',
                  parentId: parent.home.id,
                  expectedParentRevision: parent.home.revision,
                  name: 'leaked-' + id,
                },
              ],
            }),
          });
          assert.equal((await response.json()).error.code, 'MARKING_DOWNGRADE');
          return { value: response.status };
        }
        if (mode === 'file') {
          const directory = await fs.get(uid);
          const bytes = new TextEncoder().encode('hello');
          const receipt = await fs.writeFile(
            {
              op: 'create',
              parentId: uid,
              expectedParentRevision: directory.revision,
              name: 'result-' + id + '.txt',
              content: {
                size: bytes.length,
                digest: 'sha256:' + createHash('sha256').update(bytes).digest('hex'),
                mediaType: 'text/plain',
              },
            },
            bytes,
            { idempotencyKey: 'file' },
          );
          const file = receipt.entries.find((entry) => entry.kind === 'file')!;
          const response = await fetcher('/api/management/v1/fs/entries/' + file.id + '/content');
          assert.equal(response.headers.get('content-type'), 'text/plain');
          assert.equal(await response.text(), 'hello');
          return { value: 5 };
        }
        throw new Error('Unknown test mode');
      },
    };
    const p = await functionPlatform(
      { uri: 'oci://fixture/platform', digest: 'sha256:' + 'd'.repeat(64) },
      worker,
    );
    t.after(p.close);
    const uid = await p.catalog.resolveIdentity('function-platform', 'record');
    await p.objectStore.activate(
      p.environment,
      'records',
      p.records.binding.schema,
      [{ uid, value: 10 }],
      (await p.authority.current(p.environment, 'author', p.application)).epoch,
      (await p.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [p.environment]))
        .rows[0].revision,
    );
    const effect = async (effect: 'readOnly' | 'readWrite' | 'editProposal') =>
      p.publish({ ...p.declaration, spec: { ...p.declaration.spec, effect } });
    const invoke = async (mode: string, value = 20, target = uid) => {
      const accepted = await p.functions.invoke(p.owner.context, p.resourceId, randomUUID(), {
        mode,
        value,
        uid: target,
      });
      return p.functions.wait(p.owner.context, accepted.id);
    };
    const current = async () => (await p.records.read(p.owner.context.session, uid)).object.value;
    await t.test(
      'read-only SDK and raw platform reads work without identity override or external access',
      async () => {
        assert.deepEqual((await invoke('home')).result, { value: 1 });
        assert.deepEqual((await invoke('read')).result, { value: 10 });
        assert.deepEqual((await invoke('headers')).result, { value: 403 });
        assert.deepEqual((await invoke('external')).result, { value: 403 });
        assert.deepEqual((await invoke('read-only-write')).result, { value: 403 });
        assert.equal(await current(), 10);
      },
    );
    await t.test('read-write calls deduplicate and advance their own read evidence', async () => {
      await effect('readWrite');
      const result = await invoke('write', 21);
      assert.equal(result.status, 'succeeded', JSON.stringify(result));
      assert.deepEqual(result.result, { value: 21 });
      assert.equal(result.calls?.length, 1);
      assert.equal(result.calls?.[0].status, 'succeeded');
      assert.equal(await current(), 21);
      assert.equal(
        (await p.pool.query("SELECT count(*) FROM moria.action_keys WHERE action='edit-record'"))
          .rows[0].count,
        '1',
      );
    });
    await t.test('later function failure does not undo a committed call', async () => {
      const result = await invoke('write-fail', 22);
      assert.equal(result.status, 'failed');
      assert.equal(result.calls?.[0].status, 'succeeded');
      assert.equal(await current(), 22);
      const rejected = await invoke('unknown-action');
      assert.deepEqual(rejected.result, { value: 404 });
      assert.equal(rejected.calls?.[0].status, 'failed');
    });
    await t.test('edit proposals commit atomically after valid function output', async () => {
      await effect('editProposal');
      const result = await invoke('proposal', 30);
      assert.equal(result.status, 'succeeded', JSON.stringify(result));
      assert.equal(await current(), 30);
      assert.equal(result.calls?.[0].status, 'committed');
    });
    await t.test(
      'invalid function output and rejected later proposals commit nothing',
      async () => {
        const invalid = await invoke('proposal-invalid', 40);
        assert.equal(invalid.status, 'failed');
        assert.equal(await current(), 30);
        const rejected = await invoke('proposal-rollback', 41);
        assert.equal(rejected.status, 'failed');
        assert.equal(rejected.error?.code, 'ACTION_CONDITION_FAILED');
        assert.equal(await current(), 30);
        assert(rejected.calls?.every((call) => call.status === 'proposed'));
      },
    );
    await t.test(
      'binary filesystem transfers use the platform API and preserve own-write evidence',
      async () => {
        await effect('readWrite');
        const result = await invoke('file', 0, p.root.id);
        assert.equal(result.status, 'succeeded', JSON.stringify(result));
        assert.deepEqual(result.result, { value: 5 });
      },
    );
    await t.test(
      'reading marked objects cannot write into an unmarked filesystem destination',
      async () => {
        p.configureRecords((binding) => {
          binding.policies.markings = ['protected'];
        });
        const result = await invoke('tainted-write');
        assert.equal(result.status, 'succeeded', JSON.stringify(result));
        assert.deepEqual(result.result, { value: 403 });
        assert(result.markings.some((label) => label.marking === 'protected'));
        const home = await p.runtime.management.ensureHome(p.owner.context);
        assert(
          !(await p.runtime.filesystem.list(p.owner.context, home.home.id)).entries.some((entry) =>
            entry.name.startsWith('leaked-'),
          ),
        );
      },
    );
    await t.test(
      'a concurrent filesystem change after a committed write cannot replace its evidence',
      async () => {
        const original = p.functions.bindings.filesystem!;
        p.functions.bindings.filesystem = async (context, request, signal) => {
          const response = await original(context, request, signal);
          if (
            request.method === 'POST' &&
            request.path.includes('/files') &&
            response.status < 400
          ) {
            const parent = await p.runtime.filesystem.get(p.owner.context, p.root.id);
            await p.runtime.filesystem.apply(p.owner.context, randomUUID(), [
              {
                op: 'create',
                kind: 'directory',
                parentId: parent.id,
                expectedParentRevision: parent.revision,
                name: 'concurrent-' + randomUUID(),
              },
            ]);
          }
          return response;
        };
        try {
          const accepted = await p.functions.invoke(p.owner.context, p.resourceId, randomUUID(), {
            mode: 'file',
            value: 0,
            uid: p.root.id,
          });
          await assert.rejects(p.functions.wait(p.owner.context, accepted.id), {
            code: 'FUNCTION_INPUT_CHANGED',
          });
          for (let i = 0; i < 100; i++) {
            const row = (
              await p.pool.query('SELECT status FROM moria.function_invocations WHERE id=$1', [
                accepted.id,
              ])
            ).rows[0];
            if (row.status !== 'running') break;
            await new Promise((resolve) => setTimeout(resolve, 20));
          }
          const result = (
            await p.pool.query(
              'SELECT status,error,result FROM moria.function_invocations WHERE id=$1',
              [accepted.id],
            )
          ).rows[0];
          assert.equal(result.status, 'failed');
          assert.equal(result.error?.code, 'FUNCTION_INPUT_CHANGED');
          assert.equal(result.result, null);
        } finally {
          p.functions.bindings.filesystem = original;
        }
      },
    );
    await t.test(
      'declared function-backed actions compute outside the transaction and recheck conditions at commit',
      async () => {
        const definition = {
          ...p.declaration,
          metadata: { ...p.declaration.metadata, name: 'action-proposal' },
          spec: { ...p.declaration.spec, effect: 'editProposal' },
        };
        const publication = await p.runtime.source.publish(
          p.owner.context,
          p.root.id,
          (await p.runtime.source.checkout(p.owner.context, p.root.id)).digest,
          [{ ...p.declaration, spec: { ...p.declaration.spec, effect: 'readWrite' } }, definition],
        );
        await p.runtime.source.apply(
          p.owner.context,
          (await p.runtime.source.plan(p.owner.context, p.root.id, publication.digest)).id,
        );
        const functionId = publication.resources.find(
          (resource) => resource.reference === 'logic/Function/action-proposal',
        )!.id;
        const base = p.actions.get('edit-record')!;
        const action = {
          ...base.action,
          resource: 'function-record',
          function: functionId,
          inputs: p.declaration.spec.inputs,
          edits: [],
          condition: { op: 'lt' as const, args: [{ parameter: 'value' }, { literal: 200 }] },
        };
        p.actions.set('function-record', { service: p.records, action });
        const key = randomUUID();
        const before = Number(await current());
        const result = await p.records.invoke(p.owner.context.session, action, key, {
          mode: 'calculate',
          uid,
          value: 2,
        });
        assert.equal(await current(), before + 2);
        assert.deepEqual(
          await p.records.invoke(p.owner.context.session, action, key, {
            mode: 'calculate',
            uid,
            value: 2,
          }),
          result,
        );
        await assert.rejects(
          p.records.invoke(p.owner.context.session, action, randomUUID(), {
            mode: 'calculate',
            uid,
            value: 999,
          }),
          { code: 'ACTION_CONDITION_FAILED' },
        );
        assert.equal(await current(), before + 2);
        const called = await invoke('function-action', 3);
        assert.equal(called.status, 'succeeded', JSON.stringify(called));
        assert.equal(await current(), before + 5);
        const prepared = (
          await p.pool.query(
            'SELECT proposal_commit FROM moria.function_invocations WHERE prepare_only AND proposal_commit IS NOT NULL',
          )
        ).rows;
        assert.equal(prepared.length, 2);
        await effect('editProposal');
        const batch = await invoke('function-action', 4);
        assert.equal(batch.status, 'succeeded', JSON.stringify(batch));
        assert.equal(await current(), before + 9);
        const rollback = await invoke('function-action-rollback', 5);
        assert.equal(rollback.status, 'failed');
        assert.equal(await current(), before + 9);
      },
    );
    await t.test(
      'data-dependent API errors retain markings and current read permissions',
      async () => {
        await effect('readWrite');
        const missing = await p.catalog.resolveIdentity('negative-function-read', 'missing');
        const failedRead = await invoke('read-error', 0, missing);
        assert.deepEqual(failedRead.result, { value: 1 });
        assert(failedRead.markings.some((label) => label.marking === 'protected'));
        p.configureRecords((binding) => {
          binding.policies.markings.push('new-classification');
        });
        await assert.rejects(p.functions.get(p.owner.context, failedRead.id), {
          code: 'FORBIDDEN',
        });
        p.configureRecords((binding) => {
          binding.policies.markings.pop();
        });
        const rejectedWrite = await invoke('tainted-error', 0, missing);
        assert.deepEqual(rejectedWrite.result, { value: 403 });
        await p.authority.setGrants(
          p.environment,
          'operator',
          'author',
          p.application,
          p.operations.filter((operation) => operation !== 'records:read'),
        );
        await assert.rejects(p.functions.get(p.owner.context, failedRead.id), {
          code: 'FORBIDDEN',
        });
        await p.authority.setGrants(
          p.environment,
          'operator',
          'author',
          p.application,
          p.operations,
        );
        const state = await p.authority.current(p.environment, 'author', p.application);
        const revision = (
          await p.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [p.environment])
        ).rows[0].revision;
        await p.objectStore.activate(
          p.environment,
          'records',
          p.records.binding.schema,
          [
            { uid, value: await current() },
            { uid: missing, value: 100 },
          ],
          state.epoch,
          revision,
        );
        await assert.rejects(p.functions.get(p.owner.context, failedRead.id), {
          code: 'FUNCTION_INPUT_CHANGED',
        });
      },
    );
  },
);

test(
  'nested functions retain caller scope, bound recursion and cancel descendants',
  { timeout: 120000 },
  async (t) => {
    let rootId = '',
      recordUid = '';
    const targets: string[] = [];
    const worker: FunctionWorker = {
      remove: async () => {},
      execute: async (_id, _artifact, _limits, start: any, rpc, log, signal) => {
        let sequence = 0;
        const call = async (target: string, input: unknown, wait = true) => {
          const response: any = await rpc({
            type: 'http',
            id: String(++sequence),
            path: '/api/v1/functions/' + target + '/invoke',
            method: 'POST',
            headers: { 'content-type': 'application/json', 'idempotency-key': 'child' },
            body: Buffer.from(JSON.stringify({ input, wait })).toString('base64'),
          });
          return {
            status: response.status,
            value: JSON.parse(Buffer.from(response.body, 'base64').toString('utf8')),
          };
        };
        const { mode, value, uid } = start.input;
        if (mode === 'leaf') return { value: value + 1 };
        if (mode === 'leaf-read') {
          const result: any = await rpc({
            path: '/api/v1/objects/records/' + recordUid + '?properties=value',
            method: 'GET',
            input: null,
          });
          return { value: result.object.value };
        }
        if (mode === 'leaf-hold') {
          await log('child holding');
          await new Promise((_, reject) => {
            if (signal.aborted) reject(signal.reason);
            else signal.addEventListener('abort', () => reject(signal.reason), { once: true });
          });
        }
        if (mode === 'cycle') {
          const result = await call(rootId, { mode: 'leaf', value: 0 });
          assert.equal(result.value.error.code, 'FUNCTION_RECURSION');
          return { value: result.status };
        }
        if (mode === 'chain') {
          const result = await call(targets[value], { mode: 'chain', value: value + 1 });
          if (result.status === 409) {
            assert.equal(result.value.error.code, 'FUNCTION_RECURSION');
            return { value };
          }
          assert.equal(result.status, 200, JSON.stringify(result));
          return result.value.result;
        }
        const childMode =
          mode === 'nested-read'
            ? 'leaf-read'
            : mode === 'nested-hold' || mode === 'async' || mode === 'async-fail'
              ? 'leaf-hold'
              : 'leaf';
        let childValue = value;
        if (mode === 'parent-read') {
          const result: any = await rpc({
            path: '/api/v1/objects/records/' + recordUid + '?properties=value',
            method: 'GET',
            input: null,
          });
          childValue = result.object.value;
        }
        const child = await call(
          uid,
          { mode: childMode, value: childValue },
          !mode.startsWith('async'),
        );
        if (mode === 'async-fail') throw new Error('parent failed after acceptance');
        assert.equal(child.status, mode === 'async' ? 202 : 200, JSON.stringify(child));
        if (mode === 'nested') {
          const response: any = await rpc({
            type: 'http',
            id: String(++sequence),
            path: `/api/v1/invocations/${child.value.id}/events?after=0`,
            method: 'GET',
            headers: {},
            body: null,
          });
          assert.equal(response.status, 200);
          const events = JSON.parse(Buffer.from(response.body, 'base64').toString('utf8')).events;
          assert.equal(events.at(-1).message, 'succeeded');
        }
        return { value: mode === 'async' ? 202 : child.value.result.value };
      },
    };
    const p = await functionPlatform(
      { uri: 'oci://fixture/nested', digest: 'sha256:' + 'e'.repeat(64) },
      worker,
    );
    t.after(p.close);
    rootId = p.resourceId;
    const publication = await p.runtime.source.publish(
      p.owner.context,
      p.root.id,
      (await p.runtime.source.checkout(p.owner.context, p.root.id)).digest,
      [
        p.declaration,
        ...Array.from({ length: 4 }, (_, i) => ({
          ...p.declaration,
          metadata: { ...p.declaration.metadata, name: 'child' + i },
        })),
      ],
    );
    await p.runtime.source.apply(
      p.owner.context,
      (await p.runtime.source.plan(p.owner.context, p.root.id, publication.digest)).id,
    );
    targets.push(
      ...Array.from(
        { length: 4 },
        (_, i) =>
          publication.resources.find(
            (resource) => resource.reference === 'logic/Function/child' + i,
          )!.id,
      ),
    );
    const invoke = async (mode: string, value = 2) => {
      const result = await p.functions.invoke(p.owner.context, rootId, randomUUID(), {
        mode,
        value,
        uid: targets[0],
      });
      return p.functions.wait(p.owner.context, result.id);
    };
    const until = async (check: () => Promise<boolean>) => {
      for (let i = 0; i < 500; i++) {
        if (await check()) return;
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
      assert.fail('Nested condition did not settle');
    };
    await t.test(
      'read-only composition succeeds and cycles/depth fail explicitly without deadlock',
      async () => {
        const result = await invoke('nested');
        assert.equal(result.status, 'succeeded', JSON.stringify(result));
        assert.deepEqual(result.result, { value: 3 });
        assert.deepEqual((await invoke('cycle')).result, { value: 409 });
        assert.deepEqual((await invoke('chain', 0)).result, { value: 3 });
      },
    );
    await t.test(
      'acceptance checks the current child effect and cancellation checks the live parent fence',
      async () => {
        const parent = await p.functions.invoke(p.owner.context, rootId, randomUUID(), {
          mode: 'leaf-hold',
          value: 0,
        });
        const target = await p.functions.invoke(p.owner.context, targets[1], randomUUID(), {
          mode: 'leaf-hold',
          value: 0,
        });
        await until(async () =>
          (await p.functions.events(p.owner.context, parent.id)).some(
            (event) => event.message === 'child holding',
          ),
        );
        const fence = (
          await p.pool.query('SELECT fence FROM moria.function_invocations WHERE id=$1', [
            parent.id,
          ])
        ).rows[0].fence;
        const caller = {
          ...p.owner.context,
          session: { ...p.owner.context.session, execution: { id: parent.id, fence } },
        };
        try {
          const changed = await p.runtime.source.publish(
            p.owner.context,
            p.root.id,
            (await p.runtime.source.checkout(p.owner.context, p.root.id)).digest,
            [
              p.declaration,
              ...Array.from({ length: 4 }, (_, i) => ({
                ...p.declaration,
                metadata: { ...p.declaration.metadata, name: 'child' + i },
                spec: { ...p.declaration.spec, effect: i === 0 ? 'readWrite' : 'readOnly' },
              })),
            ],
          );
          await p.runtime.source.apply(
            p.owner.context,
            (await p.runtime.source.plan(p.owner.context, p.root.id, changed.digest)).id,
          );
          await assert.rejects(
            p.functions.invoke(caller, targets[0], randomUUID(), { mode: 'leaf', value: 0 }),
            { code: 'FUNCTION_EFFECT' },
          );
          await assert.rejects(p.functions.cancel(caller, target.id), { code: 'FUNCTION_EFFECT' });
          await p.functions.cancel(p.owner.context, parent.id);
          await assert.rejects(p.functions.cancel(caller, target.id), {
            code: 'FUNCTION_WORKER_LOST',
          });
          assert(
            ['accepted', 'running'].includes(
              (await p.functions.get(p.owner.context, target.id)).status,
            ),
          );
        } finally {
          await p.functions.cancel(p.owner.context, parent.id);
          await p.functions.cancel(p.owner.context, target.id);
          await p.runtime.source.apply(
            p.owner.context,
            (await p.runtime.source.plan(p.owner.context, p.root.id, publication.digest)).id,
          );
        }
      },
    );
    await t.test('parent results reauthorize the protected reads consumed by a child', async () => {
      recordUid = await p.catalog.resolveIdentity('nested-records', 'record');
      await p.objectStore.activate(
        p.environment,
        'records',
        p.records.binding.schema,
        [{ uid: recordUid, value: 77 }],
        (await p.authority.current(p.environment, 'author', p.application)).epoch,
        (await p.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [p.environment]))
          .rows[0].revision,
      );
      p.configureRecords((binding) => {
        binding.policies.markings = ['protected'];
      });
      const result = await invoke('nested-read');
      assert.deepEqual(result.result, { value: 77 });
      assert(result.markings.some((label) => label.marking === 'protected'));
      await p.authority.setGrants(
        p.environment,
        'operator',
        'author',
        p.application,
        p.operations.filter((operation) => operation !== 'records:read'),
      );
      await assert.rejects(p.functions.get(p.owner.context, result.id), { code: 'FORBIDDEN' });
      await p.authority.setGrants(p.environment, 'operator', 'author', p.application, p.operations);
    });
    await t.test('asynchronous acceptance stays inspectable as the child progresses', async () => {
      const result = await invoke('async');
      assert.equal(result.status, 'succeeded', JSON.stringify(result));
      const child = (
        await p.pool.query('SELECT id FROM moria.function_invocations WHERE parent_id=$1', [
          result.id,
        ])
      ).rows[0];
      await until(
        async () => (await p.functions.get(p.owner.context, child.id)).status === 'running',
      );
      await p.functions.cancel(p.owner.context, child.id);
      assert.deepEqual((await p.functions.get(p.owner.context, result.id)).result, { value: 202 });
    });
    await t.test(
      'children inherit the read evidence and markings of parent-derived inputs',
      async () => {
        const result = await invoke('parent-read');
        assert.deepEqual(result.result, { value: 78 });
        const child = (
          await p.pool.query(
            'SELECT id,markings FROM moria.function_invocations WHERE parent_id=$1',
            [result.id],
          )
        ).rows[0];
        assert(child.markings.some((label: { marking: string }) => label.marking === 'protected'));
        await p.authority.setGrants(
          p.environment,
          'operator',
          'author',
          p.application,
          p.operations.filter((operation) => operation !== 'records:read'),
        );
        await assert.rejects(p.functions.get(p.owner.context, child.id), { code: 'FORBIDDEN' });
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
      'parent cancellation and failure stop accepted or running descendants',
      async () => {
        const accepted = await p.functions.invoke(p.owner.context, rootId, randomUUID(), {
          mode: 'nested-hold',
          value: 0,
          uid: targets[0],
        });
        await until(
          async () =>
            (
              await p.pool.query(
                "SELECT 1 FROM moria.function_invocations WHERE parent_id=$1 AND status='running'",
                [accepted.id],
              )
            ).rowCount === 1,
        );
        await p.functions.cancel(p.owner.context, accepted.id);
        const child = (
          await p.pool.query(
            'SELECT id,status FROM moria.function_invocations WHERE parent_id=$1',
            [accepted.id],
          )
        ).rows[0];
        assert.equal(child.status, 'cancelled');
        const failed = await invoke('async-fail');
        assert.equal(failed.status, 'failed');
        await until(
          async () =>
            (
              await p.pool.query(
                "SELECT 1 FROM moria.function_invocations WHERE parent_id=$1 AND status='cancelled'",
                [failed.id],
              )
            ).rowCount === 1,
        );
      },
    );
  },
);
