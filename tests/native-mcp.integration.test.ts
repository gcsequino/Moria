import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { mcpCommand } from '../apps/cli/src/mcp.js';
import { fingerprint } from '../packages/compiler/src/index.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { ObjectStore, type ObjectSchema } from '../services/ontology/src/store.js';
import { database } from './fixtures/catalog-storage/database.js';
import { assertNativeArtifacts } from './fixtures/native-common/artifact.js';

const version = '2026-07-28';
test(
  'native MCP preserves current authority, typed results and opaque discovery across replicas',
  { timeout: 180000 },
  async (t) => {
    const runtime = process.env.MORIA_NATIVE_BINARY ?? '/usr/local/bin/moria-runtime';
    await assertNativeArtifacts(runtime, []);
    const db = await database(),
      environment = randomUUID(),
      application = 'mcp-fixture',
      principal = 'caller';
    const children: ChildProcess[] = [],
      exits: Promise<unknown>[] = [];
    t.after(async () => {
      for (const child of children) child.kill('SIGKILL');
      await Promise.allSettled(exits);
      await db.close();
    });
    await db.catalog.ensureEnvironment(environment);
    const authority = new Authority(db.catalog),
      store = new ObjectStore(db.catalog);
    const privateResource = '00000000-0000-4000-8000-000000000001',
      objects = [privateResource, ...Array.from({ length: 102 }, () => randomUUID())].sort(),
      target = objects[1],
      action = randomUUID();
    const operations = [...objects.map((id) => `${id}:read`), `${action}:invoke`];
    await authority.syncPrincipal(environment, 'fixture', principal, {
      kind: 'workload',
      enabled: true,
      attributes: { region: 'West', admin: false },
      markings: [],
    });
    await authority.setApplication(environment, 'fixture', application, operations);
    await authority.setGrants(environment, 'fixture', principal, application, operations);
    await db.pool.query(
      'INSERT INTO moria.resource_data_labels(environment,resource,marking) VALUES($1,$2,$3)',
      [environment, privateResource, 'private'],
    );
    await authority.syncPrincipal(environment, 'fixture', 'other-caller', {
      kind: 'workload',
      enabled: true,
      attributes: { region: 'West', admin: false },
      markings: [],
    });
    await authority.setGrants(environment, 'fixture', 'other-caller', application, operations);
    const sessions = new WorkloadSessions(authority),
      issued = await sessions.mint({ environment, principal, application }, 'fixture');
    const otherIssued = await sessions.mint(
      { environment, principal: 'other-caller', application },
      'fixture',
    );
    const properties = {
      uid: { type: 'uid' as const, nullable: false },
      name: { type: 'string' as const, nullable: false },
      region: { type: 'string' as const, nullable: false },
      secret: { type: 'string' as const, nullable: false },
    };
    const schema: ObjectSchema = { primaryKey: 'uid', properties };
    const uid = await db.catalog.resolveIdentity(`mcp-${environment}`, 'visible'),
      hidden = await db.catalog.resolveIdentity(`mcp-${environment}`, 'hidden');
    for (const resource of objects) {
      const definition = {
        objectTypeId: resource,
        schema,
        policies: {
          resource,
          markings: resource === privateResource ? ['private'] : [],
          properties,
          sourceColumns: properties,
          attributes: { region: properties.region, admin: { type: 'boolean', nullable: false } },
          row: { op: 'eq', args: [{ field: 'region' }, { principalAttribute: 'region' }] },
          propertyRead: { secret: { principalAttribute: 'admin' } },
        },
      };
      await store.publishStage(
        await store.stage(
          environment,
          resource,
          schema,
          resource === target
            ? [
                { uid, name: 'visible', region: 'West', secret: 'do-not-release' },
                { uid: hidden, name: 'hidden', region: 'East', secret: 'hidden-secret' },
              ]
            : [],
          (await authority.current(environment, principal, application)).epoch,
          (
            await db.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [
              environment,
            ])
          ).rows[0].revision,
          undefined,
          resource === privateResource ? ['private'] : [],
        ),
      );
      await db.pool.query(
        'INSERT INTO moria.native_serving_bindings(environment,resource,object_type_id,revision,definition) VALUES($1,$2,$5,$3,$4)',
        [environment, resource, fingerprint(definition), definition, resource],
      );
    }
    const actionDefinition = {
      resource: action,
      revision: 'v1',
      markings: [],
      targetParameter: 'uid',
      condition: { literal: true },
      inputs: { uid: properties.uid, name: properties.name },
      edits: [{ op: 'set', property: 'name', value: { parameter: 'name' } }],
    };
    await db.pool.query(
      'INSERT INTO moria.native_serving_actions(environment,resource,object_resource,revision,definition) VALUES($1,$2,$3,$4,$5)',
      [environment, action, target, 'v1', actionDefinition],
    );
    const role = `mcp_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role};GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role};GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    const profile = { environment, application, objects, actions: [action] };
    const start = async (profiles = [profile]) => {
      const child = spawn(runtime, ['serve'], {
        env: {
          PATH: process.env.PATH,
          MORIA_INDEXING_DATABASE_SECRET_REF: 'MCP_DATABASE',
          MCP_DATABASE: `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`,
          MORIA_INDEXING_DATABASE_TLS: 'disable',
          MORIA_NATIVE_MCP_PROFILES: JSON.stringify(profiles),
        },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      children.push(child);
      exits.push(new Promise((resolve) => child.once('exit', resolve)));
      let output = '',
        error = '';
      child.stdout!.on('data', (b) => (output += b));
      child.stderr!.on('data', (b) => (error = (error + b).slice(-65536)));
      for (let i = 0; i < 500; i++) {
        for (const line of output.split('\n')) {
          try {
            const event = JSON.parse(line);
            if (event.event === 'serving') return `http://${event.address}/mcp`;
          } catch {}
        }
        if (child.exitCode !== null) throw new Error(error);
        await delay(10);
      }
      throw new Error(`Startup timeout ${error}`);
    };
    const a = await start(),
      b = await start();
    let sequence = 0;
    const call = async (
      method: string,
      params: any = {},
      options: {
        url?: string;
        token?: string;
        headers?: Record<string, string>;
        body?: unknown;
      } = {},
    ) => {
      const name =
        method === 'tools/call'
          ? params.name
          : method === 'resources/read'
            ? params.uri
            : undefined;
      const response = await fetch(options.url ?? a, {
        method: 'POST',
        headers: {
          authorization: `Bearer ${options.token ?? issued.token}`,
          'content-type': 'application/json',
          accept: 'application/json, text/event-stream',
          'mcp-protocol-version': version,
          'mcp-method': method,
          ...(name ? { 'mcp-name': name } : {}),
          ...options.headers,
        },
        body: JSON.stringify(
          options.body ?? {
            jsonrpc: '2.0',
            id: ++sequence,
            method,
            params: {
              ...params,
              _meta: {
                'io.modelcontextprotocol/protocolVersion': version,
                'io.modelcontextprotocol/clientCapabilities': {},
              },
            },
          },
        ),
      });
      return { status: response.status, value: (await response.json()) as any };
    };
    const ok = async (method: string, params: any = {}, url = a) => {
      const result = await call(method, params, { url });
      assert.equal(result.status, 200, JSON.stringify(result.value));
      assert.equal(result.value.error, undefined, JSON.stringify(result.value));
      return result.value.result;
    };
    await t.test(
      'stateless discovery filters SQL-denied resources without aborting the page',
      async () => {
        assert.deepEqual((await ok('server/discover')).supportedVersions, [version]);
        const first = await ok('tools/list');
        assert.equal(first.tools.length, 50);
        assert.equal(
          first.tools.some((v: any) => v.name === `query_${privateResource}`),
          false,
        );
        assert.ok(first.nextCursor);
        const second = await ok('tools/list', { cursor: first.nextCursor }, b);
        assert.equal(second.tools.length, 50);
        assert.ok(second.nextCursor);
        const third = await ok('tools/list', { cursor: second.nextCursor });
        assert.equal(third.tools.length, 3);
        assert.equal(third.nextCursor, undefined);
        assert.equal(
          new Set([...first.tools, ...second.tools, ...third.tools].map((v) => v.name)).size,
          103,
        );
        assert.equal(
          (
            await db.pool.query(
              'SELECT a.expires_at=b.expires_at AS same FROM moria.runtime_mcp_cursors a,moria.runtime_mcp_cursors b WHERE a.token_hash=$1 AND b.token_hash=$2',
              [
                createHash('sha256').update(first.nextCursor).digest('hex'),
                createHash('sha256').update(second.nextCursor).digest('hex'),
              ],
            )
          ).rows[0].same,
          true,
        );
        assert.equal(
          (await call('tools/list', { cursor: first.nextCursor }, { token: otherIssued.token }))
            .value.error.message,
          'CURSOR_INVALIDATED',
        );
        assert.equal(
          (await call('resources/list', { cursor: first.nextCursor })).value.error.message,
          'CURSOR_INVALIDATED',
        );
        const original = (
          await db.pool.query(
            'SELECT expires_at FROM moria.runtime_mcp_cursors WHERE token_hash=$1',
            [createHash('sha256').update(first.nextCursor).digest('hex')],
          )
        ).rows[0].expires_at;
        assert.ok(original > new Date());
        const modified = await start([{ ...profile, objects: objects.slice(0, -1) }]);
        assert.equal(
          (await call('tools/list', { cursor: first.nextCursor }, { url: modified })).value.error
            .message,
          'CURSOR_INVALIDATED',
        );
        await db.pool.query(
          "UPDATE moria.runtime_mcp_cursors SET expires_at=clock_timestamp()-interval '1 second' WHERE token_hash=$1",
          [createHash('sha256').update(first.nextCursor).digest('hex')],
        );
        assert.equal(
          (await call('tools/list', { cursor: first.nextCursor })).value.error.message,
          'CURSOR_INVALIDATED',
        );
      },
    );
    await t.test('resource descriptions contain authorized typed metadata only', async () => {
      const resources = await ok('resources/list');
      assert.equal(resources.resources.length, 50);
      const resource = await ok('resources/read', { uri: `moria://resource/ObjectType/${target}` });
      const description = JSON.parse(resource.contents[0].text);
      assert.equal(description.kind, 'ObjectType');
      assert.deepEqual(description.shape.schema, schema);
      assert.equal(description.policies, undefined);
      assert.equal(
        (await call('resources/read', { uri: `moria://resource/ObjectType/${privateResource}` }))
          .value.error.message,
        'FORBIDDEN',
      );
      const tools = await ok('tools/list');
      const actionTool = tools.tools.find((v: any) => v.name === `action_${action}`);
      assert.equal(actionTool.inputSchema.properties.input.properties.uid.type, 'string');
      assert.equal(actionTool.inputSchema.additionalProperties, false);
    });
    await t.test(
      'queries and actions use protected serving and replica-stable receipts',
      async () => {
        const query = () => ok('tools/call', { name: `query_${target}`, arguments: { query: {} } });
        const result = await query();
        assert.equal(result.isError, false);
        assert.equal(result.structuredContent.objects.length, 1);
        assert.equal(result.structuredContent.objects[0].name, 'visible');
        assert.equal(Object.hasOwn(result.structuredContent.objects[0], 'secret'), false);
        const forbidden = await ok('tools/call', {
          name: `query_${target}`,
          arguments: { query: { projection: ['secret'] } },
        });
        assert.equal(forbidden.isError, true);
        assert.equal(forbidden.content[0].text, 'PROPERTY_FORBIDDEN');
        const args = {
          name: `action_${action}`,
          arguments: { idempotencyKey: 'stable-mcp-edit', input: { uid, name: 'edited' } },
        };
        const first = await ok('tools/call', args),
          replay = await ok('tools/call', args, b);
        assert.equal(first.isError, false, JSON.stringify(first));
        assert.deepEqual(replay.structuredContent, first.structuredContent);
        const conflict = await ok('tools/call', {
          ...args,
          arguments: { ...args.arguments, input: { uid, name: 'different' } },
        });
        assert.equal(conflict.isError, true);
        assert.equal(conflict.content[0].text, 'IDEMPOTENCY_CONFLICT');
        assert.equal((await query()).structuredContent.objects[0].name, 'edited');
        const hiddenEdit = await ok('tools/call', {
          name: `action_${action}`,
          arguments: { idempotencyKey: 'hidden-edit', input: { uid: hidden, name: 'exposed' } },
        });
        assert.equal(hiddenEdit.isError, true);
        assert.equal(
          (await call('tools/call', { name: 'sql', arguments: { query: 'SELECT *' } })).status,
          400,
        );
      },
    );
    await t.test('official Python SDK negotiates and reads protected resources', async () => {
      const child = spawn('python3', ['tests/fixtures/native-common/mcp_client.py'], {
        env: {
          PATH: process.env.PATH,
          PYTHONPATH: '/opt/moria-mcp-client',
          PYTHONDONTWRITEBYTECODE: '1',
        },
        stdio: ['pipe', 'pipe', 'pipe'],
      });
      children.push(child);
      const done = new Promise<number | null>((resolve) => child.once('exit', resolve));
      exits.push(done);
      let output = '',
        error = '';
      child.stdout!.on('data', (b) => (output += b));
      child.stderr!.on('data', (b) => (error = (error + b).slice(-65536)));
      child.stdin!.end(JSON.stringify({ url: a, token: issued.token, target }));
      assert.equal(await done, 0, error);
      assert.deepEqual(JSON.parse(output), {
        protocolVersion: version,
        tools: 103,
        protectedRows: 1,
      });
    });
    await t.test(
      'CLI emits a credential reference and calls the dedicated native endpoint',
      async () => {
        const url = new URL(a).origin,
          prior = process.env.MORIA_API_TOKEN;
        try {
          process.env.MORIA_API_TOKEN = issued.token;
          const config = await mcpCommand(['config'], { url });
          assert.deepEqual(config, {
            transport: 'streamable-http',
            url: a,
            protocolVersion: version,
            credentialEnv: 'MORIA_API_TOKEN',
          });
          assert.equal(JSON.stringify(config).includes(issued.token), false);
          assert.deepEqual((await mcpCommand(['discover'], { url })).supportedVersions, [version]);
          assert.equal((await mcpCommand(['tools'], { url })).tools.length, 50);
          await assert.rejects(mcpCommand(['config'], { url: 'http://remote.invalid' }), {
            code: 'USAGE',
          });
          delete process.env.MORIA_API_TOKEN;
          await assert.rejects(mcpCommand(['discover'], { url }), { code: 'USAGE' });
        } finally {
          if (prior === undefined) delete process.env.MORIA_API_TOKEN;
          else process.env.MORIA_API_TOKEN = prior;
        }
      },
    );
    await t.test(
      'transport rejects mirrored-header tampering, batches and credential substitution',
      async () => {
        assert.equal(
          (await call('server/discover', {}, { headers: { 'mcp-method': 'tools/call' } })).value
            .error.code,
          -32020,
        );
        assert.equal(
          (await call('server/discover', {}, { headers: { 'mcp-protocol-version': '2025-11-25' } }))
            .value.error.code,
          -32020,
        );
        assert.equal(
          (
            await call(
              'tools/call',
              { name: `query_${target}`, arguments: { query: {} } },
              { headers: { 'mcp-name': 'query_other' } },
            )
          ).value.error.code,
          -32020,
        );
        assert.equal((await call('server/discover', {}, { body: [] })).value.error.code, -32600);
        assert.equal(
          (
            await call(
              'server/discover',
              {},
              { body: { jsonrpc: '2.0', method: 'server/discover', params: {} } },
            )
          ).value.error.code,
          -32600,
        );
        assert.equal((await call('initialize')).value.error.code, -32601);
        assert.equal(
          (await call('server/discover', {}, { headers: { 'x-moria-key': 'global-key' } })).status,
          403,
        );
        assert.equal(
          (await fetch(a, { headers: { authorization: `Bearer ${issued.token}` } })).status,
          405,
        );
        assert.equal(
          (await call('server/discover', { actor: { principal: 'admin' } })).value.error.message,
          'INPUT_INVALID',
        );
      },
    );
    await t.test(
      'epoch changes invalidate cursors and revocation applies to every request',
      async () => {
        const cursor = (await ok('tools/list')).nextCursor;
        await authority.setGrants(
          environment,
          'fixture',
          principal,
          application,
          operations.filter((v) => v !== `${action}:invoke`),
        );
        assert.equal(
          (await call('tools/list', { cursor })).value.error.message,
          'CURSOR_INVALIDATED',
        );
        const denied = await ok('tools/call', {
          name: `action_${action}`,
          arguments: { idempotencyKey: 'denied', input: { uid, name: 'denied' } },
        });
        assert.equal(denied.isError, true);
        assert.equal(denied.content[0].text, 'FORBIDDEN');
        await authority.syncPrincipal(environment, 'fixture', principal, {
          kind: 'workload',
          enabled: false,
          attributes: { region: 'West', admin: false },
          markings: [],
        });
        assert.equal((await call('server/discover')).status, 401);
        await authority.syncPrincipal(environment, 'fixture', principal, {
          kind: 'workload',
          enabled: true,
          attributes: { region: 'West', admin: false },
          markings: [],
        });
        assert.equal((await call('server/discover')).status, 401);
      },
    );
  },
);
