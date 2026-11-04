import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { fingerprint } from '../packages/compiler/src/index.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { ObjectStore, type ObjectSchema } from '../services/ontology/src/store.js';
import { ObjectService } from '../services/ontology/src/service.js';
import { createLocalFilesystem } from '../services/filesystem/src/local.js';
import { database } from './fixtures/catalog-storage/database.js';
import { assertNativeArtifacts } from './fixtures/native-common/artifact.js';

test(
  'native serving replicas preserve protected reads, cursors and atomic action outcomes',
  { timeout: 180000 },
  async (t) => {
    await assertNativeArtifacts(
      process.env.MORIA_RUNTIME_BINARY ??
        process.env.MORIA_INDEXING_BINARY ??
        '/usr/local/bin/moria-indexing',
      [],
    );
    const db = await database();
    const processes: ChildProcess[] = [];
    const exits: Promise<unknown>[] = [];
    t.after(async () => {
      for (const process of processes) process.kill('SIGKILL');
      await Promise.allSettled(exits);
      await db.close();
    });
    const environment: string = randomUUID(),
      objectType = randomUUID(),
      application = 'native-app',
      principal = 'pipeline';
    await db.catalog.ensureEnvironment(environment);
    const authority = new Authority(db.catalog),
      sessions = new WorkloadSessions(authority),
      store = new ObjectStore(db.catalog);
    const operations = [
      'objects:read',
      'relations:read',
      'forbidden-relations:read',
      'edit:invoke',
      'clear:invoke',
      'group:invoke',
      'delete:invoke',
      'restore:invoke',
      'reset:invoke',
      'typed:invoke',
      'auxiliary:read',
      'filesystem:read',
      'filesystem:list',
      'filesystem:create',
      'filesystem:write',
      'instance:manage',
    ];
    await authority.syncPrincipal(environment, 'fixture', principal, {
      kind: 'workload',
      enabled: true,
      markings: ['private'],
      attributes: { region: 'West', admin: false },
    });
    await authority.setApplication(environment, 'fixture', application, operations);
    await authority.setGrants(environment, 'fixture', principal, application, operations);
    const issued = await sessions.mint({ environment, principal, application }, 'fixture');
    const session = await sessions.authenticate(`Bearer ${issued.token}`);
    const properties = {
      uid: { type: 'uid' as const, nullable: false },
      region: { type: 'string' as const, nullable: false },
      name: { type: 'string' as const, nullable: false },
      secret: { type: 'string' as const, nullable: false },
      rank: { type: 'int64' as const, nullable: false },
      amount: { type: 'decimal' as const, nullable: false, precision: 20, scale: 2 },
      label: { type: 'string' as const, nullable: true },
      parent: { type: 'uid' as const, nullable: true },
      secretParent: { type: 'uid' as const, nullable: true },
    };
    const schema: ObjectSchema = { primaryKey: 'uid', properties };
    const definition = {
      objectTypeId: objectType,
      schema,
      policies: {
        resource: 'objects',
        markings: ['private'],
        properties,
        sourceColumns: properties,
        attributes: {
          region: properties.region,
          admin: { type: 'boolean' as const, nullable: false },
        },
        row: { op: 'eq', args: [{ field: 'region' }, { principalAttribute: 'region' }] },
        propertyRead: {
          secret: { principalAttribute: 'admin' },
          secretParent: { principalAttribute: 'admin' },
        },
      },
    };
    const service = new ObjectService(store, authority, definition);
    const rows = await Promise.all(
      Array.from({ length: 12 }, async (_, index) => ({
        uid: await db.catalog.resolveIdentity(`native-serving-${environment}`, String(index)),
        name: index === 7 ? 'blocked' : `source-${index}`,
        region: index < 8 ? 'West' : 'East',
        secret: `secret-${index}`,
        rank: String(Math.floor(index / 2)),
        amount: `${index + 1}.25`,
        label: index % 2 === 0 ? null : `label-${index}`,
        parent: null as string | null,
        secretParent: null as string | null,
      })),
    );
    for (let index = 0; index < 4; index++) rows[index].parent = rows[4].uid;
    const source = async (resource = 'objects', data = rows) =>
      store.publishStage(
        await store.stage(
          environment,
          resource,
          schema,
          data,
          (await authority.current(environment, principal, application)).epoch,
          (
            await db.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [
              environment,
            ])
          ).rows[0].revision,
          undefined,
          ['private'],
        ),
      );
    await source();
    const register = async (resource: string, value: unknown, id: string) =>
      db.pool.query(
        'INSERT INTO moria.native_serving_bindings(environment,resource,object_type_id,revision,definition) VALUES($1,$2,$3,$4,$5)',
        [environment, resource, id, fingerprint(value), value],
      );
    await register('objects', definition, objectType);
    for (const [resource, property] of [
      ['relations', 'parent'],
      ['forbidden-relations', 'secretParent'],
    ])
      await db.pool.query(
        'INSERT INTO moria.native_serving_links(environment,resource,revision,source_resource,target_resource,definition) VALUES($1,$2,$3,$4,$4,$5)',
        [
          environment,
          resource,
          'v1',
          'objects',
          { mode: 'uidReference', sourceProperty: property, markings: ['private'] },
        ],
      );
    const action = async (resource: string, edits: unknown[], group = false) => {
      const definition = {
        resource,
        revision: 'v1',
        markings: ['private'],
        targetParameter: group ? 'uids' : 'uid',
        inputs: group
          ? {
              uids: { type: 'array', nullable: false, items: properties.uid },
              name: properties.name,
            }
          : resource === 'edit'
            ? { uid: properties.uid, name: properties.name }
            : { uid: properties.uid },
        condition:
          resource === 'reset'
            ? { literal: true }
            : { op: 'ne', args: [{ field: 'name' }, { literal: 'blocked' }] },
        edits,
      };
      await db.pool.query(
        'INSERT INTO moria.native_serving_actions(environment,resource,object_resource,revision,definition) VALUES($1,$2,$3,$4,$5)',
        [environment, resource, 'objects', 'v1', definition],
      );
    };
    await action('edit', [{ op: 'set', property: 'name', value: { parameter: 'name' } }]);
    await action('clear', [{ op: 'clearOverride', property: 'name' }]);
    await action('group', [{ op: 'set', property: 'name', value: { parameter: 'name' } }], true);
    await action('delete', [{ op: 'delete' }]);
    await action('restore', [{ op: 'restore' }]);
    await action('reset', [{ op: 'clearOverride', property: 'name' }]);
    const role = `native_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(`ALTER ROLE ${role} SET temp_file_limit='256MB'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role}; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    for (const kernel of [
      'native_serving_filesystem_resource_access_state',
      'native_serving_resource_access_state',
    ]) {
      const client = await db.pool.connect();
      try {
        await client.query('BEGIN');
        await client.query(`SET LOCAL ROLE ${role}`);
        await assert.rejects(client.query(`SELECT moria.${kernel}('{}'::jsonb,'forged','read')`), {
          code: '42501',
        });
      } finally {
        await client.query('ROLLBACK');
        client.release();
      }
    }
    const connection = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const binary =
      process.env.MORIA_RUNTIME_BINARY ??
      process.env.MORIA_INDEXING_BINARY ??
      '/usr/local/bin/moria-indexing';
    const start = async () => {
      const process = spawn(binary, ['serve'], {
        env: {
          PATH: '/usr/local/bin:/usr/bin:/bin',
          MORIA_INDEXING_DATABASE_SECRET_REF: 'NATIVE_SERVING_DATABASE',
          NATIVE_SERVING_DATABASE: connection,
          MORIA_INDEXING_DATABASE_TLS: 'disable',
          MORIA_NATIVE_LISTEN: '127.0.0.1:0',
        },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      processes.push(process);
      let stdout = '',
        stderr = '';
      process.stdout!.on('data', (data) => {
        stdout += data.toString();
      });
      process.stderr!.on('data', (data) => {
        stderr += data.toString();
      });
      const done = new Promise<void>((resolve) => process.once('exit', () => resolve()));
      exits.push(done);
      for (let attempt = 0; attempt < 500; attempt++) {
        for (const line of stdout.split('\n')) {
          try {
            const item = JSON.parse(line);
            if (item.event === 'serving') return { process, url: `http://${item.address}`, done };
          } catch {}
        }
        if (process.exitCode !== null) throw new Error(`Native serving exited: ${stderr}`);
        await delay(10);
      }
      throw new Error(`Native serving startup timeout: ${stderr}`);
    };
    const a = await start(),
      b = await start();
    const headers = (token = issued.token) => ({
      authorization: `Bearer ${token}`,
      'content-type': 'application/json',
    });
    const read = (base: string, uid: string, token = issued.token, reference = objectType) =>
      fetch(`${base}/api/v1/objects/${reference}/${uid}`, { headers: headers(token) });
    const query = (base: string, input: unknown, token = issued.token) =>
      fetch(`${base}/api/v1/objects/${objectType}/query`, {
        method: 'POST',
        headers: headers(token),
        body: JSON.stringify(input),
      });
    const invoke = (base: string, action: string, key: string, input: unknown) =>
      fetch(`${base}/api/v1/actions/${action}/invoke`, {
        method: 'POST',
        headers: { ...headers(), 'idempotency-key': key },
        body: JSON.stringify({ input }),
      });
    const aggregate = (base: string, input: unknown) =>
      fetch(`${base}/api/v1/objects/${objectType}/aggregate`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify(input),
      });
    const traverse = (base: string, input: unknown, reference = 'relations') =>
      fetch(`${base}/api/v1/links/${reference}/traverse`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify(input),
      });
    const json = async (response: Response, status = 200) => {
      const body = await response.json();
      assert.equal(response.status, status, JSON.stringify(body));
      return body;
    };
    const waitForLock = async (prefix: string) => {
      for (let attempt = 0; attempt < 500; attempt++) {
        const waiting = (
          await db.pool.query(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE usename=$1 AND wait_event_type='Lock' AND query LIKE $2) AS waiting",
            [role, prefix],
          )
        ).rows[0].waiting;
        if (waiting) return;
        await delay(10);
      }
      assert.fail('native operation must reach the real SQL lock barrier');
    };

    await t.test(
      'typed identities, canonical restrictions and forbidden projections are enforced before data release',
      async () => {
        const item = await json(await read(a.url, rows[0].uid));
        assert.equal(item.object.name, rows[0].name);
        assert.equal(Object.hasOwn(item.object, 'secret'), false);
        await json(await read(b.url, rows[8].uid), 404);
        await json(await read(b.url, randomUUID()), 404);
        const secret = await json(
          await query(a.url, {
            filter: { op: 'eq', args: [{ field: 'secret' }, { literal: 'anything' }] },
          }),
          403,
        );
        assert.equal(secret.error.code, 'PROPERTY_FORBIDDEN');
        await json(await query(a.url, { projection: ['unknown'] }), 400);
        assert.equal(
          (await json(await query(a.url, { sort: [{ property: 'name', direction: 'asc' }] })))
            .objects.length,
          8,
        );
        await json(
          await fetch(`${a.url}/api/v1/objects/${objectType}/${rows[0].uid}`, {
            headers: { ...headers(), origin: a.url },
          }),
          403,
        );
        await json(await query(a.url, { projection: null }), 400);
        await json(
          await fetch(`${a.url}/api/v1/objects/${objectType}/query`, {
            method: 'POST',
            headers: headers(),
            body: ' '.repeat(65537),
          }),
          413,
        );
        await json(await read(a.url, rows[0].uid, 'x'.repeat(43)), 401);
        const hiddenUid = {
          ...definition,
          policies: {
            ...definition.policies,
            propertyRead: { ...definition.policies.propertyRead, uid: { literal: false } },
          },
        };
        await db.pool.query(
          'UPDATE moria.native_serving_bindings SET definition=$2,revision=$3 WHERE environment=$1 AND resource=$4',
          [environment, hiddenUid, fingerprint(hiddenUid), 'objects'],
        );
        await json(
          await fetch(`${a.url}/api/v1/objects/${objectType}/${rows[0].uid}?properties=`, {
            headers: headers(),
          }),
          403,
        );
        await db.pool.query(
          'UPDATE moria.native_serving_bindings SET definition=$2,revision=$3 WHERE environment=$1 AND resource=$4',
          [environment, definition, fingerprint(definition), 'objects'],
        );
      },
    );

    await t.test(
      'global engine fallback preserves complete typed ordering and pinned pagination without a layout',
      async () => {
        const input = {
          sort: [
            { property: 'rank', direction: 'desc' },
            { property: 'label', direction: 'asc' },
          ],
          pageSize: 3,
          projection: ['uid', 'rank', 'label'],
        };
        const expected = rows
          .slice(0, 8)
          .sort(
            (a, b) =>
              Number(b.rank) - Number(a.rank) ||
              (a.label === null
                ? b.label === null
                  ? 0
                  : 1
                : b.label === null
                  ? -1
                  : Buffer.compare(Buffer.from(a.label), Buffer.from(b.label))) ||
              a.uid.localeCompare(b.uid),
          );
        const result: { uid: string }[] = [];
        let cursor: string | undefined;
        do {
          const page = await json(
            await query(result.length ? b.url : a.url, { ...input, ...(cursor ? { cursor } : {}) }),
          );
          result.push(...page.objects);
          cursor = page.nextCursor;
        } while (cursor);
        assert.deepEqual(
          result.map((row) => row.uid),
          expected.map((row) => row.uid),
        );
        await json(await query(a.url, { sort: [{ property: 'secret', direction: 'asc' }] }), 403);
        const filtered = await json(
          await query(a.url, {
            ...input,
            pageSize: 20,
            filter: { op: 'gte', args: [{ field: 'rank' }, { literal: '2' }] },
          }),
        );
        assert.deepEqual(
          filtered.objects.map((row: { uid: string }) => row.uid),
          expected.filter((row) => Number(row.rank) >= 2).map((row) => row.uid),
        );
      },
    );

    const scope = {
      fromUid: '00000000-0000-0000-0000-000000000000',
      throughUid: 'ffffffff-ffff-ffff-ffff-ffffffffffff',
    };
    const budget = { scanRows: 8, responseBytes: 1048576, timeMs: 5000 };
    await t.test(
      'bounded typed sorts pin key tuples, authorize keys, and reject complete-scope overflow',
      async () => {
        const sort = [{ property: 'rank', direction: 'desc' }];
        const input = { sort, scope, budget, pageSize: 3, projection: ['uid', 'rank', 'label'] };
        const first = await json(await query(a.url, input));
        const [left, right] = await Promise.all([
          query(a.url, { ...input, cursor: first.nextCursor }).then((response) => json(response)),
          query(b.url, { ...input, cursor: first.nextCursor }).then((response) => json(response)),
        ]);
        assert.deepEqual(left.objects, right.objects);
        const third = await json(await query(b.url, { ...input, cursor: left.nextCursor }));
        const expected = rows
          .slice(0, 8)
          .sort((a, b) => Number(b.rank) - Number(a.rank) || a.uid.localeCompare(b.uid));
        assert.deepEqual(
          [...first.objects, ...left.objects, ...third.objects].map((item) => item.uid),
          expected.map((item) => item.uid),
        );
        const position = (
          await db.pool.query(
            "SELECT position FROM moria.read_cursors WHERE environment=$1 AND jsonb_typeof(position)='object' LIMIT 1",
            [environment],
          )
        ).rows[0].position;
        assert.ok(typeof position.uid === 'string' && Array.isArray(position.keys));
        assert.equal(
          (await json(await query(a.url, { ...input, budget: { ...budget, scanRows: 7 } }), 429))
            .error.code,
          'RESOURCE_LIMIT',
        );
        await json(
          await query(a.url, { ...input, sort: [{ property: 'secret', direction: 'asc' }] }),
          403,
        );
        await json(
          await query(a.url, {
            ...input,
            sort: [{ property: 'rank', direction: 'asc' }],
            cursor: first.nextCursor,
          }),
          400,
        );
        const labels = await json(
          await query(a.url, {
            sort: [{ property: 'label', direction: 'asc' }],
            scope,
            budget,
            pageSize: 20,
            projection: ['uid', 'label'],
          }),
        );
        assert.deepEqual(
          labels.objects.map((item: { label: string | null }) => item.label),
          ['label-1', 'label-3', 'label-5', 'label-7', null, null, null, null],
        );
        const narrow = await json(
          await query(a.url, {
            ...input,
            scope: { fromUid: expected[0].uid, throughUid: expected[0].uid },
          }),
        );
        assert.equal(narrow.objects.length, 1);
        await json(await query(a.url, { ...input, budget: { ...budget, responseBytes: 10 } }), 422);
        const named = {
          sort: [{ property: 'name', direction: 'asc' }],
          scope,
          budget,
          pageSize: 2,
          projection: ['uid', 'name'],
        };
        const namedFirst = await json(await query(a.url, named));
        const candidate = rows
          .slice(0, 8)
          .filter(
            (row) => !namedFirst.objects.some((item: { uid: string }) => item.uid === row.uid),
          )
          .sort((left, right) => left.name.localeCompare(right.name))[0];
        await json(
          await invoke(a.url, 'edit', randomUUID(), {
            uid: candidate.uid,
            name: 'zz-moved-current-key',
          }),
        );
        const namedNext = await json(
          await query(b.url, { ...named, cursor: namedFirst.nextCursor }),
        );
        assert.ok(
          namedNext.objects.some(
            (item: { uid: string; name: string }) =>
              item.uid === candidate.uid && item.name === candidate.name,
          ),
        );
        await json(await invoke(a.url, 'clear', randomUUID(), { uid: candidate.uid }));
      },
    );
    await t.test(
      'bounded SQL aggregation counts only authorized rows with exact typed results and whole-request budgets',
      async () => {
        const input = {
          scope,
          budget: { ...budget, groups: 4 },
          groupBy: ['region'],
          metrics: {
            count: { op: 'count' },
            total: { op: 'sum', property: 'amount' },
            ranks: { op: 'sum', property: 'rank' },
            average: { op: 'average', property: 'rank', outputScale: 2 },
          },
        };
        const result = await json(await aggregate(a.url, input));
        assert.deepEqual(result.groups, [
          {
            key: { region: 'West' },
            values: { count: '8', total: '38.00', ranks: '12', average: '1.50' },
          },
        ]);
        assert.equal(result.types.average.scale, 2);
        assert.equal(
          (
            await json(
              await aggregate(a.url, { ...input, budget: { ...input.budget, scanRows: 7 } }),
              429,
            )
          ).error.code,
          'RESOURCE_LIMIT',
        );
        await json(
          await aggregate(a.url, {
            ...input,
            groupBy: ['rank'],
            budget: { ...input.budget, groups: 1 },
          }),
          429,
        );
        await json(
          await aggregate(a.url, { ...input, groupBy: ['secret'], filter: { literal: false } }),
          403,
        );
        await json(
          await aggregate(a.url, {
            ...input,
            metrics: { hidden: { op: 'sum', property: 'secret' } },
          }),
          403,
        );
        const empty = await json(
          await aggregate(a.url, {
            scope,
            budget: { ...budget, groups: 1 },
            filter: { op: 'eq', args: [{ field: 'region' }, { literal: 'East' }] },
            metrics: { count: { op: 'count' }, total: { op: 'sum', property: 'amount' } },
          }),
        );
        assert.deepEqual(empty.groups, [{ key: {}, values: { count: '0', total: null } }]);
        await source(
          'objects',
          rows.map((row) => ({ ...row, rank: '9223372036854775807' })),
        );
        await json(
          await aggregate(b.url, {
            scope,
            budget: { ...budget, groups: 1 },
            metrics: { total: { op: 'sum', property: 'rank' } },
          }),
          400,
        );
        await source();
      },
    );
    await t.test(
      'whole-population SQL aggregation authorizes before budgets and returns complete exact groups without UID scopes',
      async () => {
        const budget = {
          scanRows: 1_000_000_000,
          groups: 16,
          responseBytes: 1_000_000,
          timeMs: 30000,
        };
        const input = {
          budget,
          groupBy: ['region'],
          metrics: {
            count: { op: 'count' },
            sum: { op: 'sum', property: 'amount' },
            minimum: { op: 'min', property: 'rank' },
            maximum: { op: 'max', property: 'rank' },
            average: { op: 'avg', property: 'rank', outputScale: 2 },
          },
        };
        const expected = [
          {
            key: { region: 'West' },
            values: { count: '8', sum: '38.00', minimum: '0', maximum: '3', average: '1.50' },
          },
        ];
        assert.deepEqual((await json(await aggregate(a.url, input))).groups, expected);
        assert.deepEqual(
          (await json(await aggregate(b.url, { ...input, budget: { ...budget, scanRows: 8 } })))
            .groups,
          expected,
        );
        await json(await aggregate(a.url, { ...input, budget: { ...budget, scanRows: 7 } }), 429);
        await json(
          await aggregate(a.url, { ...input, groupBy: ['rank'], budget: { ...budget, groups: 1 } }),
          429,
        );
        await json(
          await aggregate(a.url, { ...input, budget: { ...budget, responseBytes: 1 } }),
          429,
        );
        await json(
          await aggregate(a.url, {
            ...input,
            groupBy: ['secret'],
            filter: { literal: false },
            budget: { ...budget, scanRows: 1 },
          }),
          403,
        );
        await json(
          await aggregate(a.url, {
            ...input,
            metrics: { hidden: { op: 'min', property: 'secret' } },
            filter: { literal: false },
          }),
          403,
        );
        await json(await aggregate(a.url, { metrics: { count: { op: 'count' } } }), 400);
        await json(
          await aggregate(a.url, { ...input, budget: { ...budget, scanRows: 1_000_000_001 } }),
          400,
        );
        const empty = await json(
          await aggregate(a.url, {
            budget,
            filter: { literal: false },
            metrics: { count: { op: 'count' }, sum: { op: 'sum', property: 'amount' } },
          }),
        );
        assert.deepEqual(empty.groups, [{ key: {}, values: { count: '0', sum: null } }]);
        await json(await invoke(a.url, 'delete', randomUUID(), { uid: rows[0].uid }));
        assert.equal((await json(await aggregate(b.url, input))).groups[0].values.count, '7');
        await json(await invoke(a.url, 'restore', randomUUID(), { uid: rows[0].uid }));
        await db.pool.query(
          'UPDATE moria.native_serving_bindings SET definition=$2 WHERE environment=$1 AND resource=$3',
          [
            environment,
            {
              ...definition,
              policies: {
                ...definition.policies,
                objectRead: { op: 'lt', args: [{ field: 'rank' }, { literal: '3' }] },
              },
            },
            'objects',
          ],
        );
        try {
          assert.equal(
            (await json(await aggregate(b.url, { ...input, budget: { ...budget, scanRows: 6 } })))
              .groups[0].values.count,
            '6',
          );
        } finally {
          await db.pool.query(
            'UPDATE moria.native_serving_bindings SET definition=$2 WHERE environment=$1 AND resource=$3',
            [environment, definition, 'objects'],
          );
        }
        await source(
          'objects',
          rows.map((row) => ({ ...row, rank: '9223372036854775807' })),
        );
        await json(
          await aggregate(a.url, { budget, metrics: { sum: { op: 'sum', property: 'rank' } } }),
          400,
        );
        await source();
        const short = await sessions.mint({ environment, principal, application }, 'fixture');
        const tokenSession = await sessions.authenticate(`Bearer ${short.token}`);
        const barrier = await db.pool.connect();
        try {
          await barrier.query('BEGIN');
          await barrier.query('LOCK moria.staged_source_rows IN ACCESS EXCLUSIVE MODE');
          const pending = fetch(`${b.url}/api/v1/objects/${objectType}/aggregate`, {
            method: 'POST',
            headers: headers(short.token),
            body: JSON.stringify(input),
          });
          await waitForLock('WITH settings AS%');
          await db.pool.query(
            'UPDATE moria.workload_sessions SET disabled=true WHERE token_hash=$1',
            [tokenSession.sessionHash],
          );
          await barrier.query('ROLLBACK');
          await json(await pending, 401);
        } finally {
          await barrier.query('ROLLBACK');
          barrier.release();
        }
      },
    );
    await t.test(
      'trusted UID-reference links enforce both endpoint policies and complete bounded fanout',
      async () => {
        const outward = await json(
          await traverse(a.url, {
            uid: rows[0].uid,
            direction: 'out',
            projection: ['uid', 'name'],
            budget,
            fanout: 1,
          }),
        );
        assert.deepEqual(outward.objects, [{ uid: rows[4].uid, name: rows[4].name }]);
        const inward = await json(
          await traverse(b.url, {
            uid: rows[4].uid,
            direction: 'in',
            projection: ['uid'],
            scope,
            budget,
            fanout: 4,
          }),
        );
        assert.deepEqual(
          inward.objects.map((item: { uid: string }) => item.uid),
          rows
            .slice(0, 4)
            .map((item) => item.uid)
            .sort(),
        );
        await json(
          await traverse(a.url, {
            uid: rows[4].uid,
            direction: 'in',
            projection: ['uid'],
            scope,
            budget,
            fanout: 3,
          }),
          429,
        );
        await json(
          await traverse(a.url, {
            uid: rows[4].uid,
            direction: 'in',
            projection: ['uid'],
            budget,
            fanout: 4,
          }),
          422,
        );
        await json(
          await traverse(a.url, {
            uid: rows[8].uid,
            direction: 'out',
            projection: ['uid'],
            budget,
            fanout: 1,
          }),
          404,
        );
        await json(
          await traverse(
            a.url,
            { uid: rows[0].uid, direction: 'out', projection: ['uid'], budget, fanout: 1 },
            'forbidden-relations',
          ),
          403,
        );
        const none = await json(
          await traverse(a.url, {
            uid: rows[5].uid,
            direction: 'out',
            projection: ['uid'],
            budget,
            fanout: 1,
          }),
        );
        assert.deepEqual(none.objects, []);
      },
    );

    await t.test(
      'cursor continuations work concurrently across native replicas and retain the original deadline',
      async () => {
        const first = await json(await query(a.url, { pageSize: 2, projection: ['uid', 'name'] }));
        assert.equal(first.objects.length, 2);
        assert.equal(first.nextCursor.length, 43);
        const [left, right] = await Promise.all([
          query(a.url, { pageSize: 2, projection: ['uid', 'name'], cursor: first.nextCursor }).then(
            (response) => json(response),
          ),
          query(b.url, { pageSize: 2, projection: ['uid', 'name'], cursor: first.nextCursor }).then(
            (response) => json(response),
          ),
        ]);
        assert.deepEqual(left.objects, right.objects);
        assert.equal(left.revision, first.revision);
        const cursors = (
          await db.pool.query(
            'SELECT count(DISTINCT expires_at) AS deadlines FROM moria.read_cursors WHERE environment=$1 AND binding_hash=(SELECT binding_hash FROM moria.read_cursors WHERE token_hash=$2)',
            [environment, createHash('sha256').update(first.nextCursor).digest('hex')],
          )
        ).rows[0];
        assert.equal(cursors.deadlines, '1');
        await db.pool.query(
          "UPDATE moria.read_cursors SET expires_at=clock_timestamp()-interval '1 second' WHERE environment=$1",
          [environment],
        );
        const expired = await json(
          await query(b.url, { pageSize: 2, projection: ['uid', 'name'], cursor: left.nextCursor }),
          410,
        );
        assert.equal(expired.error.code, 'CURSOR_EXPIRED');
      },
    );

    await t.test(
      'typed multi-type edits retain canonical policy rows, atomicity and original input receipts',
      async () => {
        const auxiliaryType = randomUUID();
        const auxiliaryUid = await db.catalog.resolveIdentity(`typed-${environment}`, 'auxiliary');
        const auxiliaryDefinition = {
          ...definition,
          objectTypeId: auxiliaryType,
          policies: {
            ...definition.policies,
            resource: 'auxiliary',
            sourceColumns: {
              rawRegion: properties.region,
              rawName: properties.name,
              rawUid: properties.uid,
            },
            row: { op: 'eq', args: [{ field: 'rawRegion' }, { principalAttribute: 'region' }] },
          },
        };
        await register('auxiliary', auxiliaryDefinition, auxiliaryType);
        await source('auxiliary', [{ ...rows[0], uid: auxiliaryUid, name: 'mapped object' }]);
        const canonical = async (region: string) =>
          db.pool.query(
            'UPDATE moria.staged_source_rows SET source_value=$3 WHERE stage=(SELECT stage FROM moria.source_generations WHERE environment=$1 AND object_type=$2 ORDER BY revision DESC LIMIT 1)',
            [
              environment,
              'auxiliary',
              { rawRegion: region, rawName: 'unmapped canonical secret', rawUid: auxiliaryUid },
            ],
          );
        await canonical('West');
        const typed = {
          resource: 'typed',
          revision: 'typed-v1',
          markings: ['private'],
          targetParameter: 'uid',
          inputs: { uid: properties.uid, other: properties.uid, name: properties.name },
          condition: { op: 'ne', args: [{ field: 'name' }, { literal: 'blocked' }] },
          edits: [],
          typedEdits: [
            {
              op: 'set',
              targetType: 'objects',
              uid: { parameter: 'uid' },
              property: 'name',
              value: { parameter: 'name' },
            },
            {
              op: 'set',
              targetType: 'auxiliary',
              uid: { parameter: 'other' },
              property: 'name',
              value: { field: 'name' },
            },
            {
              op: 'set',
              targetType: 'auxiliary',
              uid: { parameter: 'other' },
              property: 'label',
              value: { principalAttribute: 'region' },
            },
          ],
        };
        await db.pool.query(
          'INSERT INTO moria.native_serving_actions(environment,resource,object_resource,revision,definition) VALUES($1,$2,$3,$4,$5)',
          [environment, 'typed', 'objects', typed.revision, typed],
        );
        const before = (await json(await read(a.url, rows[0].uid))).object.name;
        const key = randomUUID(),
          input = { uid: rows[0].uid, other: auxiliaryUid, name: 'typed update' };
        const receipt = await json(await invoke(a.url, 'typed', key, input));
        assert.deepEqual(await json(await invoke(b.url, 'typed', key, input)), receipt);
        assert.equal((await json(await read(a.url, rows[0].uid))).object.name, input.name);
        const auxiliary = (await json(await read(b.url, auxiliaryUid, issued.token, auxiliaryType)))
          .object;
        assert.equal(auxiliary.name, before);
        assert.equal(auxiliary.label, 'West');
        assert(!Object.hasOwn(auxiliary, 'rawName'));
        assert.equal(
          (
            await db.pool.query(
              'SELECT cardinality(targets) AS n FROM moria.native_serving_audit WHERE environment=$1 AND revision=$2',
              [environment, receipt.revision],
            )
          ).rows[0].n,
          2,
        );
        await json(await invoke(b.url, 'typed', key, { ...input, name: 'different' }), 409);
        await canonical('East');
        await json(await read(a.url, auxiliaryUid, issued.token, auxiliaryType), 404);
        const filtered = await json(
          await fetch(`${b.url}/api/v1/objects/${auxiliaryType}/query`, {
            method: 'POST',
            headers: headers(),
            body: JSON.stringify({ projection: ['uid', 'name'], pageSize: 1 }),
          }),
        );
        assert.deepEqual(filtered.objects, []);
        const failedKey = randomUUID();
        await json(
          await invoke(a.url, 'typed', failedKey, { ...input, name: 'must roll back' }),
          404,
        );
        assert.equal((await json(await read(b.url, rows[0].uid))).object.name, input.name);
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*)::int AS n FROM moria.action_keys WHERE environment=$1 AND action=$2 AND key=$3',
              [environment, 'typed', failedKey],
            )
          ).rows[0].n,
          0,
        );
        await canonical('West');
        const forbidden = structuredClone(typed);
        forbidden.typedEdits[1].value = { field: 'secret' } as any;
        await db.pool.query(
          'UPDATE moria.native_serving_actions SET definition=$2 WHERE environment=$1 AND resource=$3',
          [environment, forbidden, 'typed'],
        );
        await json(await invoke(a.url, 'typed', randomUUID(), input), 403);
        await db.pool.query(
          'UPDATE moria.native_serving_actions SET definition=$2 WHERE environment=$1 AND resource=$3',
          [environment, typed, 'typed'],
        );
        await json(await invoke(a.url, 'clear', randomUUID(), { uid: rows[0].uid }));
      },
    );
    const preparedBarrier = async (action: string, input: unknown, change: () => Promise<void>) => {
      const key = randomUUID(),
        lock = `moria/action-publication/${fingerprint([environment, application, principal, action, key])}`;
      const barrier = await db.pool.connect();
      let pending: Promise<Response> | undefined;
      try {
        await barrier.query('SELECT pg_advisory_lock(hashtextextended($1,0))', [lock]);
        pending = invoke(a.url, action, key, input);
        await waitForLock('SELECT pg_advisory_lock(hashtextextended($1,0))');
        const readable = await Promise.race([
          read(b.url, rows[0].uid),
          delay(1000).then(() => {
            throw new Error('Prepared Action must not retain the environment writer barrier');
          }),
        ]);
        await json(readable);
        await change();
      } finally {
        await barrier.query('SELECT pg_advisory_unlock_all()');
        barrier.release();
      }
      assert(pending);
      return { key, response: await pending };
    };
    await t.test(
      'an ordinary Action waits for its UID home before the environment writer barrier',
      async () => {
        const barrier = await db.pool.connect();
        let pending: Promise<Response> | undefined;
        try {
          await barrier.query('BEGIN');
          await barrier.query('SELECT uid FROM moria.object_homes WHERE uid=$1 FOR UPDATE', [
            rows[0].uid,
          ]);
          pending = invoke(a.url, 'edit', randomUUID(), {
            uid: rows[0].uid,
            name: 'after-source-home',
          });
          await waitForLock(
            'SELECT uid FROM moria.object_homes WHERE uid=ANY($1) ORDER BY uid FOR UPDATE',
          );
          await json(
            await Promise.race([
              read(b.url, rows[0].uid),
              delay(1000).then(() => {
                throw Error('A UID-home wait must not hold environment admission');
              }),
            ]),
          );
          await json(
            await invoke(b.url, 'edit', randomUUID(), {
              uid: rows[1].uid,
              name: 'independent-home',
            }),
          );
          assert.equal(
            (
              await barrier.query(
                'SELECT pg_try_advisory_xact_lock(hashtextextended($1,0)) AS admitted',
                [`moria/environment/${environment}`],
              )
            ).rows[0].admitted,
            true,
            'A Source final barrier can enter while the Action waits for its home',
          );
        } finally {
          await barrier.query('ROLLBACK');
          barrier.release();
        }
        assert(pending);
        await json(await pending);
        assert.equal((await json(await read(b.url, rows[0].uid))).object.name, 'after-source-home');
        assert.equal((await json(await read(b.url, rows[1].uid))).object.name, 'independent-home');
        for (const row of rows.slice(0, 2))
          await json(await invoke(b.url, 'clear', randomUUID(), { uid: row.uid }));
      },
    );
    await t.test(
      'authority revoked during a UID-home wait cannot release a prepared Action',
      async () => {
        const barrier = await db.pool.connect();
        let pending: Promise<Response> | undefined;
        const key = randomUUID();
        try {
          await barrier.query('BEGIN');
          await barrier.query('SELECT uid FROM moria.object_homes WHERE uid=$1 FOR UPDATE', [
            rows[0].uid,
          ]);
          pending = invoke(a.url, 'edit', key, { uid: rows[0].uid, name: 'must-not-publish' });
          await waitForLock(
            'SELECT uid FROM moria.object_homes WHERE uid=ANY($1) ORDER BY uid FOR UPDATE',
          );
          await authority.setGrants(
            environment,
            'fixture',
            principal,
            application,
            operations.filter((op) => op !== 'edit:invoke'),
          );
          await barrier.query('ROLLBACK');
          assert(pending);
          await json(await pending, 403);
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*)::int n FROM moria.action_keys WHERE environment=$1 AND key=$2',
                [environment, key],
              )
            ).rows[0].n,
            0,
          );
        } finally {
          await barrier.query('ROLLBACK');
          barrier.release();
          await authority.setGrants(environment, 'fixture', principal, application, operations);
        }
      },
    );
    await t.test(
      'ordinary Action publication rechecks changed target and complete multi-target footprint',
      async () => {
        for (const group of [false, true]) {
          const target = group ? rows[1] : rows[0];
          const input = group
            ? { uids: [rows[0].uid, rows[1].uid], name: 'must not publish' }
            : { uid: rows[0].uid, name: 'must not publish' };
          const pending = await preparedBarrier(group ? 'group' : 'edit', input, async () => {
            await json(
              await invoke(b.url, 'edit', randomUUID(), { uid: target.uid, name: 'blocked' }),
            );
          });
          const failed = await json(pending.response, 409);
          assert.equal(failed.error.code, 'ACTION_CONDITION_FAILED');
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*)::int n FROM moria.action_keys WHERE environment=$1 AND key=$2',
                [environment, pending.key],
              )
            ).rows[0].n,
            0,
          );
          if (group)
            assert.equal(
              (await json(await read(b.url, rows[0].uid))).object.name,
              rows[0].name,
              'failed multi-object publication is atomic',
            );
          await json(await invoke(b.url, 'reset', randomUUID(), { uid: target.uid }));
        }
      },
    );
    await t.test(
      'ordinary Action publication rechecks replacement source and current property policy',
      async () => {
        try {
          const replaced = await preparedBarrier(
            'edit',
            { uid: rows[0].uid, name: 'must not publish' },
            async () => {
              await source(
                'objects',
                rows.map((row, index) => (index === 0 ? { ...row, name: 'blocked' } : row)),
              );
            },
          );
          assert.equal((await json(replaced.response, 409)).error.code, 'ACTION_CONDITION_FAILED');
        } finally {
          await source();
        }
        const original = (
          await db.pool.query(
            'SELECT definition,revision FROM moria.native_serving_bindings WHERE environment=$1 AND resource=$2',
            [environment, 'objects'],
          )
        ).rows[0];
        try {
          const denied = await preparedBarrier(
            'edit',
            { uid: rows[0].uid, name: 'must not publish' },
            async () => {
              const changed = {
                ...original.definition,
                propertyWrite: { name: { literal: false } },
              };
              await db.pool.query(
                'UPDATE moria.native_serving_bindings SET definition=$3,revision=$4 WHERE environment=$1 AND resource=$2',
                [environment, 'objects', changed, fingerprint(changed)],
              );
            },
          );
          assert.equal((await json(denied.response, 403)).error.code, 'PROPERTY_FORBIDDEN');
        } finally {
          await db.pool.query(
            'UPDATE moria.native_serving_bindings SET definition=$3,revision=$4 WHERE environment=$1 AND resource=$2',
            [environment, 'objects', original.definition, original.revision],
          );
        }
      },
    );
    await t.test(
      'ordinary Action publication checks current grants before releasing prepared effects',
      async () => {
        try {
          const denied = await preparedBarrier(
            'edit',
            { uid: rows[0].uid, name: 'must not publish' },
            async () => {
              await authority.setGrants(
                environment,
                'fixture',
                principal,
                application,
                operations.filter((op) => op !== 'edit:invoke'),
              );
            },
          );
          assert.equal((await json(denied.response, 403)).error.code, 'FORBIDDEN');
        } finally {
          await authority.setGrants(environment, 'fixture', principal, application, operations);
        }
      },
    );
    await t.test(
      'guarded Action batch matches scalar resource authority without caller demands',
      async () => {
        const value = (
          await db.pool.query('SELECT moria.native_action_resource_access($1,$2) value', [
            session.sessionHash,
            'edit',
          ])
        ).rows[0].value;
        assert.deepEqual(
          value.resources.map((item: { resource: string; operation: string }) => [
            item.resource,
            item.operation,
          ]),
          [
            ['objects', 'read'],
            ['objects', 'write'],
            ['edit', 'read'],
            ['edit', 'write'],
          ],
        );
        for (const item of value.resources) {
          const scalar = (
            await db.pool.query('SELECT moria.native_serving_resource_access($1,$2,$3) labels', [
              session.sessionHash,
              item.resource,
              item.operation,
            ])
          ).rows[0].labels;
          assert.deepEqual([...item.markings].sort(), [...scalar].sort());
        }
        await assert.rejects(
          db.pool.query('SELECT moria.native_action_resource_access($1,$2)', [
            '0'.repeat(64),
            'edit',
          ]),
        );
      },
    );

    await t.test(
      'typed ordinary preparation pins primary expression reads even when primary is not edited',
      async () => {
        const original = (
          await db.pool.query(
            'SELECT definition FROM moria.native_serving_actions WHERE environment=$1 AND resource=$2',
            [environment, 'typed'],
          )
        ).rows[0].definition;
        const other = (
          await db.pool.query(
            'SELECT uid FROM moria.staged_source_rows WHERE stage=(SELECT g.stage FROM moria.source_generations g WHERE g.environment=$1 AND g.object_type=$2 ORDER BY g.revision DESC LIMIT 1) LIMIT 1',
            [environment, 'auxiliary'],
          )
        ).rows[0].uid;
        const changed = {
          ...original,
          typedEdits: original.typedEdits.filter(
            (edit: { targetType: string }) => edit.targetType === 'auxiliary',
          ),
        };
        await db.pool.query(
          'UPDATE moria.native_serving_actions SET definition=$3 WHERE environment=$1 AND resource=$2',
          [environment, 'typed', changed],
        );
        try {
          const result = await preparedBarrier(
            'typed',
            { uid: rows[0].uid, other, name: 'unused' },
            async () => {
              await json(
                await invoke(b.url, 'edit', randomUUID(), {
                  uid: rows[0].uid,
                  name: 'fresh primary expression',
                }),
              );
            },
          );
          await json(result.response);
          const targetType = (
            await db.pool.query(
              'SELECT object_type_id FROM moria.native_serving_bindings WHERE environment=$1 AND resource=$2',
              [environment, 'auxiliary'],
            )
          ).rows[0].object_type_id;
          assert.equal(
            (await json(await read(b.url, other, issued.token, targetType))).object.name,
            'fresh primary expression',
          );
          assert.equal(
            (await json(await read(b.url, rows[0].uid))).object.name,
            'fresh primary expression',
          );
        } finally {
          await db.pool.query(
            'UPDATE moria.native_serving_actions SET definition=$3 WHERE environment=$1 AND resource=$2',
            [environment, 'typed', original],
          );
          await json(await invoke(b.url, 'reset', randomUUID(), { uid: rows[0].uid }));
        }
      },
    );
    await t.test(
      'typed ordinary publication rechecks secondary property policy and rolls back all targets',
      async () => {
        const row = (
          await db.pool.query(
            'SELECT definition,revision FROM moria.native_serving_bindings WHERE environment=$1 AND resource=$2',
            [environment, 'auxiliary'],
          )
        ).rows[0];
        const other = (
          await db.pool.query(
            'SELECT uid FROM moria.staged_source_rows WHERE stage=(SELECT g.stage FROM moria.source_generations g WHERE g.environment=$1 AND g.object_type=$2 ORDER BY g.revision DESC LIMIT 1) LIMIT 1',
            [environment, 'auxiliary'],
          )
        ).rows[0].uid;
        try {
          const result = await preparedBarrier(
            'typed',
            { uid: rows[0].uid, other, name: 'must roll back' },
            async () => {
              const changed = { ...row.definition, propertyWrite: { name: { literal: false } } };
              await db.pool.query(
                'UPDATE moria.native_serving_bindings SET definition=$3,revision=$4 WHERE environment=$1 AND resource=$2',
                [environment, 'auxiliary', changed, fingerprint(changed)],
              );
            },
          );
          assert.equal((await json(result.response, 403)).error.code, 'PROPERTY_FORBIDDEN');
          assert.equal((await json(await read(b.url, rows[0].uid))).object.name, rows[0].name);
        } finally {
          await db.pool.query(
            'UPDATE moria.native_serving_bindings SET definition=$3,revision=$4 WHERE environment=$1 AND resource=$2',
            [environment, 'auxiliary', row.definition, row.revision],
          );
        }
      },
    );

    await t.test(
      'same-key actions across replicas have one durable effect and Node reads native overlays',
      async () => {
        const key = randomUUID();
        const replies = await Promise.all(
          Array.from({ length: 12 }, (_, index) =>
            invoke(index % 2 ? a.url : b.url, 'edit', key, {
              uid: rows[0].uid,
              name: 'native override',
            }).then((reply) => json(reply)),
          ),
        );
        for (const reply of replies) assert.deepEqual(reply, replies[0]);
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.native_serving_audit WHERE environment=$1 AND key=$2',
              [environment, key],
            )
          ).rows[0].count,
          '1',
        );
        assert.equal((await service.read(session, rows[0].uid)).object.name, 'native override');
        const conflict = await json(
          await invoke(b.url, 'edit', key, { uid: rows[0].uid, name: 'different' }),
          409,
        );
        assert.equal(conflict.error.code, 'IDEMPOTENCY_CONFLICT');
        await json(await invoke(a.url, 'clear', randomUUID(), { uid: rows[0].uid }));
        assert.equal((await json(await read(b.url, rows[0].uid))).object.name, rows[0].name);
      },
    );

    await t.test(
      'accepted action input schemas stay pinned through redeployment and expiry never permits key reuse',
      async () => {
        const original = (
          await db.pool.query(
            'SELECT revision,definition FROM moria.native_serving_actions WHERE environment=$1 AND resource=$2',
            [environment, 'edit'],
          )
        ).rows[0];
        const key = randomUUID(),
          input = { uid: rows[0].uid, name: 'pinned input' };
        const accepted = await json(await invoke(a.url, 'edit', key, input));
        const changed = {
          ...original.definition,
          revision: 'v2',
          function: randomUUID(),
          targetParameter: 'newUid',
          inputs: { newUid: properties.uid, flag: { type: 'boolean', nullable: false } },
          edits: [],
        };
        await db.catalog.transaction(
          (client) =>
            client.query(
              'UPDATE moria.native_serving_actions SET revision=$3,definition=$4 WHERE environment=$1 AND resource=$2',
              [environment, 'edit', 'v2', changed],
            ),
          environment,
        );
        try {
          assert.deepEqual(await json(await invoke(b.url, 'edit', key, input)), accepted);
          const unavailable = await json(
            await invoke(b.url, 'edit', randomUUID(), { newUid: rows[0].uid, flag: true }),
            404,
          );
          assert.equal(unavailable.error.code, 'NOT_FOUND');
          await db.pool.query(
            "UPDATE moria.action_keys SET expires_at=clock_timestamp()-interval '1 second',result=NULL WHERE environment=$1 AND application=$2 AND principal=$3 AND action=$4 AND key=$5",
            [environment, application, principal, 'edit', key],
          );
          const expired = await json(await invoke(b.url, 'edit', key, input), 410);
          assert.equal(expired.error.code, 'RESULT_EXPIRED');
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*) FROM moria.native_serving_audit WHERE environment=$1 AND key=$2',
                [environment, key],
              )
            ).rows[0].count,
            '1',
          );
        } finally {
          await db.catalog.transaction(
            (client) =>
              client.query(
                'UPDATE moria.native_serving_actions SET revision=$3,definition=$4 WHERE environment=$1 AND resource=$2',
                [environment, 'edit', original.revision, original.definition],
              ),
            environment,
          );
        }
      },
    );

    await t.test(
      'multi-target actions roll back earlier edits on a later condition failure and commit successful batches once',
      async () => {
        const key = randomUUID();
        const denied = await json(
          await invoke(a.url, 'group', key, {
            uids: [rows[1].uid, rows[7].uid],
            name: 'must roll back',
          }),
          409,
        );
        assert.equal(denied.error.code, 'ACTION_CONDITION_FAILED');
        assert.equal((await json(await read(b.url, rows[1].uid))).object.name, rows[1].name);
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.action_keys WHERE environment=$1 AND key=$2',
              [environment, key],
            )
          ).rows[0].count,
          '0',
        );
        const result = await json(
          await invoke(b.url, 'group', randomUUID(), {
            uids: [rows[1].uid, rows[2].uid],
            name: 'group override',
          }),
        );
        const versions = (
          await db.pool.query(
            'SELECT versions.revision::text FROM (SELECT DISTINCT revision FROM moria.overlay_versions WHERE environment=$1 AND uid=ANY($2)) versions ORDER BY versions.revision DESC',
            [environment, [rows[1].uid, rows[2].uid]],
          )
        ).rows;
        assert.equal(versions[0].revision, result.revision);
        assert.equal((await json(await read(a.url, rows[2].uid))).object.name, 'group override');
      },
    );

    await t.test(
      'source refresh retains overlays and explicit tombstones survive until an authorized restore',
      async () => {
        await json(await invoke(a.url, 'delete', randomUUID(), { uid: rows[3].uid }));
        await json(await read(b.url, rows[3].uid), 404);
        await source(
          'objects',
          rows.map((row) => ({ ...row, name: `refreshed ${row.name}` })),
        );
        assert.equal((await json(await read(b.url, rows[1].uid))).object.name, 'group override');
        await json(await read(b.url, rows[3].uid), 404);
        await json(await invoke(a.url, 'restore', randomUUID(), { uid: rows[3].uid }));
        assert.equal(
          (await json(await read(b.url, rows[3].uid))).object.name,
          `refreshed ${rows[3].name}`,
        );
      },
    );

    await t.test(
      'a committed action survives loss of its HTTP acknowledgment and death of the native replica',
      async (t) => {
        let held!: (body: unknown) => void;
        const committed = new Promise<any>((resolve) => {
          held = resolve;
        });
        const proxy = createServer(async (request, response) => {
          try {
            const parts: Buffer[] = [];
            for await (const part of request) parts.push(Buffer.from(part));
            const upstream = await fetch(`${a.url}${request.url}`, {
              method: 'POST',
              headers: {
                ...headers(),
                'idempotency-key': String(request.headers['idempotency-key']),
              },
              body: Buffer.concat(parts),
            });
            const body = await upstream.json();
            held(body);
            await a.done;
            response.destroy();
          } catch {
            response.destroy();
          }
        });
        await new Promise<void>((resolve) => proxy.listen(0, '127.0.0.1', resolve));
        t.after(async () => {
          proxy.closeAllConnections();
          await new Promise<void>((resolve) => proxy.close(() => resolve()));
        });
        const port = (proxy.address() as { port: number }).port,
          key = randomUUID();
        const lost = invoke(`http://127.0.0.1:${port}`, 'edit', key, {
          uid: rows[4].uid,
          name: 'durable response loss',
        });
        void lost.catch(() => {});
        const receipt = await committed;
        assert.equal(typeof receipt.revision, 'string');
        a.process.kill('SIGKILL');
        await assert.rejects(lost);
        const replay = await json(
          await invoke(b.url, 'edit', key, { uid: rows[4].uid, name: 'durable response loss' }),
        );
        assert.deepEqual(replay, receipt);
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.native_serving_audit WHERE environment=$1 AND key=$2',
              [environment, key],
            )
          ).rows[0].count,
          '1',
        );
      },
    );

    await t.test(
      'a session expiring during blocked native evaluation cannot release buffered results',
      async () => {
        const short = await sessions.mint({ environment, principal, application }, 'fixture');
        const shortSession = await sessions.authenticate(`Bearer ${short.token}`);
        const barrier = await db.pool.connect();
        try {
          await barrier.query('BEGIN');
          await barrier.query('LOCK moria.staged_source_rows IN ACCESS EXCLUSIVE MODE');
          const pending = read(b.url, rows[0].uid, short.token);
          await waitForLock('SELECT value,source_value FROM moria.runtime_object_base%');
          await db.pool.query(
            "UPDATE moria.workload_sessions SET expires_at=clock_timestamp()+interval '1 second' WHERE token_hash=$1",
            [shortSession.sessionHash],
          );

          for (let attempt = 0; attempt < 300; attempt++) {
            if (
              (
                await db.pool.query(
                  'SELECT expires_at<=clock_timestamp() AS expired FROM moria.workload_sessions WHERE token_hash=$1',
                  [shortSession.sessionHash],
                )
              ).rows[0].expired
            )
              break;
            await delay(10);
          }
          await barrier.query('ROLLBACK');
          const denied = await json(await pending, 401);
          assert.equal(denied.error.code, 'UNAUTHENTICATED');
          assert.equal(Object.hasOwn(denied, 'object'), false);
        } finally {
          await barrier.query('ROLLBACK');
          barrier.release();
        }
      },
    );

    await t.test(
      'queued native reads see committed grant revocation before their snapshot',
      async () => {
        const barrier = await db.pool.connect();
        const lock = `moria/environment/${environment}`;
        try {
          await barrier.query('SELECT pg_advisory_lock(hashtextextended($1,0))', [lock]);
          const pending = read(b.url, rows[0].uid);
          await waitForLock('SELECT pg_advisory_lock_shared(hashtextextended($1,%');
          await barrier.query('BEGIN');
          await barrier.query(
            'UPDATE moria.permissions SET operations=$4 WHERE environment=$1 AND principal=$2 AND application=$3',
            [
              environment,
              principal,
              application,
              operations.filter((operation) => operation !== 'objects:read'),
            ],
          );
          await barrier.query(
            'UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id=$1',
            [environment],
          );
          await barrier.query('COMMIT');
          await barrier.query('SELECT pg_advisory_unlock(hashtextextended($1,0))', [lock]);
          const denied = await json(await pending, 403);
          assert.equal(denied.error.code, 'FORBIDDEN');
        } finally {
          await barrier.query('ROLLBACK');
          await barrier.query('SELECT pg_advisory_unlock_all()');
          barrier.release();
        }
        await authority.setGrants(environment, 'fixture', principal, application, operations);
      },
    );

    await t.test(
      'filesystem resource inheritance and provenance remain required by native domain reads',
      async () => {
        const runtime = await createLocalFilesystem(db.catalog, join(db.root, 'filesystem'));
        const context = { session };
        const home = await runtime.management.ensureHome(context);
        const setupOperations = [
          ...operations,
          'filesystem:register',
          'filesystem:publish',
          'filesystem:export',
        ];
        await authority.setApplication(environment, 'fixture', application, setupOperations);
        await authority.setGrants(environment, 'fixture', principal, application, setupOperations);
        const currentHome = await runtime.filesystem.get(context, home.home.id);
        const project = await runtime.management.createProject(
          context,
          currentHome.id,
          'source-project',
          currentHome.revision,
        );
        const projectChildren = await runtime.filesystem.list(context, project.entry.id);
        const sourceDirectory = projectChildren.entries.find((entry) => entry.name === 'source')!;
        const sourceParent = await runtime.filesystem.get(context, sourceDirectory.id);
        const code = (
          await runtime.filesystem.store.apply(
            { environment, principal, application },
            randomUUID(),
            [
              {
                op: 'create',
                parentId: sourceParent.id,
                expectedParentRevision: sourceParent.revision,
                name: 'ontology',
                kind: 'directory',
              },
            ],
          )
        ).entries.find((entry) => entry.name === 'ontology')!;
        const namespace = 'native_fs';
        const write = async (name: string, value: unknown) => {
          const bytes = new TextEncoder().encode(JSON.stringify(value));
          const parent = await runtime.filesystem.get(context, code.id);
          await runtime.filesystem.writeFile(
            context,
            randomUUID(),
            {
              op: 'create',
              parentId: code.id,
              expectedParentRevision: parent.revision,
              name,
              content: {
                digest: `sha256:${createHash('sha256').update(bytes).digest('hex')}`,
                size: bytes.length,
                mediaType: 'application/json',
              },
            },
            new Response(bytes).body!,
          );
        };
        await write('.ontology.json', {
          version: 1,
          name: 'Native filesystem authority',
          namespace,
        });
        await write('source.resource.json', {
          apiVersion: 'moria/v1alpha2',
          kind: 'Dataset',
          metadata: { namespace, name: 'source' },
          spec: { columns: properties, primaryKey: ['uid'], writeModes: ['append'] },
        });
        await write('records.object.json', {
          apiVersion: 'moria/v1alpha2',
          kind: 'ObjectType',
          metadata: { namespace, name: 'records' },
          spec: {
            primaryKey: 'uid',
            properties,
            source: {
              resource: `${namespace}/Dataset/source`,
              mapping: Object.fromEntries(
                Object.keys(properties).map((property) => [property, property]),
              ),
            },
          },
        });
        const root = await runtime.filesystem.get(context, code.id);
        await runtime.source.register(context, root.id, root.revision, 'ontology');
        const checkout = await runtime.source.checkout(context, root.id);
        const publication = await runtime.source.publish(context, root.id, checkout.digest);
        const plan = await runtime.source.plan(context, root.id, publication.digest);
        await runtime.source.apply(context, plan.id);
        const scopedType = publication.resources.find(
          (resource) => resource.reference === `${namespace}/ObjectType/records`,
        )!.id;
        const sourceDataset = publication.resources.find(
          (resource) => resource.reference === `${namespace}/Dataset/source`,
        )!.id;
        await db.catalog.ensureDataset(environment, sourceDataset);
        const target = await runtime.source.resource(context, scopedType);
        const originParent = await runtime.filesystem.get(context, project.entry.id);
        const origin = (
          await runtime.filesystem.store.apply(
            { environment, principal, application },
            randomUUID(),
            [
              {
                op: 'create',
                parentId: originParent.id,
                expectedParentRevision: originParent.revision,
                name: 'classified origin',
                kind: 'directory',
              },
            ],
          )
        ).entries.find((entry) => entry.name === 'classified origin')!;
        const scopedOperations = [
          ...setupOperations,
          `${scopedType}:read`,
          `${scopedType}:index`,
          `${sourceDataset}:read`,
        ];
        await authority.setApplication(environment, 'fixture', application, scopedOperations);
        await authority.setGrants(environment, 'fixture', principal, application, scopedOperations);
        const scoped = {
          ...definition,
          objectTypeId: scopedType,
          policies: { ...definition.policies, resource: scopedType },
        };
        await register(scopedType, scoped, scopedType);
        const scopedRows = [
          { ...rows[0], uid: await db.catalog.resolveIdentity(`native-fs-${environment}`, 'one') },
        ];
        await source(scopedType, scopedRows);
        await json(await read(b.url, scopedRows[0].uid, issued.token, scopedType));
        await db.catalog.transaction(async (client) => {
          await db.catalog.admitDataProvenance(
            client,
            createHash('sha256').update(issued.token).digest('hex'),
            [scopedType],
            [sourceDataset],
            'index',
          );
        }, environment);
        const sourceResource = await runtime.source.resource(context, sourceDataset);
        await runtime.filesystem.security.setPolicy('fixture', sourceResource.entryId, '0', {
          markings: [],
          grants: [
            {
              subject: { kind: 'principal', id: home.principalId },
              operations: ['read'],
              inherit: false,
              effect: 'deny',
            },
          ],
        });
        await assert.rejects(
          db.catalog.transaction((client) => authority.resourceAccess(client, session, scopedType)),
          { code: 'FORBIDDEN' },
        );
        await json(await read(b.url, scopedRows[0].uid, issued.token, scopedType), 403);
        await runtime.filesystem.security.setPolicy('fixture', sourceResource.entryId, '1', {
          markings: [],
          grants: [],
        });
        await json(await read(b.url, scopedRows[0].uid, issued.token, scopedType));
        await db.catalog.transaction(async (client) => {
          await client.query('INSERT INTO moria.fs_provenance(entry_id,source_id) VALUES($1,$2)', [
            target.entryId,
            origin.id,
          ]);
        }, environment);
        const old = (
          await db.pool.query('SELECT revision,grants FROM moria.fs_policies WHERE entry_id=$1', [
            origin.id,
          ])
        ).rows[0];
        await runtime.filesystem.security.setPolicy('fixture', origin.id, old?.revision ?? '0', {
          markings: [{ environment, marking: 'extra-secret' }],
          grants: old?.grants ?? [],
        });
        const denied = await json(
          await read(b.url, scopedRows[0].uid, issued.token, scopedType),
          403,
        );
        assert.equal(denied.error.code, 'FORBIDDEN');
      },
    );
  },
);
