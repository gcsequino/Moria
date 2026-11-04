import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { execFile, spawn, type ChildProcess } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { promisify } from 'node:util';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { fingerprint } from '../packages/compiler/src/index.js';
import { compileExpression } from '../packages/authorization/src/expressions.js';
import type { Value } from '../packages/contracts/types.js';
import { database } from './fixtures/catalog-storage/database.js';

import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { ObjectStore, type ObjectSchema } from '../services/ontology/src/store.js';
import { IcebergIndexer } from '../services/ontology/src/indexer.js';

test(
  'native effective projections build off-lock, replay concurrent edits and activate with source generations',
  { timeout: 300000 },
  async (t) => {
    const db = await database();
    const processes: ChildProcess[] = [];
    t.after(async () => {
      for (const child of processes) child.kill('SIGKILL');
      await db.close();
    });
    const environment = randomUUID(),
      source = randomUUID(),
      target = randomUUID(),
      root = join(db.root, 'objects'),
      scratch = join(db.root, 'scratch'),
      physical = join(db.root, 'fixture');
    for (const path of [root, scratch, physical]) await mkdir(path);
    await db.catalog.ensureEnvironment(environment);
    await db.catalog.ensureDataset(environment, source);
    const authority = new Authority(db.catalog),
      sessions = new WorkloadSessions(authority),
      operations = [
        `${source}:read`,
        `${source}:write`,
        `${target}:index`,
        `${target}:read`,
        'edit:invoke',
        'clear:invoke',
        'delete:invoke',
        'restore:invoke',
        'move:invoke',
      ];
    await authority.syncPrincipal(environment, 'fixture', 'pipeline', {
      enabled: true,
      kind: 'workload',
      attributes: { admin: true, tenant: 'a' },
      markings: ['private'],
    });
    await authority.setApplication(environment, 'fixture', 'native-indexing', operations);
    await authority.setGrants(environment, 'fixture', 'pipeline', 'native-indexing', operations);
    await authority.syncPrincipal(environment, 'fixture', 'reader', {
      enabled: true,
      kind: 'workload',
      attributes: { admin: false, tenant: 'a' },
      markings: ['private'],
    });
    await authority.setGrants(environment, 'fixture', 'reader', 'native-indexing', [
      `${target}:read`,
    ]);
    const readerCredential = await sessions.mint(
      { environment, principal: 'reader', application: 'native-indexing' },
      'fixture',
    );
    const context = { environment, principal: 'pipeline', application: 'native-indexing' },
      credential = await sessions.mint(context, 'fixture'),
      session = {
        ...context,
        sessionHash: createHash('sha256').update(credential.token).digest('hex'),
      };
    const generated = JSON.parse(
      (
        await promisify(execFile)(resolve('.venv/bin/python'), [
          'tests/fixtures/iceberg-scoped-storage/generate.py',
          physical,
          '--projection-snapshot',
          'moria://warehouse/table',
        ])
      ).stdout,
    );
    const metadata = 'moria://warehouse/table/metadata/root.metadata.json';
    const snapshot = {
      ...generated,
      metadata,
      objects: { ...generated.objects, [metadata]: generated.metadata },
      resource: source,
      warehouseId: 'warehouse',
      tableId: 'table',
    };
    await db.pool.query(
      "INSERT INTO moria.native_storage_namespaces(environment,warehouse_id,table_id,resource,revision,purpose,iceberg_uuid,markings) VALUES($1,'warehouse','table',$2,'v1','dataset-source',$3,ARRAY['private'])",
      [environment, source, generated.tableUuid],
    );
    const role = `snapshot_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(`ALTER ROLE ${role} SET temp_file_limit='256MB'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role}; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    const connection = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const namespace = { warehouseId: 'warehouse', tableId: 'table', revision: 'v1' },
      limits = { maxObjectBytes: 128 * 1024 * 1024, chunkBytes: 1024 * 1024 };
    const posix = { namespace, provider: { kind: 'posix', root, limits } };
    let binding = randomUUID();
    const native = async (
      mode: string,
      bindings: unknown[] = [posix],
      observe?: (child: ChildProcess, chunk: string) => void,
    ) => {
      const invocation = promisify(execFile)(
        process.env.MORIA_CATALOG_TEST_BINARY ?? '/usr/local/bin/moria-catalog-tests',
        ['--ignored', '--exact', 'catalog_publication', '--nocapture', '--test-threads=1'],
        {
          timeout: 120000,
          maxBuffer: 1024 * 1024,
          env: {
            PATH: process.env.PATH,
            MORIA_INDEXING_DATABASE_SECRET_REF: 'SNAPSHOT_DATABASE',
            SNAPSHOT_DATABASE: connection,
            MORIA_INDEXING_DATABASE_TLS: 'disable',
            MORIA_NATIVE_CATALOG_FIXTURE: JSON.stringify({
              root,
              token: credential.token,
              binding,
              mode,
              snapshots: [snapshot],
            }),
            MORIA_NATIVE_STORAGE_BINDINGS: JSON.stringify(bindings),
            MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT: scratch,
            MORIA_NATIVE_CLOSURE_LIMITS: JSON.stringify({
              maxEntries: 10000,
              maxReferencedBytes: 1024 * 1024 * 1024,
              maxSpoolBytes: 32 * 1024 * 1024,
            }),
            AWS_ACCESS_KEY_ID: 'wrong-ambient-key',
            AWS_SECRET_ACCESS_KEY: 'wrong-ambient-secret',
            AWS_ENDPOINT_URL: 'http://127.0.0.1:1',
          },
        },
      );
      processes.push(invocation.child);
      if (observe)
        invocation.child.stdout!.on('data', (bytes) => observe(invocation.child, bytes.toString()));
      const result = await invocation;
      assert.match(result.stdout, /test result: ok\. 1 passed; 0 failed; 0 ignored;/);
      return result.stdout;
    };
    const publish = async () => {
      const head = (await db.catalog.heads(environment, [source]))[0],
        epoch = (await authority.current(environment, session.principal, session.application))
          .epoch;
      const spec = {
        version: 1,
        domain: 'sync',
        release: 'a'.repeat(64),
        resourceClass: 'standard',
        timeoutSeconds: 240,
        retryPolicy: 'idempotent',
        inputSchema: { type: 'object', properties: {} },
        pinnedInputs: [],
        expectedHeads: { [source]: head.revision },
      };
      binding = (
        await db.pool.query(
          `INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation)
      SELECT $1,$2,$3,$4,$5,ARRAY['private'],$6,i.security_epoch,c.incarnation FROM moria.instance_state i,moria.indexing_instance c RETURNING id`,
          [environment, source, spec, session, [`${source}:read`, `${source}:write`], epoch],
        )
      ).rows[0].id;
      await native('valid');
      return (await db.catalog.heads(environment, [source]))[0];
    };
    await publish();
    const schema: ObjectSchema = {
      primaryKey: 'uid',
      properties: {
        uid: { type: 'uid', nullable: false },
        value: { type: 'string', nullable: true },
        id: { type: 'int64', nullable: false },
        tenant: { type: 'string', nullable: false },
        enabled: { type: 'boolean', nullable: true },
        large: { type: 'int64', nullable: true },
        amount: { type: 'decimal', nullable: true, precision: 20, scale: 4 },
        day: { type: 'date', nullable: true },
        at: { type: 'timestamp', nullable: true },
        ratio: { type: 'float64', nullable: true },
        optional: { type: 'string', nullable: true },
        tags: { type: 'array', nullable: true, items: { type: 'string', nullable: false } },
      },
    };
    const store = new ObjectStore(db.catalog),
      indexer = new IcebergIndexer(store, authority, {
        source: { kind: 'dataset', resource: source, markings: ['private'] },
        target: { resource: target, markings: ['private'], schema },
      });
    for (let i = 0; i < 300; i++)
      await db.pool.query(
        'INSERT INTO moria.identities(domain,canonical_key,uid) VALUES($1,$2,$3)',
        [environment, String(i), `${i.toString(16).padStart(8, '0')}-0000-4000-8000-000000000000`],
      );
    await indexer.registerDistributed(session, {
      targetId: target,
      warehouse: root,
      partitions: 4,
    });
    const generations = async () =>
      Number(
        (
          await db.pool.query(
            'SELECT count(*) FROM moria.source_generations WHERE environment=$1',
            [environment],
          )
        ).rows[0].count,
      );
    await native('index');
    assert.equal(await generations(), 1);
    const sourceHead = (await db.catalog.heads(environment, [source]))[0];
    const initialGeneration = (
      await db.pool.query(
        'SELECT stage,revision FROM moria.source_generations WHERE environment=$1 ORDER BY revision DESC LIMIT 1',
        [environment],
      )
    ).rows[0];
    const original: { uid: string; value: Record<string, Value> }[] = (
      await db.pool.query(
        'SELECT uid,value FROM moria.staged_source_rows WHERE stage=$1 ORDER BY uid',
        [initialGeneration.stage],
      )
    ).rows;
    const servingDefinition = {
      objectTypeId: target,
      schema,
      policies: {
        resource: target,
        markings: ['private'],
        properties: schema.properties,
        sourceColumns: schema.properties,
        attributes: {
          admin: { type: 'boolean', nullable: false },
          tenant: schema.properties.tenant,
        },
        row: {
          op: 'or',
          args: [
            { principalAttribute: 'admin' },
            { op: 'eq', args: [{ field: 'tenant' }, { principalAttribute: 'tenant' }] },
          ],
        },
        propertyRead: { optional: { principalAttribute: 'admin' } },
        objectRead: {
          op: 'or',
          args: [
            { principalAttribute: 'admin' },
            { op: 'eq', args: [{ field: 'tenant' }, { principalAttribute: 'tenant' }] },
          ],
        },
      },
    };
    await db.pool.query(
      'INSERT INTO moria.native_serving_bindings(environment,resource,object_type_id,revision,definition) VALUES($1,$2,$2::text::uuid,$3,$4)',
      [environment, target, fingerprint(servingDefinition), servingDefinition],
    );
    for (const [name, edits] of [
      ['edit', [{ op: 'set', property: 'value', value: { parameter: 'value' } }]],
      ['clear', [{ op: 'clearOverride', property: 'value' }]],
      ['delete', [{ op: 'delete' }]],
      ['restore', [{ op: 'restore' }]],
      ['move', [{ op: 'set', property: 'tenant', value: { parameter: 'tenant' } }]],
    ] as const) {
      const definition = {
        resource: name,
        revision: 'v1',
        markings: ['private'],
        targetParameter: 'uid',
        inputs: {
          uid: schema.properties.uid,
          ...(name === 'edit' ? { value: schema.properties.value } : {}),
          ...(name === 'move' ? { tenant: schema.properties.tenant } : {}),
        },
        condition: { literal: true },
        edits,
      };
      await db.pool.query(
        'INSERT INTO moria.native_serving_actions(environment,resource,object_resource,revision,definition) VALUES($1,$2,$3,$4,$5)',
        [environment, name, target, 'v1', definition],
      );
    }
    const server = spawn(
      process.env.MORIA_RUNTIME_BINARY ??
        process.env.MORIA_INDEXING_BINARY ??
        '/usr/local/bin/moria-indexing',
      ['serve'],
      {
        env: {
          PATH: process.env.PATH,
          MORIA_INDEXING_DATABASE_SECRET_REF: 'PROJECTION_DATABASE',
          PROJECTION_DATABASE: connection,
          MORIA_INDEXING_DATABASE_TLS: 'disable',
          MORIA_NATIVE_LISTEN: '127.0.0.1:0',
        },
        stdio: ['ignore', 'pipe', 'pipe'],
      },
    );
    processes.push(server);
    let output = '',
      errors = '',
      base = '';
    server.stdout.on('data', (b) => {
      output += b.toString();
    });
    server.stderr.on('data', (b) => {
      errors += b.toString();
    });
    for (let i = 0; i < 500 && !base; i++) {
      for (const line of output.split('\n')) {
        try {
          const e = JSON.parse(line);
          if (e.event === 'serving') base = `http://${e.address}`;
        } catch {}
      }
      if (server.exitCode !== null) throw new Error(errors);
      if (!base) await delay(10);
    }
    assert.ok(base, errors);
    const invoke = async (action: string, uid: string, value?: string | null) => {
      const reply = await fetch(`${base}/api/v1/actions/${action}/invoke`, {
        method: 'POST',
        headers: {
          authorization: `Bearer ${credential.token}`,
          'content-type': 'application/json',
          'idempotency-key': randomUUID(),
        },
        body: JSON.stringify({
          input: {
            uid,
            ...(action === 'edit' ? { value } : {}),
            ...(action === 'move' ? { tenant: value } : {}),
          },
        }),
      });
      const body = await reply.json();
      assert.equal(reply.status, 200, JSON.stringify(body));
      return body;
    };
    const query = async (input: unknown, token = readerCredential.token, status = 200) => {
      const response = await fetch(`${base}/api/v1/objects/${target}/query`, {
        method: 'POST',
        headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
        body: JSON.stringify(input),
      });
      const body = await response.json();
      assert.equal(response.status, status, JSON.stringify(body));
      return body;
    };
    const layouts = [
      { name: 'uid', sort: [], keyBytes: 256 },
      ...['value', 'enabled', 'large', 'amount', 'day', 'at', 'ratio'].map((property) => ({
        name: property,
        sort: [{ property, direction: 'asc' }],
        keyBytes: 1024,
      })),
      { name: 'valueDesc', sort: [{ property: 'value', direction: 'desc' }], keyBytes: 1024 },
    ];
    let projectionDefinition = {
      version: 1,
      sourceFields: Object.keys(schema.properties),
      effectiveFields: Object.keys(schema.properties),
      layouts,
      readerRoles: [role],
      budgets: {
        rowBytes: 64 * 1024,
        batchRows: 1,
        batchBytes: 128 * 1024,
        commitReplayRows: 2,
        commitReplayBytes: 128 * 1024,
        ddlTimeMs: 30000,
      },
    };
    const install = async (revision: string) => {
      await db.pool.query(
        'INSERT INTO moria.runtime_projection_bindings(environment,resource,revision,definition) VALUES($1,$2,$3,$4) ON CONFLICT(environment,resource) DO UPDATE SET revision=EXCLUDED.revision,definition=EXCLUDED.definition',
        [environment, target, revision, projectionDefinition],
      );
      await indexer.registerDistributed(session, {
        targetId: target,
        warehouse: root,
        partitions: 4,
      });
    };
    let relation = '',
      projection: any;
    await t.test(
      'off-lock paged build replays an actual concurrent native action before atomic activation',
      async () => {
        await install('p1');
        const expected = (
          await db.pool.query(
            'SELECT definition FROM moria.indexing_bindings WHERE environment=$1 AND target=$2',
            [environment, target],
          )
        ).rows[0].definition;
        assert.equal(expected.projectionRevision, 'p1');
        let paused = false,
          release!: () => void;
        const stopped = new Promise<void>((resolve) => {
          release = resolve;
        });
        const indexing = native('index', [posix], (child, chunk) => {
          if (!paused && chunk.includes('"event":"projection_page"')) {
            paused = true;
            child.kill('SIGSTOP');
            release();
          }
        });
        await Promise.race([
          stopped,
          indexing.then(() => {
            throw new Error('projection completed without paging');
          }),
        ]);
        const building = (
          await db.pool.query(
            "SELECT id,build_after,status FROM moria.runtime_projections WHERE environment=$1 AND status='building'",
            [environment],
          )
        ).rows[0];
        assert.ok(building?.build_after);
        assert.equal(await generations(), 1);
        assert.equal(
          (
            await db.pool.query(
              "SELECT count(*) FROM moria.runtime_projections WHERE environment=$1 AND status='active'",
              [environment],
            )
          ).rows[0].count,
          '0',
        );
        await invoke('edit', original[0].uid, 'concurrent-native-edit');
        for (const child of processes)
          if (child.pid && child !== server && child.exitCode === null) child.kill('SIGCONT');
        await indexing;
        assert.equal(await generations(), 2);
        assert.equal(
          (await db.catalog.heads(environment, [source]))[0].revision,
          sourceHead.revision,
        );
        projection = (
          await db.pool.query(
            "SELECT * FROM moria.runtime_projections WHERE environment=$1 AND status='active'",
            [environment],
          )
        ).rows[0];
        relation = `moria."runtime_projection_${projection.id.replaceAll('-', '')}"`;
        assert.equal(projection.row_count, '300');
        assert.equal(projection.indexes_complete, true);
        assert.equal(
          (
            await db.pool.query(
              `SELECT effective->>'value' value FROM ${relation} WHERE uid=$1 AND to_revision IS NULL`,
              [original[0].uid],
            )
          ).rows[0].value,
          'concurrent-native-edit',
        );
        const generation = (
          await db.pool.query(
            'SELECT revision,stage FROM moria.source_generations WHERE environment=$1 ORDER BY revision DESC LIMIT 1',
            [environment],
          )
        ).rows[0];
        assert.equal(projection.activation_revision, generation.revision);
        assert.equal(projection.stage, generation.stage);
        const owner = (
          await db.pool.query(
            'SELECT pg_get_userbyid(relowner) AS owner FROM pg_class WHERE oid=$1::regclass',
            [relation],
          )
        ).rows[0].owner;
        assert.notEqual(owner, role);
        assert.equal(
          (
            await db.pool.query("SELECT has_schema_privilege($1,'moria','CREATE') AS create", [
              role,
            ])
          ).rows[0].create,
          false,
        );
      },
    );
    await t.test(
      'physical primary UID checks reject missing or different values and protected UID queries seek canonical UID',
      async () => {
        for (const expression of [
          "CASE WHEN $2::uuid IS NOT NULL THEN effective-'uid' ELSE effective END",
          "jsonb_set(effective,'{uid}',to_jsonb($2::text))",
        ]) {
          await assert.rejects(
            db.pool.query(
              `INSERT INTO ${relation}(uid,from_revision,to_revision,deleted,source,effective)
               SELECT $1::uuid,from_revision,to_revision,deleted,source,${expression} FROM ${relation}
               WHERE uid=$3 AND to_revision IS NULL`,
              [randomUUID(), randomUUID(), original[0].uid],
            ),
            (error: any) => error.code === '23514',
          );
        }
        const ordered = original.map((row) => row.uid).sort();
        const anchor = ordered[150];
        const exact = await query(
          {
            filter: { op: 'eq', args: [{ field: 'uid' }, { literal: anchor }] },
            projection: ['uid'],
          },
          credential.token,
        );
        assert.deepEqual(
          exact.objects.map((row: { uid: string }) => row.uid),
          [anchor],
        );
        for (const direction of ['asc', 'desc']) {
          const expected = ordered.filter((uid) => uid >= anchor);
          if (direction === 'desc') expected.reverse();
          const page = await query(
            {
              filter: { op: 'gte', args: [{ field: 'uid' }, { literal: anchor }] },
              sort: [{ property: 'uid', direction }],
              pageSize: 37,
              projection: ['uid'],
            },
            credential.token,
          );
          assert.deepEqual(
            page.objects.map((row: { uid: string }) => row.uid),
            expected.slice(0, 37),
          );
        }
        const plan = (
          await db.pool.query(
            `EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT p.uid FROM ${relation} p
           WHERE p.uid >= $1 AND p.to_revision IS NULL AND NOT p.deleted ORDER BY p.uid LIMIT 2`,
            [ordered[ordered.length - 5]],
          )
        ).rows[0]['QUERY PLAN'];
        t.diagnostic(`canonical UID seek plan: ${JSON.stringify(plan)}`);
        assert.match(JSON.stringify(plan), /Index Cond[^}]*uid/);
      },
    );
    await t.test(
      'strict materialized scalar codecs and ordering agree with C03 including astral strings and exact int64',
      async () => {
        await invoke('clear', original[0].uid);
        for (const selected of original.slice(0, 12)) {
          const response = await fetch(`${base}/api/v1/objects/${target}/${selected.uid}`, {
            headers: { authorization: `Bearer ${credential.token}` },
          });
          const point = await response.json();
          assert.equal(response.status, 200, JSON.stringify(point));
          const page = await query(
            {
              projection: Object.keys(schema.properties),
              filter: { op: 'eq', args: [{ field: 'uid' }, { literal: selected.uid }] },
              pageSize: 2,
            },
            credential.token,
          );
          assert.deepEqual(
            page.objects,
            [point.object],
            'Protected typed projection preserves all exact scalar/array/null values against independent point read',
          );
        }
        const expectedFloat = [...original]
          .sort((left, right) => {
            const a = left.value.ratio as number | null,
              b = right.value.ratio as number | null;
            if (a === null || b === null)
              return a === b ? left.uid.localeCompare(right.uid) : a === null ? 1 : -1;
            return (a === b ? 0 : a > b ? -1 : 1) || left.uid.localeCompare(right.uid);
          })
          .map((row) => row.uid);
        const floatIds: string[] = [];
        let floatCursor: string | undefined;
        do {
          const page = await query(
            {
              projection: ['uid'],
              sort: [{ property: 'ratio', direction: 'desc' }],
              pageSize: 37,
              ...(floatCursor ? { cursor: floatCursor } : {}),
            },
            credential.token,
          );
          floatIds.push(...page.objects.map((row: { uid: string }) => row.uid));
          floatCursor = page.nextCursor;
        } while (floatCursor);
        assert.deepEqual(
          floatIds,
          expectedFloat,
          'Typed integral float64 sort keys remain canonical across durable cursor pages',
        );
        for (const [field, descriptor] of Object.entries(schema.properties)) {
          const column = projection.columns[field].effective;
          if (descriptor.type === 'array') {
            assert.deepEqual(
              (
                await db.pool.query(
                  `SELECT ${column} AS value FROM ${relation} WHERE uid=$1 AND to_revision IS NULL`,
                  [original[0].uid],
                )
              ).rows[0].value,
              ['one', 'two'],
            );
            continue;
          }
          const comparisonType =
            descriptor.type === 'uid'
              ? { type: 'string' as const, nullable: descriptor.nullable }
              : descriptor.type === 'boolean'
                ? { type: 'int64' as const, nullable: descriptor.nullable }
                : descriptor;
          const less = compileExpression(
            { op: 'lt', args: [{ parameter: 'a' }, { parameter: 'b' }] },
            { parameters: { a: comparisonType, b: comparisonType } },
          );
          const greater = compileExpression(
            { op: 'gt', args: [{ parameter: 'a' }, { parameter: 'b' }] },
            { parameters: { a: comparisonType, b: comparisonType } },
          );
          for (const direction of ['ASC', 'DESC']) {
            const expected: string[] = [...original]
              .sort((left, right) => {
                const a = left.value[field],
                  b = right.value[field];
                const parameters: Record<string, Value> =
                  descriptor.type === 'boolean'
                    ? { a: a === null ? null : a ? '1' : '0', b: b === null ? null : b ? '1' : '0' }
                    : { a, b };
                let compared: number =
                  a === null || b === null
                    ? a === b
                      ? 0
                      : a === null
                        ? 1
                        : -1
                    : less.allows({ parameters })
                      ? -1
                      : greater.allows({ parameters })
                        ? 1
                        : 0;
                if (a !== null && b !== null && direction === 'DESC') compared = -compared;
                return compared || left.uid.localeCompare(right.uid);
              })
              .map((row) => row.uid);
            const actual = (
              await db.pool.query(
                `SELECT uid FROM ${relation} WHERE NOT deleted AND to_revision IS NULL ORDER BY ${column} ${direction} NULLS LAST,uid`,
              )
            ).rows.map((row) => row.uid);
            assert.deepEqual(actual, expected, `${field} ${direction}`);
          }
        }
        assert.equal(
          (
            await db.pool.query(
              `SELECT ${projection.columns.large.effective}::text AS value FROM ${relation} WHERE uid=$1 AND to_revision IS NULL`,
              [original[0].uid],
            )
          ).rows[0].value,
          '9007199254740993',
        );
        await assert.rejects(
          db.pool.query("SELECT moria.runtime_projection_timestamp('2026-01-01T24:00:00Z')"),
          /VALUE_INVALID/,
        );
        await assert.rejects(db.pool.query("SELECT moria.runtime_projection_date('2026-02-30')"));
      },
    );
    await t.test(
      'current actions maintain typed history atomically and exact overflow remains present',
      async () => {
        const uid = original[1].uid,
          col = projection.columns.value.effective;
        const previous = (
          await db.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [environment])
        ).rows[0].revision;
        await invoke('edit', uid, 'after-activation');
        const history = (
          await db.pool.query(
            `SELECT from_revision,to_revision,${col} AS value FROM ${relation} WHERE uid=$1 ORDER BY from_revision`,
            [uid],
          )
        ).rows;
        assert.equal(history.at(-1).value, 'after-activation');
        assert.equal(history.at(-2).to_revision, history.at(-1).from_revision);
        assert.equal(
          (
            await db.pool.query(
              `SELECT ${col} AS value FROM ${relation} WHERE uid=$1 AND from_revision<=$2 AND (to_revision IS NULL OR to_revision>$2)`,
              [uid, previous],
            )
          ).rows[0].value,
          '',
        );
        await invoke('delete', uid);
        assert.equal(
          (
            await db.pool.query(
              `SELECT deleted FROM ${relation} WHERE uid=$1 AND to_revision IS NULL`,
              [uid],
            )
          ).rows[0].deleted,
          true,
        );
        await invoke('restore', uid);
        await invoke('clear', uid);
        assert.equal(
          (
            await db.pool.query(
              `SELECT ${col} AS value,deleted FROM ${relation} WHERE uid=$1 AND to_revision IS NULL`,
              [uid],
            )
          ).rows[0].deleted,
          false,
        );
        const k = 'k1';
        assert.equal(
          (
            await db.pool.query(
              `SELECT count(*) FROM ${relation} WHERE NOT ${k} AND NOT deleted AND to_revision IS NULL`,
            )
          ).rows[0].count,
          '1',
        );
        const tenant = projection.columns.tenant.effective;
        const exact = (
          await db.pool.query(
            `SELECT uid FROM ${relation} WHERE NOT deleted AND to_revision IS NULL AND ${tenant}='b' ORDER BY ${col} ASC NULLS LAST,uid LIMIT 300`,
          )
        ).rows;
        const combined = (
          await db.pool.query(
            `WITH normal AS (SELECT uid,${col} AS key FROM ${relation} WHERE ${k} AND NOT deleted AND to_revision IS NULL AND ${tenant}='b' ORDER BY ${col} ASC NULLS LAST,uid LIMIT 300),overflow AS (SELECT uid,${col} AS key FROM ${relation} WHERE NOT ${k} AND NOT deleted AND to_revision IS NULL AND ${tenant}='b' ORDER BY ${col} ASC NULLS LAST,uid LIMIT 300) SELECT uid FROM (SELECT * FROM normal UNION ALL SELECT * FROM overflow) q ORDER BY key ASC NULLS LAST,uid LIMIT 300`,
          )
        ).rows;
        assert.deepEqual(combined, exact);
        assert.ok(combined.some((row) => row.uid === original[7].uid));
        const plan = (
          await db.pool.query(
            `EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT uid FROM ${relation} WHERE ${k} AND NOT deleted AND to_revision IS NULL ORDER BY ${col} ASC NULLS LAST,uid LIMIT 10`,
          )
        ).rows[0]['QUERY PLAN'];
        t.diagnostic(`300-row projection plan: ${JSON.stringify(plan)}`);
        assert.match(
          JSON.stringify(plan),
          new RegExp(`runtime_pi_${projection.id.replaceAll('-', '')}_1_c`),
        );
      },
    );
    await t.test(
      'layout-only revision rebuild retains Dataset head, UID population, edits and old projection pins',
      async () => {
        await invoke('edit', original[2].uid, 'survives-layout-change');
        const old = projection,
          oldRelation = relation;
        projectionDefinition = {
          ...projectionDefinition,
          budgets: { ...projectionDefinition.budgets, batchRows: 100 },
          layouts: [
            ...layouts,
            {
              name: 'composite',
              sort: [
                { property: 'enabled', direction: 'asc' },
                { property: 'large', direction: 'desc' },
              ],
              keyBytes: 1024,
            },
          ],
        };
        await install('p2');
        await Promise.all([native('index'), native('index')]);
        projection = (
          await db.pool.query(
            "SELECT * FROM moria.runtime_projections WHERE environment=$1 AND status='active'",
            [environment],
          )
        ).rows[0];
        relation = `moria."runtime_projection_${projection.id.replaceAll('-', '')}"`;
        assert.notEqual(projection.id, old.id);
        assert.equal(projection.binding_revision, 'p2');
        assert.equal(
          (await db.catalog.heads(environment, [source]))[0].revision,
          sourceHead.revision,
        );
        assert.equal(
          (
            await db.pool.query(
              `SELECT effective->>'value' value FROM ${relation} WHERE uid=$1 AND to_revision IS NULL`,
              [original[2].uid],
            )
          ).rows[0].value,
          'survives-layout-change',
        );
        assert.deepEqual(
          (
            await db.pool.query(
              `SELECT uid FROM ${relation} WHERE to_revision IS NULL EXCEPT SELECT uid FROM ${oldRelation} WHERE to_revision IS NULL`,
            )
          ).rows,
          [],
        );
        const retained = (
          await db.pool.query(
            'SELECT status,closed_revision FROM moria.runtime_projections WHERE id=$1',
            [old.id],
          )
        ).rows[0];
        assert.equal(retained.status, 'retired');
        assert.equal(retained.closed_revision, projection.activation_revision);
        assert.equal(
          (
            await db.pool.query(
              `SELECT count(*) FROM ${oldRelation} WHERE from_revision<=$1 AND (to_revision IS NULL OR to_revision>$1)`,
              [old.activation_revision],
            )
          ).rows[0].count,
          '300',
        );
        await assert.rejects(install('p1'), /PROJECTION_REVISION_REUSED/);
        await assert.rejects(
          db.pool.query(
            "UPDATE moria.runtime_projection_bindings SET definition=jsonb_set(definition,'{version}','2') WHERE environment=$1",
            [environment],
          ),
          /FRESH_PROJECTION_REVISION_REQUIRED/,
        );
      },
    );
    await t.test(
      'expired coordinators resume durable bounded pages under a new fence and cannot overwrite replacement activation',
      async () => {
        projectionDefinition = {
          ...projectionDefinition,
          budgets: { ...projectionDefinition.budgets, batchRows: 1 },
        };
        await install('p3');
        let paused = false,
          release!: () => void,
          child: ChildProcess | undefined;
        const stopped = new Promise<void>((resolve) => {
          release = resolve;
        });
        const old = native('index', [posix], (running, chunk) => {
          if (!paused && chunk.includes('"event":"projection_page"')) {
            paused = true;
            child = running;
            running.kill('SIGSTOP');
            release();
          }
        });
        await Promise.race([
          stopped,
          old.then(() => {
            throw new Error('old coordinator completed before durable page takeover');
          }),
        ]);
        const partial = (
          await db.pool.query(
            "SELECT id,stage,owner,fence,build_after FROM moria.runtime_projections WHERE environment=$1 AND status='building'",
            [environment],
          )
        ).rows[0];
        assert.ok(partial.build_after);
        await db.pool.query(
          "UPDATE moria.indexing_builds SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
          [partial.stage],
        );
        await native('index');
        const replacement = (
          await db.pool.query(
            'SELECT owner,fence,status FROM moria.runtime_projections WHERE id=$1',
            [partial.id],
          )
        ).rows[0];
        assert.equal(replacement.status, 'active');
        assert.notEqual(replacement.owner, partial.owner);
        assert.ok(BigInt(replacement.fence) > BigInt(partial.fence));
        const count = await generations();
        child!.kill('SIGCONT');
        const transcript = await old;
        assert.doesNotMatch(transcript, /"event":"active"/);
        assert.equal(await generations(), count);
        assert.deepEqual(
          (
            await db.pool.query(
              'SELECT owner,fence,status FROM moria.runtime_projections WHERE id=$1',
              [partial.id],
            )
          ).rows[0],
          replacement,
        );
      },
    );
    await t.test(
      'a publication tail beyond the barrier budget releases the environment lock and resumes off-lock',
      async () => {
        projectionDefinition = {
          ...projectionDefinition,
          budgets: { ...projectionDefinition.budgets, batchRows: 100 },
        };
        await install('p4');
        const before = await generations();
        let paused = false,
          release!: () => void,
          child: ChildProcess | undefined;
        const stopped = new Promise<void>((resolve) => {
          release = resolve;
        });
        const indexing = native('index', [posix], (running, chunk) => {
          if (!paused && chunk.includes('"event":"projection_ready"')) {
            paused = true;
            child = running;
            running.kill('SIGSTOP');
            release();
          }
        });
        await Promise.race([
          stopped,
          indexing.then(() => {
            throw new Error('projection published without final preparation event');
          }),
        ]);
        assert.equal(await generations(), before);
        for (let i = 10; i < 15; i++) await invoke('edit', original[i].uid, `barrier-tail-${i}`);
        child!.kill('SIGCONT');
        const transcript = await indexing;
        assert.match(transcript, /"event":"projection_retry"/);
        assert.equal(await generations(), before + 1);
        const active = (
          await db.pool.query(
            "SELECT * FROM moria.runtime_projections WHERE environment=$1 AND status='active'",
            [environment],
          )
        ).rows[0];
        const table = `moria."runtime_projection_${active.id.replaceAll('-', '')}"`;
        for (let i = 10; i < 15; i++)
          assert.equal(
            (
              await db.pool.query(
                `SELECT effective->>'value' value FROM ${table} WHERE uid=$1 AND to_revision IS NULL`,
                [original[i].uid],
              )
            ).rows[0].value,
            `barrier-tail-${i}`,
          );
      },
    );
    await t.test(
      'a changed layout anchor cannot be adopted by an in-flight build and requires a fresh immutable indexing binding',
      async () => {
        projectionDefinition = {
          ...projectionDefinition,
          budgets: { ...projectionDefinition.budgets, batchRows: 1 },
        };
        await install('p5');
        const before = await generations();
        let paused = false,
          release!: () => void,
          child: ChildProcess | undefined;
        const stopped = new Promise<void>((resolve) => {
          release = resolve;
        });
        const old = native('index', [posix], (running, chunk) => {
          if (!paused && chunk.includes('"event":"projection_page"')) {
            paused = true;
            child = running;
            running.kill('SIGSTOP');
            release();
          }
        });
        await Promise.race([
          stopped,
          old.then(() => {
            throw new Error('old layout completed before replacement');
          }),
        ]);
        await db.pool.query(
          "UPDATE moria.runtime_projection_bindings SET revision='p6' WHERE environment=$1 AND resource=$2",
          [environment, target],
        );
        child!.kill('SIGCONT');
        await assert.rejects(old, /INDEXING_INCOMPLETE/);
        assert.equal(await generations(), before);
        projectionDefinition = {
          ...projectionDefinition,
          budgets: { ...projectionDefinition.budgets, batchRows: 100 },
        };
        await install('p7');
        await native('index');
        assert.equal(await generations(), before + 1);
        const active = (
          await db.pool.query(
            "SELECT binding_revision FROM moria.runtime_projections WHERE environment=$1 AND status='active'",
            [environment],
          )
        ).rows[0];
        assert.equal(active.binding_revision, 'p7');
      },
    );
    await t.test(
      'protected whole-population HTTP queries use exact indexed/overflow order and bounded engine fallback',
      async () => {
        const canonical = async () =>
          (
            await db.pool.query(
              `WITH generation AS (SELECT stage FROM moria.source_generations WHERE environment=$1 AND object_type=$2 ORDER BY revision DESC LIMIT 1) SELECT r.uid,r.value||COALESCE(o.overrides,'{}') AS value FROM generation g JOIN moria.staged_source_rows r ON r.stage=g.stage LEFT JOIN LATERAL (SELECT overrides,tombstone FROM moria.overlay_versions WHERE environment=$1 AND object_type=$2 AND uid=r.uid ORDER BY revision DESC LIMIT 1) o ON true WHERE NOT COALESCE(o.tombstone,false) AND r.value->>'tenant'='a' AND (r.value||COALESCE(o.overrides,'{}'))->>'tenant'='a'`,
              [environment, target],
            )
          ).rows;
        for (const sort of [
          [{ property: 'value', direction: 'asc' }],
          [
            { property: 'large', direction: 'asc' },
            { property: 'value', direction: 'desc' },
          ],
        ]) {
          const before = await canonical();
          const expected = before
            .sort((left, right) => {
              for (const key of sort) {
                const a = left.value[key.property],
                  b = right.value[key.property];
                if (a === b) continue;
                if (a === null) return 1;
                if (b === null) return -1;
                const compared =
                  key.property === 'large'
                    ? BigInt(a) < BigInt(b)
                      ? -1
                      : 1
                    : Buffer.compare(Buffer.from(a), Buffer.from(b));
                if (compared) return key.direction === 'asc' ? compared : -compared;
              }
              return left.uid.localeCompare(right.uid);
            })
            .map((row) => row.uid);
          const input = { sort, pageSize: 37, projection: ['uid', 'value', 'tenant', 'large'] };
          const ids: string[] = [];
          let cursor: string | undefined;
          do {
            const page = await query({ ...input, ...(cursor ? { cursor } : {}) });
            ids.push(...page.objects.map((row: { uid: string }) => row.uid));
            assert.ok(page.objects.every((row: { tenant: string }) => row.tenant === 'a'));
            cursor = page.nextCursor;
          } while (cursor);
          assert.deepEqual(ids, expected);
        }
        const admin = await query(
          {
            sort: [{ property: 'value', direction: 'asc' }],
            pageSize: 1000,
            projection: ['uid', 'value'],
          },
          credential.token,
        );
        assert.ok(
          admin.objects.some((row: { uid: string }) => row.uid === original[7].uid),
          'oversized sort key must remain in native HTTP result',
        );
        assert.equal(
          (
            await query(
              { sort: [{ property: 'optional', direction: 'asc' }], filter: { literal: false } },
              readerCredential.token,
              403,
            )
          ).error.code,
          'PROPERTY_FORBIDDEN',
        );
      },
    );
    await t.test(
      'protected typed projection aggregation includes overflow and current overlays with exact authorized groups',
      async () => {
        const aggregate = async (input: unknown, token = readerCredential.token, status = 200) => {
          const response = await fetch(`${base}/api/v1/objects/${target}/aggregate`, {
            method: 'POST',
            headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
            body: JSON.stringify(input),
          });
          const body = await response.json();
          assert.equal(response.status, status, JSON.stringify(body));
          return body;
        };
        const budget = { scanRows: 1000, groups: 16, responseBytes: 1024 * 1024, timeMs: 30000 };
        const canonical = async (admin: boolean) =>
          (
            await db.pool.query(
              `WITH generation AS(SELECT stage FROM moria.source_generations WHERE environment=$1 AND object_type=$2 ORDER BY revision DESC LIMIT 1)
          SELECT r.uid,r.value AS source,r.value||COALESCE(o.overrides,'{}') AS value FROM generation g JOIN moria.staged_source_rows r ON r.stage=g.stage
          LEFT JOIN LATERAL(SELECT overrides,tombstone FROM moria.overlay_versions WHERE environment=$1 AND object_type=$2 AND uid=r.uid ORDER BY revision DESC LIMIT 1)o ON true
          WHERE NOT COALESCE(o.tombstone,false) AND ($3::boolean OR(r.value->>'tenant'='a' AND(r.value||COALESCE(o.overrides,'{}'))->>'tenant'='a'))`,
              [environment, target, admin],
            )
          ).rows;
        const decimalUnits = (amount: string) => {
          const negative = amount.startsWith('-');
          const [whole, fraction = ''] = (negative ? amount.slice(1) : amount).split('.');
          const magnitude = BigInt(whole) * 10000n + BigInt(fraction.padEnd(4, '0'));
          return negative ? -magnitude : magnitude;
        };
        const grouped = (rows: { value: Record<string, any> }[], sum = false) => {
          const groups = new Map<
            string,
            { count: bigint; minimum: bigint; maximum: bigint; total: bigint }
          >();
          for (const row of rows) {
            const key = String(row.value.tenant),
              large = BigInt(row.value.large),
              amount = String(row.value.amount);
            const units = decimalUnits(amount);
            const group = groups.get(key) ?? {
              count: 0n,
              minimum: large,
              maximum: large,
              total: 0n,
            };
            group.count++;
            group.minimum = large < group.minimum ? large : group.minimum;
            group.maximum = large > group.maximum ? large : group.maximum;
            group.total += units;
            groups.set(key, group);
          }
          return [...groups]
            .sort(([a], [b]) => a.localeCompare(b))
            .map(([tenant, g]) => ({
              key: { tenant },
              values: {
                count: g.count.toString(),
                minimum: g.minimum.toString(),
                maximum: g.maximum.toString(),
                ...(sum
                  ? { sum: `${g.total / 10000n}.${(g.total % 10000n).toString().padStart(4, '0')}` }
                  : {}),
              },
            }));
        };
        const metrics = {
          count: { op: 'count' },
          minimum: { op: 'min', property: 'large' },
          maximum: { op: 'max', property: 'large' },
        };
        const input = { groupBy: ['tenant'], metrics, budget };
        const adminRows = await canonical(true);
        assert.ok(
          adminRows.some((row) => row.uid === original[7].uid),
          'overflow object exists in complete source',
        );
        assert.deepEqual((await aggregate(input, credential.token)).groups, grouped(adminRows));
        const filter = {
          op: 'and',
          args: [
            { op: 'gte', args: [{ field: 'amount' }, { literal: '0.0000' }] },
            { op: 'lte', args: [{ field: 'amount' }, { literal: '1.0000' }] },
          ],
        };
        const rows = (await canonical(false)).filter((row) => {
          const units = decimalUnits(String(row.value.amount));
          return units >= 0n && units <= 10000n;
        });
        assert.deepEqual(
          (
            await aggregate({
              ...input,
              filter,
              metrics: { ...metrics, sum: { op: 'sum', property: 'amount' } },
            })
          ).groups,
          grouped(rows, true),
        );
        assert.equal(
          (
            await aggregate(
              {
                budget,
                filter: { literal: false },
                groupBy: ['optional'],
                metrics: { count: { op: 'count' } },
              },
              readerCredential.token,
              403,
            )
          ).error.code,
          'PROPERTY_FORBIDDEN',
        );
        await invoke('delete', original[7].uid);
        assert.deepEqual(
          (await aggregate(input, credential.token)).groups,
          grouped(await canonical(true)),
        );
        await invoke('restore', original[7].uid);
        assert.deepEqual(
          (await aggregate(input, credential.token)).groups,
          grouped(await canonical(true)),
        );
      },
    );
    await t.test(
      'a pinned indexed cursor survives projection replacement and suppresses rows hidden by current overlays',
      async () => {
        const input = {
          sort: [{ property: 'value', direction: 'asc' }],
          pageSize: 7,
          projection: ['uid', 'value', 'tenant'],
        };
        const first = await query(input);
        assert.ok(first.nextCursor);
        const pin = first.revision;
        const sourceAtPin = (
          await db.pool.query(
            `WITH generation AS (SELECT stage FROM moria.source_generations WHERE environment=$1 AND object_type=$2 AND revision<=$3 ORDER BY revision DESC LIMIT 1) SELECT r.uid,r.value||COALESCE(o.overrides,'{}') AS value FROM generation g JOIN moria.staged_source_rows r ON r.stage=g.stage LEFT JOIN LATERAL (SELECT overrides,tombstone FROM moria.overlay_versions WHERE environment=$1 AND object_type=$2 AND uid=r.uid AND revision<=$3 ORDER BY revision DESC LIMIT 1) o ON true WHERE NOT COALESCE(o.tombstone,false) AND r.value->>'tenant'='a' AND (r.value||COALESCE(o.overrides,'{}'))->>'tenant'='a' ORDER BY ((r.value||COALESCE(o.overrides,'{}'))->>'value') COLLATE "C" ASC NULLS LAST,r.uid`,
            [environment, target, pin],
          )
        ).rows;
        const hidden = sourceAtPin[20].uid,
          changed = sourceAtPin[30].uid;
        await invoke('move', hidden, 'b');
        await invoke('edit', changed, 'changed-after-cursor-pin');
        await install('p8');
        await native('index');
        const seen: { uid: string; value: string | null }[] = [...first.objects];
        let cursor = first.nextCursor;
        while (cursor) {
          const page = await query({ ...input, cursor });
          assert.equal(page.revision, pin);
          seen.push(...page.objects);
          cursor = page.nextCursor;
        }
        assert.deepEqual(
          seen.map((row) => row.uid),
          sourceAtPin.filter((row) => row.uid !== hidden).map((row) => row.uid),
        );
        assert.equal(
          seen.find((row) => row.uid === changed)!.value,
          sourceAtPin.find((row) => row.uid === changed)!.value.value,
        );
        const count = (
          await db.pool.query(
            'SELECT count(DISTINCT expires_at) AS deadlines FROM moria.read_cursors WHERE environment=$1 AND binding_hash=(SELECT binding_hash FROM moria.read_cursors WHERE token_hash=$2)',
            [environment, createHash('sha256').update(first.nextCursor).digest('hex')],
          )
        ).rows[0].deadlines;
        assert.equal(count, '1');
        await sessions.revoke(
          {
            environment,
            principal: 'reader',
            application: 'native-indexing',
            sessionHash: createHash('sha256').update(readerCredential.token).digest('hex'),
          },
          'fixture',
        );
        assert.equal(
          (await query({ ...input, cursor: first.nextCursor }, readerCredential.token, 401)).error
            .code,
          'UNAUTHENTICATED',
        );
      },
    );
  },
);
