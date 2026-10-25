import { createSnowService } from '../services/snotel/src/service.js';
import { snowConfig, snowFixture } from './fixtures/snow.js';
import { createSnowJobs } from '../services/execution/src/snow-jobs.js';
import { baseline } from '../services/database/src/baseline.js';
import { externalDatabase } from '../services/database/src/config.js';
import { schema, holdSchema } from '../services/database/src/schema.js';
import {
  createClient as snowSdk,
  ApiError as SnowApiError,
} from '../apps/snotel/src/generated/index.js';
import { before, after, test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdir } from 'node:fs/promises';
import { resolve } from 'node:path';
import { createServer } from 'node:net';
import { createServer as createHttpServer } from 'node:http';
import { fork } from 'node:child_process';
import EmbeddedPostgres from 'embedded-postgres';
import pg from 'pg';
import { compileExpression } from '../packages/authorization/src/expressions.js';
import type { Expr, ValueType } from '../packages/contracts/types.js';
import { Catalog, type Publication } from '../services/catalog/src/catalog.js';
import { iceberg } from './iceberg-helper.js';
import { ObjectStore, type ObjectSchema } from '../services/ontology/src/store.js';
import { CursorStore } from '../services/ontology/src/cursors.js';
import { Authority } from '../services/security/src/authority.js';
import { authorize } from '../packages/authorization/src/access.js';
import { ObjectService, type ActionBinding } from '../services/ontology/src/service.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { createApi } from '../apps/api/src/server.js';
import { queryRows } from '../services/ontology/src/query.js';
import { icebergRows } from '../workers/iceberg/stream.js';
import { IcebergIndexer } from '../services/ontology/src/indexer.js';
import { withLease } from '../services/ontology/src/lease.js';
import { checkSnowApiSurface } from './fixtures/snow-api-security.js';

let db: EmbeddedPostgres;
let pool: pg.Pool;
let catalog: Catalog;
let connection: pg.PoolConfig;
before(
  async () => {
    const server = createServer();
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
    const port = (server.address() as { port: number }).port;
    await new Promise<void>((r, j) => server.close((e) => (e ? j(e) : r())));
    const directory = resolve('.local', `pg-test-${randomUUID()}`);
    await mkdir(directory, { recursive: true });
    const password = randomUUID();
    db = new EmbeddedPostgres({
      databaseDir: directory,
      user: 'moria_test',
      password,
      port,
      persistent: true,
      authMethod: 'scram-sha-256',
      initdbFlags: ['--encoding=UTF8', '--locale=C'],
      postgresFlags: ['-c', 'listen_addresses=127.0.0.1'],
      onLog: () => {},
      onError: () => {},
    });
    await db.initialise();
    await db.start();
    connection = {
      host: '127.0.0.1',
      port,
      user: 'moria_test',
      password,
      database: 'postgres',
      max: 6,
    };
    pool = new pg.Pool(connection);
    catalog = new Catalog(pool);
    await catalog.migrate();
  },
  { timeout: 60000 },
);
after(async () => {
  await pool?.end();
  await db?.stop();
});

test('shared reader admission releases its session lock and rolls back on failure', async () => {
  const single = new pg.Pool({ ...connection, max: 1 }),
    local = new Catalog(single);
  await local.ensureEnvironment('reader-cleanup');
  try {
    await assert.rejects(
      local.transaction(
        async (c) => {
          const lock = await c.query(
            "SELECT mode FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory'",
          );
          assert.deepEqual(
            lock.rows.map((r) => r.mode),
            ['ShareLock'],
          );
          await c.query("UPDATE moria.environments SET revision=99 WHERE id='reader-cleanup'");
          throw new Error('injected read callback failure');
        },
        'reader-cleanup',
        'repeatable-read',
        'shared',
      ),
      /injected read callback failure/,
    );
    assert.equal(
      (
        await single.query(
          "SELECT count(*) FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory'",
        )
      ).rows[0].count,
      '0',
    );
    assert.equal(
      (await single.query("SELECT revision FROM moria.environments WHERE id='reader-cleanup'"))
        .rows[0].revision,
      '0',
    );
  } finally {
    await single.end();
  }
});

test('policy reference evaluator agrees with real PostgreSQL three-valued truth tables', async () => {
  const types = {
    attributes: {
      a: { type: 'boolean', nullable: true } as ValueType,
      b: { type: 'boolean', nullable: true } as ValueType,
    },
  };
  for (const op of ['and', 'or'])
    for (const a of [true, false, null])
      for (const b of [true, false, null]) {
        const expr: Expr = { op, args: [{ principalAttribute: 'a' }, { principalAttribute: 'b' }] };
        const compiled = compileExpression(expr, types),
          values = { attributes: { a, b } };
        const sql = compiled.sql(values, {});
        const result = await pool.query(`SELECT ${sql.text} AS allowed`, sql.values);
        assert.equal(result.rows[0].allowed, compiled.evaluate(values), `${op}: ${a}, ${b}`);
      }
});
test('absent attributes never resolve inherited JavaScript object properties', async () => {
  const compiled = compileExpression(
    { principalAttribute: 'constructor' },
    { attributes: { constructor: { type: 'boolean' as const, nullable: false } } },
  );
  const sql = compiled.sql({ attributes: {} }, {});
  assert.equal(compiled.evaluate({ attributes: {} }), null);
  assert.equal(
    (await pool.query(`SELECT ${sql.text} AS allowed`, sql.values)).rows[0].allowed,
    null,
  );
});
test('numeric and Unicode comparisons match PostgreSQL without precision loss', async () => {
  for (const [type, a, b] of [
    [{ type: 'int64', nullable: false }, '9007199254740993', '9007199254740992'],
    [
      { type: 'decimal', nullable: false, precision: 30, scale: 3 },
      '-1000000000000000000.001',
      '-1000000000000000000.002',
    ],
    [{ type: 'string', nullable: false }, '😀', '\uE000'],
  ] as [ValueType, string, string][]) {
    const p = compileExpression(
      { op: 'gt', args: [{ parameter: 'a' }, { parameter: 'b' }] },
      { parameters: { a: type, b: type } },
    );
    const values = { parameters: { a, b } },
      sql = p.sql(values, {});
    assert.equal(
      (await pool.query(`SELECT ${sql.text} AS allowed`, sql.values)).rows[0].allowed,
      p.evaluate(values),
    );
  }
});
test('timestamps, array equality and null membership agree with PostgreSQL', async () => {
  for (const [type, a, b] of [
    [{ type: 'timestamp', nullable: false }, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00.000001Z'],
    [
      { type: 'array', nullable: false, items: { type: 'string', nullable: false } },
      ['a', 'b'],
      ['a', 'b'],
    ],
    [
      { type: 'array', nullable: false, items: { type: 'string', nullable: false } },
      ['a', 'b'],
      ['b', 'a'],
    ],
  ] as [ValueType, any, any][]) {
    const p = compileExpression(
      { op: 'eq', args: [{ parameter: 'a' }, { parameter: 'b' }] },
      { parameters: { a: type, b: type } },
    );
    const values = { parameters: { a, b } },
      sql = p.sql(values, {});
    assert.equal(
      (await pool.query(`SELECT ${sql.text} AS allowed`, sql.values)).rows[0].allowed,
      p.evaluate(values),
    );
  }
  for (const value of ['a', 'z', null])
    for (const list of [[], ['a'], ['a', null]]) {
      const p = compileExpression(
        { op: 'in', args: [{ parameter: 'v' }, { literal: list }] },
        { parameters: { v: { type: 'string', nullable: true } } },
      );
      const values = { parameters: { v: value } },
        sql = p.sql(values, {});
      assert.equal(
        (await pool.query(`SELECT ${sql.text} AS allowed`, sql.values)).rows[0].allowed,
        p.evaluate(values),
      );
    }
});
test('real database rollback atomically protects multiple publication heads and checkpoints', async () => {
  await pool.query(
    'CREATE TABLE heads (id text PRIMARY KEY, revision integer NOT NULL); CREATE TABLE checkpoint (id text PRIMARY KEY, cursor integer NOT NULL)',
  );
  await pool.query(
    "INSERT INTO heads VALUES ('a',1),('b',1); INSERT INTO checkpoint VALUES ('source',1)",
  );
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query("UPDATE heads SET revision=2 WHERE id='a'");
    await client.query("UPDATE checkpoint SET cursor=2 WHERE id='source'");
    await assert.rejects(client.query("INSERT INTO heads VALUES ('b',9)"));
    await client.query('ROLLBACK');
  } finally {
    client.release();
  }
  assert.deepEqual((await pool.query('SELECT revision FROM heads ORDER BY id')).rows, [
    { revision: 1 },
    { revision: 1 },
  ]);
  assert.equal((await pool.query('SELECT cursor FROM checkpoint')).rows[0].cursor, 1);
});
test('two concurrent compare-and-swap writers cannot both publish the same expected head', async () => {
  const clients = await Promise.all([pool.connect(), pool.connect()]);
  try {
    const results = await Promise.all(
      clients.map(async (c) => {
        await c.query('BEGIN');
        const changed = await c.query(
          "UPDATE heads SET revision=revision+1 WHERE id='a' AND revision=1 RETURNING revision",
        );
        await c.query('COMMIT');
        return changed.rowCount;
      }),
    );
    assert.equal(
      results.reduce<number>((n, x) => n + (x ?? 0), 0),
      1,
    );
  } finally {
    clients.forEach((c) => c.release());
  }
});

test('permanent UID allocator resolves concurrent requests to the same identity', async () => {
  const ids = await Promise.all(
    Array.from({ length: 8 }, () =>
      catalog.resolveIdentity('work-orders', 'source-a:incarnation-1:42'),
    ),
  );
  assert.equal(new Set(ids).size, 1);
  assert.notEqual(
    ids[0],
    await catalog.resolveIdentity('work-orders', 'source-a:incarnation-2:42'),
  );
});
async function publication(environment: string): Promise<Publication> {
  await catalog.ensureEnvironment(environment);
  await catalog.ensureDataset(environment, 'a');
  await catalog.ensureDataset(environment, 'b');
  const fence = await catalog.claim(environment, 'build', 'worker');
  return {
    environment,
    execution: 'build',
    worker: 'worker',
    fence,
    securityEpoch: '0',
    lineage: { input: 'snapshot-1' },
    outputs: [
      {
        dataset: 'a',
        expectedRevision: '0',
        metadataLocation: 'file:///fixture/a.metadata.json',
        snapshotId: '100',
      },
      {
        dataset: 'b',
        expectedRevision: '0',
        metadataLocation: 'file:///fixture/b.metadata.json',
        snapshotId: '101',
      },
    ],
    checkpoint: {
      connection: 'source',
      stream: 'main',
      configRevision: 'v1',
      expectedVersion: '0',
      value: { offset: 1 },
    },
  };
}
test('catalog commits multiple heads plus checkpoint and deduplicates lost acknowledgment', async () => {
  const p = await publication('commit');
  const first = await catalog.publish(p);
  const second = await catalog.publish(p);
  assert.deepEqual(second, first);
  assert.equal(first.outputs.length, 2);
  assert.equal(
    (await pool.query("SELECT version FROM moria.checkpoints WHERE environment='commit'")).rows[0]
      .version,
    '1',
  );
  await assert.rejects(catalog.publish({ ...p, lineage: { input: 'other' } }), {
    code: 'PUBLICATION_CONFLICT',
  });
});
test('catalog stale second output rolls back first output and checkpoint', async () => {
  const p = await publication('conflict');
  p.outputs[1].expectedRevision = '99';
  await assert.rejects(catalog.publish(p), { code: 'HEAD_CONFLICT' });
  assert.deepEqual(
    (
      await pool.query(
        "SELECT revision FROM moria.dataset_heads WHERE environment='conflict' ORDER BY dataset",
      )
    ).rows,
    [{ revision: '0' }, { revision: '0' }],
  );
  assert.equal(
    (await pool.query("SELECT * FROM moria.checkpoints WHERE environment='conflict'")).rowCount,
    0,
  );
});
test('revocation and stale worker fence block publication', async () => {
  const p = await publication('revoke');
  await catalog.revoke('revoke');
  await assert.rejects(catalog.publish(p), { code: 'AUTHORIZATION_CHANGED' });
  const q = await publication('fence');
  await pool.query(
    "UPDATE moria.leases SET expires_at=clock_timestamp()-interval '1 second' WHERE environment='fence'",
  );
  await catalog.claim('fence', 'build', 'new-worker');
  await assert.rejects(catalog.publish(q), { code: 'LEASE_LOST' });
});
test('action idempotency survives revision changes, conflicts on input changes and never reuses expired keys', async () => {
  await catalog.ensureEnvironment('actions');
  await pool.query(
    'CREATE TABLE effects(id integer PRIMARY KEY, value integer NOT NULL); INSERT INTO effects VALUES(1,0)',
  );
  const request = {
    environment: 'actions',
    application: 'app',
    principal: 'user',
    action: 'assign',
    key: 'request1',
    actionRevision: 'v1',
    input: { assignee: 'Bob' },
    securityEpoch: '0',
  };
  const apply = async (c: pg.PoolClient) =>
    (await c.query('UPDATE effects SET value=value+1 WHERE id=1 RETURNING value')).rows[0];
  assert.deepEqual(await catalog.action(request, apply), { value: 1 });
  assert.deepEqual(await catalog.action({ ...request, actionRevision: 'v2' }, apply), { value: 1 });
  await assert.rejects(catalog.action({ ...request, input: { assignee: 'Carol' } }, apply), {
    code: 'IDEMPOTENCY_CONFLICT',
  });
  await pool.query("UPDATE moria.action_keys SET expires_at=clock_timestamp()-interval '1 second'");
  await assert.rejects(catalog.action(request, apply), { code: 'RESULT_EXPIRED' });
  assert.equal((await pool.query('SELECT value FROM effects')).rows[0].value, 1);
});

test(
  'real Iceberg snapshots publish atomically, preserve history, and hide failed staged outputs',
  { timeout: 60000 },
  async () => {
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'uid', required: true, type: 'string' },
        { id: 2, name: 'value', required: false, type: 'long' },
      ],
    };
    const warehouse = resolve('.local', `iceberg-${randomUUID()}`);
    const a = await iceberg({
      operation: 'stage',
      warehouse,
      schema,
      rows: [{ uid: 'a', value: 1 }],
    });
    const b = await iceberg({
      operation: 'stage',
      warehouse,
      schema,
      rows: [{ uid: 'b', value: 2 }],
    });
    const p = await publication('iceberg');
    p.outputs = [
      { dataset: 'a', expectedRevision: '0', ...a },
      { dataset: 'b', expectedRevision: '0', ...b },
    ];
    await catalog.publish(p);
    const heads = async () =>
      (
        await pool.query(
          "SELECT dataset,metadata_location FROM moria.dataset_heads WHERE environment='iceberg' ORDER BY dataset",
        )
      ).rows;
    assert.deepEqual(
      (await iceberg({ operation: 'read', metadataLocation: (await heads())[0].metadata_location }))
        .rows,
      [{ uid: 'a', value: 1 }],
    );
    const a2 = await iceberg({
      operation: 'stage',
      warehouse,
      baseMetadataLocation: a.metadataLocation,
      rows: [{ uid: 'a', value: 9 }],
    });
    assert.equal(a2.tableUuid, a.tableUuid);
    assert.notEqual(a2.snapshotId, a.snapshotId);
    const fence = await catalog.claim('iceberg', 'build2', 'worker');
    const failed = {
      ...p,
      execution: 'build2',
      fence,
      outputs: [
        { dataset: 'a', expectedRevision: '1', ...a2 },
        { dataset: 'b', expectedRevision: '99', ...b },
      ],
      checkpoint: { ...p.checkpoint!, expectedVersion: '1', value: { offset: 2 } },
    };
    await assert.rejects(catalog.publish(failed), { code: 'HEAD_CONFLICT' });
    assert.equal((await heads())[0].metadata_location, a.metadataLocation);
    assert.equal(
      (await pool.query("SELECT value FROM moria.checkpoints WHERE environment='iceberg'")).rows[0]
        .value.offset,
      1,
    );
    failed.outputs[1].expectedRevision = '1';
    await catalog.publish(failed);
    assert.deepEqual(
      (await iceberg({ operation: 'read', metadataLocation: (await heads())[0].metadata_location }))
        .rows,
      [{ uid: 'a', value: 9 }],
    );
    assert.deepEqual(
      (await iceberg({ operation: 'read', metadataLocation: a.metadataLocation })).rows,
      [{ uid: 'a', value: 1 }],
    );
  },
);

test('source disappearance preserves overlays, null differs from clear, tombstones survive refresh', async () => {
  const store = new ObjectStore(catalog);
  await store.migrate();
  await catalog.ensureEnvironment('objects');
  const uid = await catalog.resolveIdentity('objects', 'work-order');
  const schema: ObjectSchema = {
    primaryKey: 'uid',
    properties: {
      uid: { type: 'uid', nullable: false },
      assignee: { type: 'string', nullable: true },
    },
  };
  const base = { uid, assignee: 'Carol' };
  let revision = await store.activate('objects', 'work-order', schema, [base], '0', '0');
  const request = (key: string) => ({
    environment: 'objects',
    application: 'app',
    principal: 'operator',
    action: 'assign',
    actionRevision: 'v1',
    key,
    input: { key },
    securityEpoch: '0',
  });
  const read = async (r = revision) => {
    const c = await pool.connect();
    try {
      return await store.read(c, 'objects', 'work-order', uid, r);
    } finally {
      c.release();
    }
  };
  revision = (
    await store.edit(
      request('bob'),
      'work-order',
      uid,
      schema,
      [{ op: 'set', property: 'assignee', value: 'Bob' }],
      () => {},
    )
  ).revision;
  const pinned = revision;
  revision = await store.activate('objects', 'work-order', schema, [], '0', revision);
  assert.equal(await read(), null);
  assert.equal((await read(pinned))!.effective.assignee, 'Bob');
  revision = await store.activate('objects', 'work-order', schema, [base], '0', revision);
  assert.equal((await read())!.effective.assignee, 'Bob');
  revision = (
    await store.edit(
      request('null'),
      'work-order',
      uid,
      schema,
      [{ op: 'set', property: 'assignee', value: null }],
      () => {},
    )
  ).revision;
  assert.equal((await read())!.effective.assignee, null);
  revision = (
    await store.edit(
      request('clear'),
      'work-order',
      uid,
      schema,
      [{ op: 'clearOverride', property: 'assignee' }],
      () => {},
    )
  ).revision;
  assert.equal((await read())!.effective.assignee, 'Carol');
  revision = (
    await store.edit(request('delete'), 'work-order', uid, schema, [{ op: 'delete' }], () => {})
  ).revision;
  revision = await store.activate('objects', 'work-order', schema, [base], '0', revision);
  assert.equal(await read(), null);
  revision = (
    await store.edit(request('restore'), 'work-order', uid, schema, [{ op: 'restore' }], () => {})
  ).revision;
  assert.equal((await read())!.effective.assignee, 'Carol');
  await assert.rejects(
    store.activate('objects', 'work-order', schema, [base, base], '0', revision),
    { code: 'DUPLICATE_UID' },
  );
});

test(
  'pinned Iceberg streams feed object staging with wire values and reject mismatched snapshots',
  { timeout: 60000 },
  async () => {
    const warehouse = resolve('.local', `iceberg-stream-${randomUUID()}`);
    const icebergSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'uid', required: true, type: 'string' },
        { id: 2, name: 'amount', required: false, type: 'long' },
      ],
    };
    const ids = Array.from({ length: 1001 }, () => randomUUID());
    const pin = await iceberg({
      operation: 'stage',
      warehouse,
      schema: icebergSchema,
      rows: ids.map((uid, i) => ({ uid, amount: i })),
    });
    const store = new ObjectStore(catalog);
    await store.migrate();
    await catalog.ensureEnvironment('iceberg-stream');
    await pool.query(
      "INSERT INTO moria.identities(domain,canonical_key,uid) SELECT 'iceberg-stream',uid,uid::uuid FROM unnest($1::text[]) uid",
      [ids],
    );
    const schema: ObjectSchema = {
      primaryKey: 'uid',
      properties: {
        uid: { type: 'uid', nullable: false },
        amount: { type: 'int64', nullable: true },
      },
    };
    const revision = await store.activate(
      'iceberg-stream',
      'streamed',
      schema,
      icebergRows(pin, { batchRows: 17 }),
      '0',
      '0',
    );
    const c = await pool.connect();
    try {
      assert.equal(
        (await store.read(c, 'iceberg-stream', 'streamed', ids[1000], revision))!.source.amount,
        '1000',
      );
    } finally {
      c.release();
    }
    const next = await iceberg({
      operation: 'stage',
      warehouse,
      baseMetadataLocation: pin.metadataLocation,
      rows: [{ uid: ids[0], amount: 999 }],
    });
    const old = icebergRows(pin, { batchRows: 1 });
    assert.deepEqual((await old.next()).value, { uid: ids[0], amount: '0' });
    await old.return(undefined);
    const cancellation = new AbortController(),
      cancelled = icebergRows(pin, { batchRows: 1, signal: cancellation.signal });
    assert.deepEqual((await cancelled.next()).value, { uid: ids[0], amount: '0' });
    cancellation.abort();
    await assert.rejects(cancelled.next(), { code: 'CANCELLED' });
    await assert.rejects(icebergRows(pin, { signal: cancellation.signal }).next(), {
      code: 'CANCELLED',
    });
    const timed = icebergRows(pin, { batchRows: 17, timeoutMs: 5000 });
    assert.deepEqual((await timed.next()).value, { uid: ids[0], amount: '0' });
    await new Promise((resolve) => setTimeout(resolve, 5100));
    await assert.rejects(timed.next(), { code: 'WORKER_TIMEOUT' });
    await assert.rejects(
      async () => {
        for await (const row of icebergRows({ ...pin, snapshotId: next.snapshotId })) void row;
      },
      { code: 'WORKER_FAILED' },
    );
    await assert.rejects(
      async () => {
        for await (const row of icebergRows(pin, { batchRows: 1001 })) void row;
      },
      { code: 'STREAM_INVALID' },
    );
    await assert.rejects(
      store.stage(
        'iceberg-stream',
        'streamed',
        schema,
        icebergRows({ ...pin, snapshotId: next.snapshotId }),
        '0',
        revision,
      ),
      { code: 'WORKER_FAILED' },
    );
    assert.equal(
      (await pool.query("SELECT revision FROM moria.environments WHERE id='iceberg-stream'"))
        .rows[0].revision,
      revision,
    );
  },
);

test('competing action conditions re-evaluate under serialization and only one commits', async () => {
  const store = new ObjectStore(catalog);
  await store.migrate();
  await catalog.ensureEnvironment('claims');
  const uid = await catalog.resolveIdentity('claims', 'one');
  const schema: ObjectSchema = {
    primaryKey: 'uid',
    properties: {
      uid: { type: 'uid', nullable: false },
      assignee: { type: 'string', nullable: true },
    },
  };
  await store.activate('claims', 'claim', schema, [{ uid, assignee: null }], '0', '0');
  const results = await Promise.allSettled(
    ['Bob', 'Carol'].map((name) =>
      store.edit(
        {
          environment: 'claims',
          application: 'app',
          principal: name,
          action: 'claim',
          actionRevision: 'v1',
          key: name,
          input: { name },
          securityEpoch: '0',
        },
        'claim',
        uid,
        schema,
        [{ op: 'set', property: 'assignee', value: name }],
        (before) => {
          if (before?.effective.assignee !== null) throw new Error('already claimed');
        },
      ),
    ),
  );
  assert.equal(results.filter((r) => r.status === 'fulfilled').length, 1);
  assert.equal(
    (await pool.query("SELECT count(*) FROM moria.overlay_versions WHERE environment='claims'"))
      .rows[0].count,
    '1',
  );
});

test(
  'streamed same-schema refresh stays invisible and publishes across concurrent edits',
  { timeout: 15000 },
  async () => {
    const store = new ObjectStore(catalog);
    await store.migrate();
    await catalog.ensureEnvironment('staging');
    const ids = (
      await pool.query(
        "INSERT INTO moria.identities(domain,canonical_key,uid) SELECT 'staging',g::text,gen_random_uuid() FROM generate_series(1,1001) g RETURNING uid",
      )
    ).rows.map((r) => r.uid as string);
    const schema: ObjectSchema = {
      primaryKey: 'uid',
      properties: {
        uid: { type: 'uid', nullable: false },
        assignee: { type: 'string', nullable: true },
      },
    };
    const initial = await store.activate(
      'staging',
      'orders',
      schema,
      [{ uid: ids[0], assignee: 'old' }],
      '0',
      '0',
    );
    let reached!: () => void, release!: () => void;
    const atBarrier = new Promise<void>((r) => (reached = r)),
      resume = new Promise<void>((r) => (release = r));
    async function* input() {
      for (let i = 0; i < 1000; i++) yield { uid: ids[i], assignee: 'new' };
      reached();
      await resume;
      yield { uid: ids[1000], assignee: 'new' };
    }
    const pending = store.stage('staging', 'orders', schema, input(), '0', initial);
    await atBarrier;
    let edited: string;
    try {
      const loading = (
        await pool.query(
          "SELECT id FROM moria.source_stages WHERE environment='staging' AND status='loading'",
        )
      ).rows[0].id;
      await assert.rejects(store.publishStage(loading), { code: 'STAGE_INVALID' });
      const c = await pool.connect();
      try {
        assert.equal(
          (await store.read(c, 'staging', 'orders', ids[0], initial))!.effective.assignee,
          'old',
        );
        assert.equal(await store.read(c, 'staging', 'orders', ids[1], initial), null);
      } finally {
        c.release();
      }
      edited = (
        await store.edit(
          {
            environment: 'staging',
            application: 'app',
            principal: 'operator',
            action: 'assign',
            actionRevision: 'v1',
            key: 'during-stage',
            input: {},
            securityEpoch: '0',
          },
          'orders',
          ids[0],
          schema,
          [{ op: 'set', property: 'assignee', value: 'Bob' }],
          () => {},
        )
      ).revision;
    } finally {
      release();
    }
    const stage = await pending;
    const next = await store.publishStage(stage);
    assert.equal(await store.publishStage(stage), next);
    assert.equal(BigInt(next), BigInt(edited!) + 1n);
    const c = await pool.connect();
    try {
      assert.equal(
        (await store.read(c, 'staging', 'orders', ids[0], next))!.effective.assignee,
        'Bob',
      );
      assert.equal(
        (await store.read(c, 'staging', 'orders', ids[1000], next))!.source.assignee,
        'new',
      );
      assert.equal(await store.read(c, 'staging', 'orders', ids[1000], initial), null);
    } finally {
      c.release();
    }
    assert.equal(
      (
        await pool.query(
          "SELECT count(*) FROM moria.source_generations WHERE environment='staging'",
        )
      ).rows[0].count,
      '2',
    );
  },
);

test('refresh source-head CAS rejects competing stages and schema migration races', async () => {
  const store = new ObjectStore(catalog);
  await store.migrate();
  await catalog.ensureEnvironment('refresh-cas');
  const uid = await catalog.resolveIdentity('refresh-cas', 'one');
  const schema: ObjectSchema = {
    primaryKey: 'uid',
    properties: { uid: { type: 'uid', nullable: false }, note: { type: 'string', nullable: true } },
  };
  const rows = [{ uid, note: 'source' }];
  let revision = await store.activate('refresh-cas', 'orders', schema, rows, '0', '0');
  const a = await store.stage('refresh-cas', 'orders', schema, rows, '0', revision),
    b = await store.stage('refresh-cas', 'orders', schema, rows, '0', revision);
  revision = await store.publishStage(a);
  await assert.rejects(store.publishStage(b), { code: 'HEAD_CONFLICT' });
  const changed: ObjectSchema = {
    ...schema,
    properties: { ...schema.properties, extra: { type: 'string', nullable: true } },
  };
  const migration = await store.stage('refresh-cas', 'orders', changed, rows, '0', revision);
  const request = {
    environment: 'refresh-cas',
    application: 'app',
    principal: 'operator',
    action: 'edit',
    actionRevision: '1',
    input: {},
    securityEpoch: '0',
  };
  revision = (
    await store.edit(
      { ...request, key: 'concurrent' },
      'orders',
      uid,
      schema,
      [{ op: 'set', property: 'note', value: 'kept' }],
      () => {},
    )
  ).revision;
  await assert.rejects(store.publishStage(migration), { code: 'HEAD_CONFLICT' });
  await assert.rejects(
    store.edit(
      { ...request, key: 'wrong-schema' },
      'orders',
      uid,
      changed,
      [{ op: 'set', property: 'extra', value: 'bad' }],
      () => {},
    ),
    { code: 'SCHEMA_CHANGED' },
  );
  revision = await store.activate('refresh-cas', 'orders', changed, rows, '0', revision);
  await assert.rejects(
    store.edit({ ...request, key: 'old-schema' }, 'orders', uid, schema, [], () => {}),
    { code: 'SCHEMA_CHANGED' },
  );
  const c = await pool.connect();
  try {
    assert.equal(
      (await store.read(c, 'refresh-cas', 'orders', uid, revision))!.effective.note,
      'kept',
    );
  } finally {
    c.release();
  }
  const authority = new Authority(catalog);
  await authority.migrate();
  await authority.syncPrincipal('refresh-cas', 'setup', 'operator', {
    kind: 'workload',
    enabled: true,
    attributes: {},
    markings: [],
  });
  await authority.setApplication('refresh-cas', 'setup', 'app', ['orders:read']);
  await authority.setGrants('refresh-cas', 'setup', 'operator', 'app', ['orders:read']);
  const stale = new ObjectService(store, authority, {
    schema,
    policies: {
      resource: 'orders',
      markings: [],
      properties: schema.properties,
      sourceColumns: schema.properties,
      attributes: {},
    },
  });
  const session = { environment: 'refresh-cas', principal: 'operator', application: 'app' };
  await assert.rejects(stale.read(session, uid), { code: 'SCHEMA_CHANGED' });
  await assert.rejects(stale.query(session, {}), { code: 'SCHEMA_CHANGED' });
});

test('ontology stage publication enforces persisted worker fences and renewal cannot revive expiry', async () => {
  const store = new ObjectStore(catalog);
  await store.migrate();
  await catalog.ensureEnvironment('stage-fence');
  const uid = await catalog.resolveIdentity('stage-fence', 'one'),
    schema: ObjectSchema = {
      primaryKey: 'uid',
      properties: { uid: { type: 'uid', nullable: false } },
    };
  const fence = await catalog.claim('stage-fence', 'build', 'worker-a');
  await assert.rejects(catalog.claim('stage-fence', 'bad-duration', 'worker', NaN), {
    code: 'LEASE_INVALID',
  });
  await assert.rejects(catalog.renew('stage-fence', 'build', 'worker-b', fence), {
    code: 'LEASE_LOST',
  });
  await catalog.renew('stage-fence', 'build', 'worker-a', fence, 120);
  const deadline = (
    await pool.query("SELECT expires_at FROM moria.leases WHERE environment='stage-fence'")
  ).rows[0].expires_at.toISOString();
  await catalog.renew('stage-fence', 'build', 'worker-a', fence, 1);
  assert.equal(
    (
      await pool.query("SELECT expires_at FROM moria.leases WHERE environment='stage-fence'")
    ).rows[0].expires_at.toISOString(),
    deadline,
  );
  const lease = { execution: 'build', worker: 'worker-a', fence };
  const stage = await store.stage('stage-fence', 'fenced', schema, [{ uid }], '0', '0', lease);
  lease.worker = 'worker-b'; // Caller mutation cannot replace the persisted owner.
  await pool.query(
    "UPDATE moria.leases SET expires_at=clock_timestamp()-interval '1 second' WHERE environment='stage-fence'",
  );
  await assert.rejects(catalog.renew('stage-fence', 'build', 'worker-a', fence), {
    code: 'LEASE_LOST',
  });
  await assert.rejects(store.publishStage(stage), { code: 'LEASE_LOST' });
  const replacement = await catalog.claim('stage-fence', 'build', 'worker-b');
  assert.equal(BigInt(replacement), BigInt(fence) + 1n);
  await assert.rejects(store.publishStage(stage), { code: 'LEASE_LOST' });
  await assert.rejects(
    store.stage('stage-fence', 'fenced', schema, [{ uid }], '0', '0', {
      execution: 'build',
      worker: 'worker-a',
      fence,
    }),
    { code: 'LEASE_LOST' },
  );
  assert.equal(
    (await pool.query("SELECT revision FROM moria.environments WHERE id='stage-fence'")).rows[0]
      .revision,
    '0',
  );
  const winner = await store.stage('stage-fence', 'fenced', schema, [{ uid }], '0', '0', {
    execution: 'build',
    worker: 'worker-b',
    fence: replacement,
  });
  const revision = await store.publishStage(winner);
  await pool.query(
    "UPDATE moria.leases SET expires_at=clock_timestamp()-interval '1 second' WHERE environment='stage-fence'",
  );
  assert.equal(await store.publishStage(winner), revision); // Lost acknowledgment is a receipt read.
  assert.equal(
    (
      await pool.query(
        "SELECT count(*) FROM moria.source_generations WHERE environment='stage-fence'",
      )
    ).rows[0].count,
    '1',
  );
});

test('failed streams and cross-batch duplicates cannot publish partial generations', async () => {
  const store = new ObjectStore(catalog);
  await store.migrate();
  await catalog.ensureEnvironment('stage-fail');
  const ids = (
    await pool.query(
      "INSERT INTO moria.identities(domain,canonical_key,uid) SELECT 'stage-fail',g::text,gen_random_uuid() FROM generate_series(1,1000) g RETURNING uid",
    )
  ).rows.map((r) => r.uid as string);
  const schema: ObjectSchema = {
    primaryKey: 'uid',
    properties: { uid: { type: 'uid', nullable: false } },
  };
  async function* broken() {
    for (const uid of ids) yield { uid };
    throw new Error('source interrupted');
  }
  await assert.rejects(
    store.activate('stage-fail', 'orders', schema, broken(), '0', '0'),
    /source interrupted/,
  );
  async function* duplicate() {
    for (const uid of ids) yield { uid };
    yield { uid: ids[0] };
  }
  await assert.rejects(store.activate('stage-fail', 'orders', schema, duplicate(), '0', '0'), {
    code: 'DUPLICATE_UID',
  });
  const failed = await pool.query(
    "SELECT id,status FROM moria.source_stages WHERE environment='stage-fail'",
  );
  assert.equal(failed.rowCount, 2);
  for (const row of failed.rows) {
    assert.equal(row.status, 'failed');
    await assert.rejects(store.publishStage(row.id), { code: 'STAGE_INVALID' });
  }
  assert.equal(
    (await pool.query("SELECT revision FROM moria.environments WHERE id='stage-fail'")).rows[0]
      .revision,
    '0',
  );
  assert.equal(
    (
      await pool.query(
        "SELECT count(*) FROM moria.source_generations WHERE environment='stage-fail'",
      )
    ).rows[0].count,
    '0',
  );
  await assert.rejects(
    store.stage('stage-fail', 'another-type', schema, [{ uid: ids[0] }], '0', '0'),
    { code: 'UID_HOME_CONFLICT' },
  );
  const ready = await store.stage('stage-fail', 'orders', schema, [], '0', '0');
  await pool.query("UPDATE moria.environments SET security_epoch=1 WHERE id='stage-fail'");
  await assert.rejects(store.publishStage(ready), { code: 'AUTHORIZATION_CHANGED' });
});

test('additive stage migration preserves legacy generations and retained edit validation', async () => {
  const store = new ObjectStore(catalog);
  await store.migrate();
  await store.migrate();
  await catalog.ensureEnvironment('legacy-stage');
  const uid = await catalog.resolveIdentity('legacy-stage', 'one');
  await pool.query("INSERT INTO moria.object_homes VALUES($1,'orders')", [uid]);
  await pool.query(
    "INSERT INTO moria.source_generations(environment,object_type,revision) VALUES('legacy-stage','orders',1)",
  );
  await pool.query("INSERT INTO moria.source_rows VALUES('legacy-stage','orders',1,$1,$2)", [
    uid,
    JSON.stringify({ uid, note: 'legacy' }),
  ]);
  await pool.query("UPDATE moria.environments SET revision=1 WHERE id='legacy-stage'");
  const schema: ObjectSchema = {
    primaryKey: 'uid',
    properties: { uid: { type: 'uid', nullable: false }, note: { type: 'string', nullable: true } },
  };
  let r = (
    await store.edit(
      {
        environment: 'legacy-stage',
        application: 'app',
        principal: 'operator',
        action: 'edit',
        actionRevision: 'v1',
        key: 'edit',
        input: {},
        securityEpoch: '0',
      },
      'orders',
      uid,
      schema,
      [{ op: 'set', property: 'note', value: 'kept' }],
      () => {},
    )
  ).revision;
  await assert.rejects(
    store.stage(
      'legacy-stage',
      'orders',
      { primaryKey: 'uid', properties: { uid: { type: 'uid', nullable: false } } },
      [],
      '0',
      r,
    ),
    { code: 'MIGRATION_REQUIRED' },
  );
  const legacyCandidate = await store.stage(
    'legacy-stage',
    'orders',
    schema,
    [{ uid, note: 'new' }],
    '0',
    r,
  );
  r = (
    await store.edit(
      {
        environment: 'legacy-stage',
        application: 'app',
        principal: 'operator',
        action: 'edit',
        actionRevision: 'v1',
        key: 'later-edit',
        input: {},
        securityEpoch: '0',
      },
      'orders',
      uid,
      schema,
      [{ op: 'set', property: 'note', value: 'kept' }],
      () => {},
    )
  ).revision;
  await assert.rejects(store.publishStage(legacyCandidate), { code: 'HEAD_CONFLICT' });
  const next = await store.activate(
    'legacy-stage',
    'orders',
    schema,
    [{ uid, note: 'new' }],
    '0',
    r,
  );
  const c = await pool.connect();
  try {
    assert.equal(
      (await store.read(c, 'legacy-stage', 'orders', uid, '1'))!.effective.note,
      'legacy',
    );
    assert.equal(
      (await store.read(c, 'legacy-stage', 'orders', uid, next))!.effective.note,
      'kept',
    );
    const policy = {
      resource: 'orders',
      markings: [],
      properties: schema.properties,
      sourceColumns: schema.properties,
      attributes: {},
    };
    assert.equal(
      (await queryRows(c, 'legacy-stage', policy, {}, '1', next, null, { pageSize: 10 })).rows[0]
        .effective.note,
      'legacy',
    );
    assert.equal(
      (await queryRows(c, 'legacy-stage', policy, {}, '1', '1', null, { pageSize: 10 })).rows[0]
        .effective.note,
      'legacy',
    );
  } finally {
    c.release();
  }
});

test('cursor pins survive data changes, bind identity/query, never extend expiry and invalidate on revocation', async () => {
  const cursors = new CursorStore(catalog);
  await cursors.migrate();
  await catalog.ensureEnvironment('cursor');
  const binding = {
    environment: 'cursor',
    principal: 'alice',
    application: 'app',
    delegation: [],
    contract: 'v1',
    securityEpoch: '0',
    query: { order: 'uid' },
  };
  const first = await cursors.issue(binding, { uid: 'one' });
  await pool.query("UPDATE moria.environments SET revision=revision+1 WHERE id='cursor'");
  const next = await cursors.advance(first.token, binding, { uid: 'two' });
  const pinned = await cursors.resume(next, binding);
  assert.equal(pinned.revision, '0');
  assert.equal(pinned.currentRevision, '1');
  assert.equal(pinned.expiresAt, first.pin.expiresAt);
  await assert.rejects(cursors.resume(next, { ...binding, principal: 'bob' }), {
    code: 'CURSOR_INVALID',
  });
  await assert.rejects(cursors.resume(next, { ...binding, query: { order: 'note' } }), {
    code: 'CURSOR_INVALID',
  });
  await catalog.revoke('cursor');
  await assert.rejects(cursors.resume(next, { ...binding, securityEpoch: '1' }), {
    code: 'CURSOR_INVALIDATED',
  });
  await pool.query(
    "UPDATE moria.read_cursors SET expires_at=clock_timestamp()-interval '1 second' WHERE environment='cursor'",
  );
  await assert.rejects(cursors.resume(next, { ...binding, securityEpoch: '1' }), {
    code: 'CURSOR_EXPIRED',
  });
});

async function crashPublisher(request: Publication, phase: 'before' | 'after'): Promise<void> {
  const child = fork(resolve('tests/fixtures/publication-child.ts'), [], {
    execArgv: ['--import', 'tsx'],
    stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
  });
  await new Promise<void>((done, reject) => {
    let reached = false;
    const timeout = setTimeout(() => {
      child.kill('SIGKILL');
      reject(new Error('Publication child timed out'));
    }, 15000);
    child.on('error', (error) => {
      clearTimeout(timeout);
      reject(error);
    });
    child.on('message', (message: any) => {
      if (message.phase !== phase) {
        child.kill('SIGKILL');
        reject(new Error('Publication child failed'));
        return;
      }
      reached = true;
      child.kill('SIGKILL');
    });
    child.on('exit', () => {
      clearTimeout(timeout);
      reached ? done() : reject(new Error('Publication child exited before fault point'));
    });
    child.send({ config: connection, request, phase });
  });
}
test(
  'killed publisher rolls back before COMMIT and lost acknowledgment recovers after COMMIT',
  { timeout: 60000 },
  async () => {
    const warehouse = resolve('.local', `crash-iceberg-${randomUUID()}`);
    const staged = await iceberg({
      operation: 'stage',
      warehouse,
      schema: {
        type: 'struct',
        'schema-id': 0,
        fields: [{ id: 1, name: 'uid', required: true, type: 'string' }],
      },
      rows: [{ uid: 'kept' }],
    });
    for (const phase of ['before', 'after'] as const) {
      const request = await publication(`crash-${phase}`);
      request.outputs = request.outputs.map((output) => ({
        ...output,
        metadataLocation: staged.metadataLocation,
        snapshotId: staged.snapshotId,
      }));
      await crashPublisher(request, phase);
      const heads = await catalog.heads(request.environment, ['a', 'b']);
      assert.deepEqual(
        heads.map((head) => head.revision),
        phase === 'before' ? ['0', '0'] : ['1', '1'],
      );
      assert.equal(
        (
          await pool.query('SELECT * FROM moria.checkpoints WHERE environment=$1', [
            request.environment,
          ])
        ).rowCount,
        phase === 'before' ? 0 : 1,
      );
      const recovered = await catalog.publish(request);
      assert.deepEqual(
        recovered.outputs.map((output) => output.revision),
        ['1', '1'],
      );
      assert.deepEqual(
        (
          await iceberg({
            operation: 'read',
            metadataLocation: (await catalog.heads(request.environment, ['a']))[0].metadataLocation,
          })
        ).rows,
        [{ uid: 'kept' }],
      );
    }
  },
);
test('current authority keeps scopes separate from grants, audits revocation and fails stale identities closed', async () => {
  const authority = new Authority(catalog);
  await authority.migrate();
  await catalog.ensureEnvironment('authority');
  const principal = {
    enabled: true,
    kind: 'human' as const,
    attributes: { region: 'East' },
    markings: ['sensitive'],
  };
  const initial = await authority.syncPrincipal('authority', 'sync', 'alice', principal);
  assert.equal(await authority.syncPrincipal('authority', 'sync', 'alice', principal), initial);
  await authority.setApplication('authority', 'admin', 'app', ['orders:read', 'orders:write']);
  await authority.setGrants('authority', 'admin', 'alice', 'app', ['orders:read']);
  const first = await authority.current('authority', 'alice', 'app');
  authorize(first.identity, 'orders', 'read', ['sensitive']);
  assert.throws(() => authorize(first.identity, 'orders', 'write'), { code: 'FORBIDDEN' });
  await authority.setGrants('authority', 'admin', 'alice', 'app', []);
  const next = await authority.current('authority', 'alice', 'app');
  assert.notEqual(next.epoch, first.epoch);
  assert.throws(() => authorize(next.identity, 'orders', 'read'), { code: 'FORBIDDEN' });
  await pool.query(
    "UPDATE moria.principals SET synced_at=clock_timestamp()-interval '61 seconds' WHERE environment='authority'",
  );
  await assert.rejects(authority.current('authority', 'alice', 'app'), { code: 'IDENTITY_STALE' });
  assert.equal(
    (await pool.query("SELECT count(*) FROM moria.security_audit WHERE environment='authority'"))
      .rows[0].count,
    '4',
  );
});
test('twenty concurrent writers queue before their snapshot without exhausting serialization retries', async () => {
  const burstPool = new pg.Pool({ ...connection, max: 20 }),
    burst = new Catalog(burstPool);
  await burst.ensureEnvironment('burst');
  try {
    const before = (await pool.query('SELECT value FROM effects WHERE id=1')).rows[0].value;
    await Promise.all(
      Array.from({ length: 20 }, (_, index) =>
        burst.action(
          {
            environment: 'burst',
            application: 'app',
            principal: 'user',
            action: 'count',
            actionRevision: 'v1',
            key: String(index),
            input: { index },
            securityEpoch: '0',
          },
          async (c) =>
            (await c.query('UPDATE effects SET value=value+1 WHERE id=1 RETURNING value')).rows[0],
        ),
      ),
    );
    assert.equal(
      (await pool.query('SELECT value FROM effects WHERE id=1')).rows[0].value,
      before + 20,
    );
  } finally {
    await burstPool.end();
  }
});

test('integrated service enforces current markings, row/property policies, action conditions and revocation', async () => {
  const authority = new Authority(catalog),
    store = new ObjectStore(catalog);
  await authority.migrate();
  await store.migrate();
  await catalog.ensureEnvironment('service');
  const properties: Record<string, ValueType> = {
    uid: { type: 'uid', nullable: false },
    region: { type: 'string', nullable: false },
    assignee: { type: 'string', nullable: true },
    note: { type: 'string', nullable: false },
  };
  const schema = { primaryKey: 'uid', properties };
  await authority.syncPrincipal('service', 'sync', 'operator', {
    kind: 'human',
    enabled: true,
    attributes: { region: 'East', isOperator: true, isAuditor: false },
    markings: ['sensitive'],
  });
  await authority.setApplication('service', 'admin', 'app', ['orders:read', 'assign:invoke']);
  const epoch = await authority.setGrants('service', 'admin', 'operator', 'app', [
    'orders:read',
    'assign:invoke',
  ]);
  const east = await catalog.resolveIdentity('service', 'east'),
    west = await catalog.resolveIdentity('service', 'west');
  await store.activate(
    'service',
    'orders',
    schema,
    [
      { uid: east, region: 'East', assignee: null, note: 'private' },
      { uid: west, region: 'West', assignee: null, note: 'private' },
    ],
    epoch,
    '0',
  );
  const region: Expr = { op: 'eq', args: [{ field: 'region' }, { principalAttribute: 'region' }] };
  const service = new ObjectService(store, authority, {
    schema,
    policies: {
      resource: 'orders',
      markings: ['sensitive'],
      properties,
      sourceColumns: properties,
      attributes: {
        region: { type: 'string', nullable: false },
        isOperator: { type: 'boolean', nullable: false },
        isAuditor: { type: 'boolean', nullable: false },
      },
      row: region,
      propertyRead: { note: { principalAttribute: 'isAuditor' } },
    },
    writePolicy: region,
    propertyWrite: { assignee: { principalAttribute: 'isOperator' } },
  });
  const session = { environment: 'service', principal: 'operator', application: 'app' };
  assert.deepEqual((await service.read(session, east)).object, {
    uid: east,
    region: 'East',
    assignee: null,
  });
  await assert.rejects(service.read(session, west), { code: 'NOT_FOUND' });
  await assert.rejects(service.read(session, randomUUID()), { code: 'NOT_FOUND' });
  await assert.rejects(service.read(session, randomUUID(), ['note']), {
    code: 'PROPERTY_FORBIDDEN',
  });
  const action: ActionBinding = {
    resource: 'assign',
    revision: 'v1',
    markings: ['sensitive'],
    targetParameter: 'uid',
    inputs: { uid: properties.uid, assignee: { type: 'string', nullable: false } },
    condition: { op: 'isNull', args: [{ field: 'assignee' }] },
    edits: [{ op: 'set', property: 'assignee', value: { parameter: 'assignee' } }],
  };
  const input = { uid: east, assignee: 'Bob' };
  const receipt = await service.invoke(session, action, 'one', input);
  assert.deepEqual(
    await service.invoke(session, { ...action, revision: 'v2' }, 'one', input),
    receipt,
  );
  assert.equal((await service.read(session, east)).object.assignee, 'Bob');
  await assert.rejects(service.invoke(session, action, 'two', { uid: east, assignee: 'Carol' }), {
    code: 'ACTION_CONDITION_FAILED',
  });
  await assert.rejects(
    service.invoke(
      session,
      {
        ...action,
        condition: { literal: true },
        edits: [{ op: 'set', property: 'region', value: { literal: 'West' } }],
      },
      'move',
      input,
    ),
    { code: 'FORBIDDEN' },
  );
  assert.equal((await service.read(session, east)).object.region, 'East');
  await authority.setGrants('service', 'admin', 'operator', 'app', []);
  await assert.rejects(service.read(session, east), { code: 'FORBIDDEN' });
  await assert.rejects(service.invoke(session, action, 'one', input), { code: 'FORBIDDEN' });
});

test('workload HTTP authenticates server-side identity and rechecks revoked sessions inside operations', async () => {
  const authority = new Authority(catalog),
    store = new ObjectStore(catalog);
  await authority.migrate();
  await store.migrate();
  await catalog.ensureEnvironment('http');
  const properties: Record<string, ValueType> = {
    uid: { type: 'uid', nullable: false },
    region: { type: 'string', nullable: false },
    assignee: { type: 'string', nullable: true },
  };
  const schema = { primaryKey: 'uid', properties };
  await authority.syncPrincipal('http', 'setup', 'worker', {
    kind: 'workload',
    enabled: true,
    attributes: { region: 'East' },
    markings: [],
  });
  await authority.setApplication('http', 'setup', 'app', ['orders:read', 'assign:invoke']);
  const epoch = await authority.setGrants('http', 'setup', 'worker', 'app', [
    'orders:read',
    'assign:invoke',
  ]);
  const uid = await catalog.resolveIdentity('http', 'order');
  await store.activate(
    'http',
    'orders',
    schema,
    [{ uid, region: 'East', assignee: null }],
    epoch,
    '0',
  );
  const service = new ObjectService(store, authority, {
    schema,
    policies: {
      resource: 'orders',
      markings: [],
      properties,
      sourceColumns: properties,
      attributes: { region: { type: 'string', nullable: false } },
      row: { op: 'eq', args: [{ field: 'region' }, { principalAttribute: 'region' }] },
    },
  });
  const action: ActionBinding = {
    resource: 'assign',
    revision: '1',
    markings: [],
    inputs: { uid: properties.uid, assignee: { type: 'string', nullable: false } },
    targetParameter: 'uid',
    condition: { literal: true },
    edits: [{ op: 'set', property: 'assignee', value: { parameter: 'assignee' } }],
  };
  const sessions = new WorkloadSessions(authority),
    session = { environment: 'http', principal: 'worker', application: 'app' };
  const credential = await sessions.mint(session, 'setup');
  const server = createApi(sessions, {
    objects: new Map([['orders', service]]),
    actions: new Map([['assign', { service, action }]]),
  });
  await new Promise<void>((done) => server.listen(0, '127.0.0.1', done));
  const base = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  const headers = {
    authorization: `Bearer ${credential.token}`,
    'content-type': 'application/json',
    'idempotency-key': 'http-one',
  };
  try {
    assert.equal((await fetch(`${base}/api/v1/objects/orders/${uid}`)).status, 401);
    assert.equal((await fetch(`${base}/api/v1/objects/orders/${uid}`, { headers })).status, 200);
    assert.equal((await fetch(`${base}/v1/objects/orders/${uid}`, { headers })).status, 200);
    assert.equal(
      (await fetch(`${base}/management/v1/objects/orders/${uid}`, { headers })).status,
      404,
    );
    const page = await fetch(`${base}/api/v1/objects/orders/query`, {
      method: 'POST',
      headers,
      body: JSON.stringify({ pageSize: 1 }),
    });
    assert.equal(page.status, 200);
    assert.equal((await page.json()).objects[0].uid, uid);
    assert.equal(
      (
        await fetch(`${base}/api/v1/objects/orders/query`, {
          method: 'POST',
          headers,
          body: JSON.stringify({ sort: ['uid'] }),
        })
      ).status,
      422,
    );
    assert.equal(
      (
        await fetch(`${base}/api/v1/objects/orders/query`, {
          method: 'POST',
          headers,
          body: JSON.stringify({ principal: 'admin' }),
        })
      ).status,
      400,
    );
    const payload = { input: { uid, assignee: 'Bob' } };
    const malformed = Buffer.concat([
      Buffer.from(`{"input":{"uid":"${uid}","assignee":"`),
      Buffer.from([0xc3, 0x28]),
      Buffer.from('"}}'),
    ]);
    const invalid = await fetch(`${base}/api/v1/actions/assign/invoke`, {
      method: 'POST',
      headers,
      body: malformed,
    });
    assert.equal(invalid.status, 400);
    const invalidBody = await invalid.json();
    assert.equal(invalidBody.error.code, 'INPUT_INVALID');
    assert.equal(invalidBody.error.retryable, false);
    assert.equal(invalidBody.error.correlationId, invalidBody.error.requestId);
    assert.deepEqual(invalidBody.error.details, {});
    assert.equal((await fetch(`${base}/api/v1/objects/orders/%FF`, { headers })).status, 400);
    assert.equal(
      (
        await fetch(`${base}/api/v1/objects/orders/${uid}?properties=uid&properties=assignee`, {
          headers,
        })
      ).status,
      400,
    );
    assert.equal(
      (
        await fetch(`${base}/api/v1/objects/orders/query`, {
          method: 'POST',
          headers: { ...headers, 'content-encoding': 'gzip' },
          body: '{}',
        })
      ).status,
      400,
    );
    const first = await fetch(`${base}/api/v1/actions/assign/invoke`, {
      method: 'POST',
      headers,
      body: JSON.stringify(payload),
    });
    assert.equal(first.status, 200);
    const receipt = await first.json();
    const aliasReplay = await fetch(`${base}/v1/actions/assign/invoke`, {
      method: 'POST',
      headers,
      body: JSON.stringify(payload),
    });
    assert.equal(aliasReplay.status, 200);
    assert.deepEqual(await aliasReplay.json(), receipt);
    const again = await fetch(`${base}/api/v1/actions/assign/invoke`, {
      method: 'POST',
      headers,
      body: JSON.stringify(payload),
    });
    assert.deepEqual(await again.json(), receipt);
    assert.equal(
      (
        await fetch(`${base}/api/v1/actions/assign/invoke`, {
          method: 'POST',
          headers,
          body: JSON.stringify({ ...payload, principal: 'admin' }),
        })
      ).status,
      400,
    );
    assert.equal(
      (
        await fetch(`${base}/api/v1/actions/assign/invoke`, {
          method: 'POST',
          headers,
          body: JSON.stringify({ input: { uid, assignee: 'x'.repeat(70000) } }),
        })
      ).status,
      413,
    );
    // Mixed readers/writers previously exhausted query serialization retries:
    // readers must acquire their shared gate before taking an MVCC snapshot.
    await Promise.all(
      Array.from({ length: 20 }, async (_, client) => {
        for (let step = 0; step < 10; step++) {
          const writing = (client + step) % 5 === 0;
          const response = await fetch(
            `${base}/api/v1/${writing ? 'actions/assign/invoke' : 'objects/orders/query'}`,
            {
              method: 'POST',
              headers: { ...headers, 'idempotency-key': `mixed-${client}-${step}` },
              body: JSON.stringify(writing ? payload : { pageSize: 1 }),
            },
          );
          assert.equal(response.status, 200, JSON.stringify(await response.json()));
        }
      }),
    );
    // Inject PostgreSQL failures through the real transaction/HTTP boundary.
    // This tests classification and rollback, not a natural deadlock workload.
    const originalQuery = service.query.bind(service);
    let attempts = 0;
    service.query = () =>
      catalog.transaction(async (c) => {
        attempts++;
        await c.query('CREATE TABLE moria.retry_rollback_probe(id integer)');
        await c.query(
          "DO $$ BEGIN RAISE EXCEPTION 'private diagnostic' USING ERRCODE='40001'; END $$",
        );
        return { revision: '0', objects: [] };
      }, 'http');
    const exhausted = await fetch(`${base}/api/v1/objects/orders/query`, {
      method: 'POST',
      headers,
      body: '{}',
    });
    assert.equal(exhausted.status, 503);
    const contention = await exhausted.json();
    assert.equal(contention.error.code, 'CONCURRENT_MODIFICATION');
    assert.equal(contention.error.retryable, true);
    assert.equal(attempts, 3);
    assert.equal(
      (await pool.query("SELECT to_regclass('moria.retry_rollback_probe') AS name")).rows[0].name,
      null,
    );
    service.query = () =>
      catalog.transaction(async (c) => {
        await c.query("SET LOCAL statement_timeout='5ms'");
        await c.query('SELECT pg_sleep(1)');
        return { revision: '0', objects: [] };
      }, 'http');
    const timeout = await fetch(`${base}/api/v1/objects/orders/query`, {
      method: 'POST',
      headers,
      body: '{}',
    });
    assert.equal(timeout.status, 504);
    assert.equal((await timeout.json()).error.code, 'QUERY_TIMEOUT');
    const blocker = await pool.connect();
    try {
      await blocker.query('BEGIN');
      await blocker.query('SELECT pg_advisory_xact_lock(738284)');
      service.query = () =>
        catalog.transaction(async (c) => {
          await c.query("SET LOCAL lock_timeout='5ms'");
          await c.query('SELECT pg_advisory_xact_lock(738284)');
          return { revision: '0', objects: [] };
        }, 'http');
      const busy = await fetch(`${base}/api/v1/objects/orders/query`, {
        method: 'POST',
        headers,
        body: '{}',
      });
      assert.equal(busy.status, 503);
      assert.equal((await busy.json()).error.code, 'RESOURCE_BUSY');
    } finally {
      await blocker.query('ROLLBACK');
      blocker.release();
    }
    service.query = () =>
      catalog.transaction(async (c) => {
        await c.query("DO $$ BEGIN RAISE EXCEPTION 'private diagnostic'; END $$");
        return { revision: '0', objects: [] };
      }, 'http');
    const unknown = await fetch(`${base}/api/v1/objects/orders/query`, {
      method: 'POST',
      headers,
      body: '{}',
    });
    assert.equal(unknown.status, 500);
    const failure = await unknown.json();
    assert.equal(failure.error.message, 'Request failed');
    assert.equal(failure.error.code, 'INTERNAL');
    service.query = originalQuery;
    assert.equal(
      (await fetch(`${base}/api/v1/objects/orders/query`, { method: 'POST', headers, body: '{}' }))
        .status,
      200,
    );
    const authenticated = await sessions.authenticate(headers.authorization);
    await sessions.revoke(authenticated, 'setup');
    assert.equal((await fetch(`${base}/api/v1/objects/orders/${uid}`, { headers })).status, 401);
    // Revocation after HTTP authentication but before a protected operation.
    await assert.rejects(service.read(authenticated, uid), { code: 'UNAUTHENTICATED' });
    await assert.rejects(service.query(authenticated, {}), { code: 'UNAUTHENTICATED' });
    await assert.rejects(service.invoke(authenticated, action, 'http-one', payload.input), {
      code: 'UNAUTHENTICATED',
    });
  } finally {
    server.closeAllConnections();
    await new Promise<void>((done) => server.close(() => done()));
  }
});

test('SQL query pages pin data, recheck current source/overlays and bind cursor identity, contract and lifetime', async () => {
  const authority = new Authority(catalog),
    store = new ObjectStore(catalog);
  await authority.migrate();
  await store.migrate();
  await catalog.ensureEnvironment('pages');
  const properties: Record<string, ValueType> = {
    uid: { type: 'uid', nullable: false },
    region: { type: 'string', nullable: false },
    note: { type: 'string', nullable: true },
    amount: { type: 'int64', nullable: false },
    tags: { type: 'array', nullable: true, items: { type: 'string', nullable: false } },
    title: { type: 'string', nullable: false },
  };
  const schema = { primaryKey: 'uid', properties };
  for (const principal of ['reader', 'other'])
    await authority.syncPrincipal('pages', 'setup', principal, {
      kind: 'workload',
      enabled: true,
      attributes: { region: 'East', auditor: false },
      markings: ['sensitive'],
    });
  await authority.setApplication('pages', 'setup', 'app', ['orders:read']);
  await authority.setGrants('pages', 'setup', 'other', 'app', ['orders:read']);
  const epoch = await authority.setGrants('pages', 'setup', 'reader', 'app', ['orders:read']);
  const ids = (
    await Promise.all(
      Array.from({ length: 10 }, (_, i) => catalog.resolveIdentity('pages', String(i))),
    )
  ).sort();
  const rows = ids.map((uid, i) => ({
    uid,
    region: i < 8 ? 'East' : 'West',
    note: 'secret',
    amount: String(9007199254740992n + BigInt(i)),
    tags: i === 0 ? null : ['a', 'b'],
    title: 'old',
  }));
  const initial = await store.activate('pages', 'orders', schema, rows, epoch, '0');
  const region: Expr = { op: 'eq', args: [{ field: 'region' }, { principalAttribute: 'region' }] };
  const binding = {
    schema,
    policies: {
      resource: 'orders',
      markings: ['sensitive'],
      properties,
      sourceColumns: properties,
      attributes: {
        region: { type: 'string', nullable: false } as ValueType,
        auditor: { type: 'boolean', nullable: false } as ValueType,
      },
      row: region,
      objectRead: region,
      propertyRead: { note: { principalAttribute: 'auditor' } as Expr },
    },
  };
  const service = new ObjectService(store, authority, binding),
    session = { environment: 'pages', principal: 'reader', application: 'app' };
  const first = await service.query(session, { pageSize: 2 });
  assert.deepEqual(
    first.objects.map((r) => r.uid),
    ids.slice(0, 2),
  );
  assert.ok(first.nextCursor);
  assert.ok(first.objects.every((r) => !Object.hasOwn(r, 'note')));
  const concurrent = await Promise.all(
    Array.from({ length: 40 }, () =>
      service.query(session, { pageSize: 2, cursor: first.nextCursor }),
    ),
  );
  for (const page of concurrent)
    assert.deepEqual(
      page.objects.map((r) => r.uid),
      ids.slice(2, 4),
    );
  await assert.rejects(
    service.query({ ...session, principal: 'other' }, { pageSize: 2, cursor: first.nextCursor }),
    { code: 'CURSOR_INVALID' },
  );
  await assert.rejects(service.query(session, { pageSize: 3, cursor: first.nextCursor }), {
    code: 'CURSOR_INVALID',
  });
  const changed = new ObjectService(store, authority, {
    ...binding,
    policies: { ...binding.policies, objectRead: { literal: true } as Expr },
  });
  await assert.rejects(changed.query(session, { pageSize: 2, cursor: first.nextCursor }), {
    code: 'CURSOR_INVALID',
  });
  await assert.rejects(
    service.query(session, {
      filter: { op: 'eq', args: [{ field: 'note' }, { literal: 'never' }] },
    }),
    { code: 'PROPERTY_FORBIDDEN' },
  );
  await assert.rejects(
    service.query(session, { projection: ['note'], filter: { literal: false } }),
    { code: 'PROPERTY_FORBIDDEN' },
  );
  await assert.rejects(
    service.query(session, { filter: { op: 'eq', args: [{ field: 'uid' }, { literal: null }] } }),
    { code: 'POLICY_INVALID' },
  );
  await assert.rejects(service.query(session, { filter: { op: 'and', args: [null] } }), {
    code: 'QUERY_INVALID',
  });
  const filtered = await service.query(session, {
    filter: {
      op: 'and',
      args: [
        { op: 'gt', args: [{ field: 'amount' }, { literal: '9007199254740997' }] },
        { op: 'contains', args: [{ field: 'tags' }, { literal: 'a' }] },
      ],
    },
  });
  assert.deepEqual(
    filtered.objects.map((r) => r.uid),
    ids.slice(6, 8),
  );
  assert.deepEqual(
    (
      await service.query(session, { filter: { op: 'isNull', args: [{ field: 'tags' }] } })
    ).objects.map((r) => r.uid),
    [ids[0]],
  );
  assert.deepEqual(
    (
      await service.query(session, {
        filter: { op: 'eq', args: [{ field: 'title' }, { literal: "' OR TRUE; --" }] },
      })
    ).objects,
    [],
  );
  const edit = async (index: number, edits: Parameters<ObjectStore['edit']>[4]) =>
    (
      await store.edit(
        {
          environment: 'pages',
          application: 'app',
          principal: 'admin',
          action: 'edit',
          actionRevision: '1',
          key: `edit-${index}`,
          input: { index },
          securityEpoch: epoch,
        },
        'orders',
        ids[index],
        schema,
        edits,
        () => {},
      )
    ).revision;
  await edit(2, [{ op: 'set', property: 'region', value: 'West' }]);
  await edit(3, [{ op: 'delete' }]);
  const current = await edit(6, [{ op: 'set', property: 'title', value: 'new' }]);
  const refreshed = rows
    .filter((_, i) => i !== 5)
    .map((r, i) => (r.uid === ids[4] ? { ...r, region: 'West' } : r));
  await store.activate('pages', 'orders', schema, refreshed, epoch, current);
  const second = await service.query(session, { pageSize: 2, cursor: first.nextCursor });
  assert.equal(second.revision, initial);
  assert.deepEqual(
    second.objects.map((r) => r.uid),
    ids.slice(6, 8),
  );
  assert.equal(second.objects[0].title, 'old');
  assert.equal(second.nextCursor, undefined);
  assert.deepEqual(await service.query(session, { pageSize: 2, cursor: first.nextCursor }), second);
  const fresh = await service.query(session, { pageSize: 1 });
  assert.ok(fresh.nextCursor);
  const next = await service.query(session, { pageSize: 1, cursor: fresh.nextCursor });
  assert.ok(next.nextCursor);
  const expiries = (
    await pool.query(
      "SELECT expires_at FROM moria.read_cursors WHERE environment='pages' ORDER BY expires_at DESC LIMIT 2",
    )
  ).rows;
  assert.equal(expiries[0].expires_at.toISOString(), expiries[1].expires_at.toISOString());
  await pool.query(
    "UPDATE moria.read_cursors SET expires_at=clock_timestamp()-interval '1 second' WHERE environment='pages'",
  );
  await assert.rejects(service.query(session, { pageSize: 1, cursor: fresh.nextCursor }), {
    code: 'CURSOR_EXPIRED',
  });
  const live = await service.query(session, { pageSize: 1 });
  await authority.setGrants('pages', 'setup', 'reader', 'app', ['orders:read']);
  await assert.rejects(service.query(session, { pageSize: 1, cursor: live.nextCursor }), {
    code: 'CURSOR_INVALIDATED',
  });
});

test(
  'catalog-resolved Iceberg indexing preserves provenance and rechecks access at publication',
  { timeout: 60000 },
  async () => {
    const environment = 'index-coordinator',
      authority = new Authority(catalog);
    await authority.migrate();
    await catalog.ensureEnvironment(environment);
    await catalog.ensureDataset(environment, 'canonical');
    await authority.syncPrincipal(environment, 'admin', 'worker', {
      enabled: true,
      kind: 'workload',
      attributes: {},
      markings: ['sensitive'],
    });
    await authority.setApplication(environment, 'admin', 'indexer', [
      'canonical:read',
      'orders:index',
    ]);
    let epoch = await authority.setGrants(environment, 'admin', 'worker', 'indexer', [
      'canonical:read',
      'orders:index',
    ]);
    const uid = await catalog.resolveIdentity(environment, 'one');
    const pin = await iceberg({
      operation: 'stage',
      warehouse: resolve('.local', `index-${randomUUID()}`),
      schema: {
        type: 'struct',
        'schema-id': 0,
        fields: [{ id: 1, name: 'uid', required: true, type: 'string' }],
      },
      rows: [{ uid }],
    });
    const fence = await catalog.claim(environment, 'source', 'worker');
    await catalog.publish({
      environment,
      execution: 'source',
      worker: 'worker',
      fence,
      securityEpoch: epoch,
      lineage: {},
      outputs: [{ dataset: 'canonical', expectedRevision: '0', ...pin }],
    });
    let beforePublish: () => Promise<void> = async () => {};
    class GuardedStore extends ObjectStore {
      override async publishStage(...args: Parameters<ObjectStore['publishStage']>) {
        await beforePublish();
        return super.publishStage(...args);
      }
    }
    const store = new GuardedStore(catalog);
    await store.migrate();
    const binding = {
      source: { kind: 'dataset' as const, resource: 'canonical', markings: ['sensitive'] },
      target: {
        resource: 'orders',
        markings: ['sensitive'],
        schema: {
          primaryKey: 'uid',
          properties: { uid: { type: 'uid' as const, nullable: false } },
        },
      },
    };
    assert.throws(
      () =>
        new IcebergIndexer(store, authority, {
          ...binding,
          target: { ...binding.target, markings: [] },
        }),
      { code: 'MARKING_DOWNGRADE' },
    );
    assert.throws(
      () =>
        new IcebergIndexer(store, authority, {
          ...binding,
          source: { ...binding.source, kind: 'restrictedView' as any },
        }),
      { code: 'SOURCE_INVALID' },
    );
    const indexer = new IcebergIndexer(store, authority, binding);
    await indexer.migrate();
    const session = { environment, principal: 'worker', application: 'indexer' };
    const lease = {
      execution: 'index',
      worker: 'worker',
      fence: await catalog.claim(environment, 'index', 'worker', 300),
    };
    await assert.rejects(indexer.run({ ...session, sessionHash: 'expired-session' }, lease), {
      code: 'UNAUTHENTICATED',
    });
    const result = await indexer.run(session, lease);
    const provenance = (
      await pool.query('SELECT * FROM moria.index_provenance WHERE stage=$1', [result.stage])
    ).rows[0];
    assert.equal(provenance.snapshot_id, pin.snapshotId);
    assert.equal(provenance.metadata_location, pin.metadataLocation);
    assert.equal(provenance.principal, 'worker');
    assert.equal(provenance.source_dataset, 'canonical');
    const c = await pool.connect();
    try {
      assert.deepEqual((await store.read(c, environment, 'orders', uid, result.revision))!.source, {
        uid,
      });
    } finally {
      c.release();
    }
    const cancelled = new AbortController();
    beforePublish = async () => {
      cancelled.abort();
    };
    await assert.rejects(indexer.runSupervised(session, lease, { signal: cancelled.signal }), {
      code: 'CANCELLED',
    });
    assert.equal(
      (
        await pool.query('SELECT count(*) FROM moria.source_generations WHERE environment=$1', [
          environment,
        ])
      ).rows[0].count,
      '1',
    );
    beforePublish = async () => {
      await authority.setGrants(environment, 'admin', 'worker', 'indexer', ['orders:index']);
    };
    await assert.rejects(indexer.run(session, lease), { code: 'AUTHORIZATION_CHANGED' });
    assert.equal(
      (
        await pool.query('SELECT count(*) FROM moria.source_generations WHERE environment=$1', [
          environment,
        ])
      ).rows[0].count,
      '1',
    );
    const stages = (
      await pool.query('SELECT count(*) FROM moria.source_stages WHERE environment=$1', [
        environment,
      ])
    ).rows[0].count;
    await assert.rejects(indexer.run(session, lease), { code: 'FORBIDDEN' });
    assert.equal(
      (
        await pool.query('SELECT count(*) FROM moria.source_stages WHERE environment=$1', [
          environment,
        ])
      ).rows[0].count,
      stages,
    );
  },
);

test(
  'local lease supervision renews a live fence and aborts on replacement',
  { timeout: 15000 },
  async () => {
    const environment = 'supervision';
    await catalog.ensureEnvironment(environment);
    const lease = {
      execution: 'job',
      worker: 'worker',
      fence: await catalog.claim(environment, 'job', 'worker', 1),
    };
    const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
    await withLease(
      catalog,
      environment,
      lease,
      async (signal) => {
        await sleep(2200);
        assert.equal(signal.aborted, false);
        const live = await pool.query(
          'SELECT 1 FROM moria.leases WHERE environment=$1 AND expires_at>clock_timestamp()',
          [environment],
        );
        assert.equal(live.rowCount, 1);
      },
      { seconds: 1 },
    );
    const expiry = (
      await pool.query('SELECT expires_at FROM moria.leases WHERE environment=$1', [environment])
    ).rows[0].expires_at;
    await sleep(450);
    assert.deepEqual(
      (await pool.query('SELECT expires_at FROM moria.leases WHERE environment=$1', [environment]))
        .rows[0].expires_at,
      expiry,
    );
    await withLease(
      catalog,
      environment,
      lease,
      async (signal) => {
        await pool.query(
          "UPDATE moria.leases SET expires_at=clock_timestamp()-interval '1 second' WHERE environment=$1",
          [environment],
        );
        await catalog.claim(environment, 'job', 'replacement', 10);
        await new Promise<void>((resolve) => {
          if (signal.aborted) resolve();
          else signal.addEventListener('abort', () => resolve(), { once: true });
        });
        signal.throwIfAborted();
      },
      { seconds: 1 },
    ).then(
      () => assert.fail('Lost worker must abort'),
      (error) => assert.equal(error.code, 'LEASE_LOST'),
    );
    await assert.rejects(
      withLease(catalog, environment, lease, async () => assert.fail('Stale lease must not start')),
      { code: 'LEASE_LOST' },
    );
  },
);

test(
  'supervision deadlines cancel work without reclassifying a committed result',
  { timeout: 10000 },
  async () => {
    const environment = 'supervision-deadline';
    await catalog.ensureEnvironment(environment);
    const lease = {
      execution: 'job',
      worker: 'worker',
      fence: await catalog.claim(environment, 'job', 'worker', 10),
    };
    await assert.rejects(
      withLease(
        catalog,
        environment,
        lease,
        async (signal) => {
          await new Promise<void>((resolve) => {
            if (signal.aborted) resolve();
            else signal.addEventListener('abort', () => resolve(), { once: true });
          });
          signal.throwIfAborted();
        },
        { timeoutMs: 100 },
      ),
      { code: 'WORKER_TIMEOUT' },
    );
    let called = false;
    await assert.rejects(
      withLease(
        catalog,
        environment,
        lease,
        async () => {
          called = true;
        },
        { timeoutMs: 0 },
      ),
      { code: 'DEADLINE_INVALID' },
    );
    assert.equal(called, false);
    const cancel = new AbortController();
    const result = await withLease(
      catalog,
      environment,
      lease,
      async () => {
        // This synthetic commit probe is not part of the application schema.
        await pool.query('CREATE TABLE public.deadline_commit_winner(id integer PRIMARY KEY)');
        await pool.query('INSERT INTO public.deadline_commit_winner VALUES(1)');
        cancel.abort();
        return 'committed';
      },
      { signal: cancel.signal },
    );
    assert.equal(result, 'committed');
    assert.equal(
      (await pool.query('SELECT count(*) FROM public.deadline_commit_winner')).rows[0].count,
      '1',
    );
  },
);

test('published markings constrain reads, queries, action replay and subsequent refreshes', async () => {
  const environment = 'generation-markings',
    store = new ObjectStore(catalog),
    authority = new Authority(catalog);
  await store.migrate();
  await authority.migrate();
  await catalog.ensureEnvironment(environment);
  await authority.syncPrincipal(environment, 'admin', 'reader', {
    kind: 'workload',
    enabled: true,
    attributes: {},
    markings: ['sensitive', 'extra'],
  });
  await authority.setApplication(environment, 'admin', 'app', ['orders:read', 'touch:invoke']);
  const epoch = await authority.setGrants(environment, 'admin', 'reader', 'app', [
    'orders:read',
    'touch:invoke',
  ]);
  const uid = await catalog.resolveIdentity(environment, 'one'),
    schema = { primaryKey: 'uid', properties: { uid: { type: 'uid' as const, nullable: false } } };
  const labels = ['sensitive'];
  const stage = await store.stage(
    environment,
    'orders',
    schema,
    [{ uid }],
    epoch,
    '0',
    undefined,
    labels,
  );
  labels.length = 0;
  let revision = await store.publishStage(stage);
  const session = { environment, principal: 'reader', application: 'app' };
  const binding = {
    schema,
    policies: {
      resource: 'orders',
      markings: ['sensitive'],
      properties: schema.properties,
      sourceColumns: schema.properties,
      attributes: {},
    },
  };
  const service = new ObjectService(store, authority, binding),
    weak = new ObjectService(store, authority, {
      ...binding,
      policies: { ...binding.policies, markings: [] },
    });
  assert.deepEqual((await service.read(session, uid)).object, { uid });
  assert.equal((await service.query(session, {})).objects.length, 1);
  await assert.rejects(weak.read(session, uid), { code: 'MARKING_DOWNGRADE' });
  await assert.rejects(weak.query(session, {}), { code: 'MARKING_DOWNGRADE' });
  const action: ActionBinding = {
    resource: 'touch',
    revision: '1',
    markings: [],
    inputs: { uid: schema.properties.uid },
    targetParameter: 'uid',
    condition: { literal: true },
    edits: [],
  };
  const receipt = await service.invoke(session, action, 'one', { uid });
  revision = receipt.revision;
  await assert.rejects(weak.invoke(session, action, 'new', { uid }), { code: 'MARKING_DOWNGRADE' });
  await assert.rejects(weak.invoke(session, action, 'one', { uid }), { code: 'MARKING_DOWNGRADE' });
  const downgrade = await store.stage(environment, 'orders', schema, [{ uid }], epoch, revision);
  await assert.rejects(store.publishStage(downgrade), { code: 'MARKING_DOWNGRADE' });
  assert.equal(
    (await pool.query('SELECT revision FROM moria.environments WHERE id=$1', [environment])).rows[0]
      .revision,
    revision,
  );
  const stronger = await store.stage(
    environment,
    'orders',
    schema,
    [{ uid }],
    epoch,
    revision,
    undefined,
    ['sensitive', 'extra'],
  );
  await store.publishStage(stronger);
  await assert.rejects(service.read(session, uid), { code: 'MARKING_DOWNGRADE' });
  await assert.rejects(service.invoke(session, action, 'one', { uid }), {
    code: 'MARKING_DOWNGRADE',
  });
  const current = new ObjectService(store, authority, {
    ...binding,
    policies: { ...binding.policies, markings: ['sensitive', 'extra'] },
  });
  assert.deepEqual((await current.read(session, uid)).object, { uid });
});

for (const external of [false, true]) {
  test(
    `${external ? 'external PostgreSQL' : 'local'} workspace HTTP preserves overrides through Iceberg refresh and restart, enforcing demo access`,
    { timeout: 90000 },
    async (t) => {
      let workspaceCatalog = external ? await externalCatalog(t) : catalog;
      const { createWorkspace } = await import('../apps/local/src/workspace.js');
      const { createLocalServer } = await import('../apps/local/src/server.js');
      const warehouse = resolve('.local', `workspace-test-${randomUUID()}`);
      let workspace = await createWorkspace(workspaceCatalog, warehouse);
      const key = randomUUID();
      let server = createLocalServer(workspace, key);
      const start = async () => {
        await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
        return `http://127.0.0.1:${(server.address() as { port: number }).port}`;
      };
      let base = await start();
      const call = async (
        path: string,
        profile = 'operator',
        data?: unknown,
        extra: Record<string, string> = {},
      ) => {
        const response = await fetch(base + path, {
          method: data === undefined ? 'GET' : 'POST',
          headers: {
            'content-type': 'application/json',
            'x-moria-key': key,
            'x-moria-profile': profile,
            ...extra,
          },
          ...(data === undefined ? {} : { body: JSON.stringify(data) }),
        });
        return { status: response.status, data: (await response.json()) as any };
      };
      const source = [
        { id: 'west', title: 'West pump', region: 'West', status: 'Original', notes: '' },
        { id: 'east', title: 'East pump', region: 'East', status: 'Original', notes: '' },
      ];
      try {
        assert.equal((await fetch(base + '/local/status')).status, 401);
        assert.equal((await call('/management/v1/apps', 'west-editor')).status, 403);
        assert.equal((await call('/management/v1/apps', 'east-reader')).status, 403);
        assert.equal((await call('/management/v1/apps', 'operator')).status, 200);
        assert.equal((await call('/management/v1/workspace', 'west-editor')).status, 200);
        assert.equal(
          (await call('/local/status', 'operator', undefined, { origin: 'https://other.example' }))
            .status,
          403,
        );
        assert.equal(
          await new Promise<number | undefined>((resolveStatus, reject) => {
            import('node:http').then(({ get }) => {
              get(base + '/local/status', { headers: { host: 'other.example' } }, (response) => {
                response.resume();
                resolveStatus(response.statusCode);
              }).on('error', reject);
            }, reject);
          }),
          403,
        );
        assert.equal(
          (await call('/management/v1/datasets/assets/import', 'west-editor', source)).status,
          403,
        );
        assert.equal(
          (await call('/management/v1/datasets/assets/import', 'operator', source)).status,
          200,
        );
        const query = (profile = 'operator') =>
          call('/api/v1/objects/assets/query', profile, { pageSize: 25 });
        const all = await query();
        assert.equal(all.data.objects.length, 2);
        const uid = all.data.objects.find((row: any) => row.id === 'west').uid;
        assert.equal((await query('west-editor')).data.objects.length, 1);
        assert.equal((await query('east-reader')).data.objects[0].id, 'east');
        assert.equal((await call(`/api/v1/objects/assets/${uid}`, 'east-reader')).status, 404);
        const input = { input: { uid, status: 'My edit' } };
        assert.equal(
          (
            await call('/api/v1/actions/edit-status/invoke', 'east-reader', input, {
              'idempotency-key': 'denied',
            })
          ).status,
          403,
        );
        const headers = { 'idempotency-key': `persistent-edit-${external}` };
        const saved = await call(
          '/api/v1/actions/edit-status/invoke',
          'west-editor',
          input,
          headers,
        );
        assert.equal(saved.status, 200);
        assert.deepEqual(
          (await call('/api/v1/actions/edit-status/invoke', 'west-editor', input, headers)).data,
          saved.data,
        );
        const next = source.map((row) => ({
          ...row,
          title: row.title + ' · updated 日本語',
          status: 'Source changed',
        }));
        assert.equal(
          (await call('/management/v1/datasets/assets/import', 'operator', [...next, next[0]]))
            .status,
          400,
        );
        assert.equal(
          (await call('/management/v1/datasets/assets/import', 'operator', next)).status,
          200,
        );
        let row = (await query('west-editor')).data.objects[0];
        assert.equal(row.uid, uid);
        assert.equal(row.status, 'My edit');
        assert.equal(row.title, 'West pump · updated 日本語');
        const heads = await workspaceCatalog.heads('local-workspace', ['assets-source']);
        assert.equal(
          (
            await iceberg({ operation: 'read', metadataLocation: heads[0].metadataLocation })
          ).rows.find((r: any) => r.id === 'west').status,
          'Source changed',
        );
        await new Promise<void>((r) => server.close(() => r()));
        if ('closeExternal' in workspaceCatalog) {
          await (workspaceCatalog as Awaited<ReturnType<typeof externalCatalog>>).closeExternal();
          assert.equal((await pool.query('SELECT 1 AS alive')).rows[0].alive, 1);
          workspaceCatalog = await externalCatalog(t);
        } else {
          await pool.end();
          await db.stop();
          await db.start();
          pool = new pg.Pool(connection);
          catalog = new Catalog(pool);
          workspaceCatalog = catalog;
        }
        workspace = await createWorkspace(workspaceCatalog, warehouse);
        server = createLocalServer(workspace, key);
        base = await start();
        row = (await query('west-editor')).data.objects[0];
        assert.equal(row.status, 'My edit');
        assert.equal(
          (await call('/management/v1/datasets/assets/import', 'operator', [next[1]])).status,
          200,
        );
        assert.equal((await query('west-editor')).data.objects.length, 0);
        assert.equal(
          (await call('/management/v1/datasets/assets/import', 'operator', next)).status,
          200,
        );
        assert.equal((await query('west-editor')).data.objects[0].status, 'My edit');
        assert.equal(
          (
            await call(
              '/api/v1/actions/reset-asset/invoke',
              'west-editor',
              { input: { uid } },
              { 'idempotency-key': `reset-${external}` },
            )
          ).status,
          200,
        );
        assert.equal((await query('west-editor')).data.objects[0].status, 'Source changed');
      } finally {
        await new Promise<void>((r) => server.close(() => r()));
      }
    },
  );
}

test(
  'SNOTEL sync persists Iceberg, atomically serves corrections and enforces app scope',
  { timeout: 120000 },
  async (t) => {
    const catalog = await externalCatalog(t);
    const { createSnowService, snowViewer } = await import('../services/snotel/src/service.js');
    const { createScopedAppServer } = await import('../services/app-host/src/scoped.js');
    const { AppHost } = await import('../services/app-host/src/store.js');
    const { snowConfig, snowFixture, reportFixture } = await import('./fixtures/snow.js');
    const snow = await createSnowService(catalog, resolve('.local', `snow-test-${randomUUID()}`));
    const first = await snow.sync(snowConfig, snowFixture());
    assert.equal(first.observations, 3);
    const initialHeads = await catalog.heads('snotel', [
      'snow-stations-source',
      'snow-observations-source',
      'snow-raw-source',
    ]);
    await assert.rejects(
      snow.sync(snowConfig, snowFixture(), async (client) => {
        if (client) throw new Error('initiating administrator revoked before commit');
      }),
      /initiating administrator revoked/,
    );
    assert.deepEqual(
      await catalog.heads('snotel', [
        'snow-stations-source',
        'snow-observations-source',
        'snow-raw-source',
      ]),
      initialHeads,
    );

    const { BrowserSessions } = await import('../services/security/src/browser-sessions.js');
    const { AppAuth } = await import('../services/app-host/src/auth.js');
    const binding = {
      environment: 'snotel',
      application: 'snow-browser',
      origin: 'http://127.0.0.1:3211',
      entryPath: '/snow/',
      initialOperations: ['snow-stations:read', 'snow-observations:read'],
      recoveryPrincipal: 'snow-reader',
    };
    const browserSessions = new BrowserSessions(snow.authority, binding, [], Buffer.alloc(32, 1));
    await browserSessions.initialize();
    const auth = new AppAuth(browserSessions, 'snow-test-key');
    const cookie = `${auth.cookieName}=${await browserSessions.recover(await browserSessions.recoveryTicket())}`;
    const server = createScopedAppServer({
      app: 'snotel',
      host: new AppHost(resolve('.local', `snow-app-${randomUUID()}`)),
      key: 'snow-test-key',
      auth,
      sessions: snow.sessions,
      token: snow.token,
      status: snow.status,
      objects: new Map([
        ['snow-stations', snow.stations],
        ['snow-observations', snow.observations],
      ]),
    });
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
    const base = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
    binding.origin = base;
    const call = async (path: string, input?: unknown, key = 'snow-test-key') => {
      const r = await fetch(base + path, {
        method: input === undefined ? 'GET' : 'POST',
        headers: {
          'x-moria-app-key': key,
          cookie: key === 'snow-test-key' ? cookie : '',
          origin: base,
          'content-type': 'application/json',
          'x-moria-profile': 'operator',
          authorization: 'Bearer forged',
        },
        ...(input === undefined ? {} : { body: JSON.stringify(input) }),
      });
      return { status: r.status, data: (await r.json()) as any };
    };
    try {
      assert.equal((await call('/api/v1/status', undefined, 'management-key')).status, 401);
      assert.equal(
        (await fetch(base + '/api/v1/status', { headers: { 'x-moria-app-key': 'snow-test-key' } }))
          .status,
        401,
      );
      assert.equal((await call('/management/v1/snotel/sync', snowConfig)).status, 404);
      assert.equal((await call('/api/v1/actions/edit-status/invoke', {})).status, 404);
      assert.equal((await call('/api/v1/objects/assets/query', {})).status, 404);
      await checkSnowApiSurface(base, cookie);
      const sdk = snowSdk({
        baseUrl: base,
        fetch: async (url, init) =>
          fetch(url, {
            ...init,
            headers: { ...init?.headers, cookie, origin: base },
          }),
      });
      assert.equal((await sdk.objects['snow-observations'].all({ pageSize: 1 })).length, 3);
      const filtered = await sdk.objects['snow-observations'].query({
        projection: ['date', 'swe'],
        filter: { op: 'eq', args: [{ field: 'date' }, { literal: '2025-04-01' }] },
      });
      assert.equal(filtered.objects.length, 1);
      assert.equal((await sdk.endpoints.status()).completed?.observation_count, 3);
      const query = () => call('/api/v1/objects/snow-observations/query', { pageSize: 100 });
      let result = await query();
      assert.equal(result.status, 200);
      assert.equal(result.data.objects.length, 3);
      const id = result.data.objects.find((o: any) => o.date === '2025-04-01').uid;
      await snow.sync(snowConfig, snowFixture(18));
      result = await query();
      assert.equal(result.data.objects.length, 3);
      assert.equal(result.data.objects.find((o: any) => o.uid === id).swe, 18);
      const complete = (await snow.status()).completed;
      await assert.rejects(
        snow.sync(snowConfig, async () => {
          throw Error('offline');
        }),
      );
      assert.equal((await snow.status()).completed.id, complete.id);
      assert.equal((await snow.status()).latest.status, 'failed');
      assert.equal((await query()).data.objects.find((o: any) => o.uid === id).swe, 18);
      const heads = await catalog.heads('snotel', ['snow-raw-source']);
      const raw = await iceberg({ operation: 'read', metadataLocation: heads[0].metadataLocation });
      assert.equal(raw.rows.length, 2);
      assert.match(raw.rows[0].body, /Niwot|stationElement/);
      await snow.sync({ ...snowConfig, source: 'report-csv' }, async () => reportFixture);
      assert.equal((await query()).data.objects.find((o: any) => o.uid === id).swe, 12.6);
      const csvHeads = await catalog.heads('snotel', ['snow-raw-source']);
      const csvRaw = await iceberg({
        operation: 'read',
        metadataLocation: csvHeads[0].metadataLocation,
      });
      assert.equal(csvRaw.rows[0].body, reportFixture);
      await snow.authority.setGrants(
        'snotel',
        'test',
        snowViewer.principal,
        snowViewer.application,
        [],
      );
      assert.equal((await query()).status, 403);
      await assert.rejects(
        sdk.objects['snow-observations'].query(),
        (e: unknown) => e instanceof SnowApiError && e.status === 403,
      );

      assert.equal((await call('/api/v1/status')).status, 403);
      const restarted = await createSnowService(catalog, resolve('.local', 'snow-unused'));
      await assert.rejects(restarted.status());
    } finally {
      await new Promise<void>((r) => server.close(() => r()));
    }
  },
);

test('related serving stages roll back together when publication completion fails', async () => {
  const env = 'batch-atomic';
  await catalog.ensureEnvironment(env);
  const store = new ObjectStore(catalog);
  await store.migrate();
  const schema: ObjectSchema = {
    primaryKey: 'uid',
    properties: {
      uid: { type: 'uid', nullable: false },
      name: { type: 'string', nullable: false },
    },
  };
  const state = await pool.query(
    'SELECT revision,security_epoch FROM moria.environments WHERE id=$1',
    [env],
  );
  async function* rows() {
    yield { uid: await catalog.resolveIdentity('batch-atomic', randomUUID()), name: 'pending' };
  }
  const stages = [];
  for (const type of ['first', 'second'])
    stages.push(
      await store.stage(
        env,
        type,
        schema,
        rows(),
        state.rows[0].security_epoch,
        state.rows[0].revision,
      ),
    );
  await assert.rejects(
    store.publishStages(stages, undefined, async () => {
      throw Error('injected completion failure');
    }),
    /injected completion failure/,
  );
  assert.equal(
    (await pool.query('SELECT * FROM moria.source_generations WHERE environment=$1', [env]))
      .rowCount,
    0,
  );
  assert.equal(
    (await pool.query('SELECT revision FROM moria.environments WHERE id=$1', [env])).rows[0]
      .revision,
    state.rows[0].revision,
  );
  await store.publishStages(stages);
  assert.equal(
    (await pool.query('SELECT * FROM moria.source_generations WHERE environment=$1', [env]))
      .rowCount,
    2,
  );
});

import { checkAuth } from './fixtures/auth-checks.js';
test('OIDC browser sessions, scoped grants, refresh, revocation and CLI recovery use real PostgreSQL', async (t) => {
  await checkAuth(await externalCatalog(t));
});

import { checkManagementAuth } from './fixtures/management-auth-checks.js';
test('management Google sessions enforce admin grants, data writes, app isolation and CLI recovery', async (t) => {
  await checkManagementAuth(await externalCatalog(t));
});

import { jobsChecks } from './fixtures/jobs-checks.js';
import { weatherChecks } from './fixtures/weather-checks.js';
import { checkLineage } from './fixtures/lineage-checks.js';
test('durable jobs coalesce, recover, fence and preserve atomic publication', async () => {
  await jobsChecks(catalog);
});
test('weather publication rolls back and replays safely after restart', async () => {
  await weatherChecks(catalog);
});
test('lineage exposes persisted publication and index relationships without private metadata', async () => {
  await checkLineage(catalog);
});

import { snowRecoveryChecks } from './fixtures/snow-recovery-checks.js';
test(
  'durable SNOTEL resumes published Iceberg snapshots without redownload or duplicate serving',
  { timeout: 120000 },
  async () => {
    await snowRecoveryChecks(catalog);
  },
);

async function externalCatalog(t: import('node:test').TestContext, poolMax = 6) {
  const role = `runtime_${randomUUID().replaceAll('-', '')}`;
  const password = randomUUID();
  // Synthetic random credentials live only in memory; no files or logging.
  await pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
  await pool.query(`GRANT USAGE ON SCHEMA moria TO ${role}`);
  await pool.query(`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA moria TO ${role}`);
  await pool.query(`REVOKE UPDATE, DELETE ON moria.security_audit FROM ${role}`);
  await pool.query(`REVOKE ALL ON moria.schema_version FROM ${role}`);
  await pool.query(`GRANT SELECT ON moria.schema_version TO ${role}`);
  await pool.query(`GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`);
  const runtime = new pg.Pool(
    externalDatabase({
      MORIA_DATABASE_SECRET_REF: 'SYNTHETIC_DATABASE',
      SYNTHETIC_DATABASE: `postgresql://${role}:${password}@127.0.0.1:${connection.port}/postgres`,
      MORIA_DATABASE_TLS: 'disable',
      MORIA_DATABASE_POOL_MAX: String(poolMax),
    }),
  );
  const release = await holdSchema(runtime);
  let closed = false;
  const closeExternal = async () => {
    if (closed) return;
    closed = true;
    release();
    await runtime.end();
    await pool.query(`DROP OWNED BY ${role}`);
    await pool.query(`DROP ROLE ${role}`);
    assert.equal((await pool.query('SELECT 1 AS alive')).rows[0].alive, 1);
  };
  t.after(closeExternal);
  await assert.rejects(
    runtime.query('CREATE TABLE moria.forbidden(id integer)'),
    /permission denied/,
  );
  await assert.rejects(schema(pool, 'apply'), /busy/);
  return Object.assign(new Catalog(runtime, 'verify'), { closeExternal });
}

test(
  'external PostgreSQL runs bundled Snow and weather at the minimum pool with a schema lease',
  { timeout: 120000 },
  async (t) => {
    const runtime = await externalCatalog(t, 4);
    const snow = await createSnowService(
      runtime,
      resolve('.local', `pool-minimum-${randomUUID()}`),
    );
    let snowDownloads = 0;
    let weatherDownloads = 0;
    const fixture = snowFixture(23);
    const areas = [
      {
        id: 'synthetic',
        name: 'Synthetic area',
        latitude: 40,
        longitude: -105,
        forecastUrl: 'https://api.weather.gov/gridpoints/BOU/1,1/forecast',
        sourceUpdatedAt: '2025-04-01T00:00:00Z',
        retrievedAt: '2025-04-01T01:00:00Z',
        periods: [],
      },
    ];
    const { jobs, weather } = await createSnowJobs(runtime, snow, {
      snow: async (url) => {
        snowDownloads++;
        await assert.rejects(schema(pool, 'apply'), /busy/);
        return fixture(url);
      },
      weather: async (guard) => {
        weatherDownloads++;
        await guard();
        await assert.rejects(schema(pool, 'apply'), /busy/);
        return { areas, raw: [] };
      },
    });
    await jobs.schedule('snotel', false, 360);
    await jobs.schedule('weather', false, 60);
    // Count checked-out connections, including the already-held lifetime schema lease.
    let active = runtime.pool.totalCount - runtime.pool.idleCount;
    let peak = active;
    const acquired = () => {
      active++;
      peak = Math.max(peak, active);
    };
    const released = () => {
      active--;
    };
    runtime.pool.on('acquire', acquired);
    runtime.pool.on('release', released);
    // Schema lock cleanup is asynchronous after client.release(). Wait for its
    // actual pool release event rather than counting a pending unlock as a leak.
    const drained = async () => {
      if (active === 1) return;
      await new Promise<void>((resolve, reject) => {
        const done = () => {
          if (active !== 1) return;
          clearTimeout(timer);
          runtime.pool.removeListener('release', done);
          resolve();
        };
        const timer = setTimeout(() => {
          runtime.pool.removeListener('release', done);
          reject(new Error('Runtime connections did not drain after schema unlock'));
        }, 5000);
        runtime.pool.on('release', done);
      });
    };
    await drained();
    t.after(() => {
      runtime.pool.removeListener('acquire', acquired);
      runtime.pool.removeListener('release', released);
    });
    for (const kind of ['snotel', 'weather']) {
      peak = active;
      const queued = await jobs.enqueue(
        kind,
        kind === 'snotel' ? snowConfig : {},
        `pool-minimum:${randomUUID()}`,
        'synthetic-operator',
      );
      await jobs.tick();
      const row = (
        await runtime.pool.query(
          'SELECT status, phase, result, attempt, error FROM moria.jobs WHERE id=$1',
          [queued.id],
        )
      ).rows[0];
      assert.equal(row.status, 'succeeded');
      assert.equal(row.phase, 'Published');
      assert.equal(row.attempt, 1);
      assert.equal(row.error, null);
      assert.deepEqual(
        (
          await runtime.pool.query(
            'SELECT status, error, finished_at IS NOT NULL AS finished FROM moria.job_attempts WHERE job_id=$1',
            [queued.id],
          )
        ).rows,
        [{ status: 'succeeded', error: null, finished: true }],
      );
      assert.equal(peak, 4, `${kind} must exercise all four connections`);
      await drained();
      assert.equal(active, 1, 'only the schema lease remains checked out');
      assert.equal(runtime.pool.waitingCount, 0);
      if (kind === 'snotel') {
        assert.equal(snowDownloads, 2);
        // Earlier security regressions revoke the shared default reader. Inspect
        // through the existing ingestion identity without resetting those grants.
        const status = await snow.status({
          environment: 'snotel',
          principal: 'snow-connector',
          application: 'snow-ingestion',
        });
        assert.equal(status.completed.id, queued.id);
        assert.equal(status.latest.status, 'completed');
        assert.equal(status.completed.station_count, 1);
        assert.equal(status.completed.observation_count, 3);
        assert.equal(row.result.snapshots.length, 3);
        assert.equal(row.result.stages.length, 2);
        const heads = await runtime.heads(
          'snotel',
          row.result.snapshots.map((s: any) => s.dataset),
        );
        for (const snapshot of row.result.snapshots) {
          assert.equal(
            heads.find((h) => h.dataset === snapshot.dataset)?.snapshotId,
            snapshot.snapshotId,
          );
        }
        assert.equal(
          (
            await runtime.pool.query(
              "SELECT count(*)::int AS n FROM moria.source_stages WHERE id=ANY($1::uuid[]) AND status='published'",
              [row.result.stages],
            )
          ).rows[0].n,
          2,
        );
      } else {
        assert.equal(weatherDownloads, 1);
        assert.equal(row.result.areaCount, 1);
        const view = await weather.read();
        assert.equal(view.jobId, queued.id);
        assert.deepEqual(view.areas, areas);
        assert.ok(view.publishedAt);
      }
      await assert.rejects(schema(pool, 'apply'), /busy/);
    }
  },
);

test('migration ownership, versioned idempotence and compatibility use real PostgreSQL', async () => {
  // Preserve populated management/Snow/auth state across repeated versioned migrations.
  const snapshot = async () => {
    const tables = await pool.query(
      "SELECT tablename FROM pg_tables WHERE schemaname='moria' AND tablename <> 'schema_version' ORDER BY tablename",
    );
    const rows = [];
    for (const { tablename } of tables.rows) {
      assert.match(tablename, /^[a-z_]+$/);
      rows.push(
        (
          await pool.query(
            `SELECT to_jsonb(t) AS value FROM moria.${tablename} t ORDER BY to_jsonb(t)::text`,
          )
        ).rows,
      );
    }
    return rows;
  };
  const before = await snapshot();
  const blocker = await pool.connect();
  try {
    await blocker.query('BEGIN');
    await blocker.query('SELECT pg_advisory_xact_lock(731804135219::bigint)');
    await assert.rejects(schema(pool, 'apply'), /busy/);
    await schema(pool, 'verify');
    await blocker.query('ROLLBACK');
  } finally {
    blocker.release();
  }
  await schema(pool, 'apply');
  assert.deepEqual(await snapshot(), before);
  for (const version of [0, 999]) {
    await pool.query('UPDATE moria.schema_version SET version=$1', [version]);
    await assert.rejects(schema(pool, 'apply'), /incompatible/);
    await assert.rejects(schema(pool, 'verify'), /incompatible/);
  }
  await pool.query('UPDATE moria.schema_version SET version=1');
  await schema(pool, 'verify');
  const savedChecksum = (await pool.query('SELECT checksum FROM moria.schema_version')).rows[0]
    .checksum;
  await pool.query("UPDATE moria.schema_version SET checksum='incompatible'");
  await assert.rejects(schema(pool, 'apply'), /incompatible/);
  await assert.rejects(schema(pool, 'verify'), /incompatible/);
  await pool.query('UPDATE moria.schema_version SET checksum=$1', [savedChecksum]);
  await assert.rejects(holdSchema(pool), /runtime role must be scoped/);
});

for (const layout of ['empty', 'partial', 'complete']) {
  test(`unversioned ${layout} schema is rejected without changing schema or data`, async () => {
    const name = `migration_${randomUUID().replaceAll('-', '')}`;
    await pool.query(`CREATE DATABASE ${name}`);
    const isolated = new pg.Pool({ ...connection, database: name });
    try {
      if (layout === 'complete') {
        await isolated.query(baseline);
        await new Catalog(isolated).ensureEnvironment('preserved');
      } else {
        await isolated.query('CREATE SCHEMA moria');
        if (layout === 'partial') {
          await isolated.query(
            'CREATE TABLE moria.environments(id integer PRIMARY KEY); INSERT INTO moria.environments VALUES(42)',
          );
        }
      }
      const snapshot = async () => {
        const definitions = await isolated.query(`
          SELECT to_jsonb(n) AS value FROM pg_namespace n WHERE n.nspname='moria'
          UNION ALL
          SELECT to_jsonb(c) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='moria'
          UNION ALL
          SELECT to_jsonb(a) FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
            JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='moria'
          UNION ALL
          SELECT to_jsonb(c) FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace
            WHERE n.nspname='moria'
          ORDER BY value
        `);
        const tables = await isolated.query(
          "SELECT tablename FROM pg_tables WHERE schemaname='moria' ORDER BY tablename",
        );
        const rows = [];
        for (const { tablename } of tables.rows) {
          assert.match(tablename, /^[a-z_]+$/);
          rows.push(
            (
              await isolated.query(
                `SELECT to_jsonb(t) AS value FROM moria.${tablename} t ORDER BY to_jsonb(t)::text`,
              )
            ).rows,
          );
        }
        return { definitions: definitions.rows, rows };
      };
      const before = await snapshot();
      for (let attempt = 0; attempt < 2; attempt++) {
        await assert.rejects(
          schema(isolated, 'apply'),
          /Unversioned Moria schema: recreate the disposable development database explicitly/,
        );
        assert.deepEqual(await snapshot(), before);
        assert.equal(
          (await isolated.query("SELECT to_regclass('moria.schema_version') AS name")).rows[0].name,
          null,
        );
        await assert.rejects(schema(isolated, 'verify'), /schema operation failed/);
      }
    } finally {
      await isolated.end();
      await pool.query(`DROP DATABASE ${name}`);
    }
  });
}

test('lease loss during an active HTTP transaction keeps migration fenced until drain', async (t) => {
  const runtime = await externalCatalog(t);
  const entered = Promise.withResolvers<void>();
  const drain = Promise.withResolvers<void>();
  const leaseLost = Promise.withResolvers<void>();
  runtime.pool.once('error', () => leaseLost.resolve());
  const lease = await pool.query(
    `SELECT pid FROM pg_stat_activity WHERE usename=$1 ORDER BY backend_start LIMIT 1`,
    [runtime.pool.options.user],
  );
  const server = createHttpServer((_request, response) => {
    void runtime
      .transaction(async (client) => {
        await client.query("INSERT INTO moria.environments(id) VALUES('lease-loss-request')");
        entered.resolve();
        await drain.promise;
        await client.query(
          "UPDATE moria.environments SET revision=1 WHERE id='lease-loss-request'",
        );
      })
      .then(
        () => response.end('committed'),
        () => {
          response.statusCode = 500;
          response.end('failed');
        },
      );
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const request = fetch(`http://127.0.0.1:${(server.address() as { port: number }).port}`);
  try {
    await entered.promise;
    await pool.query('SELECT pg_terminate_backend($1)', [lease.rows[0].pid]);
    await leaseLost.promise;
    await assert.rejects(schema(pool, 'apply'), /busy/);
    await assert.rejects(runtime.pool.query('SELECT 1'), /lease lost/);
    drain.resolve();
    assert.equal(await (await request).text(), 'committed');
    await runtime.closeExternal();
    await schema(pool, 'apply');
    assert.equal(
      (await pool.query("SELECT revision FROM moria.environments WHERE id='lease-loss-request'"))
        .rows[0].revision,
      '1',
    );
  } finally {
    drain.resolve();
    await request;
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});
