import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { createServer } from 'node:net';
import { cpus, totalmem, platform, release } from 'node:os';
import { parseArgs } from 'node:util';
import EmbeddedPostgres from 'embedded-postgres';
import pg from 'pg';
import { Catalog } from '../services/catalog/src/catalog.js';
import { ObjectStore, type ObjectSchema } from '../services/ontology/src/store.js';
import { Authority } from '../services/security/src/authority.js';
import { ObjectService, type ActionBinding } from '../services/ontology/src/service.js';

const { values } = parseArgs({
  options: {
    rows: { type: 'string', default: '10000' },
    clients: { type: 'string', default: '20' },
    output: { type: 'string', default: '.local/benchmark.json' },
  },
});
const count = Number(values.rows),
  clients = Number(values.clients);
if (
  !Number.isInteger(count) ||
  count < 100 ||
  count > 1000000 ||
  !Number.isInteger(clients) ||
  clients < 1 ||
  clients > 100
)
  throw new Error('Invalid benchmark dimensions');
const socket = createServer();
await new Promise<void>((r) => socket.listen(0, '127.0.0.1', r));
const port = (socket.address() as { port: number }).port;
await new Promise<void>((r) => socket.close(() => r()));
const directory = resolve('.local', `benchmark-${randomUUID()}`);
await mkdir(directory, { recursive: true });
const password = randomUUID();
const db = new EmbeddedPostgres({
  databaseDir: directory,
  user: 'benchmark',
  password,
  port,
  persistent: true,
  authMethod: 'scram-sha-256',
  initdbFlags: ['--encoding=UTF8', '--locale=C'],
  postgresFlags: ['-c', 'listen_addresses=127.0.0.1'],
  onLog: () => {},
  onError: () => {},
});
let pool: pg.Pool | undefined;
let peakRss = process.memoryUsage().rss;
const sampling = setInterval(() => (peakRss = Math.max(peakRss, process.memoryUsage().rss)), 50);
const percentile = (xs: number[], p: number) =>
  xs.length
    ? xs.slice().sort((a, b) => a - b)[Math.min(xs.length - 1, Math.ceil(xs.length * p) - 1)]
    : null;
async function load(total: number, operation: (index: number) => Promise<void>) {
  let next = 0;
  const latencies: number[] = [],
    failures: Record<string, number> = {};
  const start = performance.now();
  await Promise.all(
    Array.from({ length: clients }, async () => {
      while (next < total) {
        const index = next++,
          begin = performance.now();
        try {
          await operation(index);
          latencies.push(performance.now() - begin);
        } catch (error) {
          const code = (error as any).code ?? 'ERROR';
          failures[code] = (failures[code] ?? 0) + 1;
        }
      }
    }),
  );
  const elapsedMs = performance.now() - start;
  return {
    attempts: total,
    success: latencies.length,
    failures,
    elapsedMs,
    successfulOpsPerSecond: (latencies.length * 1000) / elapsedMs,
    p50Ms: percentile(latencies, 0.5),
    p95Ms: percentile(latencies, 0.95),
    maxMs: latencies.length ? Math.max(...latencies) : null,
  };
}
try {
  await db.initialise();
  await db.start();
  pool = new pg.Pool({
    host: '127.0.0.1',
    port,
    user: 'benchmark',
    password,
    database: 'postgres',
    max: clients,
  });
  const catalog = new Catalog(pool),
    store = new ObjectStore(catalog);
  await catalog.migrate();
  await store.migrate();
  await catalog.ensureEnvironment('bench');
  const schema: ObjectSchema = {
    primaryKey: 'uid',
    properties: {
      uid: { type: 'uid', nullable: false },
      region: { type: 'string', nullable: false },
      assignee: { type: 'string', nullable: true },
    },
  };
  const rows = Array.from({ length: count }, (_, i) => ({
    uid: randomUUID(),
    region: i % 2 ? 'East' : 'West',
    assignee: null,
  }));
  for (let start = 0; start < count; start += 1000) {
    const batch = rows.slice(start, start + 1000);
    await pool.query(
      `INSERT INTO moria.identities(domain,canonical_key,uid) SELECT 'bench',uid,uid::uuid FROM unnest($1::text[]) uid`,
      [batch.map((row) => row.uid)],
    );
  }
  const activationStart = performance.now();
  const revision = await store.activate('bench', 'orders', schema, rows, '0', '0');
  const activationMs = performance.now() - activationStart;
  await pool.query('ANALYZE moria.staged_source_rows');
  const sampledUid = (index: number) => rows[(index * 104729) % count].uid;
  const read = async (index: number) => {
    const c = await pool!.connect();
    try {
      const result = await store.read(c, 'bench', 'orders', sampledUid(index), revision);
      if (!result) throw new Error('Missing fixture');
    } finally {
      c.release();
    }
  };
  for (let i = 0; i < 50; i++) await read(i);
  const reads = await load(1000, read);
  const actions = await load(200, async (index) => {
    await store.edit(
      {
        environment: 'bench',
        application: 'bench',
        principal: 'bench',
        action: 'assign',
        actionRevision: '1',
        key: String(index),
        input: { index },
        securityEpoch: '0',
      },
      'orders',
      sampledUid(index),
      schema,
      [{ op: 'set', property: 'assignee', value: 'Bob' }],
      () => {},
    );
  });
  const authority = new Authority(catalog);
  await authority.migrate();
  await authority.syncPrincipal('bench', 'setup', 'bench', {
    kind: 'workload',
    enabled: true,
    attributes: { region: 'East', operator: true },
    markings: ['sensitive'],
  });
  await authority.setApplication('bench', 'setup', 'bench', ['orders:read', 'assign:invoke']);
  await authority.setGrants('bench', 'setup', 'bench', 'bench', ['orders:read', 'assign:invoke']);
  const service = new ObjectService(store, authority, {
    schema,
    policies: {
      resource: 'orders',
      markings: ['sensitive'],
      properties: schema.properties,
      sourceColumns: schema.properties,
      attributes: {
        region: { type: 'string', nullable: false },
        operator: { type: 'boolean', nullable: false },
      },
      row: { op: 'eq', args: [{ field: 'region' }, { principalAttribute: 'region' }] },
      propertyRead: { assignee: { principalAttribute: 'operator' } },
    },
    writePolicy: { principalAttribute: 'operator' },
    propertyWrite: { assignee: { principalAttribute: 'operator' } },
  });
  const session = { environment: 'bench', principal: 'bench', application: 'bench' };
  const eastRows = rows.filter((row) => row.region === 'East');
  const eastUid = (i: number) => eastRows[(i * 104729) % eastRows.length].uid;
  for (let i = 0; i < 50; i++) await service.read(session, eastUid(i));
  const authorizedReads = await load(1000, async (index) => {
    await service.read(session, eastUid(index));
  });
  const action: ActionBinding = {
    resource: 'assign',
    revision: '1',
    markings: ['sensitive'],
    targetParameter: 'uid',
    inputs: { uid: schema.properties.uid, assignee: { type: 'string', nullable: false } },
    condition: { literal: true },
    edits: [{ op: 'set', property: 'assignee', value: { parameter: 'assignee' } }],
  };
  const authorizedActions = await load(200, async (index) => {
    await service.invoke(session, action, `authorized-${index}`, {
      uid: eastUid(index),
      assignee: 'Carol',
    });
  });
  const result = {
    recordedAt: new Date().toISOString(),
    scope:
      'Internal store plus integrated authority/policy point operations. Excludes HTTP/OIDC, multirow query SQL, Iceberg ingestion and end-to-end pagination. Memory is Node process RSS only, excludes PostgreSQL.',
    hardware: {
      cpu: cpus()[0]?.model,
      logicalCpus: cpus().length,
      hostMemoryBytes: totalmem(),
      os: `${platform()} ${release()}`,
      node: process.version,
      postgres: (await pool.query('SHOW server_version')).rows[0].server_version,
    },
    rows: count,
    clients,
    activationMs,
    activationRowsPerSecond: (count * 1000) / activationMs,
    reads,
    actions,
    authorizedReads,
    authorizedActions,
    peakNodeRssBytes: Math.max(peakRss, process.memoryUsage().rss),
  };
  await writeFile(resolve(values.output!), JSON.stringify(result, null, 2) + '\n');
  console.log(JSON.stringify(result, null, 2));
} finally {
  clearInterval(sampling);
  await pool?.end();
  await db.stop();
}
