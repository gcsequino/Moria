import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { createServer } from 'node:net';
import { cpus, totalmem, platform, release } from 'node:os';
import { parseArgs } from 'node:util';
import assert from 'node:assert/strict';
import EmbeddedPostgres from 'embedded-postgres';
import pg from 'pg';
import { Catalog } from '../services/catalog/src/catalog.js';
import { ObjectStore, type ObjectSchema } from '../services/ontology/src/store.js';
import { Authority } from '../services/security/src/authority.js';
import { ObjectService } from '../services/ontology/src/service.js';
import { httpWorkload } from './http-workload.js';

const { values } = parseArgs({
  options: {
    rows: { type: 'string', default: '1000000' },
    output: { type: 'string', default: '.local/staging.json' },
    query: { type: 'boolean', default: false },
    http: { type: 'boolean', default: false },
    refresh: { type: 'boolean', default: false },
    seconds: { type: 'string', default: '30' },
  },
});
const seconds = Number(values.seconds);
assert.ok(Number.isInteger(seconds) && seconds >= 1 && seconds <= 300);
const count = Number(values.rows);
assert.ok(Number.isInteger(count) && count >= 1000 && count <= 10000000);
const socket = createServer();
await new Promise<void>((r) => socket.listen(0, '127.0.0.1', r));
const port = (socket.address() as { port: number }).port;
await new Promise<void>((r) => socket.close(() => r()));
const directory = resolve('.local', `staging-${randomUUID()}`);
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
let pool: pg.Pool | undefined,
  peak = process.memoryUsage().rss;
const timer = setInterval(() => (peak = Math.max(peak, process.memoryUsage().rss)), 50);
const uid = (i: number) => `00000000-0000-0000-0000-${i.toString(16).padStart(12, '0')}`;
try {
  await db.initialise();
  await db.start();
  pool = new pg.Pool({
    host: '127.0.0.1',
    port,
    user: 'benchmark',
    password,
    database: 'postgres',
    max: 20,
  });
  const catalog = new Catalog(pool),
    store = new ObjectStore(catalog);
  await catalog.migrate();
  await store.migrate();
  await catalog.ensureEnvironment('stream');
  for (let first = 1; first <= count; first += 100000)
    await pool.query(
      "INSERT INTO moria.identities(domain,canonical_key,uid) SELECT 'stream',g::text,('00000000-0000-0000-0000-'||lpad(to_hex(g),12,'0'))::uuid FROM generate_series($1::bigint,$2::bigint) g",
      [first, Math.min(count, first + 99999)],
    );
  const schema: ObjectSchema = {
    primaryKey: 'uid',
    properties: {
      uid: { type: 'uid', nullable: false },
      region: { type: 'string', nullable: false },
      assignee: { type: 'string', nullable: true },
    },
  };
  async function* input() {
    for (let i = 1; i <= count; i++)
      yield { uid: uid(i), region: i % 2 ? 'East' : 'West', assignee: null };
  }
  const before = performance.now(),
    stage = await store.stage('stream', 'orders', schema, input(), '0', '0', undefined, [
      'sensitive',
    ]),
    stagingMs = performance.now() - before;
  assert.equal(
    (await pool.query("SELECT revision FROM moria.environments WHERE id='stream'")).rows[0]
      .revision,
    '0',
  );
  const publishStart = performance.now(),
    revision = await store.publishStage(stage),
    publicationMs = performance.now() - publishStart;
  assert.equal(await store.publishStage(stage), revision);
  assert.equal(
    (await pool.query('SELECT count(*) FROM moria.staged_source_rows WHERE stage=$1', [stage]))
      .rows[0].count,
    String(count),
  );
  const c = await pool.connect();
  try {
    for (const i of [1, Math.floor(count / 2), count]) {
      assert.equal(await store.read(c, 'stream', 'orders', uid(i), '0'), null);
      assert.equal((await store.read(c, 'stream', 'orders', uid(i), revision))!.source.uid, uid(i));
    }
  } finally {
    c.release();
  }
  const queries: unknown[] = [];
  const queryLoads: unknown[] = [];
  if (values.query) {
    await pool.query('ANALYZE moria.staged_source_rows');
    const authority = new Authority(catalog);
    await authority.migrate();
    await authority.syncPrincipal('stream', 'setup', 'reader', {
      kind: 'workload',
      enabled: true,
      attributes: { region: 'East' },
      markings: ['sensitive'],
    });
    await authority.setApplication('stream', 'setup', 'app', ['orders:read']);
    await authority.setGrants('stream', 'setup', 'reader', 'app', ['orders:read']);
    const service = new ObjectService(store, authority, {
      schema,
      policies: {
        resource: 'orders',
        markings: ['sensitive'],
        properties: schema.properties,
        sourceColumns: schema.properties,
        attributes: { region: { type: 'string', nullable: false } },
        row: { op: 'eq', args: [{ field: 'region' }, { principalAttribute: 'region' }] },
      },
    });
    let cursor: string | undefined;
    for (let i = 0; i < 3; i++) {
      const started = performance.now();
      try {
        const page = await service.query(
          { environment: 'stream', principal: 'reader', application: 'app' },
          { pageSize: 100, ...(cursor ? { cursor } : {}) },
        );
        assert.equal(page.objects.length, 100);
        assert.ok(page.objects.every((o) => o.region === 'East'));
        cursor = page.nextCursor;
        queries.push({
          page: i + 1,
          elapsedMs: performance.now() - started,
          rows: page.objects.length,
        });
      } catch (error) {
        queries.push({
          page: i + 1,
          elapsedMs: performance.now() - started,
          error: (error as any).code ?? String(error),
        });
        break;
      }
    }
    if (queries.length === 3 && !queries.some((q) => Object.hasOwn(q as object, 'error')))
      for (const clients of [20, 100]) {
        let next = 0,
          success = 0;
        const latencies: number[] = [],
          failures: Record<string, number> = {};
        const start = performance.now();
        await Promise.all(
          Array.from({ length: clients }, async () => {
            let cursor: string | undefined;
            while (next++ < 1000) {
              const began = performance.now();
              try {
                const page = await service.query(
                  { environment: 'stream', principal: 'reader', application: 'app' },
                  { pageSize: 100, ...(cursor ? { cursor } : {}) },
                );
                assert.equal(page.objects.length, 100);
                assert.ok(page.objects.every((o) => o.region === 'East'));
                cursor = page.nextCursor;
                success++;
              } catch (error) {
                const code = (error as any).code ?? 'ERROR';
                failures[code] = (failures[code] ?? 0) + 1;
              }
              latencies.push(performance.now() - began);
            }
          }),
        );
        latencies.sort((a, b) => a - b);
        const elapsedMs = performance.now() - start;
        queryLoads.push({
          clients,
          attempts: latencies.length,
          success,
          failures,
          elapsedMs,
          successfulQueriesPerSecond: (success * 1000) / elapsedMs,
          p50Ms: latencies[Math.ceil(latencies.length * 0.5) - 1],
          p95Ms: latencies[Math.ceil(latencies.length * 0.95) - 1],
          p99Ms: latencies[Math.ceil(latencies.length * 0.99) - 1],
        });
      }
  }
  const result = {
    recordedAt: new Date().toISOString(),
    rows: count,
    batchRows: 1000,
    stagingMs,
    publicationMs,
    queries,
    queryLoads,
    peakNodeRssBytes: Math.max(peak, process.memoryUsage().rss),
    hardware: {
      cpu: cpus()[0]?.model,
      logicalCpus: cpus().length,
      hostMemoryBytes: totalmem(),
      os: `${platform()} ${release()}`,
      node: process.version,
      postgres: (await pool.query('SHOW server_version')).rows[0].server_version,
    },
    scope:
      'Single synthetic narrow-row run through real ObjectStore. UID registry seeded server-side outside timing. Async generator bounds input buffering; no retained overlays. Publication time includes lock acquisition and commit, not instrumented lock hold. Optional queries use integrated authority and pinned pagination: three sequential pages then 1000 requests each at 20/100 concurrent callers, 20 DB connections. Each caller walks from the start, so this is a warm early-queue working set, not uniform population sampling. Failure latency is included. Excludes Iceberg/HTTP and concurrent writes; security concurrency correctness is covered separately by integration tests.',
  };
  const http = values.http
    ? await httpWorkload(store, schema, count, seconds, values.refresh)
    : undefined;
  const final = {
    ...result,
    recordedAt: new Date().toISOString(),
    peakNodeRssBytes: Math.max(peak, process.memoryUsage().rss),
    ...(http
      ? {
          http,
          scope:
            result.scope +
            ' The preceding scope describes staging and optional direct queries only; the separate http section includes concurrent actions and HTTP. Peak Node RSS includes that phase and its in-process generator.',
        }
      : {}),
  };
  await writeFile(resolve(values.output!), JSON.stringify(final, null, 2) + '\n');
  console.log(JSON.stringify(final, null, 2));
} finally {
  clearInterval(timer);
  await pool?.end();
  await db.stop();
}
