import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { spawn, execFileSync, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import { cpus, release, totalmem } from 'node:os';
import assert from 'node:assert/strict';
import EmbeddedPostgres from 'embedded-postgres';
import pg from 'pg';

const output = process.env.BENCH_OUTPUT ?? '/results/technology.json';
const sizes = (process.env.BENCH_SIZES ?? '1000000,10000000').split(',').map(Number);
const duration = Number(process.env.BENCH_SECONDS ?? 6),
  repetitions = Number(process.env.BENCH_REPS ?? 3);
const clientsList = (process.env.BENCH_CLIENTS ?? '20,100').split(',').map(Number);
const implementations = (process.env.BENCH_IMPLEMENTATIONS ?? 'node,go,python').split(',');
assert.ok(
  implementations.length > 0 &&
    implementations.every((name) => ['node', 'go', 'python', 'rust'].includes(name)) &&
    new Set(implementations).size === implementations.length,
  'Invalid benchmark implementations',
);
const directory = `.local/technology-${randomUUID()}`;
await mkdir(directory, { recursive: true });
const password = randomUUID(),
  port = 55439;
const db = new EmbeddedPostgres({
  databaseDir: directory,
  user: 'benchmark',
  password,
  port,
  persistent: true,
  authMethod: 'scram-sha-256',
  initdbFlags: ['--encoding=UTF8', '--locale=C'],
  postgresFlags: [
    '-c',
    'listen_addresses=127.0.0.1',
    '-c',
    'shared_buffers=512MB',
    '-c',
    'work_mem=16MB',
    '-c',
    'max_connections=80',
    '-c',
    'max_parallel_workers_per_gather=0',
    '-c',
    'max_wal_size=4GB',
  ],
  onLog: () => {},
  onError: () => {},
});
const dsn = `postgresql://benchmark:${password}@127.0.0.1:${port}/postgres?sslmode=disable&options=${encodeURIComponent('-c statement_timeout=3000')}`;
const queries = JSON.parse(await readFile('benchmarks/ontology/queries.json', 'utf8'));
let pool: pg.Pool | undefined, active: ChildProcess | undefined;
const report: any = {
  hardware: {
    cpu: cpus()[0]?.model,
    logicalCpus: cpus().length,
    vmMemoryBytes: totalmem(),
    kernel: release(),
    cpuQuota: await readFile('/sys/fs/cgroup/cpu.max', 'utf8'),
    memoryLimit: await readFile('/sys/fs/cgroup/memory.max', 'utf8'),
  },
  settings: {
    implementations,
    rustRuntime: 'current_thread',
    databasePool: 20,
    appWorkers: 1,
    goMaxProcs: 1,
    durationSeconds: duration,
    repetitions,
    clients: clientsList,
    mix: '70% queue (100 rows), 20% detail, 10% serialized idempotent actions',
    populationSampling: 'deterministic stride across all eligible object IDs',
    postgres: { sharedBuffers: '512MB', workMem: '16MB', parallelQueryWorkers: 0, fsync: true },
    caveat:
      'Benchmark kernels, not complete Moria API. Shared SQL and per-request stored authorization; no OIDC, real policy compiler, pinned cursors, source generations or browser rendering.',
  },
  volumes: [],
};
const save = () => writeFile(output, JSON.stringify(report, null, 2) + '\n');
async function capture(
  command: string,
  args: string[],
  env: NodeJS.ProcessEnv = process.env,
): Promise<string> {
  const child = spawn(command, args, { env, stdio: ['ignore', 'pipe', 'pipe'] });
  let stdout = '',
    stderr = '';
  child.stdout.on('data', (c) => (stdout += c));
  child.stderr.on('data', (c) => (stderr += c));
  const [code] = await once(child, 'exit');
  if (code !== 0) throw new Error(`Child failed (${command}): ${stderr.slice(-1000)}`);
  return stdout.trim();
}
async function proc(pid: number) {
  try {
    const stat = await readFile(`/proc/${pid}/stat`, 'utf8'),
      fields = stat.slice(stat.lastIndexOf(')') + 2).split(' '),
      status = await readFile(`/proc/${pid}/status`, 'utf8');
    return {
      ticks: Number(fields[11]) + Number(fields[12]),
      peakRssKiB: Number(/VmHWM:\s+(\d+)/.exec(status)?.[1] ?? 0),
    };
  } catch {
    return { ticks: 0, peakRssKiB: 0 };
  }
}
async function databaseTicks() {
  const pids = (await pool!.query('SELECT pid FROM pg_stat_activity')).rows.map((r) => r.pid);
  const stats = await Promise.all(pids.map(proc));
  return stats.reduce((n, s) => n + s.ticks, 0);
}
const ticksPerSecond = Number(execFileSync('getconf', ['CLK_TCK'], { encoding: 'utf8' }).trim());
async function stopApp() {
  if (active) {
    const process = active;
    active = undefined;
    if (process.exitCode !== null || process.signalCode !== null) return;
    process.kill('SIGTERM');
    await once(process, 'exit');
  }
}
async function startApp(name: string) {
  const command =
    name === 'node'
      ? process.execPath
      : name === 'python'
        ? '.venv/bin/python'
        : name === 'rust'
          ? 'benchmarks/ontology/rust-server'
          : 'benchmarks/ontology/go-server';
  const args =
    name === 'node'
      ? ['--import', 'tsx', 'benchmarks/ontology/node.ts']
      : name === 'python'
        ? ['benchmarks/ontology/python.py']
        : [];
  const child = spawn(command, args, {
    env: { ...process.env, BENCH_DSN: dsn, PORT: '55123', GOMAXPROCS: '1' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  active = child;
  let stderr = '';
  child.stderr!.on('data', (c) => (stderr += c));
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`${name} startup timeout: ${stderr.slice(-600)}`)),
      15000,
    );
    child.stdout!.on('data', (c) => {
      if (String(c).includes('READY')) {
        clearTimeout(timer);
        resolve();
      }
    });
    child.once('exit', () => {
      clearTimeout(timer);
      reject(new Error(`${name} startup failed: ${stderr.slice(-600)}`));
    });
  });
  for (let i = 0; i < 50; i++) {
    try {
      if ((await fetch('http://127.0.0.1:55123/detail?id=1')).status === 401) return child;
    } catch {}
    await new Promise((r) => setTimeout(r, 20));
  }
  throw new Error(`${name} did not listen`);
}
async function preflight(name: string) {
  const get = async (path: string) =>
    await fetch('http://127.0.0.1:55123' + path, {
      headers: { authorization: 'benchmark-only-token' },
    });
  assert.equal((await get('/detail?id=2')).status, 404);
  const a = await (await get('/queue?layout=typed')).json(),
    b = await (await get('/queue?layout=json')).json();
  assert.deepEqual(a, b);
  assert.equal(a.length, 100);
  assert.ok(a.every((x: any) => x.region === 'East' && !Object.hasOwn(x, 'internalNote')));
  const key = `preflight-${name}-${randomUUID()}`;
  const first = await get(`/action?id=1&key=${key}`),
    second = await get(`/action?id=1&key=${key}`);
  assert.equal(first.status, 200);
  assert.equal(second.status, 200);
  const receipt = await first.json();
  assert.equal(typeof receipt.revision, 'string');
  assert.deepEqual(receipt, await second.json());
}
async function explain(sql: string, args: unknown[], scan = false) {
  const c = await pool!.connect();
  try {
    await c.query('BEGIN');
    if (scan) {
      await c.query('SET LOCAL enable_indexscan=off');
      await c.query('SET LOCAL enable_bitmapscan=off');
    }
    const start = performance.now();
    const result = await c.query('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' + sql, args);
    return { wallMs: performance.now() - start, plan: result.rows[0]['QUERY PLAN'][0] };
  } finally {
    await c.query('ROLLBACK');
    c.release();
  }
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
    max: 4,
  });
  report.versions = {
    node: process.version,
    go: await capture('benchmarks/ontology/go-server', ['--version']),
    python: await capture('.venv/bin/python', [
      '-c',
      'import sys,psycopg,aiohttp; print(sys.version); print("psycopg",psycopg.__version__,"aiohttp",aiohttp.__version__)',
    ]),
    postgres: (await pool.query('SHOW server_version')).rows[0].server_version,
  };
  if (implementations.includes('rust'))
    report.versions.rust = await capture('benchmarks/ontology/rust-server', ['--version']);
  await pool.query(`CREATE TABLE bench_sessions(token text PRIMARY KEY,region text NOT NULL,enabled boolean NOT NULL);INSERT INTO bench_sessions VALUES('benchmark-only-token','East',true);
    CREATE TABLE bench_objects(id bigint PRIMARY KEY,region text NOT NULL,status text NOT NULL,priority integer NOT NULL,marked boolean NOT NULL,assignee text,payload jsonb NOT NULL);
    CREATE INDEX typed_queue ON bench_objects(region,status,priority,id) INCLUDE(marked);
    CREATE INDEX json_queue ON bench_objects((payload->>'region'),(payload->>'status'),((payload->>'priority')::int),id) INCLUDE(marked);
    CREATE TABLE bench_head(revision bigint NOT NULL);INSERT INTO bench_head VALUES(0);
    CREATE TABLE bench_receipts(key text PRIMARY KEY,result jsonb NOT NULL);
    CREATE TABLE bench_history(id bigint NOT NULL,revision bigint NOT NULL,assignee text NOT NULL,PRIMARY KEY(id,revision));`);
  let populated = 0;
  for (const count of sizes) {
    const seedStart = performance.now();
    while (populated < count) {
      const end = Math.min(count, populated + 100000);
      await pool.query(
        `INSERT INTO bench_objects SELECT g,region,status,(g%7)::int,g%13=0,NULL,jsonb_build_object('id',g::text,'region',region,'status',status,'priority',g%7,'title','Work order '||g,'description',repeat('maintenance ',16),'internalNote',repeat('restricted ',8),'assignee',NULL) FROM (SELECT g,CASE g%4 WHEN 1 THEN 'East' WHEN 2 THEN 'West' WHEN 3 THEN 'North' ELSE 'South' END region,CASE WHEN g%5=0 THEN 'CLOSED' ELSE 'OPEN' END status FROM generate_series($1::bigint,$2::bigint) g) source`,
        [populated + 1, end],
      );
      populated = end;
      if (populated % 1000000 === 0) console.log(`Seeded ${populated} objects`);
    }
    await pool.query('VACUUM ANALYZE bench_objects');
    const volume: any = {
      rows: count,
      seedMs: performance.now() - seedStart,
      storage: (
        await pool.query(
          "SELECT pg_table_size('bench_objects')::text AS table_bytes,pg_indexes_size('bench_objects')::text AS indexes_bytes",
        )
      ).rows[0],
      sql: {},
      trials: [],
    };
    report.volumes.push(volume);
    await save();
    const cases: [string, string, unknown[], boolean?][] = [
      ['typed_indexed', queries.queue_typed, ['East', 2, Math.floor(count / 2)]],
      ['json_indexed', queries.queue_json, ['East', 2, Math.floor(count / 2)]],
      ['typed_scan_sort', queries.queue_typed, ['East', 2, Math.floor(count / 2)], true],
      ['json_scan_sort', queries.queue_json, ['East', 2, Math.floor(count / 2)], true],
      [
        'deep_offset',
        "SELECT id FROM bench_objects WHERE region=$1 AND status='OPEN' AND NOT marked ORDER BY priority,id OFFSET 100000 LIMIT 100",
        ['East'],
      ],
      [
        'live_counts',
        'SELECT status,count(*) FROM bench_objects WHERE region=$1 AND NOT marked GROUP BY status',
        ['East'],
      ],
    ];
    for (const [name, sql, args, scan] of cases) {
      volume.sql[name] = [];
      for (let repeat = 0; repeat < 3; repeat++)
        volume.sql[name].push(await explain(sql, args, scan));
      console.log(
        `SQL ${count} ${name}: ${volume.sql[name].map((x: any) => x.plan['Execution Time'].toFixed(1)).join('/')} ms`,
      );
      await save();
    }
    for (const clients of clientsList)
      for (let repeat = 0; repeat < repetitions; repeat++) {
        const order = implementations;
        for (const name of [
          ...order.slice(repeat % order.length),
          ...order.slice(0, repeat % order.length),
        ]) {
          const child = await startApp(name);
          await preflight(name);
          await capture(process.execPath, [
            '--import',
            'tsx',
            'benchmarks/ontology/load.ts',
            'http://127.0.0.1:55123',
            String(count),
            String(clients),
            '1',
            `warm-${randomUUID()}`,
          ]);
          const before = await proc(child.pid!),
            dbBefore = await databaseTicks(),
            start = performance.now();
          const result = JSON.parse(
            await capture(process.execPath, [
              '--import',
              'tsx',
              'benchmarks/ontology/load.ts',
              'http://127.0.0.1:55123',
              String(count),
              String(clients),
              String(duration),
              randomUUID(),
            ]),
          );
          const after = await proc(child.pid!),
            dbAfter = await databaseTicks(),
            elapsed = performance.now() - start;
          volume.trials.push({
            implementation: name,
            repeat,
            ...result,
            appCpuCores: (after.ticks - before.ticks) / ticksPerSecond / (elapsed / 1000),
            appPeakRssKiB: after.peakRssKiB,
            databaseCpuCores: Math.max(0, dbAfter - dbBefore) / ticksPerSecond / (elapsed / 1000),
          });
          console.log(
            `HTTP ${count} c${clients} ${name} r${repeat}: ${result.successfulRps.toFixed(0)} req/s, queue p95 ${result.operations.queue.p95Ms.toFixed(1)} ms`,
          );
          await save();
          await stopApp();
        }
      }
  }
  report.completed = true;
  await save();
} finally {
  await stopApp();
  await pool?.end();
  await db.stop();
}
