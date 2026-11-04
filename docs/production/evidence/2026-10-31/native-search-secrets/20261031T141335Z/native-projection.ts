import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { createReadStream, createWriteStream } from 'node:fs';
import { mkdir, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { ObjectStore, type ObjectSchema } from '../services/ontology/src/store.js';
import { IcebergIndexer } from '../services/ontology/src/indexer.js';
import { database } from '../tests/fixtures/catalog-storage/database.js';
import { assertNativeArtifacts } from '../tests/fixtures/native-common/artifact.js';

export interface NativeProjectionFixture {
  db: Awaited<ReturnType<typeof database>>;
  environment: string;
  source: string;
  target: string;
  schema: ObjectSchema;
  token: string;
  nativeEnv: NodeJS.ProcessEnv;
  markings: string[];
  runtime: string;
  reindex: (label: string) => Promise<{ stage: string; revision: string; projection: string }>;
}
interface Options {
  rows: number;
  rowBytes: number;
  output: string;
  timeoutMs?: number;
  profile?: 'wide-reducer-regression';
  markings?: readonly string[];
  onVerified?: (fixture: NativeProjectionFixture) => Promise<void>;
}
/** Explicit exploratory population run; no full-profile certification is inferred. */
export async function nativeProjectionCapacity(options: Options) {
  const wide = options.profile === 'wide-reducer-regression';
  const markings = [...(options.markings ?? ['private', 'enterprise'])];
  assert.ok(
    markings.length >= 2 &&
      markings.length <= 256 &&
      new Set(markings).size === markings.length &&
      markings.every(
        (value) => typeof value === 'string' && value.length > 0 && value.length <= 1024,
      ),
  );
  assert.ok(Number.isSafeInteger(options.rows) && options.rows > 0 && options.rows <= 100_000_000);
  assert.ok(
    Number.isSafeInteger(options.rowBytes) &&
      (wide
        ? options.rows <= 128 && options.rowBytes === 1024 * 1024
        : options.rowBytes >= 1024 && options.rowBytes <= 16384),
  );
  const runtime =
    process.env.MORIA_RUNTIME_BINARY ??
    process.env.MORIA_NATIVE_BINARY ??
    '/usr/local/bin/moria-runtime';
  const harness = process.env.MORIA_CATALOG_TEST_BINARY ?? '/usr/local/bin/moria-catalog-tests';
  const metadata = await assertNativeArtifacts(runtime, [harness]);
  await mkdir(options.output, { recursive: true });
  const db = await database(),
    children: ReturnType<typeof spawn>[] = [];
  const started = performance.now(),
    timings: Record<string, number> = {};
  const timeline: { phase: string; elapsedMs: number }[] = [];
  const runs: Record<string, { elapsedMs: number; exitCode: number | null }> = {};
  let activePhase = 'setup',
    phaseStarted = started;
  const enterPhase = async (name: string) => {
    const now = performance.now();
    timeline.push({ phase: activePhase, elapsedMs: now - phaseStarted });
    activePhase = name;
    phaseStarted = now;
    await writeFile(
      join(options.output, 'phase-state.json'),
      JSON.stringify({ activePhase, timeline, elapsedMs: now - started }, null, 2),
    );
  };
  const deadline = started + (options.timeoutMs ?? 24 * 60 * 60 * 1000);
  const root = join(db.root, 'objects'),
    physical = join(db.root, 'source'),
    scratch = join(db.root, 'scratch');
  for (const path of [root, physical, scratch]) await mkdir(path);
  const environment = randomUUID(),
    source = randomUUID(),
    target = randomUUID(),
    domain = `capacity-${environment}`;
  const role = `capacity_${randomUUID().replaceAll('-', '')}`,
    password = randomUUID();

  let binding = randomUUID(),
    snapshot: unknown[] = [];
  const objectsQuota = 10000;
  // Logical/reference quotas scale with this named input; fixed memory/chunk/page caps do not.
  const closureLimits = {
    maxEntries: objectsQuota,
    maxReferencedBytes: Math.max(1024 * 1024 * 1024, options.rows * options.rowBytes * 2),
    maxSpoolBytes: 64 * 1024 * 1024,
  };
  const limits = { maxObjectBytes: 128 * 1024 * 1024, chunkBytes: 1024 * 1024 };
  let commonEnv: NodeJS.ProcessEnv = {};
  try {
    const digest = async (path: string) => {
      const hash = createHash('sha256');
      for await (const bytes of createReadStream(path)) hash.update(bytes);
      return hash.digest('hex');
    };
    await writeFile(
      join(options.output, 'artifacts.json'),
      JSON.stringify(
        {
          metadata,
          runtime: { path: runtime, sha256: await digest(runtime) },
          harness: { path: harness, sha256: await digest(harness) },
        },
        null,
        2,
      ),
    );
    await db.catalog.ensureEnvironment(environment);
    await db.catalog.ensureDataset(environment, source);
    const authority = new Authority(db.catalog),
      sessions = new WorkloadSessions(authority);
    const operations = [
      `${source}:read`,
      `${source}:write`,
      `${target}:read`,
      `${target}:index`,
      'edit:invoke',
      'group:invoke',
    ];
    await authority.syncPrincipal(environment, 'fixture', 'pipeline', {
      enabled: true,
      kind: 'workload',
      attributes: { admin: true, region: 'a' },
      markings,
    });
    await authority.setApplication(environment, 'fixture', 'capacity', operations);
    await authority.setGrants(environment, 'fixture', 'pipeline', 'capacity', operations);
    let credential = await sessions.mint(
      { environment, principal: 'pipeline', application: 'capacity' },
      'fixture',
    );
    let context = {
      environment,
      principal: 'pipeline',
      application: 'capacity',
      sessionHash: createHash('sha256').update(credential.token).digest('hex'),
    };
    const freshSession = async () => {
      credential = await sessions.mint(
        { environment, principal: 'pipeline', application: 'capacity' },
        'fixture',
      );
      context = {
        ...context,
        sessionHash: createHash('sha256').update(credential.token).digest('hex'),
      };
    };
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(`ALTER ROLE ${role} SET temp_file_limit='256MB'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role};GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role};GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    const connection = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    commonEnv = {
      PATH: process.env.PATH,
      MORIA_INDEXING_DATABASE_SECRET_REF: 'CAPACITY_DATABASE',
      CAPACITY_DATABASE: connection,
      MORIA_INDEXING_DATABASE_TLS: 'disable',
      MORIA_NATIVE_STORAGE_BINDINGS: JSON.stringify([
        {
          namespace: { warehouseId: 'warehouse', tableId: 'table', revision: 'v1' },
          provider: { kind: 'posix', root, limits },
        },
      ]),
      MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT: scratch,
      MORIA_NATIVE_CLOSURE_LIMITS: JSON.stringify(closureLimits),
    };
    const run = async (
      name: string,
      binary: string,
      args: string[],
      extra: NodeJS.ProcessEnv = {},
      tap = false,
    ) => {
      const runStarted = performance.now();
      const log = createWriteStream(join(options.output, `${name}.log`), { flags: 'wx' });
      const child = spawn(binary, args, {
        env: { ...commonEnv, ...extra },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      const watchdog = setTimeout(
        () => child.kill('SIGKILL'),
        Math.max(1, deadline - performance.now()),
      ).unref();
      children.push(child);
      const events: Record<string, unknown>[] = [];
      let tail = '',
        line = '';
      child.stdout!.pipe(log, { end: false });
      child.stderr!.pipe(log, { end: false });
      child.stdout!.on('data', (bytes) => {
        const text = bytes.toString();
        tail = (tail + text).slice(-65536);
        line += text;
        assert.ok(line.length <= 131072, 'bounded native event line');
        let split;
        while ((split = line.indexOf('\n')) >= 0) {
          const value = line.slice(0, split);
          line = line.slice(split + 1);
          const start = value.indexOf('{');
          if (start >= 0) {
            try {
              const event = JSON.parse(value.slice(start));
              if (
                [
                  'identity_population_complete',
                  'receipt',
                  'capacity_verified',
                  'active',
                  'reducer_batches',
                ].includes(event.event)
              )
                events.push(event);
            } catch {}
          }
        }
      });
      let error = '';
      child.stderr!.on('data', (bytes) => {
        error = (error + bytes.toString()).slice(-65536);
      });
      const code = await new Promise<number | null>((resolve, reject) => {
        child.once('error', reject);
        child.once('exit', resolve);
      });
      clearTimeout(watchdog);
      await new Promise<void>((resolve) => log.end(resolve));
      runs[name] = { elapsedMs: performance.now() - runStarted, exitCode: code };
      assert.equal(code, 0, `${name}: ${error}\n${tail}`);
      if (tap) assert.match(tail, /test result: ok\. 1 passed; 0 failed; 0 ignored;/);
      return events;
    };
    const native = async (name: string, mode: string, extra: Record<string, unknown> = {}) =>
      run(
        name,
        harness,
        ['--ignored', '--exact', 'catalog_publication', '--nocapture', '--test-threads=1'],
        {
          MORIA_NATIVE_CATALOG_FIXTURE: JSON.stringify({
            root,
            token: credential.token,
            binding,
            mode,
            snapshots: snapshot,
            ...extra,
          }),
        },
        true,
      );
    let phase = performance.now();
    await enterPhase('identity');
    const uidFile = join(db.root, 'uid-map.jsonl'),
      lookupFile = join(db.root, 'uids.txt');
    await native('identity', 'allocate', {
      allocation: {
        domain,
        cell: randomUUID(),
        output: uidFile,
        lookupOutput: lookupFile,
        count: options.rows,
      },
    });
    timings.identityMs = performance.now() - phase;
    phase = performance.now();
    await enterPhase('source-production');
    const producerLog = createWriteStream(join(options.output, 'producer.log'), { flags: 'wx' });
    const producer = spawn(
      resolve('.venv/bin/python'),
      [
        'tests/fixtures/native-catalog/produce_capacity.py',
        physical,
        uidFile,
        String(options.rows),
        String(options.rowBytes),
        'moria-capacity-v1',
        ...(wide ? ['--wide-reducer-regression'] : []),
      ],
      { stdio: ['ignore', 'pipe', 'pipe'] },
    );
    children.push(producer);
    const producerWatchdog = setTimeout(
      () => producer.kill('SIGKILL'),
      Math.max(1, deadline - performance.now()),
    ).unref();
    let produced = '',
      producerErrors = '';
    producer.stdout!.pipe(producerLog, { end: false });
    producer.stderr!.pipe(producerLog, { end: false });
    producer.stdout!.on('data', (b) => {
      produced += b.toString();
      assert.ok(produced.length <= 65536);
    });
    producer.stderr!.on('data', (b) => {
      producerErrors = (producerErrors + b.toString()).slice(-65536);
    });
    assert.equal(
      await new Promise<number | null>((resolve, reject) => {
        producer.once('error', reject);
        producer.once('exit', resolve);
      }),
      0,
      producerErrors,
    );
    clearTimeout(producerWatchdog);
    await new Promise<void>((resolve) => producerLog.end(resolve));
    const manifest = JSON.parse(produced);
    assert.equal(manifest.rows, String(options.rows));
    assert.equal(manifest.logicalBytes, String(options.rows * options.rowBytes));
    assert.ok(manifest.objects < objectsQuota);
    await writeFile(
      join(options.output, 'source-manifest.json'),
      JSON.stringify(manifest, null, 2),
    );
    timings.producerMs = performance.now() - phase;
    phase = performance.now();
    await enterPhase('source-publication');
    await freshSession();
    await db.pool.query(
      "INSERT INTO moria.native_storage_namespaces(environment,warehouse_id,table_id,resource,revision,purpose,iceberg_uuid,markings) VALUES($1,'warehouse','table',$2,'v1','dataset-source',$3,$4)",
      [environment, source, manifest.tableUuid, markings],
    );
    const epoch = (await authority.current(environment, 'pipeline', 'capacity')).epoch;
    const spec = {
      version: 1,
      domain: 'sync',
      release: 'a'.repeat(64),
      resourceClass: 'standard',
      timeoutSeconds: 1800,
      retryPolicy: 'idempotent',
      inputSchema: { type: 'object', properties: {} },
      pinnedInputs: [],
      expectedHeads: { [source]: '0' },
    };
    binding = (
      await db.pool.query(
        'INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation) SELECT $1,$2,$3,$4,$5,$7,$6,i.security_epoch,c.incarnation FROM moria.instance_state i,moria.indexing_instance c RETURNING id',
        [
          environment,
          source,
          spec,
          context,
          [`${source}:read`, `${source}:write`],
          epoch,
          markings,
        ],
      )
    ).rows[0].id;
    snapshot = [
      {
        resource: source,
        warehouseId: 'warehouse',
        tableId: 'table',
        metadata: manifest.metadata,
        snapshotId: manifest.snapshotId,
        tableUuid: manifest.tableUuid,
        objectsFile: manifest.objectsFile,
      },
    ];
    const registeredHost = randomUUID(),
      registeredOwner = randomUUID();
    await db.pool.query(
      `INSERT INTO moria.runtime_workers(owner,host_id,incarnation,resource_classes,domains,expires_at)
      SELECT $1,$2,incarnation,ARRAY['standard'],ARRAY['sync'],clock_timestamp()+interval '1 hour' FROM moria.indexing_instance`,
      [registeredOwner, registeredHost],
    );
    await native('publication', 'valid', { closureLimits, registeredHost, registeredOwner });
    timings.publicationMs = performance.now() - phase;
    phase = performance.now();
    await enterPhase('native-indexing-projection');
    const properties = {
      uid: { type: 'uid' as const, nullable: false },
      id: { type: 'int64' as const, nullable: false },
      name: { type: 'string' as const, nullable: false },
      region: { type: 'string' as const, nullable: false },
      enabled: { type: 'boolean' as const, nullable: false },
      rank: { type: 'int64' as const, nullable: false },
      amount: { type: 'decimal' as const, nullable: false, precision: 20, scale: 2 },
      payload: { type: 'string' as const, nullable: false },
    };
    const schema: ObjectSchema = { primaryKey: 'uid', properties };
    const selected = ['uid', 'id', 'name', 'region', 'enabled', 'rank', 'amount'];
    const projection = {
      version: 1,
      sourceFields: selected,
      effectiveFields: selected,
      layouts: [
        { name: 'uid', sort: [], keyBytes: 256 },
        { name: 'name', sort: [{ property: 'name', direction: 'asc' }], keyBytes: 1024 },
        { name: 'rank', sort: [{ property: 'rank', direction: 'desc' }], keyBytes: 1024 },
      ],
      readerRoles: [role],
      budgets: {
        rowBytes: wide ? 8 * 1024 * 1024 : 65536,
        batchRows: wide
          ? Math.max(1, Math.floor((16 * 1024 * 1024) / (2 * options.rowBytes + 4096)))
          : 1000,
        batchBytes: 16 * 1024 * 1024,
        commitReplayRows: 128,
        commitReplayBytes: 16 * 1024 * 1024,
        ddlTimeMs: 900000,
      },
    };
    await db.pool.query(
      'INSERT INTO moria.runtime_projection_bindings(environment,resource,revision,definition) VALUES($1,$2,$3,$4)',
      [environment, target, 'capacity-v1', projection],
    );
    const indexer = new IcebergIndexer(new ObjectStore(db.catalog), authority, {
      source: { kind: 'dataset', resource: source, markings },
      target: { resource: target, markings, schema },
    });
    await freshSession();
    await indexer.registerDistributed(context, {
      targetId: target,
      warehouse: root,
      partitions: wide ? 1 : 16,
      timeoutSeconds: 86400,
    });
    const workerEvents = (
      await Promise.all([run('index-1', runtime, ['drain']), run('index-2', runtime, ['drain'])])
    ).flat();
    const reducerBatches = workerEvents.filter((event) => event.event === 'reducer_batches');
    assert.equal(reducerBatches.length, wide ? 1 : 16);
    for (const event of reducerBatches)
      assert.ok(BigInt(String(event.maxWireBytes)) <= 8n * 1024n * 1024n);
    if (wide)
      assert.ok(
        BigInt(String(reducerBatches[0].batches)) > 1n,
        'wide rows must span bounded reducer batches',
      );
    await writeFile(
      join(options.output, 'reducer-batches.json'),
      JSON.stringify(reducerBatches, null, 2),
    );
    const build = (
      await db.pool.query(
        'SELECT id,status,error FROM moria.indexing_builds WHERE environment=$1 ORDER BY created_at DESC LIMIT 1',
        [environment],
      )
    ).rows[0];
    assert.equal(build.status, 'active', JSON.stringify(build));
    timings.indexingMs = performance.now() - phase;
    phase = performance.now();
    await enterPhase('native-streaming-oracle');
    await freshSession();
    const verified = await native('oracle', 'verify-capacity', {
      verification: {
        stage: build.id,
        resource: target,
        domain,
        count: options.rows,
        sourceSum: manifest.sourceSum,
        sourceXor: manifest.sourceXor,
        pageRows: wide ? 8 : 1000,
      },
    });
    timings.oracleMs = performance.now() - phase;
    await enterPhase('operator-plan-samples');
    const projectionId = String(verified.at(-1)!.projection),
      relation = `moria."runtime_projection_${projectionId.replaceAll('-', '')}"`;
    const plans: Record<string, unknown> = {};
    plans.reducer = (
      await db.pool.query(
        `EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT * FROM moria.indexing_reduce_page($1,0,NULL)`,
        [build.id],
      )
    ).rows[0]['QUERY PLAN'];
    for (const [name, column, direction, k] of [
      ['uid', 'uid', 'ASC', 'k0'],
      ['name', 'e2', 'ASC', 'k1'],
      ['rank', 'e5', 'DESC', 'k2'],
    ]) {
      plans[name] = (
        await db.pool.query(
          `EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT uid FROM ${relation} WHERE to_revision IS NULL AND NOT deleted AND ${k} ORDER BY ${column} ${direction} NULLS LAST,uid ASC LIMIT 50`,
        )
      ).rows[0]['QUERY PLAN'];
    }
    await writeFile(
      join(options.output, 'operator-index-plans.json'),
      JSON.stringify(plans, null, 2),
    );
    const reindexLabels = new Set<string>();
    if (options.onVerified)
      await options.onVerified({
        db,
        environment,
        source,
        target,
        schema,
        token: credential.token,
        nativeEnv: commonEnv,
        markings,
        runtime,
        reindex: async (label: string) => {
          assert.match(label, /^[A-Za-z0-9_-]{1,48}$/);
          assert(!reindexLabels.has(label), 'fresh native reindex label');
          reindexLabels.add(label);
          await db.pool.query(
            'UPDATE moria.runtime_projection_bindings SET revision=$3 WHERE environment=$1 AND resource=$2',
            [environment, target, 'reindex-' + label],
          );
          await freshSession();
          await indexer.registerDistributed(context, {
            targetId: target,
            warehouse: root,
            partitions: wide ? 1 : 16,
            timeoutSeconds: 86400,
          });
          await Promise.all([
            run(label + '-index-1', runtime, ['drain']),
            run(label + '-index-2', runtime, ['drain']),
          ]);
          const current = (
            await db.pool.query(
              "SELECT g.stage,g.revision::text,p.id AS projection FROM moria.source_generations g JOIN moria.runtime_projections p ON p.stage=g.stage AND p.status='active' WHERE g.environment=$1 AND g.object_type=$2 ORDER BY g.revision DESC LIMIT 1",
              [environment, target],
            )
          ).rows[0];
          assert(current);
          return current;
        },
      });
    await enterPhase('complete');
    const result = {
      scope: 'exploratory',
      profile: wide ? 'wide-reducer-regression' : 'capacity',
      fullProfileCertified: false,
      population: options.rows,
      rowBytes: options.rowBytes,
      source: manifest,
      verification: verified.at(-1),
      timings,
      runs,
      timeline,
      elapsedMs: performance.now() - started,
      remaining: [
        'protected HTTP mixed load',
        'two retained source generations and 20% overlays with ten historical edits',
        'identity/visibility/width/fanout trials',
        'full repetitions, peak, recovery, soak, wider-than-memory and failure-domain proofs',
      ],
    };
    await writeFile(join(options.output, 'summary.json'), JSON.stringify(result, null, 2));
    return result;
  } catch (error) {
    const message = error instanceof Error ? error.message : '';
    const errorCode =
      /\b(UNAUTHENTICATED|AUTHORIZATION_CHANGED|BUILD_INACTIVE|RESOURCE_LIMIT|LEASE_LOST|HEAD_CONFLICT|INCOMPATIBLE_SCHEMA)\b/.exec(
        message,
      )?.[1] ?? 'UNCLASSIFIED_FAILURE';
    await writeFile(
      join(options.output, 'failure-summary.json'),
      JSON.stringify(
        {
          scope: 'exploratory',
          fullProfileCertified: false,
          outcome: 'failed',
          errorCode,
          failedPhase: activePhase,
          failedPhaseElapsedMs: performance.now() - phaseStarted,
          population: options.rows,
          rowBytes: options.rowBytes,
          elapsedMs: performance.now() - started,
          timings,
          runs,
          timeline,
        },
        null,
        2,
      ),
    );
    throw error;
  } finally {
    for (const child of children) if (child.exitCode === null) child.kill('SIGKILL');
    await db.close();
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [rows = '1000000', output, rowBytes = '4096', profile] = process.argv.slice(2);
  assert.ok(output, 'Output evidence directory is required');
  assert.ok(
    profile === undefined || profile === 'wide-reducer-regression',
    'Unknown named fixture profile',
  );
  console.log(
    JSON.stringify(
      await nativeProjectionCapacity({
        rows: Number(rows),
        rowBytes: Number(rowBytes),
        output: resolve(output),
        ...(profile ? { profile: profile as 'wide-reducer-regression' } : {}),
      }),
    ),
  );
}
