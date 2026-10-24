import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createApi } from '../apps/api/src/server.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { Authority } from '../services/security/src/authority.js';
import { ObjectStore, type ObjectSchema } from '../services/ontology/src/store.js';
import { ObjectService, type ActionBinding } from '../services/ontology/src/service.js';
import { queryRows } from '../services/ontology/src/query.js';

/** Real HTTP, session, policy, generation and overlay paths; intentionally local.
 * Generator shares the server event loop, so this is not a capacity ceiling. */
export async function httpWorkload(
  store: ObjectStore,
  schema: ObjectSchema,
  count: number,
  seconds: number,
  refresh = false,
) {
  const authority = new Authority(store.catalog);
  await authority.migrate();
  await authority.syncPrincipal('stream', 'setup', 'http-reader', {
    kind: 'workload',
    enabled: true,
    attributes: { region: 'East' },
    markings: ['sensitive'],
  });
  await authority.setApplication('stream', 'setup', 'http-app', ['orders:read', 'assign:invoke']);
  await authority.setGrants('stream', 'setup', 'http-reader', 'http-app', [
    'orders:read',
    'assign:invoke',
  ]);
  const service = new ObjectService(store, authority, {
    schema,
    policies: {
      resource: 'orders',
      markings: ['sensitive'],
      properties: schema.properties,
      sourceColumns: schema.properties,
      attributes: { region: { type: 'string', nullable: false } },
      row: { op: 'eq', args: [{ field: 'region' }, { principalAttribute: 'region' }] },
      propertyRead: { assignee: { literal: false } },
    },
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
  const internalQueryErrors: Record<string, number> = {},
    query = service.query.bind(service);
  service.query = async (...args) => {
    try {
      return await query(...args);
    } catch (error) {
      const code = (error as any).code ?? 'ERROR';
      internalQueryErrors[code] = (internalQueryErrors[code] ?? 0) + 1;
      throw error;
    }
  };
  const sessions = new WorkloadSessions(authority),
    credential = await sessions.mint(
      { environment: 'stream', principal: 'http-reader', application: 'http-app' },
      'setup',
    );
  const server = createApi(sessions, {
    objects: new Map([['orders', service]]),
    actions: new Map([['assign', { service, action }]]),
  });
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  const base = `http://127.0.0.1:${(server.address() as { port: number }).port}`,
    run = randomUUID();
  const uid = (i: number) => `00000000-0000-0000-0000-${i.toString(16).padStart(12, '0')}`;
  async function request(path: string, body?: unknown, key?: string) {
    const response = await fetch(base + path, {
      method: body ? 'POST' : 'GET',
      headers: {
        authorization: `Bearer ${credential.token}`,
        'content-type': 'application/json',
        ...(key ? { 'idempotency-key': key } : {}),
      },
      ...(body ? { body: JSON.stringify(body) } : {}),
      signal: AbortSignal.timeout(35000),
    });
    const value = (await response.json()) as any;
    if (!response.ok)
      throw Object.assign(new Error('HTTP operation failed'), {
        code: `${response.status}:${value.error?.code ?? 'UNKNOWN'}`,
      });
    return value;
  }
  const results = [];
  try {
    for (const clients of [20, 100]) {
      // Each independent phase starts with its own normal 15-minute credential.
      // No session TTL is weakened to accommodate a long benchmark.
      Object.assign(
        credential,
        await sessions.mint(
          { environment: 'stream', principal: 'http-reader', application: 'http-app' },
          'benchmark-phase',
        ),
      );
      let refreshDone = !refresh,
        refreshResult: unknown,
        started!: () => void,
        baseRevision: string | undefined,
        plan: unknown;
      const ready = new Promise<void>((r) => (started = r));
      let ingest: Promise<void> | undefined;
      if (refresh) {
        const env = (
          await store.catalog.pool.query(
            "SELECT revision,security_epoch FROM moria.environments WHERE id='stream'",
          )
        ).rows[0];
        baseRevision = env.revision;
        async function* input() {
          started();
          for (let i = 1; i <= count; i++)
            yield { uid: uid(i), region: i % 2 ? 'East' : 'West', assignee: `source-${clients}` };
        }
        const began = performance.now();
        ingest = (async () => {
          try {
            const stage = await store.stage(
                'stream',
                'orders',
                schema,
                input(),
                env.security_epoch,
                env.revision,
                undefined,
                ['sensitive'],
              ),
              stagingMs = performance.now() - began;
            const publish = performance.now(),
              revision = await store.publishStage(stage);
            refreshResult = { stagingMs, publicationMs: performance.now() - publish, revision };
          } catch (error) {
            refreshResult = { error: (error as any).code ?? 'ERROR' };
          } finally {
            refreshDone = true;
            started();
          }
        })();
        await ready;
      }
      const start = performance.now(),
        end = start + seconds * 1000;
      let next = 0;
      const stats = Object.fromEntries(
        ['query', 'detail', 'action'].map((k) => [
          k,
          { success: 0, latencies: [] as number[], failures: {} as Record<string, number> },
        ]),
      );
      await Promise.all(
        Array.from({ length: clients }, async () => {
          let cursor: string | undefined,
            last = '',
            pin: string | undefined;
          while (performance.now() < end || !refreshDone) {
            const index = next++,
              kind = index % 10 < 7 ? 'query' : index % 10 < 9 ? 'detail' : 'action',
              began = performance.now();
            try {
              const target = uid(1 + 2 * ((index * 104729) % Math.floor(count / 2)));
              if (kind === 'query') {
                const page = await request('/v1/objects/orders/query', {
                  pageSize: 100,
                  ...(cursor ? { cursor } : {}),
                });
                assert.match(page.revision, /^\d+$/);
                if (pin) assert.equal(page.revision, pin);
                else pin = page.revision;
                assert.ok(page.objects.length <= 100);
                for (const row of page.objects) {
                  assert.equal(row.region, 'East');
                  assert.equal(Object.hasOwn(row, 'assignee'), false);
                  assert.ok(row.uid > last);
                  last = row.uid;
                }
                cursor = page.nextCursor;
                if (!cursor) {
                  last = '';
                  pin = undefined;
                }
              } else if (kind === 'detail') {
                const page = await request(`/v1/objects/orders/${target}`);
                assert.equal(page.object.uid, target);
                assert.equal(page.object.region, 'East');
                assert.equal(Object.hasOwn(page.object, 'assignee'), false);
              } else {
                const receipt = await request(
                  '/v1/actions/assign/invoke',
                  { input: { uid: target, assignee: 'Benchmark' } },
                  `${run}-${clients}-${index}`,
                );
                assert.match(receipt.revision, /^\d+$/);
              }
              stats[kind].success++;
            } catch (error) {
              const code = (error as any).code ?? 'ERROR';
              stats[kind].failures[code] = (stats[kind].failures[code] ?? 0) + 1;
            }
            stats[kind].latencies.push(performance.now() - began);
          }
        }),
      );
      await ingest;
      const elapsedMs = performance.now() - start;
      if (refresh && refreshResult && !(refreshResult as { error?: string }).error) {
        const c = await store.catalog.pool.connect();
        try {
          const current = (
            await c.query("SELECT revision FROM moria.environments WHERE id='stream'")
          ).rows[0].revision;
          assert.equal(
            (await store.read(c, 'stream', 'orders', uid(1), current))!.source.assignee,
            `source-${clients}`,
          );
          const edited = await c.query(
            "SELECT DISTINCT uid FROM moria.overlay_versions WHERE environment='stream' AND object_type='orders' LIMIT 20",
          );
          for (const row of edited.rows)
            assert.equal(
              (await store.read(c, 'stream', 'orders', row.uid, current))!.effective.assignee,
              'Benchmark',
            );
          const explain = new Proxy(c, {
            get(target, key) {
              if (key === 'query')
                return (sql: string, args: unknown[]) =>
                  target.query(
                    sql.startsWith('SELECT p.uid') ? 'EXPLAIN (FORMAT JSON) ' + sql : sql,
                    args,
                  );
              return Reflect.get(target, key);
            },
          });
          plan = (
            await queryRows(
              explain,
              'stream',
              service.binding.policies,
              { region: 'East' },
              baseRevision!,
              current,
              uid(1000),
              { pageSize: 100 },
            )
          ).rows;
        } finally {
          c.release();
        }
      }
      results.push({
        clients,
        elapsedMs,
        ...(refresh ? { refresh: refreshResult, pinnedAfterRefreshPlan: plan } : {}),
        cumulativeInternalQueryErrors: { ...internalQueryErrors },
        operations: Object.fromEntries(
          Object.entries(stats).map(([kind, s]) => {
            s.latencies.sort((a, b) => a - b);
            const p = (n: number) => s.latencies[Math.ceil(s.latencies.length * n) - 1] ?? null;
            return [
              kind,
              {
                attempts: s.latencies.length,
                success: s.success,
                failures: s.failures,
                successfulOpsPerSecond: (s.success * 1000) / elapsedMs,
                p50Ms: p(0.5),
                p95Ms: p(0.95),
                p99Ms: p(0.99),
                maxMs: p(1),
                overOneSecond: s.latencies.filter((x) => x > 1000).length,
              },
            ];
          }),
        ),
      });
    }
    return {
      refreshEnabled: refresh,
      results,
      scope:
        'Loopback workload-session HTTP, 70% UID-ordered pages of 100 / 20% population-distributed detail / 10% idempotent actions. Current markings, canonical region restriction and hidden assignee property. Each caller maintains a pinned cursor chain; actions accumulate overlays. No retry by generator. All attempt latencies include failures. 20 database connections. Same Node process hosts server and fetch generator. One timed run per concurrency, no separate warmup; 100-client phase inherits prior overlays/cache. Optional refresh streams a full same-schema generation per phase; enabled runs continue until both the minimum duration and publication finish. Otherwise no ingestion during load. No OIDC, TLS, separate network host, cold-cache guarantee or capacity certification.',
    };
  } finally {
    server.closeAllConnections();
    await new Promise<void>((r, j) => server.close((e) => (e ? j(e) : r())));
  }
}
