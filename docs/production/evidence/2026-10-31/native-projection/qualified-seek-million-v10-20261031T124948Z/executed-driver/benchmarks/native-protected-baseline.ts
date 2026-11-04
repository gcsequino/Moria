import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { fingerprint } from '../packages/compiler/src/index.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import type { NativeProjectionFixture } from './native-projection.js';

type Kind = 'detail' | 'page' | 'filterSort' | 'action' | 'multiAction' | 'aggregate';
interface Arrival {
  sequence: number;
  kind: Kind;
  offsetMs: number;
  uids: string[];
}
interface Sample {
  sequence: number;
  kind: Kind;
  scheduledMs: number;
  dispatchMs?: number;
  completedMs: number;
  status?: number;
  bytes?: number;
  successful: boolean;
  error?: string;
}
interface Options {
  rows: number;
  output: string;
  seed?: number;
  rates?: number[];
  windowMs?: number;
  maxOutstanding?: number;
  timeoutMs?: number;
}

export function completeProtectedSample(sample: Sample, elapsedMs: number, timeoutMs: number) {
  sample.completedMs = elapsedMs;
  if (sample.successful && sample.completedMs - sample.scheduledMs > timeoutMs) {
    sample.successful = false;
    sample.error = 'ARRIVAL_DEADLINE';
  }
  return sample;
}

export function summarizeProtectedSamples(items: readonly Sample[]) {
  const distribution = (values: number[]) => {
    values.sort((a, b) => a - b);
    const percentile = (p: number) =>
      values.length ? values[Math.min(values.length - 1, Math.ceil(values.length * p) - 1)] : null;
    return {
      count: values.length,
      p50Ms: percentile(0.5),
      p95Ms: percentile(0.95),
      p99Ms: percentile(0.99),
      maxMs: values.at(-1) ?? null,
    };
  };
  const successful = items.filter((sample) => sample.successful);
  const failed = items.filter((sample) => !sample.successful);
  const { count: _count, ...allLatency } = distribution(
    items.map((sample) => sample.completedMs - sample.scheduledMs),
  );
  return {
    scheduled: items.length,
    successful: successful.length,
    unsuccessfulFraction: items.length ? failed.length / items.length : null,
    latencyBasis: 'all-scheduled-arrivals',
    ...allLatency,
    successfulLatency: distribution(
      successful.map((sample) => sample.completedMs - sample.scheduledMs),
    ),
    dispatchDelay: distribution(
      items
        .filter((sample) => sample.dispatchMs !== undefined)
        .map((sample) => sample.dispatchMs! - sample.scheduledMs),
    ),
    notDispatched: items.filter((sample) => sample.dispatchMs === undefined).length,
    statuses: Object.fromEntries(
      [
        ...new Set(items.map((sample) => sample.status).filter((status) => status !== undefined)),
      ].map((status) => [
        String(status),
        items.filter((sample) => sample.status === status).length,
      ]),
    ),
    errors: Object.fromEntries(
      [...new Set(failed.map((sample) => sample.error))].map((error) => [
        String(error),
        failed.filter((sample) => sample.error === error).length,
      ]),
    ),
  };
}

/** Short open-loop diagnostic. This does not replace the production workload protocol. */
export async function nativeProtectedBaseline(fixture: NativeProjectionFixture, options: Options) {
  const { db, environment, target, schema, runtime, nativeEnv } = fixture;
  const seed = options.seed ?? 0x6d6f7269;
  let state = seed >>> 0;
  const random = () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return (state >>> 0) / 4294967296;
  };
  const rates = options.rates ?? [20, 50, 100],
    windowMs = options.windowMs ?? 30000;
  const maxOutstanding = options.maxOutstanding ?? 256,
    timeoutMs = options.timeoutMs ?? 10000;
  assert.ok(Number.isSafeInteger(options.rows) && options.rows > 0);
  assert.ok(rates.every((rate) => Number.isSafeInteger(rate) && rate > 0 && rate <= 10000));
  assert.ok(
    windowMs >= 1000 &&
      windowMs <= 3600000 &&
      maxOutstanding >= 1 &&
      maxOutstanding <= 10000 &&
      timeoutMs >= 1000 &&
      timeoutMs <= 30000,
  );
  const definition = {
    objectTypeId: target,
    schema,
    policies: {
      resource: target,
      markings: ['private', 'enterprise'],
      properties: schema.properties,
      sourceColumns: schema.properties,
      attributes: {
        admin: { type: 'boolean', nullable: false },
        region: { type: 'string', nullable: false },
      },
      row: { literal: true },
    },
  };
  await db.pool.query(
    'INSERT INTO moria.native_serving_bindings(environment,resource,object_type_id,revision,definition) VALUES($1,$2,$3,$4,$5)',
    [environment, target, target, fingerprint(definition), definition],
  );
  for (const group of [false, true]) {
    const resource = group ? 'group' : 'edit';
    const action = {
      resource,
      revision: 'v1',
      markings: ['private', 'enterprise'],
      targetParameter: group ? 'uids' : 'uid',
      inputs: group
        ? {
            uids: { type: 'array', nullable: false, items: schema.properties.uid },
            name: schema.properties.name,
          }
        : { uid: schema.properties.uid, name: schema.properties.name },
      condition: { literal: true },
      edits: [{ op: 'set', property: 'name', value: { parameter: 'name' } }],
    };
    await db.pool.query(
      'INSERT INTO moria.native_serving_actions(environment,resource,object_resource,revision,definition) VALUES($1,$2,$3,$4,$5)',
      [environment, resource, target, 'v1', action],
    );
  }
  const session = await new WorkloadSessions(new Authority(db.catalog)).mint(
    { environment, principal: 'pipeline', application: 'capacity' },
    'fixture',
  );
  const child = spawn(runtime, ['serve'], {
    env: { ...nativeEnv, MORIA_NATIVE_LISTEN: '127.0.0.1:0' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '',
    stderr = '';
  child.stdout!.on('data', (data) => {
    stdout = (stdout + data.toString()).slice(-65536);
  });
  child.stderr!.on('data', (data) => {
    stderr = (stderr + data.toString()).slice(-65536);
  });
  const exited = new Promise<void>((resolve) => child.once('exit', () => resolve()));
  let base: string | undefined;
  try {
    for (let attempt = 0; attempt < 1000 && !base; attempt++) {
      for (const line of stdout.split('\n')) {
        try {
          const event = JSON.parse(line);
          if (event.event === 'serving') base = `http://${event.address}`;
        } catch {}
      }
      if (child.exitCode !== null) throw new Error(`HTTP startup failed: ${stderr}`);
      if (!base) await delay(10);
    }
    assert.ok(base, `HTTP startup deadline: ${stderr}`);
    const headers = {
      authorization: `Bearer ${session.token}`,
      'content-type': 'application/json',
    };
    const kinds: Kind[] = Array.from({ length: 100 }, (_, n) =>
      n < 35
        ? 'detail'
        : n < 65
          ? 'page'
          : n < 80
            ? 'filterSort'
            : n < 90
              ? 'action'
              : n < 95
                ? 'multiAction'
                : 'aggregate',
    );
    const windows: unknown[] = [];
    for (const rate of rates) {
      const count = Math.floor((rate * windowMs) / 1000);
      const selected: number[] = [];
      const plans = Array.from({ length: count }, (_, sequence) => {
        const kind = kinds[Math.floor(random() * 100)];
        const ordinals: number[] = [];
        const targets = kind === 'multiAction' ? 5 : 1;
        assert.ok(options.rows >= targets);
        while (ordinals.length < targets) {
          const ordinal = Math.floor(
            random() *
              (random() < 0.2 ? Math.max(1, Math.floor(options.rows / 100)) : options.rows),
          );
          if (!ordinals.includes(ordinal)) ordinals.push(ordinal);
        }
        selected.push(...ordinals);
        return { sequence, kind, offsetMs: (sequence * 1000) / rate, ordinals };
      });
      const ids = new Map<string, string>();
      const keys = [...new Set(selected)].map(String);
      for (let start = 0; start < keys.length; start += 1000) {
        const rows = await db.pool.query(
          'SELECT i.canonical_key,i.uid FROM unnest($2::text[]) k(key) JOIN moria.identities i ON i.domain=$1 AND i.key_hash=moria.identity_key_hash(k.key) AND i.canonical_key=k.key',
          [`capacity-${environment}`, keys.slice(start, start + 1000)],
        );
        for (const row of rows.rows) ids.set(row.canonical_key, row.uid);
      }
      assert.equal(ids.size, keys.length);
      const arrivals: Arrival[] = plans.map(({ ordinals, ...plan }) => ({
        ...plan,
        uids: ordinals.map((ordinal) => ids.get(String(ordinal))!),
      }));
      const started = performance.now(),
        startedAt = new Date().toISOString();
      const pending = new Set<Promise<void>>();
      const samples: Sample[] = [];
      const perform = async (arrival: Arrival) => {
        const dispatch = performance.now() - started;
        const sample: Sample = {
          sequence: arrival.sequence,
          kind: arrival.kind,
          scheduledMs: arrival.offsetMs,
          dispatchMs: dispatch,
          completedMs: dispatch,
          successful: false,
        };
        try {
          const remaining = timeoutMs - (dispatch - arrival.offsetMs);
          if (remaining <= 0) throw new Error('ARRIVAL_DEADLINE');
          const uid = arrival.uids[0];
          let path = `/api/v1/objects/${target}/${uid}`,
            input: unknown;
          if (arrival.kind === 'page' || arrival.kind === 'filterSort') {
            path = `/api/v1/objects/${target}/query`;
            input = {
              pageSize: 50,
              projection: ['uid', 'id', 'name', 'region', 'enabled', 'rank', 'amount'],
              ...(arrival.kind === 'filterSort'
                ? {
                    sort: [{ property: 'rank', direction: 'desc' }],
                    filter: { op: 'gte', args: [{ field: 'rank' }, { literal: '0' }] },
                  }
                : { filter: { op: 'gte', args: [{ field: 'uid' }, { literal: uid }] } }),
            };
          } else if (arrival.kind === 'action' || arrival.kind === 'multiAction') {
            path = `/api/v1/actions/${arrival.kind === 'action' ? 'edit' : 'group'}/invoke`;
            input = {
              input: {
                ...(arrival.kind === 'action' ? { uid } : { uids: arrival.uids }),
                name: `baseline-${rate}-${arrival.sequence}`,
              },
            };
          } else if (arrival.kind === 'aggregate') {
            path = `/api/v1/objects/${target}/aggregate`;
            input = {
              scope: { fromUid: uid, throughUid: uid },
              budget: { scanRows: 1, groups: 1, responseBytes: 262144, timeMs: 10000 },
              metrics: { count: { op: 'count' } },
            };
          }
          const response = await fetch(`${base}${path}`, {
            method: input === undefined ? 'GET' : 'POST',
            headers: {
              ...headers,
              ...(arrival.kind === 'action' || arrival.kind === 'multiAction'
                ? { 'idempotency-key': randomUUID() }
                : {}),
            },
            body: input === undefined ? undefined : JSON.stringify(input),
            signal: AbortSignal.timeout(Math.max(1, Math.ceil(remaining))),
          });
          sample.status = response.status;
          const bytes = Buffer.from(await response.arrayBuffer());
          sample.bytes = bytes.length;
          assert.ok(bytes.length <= 262144, 'RESPONSE_BYTES');
          const body = JSON.parse(bytes.toString('utf8'));
          assert.equal(response.status, 200, body.error?.code ?? JSON.stringify(body));
          if (arrival.kind === 'detail') assert.equal(body.object?.uid, uid);
          if (arrival.kind === 'page' || arrival.kind === 'filterSort')
            assert.ok(Array.isArray(body.objects) && body.objects.length <= 50);
          if (arrival.kind === 'aggregate') assert.equal(body.groups?.[0]?.values?.count, '1');
          sample.successful = true;
        } catch (error) {
          sample.error =
            error instanceof Error ? error.message.slice(0, 256) : String(error).slice(0, 256);
        }
        samples.push(completeProtectedSample(sample, performance.now() - started, timeoutMs));
      };
      for (const arrival of arrivals) {
        const wait = arrival.offsetMs - (performance.now() - started);
        if (wait > 0) await delay(wait);
        if (pending.size >= maxOutstanding) {
          const now = performance.now() - started;
          samples.push({
            sequence: arrival.sequence,
            kind: arrival.kind,
            scheduledMs: arrival.offsetMs,
            completedMs: now,
            successful: false,
            error: 'OUTSTANDING_LIMIT',
          });
          continue;
        }
        const task = perform(arrival);
        pending.add(task);
        void task.finally(() => pending.delete(task));
      }
      await Promise.all(pending);
      assert.equal(samples.length, count, 'Every scheduled arrival is counted');
      samples.sort((a, b) => a.sequence - b.sequence);
      windows.push({
        rate,
        windowMs,
        startedAt,
        total: summarizeProtectedSamples(samples),
        classes: Object.fromEntries(
          [...new Set(kinds)].map((kind) => [
            kind,
            summarizeProtectedSamples(samples.filter((s) => s.kind === kind)),
          ]),
        ),
      });
      await writeFile(
        `${options.output}/http-${rate}-arrivals.json`,
        JSON.stringify({ rate, windowMs, startedAt, arrivals, samples }, null, 2),
      );
    }
    const result = {
      classification: 'exploratory-short-open-loop-baseline',
      seed,
      rows: options.rows,
      rowBytes: 4096,
      rates,
      windowMs,
      maxOutstanding,
      timeoutMs,
      replicas: 1,
      authorization: 'one actual workload session with two markings; full-visibility row policy',
      mix: { detail: 35, page: 30, filterSort: 15, action: 10, multiAction: 5, aggregate: 5 },
      queryContract:
        'Whole-population indexed queries without optional scope/budget; page uses uid>=sampled UID; filter/sort uses rank>=0 over the complete population. Client validates actual responses <=256KiB and uses a10s arrival deadline.',
      pageProjection: ['uid', 'id', 'name', 'region', 'enabled', 'rank', 'amount'],
      distribution:
        '80% uniform full population / 20% hot 1%; multiAction five selected objects; aggregate exact one-UID scope',
      limitations: [
        'Single source generation; no preloaded overlay history',
        'No human browser or restricted-visibility mix',
        'No relation fanout or contention sub-run',
        'Short windows without production warm-up, recovery, repetitions or soak',
        'Shared host resources; no production capacity/SLO certification',
      ],
      windows,
    };
    await writeFile(`${options.output}/http-baseline.json`, JSON.stringify(result, null, 2));
    return result;
  } finally {
    child.kill('SIGKILL');
    await exited;
    await writeFile(`${options.output}/http-serving.log`, stdout + '\n' + stderr);
  }
}
