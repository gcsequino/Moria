import { performance } from 'node:perf_hooks';
const [base, rowsText, clientsText, secondsText, trial] = process.argv.slice(2);
const rows = Number(rowsText),
  clients = Number(clientsText),
  seconds = Number(secondsText);
const results: Record<
  string,
  { latency: number[]; success: number; errors: Record<string, number> }
> = {};
let sequence = 0,
  bytes = 0;
const start = performance.now(),
  cpu = process.cpuUsage(),
  end = start + seconds * 1000;
function idFor(n: number) {
  let id = ((n * 104729) % Math.floor(rows / 4)) * 4 + 1;
  while (id % 5 === 0 || id % 13 === 0) id = id + 4 > rows ? 1 : id + 4;
  return id;
}
await Promise.all(
  Array.from({ length: clients }, async () => {
    while (performance.now() < end) {
      const n = sequence++,
        id = idFor(n),
        kind = n % 10 === 0 ? 'action' : n % 10 < 3 ? 'detail' : 'queue';
      const stats = (results[kind] ??= { latency: [], success: 0, errors: {} });
      const route =
        kind === 'action'
          ? `/action?id=${id}&key=${trial}-${n}`
          : kind === 'detail'
            ? `/detail?id=${id}`
            : `/queue?after=${id}&priority=${n % 6}&layout=typed`;
      const began = performance.now();
      try {
        const response = await fetch(base + route, {
          method: kind === 'action' ? 'POST' : 'GET',
          headers: { authorization: 'benchmark-only-token' },
          signal: AbortSignal.timeout(5000),
        });
        const text = await response.text();
        bytes += Buffer.byteLength(text);
        if (response.status !== 200) throw new Error(String(response.status));
        const value = JSON.parse(text);
        const list = kind === 'queue' ? value : kind === 'detail' ? [value] : [];
        if (kind === 'queue' && (!Array.isArray(list) || list.length !== 100))
          throw new Error('BAD_PAGE');
        if (list.some((r: any) => r.region !== 'East' || Object.hasOwn(r, 'internalNote')))
          throw new Error('POLICY_LEAK');
        if (kind === 'action' && typeof value.revision !== 'string') throw new Error('BAD_RECEIPT');
        stats.success++;
      } catch (error) {
        const code =
          (error as Error).name === 'TimeoutError' ? 'TIMEOUT' : (error as Error).message;
        stats.errors[code] = (stats.errors[code] ?? 0) + 1;
      }
      stats.latency.push(performance.now() - began);
    }
  }),
);
const elapsedMs = performance.now() - start,
  usage = process.cpuUsage(cpu);
const summarized = Object.fromEntries(
  Object.entries(results).map(([name, stats]) => {
    stats.latency.sort((a, b) => a - b);
    const percentile = (p: number) =>
      stats.latency[Math.min(stats.latency.length - 1, Math.ceil(p * stats.latency.length) - 1)];
    return [
      name,
      {
        attempts: stats.latency.length,
        success: stats.success,
        errors: stats.errors,
        p50Ms: percentile(0.5),
        p95Ms: percentile(0.95),
        p99Ms: percentile(0.99),
        maxMs: stats.latency.at(-1),
      },
    ];
  }),
);
console.log(
  JSON.stringify({
    clients,
    elapsedMs,
    attempts: sequence,
    successfulRps: (Object.values(results).reduce((n, s) => n + s.success, 0) * 1000) / elapsedMs,
    responseBytes: bytes,
    generatorCpuCores: (usage.user + usage.system) / 1000 / elapsedMs,
    operations: summarized,
  }),
);
