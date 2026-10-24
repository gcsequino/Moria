import type { Jobs, Lineage } from './generated/index.js';

export const forecastAgeWarningHours = 12;
function metadata(value: string): Record<string, unknown> {
  try {
    const result: unknown = JSON.parse(value);
    return result && typeof result === 'object' && !Array.isArray(result)
      ? (result as Record<string, unknown>)
      : {};
  } catch {
    return {};
  }
}
function timestamp(value: unknown): number | undefined {
  if (typeof value !== 'string') return;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}
export function workloadHealth(
  kind: string,
  data: Jobs,
  lineage: Lineage | undefined,
  now: number,
) {
  const jobs = data.jobs.filter((job) => job.kind === kind);
  const schedule = data.schedules.find((item) => item.kind === kind);
  const latest = [...jobs].sort((a, b) => Date.parse(b.created_at) - Date.parse(a.created_at))[0];
  const successful = jobs
    .filter((job) => job.status === 'succeeded')
    .sort((a, b) => Date.parse(b.updated_at) - Date.parse(a.updated_at))[0];
  const publications = (lineage?.nodes ?? [])
    .filter((node) =>
      kind === 'weather'
        ? node.kind === 'weather-publication' && node.status === 'committed'
        : node.kind === 'snow-sync' && node.status === 'completed',
    )
    .map((node) => ({
      node,
      at: timestamp(metadata(node.detail)[kind === 'weather' ? 'publishedAt' : 'completedAt']),
    }))
    .filter((item): item is { node: Lineage['nodes'][number]; at: number } => item.at !== undefined)
    .sort((a, b) => b.at - a.at);
  const publication = publications[0];
  const completionAt = successful ? timestamp(successful.updated_at) : undefined;
  const publishedAt = publication?.at ?? completionAt;
  const nextRun = timestamp(schedule?.next_run);
  const overdue = !!schedule?.enabled && nextRun !== undefined && nextRun < now;
  const intervalExceeded =
    !!schedule?.enabled &&
    publishedAt !== undefined &&
    now - publishedAt > schedule.interval_minutes * 60000;
  const active = jobs.find((job) => ['queued', 'running', 'retrying'].includes(job.status));
  const forecasts =
    kind === 'weather' && publication
      ? (lineage?.edges ?? [])
          .filter((edge) => edge.from === publication.node.id && edge.label === 'contains')
          .flatMap((edge) => {
            const node = lineage?.nodes.find(
              (item) => item.id === edge.to && item.kind === 'area-forecast',
            );
            if (!node) return [];
            const issuedAt = timestamp(metadata(node.detail).sourceUpdatedAt);
            return [
              {
                name: node.label,
                issuedAt,
                stale: issuedAt !== undefined && now - issuedAt > forecastAgeWarningHours * 3600000,
              },
            ];
          })
      : [];
  return {
    schedule,
    latest,
    active,
    publication,
    publishedAt,
    overdue,
    intervalExceeded,
    forecasts,
    successful,
    failure: latest?.status === 'failed' ? latest : undefined,
  };
}
