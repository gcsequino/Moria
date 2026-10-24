import React from 'react';
import type { Jobs, Lineage } from './generated/index.js';
import { forecastAgeWarningHours, workloadHealth } from './health.js';
export function OperationalHealth({
  data,
  lineage,
  lineageError,
  now,
  onJob,
  onLineage,
}: {
  data: Jobs;
  lineage?: Lineage;
  lineageError: string;
  now: number;
  onJob: (id: string) => void;
  onLineage: () => void;
}) {
  return (
    <section className="panel apps-panel health-panel">
      <div className="panel-head">
        <div className="eyebrow">OPERATIONS</div>
        <h2>Refresh health</h2>
        <p>
          Publication history and schedule evidence from this instance. These checks do not
          establish whether mountain conditions are safe.
        </p>
      </div>
      {lineageError && (
        <p className="operation-error">
          Publication details unavailable: {lineageError}. Job history remains available below.
        </p>
      )}
      <div className="workloads">
        {(['snotel', 'weather'] as const).map((kind) => {
          const health = workloadHealth(kind, data, lineage, now);
          const stale = health.forecasts.filter((item) => item.stale);
          const attention =
            !!health.failure || health.overdue || health.intervalExceeded || stale.length > 0;
          return (
            <article className="workload" key={kind}>
              <div className="workload-heading">
                <h3>{kind === 'snotel' ? 'Snow observations' : 'Mountain weather'}</h3>
                <span className={`health-label ${attention ? 'attention' : 'neutral'}`}>
                  {attention
                    ? 'Review needed'
                    : health.publishedAt === undefined
                      ? 'Publication unknown'
                      : 'No warning in available evidence'}
                </span>
              </div>
              <dl>
                <div>
                  <dt>
                    {health.publication
                      ? 'Last recorded publication'
                      : 'Last successful job completion'}
                  </dt>
                  <dd>
                    {health.publishedAt !== undefined
                      ? new Date(health.publishedAt).toLocaleString()
                      : 'Not present in the returned history'}
                  </dd>
                </div>
                <div>
                  <dt>Refresh schedule</dt>
                  <dd>
                    {!health.schedule
                      ? 'No schedule returned'
                      : !health.schedule.enabled
                        ? 'Paused'
                        : health.overdue
                          ? `Due since ${new Date(health.schedule.next_run).toLocaleString()}`
                          : `Next run ${new Date(health.schedule.next_run).toLocaleString()}`}
                  </dd>
                </div>
                <div>
                  <dt>Work in progress</dt>
                  <dd>
                    {health.active
                      ? `${health.active.status} · ${health.active.phase}`
                      : 'None in recent job history'}
                  </dd>
                </div>
              </dl>
              {health.failure && (
                <p className="health-warning">
                  Latest job failed: {health.failure.error || 'No error detail recorded.'}
                </p>
              )}
              {health.overdue && (
                <p className="health-warning">
                  The enabled schedule’s next-run timestamp has passed. This can also occur while
                  the scheduler is claiming due work.
                </p>
              )}
              {health.intervalExceeded && (
                <p className="health-warning">
                  No newer successful publication or completion is visible within the configured{' '}
                  {health.schedule!.interval_minutes}-minute refresh interval.
                </p>
              )}
              {stale.length > 0 && (
                <p className="health-warning">
                  Forecast issue time exceeds {forecastAgeWarningHours} hours:{' '}
                  {stale.map((item) => item.name).join(', ')}.
                </p>
              )}
              {kind === 'weather' && (
                <p className="hint">
                  Forecast-age warning policy: {forecastAgeWarningHours} hours, matching Snow.{' '}
                  {health.forecasts.length
                    ? `${health.forecasts.filter((item) => item.issuedAt !== undefined).length} of ${health.forecasts.length} visible area forecasts have issue timestamps.`
                    : 'Source issue-time evidence unavailable.'}{' '}
                  Forecast expiration is not evaluated here.
                </p>
              )}
              {kind === 'snotel' && (
                <p className="hint">
                  This is ingestion freshness. Observation dates and NRCS reporting delays are not
                  assessed by this overview.
                </p>
              )}
              <div className="edit-buttons">
                {health.latest && (
                  <button className="quiet" onClick={() => onJob(health.latest!.id)}>
                    Inspect latest job
                  </button>
                )}
                {health.successful && health.successful.id !== health.latest?.id && (
                  <button className="quiet" onClick={() => onJob(health.successful!.id)}>
                    Inspect successful job
                  </button>
                )}
                <button className="quiet" onClick={onLineage}>
                  Explore lineage
                </button>
              </div>
            </article>
          );
        })}
      </div>
      <p className="hint">
        Schedule due means the recorded next-run time is earlier than this status snapshot; no grace
        period is applied. Publication age is compared with the configured refresh interval. Results
        use bounded job and lineage history; missing evidence is unknown, not healthy. Existing
        published data can remain available after a failed refresh.
      </p>
    </section>
  );
}
