import React, { useEffect, useRef, useState } from 'react';
import { sdk } from './client.js';
import { OperationalHealth } from './operational-health.js';
import type { Lineage } from './generated/index.js';

type Jobs = Awaited<ReturnType<typeof sdk.endpoints.jobs>>;
type Job = Jobs['jobs'][number];
type View = 'overview' | 'assets' | 'apps' | 'executions' | 'lineage';
const labels: Record<string, string> = { snotel: 'Snow observations', weather: 'Mountain weather' };
function date(value: string) {
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString();
}
function Provenance({ value }: { value: string }) {
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    return <p>{value}</p>;
  }
  const entries =
    parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? Object.entries(parsed) : [];
  return (
    <>
      <dl className="detail-grid">
        {entries
          .filter(([, entry]) => entry === null || typeof entry !== 'object')
          .map(([key, entry]) => (
            <div key={key}>
              <dt>{key.replace(/_/g, ' ').replace(/([a-z])([A-Z])/g, '$1 $2')}</dt>
              <dd>{entry === null ? 'Not recorded' : String(entry)}</dd>
            </div>
          ))}
      </dl>
      <details>
        <summary>Full result and provenance record</summary>
        <pre className="provenance">{JSON.stringify(parsed, null, 2)}</pre>
      </details>
    </>
  );
}

export function Platform({
  view,
  principal,
  ready,
  revision,
  onNavigate,
}: {
  view: 'overview' | 'executions';
  principal: string;
  ready: boolean;
  revision: string;
  onNavigate: (view: View) => void;
}) {
  const [data, setData] = useState<Jobs>();
  const [lineage, setLineage] = useState<Lineage>();
  const [lineageError, setLineageError] = useState('');
  const [apps, setApps] = useState<Awaited<ReturnType<typeof sdk.endpoints.apps>>>([]);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [pending, setPending] = useState(false);
  const [updated, setUpdated] = useState<string>();
  const [filter, setFilter] = useState('all');
  const [selected, setSelected] = useState<string>();
  const alive = useRef(true);
  const mutation = useRef(false);
  const requestKeys = useRef<Record<string, string>>({});
  const reload = useRef<() => Promise<void>>(async () => {});
  useEffect(() => {
    alive.current = true;
    const controller = new AbortController();
    let reading = false;
    async function refresh() {
      if (reading || controller.signal.aborted) return;
      reading = true;
      try {
        const [jobs, applications, graph] = await Promise.all([
          sdk.endpoints.jobs({ signal: controller.signal }),
          sdk.endpoints.apps({ signal: controller.signal }),
          view === 'overview'
            ? sdk.endpoints
                .lineage({ signal: controller.signal })
                .then((value) => ({ value, error: '' }))
                .catch((error: unknown) => ({
                  value: undefined,
                  error: error instanceof Error ? error.message : 'Unable to read lineage',
                }))
            : Promise.resolve(undefined),
        ]);
        if (controller.signal.aborted) return;
        setData(jobs);
        setApps(applications);
        if (graph) {
          setLineage(graph.value);
          setLineageError(graph.error);
        }
        setUpdated(new Date().toISOString());
        setError('');
      } catch (e) {
        if (!controller.signal.aborted) {
          setError(e instanceof Error ? e.message : 'Could not refresh instance status.');
          // Do not leave previously authorized data visible after access is revoked.
          if (
            e &&
            typeof e === 'object' &&
            'status' in e &&
            (e.status === 401 || e.status === 403)
          ) {
            setData(undefined);
            setLineage(undefined);
            setApps([]);
          }
        }
      } finally {
        reading = false;
      }
    }
    reload.current = refresh;
    void refresh();
    const visibleRefresh = () => {
      if (document.visibilityState === 'visible') void refresh();
    };
    const timer = window.setInterval(visibleRefresh, 10000);
    document.addEventListener('visibilitychange', visibleRefresh);
    return () => {
      alive.current = false;
      controller.abort();
      window.clearInterval(timer);
      document.removeEventListener('visibilitychange', visibleRefresh);
    };
  }, [principal, view]);
  async function act(work: () => Promise<unknown>, message: string) {
    if (mutation.current) return;
    mutation.current = true;
    setPending(true);
    setNotice('');
    try {
      await work();
      if (alive.current) {
        setNotice(message);
        await reload.current();
      }
    } catch (e) {
      if (alive.current) setError(e instanceof Error ? e.message : 'Operation failed.');
    } finally {
      mutation.current = false;
      if (alive.current) setPending(false);
    }
  }
  async function sync(kind: 'snotel' | 'weather') {
    const requestKey = (requestKeys.current[kind] ??= crypto.randomUUID());
    const job = await (kind === 'snotel'
      ? sdk.endpoints.syncSnow({ requestKey })
      : sdk.endpoints.syncWeather({ requestKey }));
    delete requestKeys.current[kind];
    if (alive.current) setSelected(job.id);
  }
  const jobs = data?.jobs ?? [];
  const active = jobs.filter((j) => ['queued', 'running', 'retrying'].includes(j.status));
  const failed = jobs.filter((j) => j.status === 'failed');
  const shown = jobs.filter(
    (j) =>
      filter === 'all' ||
      (filter === 'active'
        ? ['queued', 'running', 'retrying'].includes(j.status)
        : j.status === filter),
  );
  const detail = jobs.find((j) => j.id === selected);
  function jobRow(job: Job) {
    return (
      <tr key={job.id}>
        <td>
          <strong>{labels[job.kind] ?? job.kind}</strong>
          <small>{date(job.created_at)}</small>
        </td>
        <td>
          <span className={`job-status ${job.status}`}>{job.status}</span>
          <small>{job.phase || 'Awaiting execution'}</small>
        </td>
        <td>{job.attempt}</td>
        <td>
          <button className="quiet" onClick={() => setSelected(job.id)}>
            Inspect run
          </button>
        </td>
      </tr>
    );
  }
  return (
    <div className="platform">
      <div className="platform-toolbar">
        <span>
          {updated
            ? `Updated ${date(updated)} · refreshes every 10 seconds while visible`
            : 'Loading instance status…'}
        </span>
        <button className="quiet" disabled={pending} onClick={() => void reload.current()}>
          Refresh status
        </button>
      </div>
      {error && (
        <p className="operation-error" role="alert">
          {error}
        </p>
      )}
      {notice && (
        <p className="operation-notice" role="status">
          {notice}
        </p>
      )}
      {view === 'overview' && (
        <>
          <section className="metrics" aria-label="Instance overview">
            <button onClick={() => onNavigate('assets')}>
              <span>Asset dataset</span>
              <strong>{ready ? 'Published' : 'Not imported'}</strong>
              <small>{ready ? `Revision ${revision}` : 'Import your first source'}</small>
            </button>
            <button onClick={() => onNavigate('apps')}>
              <span>Applications</span>
              <strong>{data ? apps.length : '—'}</strong>
              <small>Installed on this instance</small>
            </button>
            <button onClick={() => onNavigate('executions')}>
              <span>Active executions</span>
              <strong>{data ? active.length : '—'}</strong>
              <small>Queued or running</small>
            </button>
            <button
              onClick={() => {
                setFilter('failed');
                onNavigate('executions');
              }}
            >
              <span>Failed executions</span>
              <strong>{data ? failed.length : '—'}</strong>
              <small>In recent history</small>
            </button>
          </section>
          {data && (
            <OperationalHealth
              data={data}
              lineage={lineage}
              lineageError={lineageError}
              now={updated ? Date.parse(updated) : Date.now()}
              onJob={(id) => {
                setSelected(id);
                setFilter('all');
                onNavigate('executions');
              }}
              onLineage={() => onNavigate('lineage')}
            />
          )}
          <section className="panel access-panel">
            <div>
              <div className="eyebrow">YOUR ACCESS</div>
              <h2>Instance administrator</h2>
              <p>
                You can import and edit assets, inspect applications, and manage refresh jobs. Snow
                uses a separate read-only app scope. Every API request is authorized by the
                instance.
              </p>
            </div>
            <div>
              <label>Signed-in principal</label>
              <code>{principal}</code>
              <p>
                This console currently requires administrator access. It does not yet provide a
                general explorer for every resource type or permission level.
              </p>
            </div>
          </section>
        </>
      )}
      <section className="panel apps-panel">
        <div className="panel-head">
          <div className="eyebrow">DATA REFRESH</div>
          <h2>Connected workloads</h2>
          <p>
            Run a refresh now or control its recurring schedule. Previously published data remains
            available while a refresh runs.
          </p>
        </div>
        <div className="workloads">
          {(['snotel', 'weather'] as const).map((kind) => {
            const schedule = data?.schedules.find((s) => s.kind === kind);
            const last = jobs.find((j) => j.kind === kind);
            return (
              <article className="workload" key={kind}>
                <div className="workload-heading">
                  <h3>{labels[kind]}</h3>
                  <span className="badge">
                    {!data ? 'Loading' : schedule?.enabled ? 'Scheduled' : 'Schedule paused'}
                  </span>
                </div>
                <p>
                  {kind === 'snotel'
                    ? 'NRCS station observations: snow depth and snow water equivalent.'
                    : 'Weather forecasts for the configured Front Range locations.'}
                </p>
                <dl>
                  <div>
                    <dt>Frequency</dt>
                    <dd>
                      Every {schedule?.interval_minutes ?? (kind === 'snotel' ? 360 : 60)} minutes
                    </dd>
                  </div>
                  <div>
                    <dt>Next scheduled run</dt>
                    <dd>{schedule?.enabled ? date(schedule.next_run) : 'Paused'}</dd>
                  </div>
                  <div>
                    <dt>Latest execution</dt>
                    <dd>
                      {last
                        ? `${last.status} · ${date(last.updated_at)}`
                        : 'No executions recorded'}
                    </dd>
                  </div>
                </dl>
                <div className="edit-buttons">
                  <button
                    className="primary"
                    disabled={pending || !data}
                    onClick={() => void act(() => sync(kind), `${labels[kind]} refresh requested.`)}
                  >
                    Sync now
                  </button>
                  <button
                    className="quiet"
                    disabled={pending || !data}
                    onClick={() =>
                      void act(
                        () => {
                          const input = {
                            enabled: !schedule?.enabled,
                            intervalMinutes:
                              schedule?.interval_minutes ?? (kind === 'snotel' ? 360 : 60),
                          };
                          return kind === 'snotel'
                            ? sdk.endpoints.setSnowSchedule(input)
                            : sdk.endpoints.setWeatherSchedule(input);
                        },
                        schedule?.enabled
                          ? 'Schedule paused. Existing jobs are unchanged.'
                          : 'Schedule enabled.',
                      )
                    }
                  >
                    {schedule?.enabled ? 'Pause schedule' : 'Enable schedule'}
                  </button>
                </div>
              </article>
            );
          })}
        </div>
      </section>
      <section className="panel">
        <div className="panel-head explorer-head">
          <div>
            <div className="eyebrow">DURABLE EXECUTION</div>
            <h2>{view === 'overview' ? 'Recent activity' : 'Execution history'}</h2>
          </div>
          <label className="filter-label">
            Show
            <select value={filter} onChange={(e) => setFilter(e.target.value)}>
              <option value="all">All statuses</option>
              <option value="active">Active</option>
              <option value="succeeded">Succeeded</option>
              <option value="failed">Failed</option>
            </select>
          </label>
        </div>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Workload / requested</th>
                <th>Status / phase</th>
                <th>Attempt</th>
                <th>
                  <span className="sr-only">Details</span>
                </th>
              </tr>
            </thead>
            <tbody>{(view === 'overview' ? shown.slice(0, 5) : shown).map(jobRow)}</tbody>
          </table>
        </div>
        {!shown.length && (
          <div className="empty">
            <h3>{data ? 'No matching executions yet.' : 'Waiting for execution status.'}</h3>
            <p>
              {data
                ? 'Start a sync above, or enable a schedule.'
                : 'The instance must authorize and return this information.'}
            </p>
          </div>
        )}
        {detail && (
          <section className="job-detail">
            <div className="workload-heading">
              <h2>{labels[detail.kind] ?? detail.kind} · execution detail</h2>
              <button className="quiet" onClick={() => setSelected(undefined)}>
                Close
              </button>
            </div>
            <dl className="detail-grid">
              <div>
                <dt>Execution ID</dt>
                <dd>
                  <code>{detail.id}</code>
                </dd>
              </div>
              <div>
                <dt>Status</dt>
                <dd>
                  {detail.status} · {detail.phase}
                </dd>
              </div>
              <div>
                <dt>Requested by</dt>
                <dd>{detail.requested_by}</dd>
              </div>
              <div>
                <dt>Latest update</dt>
                <dd>{date(detail.updated_at)}</dd>
              </div>
            </dl>
            {detail.error && <p className="operation-error">{detail.error}</p>}
            {detail.status === 'failed' && (
              <button
                className="primary"
                disabled={pending}
                onClick={() =>
                  void act(() => sdk.endpoints.retryJob({ id: detail.id }), 'Retry requested.')
                }
              >
                Retry execution
              </button>
            )}
            <h3>Result & provenance</h3>
            {detail.result ? (
              <Provenance value={detail.result} />
            ) : (
              <p>No published result recorded for this execution yet.</p>
            )}
          </section>
        )}
      </section>
      <section className="recovery-note">
        <h3>Operator recovery</h3>
        <p>
          The CLI remains available independently of this console. Recover management access locally
          with <code>pnpm moria auth recover --app management</code>; inspect and recover
          application releases with <code>pnpm moria apps</code>.
        </p>
      </section>
    </div>
  );
}
