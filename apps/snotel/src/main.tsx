import React, { useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { sdk, segments, type Station, type Observation, type Status } from './client.js';
import './style.css';
import { Weather } from './Weather.js';
import { useAppSession } from '../../shared/auth.js';
const date = (value: string) =>
  new Date(value + 'T00:00:00Z').toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    timeZone: 'UTC',
  });
const amount = (n: number | null | undefined) => (n == null ? '—' : n.toFixed(1));
function Chart({
  rows,
  field,
  title,
  color,
}: {
  rows: Observation[];
  field: 'snowDepth' | 'swe';
  title: string;
  color: string;
}) {
  const values = rows.flatMap((r) => (r[field] === null ? [] : [r[field]!])),
    lower = Math.min(0, ...values),
    upper = Math.max(1, ...values) * 1.08;
  const width = 780,
    height = 240,
    left = 50,
    right = 16,
    top = 22,
    bottom = 36;
  const first = rows.length ? Date.parse(rows[0].date) : 0,
    last = rows.length ? Date.parse(rows.at(-1)!.date) : 1;
  const x = (r: Observation) =>
    left +
    ((Date.parse(r.date) - first) / Math.max(86400000, last - first)) * (width - left - right);
  const y = (r: Observation) =>
    top + ((upper - r[field]!) / (upper - lower)) * (height - top - bottom);
  const [hover, setHover] = useState<Observation>();
  useEffect(() => setHover(undefined), [rows]);
  return (
    <section className="chart-card">
      <div className="chart-head">
        <div>
          <h3>{title}</h3>
          <p>Daily · inches · NRCS period-end dates</p>
        </div>
        <span className="series-key">
          {values.length} reported / {rows.length} days
        </span>
      </div>
      {hover && (
        <div className="hover-readout">
          {date(hover.date)} · {amount(hover[field])} in · QC{' '}
          {field === 'swe' ? hover.sweQc || 'not supplied' : hover.depthQc || 'not supplied'} / QA{' '}
          {field === 'swe' ? hover.sweQa || 'not supplied' : hover.depthQa || 'not supplied'}
        </div>
      )}
      {!values.length ? (
        <div className="no-values">No reported values in this range.</div>
      ) : (
        <svg
          viewBox={`0 0 ${width} ${height}`}
          role="img"
          aria-label={`${title} chart in inches. Missing values appear as gaps.`}
          onMouseLeave={() => setHover(undefined)}
        >
          {[0, 1, 2, 3, 4].map((i) => {
            const v = lower + ((upper - lower) * i) / 4,
              py = top + ((4 - i) / 4) * (height - top - bottom);
            return (
              <g key={i}>
                <line x1={left} x2={width - right} y1={py} y2={py} className="grid" />
                <text x={left - 10} y={py + 4} textAnchor="end">
                  {v.toFixed(0)}
                </text>
              </g>
            );
          })}
          {segments(rows, field).map((part, i) => (
            <polyline
              key={i}
              points={part.map((r) => `${x(r)},${y(r)}`).join(' ')}
              fill="none"
              stroke={color}
              strokeWidth="2.5"
              strokeLinejoin="round"
            />
          ))}
          {rows
            .filter((r) => r[field] !== null)
            .map((r) => (
              <circle
                key={r.date}
                cx={x(r)}
                cy={y(r)}
                r={rows.length < 40 ? 3 : 2}
                fill={color}
                onMouseEnter={() => setHover(r)}
              >
                <title>
                  {date(r.date)}: {amount(r[field])} in; QC {field === 'swe' ? r.sweQc : r.depthQc};
                  QA {field === 'swe' ? r.sweQa : r.depthQa}
                </title>
              </circle>
            ))}
          {[rows[0], rows[Math.floor(rows.length / 2)], rows.at(-1)!].map((r, i) => (
            <text
              key={i}
              x={i === 0 ? left : i === 2 ? width - right : (width + left - right) / 2}
              y={height - 9}
              textAnchor={i === 0 ? 'start' : i === 2 ? 'end' : 'middle'}
            >
              {r?.date}
            </text>
          ))}
        </svg>
      )}
      <div className="chart-foot">
        Gaps mean unavailable or withheld data, not zero. Hover over a point for its value and
        flags.
      </div>
    </section>
  );
}
function App() {
  const auth = useAppSession(sdk.auth);
  const principal = auth.session?.user?.principal;
  const [stations, setStations] = useState<Station[]>([]),
    [status, setStatus] = useState<Status>(),
    [selected, setSelected] = useState(''),
    [rows, setRows] = useState<Observation[]>([]);
  const [search, setSearch] = useState(''),
    [state, setState] = useState('all'),
    [begin, setBegin] = useState(''),
    [end, setEnd] = useState(''),
    [error, setError] = useState(''),
    [busy, setBusy] = useState(false),
    [loading, setLoading] = useState(false),
    [reload, setReload] = useState(0);
  useEffect(() => {
    let live = true;
    const controller = new AbortController();
    if (!principal) {
      setStations([]);
      setRows([]);
      setStatus(undefined);
      setSelected('');
      return;
    }
    setBusy(true);
    setError('');
    void (async () => {
      try {
        const next = await sdk.endpoints.status({ signal: controller.signal });
        const all = next.completed
          ? await sdk.objects['snow-stations'].all(
              { pageSize: 1000 },
              { signal: controller.signal },
            )
          : [];
        if (!live) return;
        setStatus(next);
        setStations(all.sort((a, b) => a.name.localeCompare(b.name)));
        setSelected((old) =>
          all.some((s) => s.triplet === old)
            ? old
            : (all.find((s) => s.triplet === '663:CO:SNTL')?.triplet ?? all[0]?.triplet ?? ''),
        );
        if (next.completed) {
          setBegin(next.completed.config.beginDate);
          setEnd(next.completed.config.endDate);
        }
      } catch (e) {
        if (live) setError(e instanceof Error ? e.message : 'Could not load stations.');
      } finally {
        if (live) setBusy(false);
      }
    })();
    return () => {
      live = false;
      controller.abort();
    };
  }, [principal, reload]);
  useEffect(() => {
    let live = true;
    const controller = new AbortController();
    setRows([]);
    if (!selected || !principal) return;
    setLoading(true);
    void sdk.objects['snow-observations']
      .all(
        {
          pageSize: 1000,
          filter: { op: 'eq', args: [{ field: 'triplet' }, { literal: selected }] },
        },
        { signal: controller.signal },
      )
      .then((data) => {
        if (live) setRows(data.sort((a, b) => a.date.localeCompare(b.date)));
      })
      .catch((e) => {
        if (live) setError(e.message);
      })
      .finally(() => {
        if (live) setLoading(false);
      });
    return () => {
      live = false;
      controller.abort();
    };
  }, [selected, principal, status?.completed?.id, reload]);
  const station = stations.find((s) => s.triplet === selected),
    visible = stations.filter(
      (s) =>
        (state === 'all' || s.state === state) &&
        `${s.name} ${s.triplet} ${s.county}`.toLowerCase().includes(search.toLowerCase()),
    );
  const filtered = useMemo(
    () => rows.filter((r) => (!begin || r.date >= begin) && (!end || r.date <= end)),
    [rows, begin, end],
  );
  const depth = filtered.findLast((r) => r.snowDepth !== null),
    swe = filtered.findLast((r) => r.swe !== null);
  const daysMissing = filtered.filter((r) => r.snowDepth === null || r.swe === null).length;
  function period(days: number) {
    if (!status?.completed) return;
    const finish = status.completed.config.endDate;
    setEnd(finish);
    setBegin(
      new Date(
        Math.max(
          Date.parse(status.completed.config.beginDate),
          Date.parse(finish) - (days - 1) * 86400000,
        ),
      )
        .toISOString()
        .slice(0, 10),
    );
  }
  return (
    <>
      <header className="top">
        <a href="/" className="brand">
          ◈ MORIA <span>/ SNOW OBSERVATORY</span>
        </a>
        <div className="top-right">
          <span className="scope">READ-ONLY APPLICATION</span>
          {auth.session?.user && (
            <>
              <span>
                {auth.session.user.name}
                {auth.session.user.recovery ? ' · 15-minute recovery' : ''}
              </span>
              <button onClick={() => void auth.logout()}>Sign out</button>
            </>
          )}
          <button onClick={() => setReload((n) => n + 1)} disabled={!principal || busy || loading}>
            ↻ Reload from Moria
          </button>
        </div>
      </header>
      <section className="hero">
        <div>
          <div className="eyebrow">COLORADO / SNOW & WEATHER</div>
          <h1>Your Front Range conditions desk.</h1>
          <p>Mountain forecasts and measured snowpack, together for your next day outside.</p>
        </div>
        <div className="source-badge">
          <span className="dot" />
          {!principal
            ? 'Protected by Moria'
            : status?.completed
              ? 'Persisted in Moria'
              : 'Loading dataset status'}
          <small>
            {status?.completed
              ? `Last successful sync ${new Date(status.completed.completed_at).toLocaleString()}`
              : 'Authorized station and observation access'}
          </small>
        </div>
      </section>
      {auth.error && (
        <div className="banner error" role="alert">
          {auth.error}
        </div>
      )}
      {principal && error && (
        <div className="banner error" role="alert">
          {error}
        </div>
      )}
      {status?.latest && status.latest.status !== 'completed' && (
        <div className="banner" role="status">
          Latest sync: {status.latest.status}.{' '}
          {status.latest.error ?? 'The last completed dataset stays available while sync runs.'}
        </div>
      )}
      {principal && status && (
        <Weather
          weather={status.weather}
          stations={stations}
          observationEnd={status.completed?.config.endDate}
          onStation={(triplet) => {
            setSelected(triplet);
            setState('all');
            setSearch('');
            requestAnimationFrame(() => {
              const target = document.getElementById('station-browser');
              target?.scrollIntoView({ behavior: 'smooth', block: 'start' });
              target?.focus({ preventScroll: true });
            });
          }}
        />
      )}
      {!principal ? (
        <section className="empty auth-card" aria-label="Sign in">
          <h2>
            {!auth.session
              ? auth.error
                ? 'Unable to check your session.'
                : 'Checking your session…'
              : 'Welcome to Snow Observatory.'}
          </h2>
          {auth.session && (
            <p>
              Sign in to explore stations and snowpack observations with your Moria permissions.
            </p>
          )}
          {auth.session?.providers.map((provider) => (
            <button
              className="sign-in"
              key={provider.id}
              onClick={() => void auth.login(provider.id)}
            >
              Continue with {provider.label}
            </button>
          ))}
          {auth.session && !auth.session.providers.length && (
            <p>
              Google sign-in is awaiting instance setup. Run <code>pnpm moria auth setup</code> to
              connect it.
            </p>
          )}
          {auth.session && (
            <p className="recovery-note">
              Instance operator? Run <code>pnpm moria auth recover</code> for a temporary access
              link.
            </p>
          )}
          {!auth.session && auth.error && (
            <button onClick={() => location.reload()}>Try again</button>
          )}
        </section>
      ) : busy ? (
        <div className="empty" role="status">
          Loading authorized stations…
        </div>
      ) : error && !status ? (
        <div className="empty">Could not load authorized data. Use Reload from Moria to retry.</div>
      ) : !status?.completed ? (
        <div className="empty">
          <h2>Connect the first stations.</h2>
          <p>
            An instance operator can run <code>pnpm moria snotel sync</code>. This app has read
            access only; it cannot start ingestion or administer Moria.
          </p>
        </div>
      ) : (
        <div className="workspace" id="station-browser" tabIndex={-1}>
          <aside>
            <div className="aside-title">
              <h2>Stations</h2>
              <span>{stations.length}</span>
            </div>
            <label htmlFor="search">Find a station</label>
            <input
              id="search"
              type="search"
              placeholder="Name, county or triplet"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
            <label htmlFor="state">State</label>
            <select id="state" value={state} onChange={(e) => setState(e.target.value)}>
              <option value="all">All loaded states</option>
              {[...new Set(stations.map((s) => s.state))].map((s) => (
                <option key={s}>{s}</option>
              ))}
            </select>
            <div className="station-list">
              {visible.map((s) => (
                <button
                  key={s.uid}
                  className={`station ${selected === s.triplet ? 'selected' : ''}`}
                  onClick={() => {
                    setSelected(s.triplet);
                    setError('');
                  }}
                >
                  <strong>{s.name}</strong>
                  <span>
                    {s.state} · {s.elevationFt.toLocaleString()} ft
                  </span>
                  <small>{s.triplet}</small>
                </button>
              ))}
              {!visible.length && <p>No matching stations.</p>}
            </div>
            <div className="source-note">
              Source: USDA NRCS
              <br />
              SNWD · snow depth
              <br />
              WTEQ · snow water equivalent
              <br />
              <a
                href="https://www.nrcs.usda.gov/resources/data-and-reports/snow-and-climate-monitoring-predefined-reports-and-maps"
                target="_blank"
                rel="noreferrer"
              >
                Official reports & maps ↗
              </a>
            </div>
          </aside>
          <main>
            {station && (
              <>
                <div className="station-heading">
                  <div>
                    <div className="eyebrow">
                      {station.state} /{' '}
                      {station.county ? `${station.county.toUpperCase()} COUNTY` : 'NRCS STATION'}
                    </div>
                    <h2>{station.name}</h2>
                    <p>
                      {station.triplet} · {station.elevationFt.toLocaleString()} ft ·{' '}
                      {station.latitude.toFixed(4)}°, {station.longitude.toFixed(4)}°
                    </p>
                  </div>
                  <span className="station-tag">SNOTEL</span>
                </div>
                <section className="filters">
                  <div>
                    <label htmlFor="begin">From</label>
                    <input
                      id="begin"
                      type="date"
                      min={status?.completed?.config.beginDate}
                      max={end}
                      value={begin}
                      onChange={(e) => setBegin(e.target.value)}
                    />
                  </div>
                  <div>
                    <label htmlFor="end">Through</label>
                    <input
                      id="end"
                      type="date"
                      min={begin}
                      max={status?.completed?.config.endDate}
                      value={end}
                      onChange={(e) => setEnd(e.target.value)}
                    />
                  </div>
                  <div className="presets">
                    <button onClick={() => period(30)}>30 days</button>
                    <button onClick={() => period(90)}>90 days</button>
                    <button
                      onClick={() => {
                        setBegin(status!.completed!.config.beginDate);
                        setEnd(status!.completed!.config.endDate);
                      }}
                    >
                      Full period
                    </button>
                  </div>
                </section>
                <section className="metrics">
                  <article>
                    <label>SNOW DEPTH</label>
                    <strong>
                      {amount(depth?.snowDepth)} <small>in</small>
                    </strong>
                    <p>{depth ? `Latest reported ${date(depth.date)}` : 'No values in range'}</p>
                  </article>
                  <article>
                    <label>SNOW WATER EQUIVALENT</label>
                    <strong>
                      {amount(swe?.swe)} <small>in</small>
                    </strong>
                    <p>{swe ? `Latest reported ${date(swe.date)}` : 'No values in range'}</p>
                  </article>
                  <article>
                    <label>DAILY COVERAGE</label>
                    <strong>
                      {filtered.length - daysMissing}
                      <small> / {filtered.length}</small>
                    </strong>
                    <p>Days with both measurements</p>
                  </article>
                </section>
                {loading ? (
                  <div className="empty" role="status">
                    Loading authorized observations…
                  </div>
                ) : (
                  <>
                    <Chart rows={filtered} field="snowDepth" title="Snow depth" color="#347e9d" />
                    <Chart
                      rows={filtered}
                      field="swe"
                      title="Snow water equivalent"
                      color="#5e8062"
                    />
                    <details className="data-table">
                      <summary>
                        Daily observations and quality flags ({filtered.length} days)
                      </summary>
                      <div className="table-wrap">
                        <table>
                          <thead>
                            <tr>
                              <th>Date</th>
                              <th>Depth (in)</th>
                              <th>SWE (in)</th>
                              <th>Depth QC / QA</th>
                              <th>SWE QC / QA</th>
                            </tr>
                          </thead>
                          <tbody>
                            {[...filtered].reverse().map((r) => (
                              <tr key={r.uid}>
                                <td>{r.date}</td>
                                <td>{amount(r.snowDepth)}</td>
                                <td>{amount(r.swe)}</td>
                                <td>
                                  {r.depthQc || '—'} / {r.depthQa || '—'}
                                </td>
                                <td>
                                  {r.sweQc || '—'} / {r.sweQa || '—'}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </details>
                  </>
                )}
                <p className="footnote">
                  Daily values use NRCS period-end dates
                  {station.timeZoneOffset === null
                    ? ' as supplied by the report.'
                    : ` in the station’s data time zone (GMT${station.timeZoneOffset >= 0 ? '+' : ''}${station.timeZoneOffset}).`}{' '}
                  {status?.completed?.config.source === 'report-csv'
                    ? 'Source: NRCS Report Generator CSV. Quality flags and station time-zone offsets are not supplied by this export; values may be provisional.'
                    : 'Source: NRCS AWDB REST. Values marked suspect are excluded by the source query.'}{' '}
                  A zero is a reported zero; a dash is unavailable. Source observations may be
                  revised by NRCS.
                </p>
              </>
            )}
          </main>
        </div>
      )}
      <footer>
        SNOW OBSERVATORY{' '}
        <span>
          Official observations · scoped Moria queries · no instance-management privileges
        </span>
      </footer>
    </>
  );
}
createRoot(document.getElementById('root')!).render(<App />);
