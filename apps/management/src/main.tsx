import React, { useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { parseCSV, sdk, sample, type Asset, type HostedApp, type Page } from './client.js';
import './style.css';
import { Platform } from './platform.js';
import { Lineage } from './lineage.js';
import { Filesystem } from './filesystem.js';
import { useAppSession } from '../../shared/auth.js';

function App() {
  const auth = useAppSession(sdk.auth);
  const principal = auth.session?.user?.principal;
  const [authorizedPrincipal, setAuthorizedPrincipal] = useState<string>();
  const administrator = !!principal && authorizedPrincipal === principal;
  const currentPrincipal = useRef(principal);
  currentPrincipal.current = principal;
  const [view, setView] = useState<
    'overview' | 'assets' | 'apps' | 'executions' | 'lineage' | 'files'
  >('overview');
  const [busy, setBusy] = useState(false),
    gate = useRef(false);
  const [notice, setNotice] = useState('Connecting to your workspace…'),
    [error, setError] = useState(false);
  const [source, setSource] = useState(JSON.stringify(sample, null, 2));
  const [page, setPage] = useState<Page>({ revision: '0', objects: [] }),
    [pageNumber, setPageNumber] = useState(1);
  const [ready, setReady] = useState(false),
    [selected, setSelected] = useState<Asset>();
  const [field, setField] = useState<'title' | 'status' | 'notes'>('status'),
    [value, setValue] = useState('');
  const [apps, setApps] = useState<HostedApp[]>([]);
  const pending = useRef<{ invoke: () => Promise<unknown> } | undefined>(undefined);
  const say = (text: string) => {
    setNotice(text);
    setError(false);
  };
  async function run(work: () => Promise<void>) {
    if (gate.current) return;
    gate.current = true;
    setBusy(true);
    try {
      await work();
    } catch (e) {
      if (e instanceof Error && e.name === 'AbortError') return;
      if (e && typeof e === 'object' && 'status' in e && (e.status === 401 || e.status === 403)) {
        setAuthorizedPrincipal(undefined);
        setSelected(undefined);
        setApps([]);
        setPage({ revision: '0', objects: [] });
      }
      setNotice(e instanceof Error ? e.message : 'Operation failed');
      setError(true);
    } finally {
      gate.current = false;
      setBusy(false);
    }
  }
  async function load(cursor?: string, signal?: AbortSignal) {
    const requestedPrincipal = principal;
    setSelected(undefined);
    pending.current = undefined;
    const status = await sdk.endpoints.workspace({ signal });
    if (signal?.aborted || currentPrincipal.current !== requestedPrincipal) return;
    setReady(status.ready);
    setAuthorizedPrincipal(status.administrator ? requestedPrincipal : undefined);
    if (!status.ready) {
      setPage({ revision: '0', objects: [] });
      setPageNumber(1);
      return;
    }
    const result = await sdk.objects.assets.query(
      {
        pageSize: 25,
        ...(cursor ? { cursor } : {}),
      },
      { signal },
    );
    if (signal?.aborted || currentPrincipal.current !== requestedPrincipal) return;
    setPage(result);
    setPageNumber((n) => (cursor ? n + 1 : 1));
  }
  useEffect(() => {
    if (!principal) {
      setAuthorizedPrincipal(undefined);
      setSelected(undefined);
      setApps([]);
      setPage({ revision: '0', objects: [] });
      setReady(false);
      return;
    }
    const controller = new AbortController();
    void run(async () => {
      await load(undefined, controller.signal);
      if (!controller.signal.aborted)
        say('Workspace ready. Your data and edits persist across refreshes.');
    });
    return () => controller.abort();
  }, [principal]);
  async function showApps() {
    const requestedPrincipal = principal;
    setView('apps');
    const result = await sdk.endpoints.apps();
    if (currentPrincipal.current !== requestedPrincipal) return;
    setApps(result);
    say('Hosted application versions. CLI recovery works independently of this console.');
  }
  function open(asset: Asset) {
    setSelected(asset);
    setValue(asset[field]);
    pending.current = undefined;
  }
  async function save() {
    if (!selected) return;
    if (!pending.current) {
      const settings = { idempotencyKey: crypto.randomUUID() };
      const uid = selected.uid;
      const invoke =
        field === 'title'
          ? () => sdk.actions['edit-title']({ uid, title: value }, settings)
          : field === 'status'
            ? () => sdk.actions['edit-status']({ uid, status: value }, settings)
            : () => sdk.actions['edit-notes']({ uid, notes: value }, settings);
      pending.current = { invoke };
    }
    await pending.current.invoke();
    pending.current = undefined;
    await load();
    say(`Saved ${field} override. It will survive the next source refresh.`);
  }
  const restricted = !administrator;
  if (!principal)
    return (
      <main className="auth-page">
        <section className="panel">
          <div className="eyebrow">MORIA / INSTANCE MANAGEMENT</div>
          <h1>Welcome to your workspace.</h1>
          <p>
            Sign in to open your files. Managing the instance requires an explicit administrator
            grant.
          </p>
          {auth.error && (
            <p role="alert" className="error">
              {auth.error}
            </p>
          )}
          {!auth.session && <p>Checking your session...</p>}
          {auth.session?.providers.map((provider) => (
            <button
              key={provider.id}
              className="primary"
              onClick={() => void auth.login(provider.id)}
            >
              Continue with {provider.label}
            </button>
          ))}
          {auth.session && !auth.session.providers.length && (
            <p>No administrator sign-in provider is configured.</p>
          )}
          <p>
            Local operator recovery: <code>pnpm moria auth recover --app management</code>
          </p>
        </section>
      </main>
    );

  return (
    <>
      <aside>
        <a className="brand" href="/">
          ◈ <span>MORIA</span>
        </a>
        <div className="workspace-label">INSTANCE MANAGEMENT</div>
        <nav aria-label="Instance navigation">
          {(
            [
              ['overview', 'Overview'],
              ['assets', 'Assets'],
              ['apps', 'Applications'],
              ['executions', 'Executions'],
              ['lineage', 'Lineage'],
              ['files', 'Files'],
            ] as const
          ).map(([key, label]) => (
            <button
              key={key}
              className={`nav ${view === key ? 'active' : ''}`}
              aria-current={view === key ? 'page' : undefined}
              disabled={busy || (!administrator && key !== 'files')}
              onClick={() =>
                void run(async () => {
                  setView(key);
                  if (key === 'assets') await load();
                  if (key === 'apps') await showApps();
                })
              }
            >
              {label}
            </button>
          ))}
        </nav>
        <div className="aside-bottom">
          <span className="dot" />
          PostgreSQL + Iceberg
          <br />
          <small>Bundled React application</small>
        </div>
      </aside>
      <main>
        <header>
          <div>
            <div className="eyebrow">INSTANCE / {view.toUpperCase()}</div>
            <h1>
              {
                {
                  overview: 'Your instance, at a glance.',
                  assets: 'Your data, with memory.',
                  apps: 'A home for your applications.',
                  executions: 'Keep your data moving.',
                  lineage: 'Trace your data and workflows.',
                  files: 'Your platform, organized.',
                }[view]
              }
            </h1>
            <p>
              {
                {
                  overview: 'A shared view of your data, applications and ongoing work.',
                  assets:
                    'Import a source. Explore what you can access. Keep your edits through every refresh.',
                  apps: 'Immutable builds, explicit promotion, and recovery from the command line.',
                  executions:
                    'Schedule refreshes, follow progress and inspect the data behind each run.',
                  lineage:
                    'Explore recorded relationships between sources, datasets and executions.',
                  files:
                    'Manage source, configuration and private workspaces with inherited access.',
                }[view]
              }
            </p>
          </div>
          <div className="identity">
            <strong>{auth.session?.user?.name}</strong>
            <small>
              {auth.session?.user?.recovery
                ? 'Temporary CLI recovery'
                : administrator
                  ? 'Instance administrator'
                  : 'Checking permissions'}
            </small>
            <button className="quiet" onClick={() => void auth.logout()}>
              Sign out
            </button>
          </div>
        </header>
        <div id="notice" className={error ? 'error' : ''} role="status" aria-live="polite">
          {notice}
        </div>
        {view === 'files' ? (
          <Filesystem key={principal} />
        ) : !administrator ? (
          <section className="panel apps-panel">
            <h2>Administrator access required</h2>
            <p>
              This view requires an explicit instance administrator grant. Open Files to browse your
              authorized workspace. Signing in does not grant access to other data.
            </p>
          </section>
        ) : view === 'lineage' ? (
          <Lineage key={principal} />
        ) : view === 'overview' || view === 'executions' ? (
          <Platform
            key={principal}
            view={view}
            principal={principal}
            ready={ready}
            revision={page.revision}
            onNavigate={(next) =>
              void run(async () => {
                setView(next);
                if (next === 'apps') await showApps();
                if (next === 'assets') await load();
              })
            }
          />
        ) : view === 'apps' ? (
          <>
            <section className="panel apps-panel">
              <div className="panel-head">
                <div className="eyebrow">APPLICATION HOST</div>
                <h2>Installed applications</h2>
                <p>
                  Builds are stored by content hash. Promotion changes the entry point; existing
                  version URLs remain available.
                </p>
              </div>
              {apps.map((app) => (
                <article className="deployment" key={app.app}>
                  <div>
                    <h3>{app.app}</h3>
                    <span className="badge">
                      {app.error ? 'Needs recovery' : app.deployment ? 'Published' : 'Not promoted'}
                    </span>
                  </div>
                  <p>{app.error ?? `Promoted ${app.deployment?.promotedAt}`}</p>
                  <label>Active version</label>
                  <code className="version">{app.deployment?.current ?? 'Not promoted'}</code>
                  <p>{app.versions.length} retained version(s)</p>
                  <label>CLI recovery</label>
                  <pre>
                    {app.app === 'management'
                      ? 'pnpm moria apps recover'
                      : `pnpm moria apps rollback ${app.app}`}
                  </pre>
                </article>
              ))}
              <button className="quiet" disabled={busy} onClick={() => void run(showApps)}>
                Refresh applications
              </button>
            </section>
          </>
        ) : (
          <>
            <section className="steps">
              <div>
                <b>01</b>
                <span>
                  Import a dataset<small>CSV or JSON → Iceberg snapshot</small>
                </span>
              </div>
              <div>
                <b>02</b>
                <span>
                  Browse & edit<small>Access checked on the server</small>
                </span>
              </div>
              <div>
                <b>03</b>
                <span>
                  Refresh with confidence<small>Saved overrides stay attached to IDs</small>
                </span>
              </div>
            </section>
            <div className="layout">
              <section className="panel source">
                <div className="panel-head">
                  <div className="eyebrow">SOURCE DATASET</div>
                  <h2>Bring in your assets</h2>
                  <p>
                    Each import replaces the source. Keep the same <code>id</code> to preserve an
                    asset’s edits.
                  </p>
                </div>
                <label className="file-label" htmlFor="file">
                  Choose CSV or JSON file
                  <input
                    id="file"
                    type="file"
                    disabled={busy}
                    accept=".csv,.json,text/csv,application/json"
                    onChange={(e) => {
                      const file = e.target.files?.[0];
                      void run(async () => {
                        if (!file) return;
                        if (file.size > 65536)
                          throw new Error('Choose a file smaller than 64 KiB.');
                        const text = (await file.text()).replace(/^\uFEFF/, '');
                        setSource(
                          JSON.stringify(
                            file.name.toLowerCase().endsWith('.csv')
                              ? parseCSV(text)
                              : JSON.parse(text),
                            null,
                            2,
                          ),
                        );
                        say('File loaded. Review it, then import to replace the source.');
                      });
                    }}
                  />
                </label>
                <label htmlFor="data">Preview & edit source JSON</label>
                <textarea
                  id="data"
                  aria-label="Source dataset JSON"
                  spellCheck={false}
                  value={source}
                  disabled={busy}
                  onChange={(e) => setSource(e.target.value)}
                />
                <div className="sample-buttons">
                  <button
                    className="quiet"
                    disabled={busy}
                    onClick={() => {
                      setSource(JSON.stringify(sample, null, 2));
                      say('Sample loaded. Import it to publish.');
                    }}
                  >
                    Load sample
                  </button>
                  <button
                    className="quiet"
                    disabled={busy}
                    onClick={() => {
                      setSource(
                        JSON.stringify(
                          sample.map((row) => ({
                            ...row,
                            title: row.title + ' · refreshed',
                            status: 'Source updated',
                            notes: 'New source snapshot.',
                          })),
                          null,
                          2,
                        ),
                      );
                      say('Refreshed sample loaded. Saved overrides will remain.');
                    }}
                  >
                    Load refreshed sample
                  </button>
                </div>
                <button
                  className="primary"
                  disabled={busy || !administrator}
                  onClick={() =>
                    void run(async () => {
                      const data = JSON.parse(source);
                      say('Writing Iceberg snapshot and indexing assets…');
                      const result = await sdk.endpoints.importAssets(data);
                      await load();
                      say(
                        `Imported ${result.rows} rows at revision ${result.revision}; existing overrides retained.`,
                      );
                    })
                  }
                >
                  Import / refresh dataset
                </button>
                <p className="hint">
                  Required fields: id, title, region, status, notes. Region: West or East. Maximum
                  1,000 rows / 64 KiB request. Missing IDs disappear from browse; retained edits
                  return when those IDs reappear.
                </p>
              </section>
              <section className="panel explorer">
                <div className="panel-head explorer-head">
                  <div>
                    <div className="eyebrow">AUTHORIZED OBJECTS</div>
                    <h2>
                      Asset explorer{' '}
                      <span className="badge">
                        {page.objects.length}
                        {page.nextCursor ? '+' : ''}
                      </span>
                    </h2>
                  </div>
                  <button
                    className="quiet"
                    disabled={busy}
                    onClick={() =>
                      void run(async () => {
                        await load();
                        say('Loaded current authorized objects.');
                      })
                    }
                  >
                    ↻ Reload
                  </button>
                </div>
                <div className="access">
                  <span className="dot" />
                  <span>
                    {administrator
                      ? 'All regions: authorized read, edit and import'
                      : 'Current permissions checked on every request'}
                  </span>
                  <span id="revision">Revision {page.revision}</span>
                </div>
                <div className="table-scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>Asset</th>
                        <th>Region</th>
                        <th>Status</th>
                        <th />
                      </tr>
                    </thead>
                    <tbody>
                      {page.objects.map((row) => (
                        <tr key={row.uid}>
                          <td>
                            <strong>{row.title}</strong>
                            <small>{row.id}</small>
                          </td>
                          <td>{row.region}</td>
                          <td>
                            <span className="status">{row.status}</span>
                          </td>
                          <td>
                            <button className="quiet" disabled={busy} onClick={() => open(row)}>
                              {restricted ? 'View' : 'Edit'}
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                {!page.objects.length && (
                  <div className="empty">
                    <div>◇</div>
                    <h3>{ready ? 'No visible assets.' : 'A fresh start.'}</h3>
                    <p>
                      {ready
                        ? 'This identity has no matching objects.'
                        : 'Import the sample to see your first assets.'}
                    </p>
                  </div>
                )}
                <div className="pagination">
                  <span>Page {pageNumber} · up to 25 objects per page</span>
                  <button
                    className="quiet"
                    disabled={busy || !page.nextCursor}
                    onClick={() => void run(() => load(page.nextCursor))}
                  >
                    Next page →
                  </button>
                </div>
                {selected && (
                  <section id="editor">
                    <div className="eyebrow">{restricted ? 'VIEW' : 'EDIT'} OBJECT</div>
                    <h2>{selected.title}</h2>
                    <p className="hint">
                      {selected.id} · {selected.uid}
                    </p>
                    <form
                      onSubmit={(e) => {
                        e.preventDefault();
                        void run(save);
                      }}
                    >
                      <label htmlFor="field">Property to override</label>
                      <select
                        id="field"
                        value={field}
                        disabled={busy}
                        onChange={(e) => {
                          const next = e.target.value as typeof field;
                          setField(next);
                          setValue(selected[next]);
                          pending.current = undefined;
                        }}
                      >
                        <option value="status">Status</option>
                        <option value="title">Title</option>
                        <option value="notes">Notes</option>
                      </select>
                      <label htmlFor="value">Your value</label>
                      <textarea
                        id="value"
                        value={value}
                        maxLength={2000}
                        disabled={busy || restricted}
                        onChange={(e) => {
                          setValue(e.target.value);
                          pending.current = undefined;
                        }}
                      />
                      <div className="edit-buttons">
                        <button className="primary" disabled={busy || restricted}>
                          Save override
                        </button>
                        <button
                          type="button"
                          className="quiet"
                          disabled={busy || restricted}
                          onClick={() =>
                            void run(async () => {
                              await sdk.actions['reset-asset'](
                                { uid: selected.uid },
                                { idempotencyKey: crypto.randomUUID() },
                              );
                              await load();
                              say('Overrides cleared. This asset now follows source values.');
                            })
                          }
                        >
                          Use all source values
                        </button>
                        <button
                          type="button"
                          className="quiet"
                          disabled={busy}
                          onClick={() => setSelected(undefined)}
                        >
                          Close
                        </button>
                      </div>
                      <p className="hint">
                        Only the selected property is overridden. Other properties follow source
                        refreshes. “Use all source values” clears title, status and notes overrides.
                      </p>
                    </form>
                  </section>
                )}
              </section>
            </div>
          </>
        )}
        <footer>
          MORIA / BUNDLED MANAGEMENT APP{' '}
          <span>Scoped API · persistent data · recoverable applications</span>
        </footer>
      </main>
    </>
  );
}
createRoot(document.getElementById('root')!).render(<App />);
