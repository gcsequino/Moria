import React, { useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { useAppSession } from '../../shared/auth.js';
import { platform } from './native-client.js';
import { Filesystem } from './filesystem.js';
import { Resources } from './resources.js';
import './style.css';
function Runtime() {
  const [value, setValue] = useState<unknown>(),
    [error, setError] = useState(''),
    [version, setVersion] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    void platform.transport
      .request('/api/management/v1/runtime', 'GET', undefined, { signal: controller.signal })
      .then((result) => {
        setValue(result);
        setError('');
      })
      .catch((error) => {
        if (!controller.signal.aborted) {
          setValue(undefined);
          setError(error instanceof Error ? error.message : 'Unable to inspect runtime');
        }
      });
    return () => controller.abort();
  }, [version]);
  return (
    <section className="panel apps-panel" aria-label="Native runtime">
      <h2>Runtime status</h2>
      <p>
        Current request budgets and drain status. Inspection requires instance management
        permission.
      </p>
      <button onClick={() => setVersion((version) => version + 1)}>Refresh runtime</button>
      {error && (
        <p role="alert" className="error">
          {error}
        </p>
      )}
      {value !== undefined && <pre>{JSON.stringify(value, null, 2)}</pre>}
    </section>
  );
}
function App() {
  const auth = useAppSession(platform.auth),
    principal = auth.session?.user?.principal;
  const [view, setView] = useState<'files' | 'resources' | 'runtime'>('files');
  if (!principal)
    return (
      <main className="auth-page">
        <section className="panel">
          <div className="eyebrow">MORIA / INSTANCE MANAGEMENT</div>
          <h1>Welcome to your workspace.</h1>
          <p>Sign in to open your files and published resources.</p>
          {auth.error && (
            <p role="alert" className="error">
              {auth.error}
            </p>
          )}
          {!auth.session && <p>Checking your session…</p>}
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
            <p>No sign-in provider is configured. Use local operator recovery to sign in.</p>
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
              ['files', 'Files and sources'],
              ['resources', 'Published resources'],
              ['runtime', 'Runtime status'],
            ] as const
          ).map(([id, label]) => (
            <button
              key={id}
              className={`nav ${view === id ? 'active' : ''}`}
              aria-current={view === id ? 'page' : undefined}
              onClick={() => setView(id)}
            >
              {label}
            </button>
          ))}
        </nav>
        <div className="aside-bottom">
          <span className="dot" />
          Native runtime
          <br />
          <small>PostgreSQL + Iceberg</small>
        </div>
      </aside>
      <main>
        <header>
          <div>
            <div className="eyebrow">INSTANCE / {view.toUpperCase()}</div>
            <h1>
              {
                {
                  files: 'Your platform, organized.',
                  resources: 'Work with published resources.',
                  runtime: 'Inspect your instance.',
                }[view]
              }
            </h1>
            <p>
              Every operation uses your current permissions and the published resource contract.
            </p>
          </div>
          <div className="identity">
            <strong>{auth.session!.user!.name}</strong>
            <small>{principal}</small>
            <button className="quiet" onClick={() => void auth.logout()}>
              Sign out
            </button>
          </div>
        </header>
        {auth.error && (
          <p role="alert" className="error">
            {auth.error}
          </p>
        )}
        {view === 'files' ? (
          <Filesystem key={principal} />
        ) : view === 'resources' ? (
          <Resources key={principal} />
        ) : (
          <Runtime key={principal} />
        )}
      </main>
    </>
  );
}
createRoot(document.getElementById('root')!).render(<App />);
