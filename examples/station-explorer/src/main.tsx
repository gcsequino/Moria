import { useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { ApiError, createClient, type AuthState, type Station } from './generated/index.js';
import './style.css';

// The SDK sends same-origin cookies. No key, bearer token, app selector or CORS proxy.
const sdk = createClient();
function App() {
  const [auth, setAuth] = useState<AuthState | null>(null);
  const [stations, setStations] = useState<Station[]>([]);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [refresh, setRefresh] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    const signal = controller.signal;
    setLoading(true);
    setError('');
    void (async () => {
      try {
        const state = await sdk.auth.session({ signal });
        if (signal.aborted) return;
        setAuth(state);
        setStations([]);
        if (state.user) {
          const rows = await sdk.objects['snow-stations'].all(
            {
              projection: ['uid', 'triplet', 'name', 'state', 'elevationFt'],
              pageSize: 100,
            },
            { signal, maxRows: 2000 },
          );
          if (!signal.aborted) setStations(rows);
        }
      } catch (cause) {
        if (signal.aborted) return;
        setStations([]);
        if (cause instanceof ApiError && cause.status === 401) setAuth(null);
        setError(
          cause instanceof ApiError && cause.status === 404
            ? 'This preview serves the app only. Connect its same-origin Moria API and auth binding to load stations.'
            : cause instanceof Error
              ? cause.message
              : 'Unable to load stations.',
        );
      } finally {
        if (!signal.aborted) setLoading(false);
      }
    })();
    return () => controller.abort();
  }, [refresh]);
  async function login(provider: string) {
    try {
      window.location.assign((await sdk.auth.login(provider)).url);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Sign-in failed.');
    }
  }
  async function logout() {
    try {
      await sdk.auth.logout();
      setAuth(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Sign-out failed.');
    } finally {
      setStations([]);
    }
  }
  return (
    <main>
      <p className="eyebrow">MORIA CUSTOM APP STARTER</p>
      <h1>Station Explorer</h1>
      <p>A small React app using a generated, station-only API client.</p>
      <nav aria-label="Session">
        <button onClick={() => setRefresh((value) => value + 1)} disabled={loading}>
          Refresh
        </button>
        {auth?.user && (
          <>
            <span>{auth.user.name}</span>
            <button onClick={() => void logout()}>Sign out</button>
          </>
        )}
        {!auth?.user &&
          auth?.providers.map((provider) => (
            <button key={provider.id} onClick={() => void login(provider.id)}>
              Continue with {provider.label}
            </button>
          ))}
      </nav>
      {error && (
        <p role="alert" className="notice">
          {error}
        </p>
      )}
      {loading ? (
        <p role="status">Loading…</p>
      ) : !auth?.user ? (
        <p>Sign in through the configured provider to browse authorized stations.</p>
      ) : (
        <>
          <h2>{stations.length} stations</h2>
          {!stations.length ? (
            <p>No stations are currently visible to this account.</p>
          ) : (
            <ul>
              {stations.map((station) => (
                <li key={station.uid}>
                  <strong>{station.name}</strong>
                  <span>
                    {station.state} · {station.elevationFt.toLocaleString()} ft
                  </span>
                  <small>{station.triplet}</small>
                </li>
              ))}
            </ul>
          )}
        </>
      )}
      <footer>Access is enforced by Moria. This app has no write actions.</footer>
    </main>
  );
}
createRoot(document.getElementById('root')!).render(<App />);
