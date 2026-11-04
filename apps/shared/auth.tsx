import { useEffect, useState } from 'react';

import type { AuthState, Transport } from '../../packages/osdk/src/runtime.js';
/** Shared hosted-app session client. No provider SDK, tokens or credentials in React. */
export function useAppSession(client: Transport['auth']) {
  const [session, setSession] = useState<AuthState>(),
    [error, setError] = useState('');
  useEffect(() => {
    let live = true;
    async function load() {
      if (live) setError('');
      try {
        const fragment = location.hash.slice(1);
        const query = new URLSearchParams(location.search);
        if (fragment || query.has('auth')) history.replaceState(null, '', location.pathname);
        sessionStorage.removeItem('snow-key');
        sessionStorage.removeItem('moria-key');
        if (fragment.startsWith('recovery=')) await client.recover(fragment.slice(9));
        if (query.has('auth') && live)
          setError('Sign-in was not completed. Check that this account is invited and try again.');
        const next = await client.session();
        if (live) setSession(next);
      } catch (e) {
        if (live) setError(e instanceof Error ? e.message : 'Sign-in failed.');
      }
    }
    void load();
    const fragmentChanged = () => {
      void load();
    };
    window.addEventListener('hashchange', fragmentChanged);
    const expired = () => {
      setSession((s) => (s ? { ...s, user: null } : s));
      setError('Your session ended. Sign in again.');
    };
    window.addEventListener('moria:unauthenticated', expired);
    return () => {
      live = false;
      window.removeEventListener('hashchange', fragmentChanged);
      window.removeEventListener('moria:unauthenticated', expired);
    };
  }, [client]);
  useEffect(() => {
    if (!session?.user) return;
    const id = setTimeout(
      () => window.dispatchEvent(new Event('moria:unauthenticated')),
      Math.max(0, Date.parse(session.user.expiresAt) - Date.now()),
    );
    return () => clearTimeout(id);
  }, [session]);
  const refreshUrl = session?.refresh?.url,
    refreshOrigin = session?.refresh?.origin;
  const refreshActive = Boolean(session?.user || session?.refresh?.required);
  useEffect(() => {
    if (!refreshActive || !refreshUrl || !refreshOrigin) return;
    const endpoint = new URL(refreshUrl);
    if (
      endpoint.origin !== refreshOrigin ||
      endpoint.hostname !== location.hostname ||
      endpoint.protocol !== location.protocol
    )
      return;
    const frame = document.createElement('iframe');
    frame.hidden = true;
    frame.title = 'Session verification';
    let live = true;
    const reload = () => {
      frame.src = refreshUrl;
    };
    const message = async (event: MessageEvent) => {
      if (
        event.origin !== refreshOrigin ||
        event.source !== frame.contentWindow ||
        event.data?.type !== 'moria:session-refreshed'
      )
        return;
      try {
        const next = await client.session();
        if (live) {
          setSession(next);
          if (next.user) setError('');
          else if (!event.data.ok) setError('Your identity could not be refreshed. Sign in again.');
        }
      } catch {
        if (live) setError('Your session could not be verified.');
      }
    };
    window.addEventListener('message', message);
    document.body.append(frame);
    reload();
    const interval = setInterval(reload, 20000);
    const visible = () => {
      if (document.visibilityState === 'visible') reload();
    };
    document.addEventListener('visibilitychange', visible);
    return () => {
      live = false;
      clearInterval(interval);
      frame.remove();
      window.removeEventListener('message', message);
      document.removeEventListener('visibilitychange', visible);
    };
  }, [client, refreshUrl, refreshOrigin, refreshActive]);
  async function login(provider: string) {
    setError('');
    try {
      const next = await client.login(provider);
      location.assign(next.url);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Sign-in failed.');
    }
  }
  async function logout() {
    try {
      await client.logout();
      setSession((s) => (s ? { ...s, user: null } : s));
    } catch {
      setError('Could not sign out. Please try again.');
    }
  }
  return { session, error, login, logout };
}
