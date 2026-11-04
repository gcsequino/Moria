import assert from 'node:assert/strict';
import { runInNewContext } from 'node:vm';
import { setImmediate } from 'node:timers/promises';

/** Exercise the exact external script returned by native HTTP with browser navigation events. */
export async function assertBootstrapRetry(script: string) {
  const state = { textContent: '' };
  const providers = { replaceChildren() {}, append() {} };
  const listeners = new Map<string, () => void>();
  const calls: string[] = [];
  const redirects: string[] = [];
  const location = {
    hash: '#recovery=' + 'a'.repeat(43),
    pathname: '/api/auth/bootstrap',
    origin: 'http://127.0.0.1',
    replace: (path: string) => redirects.push(path),
    assign() {},
  };
  let active = 0,
    maximumActive = 0,
    release: (() => void) | undefined;
  const scope = {
    location,
    history: {
      replaceState() {
        location.hash = '';
      },
    },
    document: { querySelector: (selector: string) => (selector === '#state' ? state : providers) },
    addEventListener: (event: string, listener: () => void) => listeners.set(event, listener),
    URL,
    fetch: async (path: string, request: { body?: string }) => {
      assert.equal(
        location.hash,
        '',
        'credentials must leave the URL before any network operation',
      );
      active++;
      maximumActive = Math.max(maximumActive, active);
      try {
        if (path.endsWith('/recover')) {
          const ticket = JSON.parse(request.body!).ticket as string;
          calls.push(ticket[0]);
          if (ticket[0] === 'b')
            await new Promise<void>((resolve) => {
              release = resolve;
            });
          return { ok: ticket[0] !== 'a', json: async () => ({}) };
        }
        return { ok: true, json: async () => ({ user: { name: 'fixture' }, providers: [] }) };
      } finally {
        active--;
      }
    },
  };
  runInNewContext(script, scope, { timeout: 1000 });
  for (let i = 0; i < 10 && !state.textContent.includes('unavailable'); i++) await setImmediate();
  assert.match(state.textContent, /unavailable/);
  location.hash = '#recovery=' + 'b'.repeat(43);
  listeners.get('hashchange')!();
  assert.equal(location.hash, '');
  assert(release, 'fresh hash retries the failed bootstrap without reloading');
  location.hash = '#recovery=' + 'c'.repeat(43);
  listeners.get('hashchange')!();
  assert.equal(location.hash, '');
  release();
  for (let i = 0; i < 10 && redirects.length === 0; i++) await setImmediate();
  assert.deepEqual(calls, ['a', 'b', 'c']);
  assert.deepEqual(redirects, ['/'], 'superseded recovery cannot redirect over the fresh attempt');
  assert.equal(maximumActive, 1, 'recovery effects are serialized');
}
