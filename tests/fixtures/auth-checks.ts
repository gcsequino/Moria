import assert from 'node:assert/strict';
import { randomBytes, randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { Catalog } from '../../services/catalog/src/catalog.js';
import { Authority } from '../../services/security/src/authority.js';
import { BrowserSessions, digest } from '../../services/security/src/browser-sessions.js';
import { AppAuth } from '../../services/app-host/src/auth.js';
import { authorize } from '../../packages/authorization/src/access.js';
import { oidcFixture } from './oidc.js';

export async function checkAuth(catalog: Catalog) {
  const issuer = await oidcFixture(),
    environment = `auth-${randomUUID()}`,
    authority = new Authority(catalog);
  await catalog.ensureEnvironment(environment);
  await authority.migrate();
  await authority.setApplication(environment, 'test', 'snow', ['stations:read']);
  await authority.syncPrincipal(environment, 'test', 'recovery', {
    enabled: true,
    kind: 'workload',
    attributes: {},
    markings: [],
  });
  await authority.setGrants(environment, 'test', 'recovery', 'snow', ['stations:read']);
  const binding = {
    environment,
    application: 'snow',
    origin: 'http://127.0.0.1:3211',
    entryPath: '/snow/',
    initialOperations: ['stations:read'],
    recoveryPrincipal: 'recovery',
  };
  const providers = [
    {
      id: 'test',
      label: 'Test',
      issuer: issuer.issuer,
      clientId: 'test-client',
      clientSecret: 'test-secret',
      emails: ['snow@example.test'],
    },
  ];
  const key = randomBytes(32),
    sessions = new BrowserSessions(authority, binding, providers, key, true);
  await sessions.initialize();
  async function begin() {
    const flow = await sessions.start('test');
    const response = await fetch(flow.location, { redirect: 'manual' });
    return { flow, callback: new URL(response.headers.get('location')!) };
  }
  async function login() {
    const { flow, callback } = await begin();
    return sessions.finish(callback, flow.browser);
  }
  const authenticate = (token: string) => sessions.authenticate(`Bearer ${token}`);
  let http: ReturnType<typeof createServer> | undefined;
  try {
    const initial = await begin();
    await assert.rejects(sessions.finish(initial.callback, 'wrong-browser'), /Sign in again/);
    const wrongState = new URL(initial.callback);
    wrongState.searchParams.set('state', 'wrong');
    await assert.rejects(sessions.finish(wrongState, initial.flow.browser), /Sign in again/);
    const token = await sessions.finish(initial.callback, initial.flow.browser);
    await assert.rejects(sessions.finish(initial.callback, initial.flow.browser), /Sign in again/);
    const user = await authenticate(token);
    assert.notEqual(user.principal, 'recovery');
    const snapshot = await authority.current(environment, user.principal, 'snow');
    authorize(snapshot.identity, 'stations', 'read');
    assert.throws(() => authorize(snapshot.identity, 'stations', 'write'));
    const stored = (
      await catalog.pool.query(
        'SELECT access_token FROM moria.browser_sessions WHERE token_hash=$1',
        [user.sessionHash],
      )
    ).rows[0].access_token;
    assert.ok(!stored.includes('test-access-token'));
    const restarted = new BrowserSessions(authority, binding, providers, key, true);
    await restarted.initialize();
    assert.equal((await restarted.authenticate(`Bearer ${token}`)).principal, user.principal);
    for (const mode of ['nonce', 'issuer', 'audience', 'signature', 'expired', 'unverified']) {
      issuer.state.mode = mode;
      await assert.rejects(login(), /could not verify/);
    }
    issuer.state.mode = '';
    const badPkce = await begin();
    await catalog.pool.query('UPDATE moria.auth_flows SET verifier=$1 WHERE state_hash=$2', [
      'wrong',
      digest(badPkce.callback.searchParams.get('state')!),
    ]);
    await assert.rejects(
      sessions.finish(badPkce.callback, badPkce.flow.browser),
      /could not verify/,
    );
    const expiredFlow = await begin();
    await catalog.pool.query(
      "UPDATE moria.auth_flows SET expires_at=clock_timestamp()-interval '1 second' WHERE state_hash=$1",
      [digest(expiredFlow.callback.searchParams.get('state')!)],
    );
    await assert.rejects(sessions.finish(expiredFlow.callback, expiredFlow.flow.browser));
    issuer.state.email = 'stranger@example.test';
    await assert.rejects(login(), /not been invited/);
    issuer.state.email = 'snow@example.test';
    issuer.state.subject = 'different-subject';
    await assert.rejects(login(), /already bound/);
    issuer.state.subject = 'subject-1';
    const second = await login();
    assert.equal((await authenticate(second)).principal, user.principal);
    await catalog.pool.query(
      "UPDATE moria.browser_sessions SET verified_at=clock_timestamp()-interval '31 seconds' WHERE token_hash=$1",
      [user.sessionHash],
    );
    const before = issuer.state.userInfoCalls;
    await authenticate(token);
    assert.equal(issuer.state.userInfoCalls, before + 1);
    await catalog.pool.query(
      "UPDATE moria.browser_sessions SET verified_at=clock_timestamp()-interval '61 seconds' WHERE token_hash=$1",
      [user.sessionHash],
    );
    issuer.state.mode = 'unavailable';
    await assert.rejects(authenticate(token), /refresh failed/);
    const currentEpoch = (await authority.current(environment, user.principal, 'snow')).epoch;
    await assert.rejects(
      catalog.transaction(
        (c) =>
          authority.assertCurrent(
            c,
            environment,
            user.principal,
            'snow',
            currentEpoch,
            user.sessionHash,
          ),
        environment,
      ),
      /expired or was revoked/,
    );
    issuer.state.mode = '';
    await authenticate(token);
    await authority.setGrants(environment, 'operator', user.principal, 'snow', []);
    await login(); // Returning login must not restore grants.
    assert.equal(
      (await authority.current(environment, user.principal, 'snow')).identity.grantedOperations
        .size,
      0,
    );
    await authority.syncPrincipal(environment, 'operator', user.principal, {
      enabled: false,
      kind: 'human',
      attributes: {},
      markings: [],
    });
    await assert.rejects(authenticate(token));
    await assert.rejects(login(), /disabled/);
    await authority.syncPrincipal(environment, 'operator', user.principal, {
      enabled: true,
      kind: 'human',
      attributes: {},
      markings: [],
    });
    await sessions.logout(token);
    await assert.rejects(authenticate(token));
    await catalog.pool.query(
      "UPDATE moria.browser_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token_hash=$1",
      [digest(second)],
    );
    await assert.rejects(authenticate(second));
    const active = await login();
    const other = new BrowserSessions(
      authority,
      { ...binding, application: 'different' },
      providers,
      key,
      true,
    );
    await assert.rejects(other.authenticate(`Bearer ${active}`));
    const disabled = new BrowserSessions(authority, binding, [], key, true);
    await disabled.initialize();
    await assert.rejects(authenticate(active));
    await assert.rejects(sessions.start('test'));
    await sessions.initialize();
    await assert.rejects(authenticate(active)); // Re-enabling never revives old sessions.
    // Recovery and HTTP cookie/CSRF contract work independently of provider availability.
    issuer.state.mode = 'unavailable';
    const ticket = await sessions.recoveryTicket(),
      recovered = await sessions.recover(ticket);
    await assert.rejects(sessions.recover(ticket));
    assert.equal((await authenticate(recovered)).principal, 'recovery');
    const auth = new AppAuth(sessions, 'local-key');
    http = createServer(async (req, res) => {
      try {
        if (!(await auth.handle(req, res))) {
          res.writeHead(404);
          res.end();
        }
      } catch (e) {
        res.writeHead((e as { code: string }).code === 'FORBIDDEN' ? 403 : 401);
        res.end();
      }
    });
    await new Promise<void>((r) => http!.listen(0, '127.0.0.1', r));
    binding.origin = `http://127.0.0.1:${(http.address() as { port: number }).port}`;
    const request = (path: string, input?: unknown, cookie = '', origin = binding.origin) =>
      fetch(`${binding.origin}/api/auth/${path}`, {
        method: input === undefined ? 'GET' : 'POST',
        headers: { origin, cookie, 'content-type': 'application/json' },
        ...(input === undefined ? {} : { body: JSON.stringify(input) }),
        redirect: 'manual',
      });
    assert.equal((await request('session')).status, 200);
    assert.equal((await request('logout', {}, '', 'http://evil.test')).status, 403);
    assert.equal((await request('login', { provider: 'test' }, '', '')).status, 403);
    const recovery = await request('recover', { ticket: await sessions.recoveryTicket() });
    const setCookie = recovery.headers.get('set-cookie')!;
    assert.match(setCookie, /HttpOnly/);
    assert.match(setCookie, /SameSite=Lax/);
    assert.match(setCookie, /Path=\/api\//);
    const cookie = setCookie.split(';')[0];
    assert.equal((await (await request('session', undefined, cookie)).json()).user.recovery, true);
    assert.equal((await request('logout', {}, cookie)).status, 200);
    assert.equal((await (await request('session', undefined, cookie)).json()).user, null);
    assert.equal((await request('recovery-ticket', {})).status, 401);
  } finally {
    if (http) await new Promise<void>((r) => http!.close(() => r()));
    await issuer.close();
  }
}
