import assert from 'node:assert/strict';
import test from 'node:test';
import { randomBytes, randomUUID, createHash } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { createServer as createNetServer } from 'node:net';
import { database } from './fixtures/catalog-storage/database.js';
import { checkAuth } from './fixtures/auth-checks.js';
import { oidcFixture } from './fixtures/oidc.js';
import { Authority } from '../services/security/src/authority.js';
import { BrowserSessions, digest } from '../services/security/src/browser-sessions.js';
import { AppAuth } from '../services/app-host/src/auth.js';
import { browserProfile, createHttpServer, sendJson } from '../services/http/src/server.js';
async function port() {
  const server = createNetServer();
  await new Promise<void>((done) => server.listen(0, '127.0.0.1', done));
  const result = (server.address() as { port: number }).port;
  await new Promise<void>((done) => server.close(() => done()));
  return result;
}
test(
  'bounded OIDC refresh preserves existing session/provider/CSRF semantics',
  { timeout: 90000 },
  async () => {
    const db = await database();
    try {
      await checkAuth(db.catalog);
    } finally {
      await db.close();
    }
  },
);
test(
  'split OIDC callback and accepted-human refresh use actual IdP evidence without renewing ordinary expiry',
  { timeout: 90000 },
  async (t) => {
    const db = await database(),
      issuer = await oidcFixture(),
      authority = new Authority(db.catalog),
      environment = randomUUID(),
      application = 'refresh-app';
    const nativeOrigin = `http://127.0.0.1:${await port()}`,
      authPort = await port(),
      authenticationOrigin = `http://127.0.0.1:${authPort}`;
    await db.catalog.ensureEnvironment(environment);
    await authority.setApplication(environment, 'fixture', application, ['records:read']);
    const sessions = new BrowserSessions(
      authority,
      {
        environment,
        application,
        origin: nativeOrigin,
        authenticationOrigin,
        entryPath: '/app/',
        initialOperations: ['records:read'],
        recoveryPrincipal: 'unused',
      },
      [
        {
          id: 'test',
          label: 'Test',
          issuer: issuer.issuer,
          clientId: 'test-client',
          clientSecret: 'test-secret',
          emails: ['snow@example.test'],
        },
      ],
      randomBytes(32),
      true,
    );
    await sessions.initialize();
    const auth = new AppAuth(sessions, 'fixture-key');
    const server = createHttpServer(
      browserProfile('scoped', 'fixture-key', auth),
      async (req, res) => {
        if (!(await auth.handle(req, res))) sendJson(res, 404, { error: { code: 'NOT_FOUND' } });
      },
    );
    await new Promise<void>((done) => server.listen(authPort, '127.0.0.1', done));
    let stop: (() => Promise<void>) | undefined;
    let releaseIdentity: (() => void) | undefined;
    t.after(async () => {
      releaseIdentity?.();
      await stop?.();
      server.closeAllConnections();
      await new Promise<void>((done) => server.close(() => done()));
      await issuer.close();
      await db.close();
    });
    const begin = await fetch(authenticationOrigin + '/api/auth/login?provider=test', {
      redirect: 'manual',
    });
    assert.equal(begin.status, 303);
    const flow = begin.headers
      .getSetCookie()
      .find((value) => value.includes('_flow=') && !value.includes('Max-Age=0'))!
      .split(';')[0];
    const authorize = await fetch(begin.headers.get('location')!, { redirect: 'manual' });
    const callback = await fetch(authorize.headers.get('location')!, {
      redirect: 'manual',
      headers: { cookie: flow },
    });
    assert.equal(callback.status, 303, await callback.clone().text());
    assert.equal(callback.headers.get('location'), nativeOrigin + '/app/');
    const cookie = callback.headers
        .getSetCookie()
        .find((value) => value.includes('Path=/;') && !value.includes('Max-Age=0'))!
        .split(';')[0],
      token = cookie.slice(cookie.indexOf('=') + 1),
      hash = digest(token);
    const session = await sessions.authenticate(`Bearer ${token}`);
    assert.equal(
      (
        await db.pool.query('SELECT moria.native_serving_session($1)->>$2 AS principal', [
          hash,
          'principal',
        ])
      ).rows[0].principal,
      session.principal,
    );
    const frame = await fetch(authenticationOrigin + '/api/auth/refresh', {
      headers: { cookie, 'sec-fetch-site': 'same-site' },
    });
    assert.equal(frame.status, 200);
    assert(
      frame.headers.get('content-security-policy')!.includes('frame-ancestors ' + nativeOrigin),
    );
    assert(!(await frame.text()).includes(token));
    assert.equal(
      (
        await fetch(authenticationOrigin + '/api/v1/objects/records/query', {
          method: 'POST',
          headers: { 'x-moria-app-key': 'fixture-key', 'content-type': 'application/json' },
          body: '{}',
        })
      ).status,
      404,
    );
    const resource = randomUUID(),
      spec = {
        version: 1,
        domain: 'transform',
        release: 'a'.repeat(64),
        resourceClass: 'standard',
        timeoutSeconds: 120,
        retryPolicy: 'idempotent',
        inputSchema: { type: 'object', properties: {} },
        pinnedInputs: [],
        expectedHeads: {},
      };
    const binding = (
      await db.pool.query(
        `INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation)
    SELECT $1,$2,$3,$4,ARRAY['records:read'],ARRAY[]::text[],e.security_epoch,i.security_epoch,c.incarnation FROM moria.environments e,moria.instance_state i,moria.indexing_instance c WHERE e.id=$1 RETURNING id`,
        [environment, resource, spec, { ...session }],
      )
    ).rows[0].id;
    const execution = (
      await db.pool.query('SELECT moria.runtime_execution_request($1,$2,$3,$4,$5) AS id', [
        binding,
        randomUUID(),
        {},
        createHash('sha256').update('{}').digest('hex'),
        JSON.stringify([{}]),
      ])
    ).rows[0].id;
    assert.equal(
      (
        await db.pool.query(
          'SELECT origin_session_hash FROM moria.runtime_job_delegations WHERE execution=$1',
          [execution],
        )
      ).rows[0].origin_session_hash,
      hash,
    );
    await db.pool.query(
      "UPDATE moria.browser_sessions SET expires_at=clock_timestamp()-interval '1 second',verified_at=clock_timestamp()-interval '61 seconds' WHERE token_hash=$1",
      [hash],
    );
    await db.pool.query(
      "UPDATE moria.principals SET synced_at=clock_timestamp()-interval '61 seconds' WHERE environment=$1 AND principal=$2",
      [environment, session.principal],
    );
    const before = (
        await db.pool.query(
          'SELECT expires_at::text AS expires,verified_at::text AS verified FROM moria.browser_sessions WHERE token_hash=$1',
          [hash],
        )
      ).rows[0],
      calls = issuer.state.userInfoCalls;
    await assert.rejects(sessions.authenticate(`Bearer ${token}`));
    stop = sessions.startAcceptedRefresh();
    let refreshed = false;
    for (let attempt = 0; attempt < 150; attempt++) {
      const row = (
        await db.pool.query(
          'SELECT verified_at::text AS verified,expires_at::text AS expires FROM moria.browser_sessions WHERE token_hash=$1',
          [hash],
        )
      ).rows[0];
      if (row.verified !== before.verified) {
        assert.equal(row.expires, before.expires);
        refreshed = true;
        break;
      }
      await delay(100);
    }
    assert(refreshed, 'accepted origin did not obtain fresh actual IdP evidence');
    assert.equal(issuer.state.userInfoCalls, calls + 1);
    await assert.rejects(
      db.pool.query('SELECT moria.native_serving_session_origin($1,false)', [hash]),
      /UNAUTHENTICATED/,
    );
    const proof = (
      await db.pool.query('SELECT moria.runtime_job_delegation_guard($1) AS proof', [execution])
    ).rows[0].proof;
    assert.equal(
      (
        await db.pool.query(
          'SELECT moria.native_serving_accepted_delegation($1)->>$2 AS principal',
          [proof, 'principal'],
        )
      ).rows[0].principal,
      session.principal,
    );
    await stop();
    stop = undefined;
    await db.pool.query(
      "UPDATE moria.browser_sessions SET verified_at=clock_timestamp()-interval '61 seconds' WHERE token_hash=$1",
      [hash],
    );
    await db.pool.query(
      "UPDATE moria.principals SET synced_at=clock_timestamp()-interval '61 seconds' WHERE environment=$1 AND principal=$2",
      [environment, session.principal],
    );
    const stale = (
      await db.pool.query(
        'SELECT verified_at::text AS verified FROM moria.browser_sessions WHERE token_hash=$1',
        [hash],
      )
    ).rows[0].verified;
    let identityEntered!: () => void;
    const entered = new Promise<void>((done) => (identityEntered = done)),
      released = new Promise<void>((done) => (releaseIdentity = done));
    issuer.state.beforeUserInfo = async () => {
      identityEntered();
      await released;
    };
    stop = sessions.startAcceptedRefresh();
    await Promise.race([
      entered,
      delay(15000, undefined, { ref: false }).then(() => {
        throw new Error('IdP verification barrier was not entered');
      }),
    ]);
    await authority.setApplication(environment, 'fixture', application, ['records:read'], false);
    releaseIdentity!();
    await stop();
    stop = undefined;
    issuer.state.beforeUserInfo = undefined;
    assert.equal(
      (
        await db.pool.query(
          'SELECT verified_at::text AS verified,expires_at::text AS expires FROM moria.browser_sessions WHERE token_hash=$1',
          [hash],
        )
      ).rows[0].verified,
      stale,
    );
    await assert.rejects(
      db.pool.query('SELECT moria.native_serving_accepted_delegation($1)', [proof]),
      /UNAUTHENTICATED/,
    );
    assert.deepEqual(
      (
        await db.pool.query(
          'SELECT disabled,access_token FROM moria.browser_sessions WHERE token_hash=$1',
          [hash],
        )
      ).rows[0],
      { disabled: true, access_token: null },
    );
    await authority.setApplication(environment, 'fixture', application, ['records:read'], true);
    await sessions.initialize();
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*)::int n FROM moria.browser_sessions WHERE token_hash=$1',
          [hash],
        )
      ).rows[0].n,
      1,
    );
  },
);
