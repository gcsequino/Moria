import {
  createClient as managementSdk,
  ApiError,
} from '../../apps/management/src/generated/index.js';
import { request as httpRequest } from 'node:http';
import assert from 'node:assert/strict';
import { randomBytes, randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import { Catalog } from '../../services/catalog/src/catalog.js';
import { BrowserSessions } from '../../services/security/src/browser-sessions.js';
import { AppAuth } from '../../services/app-host/src/auth.js';
import { AppHost } from '../../services/app-host/src/store.js';
import { createWorkspace, managementOperations } from '../../apps/local/src/workspace.js';
import { createLocalServer } from '../../apps/local/src/server.js';
import { oidcFixture } from './oidc.js';
import { Jobs } from '../../services/execution/src/jobs.js';

export async function checkManagementAuth(catalog: Catalog) {
  const issuer = await oidcFixture();
  const workspace = await createWorkspace(
    catalog,
    resolve('.local', `management-auth-${randomUUID()}`),
  );
  const binding = {
    environment: 'local-workspace',
    application: 'local-ui',
    origin: 'http://127.0.0.1:0',
    entryPath: '/',
    initialOperations: managementOperations,
    recoveryPrincipal: 'operator',
    initialPrincipal: { attributes: { admin: true, region: 'West' }, markings: ['workspace'] },
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
    sessions = new BrowserSessions(workspace.authority, binding, providers, key, true);
  await sessions.initialize();
  const auth = new AppAuth(sessions, 'operator-key');
  let handlerCalls = 0;
  const jobs = new Jobs(catalog, {
    snotel: async () => {
      handlerCalls++;
      return {};
    },
    weather: async () => {
      handlerCalls++;
      return {};
    },
  });
  await jobs.migrate();
  const snowKey = `admin-snow:${randomUUID()}`;
  const weatherKey = `admin-weather:${randomUUID()}`;
  const server = createLocalServer(
    workspace,
    'operator-key',
    new AppHost(resolve('.local', `management-host-${randomUUID()}`)),
    undefined,
    auth,
    undefined,
    jobs,
  );
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  binding.origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  async function login(target = sessions) {
    const flow = await target.start('test');
    const redirect = await fetch(flow.location, { redirect: 'manual' });
    return target.finish(new URL(redirect.headers.get('location')!), flow.browser);
  }
  const request = async (
    path: string,
    token = '',
    input?: unknown,
    headers: Record<string, string> = {},
  ) => {
    const response = await fetch(binding.origin + path, {
      method: input === undefined ? 'GET' : 'POST',
      headers: {
        cookie: `${auth.cookieName}=${token}`,
        origin: binding.origin,
        'content-type': 'application/json',
        ...headers,
      },
      ...(input === undefined ? {} : { body: JSON.stringify(input) }),
    });
    return { status: response.status, data: await response.json() };
  };
  try {
    const token = await login(),
      context = await sessions.authenticate(`Bearer ${token}`);
    const sdk = managementSdk({
      baseUrl: binding.origin,
      fetch: async (url, init) =>
        fetch(url, {
          ...init,
          headers: {
            ...init?.headers,
            cookie: `${auth.cookieName}=${token}`,
            origin: binding.origin,
          },
        }),
    });
    assert.equal((await sdk.endpoints.workspace()).administrator, true);
    assert.equal((await request('/api/management/v1/workspace', token)).data.administrator, true);
    assert.equal((await request('/api/management/v1/apps', token)).status, 200);
    assert.equal((await request('/api/management/v1/workspace')).status, 401);
    assert(Array.isArray((await sdk.endpoints.jobs()).jobs));
    assert(Array.isArray((await sdk.endpoints.lineage()).nodes));
    const queuedSnow = await sdk.endpoints.syncSnow({ requestKey: snowKey });
    assert.equal(queuedSnow.kind, 'snotel');
    assert.equal(queuedSnow.status, 'queued');
    assert.equal((await sdk.endpoints.syncSnow({ requestKey: snowKey })).id, queuedSnow.id);
    const queuedWeather = await sdk.endpoints.syncWeather({
      requestKey: weatherKey,
    });
    assert.equal(queuedWeather.kind, 'weather');
    assert.equal(queuedWeather.status, 'queued');
    const schedule = await sdk.endpoints.setSnowSchedule({ enabled: true, intervalMinutes: 360 });
    assert.equal(schedule.enabled, true);
    assert.equal(schedule.interval_minutes, 360);
    await sdk.endpoints.setSnowSchedule({ enabled: false, intervalMinutes: 360 });
    assert.equal(
      (await sdk.endpoints.setWeatherSchedule({ enabled: false, intervalMinutes: 60 })).enabled,
      false,
    );
    for (const path of ['jobs', 'lineage'])
      assert.equal((await request(`/api/management/v1/${path}`)).status, 401);
    const deniedCsrfKey = `csrf:${randomUUID()}`;
    assert.equal(
      (
        await request(
          '/api/management/v1/snotel/sync',
          token,
          { requestKey: deniedCsrfKey },
          { origin: 'https://evil.test' },
        )
      ).status,
      403,
    );
    assert.equal(
      (
        await request(
          '/api/management/v1/snotel/schedule',
          token,
          { enabled: true, intervalMinutes: 60 },
          { origin: 'https://evil.test' },
        )
      ).status,
      403,
    );
    assert.equal(
      (await catalog.pool.query('SELECT 1 FROM moria.jobs WHERE request_key=$1', [deniedCsrfKey]))
        .rowCount,
      0,
    );
    assert.equal(
      (await sdk.endpoints.jobs()).schedules.find((s) => s.kind === 'snotel')?.enabled,
      false,
    );
    const guardKey = `guard:${randomUUID()}`;
    await assert.rejects(
      jobs.enqueue('snotel', {}, guardKey, 'denied', async () => {
        throw new Error('test guard denied');
      }),
      /test guard denied/,
    );
    assert.equal(
      (await catalog.pool.query('SELECT 1 FROM moria.jobs WHERE request_key=$1', [guardKey]))
        .rowCount,
      0,
    );
    assert.equal(handlerCalls, 0, 'HTTP mutations enqueue work without invoking external handlers');
    const adminRoutes = [
      { path: 'jobs' },
      { path: 'lineage' },
      { path: 'snotel/sync', input: { requestKey: `security:${randomUUID()}` } },
      { path: 'weather/sync', input: { requestKey: `security:${randomUUID()}` } },
      { path: 'snotel/schedule', input: { enabled: true, intervalMinutes: 60 } },
      { path: 'weather/schedule', input: { enabled: true, intervalMinutes: 60 } },
      { path: 'jobs/retry', input: { id: queuedSnow.id } },
    ];
    const assertAdminDenied = async (deniedToken: string, expectedStatus: number) => {
      for (const prefix of ['/api/management/v1/', '/management/v1/']) {
        for (const route of adminRoutes) {
          const response = await request(prefix + route.path, deniedToken, route.input, {
            'x-moria-key': 'operator-key',
            'x-moria-profile': 'operator',
            authorization: `Bearer ${token}`,
          });
          assert.equal(response.status, expectedStatus, `${prefix}${route.path}`);
          assert.deepEqual(
            Object.keys(response.data),
            ['error'],
            'Denied responses expose no instance metadata',
          );
          assert.equal(
            response.data.error.code,
            expectedStatus === 401 ? 'UNAUTHENTICATED' : 'FORBIDDEN',
          );
        }
      }
    };
    await assertAdminDenied('', 401);
    const duplicateCookie = await request('/api/management/v1/lineage', token, undefined, {
      cookie: `${auth.cookieName}=${token}; ${auth.cookieName}=${token}`,
    });
    assert.equal(duplicateCookie.status, 401);
    const privateMarker = `lineage-sensitive-payload-${randomUUID()}`;
    await catalog.pool.query('UPDATE moria.jobs SET result=$2,error=$3 WHERE id=$1', [
      queuedSnow.id,
      JSON.stringify({ secret: privateMarker, path: privateMarker }),
      privateMarker,
    ]);
    assert(!JSON.stringify(await sdk.endpoints.lineage()).includes(privateMarker));
    // Scope revocation is independent of the principal's administrator grant.
    await workspace.authority.setApplication(binding.environment, 'test', binding.application, [
      'assets:read',
    ]);
    await assertAdminDenied(token, 403);
    await workspace.authority.setApplication(
      binding.environment,
      'test',
      binding.application,
      managementOperations,
    );
    assert(Array.isArray((await sdk.endpoints.lineage()).nodes));
    // A browser cannot use the old key/profile selector to become operator.
    assert.equal(
      (
        await request('/api/management/v1/workspace', '', undefined, {
          'x-moria-key': 'operator-key',
          'x-moria-profile': 'operator',
        })
      ).status,
      401,
    );
    // TLS terminates at the trusted proxy; the dedicated listener accepts only its
    // configured hostname and never exposes local CLI or recovery credentials.
    const publicOrigin = 'https://management.example.test';
    const publicSessions = new BrowserSessions(
      workspace.authority,
      { ...binding, origin: publicOrigin },
      providers,
      key,
      true,
    );
    const publicAuth = new AppAuth(publicSessions, 'operator-key', false);
    const publicServer = createLocalServer(
      workspace,
      'operator-key',
      new AppHost(resolve('.local', `public-host-${randomUUID()}`)),
      undefined,
      publicAuth,
      publicOrigin,
      jobs,
    );
    await new Promise<void>((r) => publicServer.listen(0, '127.0.0.1', r));
    const publicBase = `http://127.0.0.1:${(publicServer.address() as { port: number }).port}`;
    const publicRequest = (path: string, init: RequestInit = {}) =>
      new Promise<Response>((resolve, reject) => {
        const req = httpRequest(
          publicBase + path,
          {
            method: init.method ?? 'GET',
            headers: {
              host: 'management.example.test',
              'x-forwarded-proto': 'https',
              ...(init.headers as Record<string, string>),
            },
          },
          (res) => {
            const chunks: Buffer[] = [];
            res.on('data', (chunk) => chunks.push(chunk));
            res.on('end', () =>
              resolve(
                new Response(Buffer.concat(chunks), {
                  status: res.statusCode,
                  headers: res.headers as Record<string, string>,
                }),
              ),
            );
            res.on('error', reject);
          },
        );
        req.on('error', reject);
        req.end(init.body);
      });
    try {
      assert.equal((await fetch(publicBase + '/api/auth/session')).status, 403);
      assert.equal(
        (await publicRequest('/api/auth/session', { headers: { origin: 'https://evil.test' } }))
          .status,
        403,
      );
      assert.equal(
        (
          await publicRequest('/api/auth/recovery-ticket', {
            method: 'POST',
            headers: { 'x-moria-app-key': 'operator-key' },
          })
        ).status,
        403,
      );
      assert.equal(
        (
          await publicRequest('/api/auth/recover', {
            method: 'POST',
            headers: { origin: publicOrigin },
          })
        ).status,
        403,
      );
      const rejectedKey = await publicRequest('/management/v1/apps', {
        headers: { 'x-moria-key': 'operator-key' },
      });
      assert.equal(rejectedKey.status, 401, await rejectedKey.text());
      const start = await publicRequest('/api/auth/login', {
        method: 'POST',
        headers: { origin: publicOrigin, 'content-type': 'application/json' },
        body: JSON.stringify({ provider: 'test' }),
      });
      assert.equal(start.status, 200);
      const flowCookie = start.headers.get('set-cookie')!;
      assert.match(flowCookie, /; Secure/);
      const location = (await start.json()).url;
      assert.equal(
        new URL(location).searchParams.get('redirect_uri'),
        publicOrigin + '/api/auth/callback',
      );
      const redirect = await fetch(location, { redirect: 'manual' });
      const callback = new URL(redirect.headers.get('location')!);
      const finish = await publicRequest(callback.pathname + callback.search, {
        headers: { cookie: flowCookie.split(';')[0] },
      });
      assert.equal(finish.status, 303);
      const publicCookie = finish.headers.get('set-cookie')!;
      assert.match(publicCookie, /; Secure/);
      const inventory = await publicRequest('/api/management/v1/apps', {
        headers: { cookie: publicCookie.split(';')[0] },
      });
      assert.equal(inventory.status, 200);
      assert.equal(inventory.headers.get('strict-transport-security'), 'max-age=31536000');
    } finally {
      await new Promise<void>((r) => publicServer.close(() => r()));
    }
    const source = [
      { id: 'admin-west', title: 'Admin west', region: 'West', status: 'Original', notes: '' },
      { id: 'admin-east', title: 'Admin east', region: 'East', status: 'Original', notes: '' },
    ];
    assert.equal((await sdk.endpoints.importAssets(source)).rows, 2);
    const page = await request(
      '/api/v1/objects/assets/query',
      token,
      { pageSize: 25 },
      { 'x-moria-profile': 'east-reader', authorization: 'Bearer forged' },
    );
    assert.equal(page.status, 200);
    assert.equal(page.data.objects.length, 2);
    const uid = page.data.objects[0].uid;
    const sdkRows = await sdk.objects.assets.all({ pageSize: 1 });
    assert.equal(sdkRows.length, 2);
    const projected = await sdk.objects.assets.get(uid, ['uid', 'status']);
    assert.deepEqual(Object.keys(projected.object).sort(), ['status', 'uid']);
    const sdkKey = randomUUID();
    const receipt = await sdk.actions['edit-status'](
      { uid, status: 'SDK edit' },
      { idempotencyKey: sdkKey },
    );
    assert.deepEqual(
      await sdk.actions['edit-status']({ uid, status: 'SDK edit' }, { idempotencyKey: sdkKey }),
      receipt,
    );
    await assert.rejects(
      sdk.actions['edit-status']({ uid, status: 'Different input' }, { idempotencyKey: sdkKey }),
      (e: unknown) =>
        e instanceof ApiError && e.code === 'IDEMPOTENCY_CONFLICT' && e.status === 409,
    );

    const edit = await request(
      '/api/v1/actions/edit-status/invoke',
      token,
      { input: { uid, status: 'Google admin edit' } },
      { 'idempotency-key': randomUUID() },
    );
    assert.equal(edit.status, 200);
    assert.equal(
      (await request(`/api/v1/objects/assets/${uid}`, token)).data.object.status,
      'Google admin edit',
    );
    assert.equal(
      (
        await request(
          '/api/v1/actions/edit-status/invoke',
          token,
          { input: { uid, status: 'Forbidden CSRF' } },
          { origin: 'http://evil.test', 'idempotency-key': randomUUID() },
        )
      ).status,
      403,
    );
    assert.equal(
      (await request(`/api/v1/objects/assets/${uid}`, token)).data.object.status,
      'Google admin edit',
    );
    // A different app's valid Google cookie cannot be replayed to management.
    await workspace.authority.setApplication(binding.environment, 'test', 'read-only-app', [
      'snow-stations:read',
    ]);
    const scoped = new BrowserSessions(
      workspace.authority,
      {
        ...binding,
        application: 'read-only-app',
        origin: binding.origin.replace('127.0.0.1', 'localhost'),
        initialOperations: ['snow-stations:read'],
        initialPrincipal: undefined,
      },
      providers,
      key,
      true,
    );
    await scoped.initialize();
    const scopedToken = await login(scoped);
    await assertAdminDenied(scopedToken, 401);
    assert.equal((await request('/api/management/v1/workspace', scopedToken)).status, 401);
    const scopedContext = await scoped.authenticate(`Bearer ${scopedToken}`);
    assert.deepEqual(
      [
        ...(
          await workspace.authority.current(
            binding.environment,
            scopedContext.principal,
            'read-only-app',
          )
        ).identity.applicationScopes,
      ],
      ['snow-stations:read'],
    );
    await workspace.authority.setGrants(
      binding.environment,
      'operator',
      context.principal,
      binding.application,
      ['assets:read'],
    );
    await assertAdminDenied(token, 403);
    for (const call of [() => sdk.endpoints.jobs(), () => sdk.endpoints.lineage()])
      await assert.rejects(call(), (e: unknown) => e instanceof ApiError && e.status === 403);
    const deniedGrantKey = `revoked:${randomUUID()}`;
    await assert.rejects(
      sdk.endpoints.syncSnow({ requestKey: deniedGrantKey }),
      (e: unknown) => e instanceof ApiError && e.status === 403,
    );
    await assert.rejects(
      sdk.endpoints.setSnowSchedule({ enabled: true, intervalMinutes: 60 }),
      (e: unknown) => e instanceof ApiError && e.status === 403,
    );
    assert.equal(
      (await catalog.pool.query('SELECT 1 FROM moria.jobs WHERE request_key=$1', [deniedGrantKey]))
        .rowCount,
      0,
    );
    assert.equal(
      (
        await catalog.pool.query(
          'SELECT 1 FROM moria.job_requests WHERE request_key=ANY($1::text[])',
          [[deniedCsrfKey, guardKey, deniedGrantKey]],
        )
      ).rowCount,
      0,
      'Denied operations cannot attach idempotency aliases to an existing job',
    );
    assert.equal(
      (
        await request('/api/management/v1/apps', token, undefined, {
          'x-moria-key': 'operator-key',
          'x-moria-profile': 'operator',
        })
      ).status,
      403,
    );
    assert.equal(
      (
        await request(
          '/api/v1/actions/edit-status/invoke',
          token,
          { input: { uid, status: 'Denied' } },
          { 'idempotency-key': randomUUID() },
        )
      ).status,
      403,
    );
    await login(); // First-login grants must not reappear.
    await createWorkspace(catalog, resolve('.local', 'management-unused'));
    assert.deepEqual(
      [
        ...(
          await workspace.authority.current(
            binding.environment,
            context.principal,
            binding.application,
          )
        ).identity.grantedOperations,
      ],
      ['assets:read'],
    );
    // Explicit operator re-grant for the remainder of this isolated test only.
    await workspace.authority.setGrants(
      binding.environment,
      'operator',
      context.principal,
      binding.application,
      managementOperations,
    );
    const before = await catalog.heads(binding.environment, ['assets-source']);
    await assert.rejects(
      workspace.ingest(context, source, async () => {
        await sessions.logout(token);
        return context;
      }),
      /expired or was revoked/,
    );
    assert.deepEqual(await catalog.heads(binding.environment, ['assets-source']), before);
    assert.equal((await request('/api/management/v1/workspace', token)).status, 401);
    await assertAdminDenied(token, 401);
    for (const call of [
      () => sdk.endpoints.jobs(),
      () => sdk.endpoints.lineage(),
      () => sdk.endpoints.syncWeather({ requestKey: `logout:${randomUUID()}` }),
    ])
      await assert.rejects(call(), (e: unknown) => e instanceof ApiError && e.status === 401);
    await assert.rejects(
      sdk.endpoints.workspace(),
      (e: unknown) => e instanceof ApiError && e.status === 401,
    );

    // Google can be unavailable; operator CLI and short-lived recovery still work.
    issuer.state.mode = 'unavailable';
    const browserRequests: Record<string, string>[] = [
      { origin: binding.origin },
      { 'sec-fetch-site': 'same-origin' },
    ];
    for (const browserHeaders of browserRequests) {
      assert.equal(
        (
          await fetch(binding.origin + '/api/auth/recovery-ticket', {
            method: 'POST',
            headers: { 'x-moria-app-key': 'operator-key', ...browserHeaders },
          })
        ).status,
        401,
      );
    }
    const ticketResponse = await fetch(binding.origin + '/api/auth/recovery-ticket', {
      method: 'POST',
      headers: { 'x-moria-app-key': 'operator-key' },
    });
    assert.equal(ticketResponse.status, 200);
    const ticket = new URL((await ticketResponse.json()).url).hash.slice('#recovery='.length);
    const recoveryResponse = await fetch(binding.origin + '/api/auth/recover', {
      method: 'POST',
      headers: { origin: binding.origin, 'content-type': 'application/json' },
      body: JSON.stringify({ ticket }),
    });
    assert.equal(recoveryResponse.status, 200);
    const cookie = recoveryResponse.headers.get('set-cookie')!;
    assert.match(cookie, /HttpOnly/);
    assert.match(cookie, /Path=\/api\//);
    const recoveryToken = cookie.split(';')[0].split('=')[1];
    assert.equal((await request('/api/management/v1/apps', recoveryToken)).status, 200);
    assert.equal((await request('/api/management/v1/jobs', recoveryToken)).status, 200);
    assert.equal((await request('/api/management/v1/lineage', recoveryToken)).status, 200);
    for (const path of ['jobs', 'lineage']) {
      const cliRead = await fetch(binding.origin + `/management/v1/${path}`, {
        headers: { 'x-moria-key': 'operator-key' },
      });
      assert.equal(cliRead.status, 200);
      await cliRead.json();
    }
    const cliSync = await fetch(binding.origin + '/management/v1/snotel/sync', {
      method: 'POST',
      headers: { 'x-moria-key': 'operator-key', 'content-type': 'application/json' },
      body: JSON.stringify({ requestKey: `cli:${randomUUID()}` }),
    });
    assert.equal(cliSync.status, 202);
    assert.equal((await cliSync.json()).kind, 'snotel');
    assert.equal(handlerCalls, 0);
    assert.equal((await request('/api/auth/session', recoveryToken)).data.user.recovery, true);
    assert.equal(
      (
        await fetch(binding.origin + '/management/v1/apps', {
          headers: { 'x-moria-key': 'operator-key', 'x-moria-profile': 'east-reader' },
        })
      ).status,
      200,
    );
    await request('/api/auth/logout', recoveryToken, {});
    assert.equal((await request('/api/management/v1/apps', recoveryToken)).status, 401);
  } finally {
    // This fixture deliberately never starts a worker. Do not leave queued work
    // for another integration fixture's supervisor to pick up.
    await catalog.pool.query(
      "UPDATE moria.jobs SET status='cancelled',phase='Test fixture complete' WHERE request_key=ANY($1::text[]) AND status='queued'",
      [[snowKey, weatherKey]],
    );
    await new Promise<void>((r) => server.close(() => r()));
    await issuer.close();
  }
}
