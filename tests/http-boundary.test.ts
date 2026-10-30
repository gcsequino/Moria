import assert from 'node:assert/strict';
import test from 'node:test';
import { request, type Server } from 'node:http';
import {
  createHttpServer,
  browserProfile,
  body,
  sendJson,
  dispatchHttp,
} from '../services/http/src/server.js';
import { createApi } from '../apps/api/src/server.js';
import { createLocalServer } from '../apps/local/src/server.js';
import { createScopedAppServer } from '../services/app-host/src/scoped.js';
import type { Workspace } from '../apps/local/src/workspace.js';
import type { WorkloadSessions } from '../services/security/src/sessions.js';
import type { SessionContext, ObjectService } from '../services/ontology/src/service.js';
import { startStaticServer } from '../services/app-host/src/static-server.js';
import { AppHost } from '../services/app-host/src/store.js';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

async function listen(server: Server) {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  return (server.address() as { port: number }).port;
}
function call(
  port: number,
  path = '/',
  headers: Record<string, string> = {},
  method = 'GET',
  payload?: string | Buffer,
) {
  return new Promise<{
    status: number;
    headers: import('node:http').IncomingHttpHeaders;
    text: string;
  }>((resolve, reject) => {
    const req = request({ host: '127.0.0.1', port, path, method, headers }, (res) => {
      const bytes: Buffer[] = [];
      res.on('data', (part: Buffer) => bytes.push(part));
      res.on('end', () =>
        resolve({
          status: res.statusCode!,
          headers: res.headers,
          text: Buffer.concat(bytes).toString(),
        }),
      );
      res.on('aborted', () => reject(new Error('Response aborted')));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.end(payload);
  });
}
const close = (server: Server) =>
  new Promise<void>((resolve) => {
    server.close(() => resolve());
    server.closeAllConnections();
  });

test('every transport profile rejects work before handlers and preserves response contracts', async (t) => {
  for (const kind of ['workload', 'static', 'management', 'scoped'] as const) {
    for (const publicMode of kind === 'management' || kind === 'scoped' ? [false, true] : [false]) {
      await t.test(`${kind} ${publicMode ? 'public proxy' : 'loopback'}`, async () => {
        let calls = 0;
        const origin = 'https://app.example.test';
        const browser = kind === 'management' || kind === 'scoped';
        const profile = browser
          ? browserProfile(
              kind,
              'operator-secret',
              { allowRecovery: !publicMode, sessions: { binding: { origin } } },
              publicMode ? origin : undefined,
            )
          : { kind };
        const nested = createHttpServer({ kind: 'workload' }, async (_req, res, context) =>
          sendJson(res, 200, context),
        );
        const server = createHttpServer(profile, async (req, res, context) => {
          calls++;
          if (req.url === '/body') sendJson(res, 200, await body(req));
          else if (req.url === '/throw') throw new Error('secret database password');
          else if (req.url === '/after') {
            res.writeHead(200);
            res.write('partial');
            throw new Error('secret');
          } else if (req.url === '/dispatch') await dispatchHttp(nested, req, res);
          else sendJson(res, 200, context);
        });
        const port = await listen(server);
        try {
          const ownOrigin = publicMode ? origin : `http://127.0.0.1:${port}`;
          const base: Record<string, string> = publicMode
            ? { host: 'app.example.test', 'x-forwarded-proto': 'https' }
            : {};
          const rejected = async (
            path: string,
            headers: Record<string, string>,
            status = 403,
            method = 'GET',
            payload?: string,
          ) => {
            const before = calls;
            const result = await call(port, path, { ...base, ...headers }, method, payload);
            assert.equal(result.status, status, result.text);
            assert.equal(calls, before, 'rejected request reached application work');
          };
          await rejected('/', { host: 'foreign.example.test' });
          await rejected('/', { origin: 'https://foreign.example.test' });
          await rejected('//foreign.example.test/path', {}, 400);
          await rejected('/%zz', {}, 400);
          await rejected('/', { 'content-length': '65537' }, 413, 'POST');
          if (kind === 'management') {
            const bearer = {
              ...base,
              authorization: `Bearer ${'a'.repeat(43)}`,
              'content-type': 'application/json',
            };
            const result = await call(port, '/api/management/v1/fs/resolve', bearer, 'POST', '{}');
            assert.equal(result.status, 200, result.text);
            const functionCall = await call(
              port,
              '/api/v1/functions/id/invoke',
              bearer,
              'POST',
              '{}',
            );
            assert.equal(functionCall.status, 200, functionCall.text);
            await rejected(
              '/api/management/v1/fs/resolve',
              { ...bearer, cookie: 'session=ambient' },
              403,
              'POST',
              '{}',
            );
            await rejected(
              '/api/management/v1/fs/resolve',
              { ...bearer, 'sec-fetch-site': 'same-origin' },
              403,
              'POST',
              '{}',
            );
            await rejected(
              '/api/management/v1/fs/resolve',
              { ...base, authorization: 'Bearer invalid' },
              403,
              'POST',
              '{}',
            );
            await rejected('/api/management/v1/apps', bearer, 403, 'POST', '{}');
          }

          await rejected(
            '/management/v1/fs/entries/id/policy',
            { 'content-length': '65537' },
            413,
            'POST',
          );
          await rejected(
            '/management/v1/fs/entries/id/content',
            { 'content-length': String(64 * 1024 * 1024 + 1) },
            413,
            'POST',
          );
          if (kind !== 'static') {
            const payload = 'x'.repeat(65537);
            const headers = {
              ...base,
              'content-type': 'application/octet-stream',
              ...(browser ? { origin: ownOrigin } : {}),
            };
            const uploaded = await call(
              port,
              '/management/v1/fs/entries/id/content',
              headers,
              'POST',
              payload,
            );
            assert.equal(uploaded.status, 200, uploaded.text);
          }
          if (publicMode) {
            await rejected('/', { 'x-forwarded-proto': '' });
            await rejected('/', { 'x-forwarded-proto': 'http' });
            await rejected('/api/auth/recovery-ticket', { origin: ownOrigin }, 403, 'POST');
            await rejected('/api/auth/recover', { origin: ownOrigin }, 403, 'POST');
          }
          if (kind === 'workload') await rejected('/', { origin: ownOrigin });
          if (browser) {
            await rejected('/body', { 'content-type': 'application/json' }, 403, 'POST', '{}');
            const keyHeader = kind === 'management' ? 'x-moria-key' : 'x-moria-app-key';
            await rejected(
              '/body',
              {
                [keyHeader]: 'operator-secret',
                'sec-fetch-site': 'same-origin',
                'content-type': 'application/json',
              },
              403,
              'POST',
              '{}',
            );
            if (!publicMode) {
              const cli = await call(port, '/', {
                [keyHeader]: 'operator-secret',
                'x-forwarded-proto': 'https',
              });
              assert.equal(JSON.parse(cli.text).cli, true);
              assert.equal(
                JSON.parse(cli.text).origin,
                ownOrigin,
                'forwarded headers must not select public mode',
              );
            }
          }
          const supported = await call(port, '/', base);
          assert.equal(supported.status, 200);
          assert.equal(supported.headers['x-content-type-options'], 'nosniff');
          assert.match(
            supported.headers['content-security-policy'] as string,
            /frame-ancestors 'none'/,
          );
          assert.equal(supported.headers['cache-control'], 'no-store');
          if (publicMode)
            assert.equal(supported.headers['strict-transport-security'], 'max-age=31536000');
          const bodyHeaders: Record<string, string> = {
            ...base,
            ...(browser ? { origin: ownOrigin } : {}),
            'content-type': 'application/json',
          };
          assert.equal((await call(port, '/body', bodyHeaders, 'POST', '{"ok":true}')).status, 200);
          assert.equal((await call(port, '/body', bodyHeaders, 'POST', '{')).status, 400);
          assert.equal(
            (await call(port, '/body', bodyHeaders, 'POST', Buffer.from([0xff]))).status,
            400,
          );
          assert.equal(
            (
              await call(
                port,
                '/body',
                { ...bodyHeaders, 'content-encoding': 'gzip' },
                'POST',
                '{}',
              )
            ).status,
            400,
          );
          assert.equal(
            (
              await call(
                port,
                '/body',
                { ...bodyHeaders, 'content-type': 'text/plain' },
                'POST',
                '{}',
              )
            ).status,
            400,
          );
          assert.equal(
            (await call(port, '/body', bodyHeaders, 'POST', ' '.repeat(65537))).status,
            413,
          );
          const failure = await call(port, '/throw', base);
          assert.equal(failure.status, 500);
          assert.doesNotMatch(failure.text, /secret|password|stack/);
          if (kind !== 'static') {
            const error = JSON.parse(failure.text).error;
            assert.equal(error.code, 'INTERNAL');
            assert.equal(error.requestId, error.correlationId);
          }
          const dispatched = await call(port, '/dispatch', base);
          assert.equal(dispatched.status, 200);
          assert.equal(JSON.parse(dispatched.text).origin, ownOrigin);
          await assert.rejects(call(port, '/after', base));
          assert.equal(
            (await call(port, '/', base)).status,
            200,
            'post-header failure must not kill the listener',
          );
          assert.equal(server.requestTimeout, 30000);
          assert.equal(server.headersTimeout, 10000);
        } finally {
          await close(server);
        }
      });
    }
  }
});

test('real API, management and scoped factories cannot reach providers or operations before admission', async (t) => {
  const directory = await mkdtemp(join(tmpdir(), 'http-boundary-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const host = new AppHost(directory);
  let work = 0;
  const session = { principal: 'operator' } as SessionContext;
  const sessions = {
    authenticate: async () => {
      work++;
      return session;
    },
  };
  const service = {
    requiring() {
      return this;
    },
    read: async () => {
      work++;
      return { ok: true };
    },
  } as unknown as ObjectService;
  const workspace = {
    service,
    sessions,
    token: async () => {
      work++;
      return 'token';
    },
  } as unknown as Workspace;
  const constructors = [
    () => createApi(sessions, { objects: new Map([['assets', service]]), actions: new Map() }),
    () =>
      createLocalServer(
        workspace,
        'operator-secret',
        host,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        { serveManagement: false },
      ),
    () =>
      createScopedAppServer({
        app: 'sample',
        host,
        key: 'operator-secret',
        sessions: sessions as unknown as WorkloadSessions,
        objects: new Map([['assets', service]]),
        token: async () => {
          work++;
          return 'token';
        },
        status: async () => {
          work++;
          return {};
        },
      }),
  ];
  for (const [index, construct] of constructors.entries()) {
    const server = construct();
    const port = await listen(server);
    try {
      const path = '/api/v1/objects/assets/one';
      const key: Record<string, string> =
        index === 1
          ? { 'x-moria-key': 'operator-secret' }
          : index === 2
            ? { 'x-moria-app-key': 'operator-secret' }
            : {};
      for (const headers of [{ host: 'evil.test' }, { origin: 'https://evil.test' }] as Record<
        string,
        string
      >[]) {
        const before = work;
        assert.equal((await call(port, path, { ...key, ...headers })).status, 403);
        assert.equal(work, before);
      }
      assert.equal((await call(port, path, key)).status, 200);
      assert.ok(work > 0);
      if (index) {
        const before = work;
        assert.equal(
          (await call(port, path, { ...key, origin: `http://127.0.0.1:${port}` })).status,
          401,
        );
        assert.equal(work, before, 'browser Origin must not select the operator key identity');
      }
    } finally {
      await close(server);
    }
  }
});

test('forged profiles and unadmitted internal dispatch fail closed', async () => {
  assert.throws(
    () =>
      createHttpServer({ kind: 'management', cookieAuth: false, key: 'secret' }, async () => {}),
    /trusted HTTP profile/,
  );
  assert.throws(
    () => browserProfile('scoped', 'secret', undefined, 'https://app.example.test'),
    /matching browser auth/,
  );
  const target = createHttpServer({ kind: 'workload' }, async () => {
    assert.fail('unadmitted handler ran');
  });
  await assert.rejects(dispatchHttp(target, {} as never, {} as never), /transport boundary/);
});

test('actual static listener rejects before file delivery and preserves HEAD, cache and API separation', async (t) => {
  const directory = await mkdtemp(join(tmpdir(), 'http-static-boundary-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const host = new AppHost(directory);
  const artifact = await host.publish('sample', 'tests/fixtures/platform-composition');
  await host.promote('sample', artifact.version);
  const reservation = createHttpServer({ kind: 'static' }, async () => {});
  const port = await listen(reservation);
  await close(reservation);
  const server = await startStaticServer(host, 'sample', port);
  const file = t.mock.method(host, 'file');
  try {
    const foreignRequests: Record<string, string>[] = [
      { host: 'foreign.test' },
      { origin: 'https://foreign.test' },
    ];
    for (const headers of foreignRequests) {
      const count = file.mock.callCount();
      assert.equal((await call(port, '/', headers)).status, 403);
      assert.equal(file.mock.callCount(), count);
    }
    const html = await call(port);
    assert.equal(html.status, 200);
    assert.match(html.text, /<base href="\/releases\//);
    assert.equal(html.headers['cache-control'], 'no-store');
    const head = await call(port, '/', {}, 'HEAD');
    assert.equal(head.status, 200);
    assert.equal(head.text, '');
    assert.equal(head.headers['content-type'], html.headers['content-type']);
    assert.equal((await call(port, '/api/v1/objects/assets/one')).status, 404);
    assert.equal((await call(port, '/api/auth/recovery-ticket', {}, 'POST')).status, 404);
  } finally {
    await close(server);
  }
});
