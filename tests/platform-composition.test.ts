import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve } from 'node:path';
import { createServer } from 'node:net';
import { runProcess } from '../apps/local/src/runtime.js';
import { snowCommand } from '../apps/cli/src/snotel.js';
import { authCommand } from '../apps/cli/src/auth.js';
import { appCommand } from '../apps/cli/src/apps.js';
import { AppHost } from '../services/app-host/src/store.js';
import { createScopedAppServer } from '../services/app-host/src/scoped.js';
import { AppAuth } from '../services/app-host/src/auth.js';
import { Authority } from '../services/security/src/authority.js';
import { BrowserSessions } from '../services/security/src/browser-sessions.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';

async function freePort() {
  const server = createServer();
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>((r) => server.close(() => r()));
  return port;
}

test('retired commands and configuration fail before instance access', async () => {
  await assert.rejects(snowCommand(['open'], {}), /retired/);
  await assert.rejects(authCommand(['recover'], { app: 'snotel' }), /retired/);
  await assert.rejects(appCommand(['bootstrap', 'snotel'], {}), /retired/);
  const previous = process.env.MORIA_SNOTEL_PORT;
  process.env.MORIA_SNOTEL_PORT = '3211';
  try {
    await assert.rejects(runProcess('combined'), /retired/);
  } finally {
    if (previous === undefined) delete process.env.MORIA_SNOTEL_PORT;
    else process.env.MORIA_SNOTEL_PORT = previous;
  }
});

test(
  'default startup composes management only; neutral hosted apps isolate tokens and origins',
  { timeout: 120000 },
  async () => {
    const directory = await mkdtemp(resolve(tmpdir(), 'platform-composition-'));
    const oldEnv = { ...process.env };
    const port = await freePort();
    process.env.MORIA_LOCAL_DIR = directory;
    process.env.MORIA_PORT = String(port);
    process.env.MORIA_DB_PORT = String(await freePort());
    process.env.MORIA_DATABASE_MODE = 'development';
    for (const key of [
      'MORIA_SNOTEL_PORT',
      'MORIA_SNOW_PUBLIC_ORIGIN',
      'MORIA_SNOW_PUBLIC_PORT',
      'MORIA_PUBLIC_ORIGIN',
      'NODE_ENV',
    ])
      delete process.env[key];
    let runtime: Awaited<ReturnType<typeof runProcess>>;
    const servers: ReturnType<typeof createScopedAppServer>[] = [];
    try {
      runtime = await runProcess('combined');
      assert.ok(runtime);
      assert.deepEqual(Object.keys(runtime.jobs.handlers), []);
      assert.deepEqual(await runtime.jobs.inspect(), { jobs: [], schedules: [] });
      const names = await readdir(directory);
      assert.ok(!names.includes('snow-warehouse'));
      assert.ok(!names.includes('snow-access-key'));
      const host = new AppHost(resolve(directory, 'apps'));
      assert.deepEqual(
        (await host.list()).map((app) => app.app),
        ['management'],
      );
      const catalog = runtime.jobs.catalog;
      assert.deepEqual(
        (await catalog.pool.query('SELECT id FROM moria.environments ORDER BY id')).rows,
        [{ id: 'local-workspace' }],
      );
      assert.equal((await fetch(`http://127.0.0.1:${port}/`)).status, 200);
      assert.equal((await fetch(`http://127.0.0.1:${port}/management/v1/jobs`)).status, 401);
      const authority = new Authority(catalog);
      const management = new BrowserSessions(
        authority,
        {
          environment: 'local-workspace',
          application: 'local-ui',
          origin: `http://127.0.0.1:${port}`,
          entryPath: '/',
          initialOperations: [],
          recoveryPrincipal: 'operator',
        },
        [],
        Buffer.alloc(32, 8),
      );
      const managementAuth = new AppAuth(management, 'synthetic-key');
      const managementToken = await management.recover(await management.recoveryTicket());
      const control = (path: string, input?: unknown) =>
        fetch(`http://127.0.0.1:${port}/management/v1/${path}`, {
          method: input === undefined ? 'GET' : 'POST',
          headers: {
            origin: management.binding.origin,
            cookie: `${managementAuth.cookieName}=${managementToken}`,
            'content-type': 'application/json',
          },
          body: input === undefined ? undefined : JSON.stringify(input),
        });
      assert.deepEqual(await (await control('jobs')).json(), {
        jobs: [],
        schedules: [],
        registeredKinds: [],
      });
      assert.equal((await control('lineage')).status, 200);
      assert.equal((await control('jobs/retry', { id: 'bad' })).status, 400);
      assert.equal((await control('jobs/unknown/enqueue', { requestKey: 'unknown' })).status, 400);
      assert.equal((await control('snotel/sync', { requestKey: 'retired' })).status, 400);
      assert.equal(
        (await control('weather/schedule', { enabled: true, intervalMinutes: 60 })).status,
        400,
      );
      const failed = '00000000-0000-4000-8000-000000000047';
      await catalog.pool.query(
        "INSERT INTO moria.jobs(id,kind,input,request_key,requested_by,status) VALUES($1,'retired','{}','retired','fixture','failed')",
        [failed],
      );
      assert.equal((await control('jobs/retry', { id: failed })).status, 400);
      assert.equal(
        (await catalog.pool.query('SELECT status FROM moria.jobs WHERE id=$1', [failed])).rows[0]
          .status,
        'failed',
      );
      const apps = [];
      for (const application of ['sample-one', 'sample-two']) {
        await authority.setApplication('local-workspace', 'fixture', application, []);
        const artifact = await host.publish(
          application,
          resolve('tests/fixtures/platform-composition'),
        );
        await host.promote(application, artifact.version);
        const binding = {
          environment: 'local-workspace',
          application,
          origin: '',
          entryPath: '/',
          initialOperations: [],
          recoveryPrincipal: 'operator',
        };
        const sessions = new BrowserSessions(authority, binding, [], Buffer.alloc(32, 7));
        await sessions.initialize();
        const auth = new AppAuth(sessions, 'synthetic-key');
        const token = await sessions.recover(await sessions.recoveryTicket());
        const server = createScopedAppServer({
          app: application,
          host,
          key: 'synthetic-key',
          auth,
          sessions: new WorkloadSessions(authority),
          token: async () => {
            throw new Error('Browser route must not mint workload tokens');
          },
          objects: new Map(),
          status: async () => ({ application }),
        });
        servers.push(server);
        await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
        binding.origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
        apps.push({ sessions, auth, token, origin: binding.origin, application });
      }
      for (const [own, other] of [
        [apps[0], apps[1]],
        [apps[1], apps[0]],
      ]) {
        assert.match(await (await fetch(own.origin)).text(), /Synthetic platform hosting fixture/);
        const call = (token: string, origin = own.origin) =>
          fetch(`${own.origin}/api/v1/status`, {
            headers: { cookie: `${own.auth.cookieName}=${token}`, origin },
          });
        assert.deepEqual(await (await call(own.token)).json(), { application: own.application });
        assert.equal((await call(other.token)).status, 401);
        assert.equal((await call(own.token, other.origin)).status, 403);
        assert.equal((await call(managementToken)).status, 401);
        await assert.rejects(management.authenticate(`Bearer ${own.token}`), /session/);
        await assert.rejects(own.sessions.authenticate(`Bearer ${other.token}`), /session/);
      }
    } finally {
      await Promise.all(servers.map((server) => new Promise<void>((r) => server.close(() => r()))));
      await runtime!?.close();
      for (const key of Object.keys(process.env)) if (!(key in oldEnv)) delete process.env[key];
      Object.assign(process.env, oldEnv);
      await rm(directory, { recursive: true, force: true });
    }
  },
);
