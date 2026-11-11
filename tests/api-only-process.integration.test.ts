import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn, fork, type ChildProcess } from 'node:child_process';
import { randomUUID, randomBytes } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, readdir, rm, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve, join } from 'node:path';
import { createServer } from 'node:net';
import { request } from 'node:http';
import EmbeddedPostgres from 'embedded-postgres';
import pg from 'pg';
import { AppHost } from '../services/app-host/src/store.js';
import { Catalog } from '../services/catalog/src/catalog.js';
import { Authority } from '../services/security/src/authority.js';
import { schema } from '../services/database/src/schema.js';
import { digest } from '../services/security/src/browser-sessions.js';

async function available(port = 0) {
  const server = createServer();
  await new Promise<void>((ready, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', ready);
  });
  const actual = (server.address() as { port: number }).port;
  await new Promise<void>((r) => server.close(() => r()));
  return actual;
}
async function until(check: () => boolean | Promise<boolean>) {
  const deadline = Date.now() + 20000;
  while (!(await check())) {
    assert.ok(Date.now() < deadline, 'process barrier timed out');
    await new Promise((r) => setTimeout(r, 25));
  }
}
async function stop(child: ChildProcess, signal: NodeJS.Signals = 'SIGTERM') {
  if (child.exitCode !== null || child.signalCode !== null) return;
  child.kill(signal);
  await until(() => child.exitCode !== null || child.signalCode !== null);
}
function launch(args: string[]) {
  const child = spawn(
    process.execPath,
    ['--import', 'tsx', 'apps/local/src/static-host.ts', ...args],
    {
      // Static delivery gets no database/provider configuration or credentials.
      env: { PATH: process.env.PATH },
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  );
  let output = '';
  child.stdout.on('data', (data) => {
    output += data;
  });
  child.stderr.on('data', (data) => {
    output += data;
  });
  return {
    child,
    output: () => output,
    ready: () =>
      until(() => {
        assert.equal(child.exitCode, null, output);
        return output.includes('Moria static-only');
      }),
  };
}
async function artifact(directory: string, app = 'custom') {
  const source = join(directory, 'source');
  await mkdir(source, { recursive: true });
  await writeFile(join(source, 'index.html'), '<html><head></head><body>synthetic</body></html>');
  await writeFile(join(source, 'app.js'), 'one');
  const host = new AppHost(join(directory, 'apps'));
  const first = await host.publish(app, source);
  await host.promote(app, first.version);
  return { source, host, first };
}

test(
  'API-only runtime and entrypoint preserve authority and independent static/database lifecycles',
  { timeout: 120000 },
  async (t) => {
    const directory = await mkdtemp(join(tmpdir(), 'moria-static-api-'));
    const port = await available();
    const apiPort = await available();
    const staticPort = await available();
    const publicPort = await available();
    const password = randomUUID();
    const db = new EmbeddedPostgres({
      databaseDir: join(directory, 'postgres'),
      user: 'static_test',
      password,
      port,
      persistent: true,
      authMethod: 'scram-sha-256',
      initdbFlags: ['--encoding=UTF8', '--locale=C'],
      postgresFlags: ['-c', 'listen_addresses=127.0.0.1'],
      onLog: () => {},
      onError: () => {},
    });
    const children: ChildProcess[] = [];
    let pool: pg.Pool | undefined;
    t.after(async () => {
      await Promise.all(children.map((child) => stop(child, 'SIGKILL')));
      await pool?.end();
      await db.stop();
      await rm(directory, { recursive: true, force: true });
    });
    await db.initialise();
    await db.start();
    pool = new pg.Pool({
      host: '127.0.0.1',
      port,
      user: 'static_test',
      password,
      database: 'postgres',
    });
    await schema(pool, 'apply');
    await pool.query(`CREATE ROLE static_runtime LOGIN PASSWORD '${password}'`);
    await pool.query('GRANT USAGE ON SCHEMA moria TO static_runtime');
    await pool.query(
      'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA moria TO static_runtime',
    );
    await pool.query('REVOKE UPDATE, DELETE ON moria.security_audit FROM static_runtime');
    await pool.query('REVOKE ALL ON moria.schema_version FROM static_runtime');
    await pool.query('GRANT SELECT ON moria.schema_version TO static_runtime');
    await pool.query('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA moria TO static_runtime');
    const workspace = join(directory, 'workspace');
    const cwd = join(directory, 'empty-cwd');
    await mkdir(cwd);
    // Only real Iceberg worker sources are needed for imports; no UI source or dist.
    await symlink(resolve('workers'), join(cwd, 'workers'));
    const tsx = import.meta.resolve('tsx');
    const startApi = async (mode: string) => {
      const child = fork(resolve('tests/fixtures/api-only-process/child.ts'), [mode], {
        cwd,
        execArgv: ['--import', tsx],
        env: {
          PATH: process.env.PATH,
          MORIA_PYTHON: resolve('.venv/bin/python'),
          MORIA_DATABASE_MODE: 'external',
          MORIA_DATABASE_SECRET_REF: 'API_TEST_DATABASE',
          API_TEST_DATABASE: `postgresql://static_runtime:${password}@127.0.0.1:${port}/postgres`,
          MORIA_DATABASE_TLS: 'disable',
          MORIA_LOCAL_DIR: workspace,
          MORIA_PORT: String(apiPort),
          MORIA_PUBLIC_ORIGIN: 'https://management.example.test',
          MORIA_PUBLIC_PORT: String(publicPort),
        },
        stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
      });
      children.push(child);
      let ready = false;
      child.stdout!.on('data', (data) => {
        if (String(data).includes('Moria API:')) ready = true;
      });
      let errors = '';
      child.stderr!.on('data', (data) => {
        errors += String(data);
      });
      await until(() => {
        assert.equal(child.exitCode, null, `API exited before readiness: ${errors}`);
        return ready;
      });
      return child;
    };
    let api = await startApi('runtime');
    assert.equal((await readdir(workspace)).includes('apps'), false);
    assert.deepEqual(await readdir(cwd), ['workers']);
    const ticket = randomBytes(32).toString('base64url');
    await pool.query(
      "INSERT INTO moria.auth_flows(state_hash,environment,application,expires_at) VALUES($1,'local-workspace','local-ui',clock_timestamp()+interval '5 minutes')",
      [digest(ticket)],
    );
    const origin = `http://127.0.0.1:${apiPort}`;
    const auth = await fetch(`${origin}/api/auth/recover`, {
      method: 'POST',
      headers: { origin, 'content-type': 'application/json' },
      body: JSON.stringify({ ticket }),
    });
    assert.equal(auth.status, 200);
    const cookie = auth.headers.get('set-cookie')!.split(';')[0];
    const state = async () => {
      const response = await fetch(`${origin}/api/management/v1/apps`, {
        headers: { origin, cookie },
      });
      assert.equal(response.status, 200);
      return response.json();
    };
    const rows = [
      {
        id: 'synthetic-58',
        title: 'Static lifecycle fixture',
        region: 'West',
        status: 'Ready',
        notes: 'Disposable',
      },
    ];
    const imported = await fetch(`${origin}/api/management/v1/datasets/assets/import`, {
      method: 'POST',
      headers: { origin, cookie, 'content-type': 'application/json' },
      body: JSON.stringify(rows),
    });
    assert.equal(imported.status, 200);
    const objects = async () => {
      const response = await fetch(`${origin}/api/v1/objects/assets/query`, {
        method: 'POST',
        headers: { origin, cookie, 'content-type': 'application/json' },
        body: JSON.stringify({ pageSize: 25 }),
      });
      assert.equal(response.status, 200);
      return (await response.json()).objects;
    };
    const originalObjects = await objects();
    assert.equal(originalObjects.length, 1);
    assert.deepEqual(await state(), []);
    const lineage = await fetch(`${origin}/api/management/v1/lineage`, {
      headers: { origin, cookie },
    });
    assert.equal(lineage.status, 200);
    await lineage.json();
    const nonServing = async (version = 'a'.repeat(64)) => {
      for (const path of [
        '/',
        '/assets',
        '/index.html',
        '/static/app.js',
        `/releases/${version}/app.js`,
      ]) {
        for (const headers of [{}, { origin, cookie }] as Record<string, string>[]) {
          const response = await fetch(origin + path, { headers });
          assert.equal(response.status, 404);
          assert.equal(response.headers.get('content-type'), 'application/json');
          assert.equal(response.headers.get('x-moria-version'), null);
          assert.equal(response.headers.get('access-control-allow-origin'), null);
          assert.equal((await response.json()).error.code, 'NOT_FOUND');
        }
        const response = await publicRequest(path);
        assert.equal(response.status, 404);
        assert.equal(response.body.error.code, 'NOT_FOUND');
      }
    };
    async function publicRequest(
      path: string,
      headers: Record<string, string> = {},
      method = 'GET',
    ) {
      return new Promise<{ status: number; body: any }>((ready, reject) => {
        const req = request(
          `http://127.0.0.1:${publicPort}${path}`,
          {
            method,
            headers: { host: 'management.example.test', 'x-forwarded-proto': 'https', ...headers },
          },
          (response) => {
            let body = '';
            response.on('data', (part) => {
              body += part;
            });
            response.on('end', () =>
              ready({ status: response.statusCode!, body: JSON.parse(body) }),
            );
          },
        );
        req.on('error', reject);
        req.end();
      });
    }
    await nonServing();
    assert.equal((await publicRequest('/api/auth/session')).status, 200);
    assert.equal((await publicRequest('/', { 'x-forwarded-proto': 'http' })).status, 403);
    assert.equal((await publicRequest('/', { host: 'wrong.test' })).status, 403);
    assert.equal((await publicRequest('/', { origin })).status, 403);
    assert.equal((await publicRequest('/api/auth/recover', {}, 'POST')).status, 403);
    assert.equal((await publicRequest('/api/management/v1/apps', { cookie })).status, 401);
    // A distinct session for the existing non-administrator fixture cannot
    // borrow the operator's bearer/profile headers to gain browser authority.
    const otherToken = randomBytes(32).toString('base64url');
    await pool.query(
      `INSERT INTO moria.browser_sessions(token_hash,environment,principal,application,display_name,verified_at,expires_at)
       VALUES($1,'local-workspace','east-reader','local-ui','Synthetic reader',clock_timestamp(),clock_timestamp()+interval '5 minutes')`,
      [digest(otherToken)],
    );
    const otherCookie = `${cookie.split('=')[0]}=${otherToken}`;
    for (const path of [
      '/api/management/v1/apps',
      '/api/v1/objects/assets/' + originalObjects[0].uid,
    ]) {
      const denied = await fetch(origin + path, {
        headers: {
          origin,
          cookie: otherCookie,
          authorization: `Bearer ${cookie.split('=')[1]}`,
          'x-moria-profile': 'operator',
        },
      });
      assert.equal(denied.status, 403);
    }
    for (const path of [
      '/api/management/v1/apps',
      '/api/v1/objects/assets/' + originalObjects[0].uid,
    ]) {
      assert.equal((await fetch(origin + path)).status, 401);
      assert.equal(
        (await fetch(origin + path, { headers: { cookie, origin: 'https://wrong.test' } })).status,
        403,
      );
    }
    const actionPath = '/api/v1/actions/edit-status/invoke';
    const actionBody = JSON.stringify({
      input: { uid: originalObjects[0].uid, status: 'Updated' },
    });
    for (const path of [actionPath, '/api/management/v1/datasets/assets/import']) {
      const body = path === actionPath ? actionBody : JSON.stringify(rows);
      assert.equal(
        (
          await fetch(origin + path, {
            method: 'POST',
            headers: { origin, cookie: otherCookie, 'content-type': 'application/json' },
            body,
          })
        ).status,
        403,
      );
      assert.equal(
        (
          await fetch(origin + path, {
            method: 'POST',
            headers: { origin, 'content-type': 'application/json' },
            body,
          })
        ).status,
        401,
      );
      assert.equal(
        (
          await fetch(origin + path, {
            method: 'POST',
            headers: { cookie, 'content-type': 'application/json' },
            body,
          })
        ).status,
        403,
      );
      assert.equal(
        (
          await fetch(origin + path, {
            method: 'POST',
            headers: { cookie, origin: 'https://wrong.test', 'content-type': 'application/json' },
            body,
          })
        ).status,
        403,
      );
    }
    const edited = await fetch(origin + actionPath, {
      method: 'POST',
      headers: {
        origin,
        cookie,
        'content-type': 'application/json',
        'idempotency-key': randomUUID(),
      },
      body: actionBody,
    });
    assert.equal(edited.status, 200);
    await edited.json();
    const updatedObjects = await objects();
    assert.equal(updatedObjects[0].status, 'Updated');
    await stop(api);
    assert.equal(api.exitCode, 0);
    assert.equal((await pool.query('SELECT 1 AS alive')).rows[0].alive, 1);
    api = await startApi('entrypoint');
    // Inventory may create its empty root; it must not publish any app.
    assert.deepEqual(await readdir(join(workspace, 'apps')), []);
    assert.deepEqual(await objects(), updatedObjects);
    const { host, first } = await artifact(workspace, 'management');
    await nonServing(first.version);
    const before = await state();
    const sessions = (await pool.query('SELECT count(*) FROM moria.browser_sessions')).rows;
    for (const signal of ['SIGTERM', 'SIGINT'] as const) {
      const running = launch([
        '--app',
        'management',
        '--directory',
        workspace,
        '--port',
        String(staticPort),
      ]);
      children.push(running.child);
      await running.ready();
      const base = `http://127.0.0.1:${staticPort}`;
      assert.equal((await fetch(base)).headers.get('x-moria-version'), first.version);
      assert.equal(
        (
          await fetch(`${base}/api/auth/recover`, {
            method: 'POST',
            headers: { origin: base, cookie },
            body: JSON.stringify({ ticket }),
          })
        ).status,
        404,
      );
      assert.equal(
        (
          await fetch(`${base}/api/management/v1/datasets/assets/import`, {
            method: 'POST',
            headers: { origin: base, cookie },
            body: JSON.stringify([{ ...rows[0], title: 'Must not be imported' }]),
          })
        ).status,
        404,
      );
      await stop(running.child, signal);
      assert.equal(running.child.exitCode, 0);
      await available(staticPort);
      assert.equal(api.exitCode, null);
      assert.deepEqual(await state(), before);
      assert.deepEqual(await objects(), updatedObjects);
      assert.deepEqual(
        (await pool.query('SELECT count(*) FROM moria.browser_sessions')).rows,
        sessions,
      );
      await nonServing(first.version);
      await host.verify('management', first.version);
    }
    // Current grants, rather than startup role or a previously valid session,
    // still govern reads and writes after independent static restarts.
    const authority = new Authority(new Catalog(pool, 'verify'));
    await authority.setGrants('local-workspace', 'operator', 'operator', 'local-ui', [
      'assets:read',
    ]);
    for (const path of [
      '/api/management/v1/apps',
      '/api/v1/objects/assets/' + originalObjects[0].uid,
    ]) {
      assert.equal((await fetch(origin + path, { headers: { origin, cookie } })).status, 403);
    }
    for (const path of [actionPath, '/api/management/v1/datasets/assets/import']) {
      assert.equal(
        (
          await fetch(origin + path, {
            method: 'POST',
            headers: { origin, cookie, 'content-type': 'application/json' },
            body: path === actionPath ? actionBody : JSON.stringify(rows),
          })
        ).status,
        403,
      );
    }
    const survivingStatic = launch([
      '--app',
      'management',
      '--directory',
      workspace,
      '--port',
      String(staticPort),
    ]);
    children.push(survivingStatic.child);
    await survivingStatic.ready();
    await stop(api);
    assert.equal(api.exitCode, 0);
    assert.equal((await pool.query('SELECT 1 AS alive')).rows[0].alive, 1);
    assert.equal(
      (await fetch(`http://127.0.0.1:${staticPort}/`)).headers.get('x-moria-version'),
      first.version,
    );
    await stop(survivingStatic.child);
    assert.equal(survivingStatic.child.exitCode, 0);
  },
);
