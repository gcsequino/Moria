import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn, fork, type ChildProcess } from 'node:child_process';
import { randomUUID, randomBytes } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve, join } from 'node:path';
import { createServer, connect } from 'node:net';
import { request } from 'node:http';
import EmbeddedPostgres from 'embedded-postgres';
import pg from 'pg';
import { AppHost } from '../services/app-host/src/store.js';
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
  'standalone static lifecycle, promotion, pinned assets and request isolation without a database',
  { timeout: 60000 },
  async (t) => {
    const directory = await mkdtemp(join(tmpdir(), 'moria-static-'));
    const children: ChildProcess[] = [];
    t.after(async () => {
      await Promise.all(children.map((child) => stop(child, 'SIGKILL')));
      await rm(directory, { recursive: true, force: true });
    });
    const { source, host, first } = await artifact(directory);
    const port = await available();
    const args = ['--app', 'custom', '--directory', directory, '--port', String(port)];
    const start = () => {
      const result = launch(args);
      children.push(result.child);
      return result;
    };
    let running = start();
    await running.ready();
    const base = `http://127.0.0.1:${port}`;
    const page = await fetch(base);
    assert.equal(page.status, 200);
    assert.match(await page.text(), new RegExp(`/releases/${first.version}/`));
    assert.match(page.headers.get('content-security-policy')!, /worker-src 'none'/);
    assert.equal(page.headers.get('set-cookie'), null);
    assert.equal(page.headers.get('access-control-allow-origin'), null);
    assert.equal(page.headers.get('x-content-type-options'), 'nosniff');
    assert.equal(page.headers.get('cache-control'), 'no-store');
    const malformed = await new Promise<string>((ready, reject) => {
      const socket = connect(port, '127.0.0.1');
      let response = '';
      socket.setEncoding('utf8');
      socket.setTimeout(5000, () => {
        socket.destroy(new Error('malformed request response timed out'));
      });
      socket.on('error', reject);
      socket.on('data', (data) => {
        response += data;
      });
      socket.on('end', () => ready(response));
      socket.on('connect', () => {
        socket.write(
          `GET http://[ HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nConnection: close\r\n\r\n`,
        );
      });
    });
    assert.match(malformed, /^HTTP\/1\.1 400 Bad Request\r\n/);
    const afterMalformed = await fetch(base);
    assert.equal(afterMalformed.status, 200);
    assert.equal(afterMalformed.headers.get('x-moria-version'), first.version);
    assert.equal(running.child.exitCode, null);
    const wrongHost = await new Promise<number | undefined>((ready, reject) => {
      const req = request(base, { headers: { host: `localhost:${port}` } }, (response) => {
        response.resume();
        response.on('end', () => ready(response.statusCode));
      });
      req.on('error', reject);
      req.end();
    });
    assert.equal(wrongHost, 403);
    assert.equal((await fetch(base, { headers: { origin: 'https://wrong.test' } })).status, 403);
    assert.equal((await fetch(base, { headers: { origin: base } })).status, 200);
    for (const path of [
      '/api/auth/recover',
      '/api/auth/login',
      '/api/v1/objects/assets',
      '/api/v1/actions/edit',
      '/management/v1/apps',
      '/local/import',
    ]) {
      for (const method of ['GET', 'POST', 'OPTIONS']) {
        const response = await fetch(base + path, { method });
        assert.equal(response.status, 404);
        assert.equal(response.headers.get('set-cookie'), null);
        assert.equal(response.headers.get('access-control-allow-origin'), null);
      }
    }
    for (const path of ['artifact.json', '%2e%2e%2findex.html', 'missing.js'])
      assert.equal((await fetch(`${base}/releases/${first.version}/${path}`)).status, 404);
    await writeFile(join(source, 'app.js'), 'two');
    const second = await host.publish('custom', source);
    assert.equal((await fetch(base)).headers.get('x-moria-version'), first.version);
    await host.promote('custom', second.version);
    assert.equal((await fetch(base)).headers.get('x-moria-version'), second.version);
    const old = await fetch(`${base}/releases/${first.version}/app.js`);
    assert.equal(await old.text(), 'one');
    assert.match(old.headers.get('cache-control')!, /immutable/);
    assert.equal(await (await fetch(`${base}/releases/${second.version}/app.js`)).text(), 'two');
    // An incomplete HTTP client must not hold shutdown open.
    const socket = connect(port, '127.0.0.1');
    await new Promise<void>((r) => socket.once('connect', r));
    socket.write('GET / HTTP/1.1\r\n');
    await stop(running.child, 'SIGINT');
    socket.destroy();
    assert.equal(running.child.exitCode, 0);
    await available(port);
    running = start();
    await running.ready();
    assert.equal((await fetch(base)).headers.get('x-moria-version'), second.version);
    await stop(running.child);
    assert.equal(running.child.exitCode, 0);
    await available(port);
    await host.verify('custom', first.version);
    await host.verify('custom', second.version);
    assert.deepEqual((await readdir(directory)).sort(), ['apps', 'source']);
  },
);

test(
  'invalid configuration, missing promotion, tampering and occupied port fail without leaked listeners',
  { timeout: 60000 },
  async (t) => {
    const directory = await mkdtemp(join(tmpdir(), 'moria-static-fail-'));
    const children: ChildProcess[] = [];
    t.after(async () => {
      await Promise.all(children.map((child) => stop(child, 'SIGKILL')));
      await rm(directory, { recursive: true, force: true });
    });
    const port = await available();
    const baseArgs = ['--directory', directory, '--port', String(port)];
    const fail = async (args: string[]) => {
      const result = launch(args);
      children.push(result.child);
      await until(() => result.child.exitCode !== null);
      assert.equal(result.child.exitCode, 1);
      assert.match(result.output(), /Static startup failed/);
      await available(port);
    };
    for (const args of [
      [],
      baseArgs,
      [...baseArgs, '--app', '../bad'],
      [...baseArgs, '--app', 'missing'],
      ['--app', 'custom', '--directory', directory, '--port', '0'],
      [...baseArgs, '--app', 'custom', '--unknown'],
    ])
      await fail(args);
    assert.deepEqual(await readdir(directory), []);
    const { host, first, source } = await artifact(directory, 'management');
    await host.publish('unpromoted', source);
    await fail([...baseArgs, '--app', 'unpromoted']);
    const args = [...baseArgs, '--app', 'management'];
    const running = launch(args);
    children.push(running.child);
    await running.ready();
    const occupied = launch(args);
    children.push(occupied.child);
    await until(() => occupied.child.exitCode !== null);
    assert.equal(occupied.child.exitCode, 1);
    assert.match(occupied.output(), /port is already in use/);
    assert.equal((await fetch(`http://127.0.0.1:${port}`)).status, 200);
    await stop(running.child);
    await writeFile(join(host.root, 'management/releases', first.version, 'app.js'), 'tampered');
    await fail(args);
    await assert.rejects(host.verify('management', first.version), { code: 'APP_CORRUPT' });
  },
);

test(
  'static restarts preserve a separate real API process and PostgreSQL state',
  { timeout: 120000 },
  async (t) => {
    const directory = await mkdtemp(join(tmpdir(), 'moria-static-api-'));
    const port = await available();
    const apiPort = await available();
    const staticPort = await available();
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
    const { host, first } = await artifact(workspace, 'management');
    const api = fork(resolve('tests/fixtures/static-host-process/api.ts'), [], {
      env: {
        PATH: process.env.PATH,
        MORIA_DATABASE_MODE: 'external',
        MORIA_DATABASE_SECRET_REF: 'STATIC_TEST_DATABASE',
        STATIC_TEST_DATABASE: `postgresql://static_runtime:${password}@127.0.0.1:${port}/postgres`,
        MORIA_DATABASE_TLS: 'disable',
        MORIA_LOCAL_DIR: workspace,
        MORIA_PORT: String(apiPort),
      },
      stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
    });
    children.push(api);
    let ready = false;
    api.on('message', (message) => {
      if (message === 'ready') ready = true;
    });
    await until(() => {
      assert.equal(api.exitCode, null);
      return ready;
    });
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
        id: 'synthetic-55',
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
      assert.deepEqual(await objects(), originalObjects);
      assert.deepEqual(
        (await pool.query('SELECT count(*) FROM moria.browser_sessions')).rows,
        sessions,
      );
      await host.verify('management', first.version);
    }
    await stop(api);
    assert.equal((await pool.query('SELECT 1 AS alive')).rows[0].alive, 1);
  },
);
