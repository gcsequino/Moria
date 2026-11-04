import assert from 'node:assert/strict';
import test from 'node:test';
import { randomUUID } from 'node:crypto';
import { spawn, type ChildProcess } from 'node:child_process';
import { createServer } from 'node:net';
import { mkdir, writeFile, readFile, readlink } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { database } from './fixtures/catalog-storage/database.js';
import { assertNativeArtifacts } from './fixtures/native-common/artifact.js';
import { AppHost } from '../services/app-host/src/store.js';
async function port() {
  const server = createServer();
  await new Promise<void>((done) => server.listen(0, '127.0.0.1', done));
  const value = (server.address() as { port: number }).port;
  await new Promise<void>((done) => server.close(() => done()));
  return value;
}
test(
  'default local launcher hosts native HTTP and scheduler with a separate bounded auth listener',
  { timeout: 120000 },
  async (t) => {
    const binary = process.env.MORIA_RUNTIME_BINARY ?? '/usr/local/bin/moria-runtime';
    await assertNativeArtifacts(binary, []);
    const db = await database();
    let child: ChildProcess | undefined;
    let ended: Promise<number | null> | undefined;
    t.after(async () => {
      if (child?.exitCode === null) child.kill('SIGKILL');
      await ended;
      await db.close();
    });
    const directory = join(db.root, 'native-local'),
      build = join(db.root, 'management-bundle');
    await mkdir(directory);
    await mkdir(build);
    await writeFile(
      join(build, 'index.html'),
      '<!doctype html><html><head><title>Management launcher proof</title></head><body>native-management-proof</body></html>',
    );
    const host = new AppHost(join(directory, 'apps'));
    const artifact = await host.publish('management', build);
    await host.promote('management', artifact.version);
    const key = randomUUID() + randomUUID();
    await writeFile(
      join(directory, 'credentials.json'),
      JSON.stringify({ password: randomUUID(), nativePassword: randomUUID(), accessKey: key }),
      { mode: 0o600 },
    );
    const role = 'launcher_' + randomUUID().replaceAll('-', ''),
      password = randomUUID();
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role};GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role};GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role};ALTER ROLE ${role} SET temp_file_limit='256MB'`,
    );
    const apiPort = await port(),
      authPort = await port(),
      url = `http://127.0.0.1:${apiPort}`,
      authURL = `http://127.0.0.1:${authPort}`;
    child = spawn(process.execPath, ['--import', 'tsx', 'tests/fixtures/native-local-process.ts'], {
      cwd: resolve('.'),
      env: {
        ...process.env,
        NODE_ENV: 'test',
        MORIA_LOCAL_RUNTIME: 'native',
        MORIA_DATABASE_MODE: 'external',
        MORIA_DATABASE_SECRET_REF: 'NATIVE_LOCAL_TEST_DATABASE',
        NATIVE_LOCAL_TEST_DATABASE: `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`,
        MORIA_DATABASE_TLS: 'disable',
        MORIA_INDEXING_DATABASE_SECRET_REF: 'NATIVE_LOCAL_TEST_DATABASE',
        MORIA_INDEXING_DATABASE_TLS: 'disable',
        MORIA_RUNTIME_BINARY: binary,
        MORIA_LOCAL_DIR: directory,
        MORIA_PORT: String(apiPort),
        MORIA_AUTH_PORT: String(authPort),
        MORIA_NATIVE_DRAIN_SECONDS: '2',
        MORIA_NATIVE_WORKERS: '[]',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let output = '',
      errors = '';
    child.stdout!.on('data', (bytes) => (output += bytes));
    child.stderr!.on('data', (bytes) => (errors += bytes));
    ended = new Promise((done) => child!.once('exit', done));
    for (let attempt = 0; attempt < 1000 && !output.includes('native-local-ready'); attempt++) {
      assert.equal(child.exitCode, null, errors + output);
      await delay(20);
    }
    assert(output.includes('native-local-ready'), errors + output);
    const pids = (await readFile(`/proc/${child.pid}/task/${child.pid}/children`, 'utf8'))
      .trim()
      .split(/\s+/)
      .map(Number);
    assert.equal(pids.length, 2);
    const commands = [];
    for (const pid of pids) {
      assert.equal(await readlink(`/proc/${pid}/exe`), await readlink(`/proc/${pids[0]}/exe`));
      commands.push((await readFile(`/proc/${pid}/cmdline`, 'utf8')).split('\0'));
    }
    assert(commands.some((args) => args.includes('serve')));
    assert(commands.some((args) => args.includes('scheduler')));
    assert.equal((await fetch(url + '/readyz')).status, 200);
    const html = await fetch(url);
    assert.equal(html.status, 200);
    assert((await html.text()).includes('native-management-proof'));
    assert.equal(
      (
        await fetch(authURL + '/api/v1/objects/anything/query', {
          method: 'POST',
          headers: { 'content-type': 'application/json', 'x-moria-key': key },
          body: '{}',
        })
      ).status,
      404,
    );
    const ticket = await fetch(url + '/api/auth/recovery-ticket', {
      method: 'POST',
      headers: { 'x-moria-app-key': key },
    });
    assert.equal(ticket.status, 200, await ticket.clone().text());
    const recovery = new URL((await ticket.json()).url);
    const recovered = await fetch(url + '/api/auth/recover', {
      method: 'POST',
      headers: { origin: url, 'content-type': 'application/json' },
      body: JSON.stringify({ ticket: new URLSearchParams(recovery.hash.slice(1)).get('recovery') }),
    });
    assert.equal(recovered.status, 200, await recovered.clone().text());
    const cookie = recovered.headers
      .getSetCookie()
      .find((value) => value.includes('Path=/;'))!
      .split(';')[0];
    assert.equal((await fetch(url + '/api/auth/session', { headers: { cookie } })).status, 200);
    assert.equal(
      (
        await fetch(url + '/api/auth/logout', {
          method: 'POST',
          headers: { cookie, origin: 'http://foreign.test' },
        })
      ).status,
      403,
    );
    assert.equal(
      (await fetch(url + '/api/auth/logout', { method: 'POST', headers: { cookie, origin: url } }))
        .status,
      200,
    );
    child.kill('SIGTERM');
    assert.equal(await ended, 0, errors + output);
    for (const pid of pids)
      await assert.rejects(readFile(`/proc/${pid}/cmdline`), { code: 'ENOENT' });
    await assert.rejects(fetch(url + '/livez'));
    await assert.rejects(fetch(authURL + '/api/auth/session'));
  },
);
