import assert from 'node:assert/strict';
import test from 'node:test';
import { spawn, type ChildProcess } from 'node:child_process';
import { createServer } from 'node:net';
import { mkdtemp, mkdir, writeFile, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import pg from 'pg';
import { AppHost } from '../services/app-host/src/store.js';
import { assertNativeArtifacts } from './fixtures/native-common/artifact.js';
async function port() {
  const server = createServer();
  await new Promise<void>((done) => server.listen(0, '127.0.0.1', done));
  const result = (server.address() as { port: number }).port;
  await new Promise<void>((done) => server.close(() => done()));
  return result;
}
test(
  'native development owner drains actual Rust children before stopping qualified embedded PostgreSQL',
  { timeout: 120000 },
  async (t) => {
    const binary = process.env.MORIA_RUNTIME_BINARY ?? '/usr/local/bin/moria-runtime';
    await assertNativeArtifacts(binary, []);
    const directory = await mkdtemp(join(tmpdir(), 'moria-native-development-')),
      build = join(directory, 'preloaded-management');
    await mkdir(build);
    await writeFile(
      join(build, 'index.html'),
      '<!doctype html><html><head><title>Native development</title></head><body>development-owner-proof</body></html>',
    );
    const host = new AppHost(join(directory, 'apps')),
      artifact = await host.publish('management', build);
    await host.promote('management', artifact.version);
    const apiPort = await port(),
      authPort = await port(),
      dbPort = await port(),
      url = `http://127.0.0.1:${apiPort}`;
    let child: ChildProcess | undefined,
      ended: Promise<number | null> | undefined,
      pool: pg.Pool | undefined,
      stopped: number | undefined;
    t.after(async () => {
      if (stopped) {
        try {
          process.kill(stopped, 'SIGCONT');
        } catch {}
      }
      if (child?.exitCode === null) {
        child.kill('SIGTERM');
        await Promise.race([ended, delay(40000, undefined, { ref: false })]);
        if (child.exitCode === null) child.kill('SIGKILL');
      }
      await ended;
      await pool?.end();
      await rm(directory, { recursive: true, force: true });
    });
    child = spawn(process.execPath, ['--import', 'tsx', 'tests/fixtures/native-local-process.ts'], {
      cwd: resolve('.'),
      env: {
        ...process.env,
        NODE_ENV: 'test',
        MORIA_LOCAL_RUNTIME: 'native',
        MORIA_DATABASE_MODE: 'development',
        MORIA_RUNTIME_BINARY: binary,
        MORIA_LOCAL_DIR: directory,
        MORIA_PORT: String(apiPort),
        MORIA_AUTH_PORT: String(authPort),
        MORIA_DB_PORT: String(dbPort),
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
    for (let attempt = 0; attempt < 2000 && !output.includes('native-local-ready'); attempt++) {
      assert.equal(child.exitCode, null, errors + output);
      await delay(20);
    }
    assert(output.includes('native-local-ready'), errors + output);
    const credentials = JSON.parse(await readFile(join(directory, 'credentials.json'), 'utf8'));
    pool = new pg.Pool({
      host: '127.0.0.1',
      port: dbPort,
      user: 'moria_local',
      password: credentials.password,
      database: 'postgres',
    });
    pool.on('error', () => {});
    assert.match(
      (await pool.query("SELECT extversion FROM pg_extension WHERE extname='vector'")).rows[0]
        .extversion,
      /^0\.8\./,
    );
    assert.equal((await fetch(url + '/readyz')).status, 200);
    const children = (await readFile(`/proc/${child.pid}/task/${child.pid}/children`, 'utf8'))
      .trim()
      .split(/\s+/)
      .map(Number);
    let apiPid: number | undefined;
    for (const pid of children) {
      const command = (await readFile(`/proc/${pid}/cmdline`, 'utf8')).split('\0');
      if (command.includes('serve')) apiPid = pid;
    }
    assert(apiPid);
    const ticket = await fetch(url + '/api/auth/recovery-ticket', {
      method: 'POST',
      headers: { 'x-moria-key': credentials.accessKey },
    });
    assert.equal(ticket.status, 200, await ticket.clone().text());
    const recovery = new URL((await ticket.json()).url),
      recovered = await fetch(url + '/api/auth/recover', {
        method: 'POST',
        headers: { origin: url, 'content-type': 'application/json' },
        body: JSON.stringify({
          ticket: new URLSearchParams(recovery.hash.slice(1)).get('recovery'),
        }),
      });
    assert.equal(recovered.status, 200, await recovered.clone().text());
    const cookie = recovered.headers
      .getSetCookie()
      .find((value) => value.includes('Path=/;') && !value.includes('Max-Age=0'))!
      .split(';')[0];
    const home = await fetch(url + '/api/management/v1/fs/home', { headers: { cookie } });
    assert.equal(home.status, 200, await home.clone().text());
    assert.equal((await home.json()).home.kind, 'directory');
    process.kill(apiPid, 'SIGSTOP');
    stopped = apiPid;
    child.kill('SIGTERM');
    await delay(200);
    assert.equal(child.exitCode, null, 'launcher exited while its owned Rust API was stopped');
    assert.equal(
      (await pool.query('SELECT 1 AS alive')).rows[0].alive,
      1,
      'development database stopped before native children drained',
    );
    process.kill(apiPid, 'SIGCONT');
    stopped = undefined;
    assert.equal(await ended, 0, errors + output);
    for (const pid of children)
      await assert.rejects(readFile(`/proc/${pid}/cmdline`), { code: 'ENOENT' });
    await assert.rejects(pool.query('SELECT 1'));
    await assert.rejects(fetch(url + '/livez'));
  },
);
