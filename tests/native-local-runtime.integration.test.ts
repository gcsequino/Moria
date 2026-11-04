import assert from 'node:assert/strict';
import test from 'node:test';
import { randomUUID, createHash } from 'node:crypto';
import { spawn, type ChildProcess } from 'node:child_process';
import { createServer } from 'node:net';
import { request as rawRequest } from 'node:http';
import { mkdir, writeFile, readFile, readlink } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { database } from './fixtures/catalog-storage/database.js';
import { assertNativeArtifacts } from './fixtures/native-common/artifact.js';
import { AppHost } from '../services/app-host/src/store.js';
import { buildNativeManagement } from './fixtures/native-common/management.js';
import { assertBootstrapRetry } from './fixtures/native-common/bootstrap.js';
import { filesystemClient } from '../packages/platform/src/filesystem.js';
import { createTransport } from '../packages/platform/src/transport.js';
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
    await buildNativeManagement(build);
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
    const document = await html.text();
    assert(document.includes('Moria · Management'));
    assert(document.includes('<div id="root"></div>'));
    assert(!html.headers.get('content-security-policy')!.includes('unsafe-eval'));
    const script = document.match(/src="([^"]+\.js)"/)![1];
    assert.equal(
      (await fetch(new URL(script, new URL(`/releases/${artifact.version}/`, url)))).status,
      200,
    );
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
    assert.equal(recovery.pathname, '/api/auth/bootstrap');
    const signIn = await fetch(recovery);
    assert.equal(signIn.status, 200);
    assert((await signIn.text()).includes('Sign in to Moria'));
    assert(!signIn.headers.get('content-security-policy')!.includes('unsafe-inline'));
    const signInScript = await fetch(url + '/api/auth/bootstrap.js');
    assert.equal(signInScript.status, 200);
    const signInCode = await signInScript.text();
    assert(signInCode.includes("request('/api/auth/recover'"));
    await assertBootstrapRetry(signInCode);
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
    const files = filesystemClient(
      createTransport({
        baseUrl: url,
        fetch: (target, init) =>
          fetch(target, { ...init, headers: { ...init?.headers, cookie, origin: url } }),
      }),
    );
    const home = (await files.home()).home;
    for (const [name, mediaType, text] of [
      [
        'active.html',
        'text/html',
        "<!doctype html><script>fetch('/api/auth/logout',{method:'POST',headers:{'content-type':'application/json'},body:'{}'})</script>",
      ],
      [
        'active.svg',
        'image/svg+xml',
        `<svg xmlns="http://www.w3.org/2000/svg" onload="fetch('/api/auth/logout',{method:'POST'})"/>`,
      ],
    ]) {
      const bytes = new TextEncoder().encode(text),
        parent = await files.get(home.id);
      const result = await files.writeFile(
        {
          op: 'create',
          parentId: parent.id,
          expectedParentRevision: parent.revision,
          name,
          content: {
            size: bytes.length,
            digest: 'sha256:' + createHash('sha256').update(bytes).digest('hex'),
            mediaType,
          },
        },
        bytes,
        { idempotencyKey: randomUUID() },
      );
      const entry = result.entries.find((entry) => entry.name === name)!;
      const content = await files.readFile(entry.id, entry.revision);
      assert.equal(content.headers.get('content-disposition'), 'attachment');
      assert.equal(content.headers.get('x-frame-options'), 'DENY');
      assert(content.headers.get('content-security-policy')!.includes('sandbox'));
      assert(content.headers.get('content-security-policy')!.includes("default-src 'none'"));
      assert.equal(await content.text(), text);
    }
    const navigationStatus = (path: string, headers: Record<string, string>) =>
      new Promise<number>((resolve, reject) => {
        const request = rawRequest(new URL(path, url), { headers }, (response) => {
          response.resume();
          response.once('end', () => resolve(response.statusCode!));
        });
        request.once('error', reject);
        request.end();
      });

    assert.equal(
      await navigationStatus('/', {
        cookie,
        'sec-fetch-site': 'same-site',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-dest': 'document',
      }),
      200,
    );
    assert.equal(
      await navigationStatus('/api/auth/session', {
        cookie,
        'sec-fetch-site': 'same-site',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
      }),
      403,
    );
    assert.equal(
      await navigationStatus('/api/auth/session', {
        cookie,
        'sec-fetch-site': 'cross-site',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-dest': 'iframe',
      }),
      403,
    );

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
    const anotherTicket = await fetch(url + '/api/auth/recovery-ticket', {
      method: 'POST',
      headers: { 'x-moria-app-key': key },
    });
    assert.equal(anotherTicket.status, 200);
    const anotherURL = new URL((await anotherTicket.json()).url),
      secondRecovery = await fetch(url + '/api/auth/recover', {
        method: 'POST',
        headers: { origin: url, 'content-type': 'application/json' },
        body: JSON.stringify({
          ticket: new URLSearchParams(anotherURL.hash.slice(1)).get('recovery'),
        }),
      });
    assert.equal(secondRecovery.status, 200);
    const secondCookie = secondRecovery.headers
      .getSetCookie()
      .find((value) => value.includes('Path=/;'))!
      .split(';')[0];
    assert.equal(
      (await fetch(url + '/api/management/v1/runtime', { headers: { cookie: secondCookie } }))
        .status,
      200,
    );
    const tokenHash = createHash('sha256')
        .update(secondCookie.slice(secondCookie.indexOf('=') + 1))
        .digest('hex'),
      actor = (
        await db.pool.query(
          'SELECT environment,application FROM moria.browser_sessions WHERE token_hash=$1',
          [tokenHash],
        )
      ).rows[0];
    await db.pool.query(
      'UPDATE moria.application_scopes SET enabled=false WHERE environment=$1 AND application=$2',
      [actor.environment, actor.application],
    );
    await db.pool.query(
      'UPDATE moria.application_scopes SET enabled=true WHERE environment=$1 AND application=$2',
      [actor.environment, actor.application],
    );
    assert.equal(
      (await fetch(url + '/api/management/v1/runtime', { headers: { cookie: secondCookie } }))
        .status,
      401,
      'native operational cookie must not revive after a missed app-disable pulse',
    );
    assert.equal(
      (await (await fetch(url + '/api/auth/session', { headers: { cookie: secondCookie } })).json())
        .user,
      null,
    );
    child.kill('SIGTERM');
    assert.equal(await ended, 0, errors + output);
    for (const pid of pids)
      await assert.rejects(readFile(`/proc/${pid}/cmdline`), { code: 'ENOENT' });
    await assert.rejects(fetch(url + '/livez'));
    await assert.rejects(fetch(authURL + '/api/auth/session'));
  },
);
