import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve, join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { AppHost } from '../services/app-host/src/store.js';
import { serveApp } from '../services/app-host/src/http.js';
import { parseCSV } from '../apps/management/src/client.js';
const exec = promisify(execFile);

test('app artifacts publish immutably, promote atomically, retain old assets and recover damaged releases', async () => {
  const root = resolve('.local', `host-test-${randomUUID()}`),
    source = join(root, 'source');
  await mkdir(source, { recursive: true });
  await writeFile(
    join(source, 'index.html'),
    '<html><head></head><body>One<script src="app.js"></script></body></html>',
  );
  await writeFile(join(source, 'app.js'), 'console.log("one")');
  const host = new AppHost(join(root, 'apps'));
  const first = await host.publish('example', source);
  assert.equal((await host.publish('example', source)).version, first.version);
  await host.promote('example', first.version);
  await writeFile(join(source, 'app.js'), 'console.log("two")');
  const second = await host.publish('example', source);
  assert.notEqual(first.version, second.version);
  assert.equal((await host.active('example')).current, first.version);
  await host.promote('example', second.version);
  assert.equal((await host.active('example')).previous, first.version);
  assert.equal(
    (await host.file('example', first.version, 'app.js')).toString(),
    'console.log("one")',
  );
  const server = createServer(async (req, res) => {
    if (!(await serveApp(host, 'example', req, res, '/snow/'))) {
      res.writeHead(404);
      res.end();
    }
  });
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  const base = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  try {
    const redirect = await fetch(base, { redirect: 'manual' });
    assert.equal(redirect.status, 302);
    assert.equal(redirect.headers.get('location'), '/snow/');
    const readable = await fetch(base + '/snow/');
    assert.equal(readable.status, 200);
    assert.equal(readable.url, base + '/snow/');
    assert.equal(readable.headers.get('cache-control'), 'no-store');
    assert.match(await readable.text(), new RegExp(`<base href="/releases/${second.version}/">`));
    assert.equal((await fetch(base + '/snow/stations/663')).status, 200);
    assert.equal((await fetch(base + '/snow/missing.js')).status, 404);
    const page = await fetch(base + `/releases/${first.version}/detail/123`);
    assert.match(await page.text(), new RegExp(`<base href="/releases/${first.version}/">`));
    assert.equal((await fetch(base + `/releases/${first.version}/missing.js`)).status, 404);
    assert.equal(
      (await fetch(base + `/releases/${first.version}/%2e%2e%2fartifact.json`)).status,
      404,
    );
    assert.equal((await fetch(base + '/management/v1/apps')).status, 404);
    const old = await fetch(base + `/releases/${first.version}/app.js`);
    assert.match(old.headers.get('cache-control')!, /immutable/);
    assert.match(await old.text(), /one/);
    await host.rollback('example');
    assert.equal((await host.active('example')).current, first.version);
    assert.match(
      await (await fetch(base + '/snow/')).text(),
      new RegExp(`<base href="/releases/${first.version}/">`),
    );
    await writeFile(join(root, 'apps/example/releases', first.version, 'app.js'), 'tampered');
    assert.equal((await fetch(base + `/releases/${first.version}/app.js`)).status, 503);
    await assert.rejects(host.promote('example', first.version), { code: 'APP_CORRUPT' });
    assert.equal((await host.list())[0].error, 'Asset failed integrity validation.');
    await writeFile(join(source, 'app.js'), 'console.log("one")');
    const repaired = await host.publish('example', source, true);
    assert.equal(repaired.version, first.version);
    await writeFile(join(root, 'apps/example/active.json'), 'corrupt');
    await host.promote('example', first.version, true);
    assert.equal((await host.verify('example', first.version)).version, first.version);
    await assert.rejects(host.publish('../bad', source), { code: 'APP_INVALID' });
    await writeFile(join(source, '.env'), 'secret');
    await assert.rejects(host.publish('reject', source), { code: 'APP_INVALID' });
    await writeFile(join(root, 'apps/example/mutation.lock'), String(process.pid));
    await assert.rejects(host.promote('example', first.version), { code: 'APP_BUSY' });
    await assert.rejects(host.unlock('example'), { code: 'APP_BUSY' });
  } finally {
    await new Promise<void>((r) => server.close(() => r()));
  }
});

test(
  'CLI bootstraps, verifies and recovers the bundled app without a database or running UI',
  { timeout: 60000 },
  async () => {
    const directory = resolve('.local', `cli-host-${randomUUID()}`);
    const cli = async (...args: string[]) =>
      JSON.parse(
        (
          await exec(
            process.execPath,
            [
              '--import',
              'tsx',
              'apps/cli/src/main.ts',
              'apps',
              ...args,
              '--directory',
              directory,
              '--json',
            ],
            { maxBuffer: 1024 * 1024 },
          )
        ).stdout,
      );
    const first = await cli('bootstrap');
    assert.match(first.current, /^[a-f0-9]{64}$/);
    const artifact = await cli('verify', 'management');
    assert.ok(Object.keys(artifact.files).some((name) => name.endsWith('.js')));
    await writeFile(
      join(directory, 'apps/management/releases', first.current, 'index.html'),
      'broken',
    );
    await assert.rejects(cli('verify', 'management'));
    const recovered = await cli('recover');
    assert.equal(recovered.current, first.current);
    await cli('verify', 'management');
    const custom = join(directory, 'custom');
    await mkdir(custom);
    await writeFile(
      join(custom, 'index.html'),
      '<html><head></head><body>Custom app</body></html>',
    );
    const published = await cli('publish', 'custom', custom);
    await cli('promote', 'custom', published.version);
    const list = await cli('list');
    assert.equal(list.length, 2);
    assert.equal(list.find((a: any) => a.app === 'custom').deployment.current, published.version);
  },
);

test('React import parser preserves CSV Unicode, quoting and multiline notes', () => {
  const rows = parseCSV(
    'id,title,region,status,notes\r\na,"Pump, 日本語",West,Ready,"Line one\nLine ""two"""\r\n',
  );
  assert.equal(rows[0].title, 'Pump, 日本語');
  assert.equal(rows[0].notes, 'Line one\nLine "two"');
  assert.throws(() => parseCSV('id,id,region,status,notes\na,b,West,c,d'));
  assert.throws(() => parseCSV('id,title,region,status,notes\na,"unclosed'));
});
