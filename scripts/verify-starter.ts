import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { build } from 'vite';
import { writeSdk } from '../packages/osdk/src/generate.js';
import { AppHost } from '../services/app-host/src/store.js';
import { serveApp } from '../services/app-host/src/http.js';
import { readWorkflow } from '../services/lineage/src/workflow.js';

const example = resolve('examples/station-explorer');
const contract = JSON.parse(await readFile(resolve(example, 'app-contract.json'), 'utf8'));
await writeSdk(contract, resolve(example, 'src/generated'), true);
await build({ configFile: resolve(example, 'vite.config.ts'), logLevel: 'silent' });
const host = new AppHost(resolve('.local', `starter-smoke-${randomUUID()}`, 'apps'));
const artifact = await host.publish('station-explorer', resolve(example, 'dist'));
await host.promote('station-explorer', artifact.version);
await host.verify('station-explorer', artifact.version);
const workflow = await readWorkflow(host);
assert(workflow.nodes.some((n) => n.kind === 'application' && n.label === 'station-explorer'));
assert(workflow.edges.some((e) => e.label === 'declares read'));
assert(!workflow.nodes.some((n) => n.kind === 'action'));
const server = createServer(async (req, res) => {
  if (!(await serveApp(host, 'station-explorer', req, res))) {
    res.writeHead(404);
    res.end();
  }
});
await new Promise<void>((ready) => server.listen(0, '127.0.0.1', ready));
try {
  const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  const entry = await fetch(origin);
  assert.equal(entry.status, 200);
  const html = await entry.text();
  assert.match(html, /Station Explorer/);
  const source = html.match(/src="([^"]+\.js)"/)?.[1];
  assert(source, 'Production HTML must load a bundled module');
  const releaseBase = html.match(/<base href="([^"]+)"/)?.[1];
  assert(releaseBase, 'AppHost must pin assets to the immutable release');
  const asset = await fetch(new URL(source, new URL(releaseBase, entry.url)));
  assert.equal(asset.status, 200);
  await asset.arrayBuffer();
  assert.equal(
    (await fetch(`${origin}/api/auth/session`)).status,
    404,
    'Static publication must not imply an API or grant access',
  );
  console.log(
    JSON.stringify(
      {
        ok: true,
        app: artifact.app,
        version: artifact.version,
        verified: [
          'SDK drift',
          'Vite build',
          'artifact integrity',
          'workflow declaration',
          'HTTP HTML and JS',
          'static-only API isolation',
        ],
      },
      null,
      2,
    ),
  );
} finally {
  await new Promise<void>((done) => server.close(() => done()));
}
