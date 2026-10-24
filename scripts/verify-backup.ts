import assert from 'node:assert/strict';
import { cp, mkdir, readFile, readdir, stat, unlink } from 'node:fs/promises';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { resolve, join } from 'node:path';
import { createServer } from 'node:net';
import EmbeddedPostgres from 'embedded-postgres';
import pg from 'pg';
import { Catalog } from '../services/catalog/src/catalog.js';
import { Authority } from '../services/security/src/authority.js';
import { BrowserSessions } from '../services/security/src/browser-sessions.js';
import { AppHost } from '../services/app-host/src/store.js';
import { createWorkspace } from '../apps/local/src/workspace.js';

// Trusted local operator tool. Accept an OFFLINE instance backup, never a live PG directory.
const source = resolve(process.argv[2] ?? '');
assert(process.argv[2], 'Usage: pnpm exec tsx scripts/verify-backup.ts <offline-instance-backup>');
const hasPid = await stat(join(source, 'postgres', 'postmaster.pid')).then(
  () => true,
  () => false,
);
assert(
  !hasPid || process.argv.includes('--offline-crash-backup'),
  'Backup contains postmaster.pid. Only an independently verified offline crash backup may use --offline-crash-backup.',
);
const target = resolve('.local', `restore-drill-${randomUUID()}`);
await mkdir(resolve('.local'), { recursive: true });
await cp(source, target, { recursive: true, errorOnExist: true, force: false });
let checkedFiles = 0;
async function verifyFiles(directory: string, relative = '') {
  for (const item of await readdir(directory, { withFileTypes: true })) {
    assert(!item.isSymbolicLink(), 'Backup symlinks are unsupported.');
    const name = join(relative, item.name);
    if (item.isDirectory()) await verifyFiles(join(directory, item.name), name);
    else {
      const digest = (bytes: Buffer) => createHash('sha256').update(bytes).digest('hex');
      assert.equal(
        digest(await readFile(join(target, name))),
        digest(await readFile(join(source, name))),
      );
      checkedFiles++;
    }
  }
}
await verifyFiles(source);
// Only the new, verified clone is modified. PostgreSQL replays this copy's WAL.
if (hasPid) await unlink(join(target, 'postgres', 'postmaster.pid'));
const listener = createServer();
await new Promise<void>((done) => listener.listen(0, '127.0.0.1', done));
const address = listener.address();
assert(address && typeof address !== 'string');
const port = address.port;
await new Promise<void>((done, reject) =>
  listener.close((error) => (error ? reject(error) : done())),
);
const credentials = JSON.parse(await readFile(join(target, 'credentials.json'), 'utf8'));
const db = new EmbeddedPostgres({
  databaseDir: join(target, 'postgres'),
  user: 'moria_local',
  password: credentials.password,
  port,
  persistent: true,
  authMethod: 'scram-sha-256',
  postgresFlags: ['-c', 'listen_addresses=127.0.0.1'],
  onLog: () => {},
  onError: () => {},
});
let pool: pg.Pool | undefined;
let started = false;
try {
  await db.start();
  started = true;
  pool = new pg.Pool({
    host: '127.0.0.1',
    port,
    user: 'moria_local',
    password: credentials.password,
    database: 'postgres',
  });
  const catalog = new Catalog(pool);
  const workspace = await createWorkspace(catalog, join(target, 'warehouse'));
  const assets = await workspace.service.query(
    { environment: 'local-workspace', principal: 'operator', application: 'local-ui' },
    {},
  );
  const counts = (
    await pool.query(`WITH latest AS (
      SELECT DISTINCT ON(environment,object_type) * FROM moria.source_generations ORDER BY environment,object_type,revision DESC
    ), restored AS (
      SELECT g.environment,g.object_type,r.uid FROM latest g JOIN moria.staged_source_rows r ON r.stage=g.stage
      UNION ALL SELECT g.environment,g.object_type,r.uid FROM latest g JOIN moria.source_rows r
      ON g.stage IS NULL AND r.environment=g.environment AND r.object_type=g.object_type AND r.revision=g.revision
    ) SELECT environment,object_type,count(*)::int AS rows FROM restored GROUP BY environment,object_type ORDER BY environment,object_type`)
  ).rows;
  const overlays = (await pool.query('SELECT count(*)::int AS count FROM moria.overlay_versions'))
    .rows[0].count;
  const applications = await new AppHost(join(target, 'apps')).list();
  assert(applications.length > 0);
  for (const app of applications) {
    assert(!app.error);
    for (const version of app.versions)
      await new AppHost(join(target, 'apps')).verify(app.app, version);
  }
  const recovered: string[] = [];
  for (const binding of [
    { environment: 'local-workspace', application: 'local-ui', recoveryPrincipal: 'operator' },
    { environment: 'snotel', application: 'snow-browser', recoveryPrincipal: 'snow-reader' },
  ]) {
    const sessions = new BrowserSessions(
      new Authority(catalog),
      { ...binding, origin: `http://127.0.0.1:${port}`, entryPath: '/', initialOperations: [] },
      [],
      randomBytes(32),
    );
    const token = await sessions.recover(await sessions.recoveryTicket());
    const context = await sessions.authenticate(`Bearer ${token}`);
    assert.equal(context.principal, binding.recoveryPrincipal);
    await sessions.logout(token);
    recovered.push(binding.application);
  }
  console.log(
    JSON.stringify(
      {
        target,
        checkedFiles,
        counts,
        overlayVersions: overlays,
        assetCount: assets.objects.length,
        assetHash: createHash('sha256').update(JSON.stringify(assets.objects)).digest('hex'),
        applications: applications.map((a) => ({
          app: a.app,
          version: a.deployment?.current,
          verifiedReleases: a.versions.length,
        })),
        recoveryVerified: recovered,
        limitations: [
          'No HTTP server, scheduler, Google login or external source requests started.',
          'Iceberg files verified byte-for-byte; absolute warehouse paths are retained and require the original restore location for reads.',
        ],
      },
      null,
      2,
    ),
  );
} finally {
  await pool?.end();
  if (started) await db.stop();
}
