import { createSnowJobs } from '../../../services/execution/src/snow-jobs.js';
import type { Jobs } from '../../../services/execution/src/jobs.js';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { randomBytes } from 'node:crypto';
import { resolve } from 'node:path';
import { externalDatabase, integer } from '../../../services/database/src/config.js';
import { DatabaseError, holdSchema, schema } from '../../../services/database/src/schema.js';
import pg from 'pg';
import { Catalog } from '../../../services/catalog/src/catalog.js';
import { createWorkspace, managementOperations } from './workspace.js';
import { createLocalServer } from './server.js';
import {
  localAppHost,
  bootstrapManagement,
  bootstrapBundled,
} from '../../../services/app-host/src/bootstrap.js';
import { createSnowService } from '../../../services/snotel/src/service.js';
import { createScopedAppServer } from '../../../services/app-host/src/scoped.js';
import { AppAuth } from '../../../services/app-host/src/auth.js';
import { BrowserSessions } from '../../../services/security/src/browser-sessions.js';
import { loadAuthConfig } from '../../../services/security/src/auth-config.js';

import { publicHosting } from '../../../services/app-host/src/public-hosting.js';

async function main() {
  const mode = process.env.MORIA_DATABASE_MODE ?? 'development';
  if (!['development', 'external'].includes(mode))
    throw new DatabaseError('MORIA_DATABASE_MODE must be development or external.');
  if (process.env.NODE_ENV === 'production' && mode !== 'external')
    throw new DatabaseError('Production requires external database mode.');
  const connection = mode === 'external' ? externalDatabase(process.env) : undefined;
  const port = integer(process.env, 'MORIA_PORT', 3210, 1024, 65535);
  const dbPort = integer(process.env, 'MORIA_DB_PORT', 55432, 1024, 65535);
  const snowPort = integer(process.env, 'MORIA_SNOTEL_PORT', 3211, 1024, 65535);
  const ports = connection ? [port, snowPort] : [port, dbPort, snowPort];
  if (new Set(ports).size !== ports.length) throw new DatabaseError('Listener ports must differ.');
  const publicConfig = publicHosting(process.env);
  const publicServers: ReturnType<typeof createLocalServer>[] = [];
  const directory = resolve(process.env.MORIA_LOCAL_DIR ?? '.local/workspace');
  await mkdir(directory, { recursive: true });
  const credentialsPath = resolve(directory, 'credentials.json');
  let credentials: { password: string; accessKey: string };
  try {
    credentials = JSON.parse(await readFile(credentialsPath, 'utf8'));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    credentials = {
      password: randomBytes(32).toString('base64url'),
      accessKey: randomBytes(32).toString('base64url'),
    };
    await writeFile(credentialsPath, JSON.stringify(credentials), { mode: 0o600, flag: 'wx' });
  }
  let pool: pg.Pool | undefined;
  let server: ReturnType<typeof createLocalServer> | undefined;
  let snowServer: ReturnType<typeof createScopedAppServer> | undefined;
  let jobs: Jobs | undefined;
  let stopDatabase: (() => Promise<void>) | undefined;
  let releaseSchema: (() => void) | undefined;
  let closing = false;
  async function close() {
    if (closing) return;
    closing = true;
    if (server?.listening)
      await new Promise<void>((resolveClose) => server!.close(() => resolveClose()));
    if (snowServer?.listening) await new Promise<void>((r) => snowServer!.close(() => r()));
    for (const publicServer of publicServers)
      if (publicServer.listening) await new Promise<void>((r) => publicServer.close(() => r()));
    await jobs?.stop();
    releaseSchema?.();
    await pool?.end();
    await stopDatabase?.();
  }
  try {
    const appHost = localAppHost(directory);
    await bootstrapManagement(appHost);
    await bootstrapBundled(appHost, 'snotel');
    const snowKeyPath = resolve(directory, 'snow-access-key');
    let snowKey: string;
    try {
      snowKey = (await readFile(snowKeyPath, 'utf8')).trim();
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      snowKey = randomBytes(32).toString('base64url');
      await writeFile(snowKeyPath, snowKey, { mode: 0o600, flag: 'wx' });
    }
    if (connection) {
      pool = new pg.Pool(connection);
      pool.on('error', () => {
        console.error('Database connection lost; stopping services.');
        process.exitCode = 1;
        void close();
      });
      releaseSchema = await holdSchema(pool);
    } else {
      const { startDevelopmentDatabase } = await import('./development-database.js');
      const development = await startDevelopmentDatabase(directory, dbPort, credentials.password);
      pool = development.pool;
      stopDatabase = development.stop;
      await schema(pool, 'apply');
    }
    const catalog = new Catalog(pool, 'verify');
    const workspace = await createWorkspace(catalog, resolve(directory, 'warehouse'));
    const snow = await createSnowService(catalog, resolve(directory, 'snow-warehouse'));
    const execution = await createSnowJobs(catalog, snow);
    jobs = execution.jobs;
    const snowStatus = (
      session?: import('../../../services/ontology/src/service.js').SessionContext,
    ) => snow.status(session, async (c) => ({ weather: await execution.weather.read(c) }));
    const authConfig = await loadAuthConfig(directory);
    const browserSessions = new BrowserSessions(
      snow.authority,
      {
        environment: 'snotel',
        application: 'snow-browser',
        origin: `http://127.0.0.1:${snowPort}`,
        entryPath: '/snow/',
        initialOperations: ['snow-stations:read', 'snow-observations:read'],
        recoveryPrincipal: 'snow-reader',
      },
      authConfig.providers,
      authConfig.key,
    );
    await browserSessions.initialize();
    const managementSessions = new BrowserSessions(
      workspace.authority,
      {
        environment: 'local-workspace',
        application: 'local-ui',
        origin: `http://127.0.0.1:${port}`,
        entryPath: '/',
        initialOperations: managementOperations,
        recoveryPrincipal: 'operator',
        initialPrincipal: { attributes: { admin: true, region: 'West' }, markings: ['workspace'] },
      },
      authConfig.managementProviders,
      authConfig.key,
    );
    await managementSessions.initialize();
    server = createLocalServer(
      workspace,
      credentials.accessKey,
      appHost,
      snow,
      new AppAuth(managementSessions, credentials.accessKey),
      undefined,
      jobs,
    );
    snowServer = createScopedAppServer({
      app: 'snotel',
      entryPath: '/snow/',
      host: appHost,
      key: snowKey,
      auth: new AppAuth(browserSessions, snowKey),
      sessions: snow.sessions,
      token: snow.token,
      objects: new Map([
        ['snow-stations', snow.stations],
        ['snow-observations', snow.observations],
      ]),
      status: snowStatus,
    });
    await new Promise<void>((ready, reject) => {
      server!.once('error', reject);
      server!.listen(port, '127.0.0.1', ready);
    });
    await new Promise<void>((ready, reject) => {
      snowServer!.once('error', reject);
      snowServer!.listen(snowPort, '127.0.0.1', ready);
    });
    for (const entry of publicConfig) {
      const management = entry.app === 'management';
      const localSessions = management ? managementSessions : browserSessions;
      const publicSessions = new BrowserSessions(
        localSessions.authority,
        { ...localSessions.binding, origin: entry.origin },
        management ? authConfig.managementProviders : authConfig.providers,
        authConfig.key,
      );
      const publicAuth = new AppAuth(publicSessions, '', false);
      const publicServer = management
        ? createLocalServer(workspace, '', appHost, snow, publicAuth, entry.origin, jobs)
        : createScopedAppServer({
            app: 'snotel',
            entryPath: '/snow/',
            host: appHost,
            key: '',
            auth: publicAuth,
            publicOrigin: entry.origin,
            sessions: snow.sessions,
            token: snow.token,
            objects: new Map([
              ['snow-stations', snow.stations],
              ['snow-observations', snow.observations],
            ]),
            status: snowStatus,
          });
      publicServers.push(publicServer);
      await new Promise<void>((ready, reject) => {
        publicServer.once('error', reject);
        publicServer.listen(entry.port, '127.0.0.1', ready);
      });
      console.log(`Public ${entry.app}: ${entry.origin}${localSessions.binding.entryPath}`);
    }
    jobs.start();
    console.log(`Moria management: http://127.0.0.1:${port}/`);
    console.log('Management recovery: pnpm moria auth recover --app management');
    console.log(`SNOTEL app: http://127.0.0.1:${snowPort}/snow/`);
    console.log('Google setup: pnpm moria auth setup. Local recovery: pnpm moria auth recover.');
    console.log(`Data persists in ${directory}. Press Ctrl+C to stop.`);
    process.once('SIGINT', () => void close());
    process.once('SIGTERM', () => void close());
  } catch (error) {
    console.error(
      error instanceof DatabaseError
        ? error.message
        : 'Service startup failed; check configuration and database availability.',
    );
    await close();
    process.exitCode = 1;
  }
}
void main().catch((error: unknown) => {
  console.error(error instanceof DatabaseError ? error.message : 'Service configuration failed.');
  process.exitCode = 1;
});
