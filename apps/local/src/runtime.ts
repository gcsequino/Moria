import {
  createPlatformJobs,
  type ExecutionFactory,
} from '../../../services/execution/src/platform.js';
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
import { localAppHost, bootstrapManagement } from '../../../services/app-host/src/bootstrap.js';
import { AppAuth } from '../../../services/app-host/src/auth.js';
import { BrowserSessions } from '../../../services/security/src/browser-sessions.js';
import { loadAuthConfig } from '../../../services/security/src/auth-config.js';

import { publicHosting } from '../../../services/app-host/src/public-hosting.js';

export type ProcessRole = 'combined' | 'api' | 'supervisor' | 'scheduler' | 'worker';

export async function runProcess(
  role: ProcessRole,
  executionFactory: ExecutionFactory = createPlatformJobs,
) {
  const mode = process.env.MORIA_DATABASE_MODE ?? 'development';
  if (!['development', 'external'].includes(mode))
    throw new DatabaseError('MORIA_DATABASE_MODE must be development or external.');
  if (process.env.NODE_ENV === 'production' && mode !== 'external')
    throw new DatabaseError('Production requires external database mode.');
  if (role !== 'combined' && mode !== 'external')
    throw new DatabaseError('Independent processes require external database mode.');
  const connection = mode === 'external' ? externalDatabase(process.env) : undefined;
  const port = integer(process.env, 'MORIA_PORT', 3210, 1024, 65535);
  const dbPort = integer(process.env, 'MORIA_DB_PORT', 55432, 1024, 65535);
  for (const name of ['MORIA_SNOTEL_PORT', 'MORIA_SNOW_PUBLIC_ORIGIN', 'MORIA_SNOW_PUBLIC_PORT']) {
    if (process.env[name] !== undefined)
      throw new DatabaseError(`${name} configures the retired Snow POC; remove it.`);
  }
  const ports = connection ? [port] : [port, dbPort];
  if (new Set(ports).size !== ports.length) throw new DatabaseError('Listener ports must differ.');
  const publicConfig = publicHosting(process.env);
  const publicServers: ReturnType<typeof createLocalServer>[] = [];
  const directory = resolve(process.env.MORIA_LOCAL_DIR ?? '.local/workspace');
  await mkdir(directory, { recursive: true });
  let pool: pg.Pool | undefined;
  let server: ReturnType<typeof createLocalServer> | undefined;
  let jobs: Jobs | undefined;
  let stopDatabase: (() => Promise<void>) | undefined;
  let releaseSchema: (() => void) | undefined;
  let closing: Promise<void> | undefined;
  const startupStopped = new Error('Startup stopped');
  let shutdownRequested = false;
  let finishStartup!: () => void;
  const startupFinished = new Promise<void>((resolveStartup) => {
    finishStartup = resolveStartup;
  });
  const onSignal = () => void close();
  function checkStartup() {
    if (shutdownRequested) throw startupStopped;
  }
  async function listen(listener: ReturnType<typeof createLocalServer>, listenerPort: number) {
    checkStartup();
    await new Promise<void>((ready, reject) => {
      listener.once('error', reject);
      listener.listen(listenerPort, '127.0.0.1', () => {
        listener.removeListener('error', reject);
        ready();
      });
    });
    checkStartup();
  }
  function close() {
    shutdownRequested = true;
    closing ??= drain();
    return closing;
  }
  async function drain() {
    // Startup owns resource acquisition until it settles, including pending listens.
    await startupFinished;
    if (server?.listening)
      await new Promise<void>((resolveClose) => server!.close(() => resolveClose()));
    for (const publicServer of publicServers)
      if (publicServer.listening) await new Promise<void>((r) => publicServer.close(() => r()));
    await jobs?.stop();
    releaseSchema?.();
    await pool?.end();
    await stopDatabase?.();
    process.removeListener('SIGINT', onSignal);
    process.removeListener('SIGTERM', onSignal);
  }
  process.on('SIGINT', onSignal);
  process.on('SIGTERM', onSignal);
  try {
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
      const { password: developmentPassword } = await localCredentials(directory);
      const development = await startDevelopmentDatabase(directory, dbPort, developmentPassword);
      pool = development.pool;
      stopDatabase = development.stop;
      await schema(pool, 'apply');
    }
    const catalog = new Catalog(pool, 'verify');
    const execution = await executionFactory(catalog);
    jobs = execution.jobs;
    checkStartup();
    if (role === 'supervisor' || role === 'scheduler' || role === 'worker') {
      jobs.start(role === 'supervisor' ? 'combined' : role);
      console.log(`Moria execution ${role} ready.`);
      return { close, jobs };
    }
    const credentials = await localCredentials(directory);
    const appHost = localAppHost(directory);
    if (role === 'combined') await bootstrapManagement(appHost);
    const workspace = await createWorkspace(catalog, resolve(directory, 'warehouse'));

    const authConfig = await loadAuthConfig(directory, 'management');
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
      undefined,
      new AppAuth(managementSessions, credentials.accessKey),
      undefined,
      jobs,
      { serveManagement: role === 'combined' },
    );
    await listen(server, port);
    for (const entry of publicConfig) {
      const localSessions = managementSessions;
      const publicSessions = new BrowserSessions(
        localSessions.authority,
        { ...localSessions.binding, origin: entry.origin },
        authConfig.managementProviders,
        authConfig.key,
      );
      const publicAuth = new AppAuth(publicSessions, '', false);
      const publicServer = createLocalServer(
        workspace,
        '',
        appHost,
        undefined,
        publicAuth,
        entry.origin,
        jobs,
        { serveManagement: role === 'combined' },
      );
      publicServers.push(publicServer);
      await listen(publicServer, entry.port);
      console.log(`Public ${entry.app}: ${entry.origin}${localSessions.binding.entryPath}`);
    }
    checkStartup();
    if (role === 'combined') jobs.start();
    console.log(`Moria ${role === 'api' ? 'API' : 'management'}: http://127.0.0.1:${port}/`);
    console.log('Management recovery: pnpm moria auth recover --app management');
    console.log('Google setup: pnpm moria auth setup. Local recovery: pnpm moria auth recover.');
    console.log(`Data persists in ${directory}. Press Ctrl+C to stop.`);
    return { close, jobs };
  } catch (error) {
    if (error !== startupStopped) {
      console.error(
        error instanceof DatabaseError
          ? error.message
          : 'Service startup failed; check configuration and database availability.',
      );
      process.exitCode = 1;
    }
    // Resolve before awaiting drain to avoid startup waiting on itself.
    finishStartup();
    await close();
  } finally {
    finishStartup();
  }
}

async function localCredentials(directory: string) {
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
  return credentials;
}

export function launch(role: ProcessRole) {
  void runProcess(role).catch((error: unknown) => {
    console.error(error instanceof DatabaseError ? error.message : 'Service configuration failed.');
    process.exitCode = 1;
  });
}
