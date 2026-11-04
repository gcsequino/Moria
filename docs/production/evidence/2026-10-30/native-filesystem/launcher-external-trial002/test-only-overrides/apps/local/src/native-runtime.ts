import { spawn, execFile, type ChildProcess } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdir, readFile, writeFile, rename } from 'node:fs/promises';
import { randomBytes, randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import pg from 'pg';
import { externalDatabase, integer } from '../../../services/database/src/config.js';
import { DatabaseError, holdSchema, schema } from '../../../services/database/src/schema.js';
import { Catalog } from '../../../services/catalog/src/catalog.js';
import { Authority } from '../../../services/security/src/authority.js';
import { BrowserSessions } from '../../../services/security/src/browser-sessions.js';
import { loadAuthConfig } from '../../../services/security/src/auth-config.js';
import { AppAuth } from '../../../services/app-host/src/auth.js';
import { publicHosting } from '../../../services/app-host/src/public-hosting.js';
import { bootstrapManagement, localAppHost } from '../../../services/app-host/src/bootstrap.js';
import { createHttpServer, browserProfile, sendJson } from '../../../services/http/src/server.js';
import { filesystemOperations } from '../../../packages/contracts/filesystem.js';
import { startDevelopmentDatabase } from './development-database.js';
import type { ProcessRole } from './runtime.js';

const domains = [
  'indexing',
  'functions',
  'agents',
  'evaluations',
  'pipelines',
  'search',
  'delivery',
] as const;
type Domain = (typeof domains)[number];
function domain(value: string | undefined): Domain {
  if (!domains.includes(value as Domain))
    throw new DatabaseError(`MORIA_NATIVE_WORKER_DOMAIN must name ${domains.join(', ')}.`);
  return value as Domain;
}
function workerArgs(value: Domain) {
  return value === 'indexing' ? ['worker'] : [value, 'worker'];
}
function nativeEnvironment(): NodeJS.ProcessEnv {
  const environment = { ...process.env };
  environment.MORIA_INDEXING_DATABASE_SECRET_REF ??= environment.MORIA_DATABASE_SECRET_REF;
  environment.MORIA_INDEXING_DATABASE_TLS ??= environment.MORIA_DATABASE_TLS ?? 'verify';
  if (
    !environment.MORIA_INDEXING_DATABASE_SECRET_REF ||
    !environment[environment.MORIA_INDEXING_DATABASE_SECRET_REF]
  )
    throw new DatabaseError('Configure the native database secret reference.');
  return environment;
}
function binary() {
  if (process.platform !== 'linux') throw new DatabaseError('Moria services require Linux.');
  return resolve(
    process.env.MORIA_RUNTIME_BINARY ??
      process.env.MORIA_INDEXING_BINARY ??
      '/usr/local/bin/moria-runtime',
  );
}
function execute(args: string[]): never {
  if (Number(process.versions.node.split('.')[0]) !== 24 || !process.execve)
    throw new DatabaseError('Native service launch requires Linux and Node 24.');
  const path = binary();
  process.execve(path, [path, ...args], nativeEnvironment());
  throw new DatabaseError('Native service startup failed.');
}
async function credentials(directory: string) {
  const path = resolve(directory, 'credentials.json');
  let value: { password: string; accessKey: string; nativePassword?: string };
  try {
    value = JSON.parse(await readFile(path, 'utf8'));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    value = {
      password: randomBytes(32).toString('base64url'),
      accessKey: randomBytes(32).toString('base64url'),
    };
  }
  if (!value.nativePassword) {
    value.nativePassword = randomBytes(32).toString('base64url');
    const temporary = resolve(directory, `credentials-${randomUUID()}.json`);
    await writeFile(temporary, JSON.stringify(value), { mode: 0o600, flag: 'wx' });
    await rename(temporary, path);
  }
  return value as { password: string; accessKey: string; nativePassword: string };
}

/** Bounded administration and OIDC bootstrap; every operational listener and worker is native. */
export async function runNativeProcess(role: Exclude<ProcessRole, 'function-worker'>) {
  for (const name of ['MORIA_SNOTEL_PORT', 'MORIA_SNOW_PUBLIC_ORIGIN', 'MORIA_SNOW_PUBLIC_PORT'])
    if (process.env[name] !== undefined)
      throw new DatabaseError(`${name} configures the retired Snow POC; remove it.`);
  const mode = process.env.MORIA_DATABASE_MODE ?? 'development';
  if (!['development', 'external'].includes(mode))
    throw new DatabaseError('MORIA_DATABASE_MODE must be development or external.');
  if ((process.env.NODE_ENV === 'production' || role !== 'combined') && mode !== 'external')
    throw new DatabaseError('Production and independent processes require external database mode.');
  if (role !== 'combined' && process.env.MORIA_PUBLIC_ORIGIN)
    throw new DatabaseError('Independent native API hosting uses MORIA_NATIVE_PUBLIC_ORIGIN.');
  if (role === 'combined' && process.env.MORIA_NATIVE_PUBLIC_ORIGIN)
    throw new DatabaseError(
      'Combined hosting uses MORIA_PUBLIC_ORIGIN plus MORIA_AUTH_PUBLIC_ORIGIN for its separate public listeners.',
    );
  if (role === 'scheduler' || role === 'supervisor') execute(['execution', 'scheduler']);
  if (role === 'worker') execute(workerArgs(domain(process.env.MORIA_NATIVE_WORKER_DOMAIN)));
  const path = binary();
  const port = integer(process.env, 'MORIA_PORT', 3210, 1024, 65535);
  const dbPort = integer(process.env, 'MORIA_DB_PORT', 55432, 1024, 65535);
  const authPort = integer(process.env, 'MORIA_AUTH_PORT', 3211, 1024, 65535);
  if (
    new Set([port, authPort, ...(mode === 'development' ? [dbPort] : [])]).size !==
    (mode === 'development' ? 3 : 2)
  )
    throw new DatabaseError('Listener ports must differ.');
  const directory = resolve(process.env.MORIA_LOCAL_DIR ?? '.local/workspace');
  await mkdir(directory, { recursive: true });
  const children: { child: ChildProcess; exit: Promise<void> }[] = [];
  let pool: pg.Pool | undefined,
    stopDatabase: (() => Promise<void>) | undefined,
    releaseSchema: (() => void) | undefined;
  let authentication: ReturnType<typeof createHttpServer> | undefined;
  const stopIdentityRefresh: (() => Promise<void>)[] = [];
  const publicAuthentications: ReturnType<typeof createHttpServer>[] = [];
  let publicEnvironment: NodeJS.ProcessEnv | undefined;
  let stopping = false,
    signalRequested=false,
    closing: Promise<void> | undefined;
  let finishStartup!: () => void;
  const startup = new Promise<void>((done) => {
    finishStartup = done;
  });
  const check = () => {
    if (stopping) throw new DatabaseError('Startup stopped.');
  };
  const close = () => {
    stopping = true;
    return (closing ??= (async () => {
      await startup;
      await Promise.all(stopIdentityRefresh.map((stop) => stop()));
      if (authentication?.listening) {
        authentication.closeAllConnections();
        await new Promise<void>((done) => authentication!.close(() => done()));
      }
      for (const listener of publicAuthentications)
        if (listener.listening) {
          listener.closeAllConnections();
          await new Promise<void>((done) => listener.close(() => done()));
        }
      for (const { child } of children) child.kill('SIGTERM');
      for (const { child, exit } of children) {
        const ended = await Promise.race([
          exit.then(() => true),
          delay(35000, false, { ref: false }),
        ]);
        if (!ended) {
          child.kill('SIGKILL');
          await exit;
        }
      }
      releaseSchema?.();
      await pool?.end();
      await stopDatabase?.();
      // Keep idempotent handlers through process exit when a CLI loader forwards a signal twice.
      if(!signalRequested){process.removeListener('SIGINT',signal);process.removeListener('SIGTERM',signal);}
    })());
  };
  const signal = () => {
    signalRequested=true;
    void close().then(
      () => {
        if (process.exitCode === undefined || [0, 130, 143].includes(Number(process.exitCode)))
          process.exitCode = 0;
      },
      () => {
        process.exitCode = 1;
      },
    );
  };
  process.on('SIGINT', signal);
  process.on('SIGTERM', signal);
  function launch(args: string[], environment: NodeJS.ProcessEnv, readyEvent?: string) {
    check();
    const child = spawn(path, args, { env: environment, stdio: ['ignore', 'pipe', 'pipe'] });
    const exit = new Promise<void>((done) =>
      child.once('exit', () => {
        done();
        if (!stopping) {
          process.exitCode = 1;
          void close();
        }
      }),
    );
    children.push({ child, exit });
    return new Promise<void>((ready, reject) => {
      let buffered = '',
        resolved = false;
      const timeout = setTimeout(
        () => reject(new DatabaseError('Native startup exceeded its deadline.')),
        30000,
      );
      const fail = () => {
        clearTimeout(timeout);
        reject(new DatabaseError('Native startup failed; inspect the native service log.'));
      };
      child.once('error', fail);
      child.once('exit', () => {
        if (!resolved) fail();
      });
      child.stderr!.on('data', (bytes: Buffer) => process.stderr.write(bytes));
      child.stdout!.on('data', (bytes: Buffer) => {
        process.stdout.write(bytes);
        buffered += bytes.toString();
        if (buffered.length > 65536) {
          fail();
          return;
        }
        for (;;) {
          const end = buffered.indexOf('\n');
          if (end < 0) break;
          const line = buffered.slice(0, end);
          buffered = buffered.slice(end + 1);
          try {
            if (JSON.parse(line).event === readyEvent) {
              resolved = true;
              clearTimeout(timeout);
              ready();
            }
          } catch {
            /* Native diagnostics do not establish readiness. */
          }
        }
      });
      child.once('spawn', () => {
        if (!readyEvent) {
          resolved = true;
          clearTimeout(timeout);
          ready();
        }
      });
    });
  }
  try {
    const local = await credentials(directory);
    check();
    let environment: NodeJS.ProcessEnv;
    if (mode === 'external') {
      environment = nativeEnvironment();
      pool = new pg.Pool(externalDatabase(process.env));
      releaseSchema = await holdSchema(pool);
      check();
    } else {
      const database = await startDevelopmentDatabase(directory, dbPort, local.password);
      pool = database.pool;
      stopDatabase = database.stop;
      check();
      await schema(pool, 'apply');
      const exists = await pool.query('SELECT 1 FROM pg_roles WHERE rolname=$1', [
        'moria_native_local',
      ]);
      if (!exists.rowCount) {
        const ddl = (
          await pool.query(
            "SELECT format('CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS PASSWORD %L',$1::text,$2::text)",
            ['moria_native_local', local.nativePassword],
          )
        ).rows[0].format;
        await pool.query(ddl);
      }
      await pool.query(
        "GRANT USAGE ON SCHEMA moria TO moria_native_local; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO moria_native_local; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO moria_native_local; ALTER ROLE moria_native_local SET temp_file_limit='256MB'",
      );
      environment = {
        ...process.env,
        MORIA_INDEXING_DATABASE_SECRET_REF: 'MORIA_NATIVE_LOCAL_DATABASE',
        MORIA_NATIVE_LOCAL_DATABASE: `postgresql://moria_native_local:${encodeURIComponent(local.nativePassword)}@127.0.0.1:${dbPort}/postgres`,
        MORIA_INDEXING_DATABASE_TLS: 'disable',
      };
    }
    const binding: { environment: string; application: string } = JSON.parse(
      environment.MORIA_NATIVE_BROWSER_BINDING ??
        '{"environment":"local-workspace","application":"local-ui"}',
    );
    if (role === 'combined') {
      const catalog = new Catalog(pool, 'verify'),
        authority = new Authority(catalog);
      const management =
        binding.environment === 'local-workspace' && binding.application === 'local-ui';
      if (!management && !environment.MORIA_NATIVE_APP_HOST)
        throw new DatabaseError(
          'A custom application requires an explicit published native frontend host.',
        );
      await catalog.ensureEnvironment(binding.environment);
      const cell =
        environment.MORIA_NATIVE_CELL_ID ??
        (
          await pool.query('SELECT cell FROM moria.runtime_placements WHERE environment=$1', [
            binding.environment,
          ])
        ).rows[0]?.cell ??
        randomUUID();
      if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(cell))
        throw new DatabaseError('Invalid native cell configuration.');
      environment.MORIA_NATIVE_CELL_ID = cell;
      pool.options.options = `-c moria.cell_id=${cell}`;
      await pool.query("SELECT set_config('moria.cell_id',$1,false)", [cell]);
      if (
        !(
          await pool.query('SELECT 1 FROM moria.runtime_placements WHERE environment=$1', [
            binding.environment,
          ])
        ).rowCount
      ) {
        if (
          (
            await pool.query(
              'SELECT EXISTS(SELECT 1 FROM moria.source_generations WHERE environment=$1) OR EXISTS(SELECT 1 FROM moria.runtime_executions WHERE environment=$1) AS occupied',
              [binding.environment],
            )
          ).rows[0].occupied
        )
          throw new DatabaseError(
            'Existing operational data requires explicit native cell registration.',
          );
        await pool.query(
          'INSERT INTO moria.runtime_cells(id,endpoint_ref,incarnation) SELECT $1,$2,incarnation FROM moria.indexing_instance ON CONFLICT DO NOTHING',
          [cell, 'MORIA_NATIVE_LOCAL_API'],
        );
        await pool.query(
          'INSERT INTO moria.runtime_placements(environment,cell,incarnation,serving,leases_fenced) SELECT $1,$2,incarnation,true,true FROM moria.indexing_instance',
          [binding.environment, cell],
        );
      }
      const operations = [
        'instance:manage',
        'functions:invoke',
        ...filesystemOperations.map((value) => `filesystem:${value}`),
      ];
      const principal = environment.MORIA_NATIVE_RECOVERY_PRINCIPAL ?? 'operator';
      if (
        !management &&
        !(
          await pool.query(
            'SELECT 1 FROM moria.principals p JOIN moria.application_scopes a ON a.environment=p.environment AND a.application=$3 JOIN moria.permissions g ON g.environment=p.environment AND g.principal=p.principal AND g.application=a.application WHERE p.environment=$1 AND p.principal=$2',
            [binding.environment, principal, binding.application],
          )
        ).rowCount
      )
        throw new DatabaseError(
          'Custom application bootstrap requires existing operator-approved principal, application and grants.',
        );
      if (
        !(
          await pool.query('SELECT 1 FROM moria.principals WHERE environment=$1 AND principal=$2', [
            binding.environment,
            principal,
          ])
        ).rowCount
      )
        await authority.syncPrincipal(binding.environment, 'local-bootstrap', principal, {
          kind: 'workload',
          enabled: true,
          markings: ['workspace'],
          attributes: { admin: true, region: 'West' },
        });
      if (
        !(
          await pool.query(
            'SELECT 1 FROM moria.application_scopes WHERE environment=$1 AND application=$2',
            [binding.environment, binding.application],
          )
        ).rowCount
      )
        await authority.setApplication(
          binding.environment,
          'local-bootstrap',
          binding.application,
          operations,
        );
      if (
        !(
          await pool.query(
            'SELECT 1 FROM moria.permissions WHERE environment=$1 AND principal=$2 AND application=$3',
            [binding.environment, principal, binding.application],
          )
        ).rowCount
      )
        await authority.setGrants(
          binding.environment,
          'local-bootstrap',
          principal,
          binding.application,
          operations,
        );
      const origin = `http://127.0.0.1:${port}`,
        authOrigin = `http://127.0.0.1:${authPort}`;
      const authConfig = await loadAuthConfig(directory, 'management');
      const filesystemRoot = resolve(directory, 'filesystem-warehouse');
      await mkdir(filesystemRoot, { recursive: true, mode: 0o700 });
      const sessions = new BrowserSessions(
        authority,
        {
          ...binding,
          origin,
          authenticationOrigin: authOrigin,
          entryPath: '/',
          initialOperations: management ? operations : [],
          recoveryPrincipal: principal,
          initialPrincipal: management
            ? { attributes: { admin: true, region: 'West' }, markings: ['workspace'] }
            : { attributes: {}, markings: [] },
        },
        authConfig.managementProviders,
        authConfig.key,
      );
      await sessions.initialize();
      stopIdentityRefresh.push(sessions.startAcceptedRefresh());
      const auth = new AppAuth(sessions, local.accessKey);
      authentication = createHttpServer(
        browserProfile('management', local.accessKey, auth),
        async (request, response) => {
          if (!(await auth.handle(request, response)))
            sendJson(response, 404, {
              error: { code: 'NOT_FOUND', message: 'Endpoint not found.' },
            });
        },
      );
      await new Promise<void>((ready, reject) => {
        authentication!.once('error', reject);
        authentication!.listen(authPort, '127.0.0.1', ready);
      });
      const host = localAppHost(directory);
      await bootstrapManagement(host);
      check();
      environment = {
        ...environment,
        MORIA_NATIVE_LISTEN: `127.0.0.1:${port}`,
        MORIA_NATIVE_BROWSER_BINDING: JSON.stringify(binding),
        MORIA_NATIVE_AUTH_ORIGIN: authOrigin,
        MORIA_NATIVE_AUTH_PROVIDERS: JSON.stringify(
          authConfig.managementProviders.map(({ id, label }) => ({ id, label })),
        ),
        MORIA_NATIVE_RECOVERY_PRINCIPAL: principal,
        MORIA_NATIVE_CLI_KEY: local.accessKey,
        MORIA_NATIVE_CLI_PROFILES: JSON.stringify({ operator: { ...binding, principal } }),
        MORIA_NATIVE_APP_HOST:
          environment.MORIA_NATIVE_APP_HOST ??
          JSON.stringify({
            directory: host.root,
            app: 'management',
            entryPath: '/',
            ...binding,
            publicShell: true,
          }),
      };
      if (
        environment.MORIA_FILESYSTEM_BACKEND === 's3' &&
        !environment.MORIA_NATIVE_STORAGE_BINDINGS
      )
        throw new DatabaseError(
          'Native S3 filesystem hosting requires explicit MORIA_NATIVE_STORAGE_BINDINGS secret references.',
        );
      environment.MORIA_NATIVE_STORAGE_BINDINGS ??= JSON.stringify([
        {
          namespace: {
            warehouseId: 'filesystem',
            tableId: 'content',
            revision: environment.MORIA_NATIVE_FILESYSTEM_STORAGE_REVISION ?? 'v1',
          },
          provider: {
            kind: 'posix',
            root: filesystemRoot,
            limits: { maxObjectBytes: 64 * 1024 * 1024, chunkBytes: 1024 * 1024 },
          },
        },
      ]);
      environment.MORIA_NATIVE_FILESYSTEM_STORAGE_REVISION ??= 'v1';
      await promisify(execFile)(path, ['filesystem', 'configure-storage'], {
        env: environment,
        timeout: 30000,
        maxBuffer: 65536,
      });
      const publications = publicHosting(process.env);
      if (publications.length) {
        const publication = publications[0];
        const publicAuthOrigin = process.env.MORIA_AUTH_PUBLIC_ORIGIN;
        if (!publicAuthOrigin)
          throw new DatabaseError(
            'Public native hosting requires MORIA_AUTH_PUBLIC_ORIGIN for the separate bounded OIDC listener.',
          );
        const publicAuthPort = integer(process.env, 'MORIA_AUTH_PUBLIC_PORT', 3213, 1024, 65535);
        if (
          new Set([
            port,
            authPort,
            publication.port,
            publicAuthPort,
            ...(mode === 'development' ? [dbPort] : []),
          ]).size !== (mode === 'development' ? 5 : 4)
        )
          throw new DatabaseError('Listener ports must differ.');
        const publicSessions = new BrowserSessions(
          authority,
          {
            ...sessions.binding,
            origin: publication.origin,
            authenticationOrigin: publicAuthOrigin,
          },
          authConfig.managementProviders,
          authConfig.key,
        );
        const publicAuth = new AppAuth(publicSessions, '', false);
        const listener = createHttpServer(
          browserProfile('management', '', publicAuth, publicAuthOrigin),
          async (request, response) => {
            if (!(await publicAuth.handle(request, response)))
              sendJson(response, 404, {
                error: { code: 'NOT_FOUND', message: 'Endpoint not found.' },
              });
          },
        );
        publicAuthentications.push(listener);
        await new Promise<void>((ready, reject) => {
          listener.once('error', reject);
          listener.listen(publicAuthPort, '127.0.0.1', ready);
        });
        publicEnvironment = {
          ...environment,
          MORIA_NATIVE_LISTEN: `127.0.0.1:${publication.port}`,
          MORIA_NATIVE_PUBLIC_ORIGIN: publication.origin,
          MORIA_NATIVE_AUTH_ORIGIN: publicAuthOrigin,
        };
        delete publicEnvironment.MORIA_NATIVE_CLI_KEY;
        delete publicEnvironment.MORIA_NATIVE_CLI_PROFILES;
        delete publicEnvironment.MORIA_NATIVE_RECOVERY_PRINCIPAL;
      }
    } else {
      environment.MORIA_NATIVE_LISTEN ??= `127.0.0.1:${port}`;
    }
    await launch(['serve'], environment, 'serving');
    if (publicEnvironment) await launch(['serve'], publicEnvironment, 'serving');
    check();
    if (role === 'combined') {
      await launch(['execution', 'scheduler'], environment);
      const workers: unknown = JSON.parse(environment.MORIA_NATIVE_WORKERS ?? '[]');
      if (
        !Array.isArray(workers) ||
        workers.length > domains.length ||
        new Set(workers).size !== workers.length
      )
        throw new DatabaseError(
          'MORIA_NATIVE_WORKERS must be a distinct bounded native domain list.',
        );
      for (const value of workers) await launch(workerArgs(domain(String(value))), environment);
    }
    console.log(`Moria native API: http://127.0.0.1:${port}/`);
    console.log('Management recovery: pnpm moria auth recover --app management');
    finishStartup();
    return { close };
  } catch (error) {
    finishStartup();
    await close();
    throw error;
  } finally {
    finishStartup();
  }
}
