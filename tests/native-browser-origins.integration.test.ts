import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { spawn, type ChildProcess } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import pg from 'pg';
import test from 'node:test';
import { database } from './fixtures/catalog-storage/database.js';
import { Authority } from '../services/security/src/authority.js';
import { BrowserSessions } from '../services/security/src/browser-sessions.js';
import { assertNativeArtifacts } from './fixtures/native-common/artifact.js';
test(
  'browser origin ownership is canonical, immutable and checked before native exposure across replicas and restart',
  { timeout: 60000 },
  async (t) => {
    const binary = process.env.MORIA_RUNTIME_BINARY ?? '/usr/local/bin/moria-runtime';
    await assertNativeArtifacts(binary, []);
    const db = await database(),
      children: ChildProcess[] = [],
      exits: Promise<unknown>[] = [];
    t.after(async () => {
      for (const child of children) child.kill('SIGKILL');
      await Promise.allSettled(exits);
      await db.close();
    });
    const environment = randomUUID(),
      management = 'management-' + randomUUID(),
      other = 'lower-' + randomUUID();
    await db.catalog.ensureEnvironment(environment);
    const authority = new Authority(db.catalog);
    for (const app of [management, other])
      await authority.setApplication(environment, 'fixture', app, []);
    const admit = (origin: string, app: string) =>
      db.pool.query('SELECT moria.native_browser_origin_admit($1,$2,$3) AS origin', [
        origin,
        environment,
        app,
      ]);
    for (const [input, expected] of [
      ['HTTPS://ExAmPlE.TeSt:00443/', 'https://example.test'],
      ['http://EXAMPLE.test:00080', 'http://example.test'],
      ['http://[0:0:0:0:0:0:0:1]:80', 'http://[::1]'],
    ])
      assert.equal((await admit(input, management)).rows[0].origin, expected);
    await assert.rejects(admit('https://example.test:443', other), /BROWSER_ORIGIN_RESERVED/);
    for (const invalid of [
      'https://user@example.test',
      'https://example.test/api',
      'https://example.test?x=1',
      'https://example.test#fragment',
      'http://example.test:0',
      'http://example.test:65536',
    ])
      await assert.rejects(admit(invalid, management), /BROWSER_ORIGIN_INVALID/);
    await assert.rejects(
      db.pool.query('UPDATE moria.native_browser_origins SET application=$2 WHERE origin=$1', [
        'https://example.test',
        other,
      ]),
      /BROWSER_ORIGIN_RESERVED/,
    );
    await assert.rejects(
      db.pool.query('DELETE FROM moria.native_browser_origins WHERE origin=$1', [
        'https://example.test',
      ]),
      /BROWSER_ORIGIN_RESERVED/,
    );
    const role = 'origin_' + randomUUID().replaceAll('-', ''),
      password = randomUUID();
    await db.pool.query(
      `CREATE ROLE ${role} LOGIN PASSWORD '${password}';GRANT USAGE ON SCHEMA moria TO ${role};GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role};GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    const client = new pg.Client({
      host: '127.0.0.1',
      port: db.pool.options.port,
      user: role,
      password,
      database: 'postgres',
    });
    await client.connect();
    try {
      await assert.rejects(
        client.query(
          'INSERT INTO moria.native_browser_origins(origin,environment,application) VALUES($1,$2,$3)',
          ['https://forged.test', environment, other],
        ),
        /BROWSER_ORIGIN_RESERVED/,
      );
      await assert.rejects(
        client.query('UPDATE moria.native_browser_origins SET application=$2 WHERE origin=$1', [
          'https://example.test',
          other,
        ]),
        /BROWSER_ORIGIN_RESERVED/,
      );
    } finally {
      await client.end();
    }
    const connection = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`,
      origin = 'https://origin-' + randomUUID() + '.test';
    const start = (app: string, publicOrigin = origin) => {
      const child = spawn(binary, ['serve'], {
        env: {
          PATH: process.env.PATH,
          MORIA_INDEXING_DATABASE_SECRET_REF: 'ORIGIN_DATABASE',
          ORIGIN_DATABASE: connection,
          MORIA_INDEXING_DATABASE_TLS: 'disable',
          MORIA_NATIVE_LISTEN: '127.0.0.1:0',
          MORIA_NATIVE_PUBLIC_ORIGIN: publicOrigin,
          MORIA_NATIVE_BROWSER_BINDING: JSON.stringify({ environment, application: app }),
        },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      children.push(child);
      let out = '',
        error = '';
      child.stdout!.on('data', (bytes) => (out += bytes));
      child.stderr!.on('data', (bytes) => (error += bytes));
      const exited = new Promise<number | null>((resolve) => child.once('exit', resolve));
      exits.push(exited);
      return { child, exited, out: () => out, error: () => error };
    };
    const ready = async (process: ReturnType<typeof start>) => {
      for (let i = 0; i < 1000; i++) {
        const line = process
          .out()
          .split('\n')
          .find((line) => line.includes('"event":"serving"'));
        if (line) return JSON.parse(line).address as string;
        if (process.child.exitCode !== null) throw new Error(process.error());
        await delay(10);
      }
      throw new Error('Native origin startup timed out');
    };
    const first = start(management);
    await ready(first);
    const owner = (
      await db.pool.query('SELECT * FROM moria.native_browser_origins WHERE origin=$1', [origin])
    ).rows[0];
    const replica = start(
      management,
      origin.toUpperCase().replace('HTTPS://', 'https://') + ':443',
    );
    await ready(replica);
    assert.deepEqual(
      (await db.pool.query('SELECT * FROM moria.native_browser_origins WHERE origin=$1', [origin]))
        .rows[0],
      owner,
    );
    const rejected = start(other);
    assert.equal(await rejected.exited, 1);
    assert.match(rejected.error(), /BROWSER_ORIGIN_RESERVED/);
    assert(!rejected.out().includes('"event":"serving"'));
    first.child.kill('SIGKILL');
    replica.child.kill('SIGKILL');
    await Promise.all([first.exited, replica.exited]);
    const restartOther = start(other);
    assert.equal(await restartOther.exited, 1);
    assert.match(restartOther.error(), /BROWSER_ORIGIN_RESERVED/);
    const restarted = start(management);
    await ready(restarted);
    const separate = start(other, origin + '-independent');
    await ready(separate);
    assert.deepEqual(
      (await db.pool.query('SELECT * FROM moria.native_browser_origins WHERE origin=$1', [origin]))
        .rows[0],
      owner,
    );
    for (const child of children) if (child.exitCode === null) child.kill('SIGKILL');
    await Promise.allSettled(exits);
    const sessions = new BrowserSessions(
      authority,
      {
        environment,
        application: other,
        origin,
        entryPath: '/',
        initialOperations: [],
        recoveryPrincipal: 'owner',
      },
      [],
      Buffer.alloc(32),
    );
    await assert.rejects(
      sessions.initialize(),
      (error) => error instanceof Error && error.message.includes('browser origin'),
    );
    const issuer = new BrowserSessions(
      authority,
      {
        environment,
        application: management,
        origin: 'https://callback-owned.test:3210',
        authenticationOrigin: 'https://callback-owned.test:3211',
        entryPath: '/',
        initialOperations: [],
        recoveryPrincipal: 'owner',
      },
      [],
      Buffer.alloc(32),
    );
    await issuer.initialize();
    await assert.rejects(
      admit('https://callback-owned.test:3211', other),
      /BROWSER_ORIGIN_RESERVED/,
    );
  },
);
