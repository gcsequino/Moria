import assert from 'node:assert/strict';
import { randomUUID, createHash, X509Certificate } from 'node:crypto';
import { execFile, spawn } from 'node:child_process';
import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { assertNativeArtifacts } from './fixtures/native-common/artifact.js';
import { database } from './fixtures/catalog-storage/database.js';

const exec = promisify(execFile);
test(
  'native gRPC requires mutual TLS and current registered service/owner authority',
  { timeout: 90000 },
  async (t) => {
    await assertNativeArtifacts(process.env.MORIA_NATIVE_BINARY ?? '/usr/local/bin/moria-runtime', [
      process.env.MORIA_RPC_TEST_BINARY ?? '/usr/local/bin/moria-rpc-tests',
    ]);
    const db = await database();
    t.after(db.close);
    const file = (name: string) => join(db.root, name);
    await exec('openssl', [
      'req',
      '-x509',
      '-newkey',
      'rsa:2048',
      '-sha256',
      '-days',
      '1',
      '-nodes',
      '-subj',
      '/CN=MoriaNativeFixtureCA',
      '-keyout',
      file('ca.key'),
      '-out',
      file('ca.pem'),
    ]);
    const sign = async (name: string, usage: string) => {
      await exec('openssl', [
        'req',
        '-newkey',
        'rsa:2048',
        '-nodes',
        '-subj',
        `/CN=${name}`,
        '-keyout',
        file(`${name}.key`),
        '-out',
        file(`${name}.csr`),
      ]);
      await writeFile(
        file(`${name}.ext`),
        `basicConstraints=CA:FALSE\nextendedKeyUsage=${usage}\n${name === 'server' ? 'subjectAltName=DNS:localhost,IP:127.0.0.1\n' : ''}`,
      );
      await exec('openssl', [
        'x509',
        '-req',
        '-in',
        file(`${name}.csr`),
        '-CA',
        file('ca.pem'),
        '-CAkey',
        file('ca.key'),
        '-CAcreateserial',
        '-out',
        file(`${name}.pem`),
        '-days',
        '1',
        '-sha256',
        '-extfile',
        file(`${name}.ext`),
      ]);
    };
    await sign('server', 'serverAuth');
    await sign('client', 'clientAuth');
    await sign('rogue', 'clientAuth');
    const fingerprint = createHash('sha256')
      .update(new X509Certificate(await readFile(file('client.pem'))).raw)
      .digest('hex');
    const incarnation = (await db.pool.query('SELECT incarnation FROM moria.indexing_instance'))
      .rows[0].incarnation;
    const environment = randomUUID();
    await db.catalog.ensureEnvironment(environment);
    const cell = randomUUID(),
      otherCell = randomUUID(),
      owner = randomUUID(),
      domain = `rpc-${randomUUID()}`;
    await db.pool.query(
      "INSERT INTO moria.runtime_cells(id,endpoint_ref,incarnation) VALUES($1,'CELL_A_ENDPOINT',$3),($2,'CELL_B_ENDPOINT',$3)",
      [cell, otherCell, incarnation],
    );
    await db.pool.query(
      'INSERT INTO moria.runtime_identity_domains(domain,cell,incarnation) VALUES($1,$2,$3)',
      [domain, cell, incarnation],
    );
    await db.pool.query(
      "INSERT INTO moria.native_service_peers(fingerprint,owner,cell,roles,incarnation,expires_at) VALUES($1,$2,$3,ARRAY['metadata','identity-owner','environment-router'],$4,clock_timestamp()+interval '1 hour')",
      [fingerprint, owner, cell, incarnation],
    );
    await db.pool.query(
      'INSERT INTO moria.runtime_placements(environment,cell,incarnation,serving,leases_fenced) VALUES($1,$2,$3,true,true)',
      [environment, cell, incarnation],
    );
    const role = `rpc_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role}; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    const connection = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const server = spawn(
      process.env.MORIA_NATIVE_BINARY ?? '/usr/local/bin/moria-runtime',
      ['rpc'],
      {
        env: {
          PATH: '/usr/local/bin:/usr/bin:/bin',
          MORIA_INDEXING_DATABASE_SECRET_REF: 'RPC_DATABASE',
          RPC_DATABASE: connection,
          MORIA_INDEXING_DATABASE_TLS: 'disable',
          MORIA_RPC_LISTEN: '127.0.0.1:0',
          MORIA_RPC_CERT_FILE: file('server.pem'),
          MORIA_RPC_KEY_FILE: file('server.key'),
          MORIA_RPC_CLIENT_CA_FILE: file('ca.pem'),
        },
        stdio: ['ignore', 'pipe', 'pipe'],
      },
    );
    let output = '',
      errors = '';
    server.stdout.on('data', (chunk) => (output += chunk.toString()));
    server.stderr.on('data', (chunk) => (errors += chunk.toString()));
    const stopped = new Promise<void>((resolve) => server.once('exit', () => resolve()));
    t.after(async () => {
      server.kill('SIGTERM');
      await Promise.race([stopped, delay(3000)]);
      if (server.exitCode === null && server.signalCode === null) server.kill('SIGKILL');
      await stopped;
    });
    for (let i = 0; i < 300 && !output.includes('rpc-listening') && server.exitCode === null; i++)
      await delay(10);
    assert.match(output, /rpc-listening/, errors);
    const listening = JSON.parse(
      output
        .trim()
        .split('\n')
        .find((line) => line.includes('rpc-listening'))!,
    );
    const fixture = file('rpc-fixture.json');
    await writeFile(
      fixture,
      JSON.stringify({
        endpoint: `https://${listening.address}`,
        ca: file('ca.pem'),
        cert: file('client.pem'),
        key: file('client.key'),
        rogueCert: file('rogue.pem'),
        rogueKey: file('rogue.key'),
        incarnation,
        cell,
        otherCell,
        domain,
        environment,
        peerFingerprint: fingerprint,
      }),
    );
    const result = await exec(
      process.env.MORIA_RPC_TEST_BINARY ?? '/usr/local/bin/moria-rpc-tests',
      ['--ignored', '--exact', 'rpc_authority', '--nocapture', '--test-threads=1'],
      {
        env: {
          ...process.env,
          MORIA_RPC_TEST_FIXTURE: fixture,
          MORIA_RPC_TEST_DATABASE: connection,
        },
        timeout: 60000,
        maxBuffer: 1024 * 1024,
      },
    );
    assert.match(result.stdout, /test result: ok\. 1 passed; 0 failed; 0 ignored;/);
    assert.equal(
      (await db.pool.query('SELECT count(*) FROM moria.identities WHERE domain=$1', [domain]))
        .rows[0].count,
      '2',
    );
  },
);
