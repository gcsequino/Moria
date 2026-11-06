import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomUUID, createHash, randomBytes } from 'node:crypto';
import { request as httpRequest, type ClientRequest } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { Authority } from '../services/security/src/authority.js';
import { BrowserSessions } from '../services/security/src/browser-sessions.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { database } from './fixtures/catalog-storage/database.js';
import { commitProxy } from './fixtures/indexing/commit-proxy.js';
import { assertNativeArtifacts } from './fixtures/native-common/artifact.js';

test(
  'native HTTP admission bounds pending bodies across shared and component routes',
  { timeout: 90000 },
  async (t) => {
    const runtime = process.env.MORIA_NATIVE_BINARY ?? '/usr/local/bin/moria-runtime';
    await assertNativeArtifacts(runtime, []);
    const db = await database(),
      environment = randomUUID(),
      authority = new Authority(db.catalog);
    await db.catalog.ensureEnvironment(environment);
    await authority.setApplication(environment, 'fixture', 'http-client', []);
    for (const principal of ['http-user', 'cli-user']) {
      await authority.syncPrincipal(environment, 'fixture', principal, {
        enabled: true,
        kind: 'workload',
        attributes: {},
        markings: [],
      });
      await authority.setGrants(environment, 'fixture', principal, 'http-client', []);
    }
    const { token } = await new WorkloadSessions(authority).mint(
      { environment, principal: 'http-user', application: 'http-client' },
      'fixture',
    );
    const role = `http_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role};GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role};GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    const cellA = randomUUID(),
      cellB = randomUUID();
    const cliKey = randomBytes(32).toString('base64url');
    const serverEnv = {
      PATH: process.env.PATH,
      MORIA_INDEXING_DATABASE_SECRET_REF: 'HTTP_DATABASE',
      HTTP_DATABASE: `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`,
      MORIA_INDEXING_DATABASE_TLS: 'disable',
      MORIA_NATIVE_CELL_ID: cellA,
      MORIA_NATIVE_BROWSER_BINDING: JSON.stringify({ environment, application: 'http-client' }),
      MORIA_NATIVE_CLI_KEY: cliKey,
      MORIA_NATIVE_CLI_PROFILES: JSON.stringify({
        operator: { environment, principal: 'cli-user', application: 'http-client' },
        approval: { environment, principal: 'http-user', application: 'http-client' },
      }),
      MORIA_NATIVE_DRAIN_SECONDS: '2',
    };
    const child = spawn(runtime, ['serve'], {
      env: serverEnv,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    const requests: ClientRequest[] = [];
    let output = '',
      error = '';
    child.stdout.on('data', (bytes) => (output += bytes));
    child.stderr.on('data', (bytes) => (error = (error + bytes).slice(-65536)));
    const exited = new Promise((resolve) => child.once('exit', resolve));
    t.after(async () => {
      for (const request of requests) request.destroy();
      child.kill('SIGTERM');
      await exited;
      await db.close();
    });
    let address = '';
    for (let attempt = 0; attempt < 500 && !address; attempt++) {
      for (const line of output.split('\n')) {
        try {
          const event = JSON.parse(line);
          if (event.event === 'serving') address = event.address;
        } catch {}
      }
      if (child.exitCode !== null) throw new Error(`Native listener failed: ${error}`);
      if (!address) await delay(10);
    }
    assert.ok(address);
    const authRequest = async (headers: Record<string, string>, method = 'POST') => {
      const response = await fetch(
        `http://${address}/api/v1/objects/unused/${method === 'GET' ? '00000000-0000-4000-8000-000000000001' : 'query'}`,
        {
          method,
          headers: { 'content-type': 'application/json', ...headers },
          ...(method === 'POST' ? { body: '{}' } : {}),
        },
      );
      return { status: response.status, value: (await response.json()) as any };
    };
    await t.test('local CLI profiles use real revocable workload sessions', async () => {
      const headers = { 'x-moria-key': cliKey, 'x-moria-profile': 'operator' };
      const count = async () =>
        Number(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.workload_sessions WHERE environment=$1',
              [environment],
            )
          ).rows[0].count,
        );
      const before = await count();
      assert.equal((await authRequest(headers)).status, 404);
      assert.equal((await authRequest(headers)).status, 404);
      assert.equal(
        await count(),
        before + 1,
        'cache only the issued credential, never current authority',
      );
      assert.equal((await authRequest({ ...headers, 'x-moria-key': randomUUID() })).status, 401);
      assert.equal(
        (await authRequest({ ...headers, 'x-moria-profile': 'unregistered' })).status,
        401,
      );
      assert.equal((await authRequest({ ...headers, origin: `http://${address}` })).status, 403);
      assert.equal(
        (await authRequest({ ...headers, authorization: `Bearer ${token}` })).status,
        403,
      );
      await authority.syncPrincipal(environment, 'fixture', 'cli-user', {
        enabled: false,
        kind: 'workload',
        attributes: {},
        markings: [],
      });
      try {
        const revoked = await authRequest(headers);
        assert.equal(revoked.status, 401, 'disabled principal permanently revokes its credential');
        assert.equal(revoked.value.error.code, 'UNAUTHENTICATED');
        assert.equal(await count(), before + 1, 'revocation cannot silently mint a replacement');
      } finally {
        await authority.syncPrincipal(environment, 'fixture', 'cli-user', {
          enabled: true,
          kind: 'workload',
          attributes: {},
          markings: [],
        });
      }
      assert.equal(
        (await authRequest(headers)).status,
        401,
        'reenabling cannot revive the credential',
      );
      assert.equal(await count(), before + 1, 'reenabling cannot silently issue a replacement');
      assert.equal((await authRequest({ authorization: `Bearer ${token}` })).status, 404);
    });
    await t.test(
      'a missed CLI approval disable pulse permanently fences its issued session',
      async () => {
        const headers = { 'x-moria-key': cliKey, 'x-moria-profile': 'approval' };
        assert.equal((await authRequest(headers)).status, 404);
        const before = (
          await db.pool.query('SELECT count(*) FROM moria.workload_sessions WHERE environment=$1', [
            environment,
          ])
        ).rows[0].count;
        await db.pool.query(
          'BEGIN; UPDATE moria.native_cli_profile_approvals SET enabled=false; UPDATE moria.native_cli_profile_approvals SET enabled=true; COMMIT',
        );
        assert.equal((await authRequest(headers)).status, 403);
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.workload_sessions WHERE environment=$1',
              [environment],
            )
          ).rows[0].count,
          before,
        );
      },
    );
    await t.test('app-bound browser cookies enforce exact origin and current session', async () => {
      const browser = new BrowserSessions(
        authority,
        {
          environment,
          application: 'http-client',
          origin: `http://${address}`,
          entryPath: '/',
          initialOperations: [],
          recoveryPrincipal: 'http-user',
        },
        [],
        randomBytes(32),
      );
      const cookieToken = await browser.recover(await browser.recoveryTicket());
      const name = `moria_${createHash('sha256').update(`http://${address}/http-client`).digest('hex').slice(0, 16)}`;
      const cookie = `${name}=${cookieToken}`,
        origin = `http://${address}`;
      assert.equal((await authRequest({ cookie, origin })).status, 404);
      assert.equal((await authRequest({ cookie }, 'GET')).status, 404);
      assert.equal((await authRequest({ cookie })).status, 403);
      assert.equal((await authRequest({ cookie, origin: 'http://foreign.invalid' })).status, 403);
      assert.equal(
        (await authRequest({ cookie, origin, 'sec-fetch-site': 'cross-site' })).status,
        403,
      );
      assert.equal((await authRequest({ cookie: `${cookie};${cookie}`, origin })).status, 401);
      assert.equal(
        (await authRequest({ cookie: `${name}=${token}`, origin })).status,
        401,
        'a workload bearer is not a browser session',
      );
      assert.equal(
        (await authRequest({ cookie, origin, authorization: `Bearer ${token}` })).status,
        401,
      );
      await browser.logout(cookieToken);
      assert.equal((await authRequest({ cookie, origin })).status, 401);
    });
    await t.test(
      'managed cells serialize ownership, fence transfer and fail closed on a real DB partition',
      async (t) => {
        const incarnation = (await db.pool.query('SELECT incarnation FROM moria.indexing_instance'))
          .rows[0].incarnation;
        await db.pool.query(
          "INSERT INTO moria.runtime_cells(id,endpoint_ref,incarnation) VALUES($1,'CELL_A_ENDPOINT',$3),($2,'CELL_B_ENDPOINT',$3)",
          [cellA, cellB, incarnation],
        );
        const connection = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
        const directory = (command: string, input: unknown, url = connection) => {
          const child = spawn(runtime, ['directory', command], {
            env: {
              PATH: process.env.PATH,
              MORIA_INDEXING_DATABASE_SECRET_REF: 'DIRECTORY_DATABASE',
              DIRECTORY_DATABASE: url,
              MORIA_INDEXING_DATABASE_TLS: 'disable',
            },
            stdio: ['pipe', 'pipe', 'pipe'],
          });
          let output = '',
            error = '';
          child.stdout.on('data', (b) => (output += b));
          child.stderr.on('data', (b) => (error = (error + b).slice(-65536)));
          child.stdin.end(JSON.stringify(input));
          const done = new Promise<any>((resolve, reject) => {
            child.once('error', reject);
            child.once('exit', (code) =>
              code === 0 ? resolve(JSON.parse(output)) : reject(new Error(error)),
            );
          });
          void done.catch(() => {});
          t.after(() => {
            if (child.exitCode === null) child.kill('SIGKILL');
          });
          return { child, done };
        };
        const bearer = { authorization: `Bearer ${token}` };
        const initial = await directory('place', { environment, cell: cellA }).done;
        assert.equal(initial.serving, false);
        assert.equal(initial.leasesFenced, true);
        assert.equal((await authRequest(bearer)).status, 409);
        await directory('activate', { environment, owner: initial.owner }).done;
        assert.equal((await authRequest(bearer)).status, 404);
        const blocker = await db.pool.connect();
        await blocker.query('BEGIN');
        t.after(async () => {
          await blocker.query('ROLLBACK');
          blocker.release();
        });
        await blocker.query("SELECT set_config('moria.cell_id',$1,true)", [cellA]);
        await blocker.query('SELECT moria.native_serving_session($1)', [
          createHash('sha256').update(token).digest('hex'),
        ]);
        const moving = directory('freeze', { environment, owner: initial.owner, next: cellB });
        let blocked = false;
        for (let attempt = 0; attempt < 300 && !blocked; attempt++) {
          blocked = (
            await db.pool.query(
              "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE usename=$1 AND wait_event_type='Lock' AND query LIKE 'SELECT moria.runtime_environment_freeze%') AS waiting",
              [role],
            )
          ).rows[0].waiting;
          if (!blocked) await delay(10);
        }
        assert.equal(blocked, true, 'freeze waits for a real admitted authority transaction');
        await blocker.query('COMMIT');
        const moved = await moving.done;
        assert.equal(moved.owner.fence, '2');
        assert.equal(moved.serving, false);
        assert.equal(moved.leasesFenced, true);
        assert.equal((await authRequest(bearer)).status, 409);
        await directory('activate', { environment, owner: moved.owner }).done;
        assert.equal(
          (await authRequest(bearer)).status,
          409,
          'old native cell cannot release even after activation',
        );
        const partition = await commitProxy(db.pool.options.port!, 'never_hold_this_function');
        let partitionClosed = false;
        t.after(async () => {
          if (!partitionClosed) await partition.close();
        });
        const serverB = spawn(runtime, ['serve'], {
          env: {
            PATH: process.env.PATH,
            MORIA_INDEXING_DATABASE_SECRET_REF: 'CELL_DATABASE',
            CELL_DATABASE: connection.replace(`:${db.pool.options.port}/`, `:${partition.port}/`),
            MORIA_INDEXING_DATABASE_TLS: 'disable',
            MORIA_NATIVE_CELL_ID: cellB,
          },
          stdio: ['ignore', 'pipe', 'pipe'],
        });
        let outputB = '',
          errorsB = '';
        serverB.stdout.on('data', (b) => (outputB += b));
        serverB.stderr.on('data', (b) => (errorsB = (errorsB + b).slice(-65536)));
        const stoppedB = new Promise((resolve) => serverB.once('exit', resolve));
        t.after(async () => {
          serverB.kill('SIGKILL');
          await stoppedB;
        });
        let addressB = '';
        for (let attempt = 0; attempt < 500 && !addressB; attempt++) {
          for (const line of outputB.split('\n')) {
            try {
              const event = JSON.parse(line);
              if (event.event === 'serving') addressB = event.address;
            } catch {}
          }
          if (serverB.exitCode !== null) throw new Error(errorsB);
          if (!addressB) await delay(10);
        }
        assert.ok(addressB);
        const requestB = async () => {
          const r = await fetch(`http://${addressB}/api/v1/objects/unused/query`, {
            method: 'POST',
            headers: { ...bearer, 'content-type': 'application/json' },
            body: '{}',
          });
          return { status: r.status, value: (await r.json()) as any };
        };
        assert.equal(
          (await requestB()).status,
          404,
          'actual new-cell process accepts current authority',
        );
        await partition.close();
        partitionClosed = true;
        const unavailable = await requestB();
        assert.notEqual(unavailable.status, 404);
        assert.ok(
          unavailable.status >= 500,
          'a disconnected authority cannot reuse a prior successful check',
        );
        const lost = await commitProxy(db.pool.options.port!, 'moria.runtime_environment_freeze');
        t.after(lost.close);
        const pending = directory(
          'freeze',
          { environment, owner: moved.owner, next: cellA },
          connection.replace(`:${db.pool.options.port}/`, `:${lost.port}/`),
        );
        await lost.commit;
        pending.child.kill('SIGKILL');
        await assert.rejects(pending.done);
        const frozen = await directory('inspect', { environment }).done;
        assert.equal(frozen.owner.fence, '3');
        assert.equal(frozen.serving, false);
        assert.equal(
          frozen.leasesFenced,
          false,
          'lost first-phase ACK cannot activate an unfenced move',
        );
        await assert.rejects(directory('activate', { environment, owner: frozen.owner }).done);
        const recovered = await directory('freeze', {
          environment,
          owner: moved.owner,
          next: cellA,
        }).done;
        assert.equal(recovered.owner.fence, '3');
        assert.equal(
          recovered.leasesFenced,
          true,
          'same transfer resumes fencing without another ownership move',
        );
        await directory('activate', { environment, owner: recovered.owner }).done;
        assert.equal((await authRequest(bearer)).status, 404);
        await assert.rejects(
          directory('freeze', { environment, owner: initial.owner, next: cellB }).done,
        );
        assert.equal(
          (
            await db.pool.query(
              "SELECT count(*) FROM moria.runtime_ownership_events WHERE kind='environment' AND resource=$1",
              [environment],
            )
          ).rows[0].count,
          '2',
        );
      },
    );
    const hold = async (path: string) =>
      new Promise<ClientRequest>((resolve, reject) => {
        const request = httpRequest(`http://${address}${path}`, {
          method: 'POST',
          agent: false,
          headers: {
            authorization: `Bearer ${token}`,
            'content-type': 'application/json',
            'content-length': '2',
            expect: '100-continue',
            'idempotency-key': randomUUID(),
          },
        });
        requests.push(request);
        request.on('error', () => {});
        request.once('continue', () => resolve(request));
        request.once('response', (response) => {
          response.resume();
          reject(new Error(`Pending body unexpectedly returned ${response.statusCode}`));
        });
        request.flushHeaders();
      });
    const post = async (path: string) => {
      const response = await fetch(`http://${address}${path}`, {
        method: 'POST',
        headers: {
          authorization: `Bearer ${token}`,
          'content-type': 'application/json',
          'idempotency-key': randomUUID(),
        },
        body: '{}',
      });
      return { status: response.status, value: (await response.json()) as any };
    };
    const release = async (held: ClientRequest[]) => {
      for (const request of held) request.destroy();
      await delay(50);
    };
    const object = '/api/v1/objects/unused/query',
      model = '/api/v1/models/unused/chat',
      source = '/api/management/v1/fs/%73ources/00000000-0000-4000-8000-000000000001/publish';
    await t.test('four compiler requests hold their slots through an incomplete body', async () => {
      const held = [];
      for (let count = 0; count < 4; count++) held.push(await hold(source));
      const rejected = await post(source);
      assert.equal(rejected.status, 429);
      assert.equal(rejected.value.error.code, 'RESOURCE_LIMIT');
      assert.notEqual(
        (await post(object)).status,
        429,
        'compiler class must not monopolize unrelated capacity',
      );
      await release(held);
      assert.notEqual(
        (await post(source)).status,
        429,
        'client cancellation releases compiler slots',
      );
    });
    await t.test('model admission retains its separate32 request budget', async () => {
      const held = [];
      for (let count = 0; count < 32; count++) held.push(await hold(model));
      const rejected = await post(model);
      assert.equal(rejected.status, 429);
      assert.equal(rejected.value.error, 'RESOURCE_LIMIT');
      assert.notEqual((await post(object)).status, 429);
      await release(held);
      assert.notEqual((await post(model)).status, 429);
    });
    await t.test(
      'the shared64 request budget rejects additional work before buffering',
      async () => {
        const held = [];
        for (let count = 0; count < 64; count++) held.push(await hold(object));
        const rejected = await post(object);
        assert.equal(rejected.status, 429);
        assert.equal(rejected.value.error.code, 'RESOURCE_LIMIT');
        const modelRejected = await post(model);
        assert.equal(modelRejected.status, 429);
        assert.equal(modelRejected.value.error, 'RESOURCE_LIMIT');
        await release(held);
        assert.notEqual((await post(object)).status, 429);
      },
    );
    await t.test(
      'readiness is current, metrics require instance authority and disabled CLI approval survives restart',
      async () => {
        const response = await fetch(`http://${address}/livez`);
        assert.equal(response.status, 200);
        assert.equal((await response.json()).live, true);
        assert.equal((await fetch(`http://${address}/readyz`)).status, 200);
        assert.equal(
          (
            await fetch(`http://${address}/livez`, {
              headers: { origin: 'https://untrusted.invalid' },
            })
          ).status,
          403,
        );
        const metrics = () =>
          fetch(`http://${address}/api/management/v1/runtime`, {
            headers: { authorization: `Bearer ${token}` },
          });
        assert.equal((await metrics()).status, 403);
        await authority.setApplication(environment, 'fixture', 'http-client', ['instance:manage']);
        await authority.setGrants(environment, 'fixture', 'http-user', 'http-client', [
          'instance:manage',
        ]);
        const permitted = await metrics();
        assert.equal(permitted.status, 200);
        const value = await permitted.json();
        assert.equal(value.accepting, true);
        assert.match(value.requestsStarted, /^[0-9]+$/);
        assert.equal(value.latencyBucketCounts.length, 8);
        await db.pool.query('UPDATE moria.runtime_recovery_control SET dispatch_enabled=false');
        try {
          assert.equal((await fetch(`http://${address}/readyz`)).status, 503);
          assert.equal((await fetch(`http://${address}/livez`)).status, 200);
        } finally {
          await db.pool.query('UPDATE moria.runtime_recovery_control SET dispatch_enabled=true');
        }
        await db.pool.query('UPDATE moria.native_cli_profile_approvals SET enabled=false');
        const restarted = spawn(runtime, ['serve'], {
          env: serverEnv,
          stdio: ['ignore', 'pipe', 'pipe'],
        });
        let restartedOut = '',
          restartedError = '';
        restarted.stdout.on('data', (b) => (restartedOut += b));
        restarted.stderr.on('data', (b) => (restartedError = (restartedError + b).slice(-65536)));
        const restartedExit = new Promise((resolve) => restarted.once('exit', resolve));
        t.after(async () => {
          restarted.kill('SIGKILL');
          await restartedExit;
        });
        let next = '';
        for (let i = 0; i < 500 && !next; i++) {
          for (const line of restartedOut.split('\n')) {
            try {
              const event = JSON.parse(line);
              if (event.event === 'serving') next = event.address;
            } catch {}
          }
          if (restarted.exitCode !== null) throw new Error(restartedError);
          if (!next) await delay(10);
        }
        assert.ok(next);
        const denied = await fetch(`http://${next}/api/v1/objects/unused/query`, {
          method: 'POST',
          headers: {
            'x-moria-key': cliKey,
            'x-moria-profile': 'operator',
            'content-type': 'application/json',
          },
          body: '{}',
        });
        assert.equal(denied.status, 403);
        assert.equal(
          (await db.pool.query('SELECT enabled FROM moria.native_cli_profile_approvals')).rows[0]
            .enabled,
          false,
        );
        restarted.kill('SIGTERM');
        assert.equal(await restartedExit, 0);
      },
    );
    await t.test('SIGTERM bounds drain while an actual admitted body is unfinished', async () => {
      const pending = await hold(object);
      const started = Date.now();
      child.kill('SIGTERM');
      assert.equal(await exited, 1);
      assert.ok(Date.now() - started < 10000);
      assert.match(error, /NATIVE_DRAIN_TIMEOUT/);
      pending.destroy();
    });
  },
);
