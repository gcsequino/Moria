import assert from 'node:assert/strict';
import test from 'node:test';
import { randomUUID, randomBytes, createHash } from 'node:crypto';
import { database } from './fixtures/catalog-storage/database.js';
import { oidcFixture } from './fixtures/oidc.js';
import { Authority } from '../services/security/src/authority.js';
import { BrowserSessions, digest } from '../services/security/src/browser-sessions.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';

test(
  'OIDC restart and missed disable pulses cannot revive ordinary credentials, accepted origins or pending flows',
  { timeout: 90000 },
  async (t) => {
    const db = await database(),
      issuer = await oidcFixture();
    t.after(async () => {
      await issuer.close();
      await db.close();
    });
    const environment = randomUUID(),
      application = 'browser-revocation',
      authority = new Authority(db.catalog),
      key = randomBytes(32);
    await db.catalog.ensureEnvironment(environment);
    await authority.setApplication(environment, 'fixture', application, ['records:read']);
    const binding = {
      environment,
      application,
      origin: 'http://127.0.0.1:32911',
      entryPath: '/',
      initialOperations: ['records:read'],
      recoveryPrincipal: 'unused',
    };
    const providers = [
      {
        id: 'test',
        label: 'Test',
        issuer: issuer.issuer,
        clientId: 'test-client',
        clientSecret: 'test-secret',
        emails: ['snow@example.test'],
      },
    ];
    const configured = () => new BrowserSessions(authority, { ...binding }, providers, key, true);
    let sessions = configured();
    await sessions.initialize();
    const authorize = async () => {
      const started = await sessions.start('test');
      const response = await fetch(started.location, { redirect: 'manual' });
      assert.equal(response.status, 302);
      return { url: new URL(response.headers.get('location')!), browser: started.browser };
    };
    const login = async () => {
      const flow = await authorize();
      const token = await sessions.finish(flow.url, flow.browser);
      const context = await sessions.authenticate(`Bearer ${token}`);
      assert.equal(
        (
          await db.pool.query('SELECT moria.native_serving_session($1)->>$2 AS principal', [
            digest(token),
            'principal',
          ])
        ).rows[0].principal,
        context.principal,
      );
      return { token, context, hash: digest(token) };
    };
    const first = await login();
    const spec = {
      version: 1,
      domain: 'transform',
      release: 'a'.repeat(64),
      resourceClass: 'standard',
      timeoutSeconds: 120,
      retryPolicy: 'idempotent',
      inputSchema: { type: 'object', properties: {} },
      pinnedInputs: [],
      expectedHeads: {},
    };
    const registration = (
      await db.pool.query(
        `INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation) SELECT $1,$2,$3,$4,ARRAY['records:read'],ARRAY[]::text[],e.security_epoch,i.security_epoch,c.incarnation FROM moria.environments e,moria.instance_state i,moria.indexing_instance c WHERE e.id=$1 RETURNING id`,
        [environment, randomUUID(), spec, first.context],
      )
    ).rows[0].id;
    const execution = (
      await db.pool.query('SELECT moria.runtime_execution_request($1,$2,$3,$4,$5) AS id', [
        registration,
        randomUUID(),
        {},
        createHash('sha256').update('{}').digest('hex'),
        JSON.stringify([{}]),
      ])
    ).rows[0].id;
    const proof = (
      await db.pool.query('SELECT moria.runtime_job_delegation_guard($1) AS proof', [execution])
    ).rows[0].proof;
    assert.equal(
      (
        await db.pool.query(
          'SELECT moria.native_serving_accepted_delegation($1)->>$2 AS principal',
          [proof, 'principal'],
        )
      ).rows[0].principal,
      first.context.principal,
    );
    await db.pool.query(
      'UPDATE moria.auth_providers SET enabled=false WHERE environment=$1 AND application=$2',
      [environment, application],
    );
    sessions = configured();
    await sessions.initialize();
    assert.equal(
      (
        await db.pool.query(
          'SELECT enabled FROM moria.auth_providers WHERE environment=$1 AND application=$2',
          [environment, application],
        )
      ).rows[0].enabled,
      false,
    );
    assert.deepEqual(await sessions.providerOptions(), []);
    await assert.rejects(sessions.start('test'));
    await db.pool.query(
      'UPDATE moria.auth_providers SET enabled=true WHERE environment=$1 AND application=$2',
      [environment, application],
    );
    await assert.rejects(sessions.authenticate(`Bearer ${first.token}`));
    await assert.rejects(
      db.pool.query('SELECT moria.native_serving_accepted_delegation($1)', [proof]),
      /UNAUTHENTICATED/,
    );
    assert.deepEqual(
      (
        await db.pool.query(
          'SELECT disabled,access_token FROM moria.browser_sessions WHERE token_hash=$1',
          [first.hash],
        )
      ).rows[0],
      { disabled: true, access_token: null },
    );
    await assert.rejects(
      db.pool.query('UPDATE moria.browser_sessions SET disabled=false WHERE token_hash=$1', [
        first.hash,
      ]),
      /UNAUTHENTICATED/,
    );
    const second = await login(),
      pending = await authorize();
    const pulse = await db.pool.connect();
    try {
      await pulse.query('BEGIN');
      await pulse.query(
        'UPDATE moria.auth_providers SET enabled=false WHERE environment=$1 AND application=$2',
        [environment, application],
      );
      await pulse.query(
        'UPDATE moria.auth_providers SET enabled=true WHERE environment=$1 AND application=$2',
        [environment, application],
      );
      await pulse.query('COMMIT');
    } catch (error) {
      await pulse.query('ROLLBACK');
      throw error;
    } finally {
      pulse.release();
    }
    await assert.rejects(sessions.authenticate(`Bearer ${second.token}`));
    await assert.rejects(sessions.finish(pending.url, pending.browser));
    sessions = configured();
    await sessions.initialize();
    const third = await login();
    await sessions.initialize();
    assert.equal(
      (await sessions.authenticate(`Bearer ${third.token}`)).principal,
      third.context.principal,
      'unchanged enabled-provider restart must preserve a live session',
    );
    const workload = 'worker';
    await authority.syncPrincipal(environment, 'fixture', workload, {
      kind: 'workload',
      enabled: true,
      attributes: {},
      markings: [],
    });
    await authority.setGrants(environment, 'fixture', workload, application, ['records:read']);
    const workloads = new WorkloadSessions(authority),
      work = (await workloads.mint({ environment, principal: workload, application }, 'fixture'))
        .token;
    await db.pool.query(
      'UPDATE moria.application_scopes SET enabled=false WHERE environment=$1 AND application=$2',
      [environment, application],
    );
    await db.pool.query(
      'UPDATE moria.application_scopes SET enabled=true WHERE environment=$1 AND application=$2',
      [environment, application],
    );
    await assert.rejects(sessions.authenticate(`Bearer ${third.token}`));
    await assert.rejects(workloads.authenticate(`Bearer ${work}`));
    const fourth = await login();
    await db.pool.query(
      'UPDATE moria.principals SET enabled=false WHERE environment=$1 AND principal=$2',
      [environment, fourth.context.principal],
    );
    await db.pool.query(
      'UPDATE moria.principals SET enabled=true WHERE environment=$1 AND principal=$2',
      [environment, fourth.context.principal],
    );
    await assert.rejects(sessions.authenticate(`Bearer ${fourth.token}`));
    const fifth = await login();
    const instance = randomUUID();
    await db.pool.query("INSERT INTO moria.instance_principals(id,kind) VALUES($1,'human')", [
      instance,
    ]);
    await db.pool.query(
      'INSERT INTO moria.instance_principal_bindings(environment,principal,instance_id) VALUES($1,$2,$3)',
      [environment, fifth.context.principal, instance],
    );
    await db.pool.query('UPDATE moria.instance_principals SET enabled=false WHERE id=$1', [
      instance,
    ]);
    await db.pool.query('UPDATE moria.instance_principals SET enabled=true WHERE id=$1', [
      instance,
    ]);
    await assert.rejects(sessions.authenticate(`Bearer ${fifth.token}`));
    const final = await login();
    assert.equal(final.context.principal, first.context.principal);
    const freshWork = (
      await workloads.mint({ environment, principal: workload, application }, 'fixture')
    ).token;
    assert.equal((await workloads.authenticate(`Bearer ${freshWork}`)).principal, workload);
  },
);
