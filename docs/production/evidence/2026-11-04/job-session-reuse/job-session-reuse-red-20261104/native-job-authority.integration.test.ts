import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import test from 'node:test';
import type { PoolClient } from 'pg';
import { database } from './fixtures/catalog-storage/database.js';
import { Authority } from '../services/security/src/authority.js';
import { setTimeout as delay } from 'node:timers/promises';

test(
  'registered service jobs require immutable purpose and a current registered lease',
  { timeout: 120000 },
  async (t) => {
    const db = await database();
    t.after(db.close);
    const environment = randomUUID(),
      resource = randomUUID(),
      service = randomUUID();
    const principal = 'registered-pipeline',
      application = 'registered-projector',
      operations = ['source:read', 'target:project'];
    const authority = new Authority(db.catalog);
    await db.catalog.ensureEnvironment(environment);
    await db.pool.query(
      "UPDATE moria.runtime_resource_classes SET maximum_tasks=64,maximum_environment_tasks=64 WHERE name='standard'",
    );
    await authority.syncPrincipal(environment, 'fixture', principal, {
      enabled: true,
      kind: 'workload',
      attributes: {},
      markings: ['private'],
    });
    await authority.setApplication(environment, 'fixture', application, operations);
    await authority.setGrants(environment, 'fixture', principal, application, operations);
    await db.pool.query(
      `INSERT INTO moria.runtime_service_authorities(id,environment,principal,application,operations,markings,domains,resource_classes,incarnation)
    SELECT $1,$2,$3,$4,$5,ARRAY['private'],ARRAY['transform'],ARRAY['standard'],incarnation FROM moria.indexing_instance`,
      [service, environment, principal, application, operations],
    );
    await db.pool.query(
      `INSERT INTO moria.runtime_service_targets(environment,resource,domain,binding_revision,resource_class,authority) VALUES($1,$2,'transform','release-1','standard',$3)`,
      [environment, resource, service],
    );
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
    const context = {
      environment,
      principal,
      application,
      serviceAuthority: service,
      serviceTargetRevision: 'release-1',
    };
    const binding = async (changes: Record<string, unknown> = {}, registered = true) => {
      const id = (
        await db.pool.query(
          `INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation)
      SELECT $1,$2,$3,$4,$5,ARRAY['private'],e.security_epoch,i.security_epoch,c.incarnation FROM moria.environments e,moria.instance_state i,moria.indexing_instance c WHERE e.id=$1 RETURNING id`,
          [environment, resource, { ...spec, ...changes }, context, operations],
        )
      ).rows[0].id;
      if (registered)
        await db.pool.query(
          `INSERT INTO moria.runtime_execution_services(binding,authority,target_revision) VALUES($1,$2,'release-1')`,
          [id, service],
        );
      return id;
    };
    const role = `job_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(
      `CREATE ROLE ${role} LOGIN PASSWORD '${password}'; GRANT USAGE ON SCHEMA moria TO ${role}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role}; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    const connection = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const native = (command: string, input: unknown, extra: Record<string, string> = {}) =>
      new Promise<any>((resolve, reject) => {
        const child = spawn(
          process.env.MORIA_RUNTIME_BINARY ??
            process.env.MORIA_INDEXING_BINARY ??
            '/usr/local/bin/moria-indexing',
          ['execution', command],
          {
            env: {
              PATH: process.env.PATH,
              MORIA_INDEXING_DATABASE_SECRET_REF: 'JOB_DATABASE',
              JOB_DATABASE: connection,
              MORIA_INDEXING_DATABASE_TLS: 'disable',
              MORIA_EXECUTION_WORKER_OWNER: owner,
              MORIA_EXECUTION_HOST_ID: host,
              ...extra,
            },
            stdio: ['pipe', 'pipe', 'pipe'],
          },
        );
        let output = '',
          error = '';
        child.stdout.on('data', (bytes) => (output += bytes));
        child.stderr.on('data', (bytes) => (error += bytes));
        child.stdin.on('error', () => {});
        child.stdin.end(JSON.stringify(input));
        child.once('error', reject);
        child.once('exit', (code) =>
          code === 0 ? resolve(JSON.parse(output)) : reject(new Error(error)),
        );
        t.after(() => child.kill('SIGKILL'));
      });
    const request = async (bindingId: string) =>
      native('request', {
        binding: bindingId,
        requestKey: randomUUID(),
        input: {},
        tasks: [{ key: 'only', partition: 0 }],
      });
    const registered = await binding(),
      owner = randomUUID(),
      host = randomUUID();
    await db.pool.query(
      `INSERT INTO moria.runtime_workers(owner,host_id,incarnation,resource_classes,domains,expires_at) SELECT $1,$2,incarnation,ARRAY['standard'],ARRAY['transform'],clock_timestamp()+interval '1 hour' FROM moria.indexing_instance`,
      [owner, host],
    );
    const issue = async (
      lease: any,
      workerHost = host,
      client: typeof db.pool | PoolClient = db.pool,
    ) =>
      (
        await client.query('SELECT moria.runtime_job_session_issue($1,$2,$3,$4,$5,$6,$7) AS hash', [
          createHash('sha256').update(randomUUID()).digest('hex'),
          lease.execution,
          lease.task,
          lease.owner,
          lease.fence,
          workerHost,
          lease.incarnation,
        ])
      ).rows[0].hash as string;
    const state = async (hash: string) =>
      (await db.pool.query('SELECT moria.native_serving_session($1) AS state', [hash])).rows[0]
        .state;
    const job = await request(registered);
    let lease: any,
      hash = '';
    await t.test(
      'registered service admission has no ordinary session and cannot mint before an actual lease',
      async () => {
        const delegation = (
          await db.pool.query('SELECT * FROM moria.runtime_job_delegations WHERE execution=$1', [
            job.id,
          ])
        ).rows[0];
        assert.equal(delegation.origin_kind, 'service');
        assert.equal(delegation.origin_session_hash, null);
        assert.equal(delegation.origin_binding, registered);
        assert.equal(delegation.service_authority, service);
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.workload_sessions WHERE environment=$1 AND principal=$2',
              [environment, principal],
            )
          ).rows[0].count,
          '0',
        );
        const task = (
          await db.pool.query('SELECT id FROM moria.runtime_execution_tasks WHERE execution=$1', [
            job.id,
          ])
        ).rows[0].id;
        const incarnation = (await db.pool.query('SELECT incarnation FROM moria.indexing_instance'))
          .rows[0].incarnation;
        await assert.rejects(
          issue({ execution: job.id, task, owner, fence: '1', incarnation }),
          /TASK_LEASE_LOST|JOB_LEASE_LOST|EXECUTION_INACTIVE/,
        );
        const claimed = await native('worker-claim', { domains: ['transform'], target: job.id });
        lease = claimed.lease;
        assert.equal(lease.execution, job.id);
        hash = await issue(lease);
        const current = await state(hash);
        assert.equal(current.principal, principal);
        assert.deepEqual(current.identity.markings, ['private']);
        assert.deepEqual([...current.identity.grantedOperations].sort(), [...operations].sort());
        assert.equal(
          await issue(lease),
          hash,
          'same actual lease recovers its existing credential',
        );
        await assert.rejects(issue(lease, randomUUID()), /FUNCTION_WORKER_NOT_AUTHORIZED/);
        await assert.rejects(
          db.pool.query('SELECT moria.native_serving_session_origin($1,false)', [hash]),
          /UNAUTHENTICATED/,
        );
      },
    );
    await t.test(
      'unregistered legacy bindings and mismatched purposes cannot acquire a service capability',
      { skip: 'Existing-session reuse red regression only' },
      async () => {
        const unregistered = await binding({}, false),
          accepted = await request(unregistered);
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.runtime_job_delegations WHERE execution=$1',
              [accepted.id],
            )
          ).rows[0].count,
          '0',
        );
        await native('cancel', { id: accepted.id });
        await assert.rejects(binding({ domain: 'sync' }), /SERVICE_PURPOSE_REVOKED/);
        await assert.rejects(binding({ resourceClass: 'models' }), /SERVICE_PURPOSE_REVOKED/);
        await assert.rejects(
          db.pool.query(
            "UPDATE moria.runtime_service_authorities SET operations=ARRAY['*'] WHERE id=$1",
            [service],
          ),
          /IMMUTABLE_SERVICE_PURPOSE/,
        );
      },
    );
    await t.test(
      'private service descendants remain inside the exact registered target and parent capability',
      async () => {
        const childBinding = async (target: string) =>
          (
            await db.pool.query(
              `INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation)
      SELECT $1,$2,$3,$4,$5,ARRAY['private'],e.security_epoch,i.security_epoch,c.incarnation FROM moria.environments e,moria.instance_state i,moria.indexing_instance c WHERE e.id=$1 RETURNING id`,
              [
                environment,
                target,
                spec,
                { environment, principal, application, sessionHash: hash, parentExecution: job.id },
                operations,
              ],
            )
          ).rows[0].id;
        await assert.rejects(request(await childBinding(randomUUID())), /SERVICE_PURPOSE_REVOKED/);
        const child = await request(await childBinding(resource));
        const accepted = (
          await db.pool.query('SELECT * FROM moria.runtime_job_delegations WHERE execution=$1', [
            child.id,
          ])
        ).rows[0];
        assert.equal(accepted.origin_kind, 'service');
        assert.equal(accepted.parent_execution, job.id);
        assert.equal(accepted.origin_binding, registered);
        let childLease = (
          await native('worker-claim', { domains: ['transform'], target: child.id })
        ).lease;
        const underSharedAncestors = async (check: (client: PoolClient) => Promise<void>) => {
          const blocker = await db.pool.connect(),
            verifier = await db.pool.connect();
          try {
            await blocker.query('BEGIN');
            await blocker.query(
              'SELECT id FROM moria.runtime_executions WHERE id=ANY($1::uuid[]) ORDER BY id FOR SHARE',
              [[job.id, child.id]],
            );
            await verifier.query('BEGIN');
            await verifier.query("SET LOCAL lock_timeout='500ms'");
            await check(verifier);
          } finally {
            await verifier.query('ROLLBACK');
            await blocker.query('ROLLBACK');
            verifier.release();
            blocker.release();
          }
        };
        for (const phase of ['task', 'publication']) {
          if (phase === 'publication') {
            await native('seal', {
              lease: childLease,
              result: { staged: true },
              digest: 'b'.repeat(64),
            });
            childLease = (
              await native('worker-claim', {
                domains: ['transform'],
                publication: true,
                target: child.id,
              })
            ).lease;
            assert.equal(childLease.task, null);
          }
          await underSharedAncestors(async (client) => {
            await assert.rejects(
              issue(childLease, host, client),
              (error: any) => error.code === '55P03',
            );
          });
          const credential = await issue(childLease);
          await underSharedAncestors(async (client) => {
            assert.equal(await issue(childLease, host, client), credential);
          });
          assert.equal((await state(credential)).principal, principal);
        }
        await native('cancel', { id: child.id });
      },
    );
    await t.test(
      'a generic operator claim cannot mint a registered private worker credential',
      { skip: 'Existing-session reuse red regression only' },
      async () => {
        const next = await request(await binding());
        const lease = (await native('claim', { owner, domains: ['transform'] })).lease;
        assert.equal(lease.execution, next.id);
        await assert.rejects(issue(lease), /JOB_LEASE_LOST/);
        assert.equal(
          (
            await db.pool.query(
              'SELECT worker_generation FROM moria.runtime_execution_tasks WHERE id=$1',
              [lease.task],
            )
          ).rows[0].worker_generation,
          null,
        );
        await native('cancel', { id: next.id });
      },
    );
    await t.test(
      'missed registry disable pulses cannot be repaired by acknowledging current authorization epochs',
      { skip: 'Existing-session reuse red regression only' },
      async () => {
        for (const table of ['runtime_service_targets', 'runtime_service_authorities']) {
          const pinned = await binding();
          const accepted = await request(pinned);
          const current = (
            await native('worker-claim', { domains: ['transform'], target: accepted.id })
          ).lease;
          assert.equal(current.execution, accepted.id);
          const privateHash = await issue(current);
          const before = (
            await db.pool.query(
              `SELECT generation::text FROM moria.${table} WHERE ${table === 'runtime_service_targets' ? 'authority' : 'id'}=$1`,
              [service],
            )
          ).rows[0].generation;
          await db.pool.query(
            `UPDATE moria.${table} SET enabled=false WHERE ${table === 'runtime_service_targets' ? 'authority' : 'id'}=$1`,
            [service],
          );
          await db.pool.query(
            `UPDATE moria.${table} SET enabled=true WHERE ${table === 'runtime_service_targets' ? 'authority' : 'id'}=$1`,
            [service],
          );
          assert.equal(
            BigInt(
              (
                await db.pool.query(
                  `SELECT generation::text FROM moria.${table} WHERE ${table === 'runtime_service_targets' ? 'authority' : 'id'}=$1`,
                  [service],
                )
              ).rows[0].generation,
            ),
            BigInt(before) + 2n,
          );
          await assert.rejects(state(privateHash), /SERVICE_PURPOSE_REVOKED/);
          await assert.rejects(
            db.pool.query(
              'SELECT moria.runtime_binding_authority($1,e.security_epoch,i.security_epoch,$2) FROM moria.environments e,moria.instance_state i WHERE e.id=$3',
              [pinned, accepted.id, environment],
            ),
            /SERVICE_PURPOSE_REVOKED/,
          );
          await assert.rejects(
            db.pool.query(
              `UPDATE moria.${table} SET generation=generation+1 WHERE ${table === 'runtime_service_targets' ? 'authority' : 'id'}=$1`,
              [service],
            ),
            /IMMUTABLE_SERVICE_PURPOSE/,
          );
        }
      },
    );
    await t.test(
      'current target, application, grants, clearance, worker scope and actor disable revoke private authority',
      { skip: 'Existing-session reuse red regression only' },
      async () => {
        const fresh = async () => {
          const next = await request(await binding());
          lease = (await native('worker-claim', { domains: ['transform'], target: next.id })).lease;
          assert.equal(lease.execution, next.id);
          hash = await issue(lease);
          assert.equal((await state(hash)).principal, principal);
        };
        await db.pool.query(
          'UPDATE moria.runtime_service_targets SET enabled=false WHERE authority=$1',
          [service],
        );
        await assert.rejects(state(hash), /SERVICE_PURPOSE_REVOKED/);
        await db.pool.query(
          'UPDATE moria.runtime_service_targets SET enabled=true WHERE authority=$1',
          [service],
        );
        await assert.rejects(state(hash), /SERVICE_PURPOSE_REVOKED/);
        await fresh();
        await db.pool.query(
          'UPDATE moria.runtime_service_authorities SET enabled=false WHERE id=$1',
          [service],
        );
        await assert.rejects(state(hash), /SERVICE_PURPOSE_REVOKED/);
        await db.pool.query(
          'UPDATE moria.runtime_service_authorities SET enabled=true WHERE id=$1',
          [service],
        );
        await assert.rejects(state(hash), /SERVICE_PURPOSE_REVOKED/);
        await fresh();
        await authority.setGrants(environment, 'fixture', principal, application, ['source:read']);
        await assert.rejects(state(hash), /SERVICE_PURPOSE_REVOKED|AUTHORIZATION_CHANGED/);
        await authority.setGrants(environment, 'fixture', principal, application, operations);
        await assert.rejects(state(hash), /AUTHORIZATION_CHANGED|EXECUTION_AUTHORITY_CHANGED/);
        await fresh();
        await authority.setApplication(environment, 'fixture', application, ['source:read']);
        await assert.rejects(state(hash), /SERVICE_PURPOSE_REVOKED|AUTHORIZATION_CHANGED/);
        await authority.setApplication(environment, 'fixture', application, operations);
        await assert.rejects(state(hash), /AUTHORIZATION_CHANGED|EXECUTION_AUTHORITY_CHANGED/);
        await fresh();
        await authority.syncPrincipal(environment, 'fixture', principal, {
          enabled: true,
          kind: 'workload',
          attributes: {},
          markings: [],
        });
        await assert.rejects(state(hash), /SERVICE_PURPOSE_REVOKED|AUTHORIZATION_CHANGED/);
        await authority.syncPrincipal(environment, 'fixture', principal, {
          enabled: true,
          kind: 'workload',
          attributes: {},
          markings: ['private'],
        });
        await assert.rejects(state(hash), /AUTHORIZATION_CHANGED|EXECUTION_AUTHORITY_CHANGED/);
        await fresh();
        await db.pool.query(
          "UPDATE moria.runtime_workers SET domains=ARRAY['sync'] WHERE owner=$1",
          [owner],
        );
        await assert.rejects(state(hash), /FUNCTION_WORKER_NOT_AUTHORIZED/);
        await db.pool.query(
          "UPDATE moria.runtime_workers SET domains=ARRAY['transform'] WHERE owner=$1",
          [owner],
        );
        await assert.rejects(state(hash), /JOB_LEASE_LOST/);
        await fresh();
        await authority.syncPrincipal(environment, 'fixture', principal, {
          enabled: false,
          kind: 'workload',
          attributes: {},
          markings: ['private'],
        });
        await assert.rejects(state(hash), /SERVICE_PURPOSE_REVOKED/);
        await authority.syncPrincipal(environment, 'fixture', principal, {
          enabled: true,
          kind: 'workload',
          attributes: {},
          markings: ['private'],
        });
        await assert.rejects(state(hash), /SERVICE_PURPOSE_REVOKED/);
        assert.equal(
          (
            await db.pool.query(
              'SELECT enabled FROM moria.runtime_service_authorities WHERE id=$1',
              [service],
            )
          ).rows[0].enabled,
          false,
        );
        await db.pool.query(
          'UPDATE moria.runtime_service_authorities SET enabled=true WHERE id=$1',
          [service],
        );
        await assert.rejects(state(hash), /SERVICE_PURPOSE_REVOKED/);
        await assert.rejects(native('cancel', { id: job.id }), /AUTHORIZATION_CHANGED/);
      },
    );
    await t.test(
      'missed principal, application and instance disable pulses require explicit fresh service reapproval',
      { skip: 'Existing-session reuse red regression only' },
      async () => {
        const identity = randomUUID(),
          replacement = randomUUID();
        await db.pool.query(
          "INSERT INTO moria.instance_principals(id,kind) VALUES($1,'workload'),($2,'workload')",
          [identity, replacement],
        );
        await db.pool.query(
          'INSERT INTO moria.instance_principal_bindings(environment,principal,instance_id) VALUES($1,$2,$3)',
          [environment, principal, identity],
        );
        await db.pool.query(
          'UPDATE moria.runtime_service_authorities SET enabled=true WHERE id=$1',
          [service],
        );
        for (const kind of ['principal', 'application', 'instance', 'binding']) {
          const registered = await binding(),
            accepted = await request(registered);
          const original = (
            await native('worker-claim', { domains: ['transform'], target: accepted.id })
          ).lease;
          const oldHash = await issue(original);
          assert.equal((await state(oldHash)).principal, principal);
          if (kind === 'principal') {
            await db.pool.query(
              'UPDATE moria.principals SET enabled=false WHERE environment=$1 AND principal=$2',
              [environment, principal],
            );
            await db.pool.query(
              'UPDATE moria.principals SET enabled=true WHERE environment=$1 AND principal=$2',
              [environment, principal],
            );
          } else if (kind === 'application') {
            await db.pool.query(
              'UPDATE moria.application_scopes SET enabled=false WHERE environment=$1 AND application=$2',
              [environment, application],
            );
            await db.pool.query(
              'UPDATE moria.application_scopes SET enabled=true WHERE environment=$1 AND application=$2',
              [environment, application],
            );
          } else if (kind === 'instance') {
            await db.pool.query('UPDATE moria.instance_principals SET enabled=false WHERE id=$1', [
              identity,
            ]);
            await db.pool.query('UPDATE moria.instance_principals SET enabled=true WHERE id=$1', [
              identity,
            ]);
          } else {
            await db.pool.query(
              'UPDATE moria.instance_principal_bindings SET instance_id=$3 WHERE environment=$1 AND principal=$2',
              [environment, principal, replacement],
            );
            await db.pool.query(
              'UPDATE moria.instance_principal_bindings SET instance_id=$3 WHERE environment=$1 AND principal=$2',
              [environment, principal, identity],
            );
          }
          assert.equal(
            (
              await db.pool.query(
                'SELECT enabled FROM moria.runtime_service_authorities WHERE id=$1',
                [service],
              )
            ).rows[0].enabled,
            false,
            kind,
          );
          await assert.rejects(state(oldHash), /SERVICE_PURPOSE_REVOKED/);
          await assert.rejects(binding(), /SERVICE_PURPOSE_REVOKED/);
          await db.pool.query(
            'UPDATE moria.runtime_service_authorities SET enabled=true WHERE id=$1',
            [service],
          );
          await assert.rejects(state(oldHash), /SERVICE_PURPOSE_REVOKED/);
          const fresh = await request(await binding());
          const renewed = (
            await native('worker-claim', { domains: ['transform'], target: fresh.id })
          ).lease;
          assert.equal((await state(await issue(renewed))).principal, principal);
        }
      },
    );
    await t.test(
      'an unobserved worker disable and restore cannot revive a live lease or private credential',
      { skip: 'Existing-session reuse red regression only' },
      async () => {
        const accepted = await request(await binding());
        const lease = (
          await native('worker-claim', { domains: ['transform'], target: accepted.id })
        ).lease;
        assert.equal(lease.execution, accepted.id);
        const credential = await issue(lease);
        assert.equal((await state(credential)).principal, principal);
        const before = BigInt(
          (
            await db.pool.query('SELECT generation FROM moria.runtime_workers WHERE owner=$1', [
              owner,
            ])
          ).rows[0].generation,
        );
        await db.pool.query('UPDATE moria.runtime_workers SET enabled=false WHERE owner=$1', [
          owner,
        ]);
        await db.pool.query('UPDATE moria.runtime_workers SET enabled=true WHERE owner=$1', [
          owner,
        ]);
        const after = BigInt(
          (
            await db.pool.query('SELECT generation FROM moria.runtime_workers WHERE owner=$1', [
              owner,
            ])
          ).rows[0].generation,
        );
        assert.equal(after, before + 2n);
        assert.equal(
          (
            await db.pool.query(
              'SELECT lease_until>clock_timestamp() AS live FROM moria.runtime_execution_tasks WHERE id=$1',
              [lease.task],
            )
          ).rows[0].live,
          true,
        );
        await assert.rejects(state(credential), /JOB_LEASE_LOST/);
        await assert.rejects(issue(lease), /LEASE_LOST|JOB_LEASE_LOST/);
        await assert.rejects(native('renew', lease), /LEASE_LOST/);
        await native('cancel', { id: accepted.id });
      },
    );
    await t.test(
      'a genuinely expired worker registration remains fenced after its expiry is extended',
      { skip: 'Existing-session reuse red regression only' },
      async () => {
        const owner2 = randomUUID(),
          host2 = randomUUID();
        await db.pool.query(
          "INSERT INTO moria.runtime_workers(owner,host_id,incarnation,resource_classes,domains,expires_at) SELECT $1,$2,incarnation,ARRAY['standard'],ARRAY['transform'],clock_timestamp()+interval '2 seconds' FROM moria.indexing_instance",
          [owner2, host2],
        );
        const accepted = await request(await binding());
        const lease = (
          await native(
            'worker-claim',
            { domains: ['transform'], target: accepted.id },
            { MORIA_EXECUTION_WORKER_OWNER: owner2, MORIA_EXECUTION_HOST_ID: host2 },
          )
        ).lease;
        assert.equal(lease.execution, accepted.id);
        const credential = await issue(lease, host2);
        assert.equal((await state(credential)).principal, principal);
        await delay(2200);
        assert.equal(
          (
            await db.pool.query(
              'SELECT expires_at<=clock_timestamp() AS expired FROM moria.runtime_workers WHERE owner=$1',
              [owner2],
            )
          ).rows[0].expired,
          true,
        );
        await db.pool.query(
          "UPDATE moria.runtime_workers SET expires_at=clock_timestamp()+interval '1 hour' WHERE owner=$1",
          [owner2],
        );
        assert.equal(
          (
            await db.pool.query('SELECT generation FROM moria.runtime_workers WHERE owner=$1', [
              owner2,
            ])
          ).rows[0].generation,
          '2',
        );
        assert.equal(
          (
            await db.pool.query(
              'SELECT lease_until>clock_timestamp() AS live FROM moria.runtime_execution_tasks WHERE id=$1',
              [lease.task],
            )
          ).rows[0].live,
          true,
        );
        await assert.rejects(state(credential), /JOB_LEASE_LOST/);
        await assert.rejects(issue(lease, host2), /LEASE_LOST|JOB_LEASE_LOST/);
        await native('cancel', { id: accepted.id });
      },
    );
    await t.test(
      'lease takeover invalidates the old private credential and replacement capabilities remain bounded',
      { skip: 'Existing-session reuse red regression only' },
      async () => {
        const next = await request(await binding());
        const first = (await native('worker-claim', { domains: ['transform'], target: next.id }))
          .lease;
        assert.equal(first.execution, next.id);
        const oldHash = await issue(first);
        await db.pool.query(
          "UPDATE moria.runtime_execution_tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
          [first.task],
        );
        await assert.rejects(issue(first), /JOB_LEASE_LOST/);
        const replacement = (
          await native('worker-claim', { domains: ['transform'], target: next.id })
        ).lease;
        assert.equal(replacement.task, first.task);
        assert.ok(BigInt(replacement.fence) > BigInt(first.fence));
        await assert.rejects(state(oldHash), /JOB_LEASE_LOST/);
        await assert.rejects(issue(first), /JOB_LEASE_LOST/);
        const currentHash = await issue(replacement);
        assert.notEqual(currentHash, oldHash);
        assert.equal((await state(currentHash)).principal, principal);
        await native('cancel', { id: next.id });
        await assert.rejects(state(currentHash), /JOB_LEASE_LOST|JOB_DELEGATION_INACTIVE/);
      },
    );
  },
);
