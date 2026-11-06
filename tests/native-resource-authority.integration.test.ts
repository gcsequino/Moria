import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import pg from 'pg';
import { filesystemClient } from '../packages/platform/src/filesystem.js';
import { createTransport } from '../packages/platform/src/transport.js';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';

test(
  'credential-bound Source authority resolves its session once and retains current fences',
  { timeout: 120000 },
  async (t) => {
    const fixture = await managedRetentionDataset();
    const resources: { client?: pg.PoolClient; ordinary?: pg.Client } = {};
    t.after(async () => {
      try {
        await resources.ordinary?.end();
      } finally {
        resources.client?.release();
        await fixture.close();
      }
    });
    const { db, environment, principal, application, dataset, token } = fixture;
    const hash = createHash('sha256').update(token).digest('hex');
    const client = (resources.client = await db.pool.connect());
    const ordinary = (resources.ordinary = new pg.Client({
      connectionString: fixture.nativeEnv.RETENTION_DATABASE,
      options: `-c moria.cell_id=${fixture.cell}`,
    }));
    await ordinary.connect();
    const current = async (credential = hash) =>
      (await client.query('SELECT moria.native_serving_session($1) AS state', [credential])).rows[0]
        .state;
    const checkIdentity = (state: any, expected = { environment, principal, application }) => {
      assert.equal(state.environment, expected.environment);
      assert.equal(state.principal, expected.principal);
      assert.equal(state.application, expected.application);
    };
    const source = async (credential = hash, resource = dataset, operation = 'read') =>
      (
        await client.query('SELECT moria.native_resource_filesystem_access($1,$2,$3) AS labels', [
          credential,
          resource,
          operation,
        ])
      ).rows[0].labels;
    const grouped = async (
      credential = hash,
      resource = dataset,
      operation = 'read',
      identity = { environment, principal, application },
      connection = client,
    ) =>
      (
        await connection.query(
          'SELECT moria.native_resource_filesystem_authority($1,$2,$3,$4,$5,$6) AS result',
          [
            credential,
            identity.environment,
            identity.principal,
            identity.application,
            resource,
            operation,
          ],
        )
      ).rows[0].result;
    const finish = async (state: any, credential = hash) => {
      const latest = await current(credential);
      checkIdentity(latest);
      assert.equal(latest.securityEpoch, state.securityEpoch);
      assert.equal(latest.instanceEpoch, state.instanceEpoch);
    };
    const calls = async (run: () => Promise<unknown>) => {
      await client.query("SET track_functions='pl'");
      try {
        await client.query(
          "SELECT pg_stat_reset_single_function_counters('moria.native_serving_session(text)'::regprocedure::oid)",
        );
        await run();
        await client.query('SELECT pg_stat_force_next_flush()');
        let count = 0;
        for (let attempt = 0; attempt < 30; attempt++) {
          await client.query('SELECT pg_stat_clear_snapshot()');
          count =
            (
              await client.query(
                "SELECT calls::int FROM pg_stat_user_functions WHERE funcid='moria.native_serving_session(text)'::regprocedure::oid",
              )
            ).rows[0]?.calls ?? 0;
          if (count) break;
          await delay(10);
        }
        return count;
      } finally {
        await client.query("SET track_functions='none'");
      }
    };
    let original: any;
    await t.test(
      'actual PostgreSQL counts retain the final fresh check while reducing three resolutions to two',
      async () => {
        const oldCount = await calls(async () => {
          const state = await current();
          checkIdentity(state);
          original = { state, labels: await source() };
          await finish(state);
        });
        const groupedCount = await calls(async () => {
          const result = await grouped();
          assert.deepEqual(result, original);
          await finish(result.state);
        });
        assert.equal(oldCount, 3);
        assert.equal(groupedCount, 2);
        console.log(
          JSON.stringify({
            event: 'resource_authority_session_count',
            oldCount,
            groupedCount,
            exactStateAndLabels: true,
            freshFinishRetained: true,
          }),
        );
      },
    );
    await t.test(
      'actual private job credentials retain the same bounded reduction and exact accepted identity',
      async () => {
        const owner = randomUUID(),
          host = randomUUID();
        await client.query(
          "INSERT INTO moria.runtime_workers(owner,host_id,incarnation,resource_classes,domains,expires_at) SELECT $1,$2,incarnation,ARRAY['standard'],ARRAY['sync'],clock_timestamp()+interval '1 hour' FROM moria.indexing_instance",
          [owner, host],
        );
        const accepted = await fixture.command(['execution', 'request'], {
          binding: await fixture.bind([], {}),
          requestKey: randomUUID(),
          input: {},
          tasks: [{ key: 'only', partition: 0 }],
        });
        try {
          const { lease } = await fixture.command(
            ['execution', 'worker-claim'],
            { domains: ['sync'], target: accepted.id },
            {
              ...fixture.nativeEnv,
              MORIA_EXECUTION_WORKER_OWNER: owner,
              MORIA_EXECUTION_HOST_ID: host,
            },
          );
          const credential = (
            await client.query(
              'SELECT moria.runtime_job_session_issue($1,$2,$3,$4,$5,$6,$7) AS hash',
              [
                createHash('sha256').update(randomUUID()).digest('hex'),
                lease.execution,
                lease.task,
                lease.owner,
                lease.fence,
                host,
                lease.incarnation,
              ],
            )
          ).rows[0].hash;
          let baseline: any;
          const oldCount = await calls(async () => {
            const state = await current(credential);
            checkIdentity(state);
            assert.equal(state.privateValidatedExecution, accepted.id);
            baseline = { state, labels: await source(credential) };
            await finish(state, credential);
          });
          const groupedCount = await calls(async () => {
            const result = await grouped(credential);
            assert.deepEqual(result, baseline);
            await finish(result.state, credential);
          });
          assert.equal(oldCount, 3);
          assert.equal(groupedCount, 2);
          await client.query('UPDATE moria.runtime_execution_tasks SET fence=fence+1 WHERE id=$1', [
            lease.task,
          ]);
          await assert.rejects(
            grouped(credential, randomUUID(), 'unsupported', {
              environment,
              principal: 'another-principal',
              application,
            }),
            { code: 'P0001', message: 'JOB_LEASE_LOST' },
          );
        } finally {
          await fixture.command(['execution', 'cancel'], { id: accepted.id });
        }
      },
    );
    await t.test(
      'ordinary database roles cannot supply trusted state and exact verified identity precedes Source errors',
      async () => {
        const result = await grouped(
          hash,
          dataset,
          'read',
          { environment, principal, application },
          ordinary as any,
        );
        assert.deepEqual(result, original);
        await assert.rejects(
          ordinary.query("SELECT moria.native_resource_filesystem_access_state($1,$2,'read')", [
            original.state,
            dataset,
          ]),
          { code: '42501' },
        );
        for (const identity of [
          { environment: randomUUID(), principal, application },
          { environment, principal: 'another-principal', application },
          { environment, principal, application: 'another-application' },
        ]) {
          await assert.rejects(grouped(hash, randomUUID(), 'unsupported', identity), {
            code: 'P0001',
            message: 'AUTHORIZATION_CHANGED',
          });
        }
        await assert.rejects(grouped('0'.repeat(64), randomUUID(), 'unsupported'), {
          code: 'P0001',
          message: 'UNAUTHENTICATED',
        });
        await assert.rejects(grouped(hash, dataset, 'unsupported'), {
          code: 'P0001',
          message: 'FORBIDDEN',
        });
      },
    );
    const entry = (
      await client.query(
        'SELECT e.id FROM moria.fs_entries e WHERE e.environment_id=$1 AND e.resource_id=$2',
        [environment, dataset],
      )
    ).rows[0].id;
    const filesystem = filesystemClient(
      createTransport({
        baseUrl: fixture.baseUrl,
        fetch: (url, init) =>
          fetch(url, { ...init, headers: { ...init?.headers, authorization: 'Bearer ' + token } }),
      }),
    );
    const parent = await filesystem.get(fixture.published.project.entry.id);
    const created = await filesystem.changes(
      [
        {
          op: 'create',
          parentId: parent.id,
          expectedParentRevision: parent.revision,
          name: 'incomplete-negative-lineage',
          kind: 'directory',
        },
      ],
      { idempotencyKey: randomUUID() },
    );
    const unproved = created.entries.find(
      (entry) => entry.parentId === parent.id && entry.name === 'incomplete-negative-lineage',
    )!;
    const bothReject = async (message: string) => {
      await assert.rejects(source(), { code: 'P0001', message });
      await assert.rejects(grouped(), { code: 'P0001', message });
    };
    await t.test(
      'deleted resources and missing immutable Source proofs retain their original ordered error class',
      async () => {
        await client.query('BEGIN');
        try {
          await client.query('UPDATE moria.fs_entries SET deleted=true WHERE id=$1', [entry]);
          for (const check of [source, grouped]) {
            await client.query('SAVEPOINT expected_denial');
            await assert.rejects(check(), { code: 'P0001', message: 'NOT_FOUND' });
            await client.query('ROLLBACK TO SAVEPOINT expected_denial');
          }
        } finally {
          await client.query('ROLLBACK');
        }
        await client.query('BEGIN');
        try {
          // Deliberately incomplete lineage is confined to a rolled-back negative fixture.
          await client.query(
            "INSERT INTO moria.fs_resource_bindings(environment,reference,project_id,entry_id,root_id,digest) SELECT environment,'missing-proof',project_id,$1,root_id,digest FROM moria.fs_resource_bindings WHERE entry_id=$2",
            [unproved.id, entry],
          );
          await client.query('INSERT INTO moria.fs_provenance(entry_id,source_id) VALUES($1,$2)', [
            entry,
            unproved.id,
          ]);
          for (const check of [source, grouped]) {
            await client.query('SAVEPOINT expected_denial');
            await assert.rejects(check(), { code: 'P0001', message: 'SOURCE_FILE_PROOF_REQUIRED' });
            await client.query('ROLLBACK TO SAVEPOINT expected_denial');
          }
        } finally {
          await client.query('ROLLBACK');
        }
      },
    );
    await t.test(
      'a policy deny committed between statements is rejected by the next fresh grouped check',
      async () => {
        const state = await grouped();
        const policy = (
          await client.query(
            'SELECT to_jsonb(p) AS value FROM moria.fs_policies p WHERE entry_id=$1',
            [entry],
          )
        ).rows[0]?.value;
        const instance = state.state.instanceId;
        try {
          await client.query(
            "INSERT INTO moria.fs_policies(entry_id,revision,markings,retained_markings,grants) VALUES($1,1,'[]','[]',$2) ON CONFLICT(entry_id) DO UPDATE SET grants=EXCLUDED.grants",
            [
              entry,
              JSON.stringify([
                {
                  subject: { kind: 'principal', id: instance },
                  operations: ['read'],
                  inherit: false,
                  effect: 'deny',
                },
              ]),
            ],
          );
          await bothReject('FORBIDDEN');
        } finally {
          if (policy)
            await client.query('UPDATE moria.fs_policies SET grants=$2 WHERE entry_id=$1', [
              entry,
              JSON.stringify(policy.grants),
            ]);
          else await client.query('DELETE FROM moria.fs_policies WHERE entry_id=$1', [entry]);
        }
        assert.deepEqual((await grouped()).labels, state.labels);
      },
    );
    await t.test(
      'new classifications are observed before any grouped labels are released',
      async () => {
        const before = await grouped();
        const policy = (
          await client.query(
            'SELECT to_jsonb(p) AS value FROM moria.fs_policies p WHERE entry_id=$1',
            [entry],
          )
        ).rows[0]?.value;
        try {
          const marking = [{ environment, marking: 'restricted' }];
          await client.query(
            "INSERT INTO moria.fs_policies(entry_id,revision,markings,retained_markings,grants) VALUES($1,1,$2,'[]','[]') ON CONFLICT(entry_id) DO UPDATE SET markings=EXCLUDED.markings",
            [entry, JSON.stringify(marking)],
          );
          await bothReject('FORBIDDEN');
        } finally {
          if (policy)
            await client.query('UPDATE moria.fs_policies SET markings=$2 WHERE entry_id=$1', [
              entry,
              JSON.stringify(policy.markings),
            ]);
          else await client.query('DELETE FROM moria.fs_policies WHERE entry_id=$1', [entry]);
        }
        assert.deepEqual((await grouped()).labels, before.labels);
      },
    );
    await t.test(
      'session revocation after the first statement withholds the next result',
      async () => {
        await grouped();
        await client.query('UPDATE moria.workload_sessions SET disabled=true WHERE token_hash=$1', [
          hash,
        ]);
        await assert.rejects(grouped(), { code: 'P0001', message: 'UNAUTHENTICATED' });
        await assert.rejects(
          client.query('UPDATE moria.workload_sessions SET disabled=false WHERE token_hash=$1', [
            hash,
          ]),
          { code: 'P0001', message: 'UNAUTHENTICATED' },
        );
        await assert.rejects(grouped(), { code: 'P0001', message: 'UNAUTHENTICATED' });
      },
    );
  },
);
