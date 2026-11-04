import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import pg from 'pg';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import test from 'node:test';
import { ObjectStore } from '../services/ontology/src/store.js';
import { database } from './fixtures/catalog-storage/database.js';

test(
  'approved operational history collection preserves pins, permanent identity and two source generations',
  { timeout: 120000 },
  async (t) => {
    const db = await database();
    t.after(db.close);
    const environment = randomUUID(),
      resource = 'history-objects',
      store = new ObjectStore(db.catalog);
    await db.catalog.ensureEnvironment(environment);
    const schema = {
      primaryKey: 'uid',
      properties: {
        uid: { type: 'uid' as const, nullable: false },
        name: { type: 'string' as const, nullable: false },
      },
    };
    const rows = await Promise.all(
      Array.from({ length: 501 }, async (_, i) => ({
        uid: await db.catalog.resolveIdentity('history-' + environment, String(i)),
        name: 'original-' + i,
      })),
    );
    const epoch = (
      await db.pool.query('SELECT security_epoch::text FROM moria.environments WHERE id=$1', [
        environment,
      ])
    ).rows[0].security_epoch;
    const stages: string[] = [];
    for (let generation = 0; generation < 5; generation++) {
      const revision = (
        await db.pool.query('SELECT revision::text FROM moria.environments WHERE id=$1', [
          environment,
        ])
      ).rows[0].revision;
      const stage = await store.stage(
        environment,
        resource,
        schema,
        rows.map((r) => ({ ...r, name: `source-${generation}` })),
        epoch,
        revision,
      );
      stages.push(stage);
      await store.publishStage(stage);
    }
    for (let version = 0; version < 6; version++) {
      const client = await db.pool.connect();
      try {
        await client.query('BEGIN');
        const revision = (
          await client.query(
            'UPDATE moria.environments SET revision=revision+1 WHERE id=$1 RETURNING revision::text',
            [environment],
          )
        ).rows[0].revision;
        await client.query(
          'INSERT INTO moria.overlay_versions(environment,object_type,uid,revision,overrides,tombstone) SELECT $1,$2,uid,$3::bigint,$4::jsonb,false FROM unnest($5::uuid[]) ids(uid)',
          [
            environment,
            resource,
            revision,
            JSON.stringify({ name: `edit-${version}` }),
            rows.map((r) => r.uid),
          ],
        );
        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }
    }
    const policy = (
      await db.pool.query('SELECT moria.runtime_history_approve($1,$2,86400,2) AS id', [
        environment,
        'history-policy-' + environment,
      ])
    ).rows[0].id;
    const plan = (await db.pool.query('SELECT moria.runtime_history_plan($1) AS id', [policy]))
      .rows[0].id;
    assert.equal(
      (await db.pool.query('SELECT moria.runtime_history_step($1) AS value', [policy])).rows[0]
        .value.state,
      'grace',
    );
    assert.equal(
      (
        await db.pool.query('SELECT count(*) FROM moria.overlay_versions WHERE environment=$1', [
          environment,
        ])
      ).rows[0].count,
      '3006',
    );
    await db.pool.query(
      "UPDATE moria.runtime_history_collections SET status='blocked' WHERE id=$1",
      [plan],
    );
    // Synthetic historical DATA advances the clock fields only; no live lease or elapsed-day claim.
    await db.pool.query(
      "UPDATE moria.runtime_history_revisions SET observed_at=clock_timestamp()-interval '3 days' WHERE environment=$1",
      [environment],
    );
    const current = (
      await db.pool.query('SELECT revision::text FROM moria.environments WHERE id=$1', [
        environment,
      ])
    ).rows[0].revision;
    const cursor = randomUUID();
    await db.pool.query(
      "INSERT INTO moria.read_cursors VALUES($1,$2,'fixture',0,3,'{}'::jsonb,clock_timestamp()+interval '15 minutes')",
      [cursor, environment],
    );
    const pinnedPlan = (
      await db.pool.query('SELECT moria.runtime_history_plan($1) AS id', [policy])
    ).rows[0].id;
    assert.equal(
      (
        await db.pool.query(
          'SELECT floor::text FROM moria.runtime_history_collections WHERE id=$1',
          [pinnedPlan],
        )
      ).rows[0].floor,
      '3',
    );
    await db.pool.query('DELETE FROM moria.read_cursors WHERE token_hash=$1', [cursor]);
    await db.pool.query(
      "UPDATE moria.runtime_history_collections SET status='blocked' WHERE id=$1",
      [pinnedPlan],
    );
    let ready = (await db.pool.query('SELECT moria.runtime_history_plan($1) AS id', [policy]))
      .rows[0].id;
    assert.equal(
      (
        await db.pool.query(
          'SELECT floor::text FROM moria.runtime_history_collections WHERE id=$1',
          [ready],
        )
      ).rows[0].floor,
      current,
    );
    const age = async (id: string) =>
      db.pool.query(
        "UPDATE moria.runtime_history_collections SET observed_at=clock_timestamp()-interval '3 days',not_before=clock_timestamp()-interval '1 day' WHERE id=$1",
        [id],
      );
    await age(ready);
    // A new cursor after planning must block floor publication rather than silently rewrite the plan.
    const late = randomUUID();
    await db.pool.query(
      "INSERT INTO moria.read_cursors VALUES($1,$2,'late-fixture',0,3,'{}'::jsonb,clock_timestamp()+interval '15 minutes')",
      [late, environment],
    );
    assert.equal(
      (await db.pool.query('SELECT moria.runtime_history_step($1) value', [policy])).rows[0].value
        .state,
      'pinned',
    );
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*)::int n FROM moria.runtime_history_floors WHERE environment=$1',
          [environment],
        )
      ).rows[0].n,
      0,
    );
    assert.equal(
      (
        await db.pool.query(
          'SELECT floor::text,status FROM moria.runtime_history_collections WHERE id=$1',
          [ready],
        )
      ).rows[0].floor,
      current,
    );
    await db.pool.query('DELETE FROM moria.read_cursors WHERE token_hash=$1', [late]);
    ready = (await db.pool.query('SELECT moria.runtime_history_plan($1) id', [policy])).rows[0].id;
    await age(ready);
    const authority = new Authority(db.catalog),
      operations = ['history-work:execute'];
    await authority.syncPrincipal(environment, 'fixture', 'history-reader', {
      kind: 'workload',
      enabled: true,
      attributes: {},
      markings: [],
    });
    await authority.setApplication(environment, 'fixture', 'history-reader', operations);
    await authority.setGrants(
      environment,
      'fixture',
      'history-reader',
      'history-reader',
      operations,
    );
    const credential = await new WorkloadSessions(authority).mint(
      { environment, principal: 'history-reader', application: 'history-reader' },
      'fixture',
    );
    const security = await authority.current(environment, 'history-reader', 'history-reader');
    const binding = (
      await db.pool.query(
        'INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation) SELECT $1,$2,$3,$4,$5,ARRAY[]::text[],$6,s.security_epoch,i.incarnation FROM moria.instance_state s,moria.indexing_instance i RETURNING id',
        [
          environment,
          randomUUID(),
          {
            version: 1,
            domain: 'transform',
            release: 'a'.repeat(64),
            resourceClass: 'standard',
            timeoutSeconds: 120,
            retryPolicy: 'idempotent',
            inputSchema: { type: 'object', properties: {} },
            pinnedInputs: [],
            expectedHeads: {},
          },
          {
            environment,
            principal: 'history-reader',
            application: 'history-reader',
            sessionHash: createHash('sha256').update(credential.token).digest('hex'),
          },
          operations,
          security.epoch,
        ],
      )
    ).rows[0].id;
    const execution = (
      await db.pool.query(
        "SELECT moria.runtime_execution_request($1,$2,'{}'::jsonb,$3,$4::jsonb) id",
        [binding, randomUUID(), 'b'.repeat(64), JSON.stringify([{ key: 'main' }])],
      )
    ).rows[0].id;
    const record = () =>
      db.pool.query(
        "INSERT INTO moria.native_function_reads(execution,call_key,evidence,markings,fingerprint) VALUES($1,'after-plan',$2::jsonb,'[]','fixture')",
        [execution, JSON.stringify([{ resource, revision: '4' }])],
      );
    await record();
    assert.equal(
      (await db.pool.query('SELECT moria.runtime_history_step($1) value', [policy])).rows[0].value
        .state,
      'pinned',
    );
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*)::int n FROM moria.runtime_history_floors WHERE environment=$1',
          [environment],
        )
      ).rows[0].n,
      0,
    );
    await db.pool.query(
      "DELETE FROM moria.native_function_reads WHERE execution=$1 AND call_key='after-plan'",
      [execution],
    );
    ready = (await db.pool.query('SELECT moria.runtime_history_plan($1) id', [policy])).rows[0].id;
    await age(ready);
    // A real non-owning connection cannot self-approve policy, rewrite a floor, or age a collection.
    const role = 'history_' + randomUUID().replaceAll('-', ''),
      password = randomUUID();
    await db.pool.query(
      `CREATE ROLE ${role} LOGIN PASSWORD '${password}';GRANT USAGE ON SCHEMA moria TO ${role};GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role};GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role};GRANT EXECUTE ON FUNCTION moria.runtime_history_operator() TO ${role}`,
    );
    const unprivileged = new pg.Client({
      host: '127.0.0.1',
      port: db.pool.options.port,
      user: role,
      password,
      database: 'postgres',
    });
    await unprivileged.connect();
    try {
      await assert.rejects(
        unprivileged.query('SELECT moria.runtime_history_approve($1,$2,86400,2)', [
          environment,
          randomUUID(),
        ]),
        { code: '42501' },
      );
      await assert.rejects(
        unprivileged.query(
          "INSERT INTO moria.runtime_history_policies(request_key,environment,retain_seconds,keep_generations,incarnation,approved_role) SELECT $1,$2,86400,2,incarnation,'moria_local' FROM moria.indexing_instance",
          [randomUUID(), environment],
        ),
        /HISTORY_OPERATOR_REQUIRED/,
      );
      await assert.rejects(
        unprivileged.query(
          "UPDATE moria.runtime_history_collections SET not_before=observed_at+interval '24 hours' WHERE id=$1",
          [ready],
        ),
        /HISTORY_OPERATOR_REQUIRED/,
      );
      await assert.rejects(
        unprivileged.query(
          'INSERT INTO moria.runtime_history_floors(environment,revision,collection) VALUES($1,0,$2)',
          [environment, ready],
        ),
        /HISTORY_OPERATOR_REQUIRED/,
      );
    } finally {
      await unprivileged.end();
    }

    const identityCount = (
      await db.pool.query('SELECT count(*) FROM moria.identities WHERE domain=$1', [
        'history-' + environment,
      ])
    ).rows[0].count;
    const phases = new Set<string>();
    let result: any;
    for (let i = 0; i < 50; i++) {
      result = (await db.pool.query('SELECT moria.runtime_history_step($1) AS value', [policy]))
        .rows[0].value;
      if (result.phase) phases.add(result.phase);
      if (result.state === 'complete') break;
    }
    assert.equal(result.state, 'complete', JSON.stringify(result));
    const floorBefore = (
      await db.pool.query(
        'SELECT revision::text FROM moria.runtime_history_floors WHERE environment=$1',
        [environment],
      )
    ).rows[0].revision;
    await assert.rejects(
      db.pool.query(
        "INSERT INTO moria.read_cursors VALUES($1,$2,'too-late',0,3,'{}'::jsonb,clock_timestamp()+interval '15 minutes')",
        [randomUUID(), environment],
      ),
      /HISTORY_EXPIRED/,
    );
    await assert.rejects(record(), /HISTORY_EXPIRED/);
    assert.equal(
      (
        await db.pool.query(
          'SELECT revision::text FROM moria.runtime_history_floors WHERE environment=$1',
          [environment],
        )
      ).rows[0].revision,
      floorBefore,
    );

    assert.deepEqual([...phases].sort(), [
      'changes',
      'complete',
      'links',
      'overlays',
      'pins',
      'projections',
      'sources',
    ]);
    assert.equal(
      (
        await db.pool.query('SELECT count(*) FROM moria.overlay_versions WHERE environment=$1', [
          environment,
        ])
      ).rows[0].count,
      '501',
    );
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.staged_source_rows WHERE stage=ANY($1::uuid[])',
          [stages.slice(0, 3)],
        )
      ).rows[0].count,
      '0',
    );
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.staged_source_rows WHERE stage=ANY($1::uuid[])',
          [stages.slice(3)],
        )
      ).rows[0].count,
      '1002',
    );
    assert.equal(
      (
        await db.pool.query('SELECT count(*) FROM moria.identities WHERE domain=$1', [
          'history-' + environment,
        ])
      ).rows[0].count,
      identityCount,
    );
    assert.equal(
      (
        await db.pool.query('SELECT count(*) FROM moria.source_generations WHERE environment=$1', [
          environment,
        ])
      ).rows[0].count,
      '5',
    );
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.runtime_change_transactions WHERE environment=$1',
          [environment],
        )
      ).rows[0].count,
      '1',
    );
    const reader = await db.pool.connect();
    try {
      assert.deepEqual(
        (await store.read(reader, environment, resource, rows[0].uid, current))?.effective,
        { uid: rows[0].uid, name: 'edit-5' },
      );
    } finally {
      reader.release();
    }
    await assert.rejects(
      db.pool.query('SELECT moria.runtime_history_assert($1,0)', [environment]),
      /HISTORY_EXPIRED/,
    );
    const last = (
      await db.pool.query('SELECT id FROM moria.runtime_change_transactions WHERE environment=$1', [
        environment,
      ])
    ).rows[0].id;
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.runtime_change_items WHERE transaction_id=$1',
          [last],
        )
      ).rows[0].count,
      '501',
    );
    await db.pool.query('UPDATE moria.runtime_history_policies SET enabled=false WHERE id=$1', [
      policy,
    ]);
    await assert.rejects(
      db.pool.query('SELECT moria.runtime_history_step($1)', [policy]),
      /HISTORY_POLICY_REVOKED/,
    );
  },
);
