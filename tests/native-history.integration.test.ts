import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import test from 'node:test';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { database } from './fixtures/catalog-storage/database.js';

test(
  'operational history normalizes bounded read pins against actual execution and cursor authority',
  { timeout: 60000 },
  async (t) => {
    const db = await database();
    t.after(db.close);
    const environment = randomUUID(),
      resource = randomUUID(),
      principal = 'reader',
      application = 'history-fixture';
    await db.catalog.ensureEnvironment(environment);
    const authority = new Authority(db.catalog),
      operations = ['work:execute'];
    await authority.syncPrincipal(environment, 'fixture', principal, {
      kind: 'workload',
      enabled: true,
      attributes: {},
      markings: [],
    });
    await authority.setApplication(environment, 'fixture', application, operations);
    await authority.setGrants(environment, 'fixture', principal, application, operations);
    const issued = await new WorkloadSessions(authority).mint(
      { environment, principal, application },
      'fixture',
    );
    const state = await authority.current(environment, principal, application);
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
    const binding = (
      await db.pool.query(
        'INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation) SELECT $1,$2,$3,$4,$5,ARRAY[]::text[],$6,s.security_epoch,i.incarnation FROM moria.instance_state s,moria.indexing_instance i RETURNING id',
        [
          environment,
          resource,
          spec,
          {
            environment,
            principal,
            application,
            sessionHash: createHash('sha256').update(issued.token).digest('hex'),
          },
          operations,
          state.epoch,
        ],
      )
    ).rows[0].id;
    const execution = (
      await db.pool.query(
        "SELECT moria.runtime_execution_request($1,$2,'{}'::jsonb,$3,$4::jsonb) AS id",
        [binding, randomUUID(), 'b'.repeat(64), JSON.stringify([{ key: 'main' }])],
      )
    ).rows[0].id;
    // Private ledger representation test; no guest read or fabricated worker lease is claimed.
    const record = (key: string, evidence: unknown) =>
      db.pool.query(
        'INSERT INTO moria.native_function_reads(execution,call_key,evidence,markings,fingerprint) VALUES($1,$2,$3,$4,$5)',
        [execution, key, JSON.stringify(evidence), '[]', 'fixture'],
      );
    await record('actual-execution', [
      { resource, revision: '0' },
      { resource, revision: '0' },
    ]);
    const pins = (
      await db.pool.query(
        'SELECT environment,revision::text,expires_at::text FROM moria.runtime_history_read_pins WHERE execution=$1',
        [execution],
      )
    ).rows;
    assert.deepEqual(pins, [{ environment, revision: '0', expires_at: 'infinity' }]);
    await assert.rejects(
      record('noncanonical', [{ resource, revision: '01' }]),
      /HISTORY_PIN_INVALID/,
    );
    assert.equal(
      (
        await db.pool.query('SELECT count(*) FROM moria.native_function_reads WHERE execution=$1', [
          execution,
        ])
      ).rows[0].count,
      '1',
    );
    const tx = await db.pool.connect();
    try {
      await tx.query('BEGIN');
      await tx.query(
        "UPDATE moria.runtime_executions SET status='cancelled',finished_at=clock_timestamp(),lease_until=NULL WHERE id=$1",
        [execution],
      );
      assert.equal(
        (
          await tx.query(
            "SELECT p.expires_at=e.finished_at+interval '30 days' AS exact FROM moria.runtime_history_read_pins p JOIN moria.runtime_executions e ON e.id=p.execution WHERE p.execution=$1",
            [execution],
          )
        ).rows[0].exact,
        true,
      );
      await tx.query('ROLLBACK');
    } finally {
      tx.release();
    }
    assert.equal(
      (
        await db.pool.query(
          'SELECT expires_at::text FROM moria.runtime_history_read_pins WHERE execution=$1',
          [execution],
        )
      ).rows[0].expires_at,
      'infinity',
    );
    await record('labels-only', []);
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.runtime_history_read_pins WHERE execution=$1',
          [execution],
        )
      ).rows[0].count,
      '1',
    );
    const cursor = () =>
      db.pool.query(
        "INSERT INTO moria.read_cursors VALUES($1,$2,'binding',0,0,'{}'::jsonb,clock_timestamp()+interval '15 minutes')",
        [randomUUID(), environment],
      );
    await cursor();
    // Synthetic floor fixture exercises denial only; no collector or elapsed retention is claimed.
    await db.pool.query(
      'INSERT INTO moria.runtime_history_floors(environment,revision,collection) VALUES($1,1,$2)',
      [environment, randomUUID()],
    );
    await assert.rejects(cursor(), /HISTORY_EXPIRED/);
    await assert.rejects(record('below-floor', [{ resource, revision: '0' }]), /HISTORY_EXPIRED/);
    await assert.rejects(
      db.pool.query('UPDATE moria.runtime_history_floors SET revision=0 WHERE environment=$1', [
        environment,
      ]),
      /HISTORY_FLOOR_RESERVED/,
    );
    await assert.rejects(
      db.pool.query('DELETE FROM moria.runtime_history_floors WHERE environment=$1', [environment]),
      /HISTORY_FLOOR_RESERVED/,
    );
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.runtime_history_read_pins WHERE execution=$1',
          [execution],
        )
      ).rows[0].count,
      '1',
    );
  },
);
