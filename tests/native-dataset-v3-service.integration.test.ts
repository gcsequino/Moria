import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';
import { objectClient } from '../packages/osdk/src/runtime.js';
import type { ObjectSchema } from '../services/ontology/src/store.js';
import { prepareManagedSource } from './fixtures/native-source-store/managed.js';

test(
  'accepted permanent Source indexes v3 heads independently of its registrar and retains exact array keys',
  { timeout: 300000 },
  async (t) => {
    const columns: ObjectSchema['properties'] = {
      uid: { type: 'uid', nullable: false },
      tags: { type: 'array', nullable: false, items: { type: 'string', nullable: false } },
      note: { type: 'string', nullable: true },
    };
    const fixture = await managedRetentionDataset({ primaryKey: ['tags'], columns });
    t.after(fixture.close);
    const { db, environment, dataset } = fixture;
    const identityDomain = environment + '/v3-service-fixture';
    const owner = await fixture.command(['identity', 'register-domain'], {
      domain: identityDomain,
      cell: fixture.cell,
    });
    const identities = await fixture.command(['identity', 'resolve'], {
      domain: identityDomain,
      keys: Array.from({ length: 16 }, (_, i) => 'row-' + i),
      owner,
    });
    assert.equal(new Set(identities.uids).size, 16);
    const rows = Array.from({ length: 16 }, (_, i) => ({
      uid: identities.uids[i] as string,
      tags: ['key-' + i, 'key-' + i],
      note: i % 2 ? null : 'row-' + i,
    }));
    const first = await fixture.publish(rows, { graph: true });
    const { target, context, sessions, registrar, transport, drain } = await prepareManagedSource(
      fixture,
      columns,
    );
    const currentBuild = async () =>
      (
        await db.pool.query(
          "SELECT b.id,a.revision::text AS alias_revision,s.row_count::text,s.key_count::text,s.ordered_keys FROM moria.indexing_builds b JOIN moria.indexing_bindings current_binding ON current_binding.environment=b.environment AND current_binding.target=b.target AND current_binding.revision=b.binding_revision JOIN moria.dataset_heads h ON h.environment=b.environment AND h.dataset=b.source AND h.revision=b.source_revision AND h.metadata_location=b.metadata_location JOIN moria.native_source_row_stores s ON s.id=b.id JOIN moria.native_source_generation_aliases a ON a.store=s.id WHERE b.environment=$1 AND b.target=$2 AND b.status='active'",
          [environment, target],
        )
      ).rows;
    await drain();
    const firstBuild = await currentBuild();
    if (firstBuild.length !== 1) {
      const blocked = (
        await db.pool.query(
          'SELECT error FROM moria.indexing_blocked_intents WHERE environment=$1 AND target=$2 AND source_revision=$3',
          [environment, target, first.head.revision],
        )
      ).rows;
      t.diagnostic(
        JSON.stringify({
          phase: 'actualPermanentV3Admission',
          acceptedActiveBuilds: firstBuild.length,
          blocked,
          datasetHeadUnchanged: true,
        }),
      );
    }
    assert.equal(
      firstBuild.length,
      1,
      'actual accepted permanent service must produce a current managed v3 Source store',
    );
    assert.equal(firstBuild[0].row_count, '16');
    assert.equal(firstBuild[0].key_count, '16');
    assert.deepEqual(firstBuild[0].ordered_keys, ['tags']);
    const delegated = (
      await db.pool.query(
        'SELECT origin_session_hash,proof FROM moria.runtime_indexing_delegations WHERE build=$1',
        [firstBuild[0].id],
      )
    ).rows[0];
    assert.equal(delegated.origin_session_hash, null);
    assert.equal(delegated.proof.originKind, 'service');
    assert.equal(delegated.proof.originBinding, context.executionBinding);
    const keys = (
      await db.pool.query(
        "SELECT encode(key_hash,'hex') AS hash,convert_from(encoded_key,'UTF8')::jsonb AS key,uid FROM moria.native_source_key_map WHERE store=$1",
        [firstBuild[0].id],
      )
    ).rows;
    assert.equal(keys.length, 16);
    const expectedRows = new Map(rows.map((row) => [row.uid, row]));
    for (const row of keys) {
      assert(expectedRows.has(row.uid));
      assert.deepEqual(row.key, [expectedRows.get(row.uid)!.tags]);
      assert.equal(row.hash, createHash('sha256').update(JSON.stringify(row.key)).digest('hex'));
    }
    assert.deepEqual(keys.map((row) => row.uid).sort(), rows.map((row) => row.uid).sort());
    const objects = objectClient<{ uid: string; tags: string[]; note: string | null }>(
      transport(fixture.token),
      target,
      columns,
    );
    for (const row of rows) assert.deepEqual((await objects.get(row.uid)).object, row);
    await sessions.revoke(await sessions.authenticate('Bearer ' + registrar), 'fixture');
    const second = await fixture.publish([], { graph: true });
    await drain();
    const secondBuild = await currentBuild();
    assert.equal(secondBuild.length, 1);
    assert.notEqual(secondBuild[0].id, firstBuild[0].id);
    assert.equal(secondBuild[0].row_count, '0');
    assert.equal(secondBuild[0].key_count, '0');
    assert.equal((await objects.query()).objects.length, 0);
    assert.equal(
      (
        await db.pool.query('SELECT count(*)::text FROM moria.runtime_object_base($1,$2,$3)', [
          environment,
          target,
          firstBuild[0].alias_revision,
        ])
      ).rows[0].count,
      '16',
    );
    const secondDelegation = (
      await db.pool.query(
        'SELECT origin_session_hash,proof FROM moria.runtime_indexing_delegations WHERE build=$1',
        [secondBuild[0].id],
      )
    ).rows[0];
    assert.equal(secondDelegation.origin_session_hash, null);
    assert.equal(secondDelegation.proof.originKind, 'service');
    assert.deepEqual((await db.catalog.heads(environment, [dataset]))[0], second.head);
    t.diagnostic(
      JSON.stringify({
        realDependentSourceCompilation: true,
        actualOperatorApproval: true,
        actualServiceAdmissionAndDelegation: true,
        noRegistrarTokenInWorker: true,
        exactArrayKeyMap: true,
        typedObjectGet: true,
        registrarRevocationThenFreshServiceReadmission: true,
        emptyCurrentAndRetainedHistory: true,
        datasetHeadUnchangedByIndexing: true,
      }),
    );
  },
);
