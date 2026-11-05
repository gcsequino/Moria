import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import test from 'node:test';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';
import { Authority } from '../services/security/src/authority.js';
import { ObjectStore, type ObjectSchema } from '../services/ontology/src/store.js';
import { IcebergIndexer } from '../services/ontology/src/indexer.js';

test(
  'legacy full-build indexing reads v3 array-key heads and preserves empty/current history',
  { timeout: 300000 },
  async (t) => {
    const columns: ObjectSchema['properties'] = {
      uid: { type: 'uid' as const, nullable: false },
      tags: {
        type: 'array' as const,
        nullable: false,
        items: { type: 'string' as const, nullable: false },
      },
      note: { type: 'string' as const, nullable: true },
    };
    const fixture = await managedRetentionDataset({ primaryKey: ['tags'], columns });
    t.after(fixture.close);
    const { db, environment, dataset, principal, application } = fixture;
    const identityDomain = environment + '/v3-source-fixture';
    const identityOwner = await fixture.command(
      ['identity', 'register-domain'],
      { domain: identityDomain, cell: fixture.cell },
      fixture.nativeEnv,
    );
    const identity = await fixture.command(
      ['identity', 'resolve'],
      {
        domain: identityDomain,
        keys: Array.from({ length: 16 }, (_, i) => 'v3-row-' + i),
        owner: identityOwner,
      },
      fixture.nativeEnv,
    );
    assert.equal(identity.uids.length, 16);
    assert.equal(new Set(identity.uids).size, 16);
    const rows = Array.from({ length: 16 }, (_, i) => ({
      uid: identity.uids[i] as string,
      tags: ['key-' + i, 'key-' + i],
      note: i % 2 ? null : 'row-' + i,
    }));
    const first = await fixture.publish(rows, { graph: true });
    assert.match(first.head.metadataLocation!, /^moria-snapshot-v3:/);
    const target = randomUUID();
    const authority = new Authority(db.catalog);
    const current = await authority.current(environment, principal, application);
    const operations = [
      ...new Set([
        ...current.identity.grantedOperations,
        ...current.identity.applicationScopes,
        dataset + ':read',
        dataset + ':write',
        target + ':index',
        target + ':read',
      ]),
    ];
    await authority.setApplication(environment, 'fixture', application, operations);
    await authority.setGrants(environment, 'fixture', principal, application, operations);
    const session = {
      environment,
      principal,
      application,
      sessionHash: createHash('sha256').update(fixture.token).digest('hex'),
    };
    const schema: ObjectSchema = { primaryKey: 'uid', properties: columns };
    const indexer = new IcebergIndexer(new ObjectStore(db.catalog), authority, {
      source: { kind: 'dataset', resource: dataset, markings: ['private'] },
      target: { resource: target, markings: ['private'], schema },
    });
    const index = async () => {
      await indexer.registerDistributed(session, {
        targetId: target,
        warehouse: fixture.db.root,
        partitions: 4,
      });
      const output = await fixture.invoke({
        root: db.root,
        token: fixture.token,
        binding: first.binding,
        mode: 'index',
        snapshots: [fixture.snapshot],
        s3Bindings: [fixture.datasetStorage],
      });
      assert.match(output, /"event":"indexed"/);
      return (
        await db.pool.query(
          "SELECT b.id,b.metadata_location,g.revision::text AS alias_revision,b.source_revision::text,(SELECT count(*)::text FROM moria.staged_source_rows r WHERE r.stage=b.id) AS row_count FROM moria.indexing_builds b JOIN moria.indexing_bindings current_binding ON current_binding.environment=b.environment AND current_binding.target=b.target AND current_binding.revision=b.binding_revision JOIN moria.dataset_heads current_head ON current_head.environment=b.environment AND current_head.dataset=b.source AND current_head.revision=b.source_revision AND current_head.snapshot_id=b.snapshot_id AND current_head.metadata_location=b.metadata_location JOIN moria.source_generations g ON g.stage=b.id WHERE b.environment=$1 AND b.target=$2 AND b.status='active'",
          [environment, target],
        )
      ).rows;
    };
    const firstBuild = await index();
    assert.equal(firstBuild.length, 1);
    assert.equal(firstBuild[0].metadata_location, first.head.metadataLocation);
    assert.equal(firstBuild[0].source_revision, first.head.revision);
    assert.equal(firstBuild[0].row_count, String(rows.length));
    const map = (
      await db.pool.query('SELECT uid,value FROM moria.staged_source_rows WHERE stage=$1', [
        firstBuild[0].id,
      ])
    ).rows;
    assert.equal(map.length, rows.length);
    for (const row of map) {
      assert.equal(row.uid, row.value.uid);
    }
    assert.deepEqual(
      map.map((row) => row.value).sort((a, b) => a.uid.localeCompare(b.uid)),
      [...rows].sort((a, b) => a.uid.localeCompare(b.uid)),
    );
    const tasks = (
      await db.pool.query(
        "SELECT plan FROM moria.indexing_tasks WHERE build=$1 AND plan->>'kind'='scan' ORDER BY id",
        [firstBuild[0].id],
      )
    ).rows;
    assert(tasks.length > 0);
    for (const task of tasks) {
      assert.equal(task.plan.version, 1);
      assert.equal(task.plan.readerProfile, 'iceberg-v2-native-mor-bounded-v3');
      assert(task.plan.file);
    }
    assert.deepEqual((await db.catalog.heads(environment, [dataset]))[0], first.head);
    const second = await fixture.publish([], { graph: true });
    const secondBuild = await index();
    assert.equal(secondBuild.length, 1);
    assert.notEqual(secondBuild[0].id, firstBuild[0].id);
    assert.equal(secondBuild[0].metadata_location, second.head.metadataLocation);
    assert.equal(secondBuild[0].row_count, '0');
    const emptyTasks = (
      await db.pool.query(
        "SELECT plan FROM moria.indexing_tasks WHERE build=$1 AND plan->>'kind'='scan'",
        [secondBuild[0].id],
      )
    ).rows;
    assert.equal(emptyTasks.length, 1);
    assert.equal(emptyTasks[0].plan.version, 1);
    assert.equal(emptyTasks[0].plan.readerProfile, 'iceberg-v2-native-mor-bounded-v3');
    assert.equal(emptyTasks[0].plan.file, null);
    for (const [revision, expected] of [
      [firstBuild[0].alias_revision, rows.length],
      [secondBuild[0].alias_revision, 0],
    ] as const) {
      assert.equal(
        (
          await db.pool.query('SELECT count(*)::text FROM moria.runtime_object_base($1,$2,$3)', [
            environment,
            target,
            revision,
          ])
        ).rows[0].count,
        String(expected),
      );
    }
    assert.deepEqual((await db.catalog.heads(environment, [dataset]))[0], second.head);
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.runtime_dataset_commit_deltas WHERE dataset=$1',
          [dataset],
        )
      ).rows[0].count,
      '2',
    );
    t.diagnostic(
      JSON.stringify({
        actualV3Head: true,
        arrayKeysAndNullableRows: true,
        nativeGraphScanTasks: tasks.length,
        legacySourceGeneration: true,
        registeredIdentityUids: 16,
        explicitEmptyGraphTask: true,
        emptyCurrentAndRetainedSourceHistory: true,
        datasetHeadUnchangedByIndexing: true,
      }),
    );
  },
);
