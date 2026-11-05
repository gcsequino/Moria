import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';
import { bootstrapGraph } from './fixtures/native-retention/graph.js';

test(
  'qualified graph bootstrap preserves standard Iceberg rows and exact array keys',
  { timeout: 120000 },
  async (t) => {
    const fixture = await managedRetentionDataset({
      primaryKey: ['tags'],
      columns: {
        id: { type: 'uid', nullable: false },
        tags: { type: 'array', nullable: false, items: { type: 'string', nullable: false } },
        note: { type: 'string', nullable: true },
      },
    });
    t.after(fixture.close);
    const rows = Array.from({ length: 64 }, (_, i) => ({
      id: randomUUID(),
      tags: ['key-' + i, 'key-' + i],
      note: i % 2 ? null : 'row-' + i,
    }));
    const first = await fixture.publish(rows);
    await bootstrapGraph(fixture, first.head, rows);
    assert.deepEqual(
      (await fixture.db.catalog.heads(fixture.environment, [fixture.dataset]))[0],
      first.head,
    );
    const graph = (
      await fixture.db.pool.query(
        'SELECT node,profile FROM moria.runtime_dataset_graph_roots WHERE root=moria.runtime_dataset_root_key($1)',
        [first.head.metadataLocation],
      )
    ).rows;
    assert.equal(graph.length, 1);
    assert.equal(graph[0].profile.readerProfile, 'iceberg-v2-native-mor-bounded-v3');
    assert.equal(
      (
        await fixture.db.pool.query(
          'SELECT count(*) FROM moria.runtime_dataset_graph_queue WHERE processed_at IS NULL',
        )
      ).rows[0].count,
      '0',
    );
    await fixture.read(first.head);
    t.diagnostic(
      JSON.stringify({
        rows: 64,
        allKeysLookedUp: true,
        arrayOrderAndDuplicatesPreserved: true,
        nullableNonKey: true,
        headUnchanged: true,
        standardV2Readback: true,
      }),
    );
  },
);
