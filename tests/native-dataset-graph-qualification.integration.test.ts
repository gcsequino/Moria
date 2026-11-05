import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';
import { bootstrapGraph } from './fixtures/native-retention/graph.js';
import {
  qualificationFault,
  type QualificationFault,
} from './fixtures/native-retention/qualification.js';

for (const fault of [
  'head',
  'profile',
  'physical',
  'incarnation',
  'memberVersion',
  'missingMember',
] as const) {
  test(
    `qualified bootstrap rejects ${fault} mismatch without publishing a sidecar`,
    { timeout: 120000 },
    async (t) => {
      const fixture = await managedRetentionDataset({ primaryKey: ['id'] });
      t.after(fixture.close);
      const rows = Array.from({ length: 8 }, (_, i) => ({
        id: randomUUID(),
        name: 'qualified-' + i,
        tags: ['proof'],
      }));
      const first = await fixture.publish(rows);
      let current = first.head;
      if (fault === 'head') {
        current = (
          await fixture.publish(rows.map((row) => ({ ...row, name: 'later-' + row.name })))
        ).head;
      } else {
        await qualificationFault(fixture, first.head, fault satisfies QualificationFault);
      }
      const dispatch = (
        await fixture.db.pool.query(
          'SELECT dispatch_enabled FROM moria.runtime_recovery_control WHERE singleton',
        )
      ).rows[0].dispatch_enabled;
      await assert.rejects(
        bootstrapGraph(fixture, first.head, rows),
        fault === 'memberVersion'
          ? /STORAGE_VERSION_CHANGED/
          : fault === 'head'
            ? /HEAD_CONFLICT|INPUT_PIN_CHANGED/
            : /DATASET_GRAPH_ANCESTRY_REQUIRED|DATASET_BINDING_INVALID|SNAPSHOT_PIN_MISMATCH/,
      );
      assert.deepEqual(
        (await fixture.db.catalog.heads(fixture.environment, [fixture.dataset]))[0],
        current,
      );
      assert.equal(
        (
          await fixture.db.pool.query(
            'SELECT dispatch_enabled FROM moria.runtime_recovery_control WHERE singleton',
          )
        ).rows[0].dispatch_enabled,
        dispatch,
      );
      assert.equal(
        (await fixture.db.pool.query('SELECT count(*) FROM moria.runtime_dataset_graph_roots'))
          .rows[0].count,
        '0',
      );
      assert.equal(
        (
          await fixture.db.pool.query(
            "SELECT count(*) FROM moria.runtime_dataset_graph_stages WHERE status='published'",
          )
        ).rows[0].count,
        '0',
      );
      t.diagnostic(
        JSON.stringify({
          fault,
          headUnchanged: true,
          dispatchUnchanged: true,
          noGraphPublication: true,
          guardsEnabledDuringProof: true,
        }),
      );
    },
  );
}
