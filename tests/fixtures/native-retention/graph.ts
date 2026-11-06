import assert from 'node:assert/strict';
import type { managedRetentionDataset } from './dataset.js';

type Fixture = Awaited<ReturnType<typeof managedRetentionDataset>>;
type Head = { revision: string; snapshotId: string | null; metadataLocation: string | null };

export async function bootstrapGraph(
  fixture: Fixture,
  head: Head,
  rows: Record<string, unknown>[],
) {
  const binding = await fixture.bind([], { [fixture.dataset]: head.revision });
  const output = await fixture.invoke({
    root: fixture.db.root,
    token: fixture.token,
    binding,
    mode: 'graph-bootstrap',
    expectedRows: rows.length,
    writerRows: { [fixture.dataset]: rows },
    snapshots: [{ ...fixture.snapshot, snapshotId: head.snapshotId }],
    s3Bindings: [fixture.datasetStorage],
  });
  assert.match(output, /"event":"graph_bootstrap_read"/);
  const executions = await fixture.db.pool.query(
    'SELECT id FROM moria.runtime_executions WHERE binding=$1',
    [binding],
  );
  assert.equal(executions.rows.length, 1);
  await fixture.command(['execution', 'cancel'], { id: executions.rows[0].id });
  return output;
}
