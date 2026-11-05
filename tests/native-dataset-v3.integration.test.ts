import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { promisify } from 'node:util';
import test from 'node:test';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';

test(
  'qualified v3 replacement heads bind standard metadata and typed array-key rows',
  { timeout: 300000 },
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
    const first = await fixture.publish(rows, { graph: true }).catch(async (error) => {
      assert.equal(
        (await fixture.db.catalog.heads(fixture.environment, [fixture.dataset]))[0].revision,
        '0',
      );
      assert.equal(
        (
          await fixture.db.pool.query(
            'SELECT count(*) FROM moria.runtime_snapshot_candidates WHERE resource=$1',
            [fixture.dataset],
          )
        ).rows[0].count,
        '0',
      );
      assert.equal(
        (
          await fixture.db.pool.query(
            'SELECT count(*) FROM moria.runtime_dataset_commit_deltas WHERE dataset=$1',
            [fixture.dataset],
          )
        ).rows[0].count,
        '0',
      );
      t.diagnostic(
        JSON.stringify({ failedPrepareLeavesHeadRevision: '0', candidates: 0, journalEdges: 0 }),
      );
      throw error;
    });
    assert.match(first.head.metadataLocation!, /^moria-snapshot-v3:/);
    await fixture.read(first.head);
    t.diagnostic(
      JSON.stringify({ phase: 'firstPublishedAndTypedRead', revision: first.head.revision }),
    );
    const reader = await fixture.bind(
      [
        {
          resource: fixture.dataset,
          revision: first.head.revision,
          snapshot: first.head.snapshotId,
        },
      ],
      {},
    );
    const standardExport = await mkdtemp(join(tmpdir(), 'moria-v3-iceberg-export-'));
    t.after(() => rm(standardExport, { recursive: true, force: true }));
    const graph = await fixture.invoke({
      root: fixture.db.root,
      token: fixture.token,
      binding: reader,
      mode: 'graph-read',
      standardExport,
      graphSerialParity: process.env.MORIA_TEST_GRAPH_SERIAL_PARITY === '1',
      expectedRows: rows.length,
      writerRows: { [fixture.dataset]: rows },
      snapshots: [{ ...fixture.snapshot, snapshotId: first.head.snapshotId }],
      s3Bindings: [fixture.datasetStorage],
    });
    assert.match(graph, /"event":"graph_bootstrap_read"/);
    assert.match(graph, /"event":"standard_iceberg_export"/);
    for (const line of graph.split('\n')) {
      if (/"event":"graph_(scan_complete|batch_keys_complete|key_parity_checked)"/.test(line))
        t.diagnostic(line);
    }
    const { stdout } = await promisify(execFile)(resolve('.venv/bin/python'), [
      resolve('tests/fixtures/native-retention/iceberg_oracle.py'),
      join(standardExport, 'export.json'),
    ]);
    const oracle = JSON.parse(stdout) as {
      pyiceberg: string;
      pyarrow: string;
      snapshot: string;
      sequence: string;
      positionDeleteFiles: number;
      matchedDeletePairs: number;
      rows: typeof rows;
    };
    assert.equal(oracle.snapshot, first.head.snapshotId);
    assert.equal(oracle.sequence, '1');
    assert.equal(oracle.positionDeleteFiles, 0);
    assert.equal(oracle.matchedDeletePairs, 0);
    const byId = (a: (typeof rows)[number], b: (typeof rows)[number]) => a.id.localeCompare(b.id);
    assert.deepEqual(oracle.rows.toSorted(byId), rows.toSorted(byId));
    t.diagnostic(
      JSON.stringify({
        independentIceberg: oracle.pyiceberg,
        pyarrow: oracle.pyarrow,
        rows: rows.length,
      }),
    );
    t.diagnostic(JSON.stringify({ phase: 'arrayKeysAndPointDigestVerified', rows: rows.length }));
    const second = await fixture.publish([], { graph: true });
    t.diagnostic(JSON.stringify({ phase: 'emptyPublished', revision: second.head.revision }));
    assert.match(second.head.metadataLocation!, /^moria-snapshot-v3:/);
    await fixture.read(second.head);
    await fixture.read(first.head);
    const proof = (
      await fixture.db.pool.query(
        'SELECT c.proof,c.metadata_location,s.status,s.node,r.reader_profile,(SELECT count(*) FROM moria.runtime_dataset_root_members m WHERE m.root=r.key) AS members FROM moria.runtime_snapshot_candidates c JOIN moria.runtime_dataset_graph_stages s ON s.candidate=c.id JOIN moria.runtime_dataset_roots r ON r.key=moria.runtime_dataset_root_key(c.metadata_location) WHERE c.execution=$1',
        [second.execution],
      )
    ).rows;
    assert.equal(proof.length, 1);
    assert.equal(proof[0].proof.version, 3);
    assert.equal(proof[0].proof.rows, 0);
    assert.equal(proof[0].reader_profile, 'iceberg-v2-native-mor-bounded-v3');
    assert.equal(proof[0].status, 'published');
    assert.equal(proof[0].members, '1');
    const journals = (
      await fixture.db.pool.query(
        'SELECT change_kind,complete_rows_root,result_root,result_metadata FROM moria.runtime_dataset_commit_deltas WHERE execution=ANY($1::uuid[]) ORDER BY result_revision',
        [[first.execution, second.execution]],
      )
    ).rows;
    assert.equal(journals.length, 2);
    for (const edge of journals) {
      assert.equal(edge.change_kind, 'replace');
      assert.equal(edge.complete_rows_root, edge.result_root);
      assert.match(edge.result_metadata, /^moria-snapshot-v3:/);
    }
    t.diagnostic(
      JSON.stringify({
        rows: 64,
        arrayKeys: true,
        nullableNonKey: true,
        emptyReplacement: true,
        retainedRead: true,
        rootMembers: 1,
        actualJournalEdges: 2,
      }),
    );
  },
);
