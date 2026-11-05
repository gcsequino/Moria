import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import type { TestContext } from 'node:test';
import pg from 'pg';
import {
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  S3ServiceException,
} from '@aws-sdk/client-s3';
import { managedRetentionDataset } from './dataset.js';
import { bootstrapGraph } from './graph.js';
import { retentionRecovery } from './recovery.js';
import { journalRows, assertRestoredReplacementJournal } from './journal.js';
import { ageClosedDatasetHistory, ageClosedUnclaimedCandidates } from './history.js';

import { versionTrace, currentWrapperCopy } from './version-trace.js';

export async function graphCustody(t: TestContext, nativeHead = false) {
  const trace = nativeHead ? versionTrace() : undefined;
  const fixture = await managedRetentionDataset({
    primaryKey: ['id'],
    ...(trace ? { wrapS3: trace.wrap } : {}),
  });
  t.after(fixture.close);
  const { db, environment, dataset } = fixture;
  const firstRows = Array.from({ length: 8 }, (_, i) => ({
    id: randomUUID(),
    name: 'first-' + i,
    tags: ['exact', String(i)],
  }));
  const first = await fixture.publish(firstRows, { graph: nativeHead });
  if (nativeHead) assert.match(first.head.metadataLocation!, /^moria-snapshot-v3:/);
  else await bootstrapGraph(fixture, first.head, firstRows);
  const sourceRoot = (
    await db.pool.query(
      'SELECT g.node,n.content,q.physical_version FROM moria.runtime_dataset_graph_roots g JOIN moria.runtime_dataset_graph_nodes n ON n.id=g.node JOIN moria.runtime_dataset_graph_qualifications q USING(node,physical_fingerprint,incarnation) WHERE g.root=moria.runtime_dataset_root_key($1)',
      [first.head.metadataLocation],
    )
  ).rows[0];
  let retainedData:
    | {
        node: string;
        content: {
          object: { warehouseId: string; tableId: string; objectId: string };
          size: number;
        };
        versionId: string;
      }
    | undefined;
  if (nativeHead) {
    const files = (
      await db.pool.query(
        "WITH RECURSIVE subtree(node) AS(SELECT child FROM moria.runtime_dataset_graph_edges WHERE parent=$1 AND label='files' UNION SELECT e.child FROM moria.runtime_dataset_graph_edges e JOIN subtree p ON p.node=e.parent) SELECT n.id AS node,n.content,q.physical_version FROM subtree p JOIN moria.runtime_dataset_graph_nodes n ON n.id=p.node JOIN moria.runtime_dataset_graph_qualifications q ON q.node=n.id AND q.incarnation=(SELECT incarnation FROM moria.indexing_instance) WHERE n.kind='content' LIMIT 257",
        [sourceRoot.node],
      )
    ).rows;
    assert(files.length <= 256);
    for (const file of files) {
      const [, versionId] = JSON.parse(file.physical_version.token.slice('s3v1/'.length));
      assert.equal(typeof versionId, 'string');
      const object = file.content.object;
      const response = await fixture.s3.client.send(
        new GetObjectCommand({
          Bucket: fixture.s3.bucket,
          Key:
            fixture.datasetStorage.prefix +
            '/' +
            object.warehouseId +
            '/' +
            object.tableId +
            '/objects/' +
            object.objectId,
          VersionId: versionId,
          Range: 'bytes=0-3',
        }),
      );
      assert.equal(response.VersionId, versionId);
      assert(response.Body);
      if (Buffer.from(await response.Body.transformToByteArray()).toString() === 'PAR1') {
        retainedData = { node: file.node, content: file.content, versionId };
        break;
      }
    }
    assert(retainedData);
  }
  const secondRows = firstRows.map((row) => ({ ...row, name: 'second-' + row.name }));
  const second = await fixture.publish(secondRows, { graph: nativeHead });
  if (nativeHead) assert.match(second.head.metadataLocation!, /^moria-snapshot-v3:/);
  else await bootstrapGraph(fixture, second.head, secondRows);
  if (nativeHead) {
    const roots = (
      await db.pool.query(
        'SELECT r.metadata_location,count(m.*)::text AS members FROM moria.runtime_dataset_roots r JOIN moria.runtime_dataset_root_members m ON m.root=r.key WHERE r.metadata_location=ANY($1::text[]) GROUP BY r.metadata_location',
        [[first.head.metadataLocation, second.head.metadataLocation]],
      )
    ).rows;
    assert.equal(roots.length, 2);
    for (const root of roots) assert.equal(root.members, '1');
  }
  await ageClosedDatasetHistory(fixture);
  const recovery = await retentionRecovery(fixture);
  const capturedJournal = await journalRows(db.pool, environment, dataset);
  assert.equal(capturedJournal.length, 2);
  const copied = nativeHead
    ? await currentWrapperCopy(fixture, first.head.metadataLocation, 'root')
    : undefined;
  if (copied) {
    trace!.select('/' + copied.object.Bucket + '/' + copied.object.Key);
    t.after(() => {
      if (trace!.entries.length)
        t.diagnostic(
          JSON.stringify({
            phase: 'captureVersionTraceOnExit',
            pinnedVersion: copied.pinned,
            currentVersion: copied.current,
            requests: trace!.entries,
          }),
        );
    });
  }
  const captured = await recovery.capture('graph-exact-backup');
  if (copied) {
    assert(
      trace!.entries.some(
        (entry) =>
          entry.method === 'GET' &&
          entry.requestVersion === copied.pinned &&
          entry.responseVersion === copied.pinned &&
          entry.status === 206,
      ),
    );
    assert(
      !trace!.entries.some(
        (entry) => entry.method === 'GET' && entry.requestVersion === copied.current,
      ),
    );
    assert.deepEqual(
      (
        await db.pool.query(
          'SELECT source_version FROM moria.runtime_recovery_members WHERE recovery_set=$1 AND content=$2',
          [captured.recoverySet, sourceRoot.content],
        )
      ).rows[0].source_version,
      sourceRoot.physical_version,
    );
    t.diagnostic(
      JSON.stringify({ phase: 'backupPinnedVersionDespiteCurrentCopy', requests: trace!.entries }),
    );
    trace!.select('/disabled');
  }
  assert.equal(
    (
      await db.pool.query(
        'SELECT count(*) FROM moria.runtime_dataset_graph_nodes n JOIN moria.runtime_dataset_graph_qualifications q ON q.node=n.id JOIN moria.runtime_dataset_graph_counts c USING(node,physical_fingerprint,incarnation) WHERE q.incarnation=(SELECT incarnation FROM moria.indexing_instance) AND c.live_references>0 AND NOT EXISTS(SELECT 1 FROM moria.runtime_recovery_members m WHERE m.recovery_set=$1 AND m.content=n.content AND m.backup IS NOT NULL AND m.backup_version IS NOT NULL)',
        [captured.recoverySet],
      )
    ).rows[0].count,
    '0',
  );
  let policy = await fixture.command(
    ['dataset-retention', 'approve'],
    {
      requestKey: 'graph-custody-policy',
      environment,
      dataset,
      retainSeconds: 86400,
      recheckSeconds: 30,
    },
    fixture.operatorEnv,
  );
  const graphMetadata = async (location: string) => {
    const root = (
      await db.pool.query(
        'SELECT n.content,q.physical_version FROM moria.runtime_dataset_graph_roots g JOIN moria.runtime_dataset_graph_nodes n ON n.id=g.node JOIN moria.runtime_dataset_graph_qualifications q USING(node,physical_fingerprint,incarnation) WHERE g.root=moria.runtime_dataset_root_key($1)',
        [location],
      )
    ).rows[0];
    const read = async (content: any, version: any) => {
      const [, versionId] = JSON.parse(version.token.slice('s3v1/'.length));
      const object = content.object;
      const response = await fixture.s3.client.send(
        new GetObjectCommand({
          Bucket: fixture.s3.bucket,
          Key:
            fixture.datasetStorage.prefix +
            '/' +
            object.warehouseId +
            '/' +
            object.tableId +
            '/objects/' +
            object.objectId,
          VersionId: versionId,
        }),
      );
      assert.equal(response.VersionId, versionId);
      assert(response.Body);
      return JSON.parse(Buffer.from(await response.Body.transformToByteArray()).toString());
    };
    const descriptor = await read(root.content, root.physical_version);
    const metadata = (
      await db.pool.query(
        "SELECT q.physical_version FROM moria.runtime_dataset_graph_nodes n JOIN moria.runtime_dataset_graph_qualifications q ON q.node=n.id AND q.incarnation=(SELECT incarnation FROM moria.indexing_instance) WHERE n.content=$1 AND n.kind='content'",
        [descriptor.metadata.content],
      )
    ).rows[0];
    return {
      descriptor,
      metadata: await read(descriptor.metadata.content, metadata.physical_version),
    };
  };
  const originalMetadata = nativeHead
    ? await graphMetadata(second.head.metadataLocation!)
    : undefined;
  let beforeExpiration = second.head;
  if (nativeHead) {
    const binding = await fixture.bind(
      [{ resource: dataset, revision: first.head.revision, snapshot: first.head.snapshotId }],
      {},
    );
    const child = spawn(
      fixture.harness,
      ['--ignored', '--exact', 'catalog_publication', '--nocapture', '--test-threads=1'],
      {
        env: {
          ...fixture.nativeEnv,
          MORIA_NATIVE_CATALOG_FIXTURE: JSON.stringify({
            root: db.root,
            token: fixture.token,
            binding,
            mode: 'graph-read-held',
            expectedRows: firstRows.length,
            writerRows: { [dataset]: firstRows },
            snapshots: [{ ...fixture.snapshot, snapshotId: first.head.snapshotId }],
            s3Bindings: [fixture.datasetStorage],
          }),
        },
        stdio: ['pipe', 'pipe', 'pipe'],
      },
    );
    let output = '',
      errors = '';
    child.stdout.on('data', (part) => {
      output = (output + part).slice(-65536);
    });
    child.stderr.on('data', (part) => {
      errors = (errors + part).slice(-65536);
    });
    const exited = new Promise<number | null>((resolve, reject) => {
      child.once('error', reject);
      child.once('close', resolve);
    });
    t.after(async () => {
      child.kill('SIGKILL');
      await exited.catch(() => undefined);
    });
    let timer: NodeJS.Timeout | undefined;
    try {
      const execution = await new Promise<string>((resolve, reject) => {
        child.stdout.on('data', () => {
          const line = output
            .split('\n')
            .find((line) => line.includes('"event":"graph_cached_ready"'));
          if (line) resolve(JSON.parse(line.slice(line.indexOf('{'))).execution);
        });
        child.once('error', reject);
        child.once('close', () =>
          reject(new Error('Held graph reader exited: ' + errors + ' ' + output)),
        );
        timer = setTimeout(() => reject(new Error('Held graph pin deadline: ' + errors)), 30000);
      });
      const pin = (
        await db.pool.query(
          "SELECT moria.runtime_dataset_execution_pin_live(owner_id) AS live FROM moria.runtime_dataset_root_pins WHERE owner_kind='execution' AND owner_id=$1",
          [execution],
        )
      ).rows;
      assert.equal(pin.length, 1);
      assert.equal(pin[0].live, true);
      const heldPrune = await fixture.command(
        ['dataset-retention', 'expire'],
        {
          policy: policy.policy,
          requestKey: 'graph-prune-with-real-retained-pin',
          owner: randomUUID(),
          beforeRevision: second.head.revision,
          maximumRoots: 64,
          timeSeconds: 120,
        },
        fixture.operatorEnv,
      );
      assert.equal(heldPrune.headChanged, true);
      assert.deepEqual(heldPrune.expiredRoots, []);
      beforeExpiration = (await db.catalog.heads(environment, [dataset]))[0];
      assert.equal(beforeExpiration.snapshotId, second.head.snapshotId);
      const heldMetadata = await graphMetadata(beforeExpiration.metadataLocation!);
      assert(originalMetadata);
      assert.deepEqual(heldMetadata.metadata.snapshots, originalMetadata.metadata.snapshots);
      assert.deepEqual(heldMetadata.metadata['metadata-log'], []);
      assert.deepEqual(
        heldMetadata.descriptor.indexSummary,
        originalMetadata.descriptor.indexSummary,
      );
      child.stdin.end('continue\n');
      assert.equal(await exited, 0, errors + ' ' + output);
      await fixture.command(['execution', 'cancel'], { id: execution });
      await ageClosedDatasetHistory(fixture);
    } finally {
      clearTimeout(timer);
      child.kill('SIGKILL');
      await exited.catch(() => undefined);
    }
  }
  let prunedCapture: Awaited<ReturnType<typeof recovery.capture>> | undefined;
  let prunedHead: typeof second.head | undefined;
  let prunedJournal: Awaited<ReturnType<typeof journalRows>> | undefined;
  const expired = await fixture.command(
    ['dataset-retention', 'expire'],
    {
      policy: policy.policy,
      requestKey: 'graph-expire-first',
      owner: randomUUID(),
      beforeRevision: second.head.revision,
      maximumRoots: 64,
      timeSeconds: 120,
    },
    fixture.operatorEnv,
  );
  assert(
    expired.expiredRoots.includes(
      (
        await db.pool.query('SELECT moria.runtime_dataset_root_key($1) AS key', [
          first.head.metadataLocation,
        ])
      ).rows[0].key,
    ),
  );
  if (nativeHead) {
    assert.equal(expired.headChanged, true);
    const pruned = (await db.catalog.heads(environment, [dataset]))[0];
    assert.equal(BigInt(pruned.revision), BigInt(beforeExpiration.revision) + 1n);
    assert.equal(pruned.snapshotId, second.head.snapshotId);
    assert.notEqual(pruned.metadataLocation, second.head.metadataLocation);
    const journal = await journalRows(db.pool, environment, dataset);
    assert.equal(journal.length, 4);
    const edge = journal[3];
    assert.equal(edge.producer_kind, 'maintenance');
    assert.equal(edge.commit, expired.run);
    assert.equal(edge.maintenance, expired.run);
    assert.equal(edge.mode, null);
    assert.equal(edge.change_kind, 'unchanged');
    assert.equal(edge.unchanged_reason, 'metadataPrune');
    assert.equal(edge.equivalence_receipt.kind, 'metadataPrune');
    assert.match(edge.equivalence_receipt.currentSnapshotHash, /^sha256:[0-9a-f]{64}$/);
    assert.equal(edge.equivalence_receipt.snapshot, second.head.snapshotId);
    assert.equal(String(edge.basis_revision), beforeExpiration.revision);
    assert.equal(String(edge.result_revision), pruned.revision);
    assert.equal(edge.basis_root, edge.equivalence_receipt.basisRoot);
    assert.equal(edge.result_root, edge.equivalence_receipt.resultRoot);
    assert.deepEqual(edge.sealed_receipt, expired);
    const after = await graphMetadata(pruned.metadataLocation!);
    assert(originalMetadata);
    assert.deepEqual(after.descriptor.indexSummary, originalMetadata.descriptor.indexSummary);
    for (const key of [
      'tableUuid',
      'snapshotId',
      'sequence',
      'schemaHash',
      'keySchemaHash',
      'primaryKey',
      'dataFiles',
      'rows',
      'selections',
      'visibility',
    ])
      assert.deepEqual(after.descriptor[key], originalMetadata.descriptor[key]);
    const current = (metadata: any) =>
      metadata.snapshots.find(
        (snapshot: any) => String(snapshot['snapshot-id']) === second.head.snapshotId,
      );
    assert.deepEqual(current(after.metadata), current(originalMetadata.metadata));
    assert.equal(after.metadata.snapshots.length, 1);
    assert.deepEqual(after.metadata['metadata-log'], []);
    assert.deepEqual(after.metadata.schemas, originalMetadata.metadata.schemas);
    assert.deepEqual(after.metadata.refs, originalMetadata.metadata.refs);
    await fixture.read(pruned);
    await assert.rejects(fixture.read(first.head), /DATASET_SNAPSHOT_EXPIRED/);
    const stage = (
      await db.pool.query(
        "SELECT s.status,s.candidate,s.maintenance,(SELECT count(*) FROM moria.runtime_dataset_graph_references WHERE kind='stage' AND owner=s.id::text)::text AS stage_refs,(SELECT count(*) FROM moria.runtime_dataset_graph_references WHERE kind='history' AND owner=s.root)::text AS history_refs FROM moria.runtime_dataset_graph_stages s WHERE s.maintenance=$1",
        [expired.run],
      )
    ).rows;
    assert.equal(stage.length, 1);
    assert.equal(stage[0].status, 'published');
    assert.equal(stage[0].candidate, null);
    assert.equal(stage[0].maintenance, expired.run);
    assert.equal(stage[0].stage_refs, '0');
    assert.equal(stage[0].history_refs, '1');
    assert(retainedData);
    const object = retainedData.content.object;
    const pinned = {
      Bucket: fixture.s3.bucket,
      Key:
        fixture.datasetStorage.prefix +
        '/' +
        object.warehouseId +
        '/' +
        object.tableId +
        '/objects/' +
        object.objectId,
      VersionId: retainedData.versionId,
    };
    assert.equal(
      (await fixture.s3.client.send(new HeadObjectCommand(pinned))).VersionId,
      retainedData.versionId,
    );
    prunedHead = pruned;
    prunedJournal = journal;
    prunedCapture = await recovery.capture('graph-pruned-current-with-retained-history');
    await ageClosedDatasetHistory(fixture);
    assert.equal(
      (
        await db.pool.query(
          "SELECT count(*) FROM moria.runtime_dataset_root_pins WHERE root_key=$1 AND owner_kind='execution' AND moria.runtime_dataset_execution_pin_live(owner_id)",
          [edge.result_root],
        )
      ).rows[0].count,
      '0',
    );
    // The previous physical head still protects history bytes until its own root is retired.
    const retired = await fixture.command(
      ['dataset-retention', 'expire'],
      {
        policy: policy.policy,
        requestKey: 'graph-expire-previous-physical-head',
        owner: randomUUID(),
        beforeRevision: (BigInt(pruned.revision) + 1n).toString(),
        maximumRoots: 64,
        timeSeconds: 120,
      },
      fixture.operatorEnv,
    );
    assert.equal(retired.headChanged, false);
    assert(retired.expiredRoots.includes(edge.basis_root));
    assert(!retired.expiredRoots.includes(edge.result_root));
    assert.deepEqual((await db.catalog.heads(environment, [dataset]))[0], pruned);
    await fixture.read(pruned);
    assert.equal((await journalRows(db.pool, environment, dataset)).length, 4);
  }
  const originalPolicy = policy.policy;
  await fixture.command(
    ['dataset-retention', 'disable'],
    { policy: originalPolicy },
    fixture.operatorEnv,
  );
  policy = await fixture.command(
    ['dataset-retention', 'approve'],
    {
      requestKey: 'graph-successor-policy',
      environment,
      dataset,
      retainSeconds: 86400,
      recheckSeconds: 30,
    },
    fixture.operatorEnv,
  );
  assert.notEqual(policy.policy, originalPolicy);
  assert.equal(
    (
      await db.pool.query(
        'SELECT enabled FROM moria.runtime_dataset_retention_policies WHERE id=$1',
        [originalPolicy],
      )
    ).rows[0].enabled,
    false,
  );
  assert(
    BigInt(
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.runtime_dataset_graph_retirements r JOIN moria.runtime_dataset_maintenance_runs m ON m.id=r.maintenance WHERE m.policy=$1 AND NOT r.completed',
          [originalPolicy],
        )
      ).rows[0].count,
    ) > 0n,
  );
  for (let i = 0; i < (nativeHead ? 32 : 8); i++) {
    await fixture.command(
      ['dataset-retention', 'plan'],
      { policy: policy.policy, requestKey: 'graph-drain-' + i, after: null, limit: 256 },
      fixture.operatorEnv,
    );
    if (i === 0) {
      const progress = (
        await db.pool.query(
          'SELECT count(*) FILTER(WHERE r.completed)::text AS completed,count(*) FILTER(WHERE NOT r.completed)::text AS pending FROM moria.runtime_dataset_graph_retirements r JOIN moria.runtime_dataset_maintenance_runs m ON m.id=r.maintenance WHERE m.policy=$1',
          [originalPolicy],
        )
      ).rows[0];
      assert(BigInt(progress.completed) > 0n);
      if (!nativeHead) assert.equal(progress.pending, '0');
      assert(
        BigInt(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.runtime_dataset_graph_queue q JOIN moria.runtime_dataset_maintenance_runs m ON m.id=q.origin WHERE m.policy=$1 AND q.processed_at IS NULL',
              [originalPolicy],
            )
          ).rows[0].count,
        ) > 0n,
      );
      assert.equal(
        (
          await db.pool.query(
            "SELECT count(*) FROM moria.runtime_dataset_gc_candidates c JOIN moria.runtime_dataset_graph_nodes n ON n.content->'object'->>'warehouseId'=c.warehouse_id AND n.content->'object'->>'tableId'=c.table_id AND n.content->'object'->>'objectId'=c.object_id WHERE c.policy=$1",
            [policy.policy],
          )
        ).rows[0].count,
        '0',
      );
    }
    if (
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.runtime_dataset_graph_queue WHERE processed_at IS NULL',
        )
      ).rows[0].count === '0' &&
      (
        await db.pool.query(
          'SELECT count(*) FROM moria.runtime_dataset_graph_retirements WHERE NOT completed',
        )
      ).rows[0].count === '0'
    )
      break;
  }
  assert.equal(
    (
      await db.pool.query(
        'SELECT count(*) FROM moria.runtime_dataset_graph_queue WHERE processed_at IS NULL',
      )
    ).rows[0].count,
    '0',
  );
  assert.equal(
    (
      await db.pool.query(
        'SELECT count(*) FROM moria.runtime_dataset_graph_retirements WHERE NOT completed',
      )
    ).rows[0].count,
    '0',
  );
  const candidate = (
    await db.pool.query(
      'SELECT id FROM moria.runtime_dataset_gc_candidates WHERE policy=$1 AND object_id=$2',
      [policy.policy, sourceRoot.content.object.objectId],
    )
  ).rows[0];
  assert(candidate);
  const immediate = await fixture.command(
    ['dataset-retention', 'once'],
    { policy: policy.policy, owner: randomUUID(), timeSeconds: 30 },
    fixture.operatorEnv,
  );
  assert.equal(immediate.claimed, false);
  await ageClosedUnclaimedCandidates(fixture, policy.policy);
  let removed = false;
  for (let i = 0; i < 128; i++) {
    const result = await fixture.command(
      ['dataset-retention', 'once'],
      { policy: policy.policy, owner: randomUUID(), timeSeconds: 30 },
      fixture.operatorEnv,
    );
    const versions = (
      await db.pool.query(
        "SELECT version FROM moria.runtime_dataset_gc_versions WHERE candidate=$1 AND result='removed'",
        [candidate.id],
      )
    ).rows;
    if (
      versions.some(
        (row) => JSON.stringify(row.version) === JSON.stringify(sourceRoot.physical_version),
      )
    ) {
      removed = true;
      break;
    }
    assert.equal(result.claimed, true);
  }
  assert.equal(removed, true);
  if (nativeHead) {
    assert(retainedData);
    const object = retainedData.content.object;
    assert.equal(
      (
        await db.pool.query(
          'SELECT live_references FROM moria.runtime_dataset_graph_counts WHERE node=$1 AND incarnation=(SELECT incarnation FROM moria.indexing_instance)',
          [retainedData.node],
        )
      ).rows[0].live_references,
      '0',
    );
    const dataCandidate = (
      await db.pool.query(
        'SELECT id FROM moria.runtime_dataset_gc_candidates WHERE policy=$1 AND object_id=$2',
        [policy.policy, object.objectId],
      )
    ).rows[0];
    assert(dataCandidate);
    for (let i = 0; i < 128; i++) {
      if (
        (
          await db.pool.query(
            "SELECT count(*) FROM moria.runtime_dataset_gc_versions WHERE candidate=$1 AND result='removed'",
            [dataCandidate.id],
          )
        ).rows[0].count !== '0'
      )
        break;
      await fixture.command(
        ['dataset-retention', 'once'],
        { policy: policy.policy, owner: randomUUID(), timeSeconds: 30 },
        fixture.operatorEnv,
      );
    }
    await assert.rejects(
      fixture.s3.client.send(
        new HeadObjectCommand({
          Bucket: fixture.s3.bucket,
          Key:
            fixture.datasetStorage.prefix +
            '/' +
            object.warehouseId +
            '/' +
            object.tableId +
            '/objects/' +
            object.objectId,
          VersionId: retainedData.versionId,
        }),
      ),
      (error: unknown) =>
        error instanceof S3ServiceException && error.$metadata.httpStatusCode === 404,
    );
  }
  if (nativeHead) {
    assert(prunedCapture && prunedHead && prunedJournal);
    const prunedRestore = await recovery.restore(prunedCapture.manifest);
    try {
      assert.deepEqual(await journalRows(prunedRestore.pool, environment, dataset), prunedJournal);
      await prunedRestore.read(prunedHead, secondRows, 'graph-read');
      await prunedRestore.read(prunedHead, secondRows);
      await prunedRestore.read(second.head, secondRows, 'graph-read');
      assert.notEqual(prunedRestore.restoredStorage.prefix, fixture.datasetStorage.prefix);
      const qualification = (
        await prunedRestore.pool.query(
          'SELECT q.physical_version,old.physical_version AS old_version FROM moria.runtime_dataset_graph_roots g JOIN moria.runtime_dataset_graph_qualifications q ON q.node=g.node AND q.incarnation=g.incarnation AND q.physical_fingerprint=g.physical_fingerprint JOIN moria.runtime_dataset_graph_qualifications old ON old.node=q.node AND old.incarnation<>q.incarnation WHERE g.root=moria.runtime_dataset_root_key($1) AND q.incarnation=(SELECT incarnation FROM moria.indexing_instance)',
          [prunedHead.metadataLocation],
        )
      ).rows;
      assert.equal(qualification.length, 1);
      assert.notDeepEqual(qualification[0].physical_version, qualification[0].old_version);
    } finally {
      await prunedRestore.close();
    }
  }
  const restored = await recovery.restore(captured.manifest);
  try {
    await assertRestoredReplacementJournal(fixture, restored, capturedJournal);
    await restored.read(first.head, firstRows, 'graph-read');
    await restored.read(second.head, secondRows, 'graph-read');
    await restored.read(second.head, secondRows);
    const fresh = (
      await restored.pool.query(
        'SELECT q.physical_version,c.live_references FROM moria.runtime_dataset_graph_qualifications q JOIN moria.runtime_dataset_graph_counts c USING(node,physical_fingerprint,incarnation) WHERE q.node=$1 AND q.incarnation=(SELECT incarnation FROM moria.indexing_instance)',
        [sourceRoot.node],
      )
    ).rows;
    assert.equal(fresh.length, 1);
    assert(BigInt(fresh[0].live_references) > 0n);
    assert.notDeepEqual(fresh[0].physical_version, sourceRoot.physical_version);
    await assert.rejects(
      restored.pool.query(
        'UPDATE moria.runtime_dataset_graph_qualifications SET physical_version=$2 WHERE node=$1 AND incarnation=(SELECT incarnation FROM moria.indexing_instance)',
        [sourceRoot.node, sourceRoot.physical_version],
      ),
      /IMMUTABLE_RECOVERY_CONTENT/,
    );
    const unchanged = (
      await restored.pool.query(
        'SELECT physical_version FROM moria.runtime_dataset_graph_qualifications WHERE node=$1 AND incarnation=(SELECT incarnation FROM moria.indexing_instance)',
        [sourceRoot.node],
      )
    ).rows;
    assert.deepEqual(unchanged[0].physical_version, fresh[0].physical_version);
    await restored.rejectPhysicalReuse();
  } finally {
    await restored.close();
  }
  const missing = (
    await db.pool.query(
      "SELECT m.backup,m.backup_version FROM moria.runtime_dataset_graph_edges e JOIN moria.runtime_dataset_graph_nodes n ON n.id=e.child JOIN moria.runtime_recovery_members m ON m.content=n.content AND m.recovery_set=$2 WHERE e.parent=$1 AND n.kind='directory' ORDER BY e.label LIMIT 1",
      [sourceRoot.node, captured.recoverySet],
    )
  ).rows[0];
  assert(missing);
  assert.equal(missing.backup.object.warehouseId, fixture.backupNamespace.warehouseId);
  assert.equal(missing.backup.object.tableId, fixture.backupNamespace.tableId);
  assert.match(missing.backup_version.token, /^s3v1\//);
  const [, version] = JSON.parse(missing.backup_version.token.slice('s3v1/'.length));
  assert(version && version !== 'null');
  const databasesBefore = new Set(
    (
      await db.pool.query(
        "SELECT datname FROM pg_database WHERE datname LIKE 'retention_restore_%'",
      )
    ).rows.map((row) => row.datname),
  );
  const missingBackupKey = `${fixture.backupStorage.prefix}/${missing.backup.object.warehouseId}/${missing.backup.object.tableId}/objects/${missing.backup.object.objectId}`;
  const exactBackupVersion = {
    Bucket: fixture.s3.bucket,
    Key: missingBackupKey,
    VersionId: version,
  };
  const present = await fixture.s3.client.send(new HeadObjectCommand(exactBackupVersion));
  assert.equal(present.VersionId, version);
  await fixture.s3.client.send(new DeleteObjectCommand(exactBackupVersion));
  await assert.rejects(
    fixture.s3.client.send(new HeadObjectCommand(exactBackupVersion)),
    (error: unknown) =>
      error instanceof S3ServiceException && error.$metadata.httpStatusCode === 404,
  );
  await assert.rejects(
    recovery.restore(captured.manifest),
    /^Error: restore-from: \{"databaseCode":null,"error":"INDEXING_FAILED"\}\n$/,
  );
  const failedDatabases = (
    await db.pool.query("SELECT datname FROM pg_database WHERE datname LIKE 'retention_restore_%'")
  ).rows
    .map((row) => row.datname)
    .filter((name) => !databasesBefore.has(name));
  assert.equal(failedDatabases.length, 1);
  assert.match(failedDatabases[0], /^retention_restore_[0-9a-f]{32}$/);
  const failedUrl = new URL(fixture.operatorEnv.RETENTION_DATABASE!);
  failedUrl.pathname = '/' + failedDatabases[0];
  const failedPool = new pg.Pool({ connectionString: failedUrl.href });
  try {
    assert.equal(
      (await failedPool.query('SELECT dispatch_approved FROM public.moria_restore_bootstrap'))
        .rows[0].dispatch_approved,
      false,
    );
    const disabled = (
      await failedPool.query(
        'SELECT c.dispatch_enabled,r.status FROM moria.runtime_recovery_control c JOIN moria.runtime_restores r ON r.id=c.restore_id WHERE c.singleton',
      )
    ).rows;
    assert.equal(disabled.length, 1);
    assert.equal(disabled[0].dispatch_enabled, false);
    assert.equal(disabled[0].status, 'disabled');
    assert.equal(
      (
        await failedPool.query(
          'SELECT count(*) FROM moria.runtime_dataset_graph_qualifications WHERE incarnation=(SELECT incarnation FROM moria.indexing_instance)',
        )
      ).rows[0].count,
      '0',
    );
  } finally {
    await failedPool.end();
  }
  t.diagnostic(
    JSON.stringify({
      heads: 2,
      allGraphBytesBackedUp: true,
      sourceGraphRootExactVersionRemoved: true,
      nativeCatalogHead: nativeHead,
      backupPinnedVersionDespiteNewCurrent: nativeHead,
      actualMetadataPruneAdvancedUnchangedSnapshotAndJournal: nativeHead,
      liveRetainedPinProtectedDuringPrune: nativeHead,
      prunedCurrentAndRetainedHeadRestoredAfterSourceGc: nativeHead,
      oldStandardDataCollectedAfterPhysicalHeadRetirement: nativeHead,
      expiredNativeHeadRejected: nativeHead,
      noFabricatedDeltaForSecondNonAdvancingExpiry: nativeHead,
      orphanGracePreserved: true,
      freshPhysicalQualification: true,
      independentTypedGraphReadback: true,
      immutableReplacementJournalRestored: true,
      journalRootsResolveFreshProviderVersions: true,
      oldProducerReplayRejected: true,
      incrementalReplayActivated: false,
      successorPolicyDrainsOriginalWork: true,
      pendingPropagationProtectsGraphObjects: true,
      conflictingTargetVersionRejected: true,
      reusedTargetRejected: true,
      missingGraphBackupChildRejectedWithDispatchDisabled: true,
    }),
  );
}
