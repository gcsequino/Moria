import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { sourceStagingCli } from './staging.js';

type Input = Parameters<typeof sourceStagingCli>[0] & {
  environment: string;
  target: string;
  publishedBuild: string;
  publish: () => Promise<unknown>;
  diagnostic: (value: unknown) => void;
};

/** Native-admitted typed values plus a separately labelled physical layout fault; identity/content stay real. */
export async function sourceStagingWidth(input: Input) {
  const { db, environment, target, publishedBuild } = input;
  const cli = sourceStagingCli(input);
  const measure = async () =>
    (
      await db.pool.query(
        "SELECT uid::text,(octet_length(value::text)::bigint+octet_length(source_value::text))::text AS wire_bytes,pg_column_size(value)::text AS value_storage,pg_column_size(source_value)::text AS source_storage,(pg_column_size(value)::bigint+pg_column_size(source_value))::text AS storage_bytes,pg_column_size(source_value::text::jsonb)::text AS inline_source_storage,encode(sha256(convert_to(source_value::text,'UTF8')),'hex') AS source_hash FROM moria.indexing_rows WHERE build=$1 ORDER BY partition,uid,task,fence",
        [publishedBuild],
      )
    ).rows;
  const accepted = await measure();
  assert.equal(accepted.length, 2);
  assert.ok(
    accepted.every(
      (row) => Number(row.wire_bytes) > 6 * 1024 * 1024 && Number(row.wire_bytes) < 8 * 1024 * 1024,
    ),
  );
  assert.ok(
    accepted.every((row) => BigInt(row.storage_bytes) <= 16777216n),
    'ordinary current storage remains collectible',
  );
  assert.ok(
    accepted.every((row) => BigInt(row.inline_source_storage) > 16777216n),
    'actual typed JSONB has uncompressed amplification',
  );
  input.diagnostic({
    profile: 'native-admitted-decoded-boundary',
    decodedValueBytes: 4194273,
    perArrayElements: 99862,
    arrays: 21,
    measured: accepted,
  });
  const canonical = (
    await db.pool.query(
      'SELECT pg_column_size(value)::text AS value_storage,pg_column_size(source_value)::text AS source_storage,(octet_length(value::text)::bigint+octet_length(source_value::text))::text AS wire_bytes FROM moria.staged_source_rows WHERE stage=$1 ORDER BY uid',
      [publishedBuild],
    )
  ).rows;
  assert.equal(canonical.length, 2);
  input.diagnostic({ profile: 'native-admitted-canonical-storage', measured: canonical });
  // A closed legacy row may retain an old uncompressed physical layout after current storage policy is restored.
  // Re-encode identical temporary data under that layout; canonical payloads and all identity/fence fields stay real.
  const physical = await db.pool.connect();
  try {
    await physical.query('BEGIN');
    await physical.query(
      "SELECT pg_advisory_xact_lock(hashtextextended('moria/indexing-staging/v1/'||$1::text,0))",
      [publishedBuild],
    );
    assert.equal(
      (
        await physical.query(
          "SELECT attstorage FROM pg_attribute WHERE attrelid='moria.indexing_rows'::regclass AND attname='source_value'",
        )
      ).rows[0].attstorage,
      'x',
    );
    await physical.query(
      'ALTER TABLE moria.indexing_rows ALTER COLUMN source_value SET STORAGE EXTERNAL',
    );
    await physical.query(
      'UPDATE moria.indexing_rows SET source_value=source_value::text::jsonb WHERE build=$1',
      [publishedBuild],
    );
    await physical.query(
      'ALTER TABLE moria.indexing_rows ALTER COLUMN source_value SET STORAGE EXTENDED',
    );
    await physical.query('COMMIT');
  } catch (error) {
    await physical.query('ROLLBACK');
    throw error;
  } finally {
    physical.release();
  }
  const measured = await measure();
  assert.deepEqual(
    measured.map((row) => ({ uid: row.uid, wire: row.wire_bytes, hash: row.source_hash })),
    accepted.map((row) => ({ uid: row.uid, wire: row.wire_bytes, hash: row.source_hash })),
  );
  assert.ok(measured.every((row) => BigInt(row.storage_bytes) > 16777216n));
  input.diagnostic({ profile: 'controlled-legacy-external-layout', measured });
  const policy = JSON.parse(
    await cli(
      ['history-retention', 'approve'],
      {
        environment,
        requestKey: 'near-cap-' + randomUUID(),
        retainSeconds: 86400,
        keepGenerations: 2,
      },
      true,
    ),
  ).policy as string;
  await cli(['history-retention', 'plan'], { id: policy }, true);
  // Another real accepted build can close before it stages rows; its actual pending owner/fence is retained.
  await input.publish();
  const empty = (
    await db.pool.query(
      "SELECT id FROM moria.indexing_builds WHERE environment=$1 AND target=$2 AND status='pending' ORDER BY created_at DESC LIMIT 1",
      [environment, target],
    )
  ).rows[0].id as string;
  await cli(['cancel', empty]);
  const fault = await db.pool.connect();
  try {
    await fault.query('BEGIN');
    await fault.query('LOCK TABLE moria.runtime_indexing_stage_closures IN ACCESS EXCLUSIVE MODE');
    await fault.query(
      'ALTER TABLE moria.runtime_indexing_stage_closures DISABLE TRIGGER runtime_indexing_closure_immutable',
    );
    await fault.query(
      "UPDATE moria.indexing_builds SET deadline=clock_timestamp()-interval '1 hour' WHERE id=$1",
      [empty],
    );
    await fault.query(
      "UPDATE moria.runtime_indexing_stage_closures c SET observed_at=clock_timestamp()-CASE WHEN c.build=$1::uuid THEN interval '4 days' ELSE interval '3 days' END,scope_hash=moria.runtime_indexing_staging_scope(b) FROM moria.indexing_builds b WHERE c.build=b.id AND c.build=ANY($2::uuid[])",
      [publishedBuild, [publishedBuild, empty]],
    );
    await fault.query(
      'UPDATE moria.runtime_indexing_staging_pending q SET observed_at=c.observed_at FROM moria.runtime_indexing_stage_closures c WHERE q.closure=c.id AND q.build=ANY($1::uuid[])',
      [[publishedBuild, empty]],
    );
    await fault.query(
      'ALTER TABLE moria.runtime_indexing_stage_closures ENABLE TRIGGER runtime_indexing_closure_immutable',
    );
    await fault.query('COMMIT');
  } catch (error) {
    await fault.query('ROLLBACK');
    throw error;
  } finally {
    fault.release();
  }
  const once = async () =>
    JSON.parse(await cli(['history-retention', 'once'], { id: policy }, true));
  const blocked = (await once()).indexingStaging;
  assert.equal(blocked.state, 'operatorBlocked');
  assert.equal(blocked.worked, false);
  assert.equal(blocked.pending, true, 'another actually eligible build remains');
  assert.equal(blocked.block.reason, 'rowStorageBudget');
  assert.equal(blocked.block.policy, policy);
  assert.equal(blocked.block.phase, 'spills');
  assert.deepEqual(blocked.block.cursor, {});
  assert.equal(blocked.block.candidate.uid, measured[0].uid);
  assert.equal(blocked.block.storageBytes, measured[0].storage_bytes);
  assert.equal(blocked.block.limitBytes, '16777216');
  const sample = JSON.parse(
    await cli(['history-retention', 'inspect'], { id: policy }, true),
  ).indexingStagingBlockedSample;
  assert.ok(sample.length <= 20);
  assert.deepEqual(sample[0].block, blocked.block);
  let completed = false;
  for (let i = 0; i < 8; i++) {
    await once();
    completed =
      (
        await db.pool.query(
          'SELECT phase FROM moria.runtime_indexing_staging_work WHERE build=$1',
          [empty],
        )
      ).rows[0]?.phase === 'complete';
    if (completed) break;
  }
  assert.ok(completed, 'a width-blocked build must not poison another real closed build');
  assert.equal(
    (
      await db.pool.query('SELECT count(*)::text FROM moria.indexing_rows WHERE build=$1', [
        publishedBuild,
      ])
    ).rows[0].count,
    '2',
  );
  assert.equal(
    (
      await db.pool.query('SELECT count(*)::text FROM moria.staged_source_rows WHERE stage=$1', [
        publishedBuild,
      ])
    ).rows[0].count,
    '2',
  );
  assert.deepEqual(
    (
      await db.pool.query(
        'SELECT position FROM moria.runtime_indexing_staging_work WHERE build=$1',
        [publishedBuild],
      )
    ).rows[0].position,
    {},
  );
  input.diagnostic({ profile: 'legacy-layout-width-blocked-not-reclaimed', blocked });
}
