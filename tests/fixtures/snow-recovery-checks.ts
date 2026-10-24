import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import type { Catalog } from '../../services/catalog/src/catalog.js';
import type { JobContext } from '../../services/execution/src/jobs.js';
import { createSnowService } from '../../services/snotel/src/service.js';
import { snowConfig, snowFixture } from './snow.js';

export async function snowRecoveryChecks(catalog: Catalog) {
  const warehouse = resolve('.local', `snow-recovery-${randomUUID()}`);
  const snow = await createSnowService(catalog, warehouse);
  const datasets = ['snow-stations-source', 'snow-observations-source', 'snow-raw-source'];
  const served = async () =>
    (
      await catalog.pool.query(
        "SELECT object_type,revision,stage FROM moria.source_generations WHERE environment='snotel' ORDER BY object_type,revision",
      )
    ).rows;
  const before = await served();
  const id = randomUUID();
  let completions = 0;
  const job: JobContext = {
    id,
    input: snowConfig,
    guard: async () => {},
    phase: async (phase) => {
      if (phase === 'Indexing authorized snow objects')
        throw new Error('Stopped after catalog publication');
    },
    complete: async () => {
      completions++;
    },
  };
  await assert.rejects(
    snow.sync(snowConfig, snowFixture(17), job.guard, job),
    /Stopped after catalog publication/,
  );
  assert.deepEqual(
    await served(),
    before,
    'catalog publication must not expose partial serving state',
  );
  const published = await catalog.heads('snotel', datasets);
  assert.equal(
    (
      await catalog.pool.query(
        'SELECT 1 FROM moria.publications WHERE environment=$1 AND execution=$2',
        ['snotel', id],
      )
    ).rowCount,
    1,
  );
  const restarted = await createSnowService(catalog, warehouse);
  const noFetch = async () => {
    throw new Error('Recovery must reuse the durable staging plan');
  };
  job.phase = async () => {};
  // Intentionally different caller input proves the original durable config is restored.
  const result = await restarted.sync(
    { ...snowConfig, endDate: '2025-04-02' },
    noFetch,
    job.guard,
    job,
  );
  assert.deepEqual(result.config, snowConfig);
  assert.deepEqual(await catalog.heads('snotel', datasets), published);
  assert.equal((await served()).length, before.length + 2);
  assert.equal(completions, 1);
  const rows = await catalog.pool.query(
    "SELECT r.value FROM moria.source_generations g JOIN moria.staged_source_rows r ON r.stage=g.stage WHERE g.environment='snotel' AND g.object_type='snow-observations' AND g.stage=ANY($1::uuid[])",
    [result.stages],
  );
  assert.equal(rows.rows.find((r) => r.value.date === '2025-04-01')?.value.snowDepth, 17);
  const servedOnce = await served();
  assert.deepEqual(await restarted.sync(snowConfig, noFetch, job.guard, job), result);
  assert.deepEqual(await served(), servedOnce);
  assert.deepEqual(await catalog.heads('snotel', datasets), published);
  const failedId = randomUUID();
  const failing: JobContext = {
    ...job,
    id: failedId,
    complete: async () => {
      throw new Error('Completion transaction failed');
    },
  };
  await assert.rejects(
    restarted.sync(snowConfig, snowFixture(22), failing.guard, failing),
    /Completion transaction failed/,
  );
  const secondPublication = await catalog.heads('snotel', datasets);
  assert.deepEqual(
    await served(),
    servedOnce,
    'serving generations must roll back with completion marker',
  );
  assert.equal(
    (await catalog.pool.query('SELECT status FROM moria.snow_syncs WHERE id=$1', [failedId]))
      .rows[0].status,
    'failed',
  );
  failing.complete = async () => {};
  const resumed = await createSnowService(catalog, warehouse);
  const second = await resumed.sync(snowConfig, noFetch, failing.guard, failing);
  assert.equal(second.id, failedId);
  assert.deepEqual(await catalog.heads('snotel', datasets), secondPublication);
  assert.equal((await served()).length, servedOnce.length + 2);
  assert.equal(
    (await catalog.pool.query('SELECT status FROM moria.snow_syncs WHERE id=$1', [failedId]))
      .rows[0].status,
    'completed',
  );
}
