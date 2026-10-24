import assert from 'node:assert/strict';
import type { Catalog } from '../../services/catalog/src/catalog.js';
import { createWeatherService } from '../../services/weather/src/service.js';

/** Called with an isolated real PostgreSQL catalog by the integration suite. */
export async function weatherChecks(catalog: Catalog) {
  let downloads = 0;
  const weather = await createWeatherService(catalog, async () => {
    downloads++;
    return { areas: [], raw: [] };
  });
  const guard = async () => {};
  const complete = async () => {};
  await weather.sync('weather-good', guard, complete);
  assert.equal((await weather.read()).jobId, 'weather-good');
  await weather.sync('weather-good', guard, complete);
  assert.equal(downloads, 1, 'receipt replay must avoid downloading and publishing again');
  await assert.rejects(
    weather.sync('weather-failed', guard, async () => {
      throw new Error('completion failed');
    }),
    /completion failed/,
  );
  assert.equal(
    (await weather.read()).jobId,
    'weather-good',
    'failed job completion must roll back the head',
  );
  assert.equal(
    (
      await catalog.pool.query(
        "SELECT 1 FROM moria.weather_publications WHERE job_id='weather-failed'",
      )
    ).rowCount,
    0,
  );
  await assert.rejects(
    weather.sync(
      'weather-fenced',
      async (client) => {
        if (client) throw new Error('fence expired');
      },
      complete,
    ),
    /fence expired/,
  );
  assert.equal((await weather.read()).jobId, 'weather-good');
  const restarted = await createWeatherService(catalog, async () => {
    throw new Error('unexpected download');
  });
  assert.equal((await restarted.read()).jobId, 'weather-good');
  await restarted.sync('weather-good', guard, complete);
  const unavailable = await createWeatherService(catalog, async () => {
    throw new Error('upstream unavailable');
  });
  await assert.rejects(
    unavailable.sync('weather-unavailable', guard, complete),
    /upstream unavailable/,
  );
  assert.equal((await weather.read()).jobId, 'weather-good');
}
