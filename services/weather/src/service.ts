import type { PoolClient } from 'pg';
import type { Catalog } from '../../catalog/src/catalog.js';
import { downloadWeather, type WeatherView } from './connector.js';
export type { WeatherView } from './connector.js';

/** Internal workload API. Callers must enforce instance and application authority. */
export async function createWeatherService(catalog: Catalog, download = downloadWeather) {
  await catalog.migrate();
  await catalog.pool.query(`
    CREATE TABLE IF NOT EXISTS moria.weather_publications (
      job_id text PRIMARY KEY, published_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      view jsonb NOT NULL, raw jsonb NOT NULL
    );
    CREATE TABLE IF NOT EXISTS moria.weather_head (
      singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
      job_id text NOT NULL REFERENCES moria.weather_publications(job_id)
    );
  `);
  async function read(client?: PoolClient): Promise<WeatherView> {
    const result = await (client ?? catalog.pool).query(
      `SELECT p.view FROM moria.weather_head h JOIN moria.weather_publications p USING(job_id) WHERE singleton=true`,
    );
    return result.rows[0]?.view ?? { publishedAt: null, jobId: null, areas: [] };
  }
  async function sync(
    jobId: string,
    guard: (client?: PoolClient) => Promise<void>,
    complete: (client: PoolClient, result: unknown) => Promise<void>,
  ) {
    await guard();
    const previous = await catalog.pool.query(
      'SELECT view FROM moria.weather_publications WHERE job_id=$1',
      [jobId],
    );
    const downloaded = previous.rowCount ? undefined : await download(() => guard());
    const client = await catalog.pool.connect();
    try {
      await client.query('BEGIN');
      await guard(client);
      // Serialize publication, including receipt replay, without locking during network I/O.
      await client.query("SELECT pg_advisory_xact_lock(hashtext('moria-weather-publication'))");
      await guard(client);
      const receipt = await client.query(
        'SELECT view FROM moria.weather_publications WHERE job_id=$1',
        [jobId],
      );
      let view: WeatherView = receipt.rows[0]?.view;
      if (!view) {
        if (!downloaded) throw new Error('Weather publication receipt disappeared');
        view = { publishedAt: new Date().toISOString(), jobId, areas: downloaded.areas };
        await client.query(
          'INSERT INTO moria.weather_publications(job_id,view,raw) VALUES($1,$2,$3)',
          [jobId, JSON.stringify(view), JSON.stringify(downloaded.raw)],
        );
        await client.query(
          'INSERT INTO moria.weather_head(singleton,job_id) VALUES(true,$1) ON CONFLICT(singleton) DO UPDATE SET job_id=EXCLUDED.job_id',
          [jobId],
        );
      }
      const result = {
        jobId,
        publishedAt: view.publishedAt,
        areaCount: view.areas.length,
        sources: view.areas.map((a) => ({
          id: a.id,
          url: a.forecastUrl,
          sourceUpdatedAt: a.sourceUpdatedAt,
          retrievedAt: a.retrievedAt,
        })),
      };
      await complete(client, result);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
  return { read, sync };
}
