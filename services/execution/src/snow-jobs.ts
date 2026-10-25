import type { Catalog } from '../../catalog/src/catalog.js';
import type { SnowService } from '../../snotel/src/service.js';
import { configInput, defaultConfig, type JsonLoader } from '../../snotel/src/connector.js';
import { createWeatherService } from '../../weather/src/service.js';
import { Jobs } from './jobs.js';

export function rollingSnowConfig(now = new Date()) {
  const endDate = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Denver',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(now);
  const beginDate = new Date(Date.parse(endDate) - 384 * 86400000).toISOString().slice(0, 10);
  return { ...defaultConfig, beginDate, endDate };
}
export async function createSnowJobs(
  catalog: Catalog,
  snow: SnowService,
  downloads: {
    snow?: JsonLoader;
    weather?: Parameters<typeof createWeatherService>[1];
  } = {},
) {
  const weather = await createWeatherService(catalog, downloads.weather);
  const jobs = new Jobs(catalog, {
    snotel: async (job) => {
      const config =
        job.input && Object.keys(job.input as object).length
          ? configInput(job.input)
          : rollingSnowConfig();
      return snow.sync(config, downloads.snow, job.guard, job);
    },
    weather: async (job) => {
      await job.phase('Downloading official NWS forecasts');
      const guard = async (c?: import('pg').PoolClient) => {
        await job.guard(c);
        await snow.ingestionGuard(c);
      };
      return weather.sync(job.id, guard, job.complete);
    },
  });
  await jobs.migrate();
  return { jobs, weather };
}
