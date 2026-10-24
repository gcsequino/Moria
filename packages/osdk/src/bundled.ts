import {
  assetSchema,
  assetActionInputs,
  stationSchema,
  observationSchema,
} from '../../contracts/bundled-objects.js';
import type { ApiType, AppContract } from './contract.js';
const str: ApiType = { type: 'string', nullable: false };
const num: ApiType = { type: 'float64', nullable: false };
const bool: ApiType = { type: 'boolean', nullable: false };
const obj = (
  properties: Record<string, ApiType>,
  optional?: string[],
  nullable = false,
): ApiType => ({ type: 'object', properties, optional, nullable });
const list = (items: ApiType): ApiType => ({ type: 'list', items });
const nullableString: ApiType = { type: 'string', nullable: true };
const jobType = obj({
  id: str,
  kind: str,
  status: str,
  phase: str,
  attempt: num,
  created_at: str,
  updated_at: str,
  error: nullableString,
  requested_by: str,
  result: nullableString,
});
const scheduleType = obj({ kind: str, enabled: bool, interval_minutes: num, next_run: str });
const weatherType = obj({
  publishedAt: nullableString,
  jobId: nullableString,
  areas: list(
    obj({
      id: str,
      name: str,
      latitude: num,
      longitude: num,
      forecastUrl: str,
      sourceUpdatedAt: str,
      retrievedAt: str,
      periods: list(
        obj({
          name: str,
          startTime: str,
          endTime: str,
          isDaytime: bool,
          temperature: num,
          temperatureUnit: str,
          windSpeed: str,
          windDirection: str,
          precipitationProbability: { type: 'float64', nullable: true },
          shortForecast: str,
          detailedForecast: str,
        }),
      ),
    }),
  ),
});
export const managementContract: AppContract = {
  version: 1,
  app: 'management',
  types: {
    Job: jobType,
    Schedule: scheduleType,
    Jobs: obj({ jobs: list({ ref: 'Job' }), schedules: list({ ref: 'Schedule' }) }),
    Lineage: obj({
      nodes: list(obj({ id: str, label: str, kind: str, status: nullableString, detail: str })),
      edges: list(obj({ from: str, to: str, label: str })),
      limitations: list(str),
    }),
    SourceRow: obj(
      Object.fromEntries(Object.entries(assetSchema.properties).filter(([k]) => k !== 'uid')),
    ),
    HostedApp: obj(
      {
        app: str,
        deployment: obj({ current: str, previous: str, promotedAt: str }, ['previous']),
        versions: list(str),
        error: str,
      },
      ['deployment', 'error'],
    ),
    WorkspaceStatus: obj({
      ready: bool,
      administrator: bool,
      revision: str,
      importing: bool,
      principal: str,
    }),
    ImportResult: obj({ rows: num, revision: str }),
  },
  objects: { assets: { type: 'Asset', properties: assetSchema.properties } },
  actions: Object.fromEntries(
    Object.entries(assetActionInputs).map(([n, inputs]) => [n, { inputs }]),
  ),
  endpoints: {
    jobs: { path: '/api/management/v1/jobs', method: 'GET', output: { ref: 'Jobs' } },
    lineage: { path: '/api/management/v1/lineage', method: 'GET', output: { ref: 'Lineage' } },
    syncSnow: {
      path: '/api/management/v1/snotel/sync',
      method: 'POST',
      input: obj({ requestKey: str }),
      output: { ref: 'Job' },
    },
    syncWeather: {
      path: '/api/management/v1/weather/sync',
      method: 'POST',
      input: obj({ requestKey: str }),
      output: { ref: 'Job' },
    },
    setSnowSchedule: {
      path: '/api/management/v1/snotel/schedule',
      method: 'POST',
      input: obj({ enabled: bool, intervalMinutes: num }),
      output: { ref: 'Schedule' },
    },
    setWeatherSchedule: {
      path: '/api/management/v1/weather/schedule',
      method: 'POST',
      input: obj({ enabled: bool, intervalMinutes: num }),
      output: { ref: 'Schedule' },
    },
    retryJob: {
      path: '/api/management/v1/jobs/retry',
      method: 'POST',
      input: obj({ id: str }),
      output: { ref: 'Job' },
    },
    workspace: {
      path: '/api/management/v1/workspace',
      method: 'GET',
      output: { ref: 'WorkspaceStatus' },
    },
    apps: { path: '/api/management/v1/apps', method: 'GET', output: list({ ref: 'HostedApp' }) },
    importAssets: {
      path: '/api/management/v1/datasets/assets/import',
      method: 'POST',
      input: list({ ref: 'SourceRow' }),
      output: { ref: 'ImportResult' },
    },
  },
};
export const snowContract: AppContract = {
  version: 1,
  app: 'snotel',
  types: {
    Status: obj(
      {
        weather: weatherType,
        completed: obj(
          {
            id: str,
            config: obj({ stations: list(str), beginDate: str, endDate: str, source: str }, [
              'source',
            ]),
            completed_at: str,
            station_count: num,
            observation_count: num,
          },
          undefined,
          true,
        ),
        latest: obj(
          { status: str, error: { type: 'string', nullable: true }, started_at: str },
          undefined,
          true,
        ),
      },
      ['weather'],
    ),
  },
  objects: {
    'snow-stations': { type: 'Station', properties: stationSchema.properties },
    'snow-observations': { type: 'Observation', properties: observationSchema.properties },
  },
  actions: {},
  endpoints: { status: { path: '/api/v1/status', method: 'GET', output: { ref: 'Status' } } },
};
