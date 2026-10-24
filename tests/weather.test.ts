import { test } from 'node:test';
import assert from 'node:assert/strict';
import { downloadWeather, officialUrl, parseForecast } from '../services/weather/src/connector.js';

const period = {
  name: 'Tonight',
  startTime: '2026-10-21T18:00:00-06:00',
  endTime: '2026-10-22T06:00:00-06:00',
  isDaytime: false,
  temperature: 20,
  temperatureUnit: 'F',
  windSpeed: '10 to 20 mph',
  windDirection: 'W',
  probabilityOfPrecipitation: { value: null },
  shortForecast: 'Snow',
  detailedForecast: 'A chance of snow.',
};
const forecast = (p = period) =>
  JSON.stringify({ properties: { updateTime: '2026-10-21T12:00:00Z', periods: [p] } });
test('NWS connector preserves missing precipitation and rejects malformed forecasts and source URLs', () => {
  assert.equal(parseForecast(forecast()).periods[0].precipitationProbability, null);
  assert.equal(
    parseForecast(forecast({ ...period, probabilityOfPrecipitation: { value: 0 as never } }))
      .periods[0].precipitationProbability,
    0,
  );
  assert.throws(() => parseForecast(forecast({ ...period, temperature: NaN })));
  assert.throws(() => parseForecast(forecast({ ...period, endTime: period.startTime })));
  for (const url of [
    'http://api.weather.gov/points/1,2',
    'https://evil.test/points/1,2',
    'https://api.weather.gov.evil.test/points/1,2',
    'https://api.weather.gov/points/1,2?next=foo',
    'https://user@api.weather.gov/points/1,2',
    'https://api.weather.gov/alerts',
  ])
    assert.throws(() => officialUrl(url));
  assert.equal(
    officialUrl('https://api.weather.gov/gridpoints/BOU/36,66/forecast'),
    'https://api.weather.gov/gridpoints/BOU/36,66/forecast',
  );
});
test('NWS workload bounds requests, checks its fence before each source, and retains provenance', async () => {
  let calls = 0,
    guards = 0;
  const result = await downloadWeather(
    async () => {
      guards++;
    },
    async (url) => {
      calls++;
      return {
        url,
        sha256: 'test',
        retrievedAt: '2026-10-21T12:00:00Z',
        body: url.includes('/points/')
          ? JSON.stringify({
              properties: { forecast: 'https://api.weather.gov/gridpoints/BOU/36,66/forecast' },
            })
          : forecast(),
      };
    },
  );
  assert.equal(calls, 10);
  assert.equal(guards, 10);
  assert.equal(result.raw.length, 10);
  assert.equal(result.areas.length, 5);
  assert.equal(result.areas[0].sourceUpdatedAt, '2026-10-21T12:00:00Z');
  await assert.rejects(
    downloadWeather(
      async () => {
        throw new Error('Fence lost');
      },
      async () => {
        throw new Error('Should never fetch');
      },
    ),
    /Fence lost/,
  );
});
