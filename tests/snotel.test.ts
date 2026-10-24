import { snowConfig, snowFixture, reportFixture } from './fixtures/snow.js';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { downloadSnow, configInput, type JsonLoader } from '../services/snotel/src/connector.js';
import { segments, type Observation } from '../apps/snotel/src/client.js';
test('SNOTEL preserves zero, missing days, quality flags and request provenance', async () => {
  const data = await downloadSnow(snowConfig, snowFixture());
  assert.deepEqual(
    data.observations.map((o) => o.swe),
    [12, null, 0],
  );
  assert.equal(data.observations[0].depthQc, 'V');
  assert.equal(data.raw.length, 2);
  assert.match(data.raw[1].url, /returnSuspectData=false/);
  assert.equal(data.raw[0].sha256.length, 64);
  assert.equal(segments(data.observations as Observation[], 'swe').length, 2);
});
test('SNOTEL rejects invalid ranges, units, duplicates and source failure', async () => {
  assert.throws(() => configInput({ ...snowConfig, endDate: '2025-02-30' }));
  assert.throws(() => configInput({ ...snowConfig, stations: ['*:CO:SNTL'] }));
  assert.throws(() => configInput({ ...snowConfig, endDate: '2027-04-01' }));
  for (const kind of ['units', 'duplicate', 'empty'])
    await assert.rejects(
      downloadSnow(snowConfig, async (url) => {
        const data = (await snowFixture()(url)) as any[];
        if (url.pathname.endsWith('/data')) {
          if (kind === 'units') data[0].data[0].stationElement.storedUnitCode = 'cm';
          if (kind === 'duplicate') data[0].data.push(data[0].data[0]);
          if (kind === 'empty') data[0].data = [];
        }
        return data;
      }),
    );
  await assert.rejects(
    downloadSnow(snowConfig, async () => {
      throw Error('offline');
    }),
    /offline/,
  );
});

import { parseReport } from '../services/snotel/src/reports.js';
test('NRCS CSV preserves exact text, identities, zero, gaps and unknown metadata', async () => {
  const result = await downloadSnow(
    { ...snowConfig, source: 'report-csv' },
    async () => reportFixture,
  );
  assert.deepEqual(
    result.observations.map((o) => o.swe),
    [12.6, null, 0],
  );
  assert.equal(result.stations[0].name, 'Niwot, test');
  assert.equal(result.stations[0].timeZoneOffset, null);
  assert.equal(result.observations[0].sweQa, '');
  assert.equal(result.raw[0].body, reportFixture);
  assert.match(result.raw[0].url, /end_of_period/);
});
test('NRCS CSV fails closed on incorrect period, units, station, duplicate dates or malformed data', () => {
  for (const invalid of [
    reportFixture.replace('End of Day', 'Start of Day'),
    reportFixture.replace('Equivalent (in)', 'Equivalent (cm)'),
    reportFixture.replaceAll(',663,', ',664,'),
    reportFixture + reportFixture.split('\n')[3] + '\n',
    reportFixture.replace(',12.6,37', ',N/A,37'),
    '<html>Bad gateway</html>',
    reportFixture.replace('2025-04-03', '2027-04-03'),
  ])
    assert.throws(() => parseReport(invalid, '663:CO:SNTL', snowConfig, 'now'));
});
