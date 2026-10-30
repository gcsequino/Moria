import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  distanceMiles,
  latestReport,
  reportIsStale,
  nearestStation,
  daysBefore,
  snowChange,
} from '../apps/snotel/src/snow-comparison.js';
import type {
  ObservationProperties as Observation,
  StationProperties as Station,
} from '../apps/snotel/src/generated/index.js';
const station = (triplet: string, latitude: number, longitude: number): Station => ({
  triplet,
  latitude,
  longitude,
  uid: triplet,
  name: triplet,
  elevationFt: 10000,
  county: '',
  state: 'CO',
  timeZoneOffset: null,
  retrievedAt: '2026-10-21T12:00:00Z',
});
const row = (date: string, snowDepth: number | null, swe: number | null = 2): Observation => ({
  date,
  snowDepth,
  swe,
  unit: 'in',
  uid: date,
  triplet: 'test',
  depthQa: '',
  depthQc: '',
  sweQa: '',
  sweQc: '',
  periodRef: 'end',
  retrievedAt: '2026-10-21T12:00:00Z',
});
test('area comparison uses coordinate distance and only the loaded valid station set', () => {
  const point = { latitude: 40, longitude: -105 };
  assert.equal(distanceMiles(point, point), 0);
  assert.ok(Math.abs(distanceMiles(point, { latitude: 41, longitude: -105 }) - 69.09) < 0.1);
  assert.equal(
    nearestStation(point, [
      station('far', 41, -105),
      station('close', 40.1, -105),
      station('invalid', NaN, 0),
    ])?.station.triplet,
    'close',
  );
  assert.equal(nearestStation(point, []), undefined);
  assert.equal(nearestStation(point, [station('invalid', 91, 0)]), undefined);
});
test('snowpack change compares exact daily endpoints and preserves zero, missing values and units', () => {
  const rows = [row('2026-10-20', 0, 0), row('2026-10-19', 2, 1), row('2026-10-13', 8, 3)];
  assert.equal(snowChange(rows, '2026-10-20', 1, 'snowDepth'), -2);
  assert.equal(snowChange(rows, '2026-10-20', 7, 'swe'), -3);
  assert.equal(snowChange(rows, '2026-10-21', 1, 'snowDepth'), null);
  assert.equal(
    snowChange([row('2026-10-20', null), row('2026-10-19', 2)], '2026-10-20', 1, 'snowDepth'),
    null,
  );
  assert.equal(
    snowChange(
      [row('2026-10-20', 2), { ...row('2026-10-19', 1), unit: 'cm' }],
      '2026-10-20',
      1,
      'snowDepth',
    ),
    null,
  );
  assert.equal(daysBefore('2024-03-01', 1), '2024-02-29');
  assert.equal(daysBefore('2026-01-01', 7), '2025-12-25');
});

test('latest report skips empty dates, preserves a single metric date, and excludes old/future rows', () => {
  const rows = [
    row('2026-10-21', null, null),
    row('2026-10-20', null, 2),
    row('2026-10-19', 12, 3),
    row('2026-10-22', 99, 99),
  ];
  const selected = latestReport(rows, '2026-10-21');
  assert.equal(selected?.date, '2026-10-20');
  assert.equal(selected?.snowDepth, null);
  assert.equal(selected?.swe, 2);
  assert.equal(latestReport([row('2026-10-13', 1)], '2026-10-21'), undefined);
  assert.equal(latestReport([row('2026-10-14', 0, 0)], '2026-10-21')?.date, '2026-10-14');
  assert.equal(latestReport([row('2026-10-21', null, null)], '2026-10-21'), undefined);
  assert.equal(reportIsStale('2026-10-17', Date.parse('2026-10-21T00:00:00Z')), true);
  assert.equal(reportIsStale('2026-10-20', Date.parse('2026-10-21T00:00:00Z')), false);
});
