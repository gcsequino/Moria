import assert from 'node:assert/strict';
import test from 'node:test';
import { workloadHealth } from '../apps/management/src/health.js';
import type { Job, Lineage } from '../apps/management/src/generated/index.js';
const now = Date.parse('2026-10-21T12:00:00Z');
const job = (status: string, updated_at: string): Job => ({
  id: status,
  kind: 'weather',
  status,
  phase: status,
  created_at: updated_at,
  updated_at,
  attempt: 1,
  error: status === 'failed' ? 'Download failed' : null,
  requested_by: 'operator',
  result: null,
});
test('operational health separates schedule lateness, last success and failure evidence', () => {
  const health = workloadHealth(
    'weather',
    {
      jobs: [job('failed', '2026-10-21T11:30:00Z'), job('succeeded', '2026-10-21T09:00:00Z')],
      schedules: [
        { kind: 'weather', enabled: true, interval_minutes: 60, next_run: '2026-10-21T11:00:00Z' },
      ],
    },
    undefined,
    now,
  );
  assert.equal(health.failure?.id, 'failed');
  assert.equal(health.publishedAt, Date.parse('2026-10-21T09:00:00Z'));
  assert.equal(health.publication, undefined);
  assert.equal(health.overdue, true);
  assert.equal(health.intervalExceeded, true);
  const unknown = workloadHealth('weather', { jobs: [], schedules: [] }, undefined, now);
  assert.equal(unknown.publishedAt, undefined);
  assert.equal(unknown.overdue, false);
  assert.equal(unknown.intervalExceeded, false);
});
test('operational health checks source ages only in latest committed publication and tolerates missing metadata', () => {
  const lineage: Lineage = {
    limitations: [],
    nodes: [
      {
        id: 'old',
        kind: 'weather-publication',
        status: 'committed',
        label: 'Old',
        detail: JSON.stringify({ publishedAt: '2026-10-20T01:00:00Z' }),
      },
      {
        id: 'new',
        kind: 'weather-publication',
        status: 'committed',
        label: 'New',
        detail: JSON.stringify({ publishedAt: '2026-10-21T11:00:00Z' }),
      },
      {
        id: 'old-area',
        kind: 'area-forecast',
        status: 'published',
        label: 'Old area',
        detail: JSON.stringify({ sourceUpdatedAt: '2026-10-19T01:00:00Z' }),
      },
      {
        id: 'area',
        kind: 'area-forecast',
        status: 'published',
        label: 'Current area',
        detail: JSON.stringify({ sourceUpdatedAt: '2026-10-20T20:00:00Z' }),
      },
      {
        id: 'unknown',
        kind: 'area-forecast',
        status: 'published',
        label: 'Unknown area',
        detail: 'malformed',
      },
    ],
    edges: [
      { from: 'old', to: 'old-area', label: 'contains' },
      { from: 'new', to: 'area', label: 'contains' },
      { from: 'new', to: 'unknown', label: 'contains' },
    ],
  };
  const health = workloadHealth(
    'weather',
    {
      jobs: [],
      schedules: [
        { kind: 'weather', enabled: false, interval_minutes: 60, next_run: '2026-10-01T00:00:00Z' },
      ],
    },
    lineage,
    now,
  );
  assert.equal(health.publication?.node.id, 'new');
  assert.equal(health.overdue, false);
  assert.equal(health.intervalExceeded, false);
  assert.deepEqual(
    health.forecasts.map((item) => [item.name, item.stale]),
    [
      ['Current area', true],
      ['Unknown area', false],
    ],
  );
  assert.equal(health.forecasts[1].issuedAt, undefined);
});
