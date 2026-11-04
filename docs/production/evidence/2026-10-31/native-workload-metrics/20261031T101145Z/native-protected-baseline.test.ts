import assert from 'node:assert/strict';
import test from 'node:test';
import {
  completeProtectedSample,
  summarizeProtectedSamples,
} from '../benchmarks/native-protected-baseline.js';

test('protected workload reports successful latency separately from fast rejections and missed dispatches', () => {
  const summary = summarizeProtectedSamples([
    {
      sequence: 0,
      kind: 'detail',
      scheduledMs: 0,
      dispatchMs: 2,
      completedMs: 1200,
      successful: true,
      status: 200,
    },
    ...Array.from({ length: 99 }, (_, sequence) => ({
      sequence: sequence + 1,
      kind: 'detail' as const,
      scheduledMs: 0,
      dispatchMs: 0,
      completedMs: 1,
      successful: false,
      status: 429,
      error: 'RESOURCE_LIMIT',
    })),
    {
      sequence: 100,
      kind: 'detail',
      scheduledMs: 0,
      completedMs: 3,
      successful: false,
      error: 'OUTSTANDING_LIMIT',
    },
  ]);
  assert.equal(summary.scheduled, 101);
  assert.equal(summary.successful, 1);
  assert.equal(summary.unsuccessfulFraction, 100 / 101);
  assert.equal(summary.latencyBasis, 'all-scheduled-arrivals');
  assert.equal(summary.p95Ms, 1);
  assert.deepEqual(summary.successfulLatency, {
    count: 1,
    p50Ms: 1200,
    p95Ms: 1200,
    p99Ms: 1200,
    maxMs: 1200,
  });
  assert.equal(summary.dispatchDelay.count, 100);
  assert.equal(summary.notDispatched, 1);
  assert.deepEqual(summary.statuses, { 200: 1, 429: 99 });
  assert.deepEqual(summary.errors, { RESOURCE_LIMIT: 99, OUTSTANDING_LIMIT: 1 });
  const empty = summarizeProtectedSamples([]);
  assert.equal(empty.unsuccessfulFraction, null);
  assert.equal(empty.successfulLatency.p95Ms, null);
  assert.equal(empty.p99Ms, null);
});

test('a successful HTTP response after the original arrival deadline remains unsuccessful', () => {
  const sample = {
    sequence: 0,
    kind: 'action' as const,
    scheduledMs: 50,
    dispatchMs: 60,
    completedMs: 60,
    successful: true,
    status: 200,
  };
  const late = completeProtectedSample({ ...sample }, 1051, 1000);
  assert.equal(late.status, 200);
  assert.equal(late.successful, false);
  assert.equal(late.error, 'ARRIVAL_DEADLINE');
  assert.equal(summarizeProtectedSamples([late]).successfulLatency.count, 0);
  assert.equal(completeProtectedSample({ ...sample }, 1050, 1000).successful, true);
  const rejected = completeProtectedSample(
    { ...sample, successful: false, status: 429, error: 'RESOURCE_LIMIT' },
    1060,
    1000,
  );
  assert.equal(rejected.error, 'RESOURCE_LIMIT');
});
