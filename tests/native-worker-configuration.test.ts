import assert from 'node:assert/strict';
import test from 'node:test';
import { validateNativeWorkerConfiguration } from '../apps/local/src/native-runtime.js';
test('configured native indexing workers require explicit bounded scratch configuration before launch', () => {
  const valid = {
    MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT: '/private/worker',
    MORIA_NATIVE_CLOSURE_LIMITS: JSON.stringify({
      maxEntries: 10000,
      maxReferencedBytes: 1073741824,
      maxSpoolBytes: 33554432,
    }),
  };
  assert.doesNotThrow(() => validateNativeWorkerConfiguration('indexing', valid));
  assert.throws(() => validateNativeWorkerConfiguration('indexing', {}), /SCRATCH_ROOT/);
  for (const root of ['relative', '/private/../shared', '/private\0escape'])
    assert.throws(
      () =>
        validateNativeWorkerConfiguration('indexing', {
          ...valid,
          MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT: root,
        }),
      /SCRATCH_ROOT/,
    );
  for (const value of [
    '{}',
    'null',
    '[]',
    'malformed',
    JSON.stringify({ maxEntries: 0, maxReferencedBytes: 1, maxSpoolBytes: 1 }),
    JSON.stringify({ maxEntries: 1, maxReferencedBytes: 1, maxSpoolBytes: 1, unbounded: true }),
  ])
    assert.throws(
      () =>
        validateNativeWorkerConfiguration('indexing', {
          ...valid,
          MORIA_NATIVE_CLOSURE_LIMITS: value,
        }),
      /CLOSURE_LIMITS/,
    );
  assert.doesNotThrow(() => validateNativeWorkerConfiguration('functions', {}));
});
