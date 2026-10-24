import { test } from 'node:test';
import assert from 'node:assert/strict';
import { validateValue, validateRow } from '../packages/contracts/values.js';

test('wire values reject invalid dates, lossy integers, malformed text and decimal overflow', () => {
  for (const [value, type] of [
    ['2025-02-29', 'date'],
    ['2024-02-30', 'date'],
    ['9223372036854775808', 'int64'],
    [5, 'int64'],
    ['\uD800', 'string'],
    ['nul\0byte', 'string'],
    [Infinity, 'float64'],
  ] as const)
    assert.throws(() => validateValue(value, { type, nullable: false }), { code: 'VALUE_INVALID' });
  assert.equal(validateValue('2024-02-29', { type: 'date', nullable: false }), '2024-02-29');
  assert.throws(
    () => validateValue('100.00', { type: 'decimal', precision: 4, scale: 2, nullable: false }),
    { code: 'VALUE_INVALID' },
  );
  assert.equal(
    validateValue('0.12', { type: 'decimal', precision: 2, scale: 2, nullable: false }),
    '0.12',
  );
});
test('timestamps normalize to UTC microseconds without truncating precision', () => {
  assert.equal(
    validateValue('2026-01-01T12:30:40.1Z', { type: 'timestamp', nullable: false }),
    '2026-01-01T12:30:40.100000Z',
  );
  for (const value of [
    '2026-01-01T25:30:40Z',
    '2026-01-01T12:30:40.1234567Z',
    '2026-01-01T12:30:40+01:00',
  ])
    assert.throws(() => validateValue(value, { type: 'timestamp', nullable: false }));
});
test('row ingestion normalizes missing nullable values and rejects unknown or missing required fields', () => {
  const columns = {
    id: { type: 'string', nullable: false },
    note: { type: 'string', nullable: true },
  } as const;
  assert.deepEqual(validateRow({ id: 'a' }, columns), { id: 'a', note: null });
  assert.throws(() => validateRow({}, columns));
  assert.throws(() => validateRow({ id: 'a', extra: 1 }, columns));
  assert.throws(() =>
    validateValue([null], {
      type: 'array',
      nullable: false,
      items: { type: 'string', nullable: false },
    }),
  );
});
