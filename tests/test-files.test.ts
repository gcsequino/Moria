import assert from 'node:assert/strict';
import { readdirSync } from 'node:fs';
import test from 'node:test';
import { scheduleTestFiles, selectTestFiles } from '../scripts/test-files.mjs';

test('small test gates partition every test file without dropping or duplicating cases', () => {
  const files = readdirSync('tests');
  const local = selectTestFiles(files, 'local');
  const integration = selectTestFiles(files, 'integration');
  assert.equal(
    local.some((file) => integration.includes(file)),
    false,
  );
  assert.deepEqual(
    [...local, ...integration].sort(),
    files
      .filter((file) => file.endsWith('.test.ts'))
      .map((file) => `tests/${file}`)
      .sort(),
  );
});

test('a focused gate runs only explicit existing files and rejects wrong or empty selections', () => {
  const files = ['a.test.ts', 'b.integration.test.ts', 'c.integration.test.ts', 'notes.md'];
  assert.deepEqual(selectTestFiles(files, 'integration', ['tests/c.integration.test.ts']), [
    'tests/c.integration.test.ts',
  ]);
  assert.deepEqual(selectTestFiles(files, 'local', ['tests/a.test.ts', 'tests/a.test.ts']), [
    'tests/a.test.ts',
  ]);
  assert.throws(() => selectTestFiles(files, 'integration', ['tests/a.test.ts']), /Unknown/);
  assert.throws(() => selectTestFiles(files, 'local', ['tests/typo.test.ts']), /Unknown/);
  assert.throws(() => selectTestFiles(files, 'invalid'), /Choose/);
  assert.throws(() => selectTestFiles([], 'integration'), /No integration/);
});

test('the full test command schedules deadline-sensitive integration files alone', () => {
  const files = [
    'a.test.ts',
    'b.integration.test.ts',
    'native-evaluations.integration.test.ts',
    'native-pipelines.integration.test.ts',
    'native-catalog.integration.test.ts',
  ];
  const selected = selectTestFiles(files, 'all');
  const schedule = scheduleTestFiles(selected);
  assert.deepEqual(schedule, [
    {
      concurrency: 2,
      files: ['tests/a.test.ts', 'tests/b.integration.test.ts'],
    },
    {
      concurrency: 1,
      files: [
        'tests/native-catalog.integration.test.ts',
        'tests/native-evaluations.integration.test.ts',
        'tests/native-pipelines.integration.test.ts',
      ],
    },
  ]);
  assert.deepEqual(schedule.flatMap((group) => group.files).sort(), selected);
  assert.deepEqual(scheduleTestFiles(['tests/a.test.ts']), [
    { concurrency: 2, files: ['tests/a.test.ts'] },
  ]);
});
