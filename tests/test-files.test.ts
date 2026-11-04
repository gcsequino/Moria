import assert from 'node:assert/strict';
import { readdirSync } from 'node:fs';
import test from 'node:test';
import { selectTestFiles } from '../scripts/test-files.mjs';

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
  assert.throws(() => selectTestFiles(files, 'all'), /Choose/);
  assert.throws(() => selectTestFiles([], 'integration'), /No integration/);
});
