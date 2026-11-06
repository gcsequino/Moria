import { spawnSync } from 'node:child_process';
import { readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * @param {string[]} files
 * @param {string} kind
 * @param {string[]} requested
 */
export function selectTestFiles(files, kind, requested = []) {
  if (!['all', 'local', 'integration'].includes(kind))
    throw new Error('Choose all, local, or integration tests.');
  const selected = files
    .filter((file) => file.endsWith('.test.ts'))
    .filter(
      (file) =>
        kind === 'all' || file.endsWith('.integration.test.ts') === (kind === 'integration'),
    )
    .map((file) => `tests/${file}`)
    .sort();
  for (const file of requested)
    if (!selected.includes(file)) throw new Error(`Unknown ${kind} test file: ${file}`);
  const result = requested.length ? [...new Set(requested)].sort() : selected;
  if (!result.length) throw new Error(`No ${kind} test files selected.`);
  return result;
}

// These files exercise deadlines while running several native workers and stores.
const isolatedFiles = new Set([
  'tests/native-catalog.integration.test.ts',
  'tests/native-evaluations.integration.test.ts',
  'tests/native-pipelines.integration.test.ts',
]);

/** @param {string[]} files */
export function scheduleTestFiles(files) {
  return [
    { concurrency: 2, files: files.filter((file) => !isolatedFiles.has(file)) },
    { concurrency: 1, files: files.filter((file) => isolatedFiles.has(file)) },
  ].filter((group) => group.files.length);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [kind, ...requested] = process.argv.slice(2);
  const files = selectTestFiles(readdirSync('tests'), kind, requested);
  let status = 0;
  for (const group of scheduleTestFiles(files)) {
    console.error(
      `Running ${group.files.length} ${kind} test files at concurrency ${group.concurrency}.`,
    );
    const result = spawnSync(
      process.execPath,
      [
        '--import',
        'tsx',
        '--test',
        `--test-concurrency=${group.concurrency}`,
        '--test-reporter=./scripts/test-reporter.ts',
        ...group.files,
      ],
      { stdio: 'inherit' },
    );
    if (result.error) throw result.error;
    if (result.signal) process.exit(1);
    status ||= result.status ?? 1;
  }
  process.exit(status);
}
