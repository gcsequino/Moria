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
  if (!['local', 'integration'].includes(kind))
    throw new Error('Choose local or integration tests.');
  const selected = files
    .filter((file) => file.endsWith('.test.ts'))
    .filter((file) => file.endsWith('.integration.test.ts') === (kind === 'integration'))
    .map((file) => `tests/${file}`)
    .sort();
  for (const file of requested)
    if (!selected.includes(file)) throw new Error(`Unknown ${kind} test file: ${file}`);
  const result = requested.length ? [...new Set(requested)].sort() : selected;
  if (!result.length) throw new Error(`No ${kind} test files selected.`);
  return result;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [kind, ...requested] = process.argv.slice(2);
  const files = selectTestFiles(readdirSync('tests'), kind, requested);
  console.error(`Running ${files.length} ${kind} test files.`);
  const result = spawnSync(
    process.execPath,
    [
      '--import',
      'tsx',
      '--test',
      '--test-concurrency=2',
      '--test-reporter=./scripts/test-reporter.ts',
      ...files,
    ],
    { stdio: 'inherit' },
  );
  if (result.error) throw result.error;
  process.exit(result.status ?? 1);
}
