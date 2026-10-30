import { mkdir, readFile, writeFile, copyFile } from 'node:fs/promises';
import { stripTypeScriptTypes } from 'node:module';
import { join } from 'node:path';

const output = process.argv[2];
if (!output) throw new Error('Usage: node scripts/build-function-runtime.mjs <output-directory>');
await mkdir(output, { recursive: true });
for (const name of ['errors', 'value-types', 'values']) {
  const source = await readFile(
    new URL(`../packages/contracts/${name}.ts`, import.meta.url),
    'utf8',
  );
  await writeFile(join(output, `${name}.js`), stripTypeScriptTypes(source));
}
await copyFile(
  new URL('../workers/functions/runner.mjs', import.meta.url),
  join(output, 'runner.mjs'),
);
await writeFile(join(output, 'package.json'), JSON.stringify({ type: 'module' }) + '\n');
