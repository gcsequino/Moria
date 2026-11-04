import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { baseline } from '../services/database/src/baseline.js';
import { schemaVersion } from '../services/database/src/schema.js';

const path = new URL('../services/runtime/schema.json', import.meta.url);
const expected =
  JSON.stringify(
    { version: schemaVersion, checksum: createHash('sha256').update(baseline).digest('hex') },
    null,
    2,
  ) + '\n';
if (process.argv.includes('--write')) writeFileSync(path, expected);
else if (readFileSync(path, 'utf8') !== expected)
  throw new Error(
    'Rust indexing schema contract is stale; run pnpm exec tsx scripts/indexing-schema.ts --write.',
  );
