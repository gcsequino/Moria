import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { readFileSync, readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import './indexing-schema.js';

const root = new URL('../services/indexing/', import.meta.url);
const paths = [
  'Cargo.toml',
  'Cargo.lock',
  'build.rs',
  'schema.json',
  'protocol/indexing.proto',
  '../../packages/contracts/value-conformance.json',
  ...readdirSync(new URL('src/', root))
    .filter((name) => name.endsWith('.rs'))
    .map((name) => `src/${name}`),
].sort();
const digest = createHash('sha256');
for (const path of paths) {
  const name = Buffer.from(path),
    bytes = readFileSync(new URL(path, root));
  for (const value of [name, bytes]) {
    const length = Buffer.alloc(8);
    length.writeBigUInt64BE(BigInt(value.length));
    digest.update(length);
    digest.update(value);
  }
}
const expected = digest.digest('hex');
const binary = process.env.MORIA_INDEXING_BINARY ?? '/usr/local/bin/moria-indexing';
const testBinary = process.env.MORIA_INDEXING_TEST_BINARY ?? '/usr/local/bin/moria-indexing-tests';
const compiled = JSON.parse(execFileSync(binary, ['schema'], { encoding: 'utf8' }));
const schema = JSON.parse(readFileSync(new URL('schema.json', root), 'utf8'));
if (
  compiled.protocolVersion !== 1 ||
  compiled.sourceDigest !== expected ||
  compiled.schema.version !== schema.version ||
  compiled.schema.checksum !== schema.checksum
)
  throw new Error('Rust indexing artifact is stale; rebuild from this source.');
const result = execFileSync(testBinary, ['--test-threads=1'], {
  encoding: 'utf8',
  env: { ...process.env, MORIA_INDEXING_EXPECTED_SOURCE_SHA256: expected },
});
process.stdout.write(result);
if (!/test result: ok\. [1-9][0-9]* passed; 0 failed; 0 ignored;/.test(result))
  throw new Error('Rust tests did not execute the required cases.');
console.log(`Rust indexing artifact and tests match ${fileURLToPath(root)} (${expected}).`);
