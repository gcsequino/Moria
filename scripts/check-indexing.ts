import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import './indexing-schema.js';
import './generate-native-conformance.js';
import { nativeInputs, nativeRoot } from './native-inputs.js';

const root = nativeRoot;
const paths = nativeInputs();
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
const nativeBinary = process.env.MORIA_NATIVE_BINARY ?? '/usr/local/bin/moria-runtime';
const nativeCompiled = JSON.parse(execFileSync(nativeBinary, ['schema'], { encoding: 'utf8' }));
if (JSON.stringify(nativeCompiled) !== JSON.stringify(compiled))
  throw new Error('Native runtime and indexing executables disagree; rebuild both.');
const artifacts = [
  ['MORIA_POLICY_TEST_BINARY', 'moria-native-policy-test'],
  ['MORIA_STORAGE_TEST_BINARY', 'moria-native-storage-test'],
  ['MORIA_RPC_TEST_BINARY', 'moria-native-transport-test'],
  ['MORIA_CATALOG_TEST_BINARY', 'moria-native-catalog-test'],
  ['MORIA_NATIVE_SANDBOX_TEST_BINARY', 'moria-native-sandbox-test'],
  ['MORIA_NATIVE_RETENTION_TEST_BINARY', 'moria-native-retention-test'],
];
for (const [variable, name] of artifacts) {
  const output = execFileSync(
    process.env[variable] ?? `/usr/local/bin/${name}`,
    ['--ignored', '--exact', 'native_artifact_metadata', '--nocapture', '--test-threads=1'],
    { encoding: 'utf8', env: { ...process.env, MORIA_INDEXING_EXPECTED_SOURCE_SHA256: expected } },
  );
  if (!/test result: ok\. 1 passed; 0 failed;/.test(output))
    throw new Error(`Native artifact provenance check did not run: ${name}`);
}
const result = execFileSync(testBinary, ['--test-threads=1'], {
  encoding: 'utf8',
  env: { ...process.env, MORIA_INDEXING_EXPECTED_SOURCE_SHA256: expected },
});
process.stdout.write(result);
if (!/test result: ok\. [1-9][0-9]* passed; 0 failed; 0 ignored;/.test(result))
  throw new Error('Rust tests did not execute the required cases.');
console.log(`Rust indexing artifact and tests match ${fileURLToPath(root)} (${expected}).`);
