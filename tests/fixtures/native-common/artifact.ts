import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFile } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import { promisify } from 'node:util';
import { nativeInputs, nativeRoot } from '../../../scripts/native-inputs.js';

/** Component proofs must establish provenance before starting disposable infrastructure. */
export async function assertNativeArtifacts(runtime: string, harnesses: string[]) {
  const digest = createHash('sha256');
  for (const path of nativeInputs()) {
    for (const bytes of [Buffer.from(path), await readFile(new URL(path, nativeRoot))]) {
      const length = Buffer.alloc(8);
      length.writeBigUInt64BE(BigInt(bytes.length));
      digest.update(length);
      digest.update(bytes);
    }
  }
  const expected = digest.digest('hex');
  const execute = promisify(execFile);
  const metadata = JSON.parse((await execute(runtime, ['schema'])).stdout);
  assert.equal(
    metadata.sourceDigest,
    expected,
    'Native runtime does not match this immutable source',
  );
  assert.deepEqual(
    metadata.schema,
    JSON.parse(await readFile(new URL('schema.json', nativeRoot), 'utf8')),
  );
  for (const harness of harnesses) {
    const result = await execute(
      harness,
      ['--ignored', '--exact', 'native_artifact_metadata', '--nocapture', '--test-threads=1'],
      {
        env: { ...process.env, MORIA_INDEXING_EXPECTED_SOURCE_SHA256: expected },
      },
    );
    assert.match(
      result.stdout,
      /test result: ok\. 1 passed; 0 failed; 0 ignored;/,
      'Native harness provenance case did not execute',
    );
  }
  return metadata;
}
