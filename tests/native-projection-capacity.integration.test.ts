import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { nativeProjectionCapacity } from '../benchmarks/native-projection.js';

test(
  'native capacity preparation uses permanent UID allocation, bounded real source production and complete native checksums',
  { timeout: 180000 },
  async (t) => {
    const output = await mkdtemp(join(tmpdir(), 'native-projection-capacity-evidence-'));
    t.after(() => rm(output, { recursive: true, force: true }));
    const result = await nativeProjectionCapacity({
      rows: 6000,
      rowBytes: 4096,
      output,
      timeoutMs: 150000,
    });
    assert.equal(result.population, 6000);
    assert.equal(result.source.logicalBytes, String(6000 * 4096));
    assert.equal(result.verification!.count, '6000');
    assert.equal(result.verification!.sourceSum, result.source.sourceSum);
    assert.equal(result.verification!.sourceXor, result.source.sourceXor);
    assert.equal(result.fullProfileCertified, false);
    assert.match(
      await readFile(join(output, 'identity.log'), 'utf8'),
      /identity_population_complete/,
    );
    assert.match(await readFile(join(output, 'publication.log'), 'utf8'), /"event":"receipt"/);
    assert.match(await readFile(join(output, 'oracle.log'), 'utf8'), /capacity_verified/);
  },
);
