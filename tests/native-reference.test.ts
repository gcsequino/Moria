import assert from 'node:assert/strict';
import test from 'node:test';
import { decodeMetadataLocation } from '../services/catalog/src/storage.js';
import { icebergRows } from '../workers/iceberg/stream.js';

test('legacy catalog readers explicitly reject native snapshot references without resolving storage', () => {
  for (const suffix of ['{}', 'malformed', '{"version":2,"root":{}}'])
    assert.throws(() => decodeMetadataLocation(`moria-snapshot-v2:${suffix}`), {
      code: 'UNSUPPORTED_CAPABILITY',
    });
});
test('legacy Python stream dispatch rejects native references before starting a process', async () => {
  for (const suffix of ['{}', 'malformed', '{"version":2,"root":{}}'])
    await assert.rejects(
      icebergRows({ metadataLocation: `moria-snapshot-v2:${suffix}`, snapshotId: '1' }).next(),
      { code: 'UNSUPPORTED_CAPABILITY' },
    );
});
