import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import test from 'node:test';
import { database } from './fixtures/catalog-storage/database.js';

test('permanent identity keys retain full-width canonical values outside B-tree tuples', async (t) => {
  const db = await database();
  t.after(db.close);
  const key = randomBytes(32768).toString('hex');
  const values = await Promise.all(
    Array.from({ length: 8 }, () => db.catalog.resolveIdentity('wide-key', key)),
  );
  assert.equal(new Set(values).size, 1);
  assert.equal(await db.catalog.resolveIdentity('wide-key', key), values[0]);
  assert.notEqual(
    await db.catalog.resolveIdentity(
      'wide-key',
      key.slice(0, -1) + (key.at(-1) === 'a' ? 'b' : 'a'),
    ),
    values[0],
  );
  assert.equal(
    (await db.pool.query('SELECT canonical_key FROM moria.identities WHERE uid=$1', [values[0]]))
      .rows[0].canonical_key,
    key,
  );
  await assert.rejects(
    db.pool.query('DELETE FROM moria.identities WHERE uid=$1', [values[0]]),
    /IDENTITY_RESERVED/,
  );
  await assert.rejects(
    db.pool.query('UPDATE moria.identities SET uid=$2 WHERE uid=$1', [
      values[0],
      'ffffffff-ffff-4fff-8fff-ffffffffffff',
    ]),
    /IDENTITY_RESERVED/,
  );
});
