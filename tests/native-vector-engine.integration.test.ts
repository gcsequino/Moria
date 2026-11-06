import assert from 'node:assert/strict';
import test from 'node:test';
import { database } from './fixtures/catalog-storage/database.js';

test('the pinned PostgreSQL engine provides real pgvector exact cosine', async (t) => {
  const db = await database();
  t.after(db.close);
  await db.pool.query('CREATE EXTENSION IF NOT EXISTS vector');
  const version = (
    await db.pool.query("SELECT extversion FROM pg_extension WHERE extname='vector'")
  ).rows[0].extversion;
  assert.equal(version, '0.8.7');
  const rows = (
    await db.pool.query(`SELECT label,embedding <=> '[1,0]'::vector AS distance
    FROM (VALUES ('same','[1,0]'::vector),('orthogonal','[0,1]'::vector),('opposite','[-1,0]'::vector)) AS candidates(label,embedding)
    ORDER BY distance,label`)
  ).rows;
  assert.deepEqual(
    rows.map((r) => r.label),
    ['same', 'orthogonal', 'opposite'],
  );
  assert.deepEqual(
    rows.map((r) => r.distance),
    [0, 1, 2],
  );
});
