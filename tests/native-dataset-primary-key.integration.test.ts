import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';

test(
  'declared Dataset keys constrain native replacement and every head publication',
  { timeout: 120000 },
  async (t) => {
    const fixture = await managedRetentionDataset({ primaryKey: ['id'] });
    t.after(fixture.close);
    const { db, environment, dataset } = fixture;
    const binding = (
      await db.pool.query(
        'SELECT primary_key,write_modes,key_schema_hash FROM moria.native_dataset_bindings WHERE environment=$1 AND resource=$2',
        [environment, dataset],
      )
    ).rows[0];
    assert.deepEqual(binding.primary_key, ['id']);
    assert.deepEqual(binding.write_modes, ['replace']);
    assert.match(binding.key_schema_hash, /^sha256:[0-9a-f]{64}$/);
    const rows = Array.from({ length: 64 }, (_, i) => ({
      id: randomUUID(),
      name: 'keyed-' + i,
      tags: ['typed'],
    }));
    const first = await fixture.publish(rows);
    await fixture.read(first.head);
    const certificates = (
      await db.pool.query(
        'SELECT c.key_rows,c.key_schema_hash,c.snapshot_id FROM moria.runtime_dataset_key_certificates c JOIN moria.runtime_dataset_roots r ON r.key=c.root WHERE r.metadata_location=$1',
        [first.head.metadataLocation],
      )
    ).rows;
    assert.equal(certificates.length, 1);
    assert.equal(certificates[0].key_rows, '64');
    assert.equal(certificates[0].key_schema_hash, binding.key_schema_hash);
    const bad = rows.map((row) => ({ ...row }));
    bad[63].id = bad[0].id;
    await assert.rejects(fixture.publish(bad), /DATASET_PRIMARY_KEY_DUPLICATE/);
    assert.deepEqual((await db.catalog.heads(environment, [dataset]))[0], first.head);
    assert.equal(
      (await db.pool.query('SELECT count(*) FROM moria.runtime_dataset_key_certificates')).rows[0]
        .count,
      '1',
    );
    for (const row of (
      await db.pool.query(
        "SELECT id FROM moria.runtime_executions WHERE environment=$1 AND status IN ('queued','running','preparing')",
        [environment],
      )
    ).rows)
      await fixture.command(['execution', 'cancel'], { id: row.id });
    await fixture.read(first.head);
    const raw = await db.pool.connect();
    try {
      await raw.query('BEGIN');
      await raw.query(
        'UPDATE moria.dataset_heads SET revision=revision+1 WHERE environment=$1 AND dataset=$2',
        [environment, dataset],
      );
      await assert.rejects(raw.query('COMMIT'), /DATASET_PUBLICATION_CONTRACT_REQUIRED/);
    } finally {
      await raw.query('ROLLBACK');
      raw.release();
    }
    assert.deepEqual((await db.catalog.heads(environment, [dataset]))[0], first.head);
    await fixture.read(first.head);
    t.diagnostic(
      JSON.stringify({
        rows: 64,
        duplicateRejectedBeforeHead: true,
        certificateCurrent: true,
        rawHeadWithoutHistoryDenied: true,
      }),
    );
  },
);

test(
  'composite declared keys preserve C03 scalar equality and exact boundary values',
  { timeout: 120000 },
  async (t) => {
    const columns = {
      id: { type: 'uid', nullable: false },
      ratio: { type: 'float64', nullable: false },
      optionalNote: { type: 'string', nullable: true },
      sequence: { type: 'int64', nullable: false },
      amount: { type: 'decimal', precision: 18, scale: 2, nullable: false },
      day: { type: 'date', nullable: false },
      stamp: { type: 'timestamp', nullable: false },
    };
    const fixture = await managedRetentionDataset({
      columns,
      primaryKey: ['sequence', 'ratio', 'amount', 'day', 'stamp'],
    });
    t.after(fixture.close);
    const row = {
      id: randomUUID(),
      ratio: 0,
      optionalNote: null,
      sequence: '9007199254740992',
      amount: '1.00',
      day: '2024-02-29',
      stamp: '2024-02-29T12:00:00.000000Z',
    };
    const rows = [
      row,
      { ...row, id: randomUUID(), sequence: '9007199254740993' },
      {
        ...row,
        id: randomUUID(),
        ratio: 1,
        sequence: '9223372036854775807',
        amount: '1234567890123456.78',
        day: '0001-01-01',
        stamp: '9999-12-31T23:59:59.999999Z',
      },
    ];
    const first = await fixture.publish(rows);
    await fixture.read(first.head);
    const duplicate = [
      row,
      { ...row, id: randomUUID(), ratio: -0, amount: '1', stamp: '2024-02-29T12:00:00Z' },
    ];
    await assert.rejects(fixture.publish(duplicate), /DATASET_PRIMARY_KEY_DUPLICATE/);
    assert.deepEqual(
      (await fixture.db.catalog.heads(fixture.environment, [fixture.dataset]))[0],
      first.head,
    );
    await assert.rejects(
      fixture.publish([{ ...row, ratio: null }]),
      /DATASET_PRIMARY_KEY_NULL|Unexpected null|VALUE_INVALID|SCHEMA_CHANGED/,
    );
    assert.deepEqual(
      (await fixture.db.catalog.heads(fixture.environment, [fixture.dataset]))[0],
      first.head,
    );
    for (const { id } of (
      await fixture.db.pool.query(
        "SELECT id FROM moria.runtime_executions WHERE environment=$1 AND status IN ('queued','running','preparing')",
        [fixture.environment],
      )
    ).rows)
      await fixture.command(['execution', 'cancel'], { id });
    await fixture.read(first.head);
    t.diagnostic(
      JSON.stringify({
        rows: 3,
        adjacentInt64BeyondSafeIntegerDistinct: true,
        compositeOrdered: true,
        decimalAndTimestampCanonicalDuplicatesDenied: true,
        nullKeyDenied: true,
        nullableNonKeyPreserved: true,
        headAndTypedRowsPreserved: true,
      }),
    );
  },
);
