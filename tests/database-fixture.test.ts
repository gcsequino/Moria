import assert from 'node:assert/strict';
import test from 'node:test';
import { database } from './fixtures/catalog-storage/database.js';

test(
  'database fixture waits for actual pooled socket closure before stopping PostgreSQL',
  { timeout: 15000 },
  async () => {
    const db = await database(),
      client = await db.pool.connect(),
      end = client.end.bind(client),
      errors: unknown[] = [];
    client.on('error', (error) => errors.push(error));
    let ended = false,
      finished!: () => void;
    const delayed = new Promise<void>((resolve) => {
      finished = resolve;
    });
    client.end = ((callback?: () => void) => {
      setTimeout(() => {
        void end().then(() => {
          ended = true;
          callback?.();
          finished();
        });
      }, 200);
    }) as typeof client.end;
    client.release();
    await db.close();
    const drained = ended;
    await delayed;
    assert.equal(drained, true, 'PostgreSQL must remain running until the client has really ended');
    assert.deepEqual(errors, []);
  },
);
