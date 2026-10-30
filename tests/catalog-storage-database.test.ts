import assert from 'node:assert/strict';
import test from 'node:test';
import { setImmediate } from 'node:timers/promises';
import EmbeddedPostgres from 'embedded-postgres';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { startDevelopmentDatabase } from '../apps/local/src/development-database.js';
import { database } from './fixtures/catalog-storage/database.js';

test('catalog storage fixture waits for removed and idle clients to disconnect before stopping PostgreSQL', async (t) => {
  const db = await database();
  const clients = await Promise.all([db.pool.connect(), db.pool.connect()]);
  const disconnects: (() => void)[] = [];
  let disconnected = 0,
    disconnectedAtStop = -1;
  const ended = clients.map(
    (client) =>
      new Promise<void>((resolve) =>
        client.once('end', () => {
          disconnected++;
          resolve();
        }),
      ),
  );
  const stop = EmbeddedPostgres.prototype.stop;
  const stopping = t.mock.method(
    EmbeddedPostgres.prototype,
    'stop',
    async function (this: EmbeddedPostgres) {
      disconnectedAtStop = disconnected;
      await Promise.all(ended);
      await stop.call(this);
    },
  );
  for (const client of clients) {
    const end = client.end;
    t.mock.method(client, 'end', ((...args: unknown[]) => {
      disconnects.push(() => Reflect.apply(end, client, args));
    }) as typeof client.end);
  }
  clients[0].release(true);
  clients[1].release();
  const closing = db.close();
  let premature = false;
  try {
    await setImmediate();
    assert.equal(disconnects.length, 2);
    premature = disconnectedAtStop !== -1;
  } finally {
    for (const disconnect of disconnects) disconnect();
    await Promise.all(ended);
    await closing;
    stopping.mock.restore();
  }
  assert.equal(premature, false, 'cluster shutdown began while client sockets were still open');
  assert.equal(disconnectedAtStop, 2);
});

test('development database asks its dependency to clean up failed startup', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'database-startup-failure-'));
  const failure = new Error('synthetic startup failure');
  t.mock.method(EmbeddedPostgres.prototype, 'initialise', async () => {});
  t.mock.method(EmbeddedPostgres.prototype, 'start', async () => {
    throw failure;
  });
  const stop = t.mock.method(EmbeddedPostgres.prototype, 'stop', async () => {});
  try {
    await assert.rejects(
      startDevelopmentDatabase(root, 23456, 'synthetic-password'),
      (error) => error === failure,
    );
    assert.equal(stop.mock.callCount(), 1);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
