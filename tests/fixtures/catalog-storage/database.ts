import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:net';
import EmbeddedPostgres from 'embedded-postgres';
import pg from 'pg';
import { Catalog } from '../../../services/catalog/src/catalog.js';

export async function database() {
  const root = await mkdtemp(join(tmpdir(), 'catalog-storage-'));
  const reservation = createServer();
  await new Promise<void>((resolve) => reservation.listen(0, '127.0.0.1', resolve));
  const port = (reservation.address() as { port: number }).port;
  await new Promise<void>((resolve) => reservation.close(() => resolve()));
  const password = randomUUID();
  const db = new EmbeddedPostgres({
    databaseDir: join(root, 'pg'),
    user: 'moria_test',
    password,
    port,
    persistent: true,
    authMethod: 'scram-sha-256',
    initdbFlags: ['--encoding=UTF8', '--locale=C'],
    postgresFlags: ['-c', 'listen_addresses=127.0.0.1'],
    onLog: () => {},
    onError: () => {},
  });
  let pool: pg.Pool | undefined;
  const clients = new Set<Promise<void>>();
  const close = async () => {
    await pool?.end();
    // Pool.end resolves before idle Client.end callbacks have drained their sockets.
    await Promise.all(clients);
    await db.stop();
    await rm(root, { recursive: true, force: true });
  };
  try {
    await db.initialise();
    await db.start();
    pool = new pg.Pool({
      host: '127.0.0.1',
      port,
      user: 'moria_test',
      password,
      database: 'postgres',
    });
    pool.on('connect', (client) => {
      const ended = new Promise<void>((resolve) => client.once('end', resolve));
      clients.add(ended);
      void ended.then(() => clients.delete(ended));
    });
    const catalog = new Catalog(pool);
    await catalog.migrate();
    return { root, pool, catalog, close };
  } catch (error) {
    await close();
    throw error;
  }
}
