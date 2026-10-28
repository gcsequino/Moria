import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:net';
import { startDevelopmentDatabase } from '../../../apps/local/src/development-database.js';
import { Catalog } from '../../../services/catalog/src/catalog.js';

export async function database() {
  const root = await mkdtemp(join(tmpdir(), 'catalog-storage-'));
  let db: Awaited<ReturnType<typeof startDevelopmentDatabase>> | undefined;
  const close = async () => {
    await db?.pool.end();
    await db?.stop();
    await rm(root, { recursive: true, force: true });
  };
  try {
    const reservation = createServer();
    await new Promise<void>((resolve) => reservation.listen(0, '127.0.0.1', resolve));
    const port = (reservation.address() as { port: number }).port;
    await new Promise<void>((resolve) => reservation.close(() => resolve()));
    db = await startDevelopmentDatabase(root, port, randomUUID());
    const catalog = new Catalog(db.pool);
    await catalog.migrate();
    return { root, pool: db.pool, catalog, close };
  } catch (error) {
    await close();
    throw error;
  }
}
