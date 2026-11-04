import { access } from 'node:fs/promises';
import { resolve } from 'node:path';
import EmbeddedPostgres from 'embedded-postgres';
import exitHooks from 'async-exit-hook';
import pg from 'pg';

/** Only the development launcher owns a cluster. Existing paths and credentials
 * stay unchanged for supported versioned databases. */
export async function startDevelopmentDatabase(
  directory: string,
  port: number,
  password: string,
  signalOwner: 'library' | 'caller' = 'library',
) {
  if (signalOwner === 'caller') {
    // The caller drains native workers and clients before explicitly stopping this database.
    exitHooks.unhookEvent('SIGINT');
    exitHooks.unhookEvent('SIGTERM');
  }
  const db = new EmbeddedPostgres({
    databaseDir: resolve(directory, 'postgres'),
    user: 'moria_local',
    password,
    port,
    persistent: true,
    authMethod: 'scram-sha-256',
    initdbFlags: ['--encoding=UTF8', '--locale=C'],
    postgresFlags: ['-c', 'listen_addresses=127.0.0.1'],
    onLog: (message) => console.error(message),
    onError: (message) => console.error(message),
  });
  try {
    try {
      await access(resolve(directory, 'postgres', 'PG_VERSION'));
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      await db.initialise();
    }
    await db.start();
    const pool = new pg.Pool({
      host: '127.0.0.1',
      port,
      user: 'moria_local',
      password,
      database: 'postgres',
      max: 10,
    });
    const disconnects = new Set<Promise<void>>();
    pool.on('connect', (client) => {
      const disconnected = new Promise<void>((resolveEnd) => {
        client.once('end', () => {
          disconnects.delete(disconnected);
          resolveEnd();
        });
      });
      disconnects.add(disconnected);
    });
    return {
      pool,
      stop: async () => {
        // The caller ends the pool first, but pg-pool can resolve end() before
        // removed clients disconnect. Keep the cluster alive until they finish.
        await Promise.all(disconnects);
        await db.stop();
      },
    };
  } catch (error) {
    await db.stop();
    throw error;
  }
}
