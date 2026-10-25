import { access } from 'node:fs/promises';
import { resolve } from 'node:path';
import EmbeddedPostgres from 'embedded-postgres';
import pg from 'pg';

/** Only the development launcher owns a cluster. Existing paths and credentials
 * stay unchanged for supported versioned databases. */
export async function startDevelopmentDatabase(directory: string, port: number, password: string) {
  const db = new EmbeddedPostgres({
    databaseDir: resolve(directory, 'postgres'),
    user: 'moria_local',
    password,
    port,
    persistent: true,
    authMethod: 'scram-sha-256',
    initdbFlags: ['--encoding=UTF8', '--locale=C'],
    postgresFlags: ['-c', 'listen_addresses=127.0.0.1'],
    onLog: () => {},
    onError: () => {},
  });
  try {
    await access(resolve(directory, 'postgres', 'PG_VERSION'));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    await db.initialise();
  }
  await db.start();
  return {
    pool: new pg.Pool({
      host: '127.0.0.1',
      port,
      user: 'moria_local',
      password,
      database: 'postgres',
      max: 10,
    }),
    stop: () => db.stop(),
  };
}
