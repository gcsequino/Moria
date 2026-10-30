import pg from 'pg';
import { Catalog } from '../../../services/catalog/src/catalog.js';
import { createLocalFilesystem } from '../../../services/filesystem/src/local.js';
import { Functions } from '../../../services/functions/src/service.js';
import { DockerFunctionWorker } from '../../../services/functions/src/docker.js';

process.once('message', async (input: { connection: pg.PoolConfig; files: string }) => {
  const pool = new pg.Pool(input.connection);
  const runtime = await createLocalFilesystem(new Catalog(pool, 'verify'), input.files);
  const functions = new Functions(runtime, new Map(), new DockerFunctionWorker());
  const close = async () => {
    await functions.close();
    await pool.end();
    process.disconnect?.();
  };
  process.once('SIGTERM', () => void close());
  functions.startWorker();
  process.send?.({ ready: true });
});
