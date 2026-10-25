import pg from 'pg';
import { Catalog } from '../../services/catalog/src/catalog.js';
import { Jobs } from '../../services/execution/src/jobs.js';

process.on(
  'message',
  async (message: { connection: pg.PoolConfig; mode: string; kind: string }) => {
    const pool = new pg.Pool(message.connection),
      catalog = new Catalog(pool);
    const hold = () => new Promise<void>(() => {});
    const jobs = new Jobs(catalog, {
      [message.kind]: async (job) => {
        await catalog.transaction(async (c) => {
          await job.guard(c);
          await c.query(
            'INSERT INTO public.job_test_publications(id) VALUES($1) ON CONFLICT DO NOTHING',
            [job.id],
          );
          await job.complete(c, { published: true });
          if (message.mode === 'before') {
            process.send?.('ready');
            await hold();
          }
        });
        process.send?.('ready');
        await hold();
      },
    });
    await jobs.tick();
  },
);
