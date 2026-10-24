import pg from 'pg';
import { Catalog, type Publication } from '../../services/catalog/src/catalog.js';

process.on(
  'message',
  async (message: { config: pg.PoolConfig; request: Publication; phase: 'before' | 'after' }) => {
    const pool = new pg.Pool(message.config);
    class CrashProbe extends Catalog {
      override async transaction<T>(
        fn: (client: pg.PoolClient) => Promise<T>,
        environment?: string,
      ): Promise<T> {
        const result = await super.transaction(async (c) => {
          const value = await fn(c);
          if (message.phase === 'before') {
            process.send?.({ phase: 'before' });
            await new Promise(() => {});
          }
          return value;
        }, environment);
        process.send?.({ phase: 'after' });
        await new Promise(() => {});
        return result;
      }
    }
    try {
      await new CrashProbe(pool).publish(message.request);
    } catch {
      process.send?.({ phase: 'error' });
      await pool.end();
      process.exit(1);
    }
  },
);
