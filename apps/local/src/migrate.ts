import pg from 'pg';
import { externalDatabase } from '../../../services/database/src/config.js';
import { schema, DatabaseError } from '../../../services/database/src/schema.js';

let pool: pg.Pool | undefined;
try {
  pool = new pg.Pool(externalDatabase(process.env, 'migration'));
  await schema(pool, 'apply');
  console.log('Database schema is ready.');
} catch (error) {
  console.error(error instanceof DatabaseError ? error.message : 'Database migration failed.');
  process.exitCode = 1;
} finally {
  await pool?.end();
}
