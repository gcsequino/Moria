import { createHash } from 'node:crypto';
import type { Pool, PoolClient } from 'pg';
import { baseline } from './baseline.js';

export const schemaVersion = 1;
const checksum = createHash('sha256').update(baseline).digest('hex');
// Separate from environment publication/authorization locks.
const lock = '731804135219';
export class DatabaseError extends Error {}

async function verify(client: PoolClient) {
  const result = await client.query(
    'SELECT version, checksum FROM moria.schema_version WHERE singleton',
  );
  if (
    result.rows.length !== 1 ||
    result.rows[0].version !== schemaVersion ||
    result.rows[0].checksum !== checksum
  )
    throw new DatabaseError(
      'Database schema is incompatible; use the matching release and migration command.',
    );
}

/** The migration command is the sole external schema owner. Fresh initialization
 * commits with its version marker, without application bootstrap. */
export async function schema(pool: Pool, mode: 'apply' | 'verify') {
  const client = await pool.connect().catch(() => {
    throw new DatabaseError('Database connection failed; verify connectivity and credentials.');
  });
  try {
    if (mode === 'verify') {
      await verify(client);
      return;
    }
    await client.query('BEGIN');
    await client.query("SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='60s'");
    const acquired = await client.query(
      'SELECT pg_try_advisory_xact_lock($1::bigint) AS acquired',
      [lock],
    );
    if (!acquired.rows[0].acquired)
      throw new DatabaseError('Database migration is busy; stop services and retry.');
    const exists = await client.query("SELECT to_regclass('moria.schema_version') AS name");
    if (exists.rows[0].name) {
      await verify(client);
    } else {
      const existingSchema = await client.query("SELECT to_regnamespace('moria') AS name");
      if (existingSchema.rows[0].name)
        throw new DatabaseError(
          'Unversioned Moria schema: recreate the disposable development database explicitly; see docs/DATABASE.md. No schema or data was changed.',
        );
      await client.query(baseline);
      await client.query(`CREATE TABLE moria.schema_version (
        singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
        version integer NOT NULL, checksum text NOT NULL
      )`);
      await client.query('INSERT INTO moria.schema_version VALUES(true,$1,$2)', [
        schemaVersion,
        checksum,
      ]);
    }
    await client.query('COMMIT');
  } catch (error) {
    if (mode === 'apply') await client.query('ROLLBACK');
    if (error instanceof DatabaseError) throw error;
    throw new DatabaseError(
      'Database schema operation failed; verify migration ownership and schema version.',
    );
  } finally {
    client.release();
  }
}

/** Keep a lifetime lease and independently fence each runtime checkout. Lease
 * loss stops admission while active checkouts retain migration exclusion. */
export async function holdSchema(pool: Pool) {
  const client = await pool.connect().catch(() => {
    throw new DatabaseError('Database connection failed; verify connectivity and credentials.');
  });
  let lost = false;
  client.on('error', () => {
    if (lost) return;
    lost = true;
    pool.emit('error', new DatabaseError('Database schema lease lost.'), client);
  });
  try {
    const result = await client.query(
      'SELECT pg_try_advisory_lock_shared($1::bigint) AS acquired',
      [lock],
    );
    if (!result.rows[0].acquired) throw new DatabaseError('Database migration is in progress.');
    await verify(client);
    const role = await client.query(`SELECT rolsuper, rolbypassrls, rolcreatedb, rolcreaterole,
      EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='moria' AND pg_has_role(current_user,c.relowner,'MEMBER')) AS owns_tables,
      has_schema_privilege(current_user,'moria','CREATE') AS can_create,
      EXISTS (SELECT 1 FROM pg_roles r WHERE
        (r.rolsuper OR r.rolbypassrls OR r.rolcreatedb OR r.rolcreaterole)
        AND pg_has_role(current_user,r.oid,'MEMBER')) AS privileged_membership
      FROM pg_roles WHERE rolname=current_user`);
    if (Object.values(role.rows[0]).some(Boolean))
      throw new DatabaseError(
        'Database runtime role must be scoped: no ownership, schema CREATE or administrative privileges.',
      );
    fenceCheckouts(pool, () => lost);
    return () => {
      lost = true;
      client.release(true);
    };
  } catch (error) {
    client.release(true);
    if (error instanceof DatabaseError) throw error;
    throw new DatabaseError(
      'Database startup failed; verify connectivity, runtime grants and schema version.',
    );
  }
}

/** Each checkout owns exclusion independently of the lifetime lease. This covers
 * pool.query as well as explicit transactions, including work already draining
 * when the lease connection disappears. Never return an unlocked client. */
function fenceCheckouts(pool: Pool, lost: () => boolean) {
  const connect = pool.connect.bind(pool);
  const acquire = async () => {
    if (lost()) throw new DatabaseError('Database schema lease lost.');
    const client = await connect().catch(() => {
      throw new DatabaseError('Database connection failed; verify connectivity and credentials.');
    });
    try {
      const result = await client.query(
        'SELECT pg_try_advisory_lock_shared($1::bigint) AS acquired',
        [lock],
      );
      if (!result.rows[0].acquired) throw new DatabaseError('Database migration is in progress.');
      // Migration may have completed between checkouts. Check compatibility while
      // holding exclusion, and recheck lease loss after asynchronous admission.
      await verify(client);
      if (lost()) throw new DatabaseError('Database schema lease lost.');
    } catch (error) {
      client.release(true);
      throw error instanceof DatabaseError
        ? error
        : new DatabaseError('Database operation admission failed.');
    }
    const release = client.release.bind(client);
    let released = false;
    client.release = (error?: Error | boolean) => {
      if (released) throw new Error('Database client already released.');
      released = true;
      if (error) {
        release(error);
        return;
      }
      // A failed/abandoned transaction cannot unlock: destroy that connection.
      // The pool must not reuse it until the shared lock is released.
      void client.query('SELECT pg_advisory_unlock_shared($1::bigint)', [lock]).then(
        () => release(),
        () => release(true),
      );
    };
    return client;
  };
  pool.connect = ((
    callback?: (
      error: Error | undefined,
      client?: PoolClient,
      release?: PoolClient['release'],
    ) => void,
  ) => {
    if (!callback) return acquire();
    void acquire().then(
      (client) => callback(undefined, client, client.release),
      (error: Error) => callback(error),
    );
  }) as Pool['connect'];
}
