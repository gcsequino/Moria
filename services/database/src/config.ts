import type { PoolConfig } from 'pg';
import { DatabaseError } from './schema.js';

type Environment = Record<string, string | undefined>;
export function integer(
  env: Environment,
  name: string,
  fallback: number,
  min: number,
  max: number,
) {
  const text = env[name] ?? String(fallback);
  const value = Number(text);
  if (!/^\d+$/.test(text) || !Number.isSafeInteger(value) || value < min || value > max)
    throw new DatabaseError(`${name} must be an integer from ${min} to ${max}.`);
  return value;
}

/** Secret references resolve only from the supplied environment; never files.
 * URL query options are rejected to prevent implicit certificate/file reads. */
export function externalDatabase(
  env: Environment,
  purpose: 'runtime' | 'migration' = 'runtime',
): PoolConfig {
  const key = purpose === 'runtime' ? 'MORIA_DATABASE_SECRET_REF' : 'MORIA_MIGRATION_SECRET_REF';
  const reference = env[key];
  if (!reference || !/^[A-Z_][A-Z0-9_]*$/.test(reference) || !env[reference])
    throw new DatabaseError(`${key} must reference a populated environment variable.`);
  let url: URL;
  try {
    url = new URL(env[reference]!);
    if (
      !['postgres:', 'postgresql:'].includes(url.protocol) ||
      !url.hostname ||
      !url.username ||
      !url.password ||
      url.pathname.length < 2 ||
      url.search ||
      url.hash
    )
      throw new Error();
    decodeURIComponent(url.username);
    decodeURIComponent(url.password);
    decodeURIComponent(url.pathname.slice(1));
  } catch {
    throw new DatabaseError(
      `${key} must resolve to a PostgreSQL URL with user, password and database, without query options.`,
    );
  }
  const ssl = env.MORIA_DATABASE_TLS ?? 'verify';
  if (!['verify', 'disable'].includes(ssl))
    throw new DatabaseError('MORIA_DATABASE_TLS must be verify or disable.');
  // Snow holds the schema lease, supervisor and ingestion locks plus a query/transaction.
  // Weather holds the lease, supervisor and publication transaction plus an authority query.
  const max = integer(env, 'MORIA_DATABASE_POOL_MAX', 10, 4, 100);
  const replicas = integer(env, 'MORIA_DATABASE_REPLICAS', 1, 1, 1000);
  const budget = integer(env, 'MORIA_DATABASE_CONNECTION_BUDGET', 20, 5, 100000);
  if (max * replicas + 1 > budget)
    throw new DatabaseError(
      'Database pool budget exceeded: replicas × pool maximum + one migration connection must fit.',
    );
  return {
    host: url.hostname.replace(/^\[(.*)\]$/, '$1'),
    port: Number(url.port || 5432),
    user: decodeURIComponent(url.username),
    password: decodeURIComponent(url.password),
    database: decodeURIComponent(url.pathname.slice(1)),
    ssl: ssl === 'verify' ? { rejectUnauthorized: true } : false,
    max: purpose === 'migration' ? 1 : max,
    connectionTimeoutMillis: 5000,
    idleTimeoutMillis: 30000,
    application_name: `moria-${purpose}`,
  };
}
