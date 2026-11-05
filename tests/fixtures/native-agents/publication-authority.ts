import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import type { Pool } from 'pg';

export async function publicationAuthorityChecks(
  pool: Pool,
  execution: string,
  connection: string,
  publish: (connection: string) => Promise<unknown>,
): Promise<number> {
  const url = new URL(connection);
  const originalRole = url.username;
  assert.match(originalRole, /^agent_[a-f0-9]{32}$/);
  const role = 'agent_proof_' + randomUUID().replaceAll('-', '');
  const password = randomUUID();
  url.username = role;
  url.password = password;
  const definition = async () =>
    (
      await pool.query(`SELECT pg_get_functiondef(p.oid) AS sql,p.proowner,p.proacl::text,p.prosecdef
      FROM pg_proc p WHERE p.oid='moria.runtime_function_worker_check(uuid,uuid,text,text)'::regprocedure`)
    ).rows[0];
  const original = await definition();
  const workerCheck =
    'PERFORM moria.runtime_worker_check(worker,host,resource_class,expected_domain);';
  assert.equal(original.sql.split(workerCheck).length, 2);
  const measured = original.sql.replace(
    workerCheck,
    `${workerCheck}
    IF session_user='${role}' AND current_setting('transaction_read_only')='off' THEN
      PERFORM public.record_agent_publication_check();
    END IF;`,
  );
  // The worker inherits the existing fixture role; only its committed transaction is measured.
  await pool.query(`
    CREATE ROLE ${role} LOGIN INHERIT PASSWORD '${password}';
    GRANT ${originalRole} TO ${role};
    CREATE TABLE public.agent_publication_checks(transaction_id text PRIMARY KEY,checks integer NOT NULL);
    CREATE FUNCTION public.record_agent_publication_check() RETURNS void
      LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
      INSERT INTO public.agent_publication_checks VALUES(pg_current_xact_id()::xid::text,1)
      ON CONFLICT(transaction_id) DO UPDATE SET checks=agent_publication_checks.checks+1
    $$;
    REVOKE ALL ON FUNCTION public.record_agent_publication_check() FROM PUBLIC;
    GRANT EXECUTE ON FUNCTION public.record_agent_publication_check() TO ${role};
    ${measured};
  `);
  try {
    await publish(url.href);
    const rows = (
      await pool.query(
        `SELECT c.checks FROM moria.runtime_agent_receipts r
      JOIN public.agent_publication_checks c ON c.transaction_id=r.xmin::text WHERE r.execution=$1`,
        [execution],
      )
    ).rows;
    return rows[0]?.checks ?? 0;
  } finally {
    await pool.query(`${original.sql};
      DROP FUNCTION public.record_agent_publication_check();
      DROP TABLE public.agent_publication_checks;
      DROP ROLE ${role};
    `);
    assert.deepEqual(await definition(), original);
  }
}
