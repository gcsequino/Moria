import { createServer } from 'node:http';
import { readFileSync } from 'node:fs';
import pg from 'pg';
const queries = JSON.parse(readFileSync('benchmarks/ontology/queries.json', 'utf8'));
const pool = new pg.Pool({ connectionString: process.env.BENCH_DSN, max: 20 });
const server = createServer(async (req, res) => {
  let c: pg.PoolClient | undefined,
    locked = false,
    began = false;
  try {
    const url = new URL(req.url!, 'http://localhost');
    c = await pool.connect();
    if (url.pathname === '/action') {
      await c.query('SELECT pg_advisory_lock(91011)');
      locked = true;
      await c.query('BEGIN ISOLATION LEVEL SERIALIZABLE');
      began = true;
    }
    const auth = await c.query(queries.auth, [req.headers.authorization ?? '']);
    if (!auth.rowCount) {
      res.writeHead(401);
      res.end('{}');
      return;
    }
    const { region } = JSON.parse(Object.values(auth.rows[0])[0] as string);
    let result;
    if (url.pathname === '/queue')
      result = await c.query(
        queries[`queue_${url.searchParams.get('layout') === 'json' ? 'json' : 'typed'}`],
        [
          region,
          Number(url.searchParams.get('priority') ?? 0),
          url.searchParams.get('after') ?? '0',
        ],
      );
    else if (url.pathname === '/detail')
      result = await c.query(queries.detail, [url.searchParams.get('id'), region]);
    else if (url.pathname === '/action') {
      result = await c.query(queries.receipt, [url.searchParams.get('key')]);
      if (!result.rowCount)
        result = await c.query(queries.action, [
          url.searchParams.get('id'),
          region,
          url.searchParams.get('key'),
        ]);
      await c.query('COMMIT');
      began = false;
    } else {
      res.writeHead(404);
      res.end('{}');
      return;
    }
    const payload = result.rowCount ? JSON.parse(Object.values(result.rows[0])[0] as string) : {};
    res.writeHead(result.rowCount ? 200 : 404, { 'content-type': 'application/json' });
    res.end(JSON.stringify(payload));
  } catch {
    if (!res.headersSent) res.writeHead(500);
    res.end('{}');
  } finally {
    if (c) {
      try {
        if (began) await c.query('ROLLBACK');
        if (locked) await c.query('SELECT pg_advisory_unlock(91011)');
        c.release();
      } catch (error) {
        c.release(error as Error);
      }
    }
  }
});
server.listen(Number(process.env.PORT), '127.0.0.1', () => console.log('READY'));
