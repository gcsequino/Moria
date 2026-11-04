import assert from 'node:assert/strict';
import { randomBytes, randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { database } from './fixtures/catalog-storage/database.js';
import { commitProxy } from './fixtures/indexing/commit-proxy.js';

test(
  'native identity batches preserve canonical allocation through transfer and lost acknowledgment',
  { timeout: 120000 },
  async (t) => {
    const db = await database();
    t.after(db.close);
    const role = `identity_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role}; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    const connection = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const binary = process.env.MORIA_NATIVE_BINARY ?? '/usr/local/bin/moria-runtime';
    const start = (command: string, input: unknown, url = connection) => {
      const child = spawn(binary, ['identity', command], {
        env: {
          PATH: '/usr/local/bin:/usr/bin:/bin',
          MORIA_INDEXING_DATABASE_SECRET_REF: 'IDENTITY_DATABASE',
          IDENTITY_DATABASE: url,
          MORIA_INDEXING_DATABASE_TLS: 'disable',
        },
        stdio: ['pipe', 'pipe', 'pipe'],
      });
      let output = '',
        errors = '';
      child.stdout.on('data', (chunk) => (output += chunk.toString()));
      child.stderr.on('data', (chunk) => (errors += chunk.toString()));
      child.stdin.end(JSON.stringify(input));
      const done = new Promise<any>((resolve, reject) => {
        child.once('error', reject);
        child.once('exit', (code, signal) =>
          code === 0
            ? resolve(JSON.parse(output))
            : reject(new Error(`Native identity ${command} failed (${code ?? signal}): ${errors}`)),
        );
      });
      void done.catch(() => {});
      return { child, done };
    };
    const a = randomUUID(),
      b = randomUUID(),
      domain = `native-${randomUUID()}`;
    await start('register-cell', { cell: a, endpointRef: 'CELL_A_ENDPOINT' }).done;
    await start('register-cell', { cell: b, endpointRef: 'CELL_B_ENDPOINT' }).done;
    let owner = await start('register-domain', { domain, cell: a }).done;
    const keys = Array.from({ length: 256 }, (_, i) => `canonical-${i}`);
    let allocated: string[];

    await t.test(
      'independent native processes resolve each canonical key once in bounded batches',
      async () => {
        const results = await Promise.all(
          Array.from({ length: 8 }, () => start('resolve', { domain, keys, owner }).done),
        );
        allocated = results[0].uids;
        for (const result of results) assert.deepEqual(result.uids, allocated);
        assert.equal(new Set(allocated).size, keys.length);
        assert.equal(
          (await db.pool.query('SELECT count(*) FROM moria.identities WHERE domain=$1', [domain]))
            .rows[0].count,
          String(keys.length),
        );
        assert.deepEqual(
          (await start('resolve', { domain, keys: [keys[0], keys[0]], owner }).done).uids,
          [allocated[0], allocated[0]],
        );
        await assert.rejects(db.catalog.resolveIdentity(domain, keys[0]), {
          code: 'OWNERSHIP_REQUIRED',
        });
        await assert.rejects(
          start('resolve', { domain, keys: [...keys, ...keys, ...keys, ...keys], owner }).done,
        );
      },
    );

    await t.test(
      'full-width canonical keys remain stable through concurrent native allocation',
      async () => {
        const key = randomBytes(32768).toString('hex');
        const result = await Promise.all(
          Array.from(
            { length: 8 },
            () => start('resolve', { domain, keys: [key, key], owner }).done,
          ),
        );
        for (const row of result) assert.deepEqual(row.uids, result[0].uids);
        assert.equal(result[0].uids[0], result[0].uids[1]);
        assert.equal(
          (
            await db.pool.query('SELECT canonical_key FROM moria.identities WHERE uid=$1', [
              result[0].uids[0],
            ])
          ).rows[0].canonical_key,
          key,
        );
      },
    );

    await t.test(
      'owner transfer waits for an already-admitted allocation and fences all later old-owner requests',
      async (t) => {
        const blocker = await db.pool.connect();
        await blocker.query('BEGIN');
        await blocker.query('LOCK moria.identities IN ACCESS EXCLUSIVE MODE');
        t.after(async () => {
          await blocker.query('ROLLBACK');
          blocker.release();
        });
        const inFlight = start('resolve', { domain, keys: ['before-transfer'], owner });
        t.after(() => inFlight.child.kill('SIGKILL'));
        let waiting = false;
        for (let i = 0; i < 200 && !waiting; i++) {
          waiting = (
            await db.pool.query(
              "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE usename=$1 AND wait_event_type='Lock' AND query LIKE 'SELECT uid FROM moria.runtime_identity_resolve%') AS waiting",
              [role],
            )
          ).rows[0].waiting;
          if (!waiting) await delay(10);
        }
        assert.equal(
          waiting,
          true,
          'the native allocation must hold the ownership row at an actual SQL barrier',
        );
        const transfer = start('transfer', { domain, owner, next: b });
        t.after(() => transfer.child.kill('SIGKILL'));
        await blocker.query('COMMIT');
        const [allocation, moved] = await Promise.all([inFlight.done, transfer.done]);
        assert.equal(moved.cell, b);
        assert.equal(moved.fence, '2');
        await assert.rejects(start('resolve', { domain, keys: ['stale'], owner }).done);
        owner = moved;
        assert.deepEqual(
          (await start('resolve', { domain, keys: ['before-transfer'], owner }).done).uids,
          allocation.uids,
        );
        assert.deepEqual((await start('resolve', { domain, keys, owner }).done).uids, allocated!);
      },
    );

    await t.test(
      'a committed allocation survives native process death before COMMIT acknowledgment',
      async (t) => {
        const proxy = await commitProxy(db.pool.options.port!, 'moria.runtime_identity_resolve');
        t.after(proxy.close);
        const pending = start(
          'resolve',
          { domain, keys: ['lost-ack'], owner },
          connection.replace(`:${db.pool.options.port}/`, `:${proxy.port}/`),
        );
        t.after(() => pending.child.kill('SIGKILL'));
        await proxy.commit;
        const durable = (
          await db.pool.query(
            'SELECT uid FROM moria.identities WHERE domain=$1 AND canonical_key=$2',
            [domain, 'lost-ack'],
          )
        ).rows[0].uid;
        pending.child.kill('SIGKILL');
        await assert.rejects(pending.done);
        assert.deepEqual(
          (await start('resolve', { domain, keys: ['lost-ack'], owner }).done).uids,
          [durable],
        );
      },
    );

    await t.test(
      'lost transfer acknowledgment is inspected without repeating a stale ownership move',
      async (t) => {
        const proxy = await commitProxy(
          db.pool.options.port!,
          'UPDATE moria.runtime_identity_domains',
        );
        t.after(proxy.close);
        const pending = start(
          'transfer',
          { domain, owner, next: a },
          connection.replace(`:${db.pool.options.port}/`, `:${proxy.port}/`),
        );
        t.after(() => pending.child.kill('SIGKILL'));
        await proxy.commit;
        pending.child.kill('SIGKILL');
        await assert.rejects(pending.done);
        await assert.rejects(start('transfer', { domain, owner, next: a }).done);
        owner = await start('inspect', { domain }).done;
        assert.equal(owner.fence, '3');
        assert.equal(owner.cell, a);
        assert.deepEqual((await start('resolve', { domain, keys, owner }).done).uids, allocated!);
        assert.equal(
          (
            await db.pool.query(
              "SELECT count(*) FROM moria.runtime_ownership_events WHERE kind='identity' AND resource=$1",
              [domain],
            )
          ).rows[0].count,
          '2',
        );
      },
    );

    await t.test('disabled owners and restored incarnations cannot allocate', async () => {
      await db.pool.query('UPDATE moria.runtime_cells SET enabled=false WHERE id=$1', [a]);
      await assert.rejects(start('resolve', { domain, keys: ['disabled'], owner }).done);
      await db.pool.query('UPDATE moria.runtime_cells SET enabled=true WHERE id=$1', [a]);
      await db.pool.query('UPDATE moria.indexing_instance SET incarnation=gen_random_uuid()');
      await assert.rejects(start('resolve', { domain, keys: ['old-incarnation'], owner }).done);
      assert.equal(
        (
          await db.pool.query(
            "SELECT count(*) FROM moria.identities WHERE domain=$1 AND canonical_key IN ('disabled','stale','old-incarnation')",
            [domain],
          )
        ).rows[0].count,
        '0',
      );
    });
  },
);
