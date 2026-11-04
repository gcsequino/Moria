import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import test from 'node:test';
import { database } from './fixtures/catalog-storage/database.js';

test(
  'native change replay preserves complete authority transactions and replay pins',
  { timeout: 60000 },
  async (t) => {
    const db = await database();
    let writer: import('pg').PoolClient | undefined;
    t.after(async () => {
      writer?.release();
      await db.close();
    });
    const environment = 'change-fixture';
    await db.catalog.ensureEnvironment(environment);
    const role = `changes_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role}`,
    );
    const connection = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const native = (command: string, input: unknown) =>
      new Promise<any>((resolve, reject) => {
        const child = spawn(
          process.env.MORIA_NATIVE_BINARY ?? '/usr/local/bin/moria-runtime',
          ['changes', command],
          {
            env: {
              PATH: '/usr/local/bin:/usr/bin:/bin',
              MORIA_INDEXING_DATABASE_SECRET_REF: 'CHANGE_DATABASE',
              CHANGE_DATABASE: connection,
              MORIA_INDEXING_DATABASE_TLS: 'disable',
            },
            stdio: ['pipe', 'pipe', 'pipe'],
          },
        );
        let output = '',
          errors = '';
        child.stdout.on('data', (chunk) => (output += chunk));
        child.stderr.on('data', (chunk) => (errors += chunk));
        child.once('error', reject);
        child.once('exit', (code) =>
          code === 0 ? resolve(JSON.parse(output)) : reject(new Error(errors)),
        );
        child.stdin.end(JSON.stringify(input));
      });
    const origin = await native('position', { environment });
    assert.equal(origin.commitRevision, '0');
    const a = randomUUID(),
      b = randomUUID();
    for (const uid of [a, b]) {
      await db.pool.query(
        'INSERT INTO moria.identities(domain,canonical_key,uid) VALUES($1,$2,$3)',
        ['changes', uid, uid],
      );
      await db.pool.query('INSERT INTO moria.object_homes VALUES($1,$2)', [uid, 'objects']);
    }
    writer = await db.pool.connect();
    const client = writer;
    const edit = async (uid: string, revision: number, tombstone = false) =>
      client.query('INSERT INTO moria.overlay_versions VALUES($1,$2,$3,$4,$5,$6)', [
        environment,
        'objects',
        uid,
        revision,
        {},
        tombstone,
      ]);
    let first: any;

    await t.test(
      'uncommitted edits are invisible; all revisions in one transaction share one end marker',
      async () => {
        await client.query('BEGIN');
        await client.query('UPDATE moria.environments SET revision=revision+1 WHERE id=$1', [
          environment,
        ]);
        await edit(a, 1);
        await client.query('UPDATE moria.environments SET revision=revision+1 WHERE id=$1', [
          environment,
        ]);
        await edit(b, 2, true);
        assert.deepEqual((await native('read', { after: origin })).transactions, []);
        await client.query('COMMIT');
        const page = await native('read', { after: origin });
        assert.equal(page.transactions.length, 1);
        const frame = page.transactions[0];
        assert.equal(frame.transactionEnd, true);
        assert.equal(frame.position.commitRevision, '2');
        assert.deepEqual(
          frame.events.map((x: any) => [x.ordinal, x.revision, x.uid]),
          [
            [0, '1', a],
            [1, '2', b],
          ],
        );
        first = page.next;
        await assert.rejects(native('read', { after: origin, itemBudget: 1 }));
        await assert.rejects(native('read', { after: { ...origin, commitRevision: '1' } }));
      },
    );

    await t.test(
      'rollback and actual process loss leave no event; complete generation requests pinned rebuild',
      async () => {
        await client.query('BEGIN');
        await client.query('UPDATE moria.environments SET revision=revision+1 WHERE id=$1', [
          environment,
        ]);
        await edit(a, 3);
        await client.query('ROLLBACK');
        assert.deepEqual((await native('read', { after: first })).transactions, []);
        const abandoned = spawn(
          process.execPath,
          [
            '--input-type=module',
            '-e',
            `
      import pg from 'pg';
      const client=new pg.Client({connectionString:process.env.CHANGE_DATABASE});
      await client.connect();
      await client.query('BEGIN');
      await client.query('UPDATE moria.environments SET revision=revision+1 WHERE id=$1',[process.env.CHANGE_ENVIRONMENT]);
      await client.query('INSERT INTO moria.overlay_versions VALUES($1,$2,$3,3,$4,false)',[process.env.CHANGE_ENVIRONMENT,'objects',process.env.CHANGE_UID,{}]);
      console.log('ready');
    `,
          ],
          {
            env: {
              ...process.env,
              CHANGE_DATABASE: connection,
              CHANGE_ENVIRONMENT: environment,
              CHANGE_UID: a,
            },
            stdio: ['ignore', 'pipe', 'pipe'],
          },
        );
        t.after(() => abandoned.kill('SIGKILL'));
        await new Promise<void>((resolve, reject) => {
          abandoned.stdout.once('data', () => resolve());
          abandoned.once('error', reject);
          abandoned.once('exit', () => reject(new Error('writer exited before barrier')));
        });
        const stopped = new Promise<void>((resolve) => abandoned.once('exit', () => resolve()));
        abandoned.kill('SIGKILL');
        await stopped;
        assert.deepEqual((await native('read', { after: first })).transactions, []);
        await client.query('BEGIN');
        await client.query('UPDATE moria.environments SET revision=revision+1 WHERE id=$1', [
          environment,
        ]);
        await client.query(
          'INSERT INTO moria.source_generations(environment,object_type,revision) VALUES($1,$2,3)',
          [environment, 'objects'],
        );
        await client.query('COMMIT');
        const result = await native('read', { after: first });
        assert.equal(result.transactions[0].events[0].kind, 'rebuild');
        assert.equal(result.transactions[0].events[0].revision, '3');
        assert.deepEqual(await native('read', { after: first }), result);
      },
    );

    await t.test(
      'pins cannot change owner, rewind, revive after expiry or cross incarnation',
      async () => {
        const id = randomUUID(),
          owner = randomUUID();
        await native('pin', { id, owner, after: origin, seconds: 60 });
        await native('pin', { id, owner, after: first, seconds: 60 });
        await assert.rejects(native('pin', { id, owner, after: origin, seconds: 60 }));
        await assert.rejects(native('pin', { id, owner: randomUUID(), after: first, seconds: 60 }));
        await native('release', { id, owner: randomUUID() });
        assert.equal(
          (await db.pool.query('SELECT count(*) FROM moria.runtime_change_pins WHERE id=$1', [id]))
            .rows[0].count,
          '1',
        );
        await db.pool.query(
          "UPDATE moria.runtime_change_pins SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
          [id],
        );
        await assert.rejects(native('pin', { id, owner, after: first, seconds: 60 }));
        await assert.rejects(native('read', { after: { ...first, incarnation: randomUUID() } }));
        await native('release', { id, owner });
      },
    );
    await t.test('pagination orders numeric commit positions past revision nine', async () => {
      for (let revision = 4; revision <= 12; revision++) {
        await client.query('BEGIN');
        await client.query('UPDATE moria.environments SET revision=$2 WHERE id=$1', [
          environment,
          revision,
        ]);
        await edit(a, revision);
        await client.query('COMMIT');
      }
      let after = origin;
      const revisions: string[] = [];
      for (let page = 0; page < 20; page++) {
        const result = await native('read', { after, transactions: 2 });
        if (!result.transactions.length) break;
        revisions.push(...result.transactions.map((frame: any) => frame.position.commitRevision));
        after = result.next;
      }
      assert.deepEqual(
        revisions,
        Array.from({ length: 11 }, (_, index) => String(index + 2)),
      );
      assert.equal(after.commitRevision, '12');
    });
  },
);
