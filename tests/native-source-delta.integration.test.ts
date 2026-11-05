import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { execFile, spawn } from 'node:child_process';
import { promisify } from 'node:util';
import test from 'node:test';
import pg from 'pg';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';
import { ageClosedDatasetHistory } from './fixtures/native-retention/history.js';
import { prepareManagedSource } from './fixtures/native-source-store/managed.js';
import { requestCommitProxy } from './fixtures/runtime/postgres-proxy.js';
import type { ObjectSchema } from '../services/ontology/src/store.js';

for (const graph of [false, true])
  test(
    `real ${graph ? 'v3' : 'v2'} Source windows retain superseded intermediate roots and qualify resumable journal pages`,
    { timeout: 300000 },
    async (t) => {
      const columns: ObjectSchema['properties'] = {
        uid: { type: 'uid', nullable: false },
        tags: { type: 'array', nullable: false, items: { type: 'string', nullable: false } },
        note: { type: 'string', nullable: true },
      };
      const fixture = await managedRetentionDataset({ primaryKey: ['tags'], columns });
      const cleanup: (() => Promise<unknown>)[] = [];
      t.after(async () => {
        const errors: unknown[] = [];
        for (const close of cleanup.reverse().concat(fixture.close)) {
          try {
            await close();
          } catch (error) {
            errors.push(error);
          }
        }
        assert.deepEqual(
          errors,
          [],
          'close owned SQL pools and coordinator before fixture shutdown',
        );
      });
      const { db, environment, dataset } = fixture;
      const domain = environment + '/source-delta';
      const owner = await fixture.command(['identity', 'register-domain'], {
        domain,
        cell: fixture.cell,
      });
      const identity = await fixture.command(['identity', 'resolve'], {
        domain,
        keys: ['one', 'two'],
        owner,
      });
      const rows = identity.uids.map((uid: string, index: number) => ({
        uid,
        tags: [String(index)],
        note: index ? null : 'visible',
      }));
      await fixture.publish(rows, { graph });
      const retainedSecond = await fixture.publish(rows, { graph });
      const baseline = await fixture.publish(rows, { graph });
      const managed = await prepareManagedSource(fixture, columns);
      await managed.drain();
      const alias = (
        await db.pool.query(
          'SELECT * FROM moria.native_source_generation_aliases WHERE environment=$1 AND object_type=$2 ORDER BY revision DESC LIMIT 1',
          [environment, managed.target],
        )
      ).rows[0];
      assert.equal(alias.indexed_dataset_revision, baseline.head.revision);
      assert.equal(
        (
          await db.pool.query(
            'SELECT count(*)::text FROM moria.staged_source_rows WHERE stage=$1',
            [alias.store],
          )
        ).rows[0].count,
        '2',
      );
      await ageClosedDatasetHistory(fixture);
      const policy = await fixture.command(
        ['dataset-retention', 'approve'],
        {
          requestKey: randomUUID(),
          environment,
          dataset,
          retainSeconds: 86400,
          recheckSeconds: 30,
        },
        fixture.operatorEnv,
      );
      const expire = async (beforeRevision: string) =>
        await fixture.command(
          ['dataset-retention', 'expire'],
          {
            policy: policy.policy,
            requestKey: randomUUID(),
            owner: randomUUID(),
            beforeRevision,
            maximumRoots: 1,
            timeSeconds: 120,
          },
          fixture.operatorEnv,
        );
      const first = await expire(retainedSecond.head.revision);
      assert.equal(first.headChanged, true);
      const firstHead = (await db.catalog.heads(environment, [dataset]))[0];
      const firstBuild = (
        await db.pool.query(
          'SELECT id FROM moria.indexing_builds WHERE environment=$1 AND target=$2 AND source_revision=$3',
          [environment, managed.target, firstHead.revision],
        )
      ).rows[0].id;
      const second = await expire(baseline.head.revision);
      assert.equal(second.headChanged, true);
      const target = (await db.catalog.heads(environment, [dataset]))[0];
      assert.equal(
        (await db.pool.query('SELECT status FROM moria.indexing_builds WHERE id=$1', [firstBuild]))
          .rows[0].status,
        'superseded',
      );
      const build = (
        await db.pool.query(
          "SELECT id FROM moria.indexing_builds WHERE environment=$1 AND target=$2 AND source_revision=$3 AND status='pending'",
          [environment, managed.target, target.revision],
        )
      ).rows[0].id;
      const window = (
        await db.pool.query('SELECT * FROM moria.native_source_delta_windows WHERE build=$1', [
          build,
        ])
      ).rows[0];
      assert.equal(window.basis_dataset_revision, baseline.head.revision);
      assert.equal(window.cursor_revision, baseline.head.revision);
      assert.equal(window.target_dataset_revision, target.revision);
      const root = (
        await db.pool.query(
          'SELECT result_root FROM moria.runtime_dataset_commit_deltas WHERE environment=$1 AND dataset=$2 AND result_revision=$3',
          [environment, dataset, firstHead.revision],
        )
      ).rows[0].result_root;
      assert.equal(
        (await db.pool.query('SELECT moria.runtime_dataset_root_pinned($1) AS pinned', [root]))
          .rows[0].pinned,
        true,
        'latest actual range preserves the superseded intermediate receipt',
      );
      assert.equal(
        (
          await db.pool.query(
            'SELECT count(*)::text FROM moria.native_source_delta_members WHERE build=$1',
            [build],
          )
        ).rows[0].count,
        '0',
        'range custody precedes bounded member capture',
      );

      // Hold the real coordinator's committed claim check before its first page; no worker tuple is fabricated.
      const proxy = await requestCommitProxy(db.pool.options.port!, 'indexing_claim_check');
      cleanup.push(proxy.close);
      const connection = new URL(fixture.nativeEnv.RETENTION_DATABASE!);
      connection.port = String(proxy.port);
      const child = spawn(fixture.runtime, ['plan'], {
        env: { ...fixture.nativeEnv, RETENTION_DATABASE: connection.href },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      let output = '',
        error = '';
      child.stdout.on('data', (part) => {
        output = (output + part).slice(-65536);
      });
      child.stderr.on('data', (part) => {
        error = (error + part).slice(-65536);
      });
      const exited = new Promise<void>((resolve, reject) => {
        child.once('error', reject);
        child.once('exit', () => resolve());
      });
      cleanup.push(async () => {
        if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
        await exited;
      });
      await Promise.race([
        proxy.committed,
        exited.then(() => {
          throw new Error('coordinator exited before held claim: ' + error + output);
        }),
      ]);
      const claim = (
        await db.pool.query(
          "SELECT owner,fence::text,lease_until>clock_timestamp() AS live FROM moria.indexing_builds WHERE id=$1 AND status='planning'",
          [build],
        )
      ).rows[0];
      assert.equal(claim.live, true);
      const ordinary = new pg.Pool({
        connectionString: fixture.nativeEnv.RETENTION_DATABASE,
        options:
          '-c moria.cell_id=' +
          fixture.cell +
          ' -c statement_timeout=30000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=30000',
      });
      cleanup.push(() => ordinary.end());
      await assert.rejects(
        ordinary.query(
          'UPDATE moria.native_source_delta_windows SET cursor_revision=cursor_revision WHERE build=$1',
          [build],
        ),
        /SOURCE_STORE_AUTHORITY_REQUIRED/,
      );
      await assert.rejects(
        ordinary.query('SELECT moria.runtime_source_delta_page($1,$2,$3,1)', [
          build,
          randomUUID(),
          claim.fence,
        ]),
        /LEASE_LOST/,
      );
      await assert.rejects(
        ordinary.query('SELECT moria.runtime_source_delta_page($1,$2,$3,257)', [
          build,
          claim.owner,
          claim.fence,
        ]),
        /RESOURCE_LIMIT/,
      );
      await assert.rejects(
        ordinary.query('SELECT moria.runtime_source_delta_page($1,$2,$3,NULL)', [
          build,
          claim.owner,
          claim.fence,
        ]),
        /RESOURCE_LIMIT/,
      );
      const page = async () =>
        (
          await ordinary.query('SELECT moria.runtime_source_delta_page($1,$2,$3,1) AS page', [
            build,
            claim.owner,
            claim.fence,
          ])
        ).rows[0].page;
      const one = await page();
      assert.equal(one.captured, 1);
      assert.equal(one.state, 'capturing');
      assert.equal(one.pending, true);
      assert.equal(one.capturedThrough, firstHead.revision);
      assert.equal(one.targetDatasetRevision, target.revision);
      assert(Number(one.descriptorBytes) <= 1048576);
      const two = await page();
      assert.equal(two.captured, 1);
      assert.equal(two.state, 'qualified');
      assert.equal(two.kind, 'unchanged');
      assert.equal(two.pending, false);
      assert.equal(two.capturedThrough, target.revision);
      assert.equal((await page()).captured, 0, 'acknowledgement retry does not duplicate a member');
      assert.equal(
        (
          await db.pool.query(
            'SELECT count(*)::text FROM moria.native_source_delta_members WHERE build=$1',
            [build],
          )
        ).rows[0].count,
        '2',
      );
      assert.equal(
        (
          await db.pool.query(
            'SELECT count(*)::text FROM moria.native_source_generation_aliases WHERE environment=$1 AND object_type=$2',
            [environment, managed.target],
          )
        ).rows[0].count,
        '1',
        'custody qualification does not publish an alias or watermark',
      );
      assert.equal(
        (
          await db.pool.query(
            'SELECT count(*)::text FROM moria.staged_source_rows WHERE stage=$1',
            [alias.store],
          )
        ).rows[0].count,
        '2',
      );
      assert.deepEqual((await db.catalog.heads(environment, [dataset]))[0], target);
      child.kill('SIGKILL');
      await exited;
      await proxy.close();
      await promisify(execFile)(fixture.runtime, ['cancel', build], {
        env: fixture.nativeEnv,
        timeout: 30000,
        maxBuffer: 1048576,
      });
      assert.equal(
        (
          await db.pool.query(
            'SELECT closed_at IS NOT NULL AS closed FROM moria.native_source_delta_windows WHERE build=$1',
            [build],
          )
        ).rows[0].closed,
        true,
      );
      assert.equal(
        (await db.pool.query('SELECT moria.runtime_dataset_root_pinned($1) AS pinned', [root]))
          .rows[0].pinned,
        false,
        'actual cancellation releases transient Dataset custody',
      );
      assert.equal(
        (
          await db.pool.query('SELECT count(*)::text FROM moria.runtime_object_base($1,$2,$3)', [
            environment,
            managed.target,
            alias.revision,
          ])
        ).rows[0].count,
        '2',
      );
      t.diagnostic(
        JSON.stringify({
          realFormat: graph ? 'v3' : 'v2',
          actualUnchangedEdges: 2,
          realSupersession: true,
          intermediateCustodyBeforePages: true,
          pageLimit: 1,
          first: one,
          second: two,
          noAliasOrPopulationCopy: true,
          realCancellationReleasedCustody: true,
        }),
      );
    },
  );
