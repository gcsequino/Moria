import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { readFile, readdir, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { nativeProjectionCapacity } from '../benchmarks/native-projection.js';

test(
  'actual accepted verifier scopes bound encoded multi-column row-group header IO',
  { timeout: 120000 },
  async (t) => {
    const output = await mkdtemp(join(tmpdir(), 'moria-header-proof-'));
    t.after(() => rm(output, { recursive: true, force: true }));
    await nativeProjectionCapacity({
      rows: 3000,
      rowBytes: 1024,
      output,
      onVerified: async (fixture) => {
        const storage = JSON.parse(fixture.nativeEnv.MORIA_NATIVE_STORAGE_BINDINGS!);
        const selected = storage.find(
          (value: any) =>
            value.namespace.warehouseId === 'warehouse' && value.namespace.tableId === 'table',
        );
        assert(selected?.provider.kind === 'posix');
        // A stricter reader limit selects a fresh verification key; accepted source identity and bytes stay fixed.
        selected.provider.limits.maxObjectBytes--;
        fixture.nativeEnv.MORIA_NATIVE_STORAGE_BINDINGS = JSON.stringify(storage);
        const indexed = fixture.reindex('header-window');
        void indexed.catch(() => {});
        let record: any;
        for (let i = 0; i < 500 && !record; i++) {
          record = (
            await fixture.db.pool.query(
              `SELECT v.key AS "verifierKey",v.owner AS "verifierOwner",v.fence AS "verifierFence",b.id AS build,b.owner AS "buildOwner",b.fence AS "buildFence"
       FROM moria.runtime_snapshot_verifications v JOIN moria.indexing_builds b ON b.environment=v.environment AND b.source=v.resource
       WHERE b.environment=$1 AND b.target=$2 AND b.status='planning' AND v.status='verifying' ORDER BY b.created_at DESC LIMIT 1`,
              [fixture.environment, fixture.target],
            )
          ).rows[0];
          if (!record) await delay(10);
        }
        assert(record, 'a real new verifier must claim its current scope');
        record.buildFence = Number(record.buildFence);
        record.verifierFence = Number(record.verifierFence);
        assert(
          Number.isSafeInteger(record.buildFence) &&
            record.buildFence > 0 &&
            Number.isSafeInteger(record.verifierFence) &&
            record.verifierFence > 0,
        );
        const pids: number[] = [];
        for (const pid of await readdir('/proc')) {
          if (!/^\d+$/.test(pid)) continue;
          try {
            const comm = (await readFile('/proc/' + pid + '/comm', 'utf8')).trim();
            if (comm !== 'moria-runtime') continue;
            const env = (await readFile('/proc/' + pid + '/environ', 'utf8')).split('\0');
            if (!env.includes('CAPACITY_DATABASE=' + fixture.nativeEnv.CAPACITY_DATABASE)) continue;
            process.kill(Number(pid), 'SIGSTOP');
            pids.push(Number(pid));
          } catch (error) {
            if (
              (error as NodeJS.ErrnoException).code !== 'ENOENT' &&
              (error as NodeJS.ErrnoException).code !== 'ESRCH'
            )
              throw error;
          }
        }
        assert(pids.length > 0);
        try {
          const head = (await fixture.db.catalog.heads(fixture.environment, [fixture.source]))[0];
          const definition = (
            await fixture.db.pool.query(
              'SELECT iceberg_uuid FROM moria.native_storage_namespaces WHERE environment=$1 AND resource=$2',
              [fixture.environment, fixture.source],
            )
          ).rows[0];
          const result = await promisify(execFile)(
            process.env.MORIA_CATALOG_TEST_BINARY ?? '/usr/local/bin/moria-catalog-tests',
            ['--ignored', '--exact', 'catalog_publication', '--nocapture', '--test-threads=1'],
            {
              timeout: 15000,
              maxBuffer: 1024 * 1024,
              env: {
                ...fixture.nativeEnv,
                MORIA_NATIVE_CATALOG_FIXTURE: JSON.stringify({
                  root: fixture.db.root,
                  token: fixture.token,
                  binding: record.build,
                  mode: 'header-window',
                  nativeRef: head.metadataLocation,
                  headerProbe: record,
                  snapshots: [
                    {
                      resource: fixture.source,
                      warehouseId: 'warehouse',
                      tableId: 'table',
                      revision: selected.namespace.revision,
                      metadata: 'unused',
                      snapshotId: head.snapshotId,
                      tableUuid: definition.iceberg_uuid,
                    },
                  ],
                }),
              },
            },
          );
          const line = result.stdout
            .split('\n')
            .find((value) => value.includes('"event":"qualified_header_counts"'));
          assert(line, result.stdout);
          const counts = JSON.parse(line.slice(line.indexOf('{')));
          assert(counts.rowGroups >= 2 && counts.columns >= 4);
          assert(counts.window.providerRanges * 2 < counts.baseline.providerRanges);
          assert(
            counts.actualWindowGuards.nativeStorage < counts.actualBaselineGuards.nativeStorage,
          );
          assert(counts.actualWindowGuards.verifier < counts.actualBaselineGuards.verifier);
          t.diagnostic(JSON.stringify(counts));
        } finally {
          for (const pid of pids) {
            try {
              process.kill(pid, 'SIGCONT');
            } catch (error) {
              if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error;
            }
          }
        }
        await indexed;
      },
    });
  },
);
