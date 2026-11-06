import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { Authority } from '../services/security/src/authority.js';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';
import { bootstrapGraph } from './fixtures/native-retention/graph.js';

test(
  'a warmed native key-run reader rejects revoked current authority',
  { timeout: 120000 },
  async (t) => {
    const fixture = await managedRetentionDataset({ primaryKey: ['id'] });
    t.after(fixture.close);
    const rows = Array.from({ length: 8 }, (_, i) => ({
      id: randomUUID(),
      name: 'guard-' + i,
      tags: ['cached-metadata'],
    }));
    const first = await fixture.publish(rows);
    await bootstrapGraph(fixture, first.head, rows);
    const binding = await fixture.bind(
      [
        {
          resource: fixture.dataset,
          revision: first.head.revision,
          snapshot: first.head.snapshotId,
        },
      ],
      {},
    );
    const child = spawn(
      fixture.harness,
      ['--ignored', '--exact', 'catalog_publication', '--nocapture', '--test-threads=1'],
      {
        env: {
          ...fixture.nativeEnv,
          MORIA_NATIVE_CATALOG_FIXTURE: JSON.stringify({
            root: fixture.db.root,
            token: fixture.token,
            binding,
            mode: 'graph-read-held',
            expectedRows: rows.length,
            writerRows: { [fixture.dataset]: rows },
            snapshots: [{ ...fixture.snapshot, snapshotId: first.head.snapshotId }],
            s3Bindings: [fixture.datasetStorage],
          }),
        },
        stdio: ['pipe', 'pipe', 'pipe'],
      },
    );
    let output = '',
      error = '';
    child.stdout.on('data', (chunk) => {
      output += chunk;
    });
    child.stderr.on('data', (chunk) => {
      error += chunk;
    });
    const exited = new Promise<number | null>((resolve, reject) => {
      child.once('error', reject);
      child.once('close', resolve);
    });
    t.after(async () => {
      child.kill('SIGKILL');
      await exited.catch(() => undefined);
    });
    let timer: NodeJS.Timeout | undefined;
    try {
      await new Promise<void>((resolve, reject) => {
        child.stdout.on('data', () => {
          if (output.includes('"event":"graph_cached_ready"')) resolve();
        });
        child.once('error', reject);
        child.once('close', () =>
          reject(new Error('Native graph reader exited before cache barrier: ' + error)),
        );
        timer = setTimeout(
          () => reject(new Error('Native graph cache barrier deadline: ' + error)),
          30000,
        );
      });
    } finally {
      clearTimeout(timer);
    }
    await new Authority(fixture.db.catalog).setGrants(
      fixture.environment,
      'fixture',
      fixture.principal,
      fixture.application,
      [],
    );
    child.stdin.end('continue\n');
    assert.equal(await exited, 101);
    assert.match(error, /AUTHORIZATION_CHANGED|FORBIDDEN/);
    assert.doesNotMatch(output, /"event":"graph_bootstrap_read"/);
    t.diagnostic(
      JSON.stringify({ readerWarmed: true, revocationObserved: true, noResultReleased: true }),
    );
  },
);
