import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { promisify } from 'node:util';
import pg from 'pg';
import { Catalog } from '../../../services/catalog/src/catalog.js';
import { Authority } from '../../../services/security/src/authority.js';
import { WorkloadSessions } from '../../../services/security/src/sessions.js';
import type { managedRetentionDataset } from './dataset.js';

type Fixture = Awaited<ReturnType<typeof managedRetentionDataset>>;

export async function retentionRecovery(fixture: Fixture) {
  const { db, namespace, backupNamespace, storageBindings } = fixture;
  const local = join(db.root, 'retention-recovery-local');
  const staging = join(db.root, 'retention-recovery-staging');
  await mkdir(join(local, 'auth'), { recursive: true, mode: 0o700 });
  await mkdir(staging, { recursive: true, mode: 0o700 });
  const key = join(local, 'auth/session-key');
  await writeFile(key, randomBytes(32), { mode: 0o600 });
  const env = {
    ...fixture.operatorEnv,
    MORIA_LOCAL_DIR: local,
    MORIA_RECOVERY_KEYS: JSON.stringify({ 'session-encryption': key }),
    MORIA_ISOLATED_RESTORE: '1',
    MORIA_RECOVERY_PG_DUMP: '/usr/local/bin/pg_dump',
    MORIA_RECOVERY_PG_RESTORE: '/usr/local/bin/pg_restore',
    MORIA_NATIVE_RECOVERY_NAMESPACE: JSON.stringify(backupNamespace),
    MORIA_NATIVE_RECOVERY_LIMITS: JSON.stringify({
      roots: 5000,
      members: 50000,
      bytes: 1024 * 1024 * 1024,
      spillBytes: 32 * 1024 * 1024,
      dumpBytes: 512 * 1024 * 1024,
      timeSeconds: 120,
    }),
    MORIA_RECOVERY_STAGING_ROOT: staging,
  };
  const capture = async (requestKey: string) =>
    fixture.command(
      ['recovery', 'capture'],
      {
        requestKey,
        owner: randomUUID(),
        retentionSeconds: 86400,
      },
      env,
    );
  const restore = async (manifest: unknown) => {
    const databaseName = 'retention_restore_' + randomUUID().replaceAll('-', '');
    await db.pool.query(`CREATE DATABASE ${databaseName}`);
    const operatorUrl = new URL(fixture.operatorEnv.RETENTION_DATABASE!);
    operatorUrl.pathname = '/' + databaseName;
    const pool = new pg.Pool({ connectionString: operatorUrl.href });
    try {
      const targetLocal = join(db.root, databaseName + '-local');
      const targetFilesystem = join(db.root, databaseName + '-filesystem');
      await mkdir(join(targetLocal, 'auth'), { recursive: true, mode: 0o700 });
      await mkdir(targetFilesystem, { recursive: true, mode: 0o700 });
      const restoredStorage = { ...fixture.datasetStorage, prefix: databaseName };
      const targetBindings = storageBindings.map((binding) => {
        if (
          binding.namespace.warehouseId === namespace.warehouseId &&
          binding.namespace.tableId === namespace.tableId
        )
          return { namespace, provider: { kind: 's3', binding: restoredStorage } };
        if (binding.namespace.warehouseId === 'filesystem')
          return {
            ...binding,
            provider: {
              kind: 'posix',
              root: targetFilesystem,
              limits: { maxObjectBytes: 128 * 1024 * 1024, chunkBytes: 1024 * 1024 },
            },
          };
        return binding;
      });
      const targetEnv = {
        ...env,
        RETENTION_DATABASE: operatorUrl.href,
        MORIA_LOCAL_DIR: targetLocal,
        MORIA_RECOVERY_KEYS: JSON.stringify({
          'session-encryption': join(targetLocal, 'auth/session-key'),
        }),
        MORIA_RECOVERY_RESTORE_KEYS: JSON.stringify({
          'session-encryption': join(targetLocal, 'auth/session-key'),
        }),
        MORIA_NATIVE_STORAGE_BINDINGS: JSON.stringify(targetBindings),
      };
      const restored = await fixture.command(
        ['restore-from'],
        {
          restore: randomUUID(),
          manifest,
          targetNamespaces: [
            namespace,
            { warehouseId: 'filesystem', tableId: 'content', revision: 'v1' },
          ],
        },
        targetEnv,
      );
      assert.equal(restored.dispatchEnabled, true);
      const runtimeUrl = new URL(fixture.nativeEnv.RETENTION_DATABASE!);
      runtimeUrl.pathname = '/' + databaseName;
      const readEnv = { ...targetEnv, RETENTION_DATABASE: runtimeUrl.href };
      const catalog = new Catalog(pool, 'verify');
      const authority = new Authority(catalog);
      const restoredOperations = [`${fixture.dataset}:read`, `${fixture.dataset}:write`, 'filesystem:read'];
      await assert.rejects(new WorkloadSessions(authority).authenticate('Bearer ' + fixture.token));
      assert.equal((await pool.query('SELECT count(*) FROM moria.runtime_workers WHERE enabled')).rows[0].count, '0');
      await authority.syncPrincipal(fixture.environment, 'recovery-operator', fixture.principal, {
        kind: 'workload', enabled: true, attributes: {}, markings: ['private'],
      });
      await authority.setApplication(fixture.environment, 'recovery-operator', fixture.application, restoredOperations);
      await authority.setGrants(fixture.environment, 'recovery-operator', fixture.principal, fixture.application, restoredOperations);
      assert.equal((await pool.query(
        'UPDATE moria.instance_principals p SET enabled=true FROM moria.instance_principal_bindings b WHERE b.environment=$1 AND b.principal=$2 AND b.instance_id=p.id',
        [fixture.environment, fixture.principal],
      )).rowCount, 1);

      const token = (
        await new WorkloadSessions(authority).mint(
          {
            environment: fixture.environment,
            principal: fixture.principal,
            application: fixture.application,
          },
          'retention-restore',
        )
      ).token;
      const read = async (
        head: { revision: string; snapshotId: string | null; metadataLocation: string | null },
        rows: Record<string, unknown>[],
      ) => {
        const state = await authority.current(
          fixture.environment,
          fixture.principal,
          fixture.application,
        );
        const spec = {
          version: 1,
          domain: 'sync',
          release: 'a'.repeat(64),
          resourceClass: 'standard',
          timeoutSeconds: 120,
          retryPolicy: 'idempotent',
          inputSchema: { type: 'object', properties: {} },
          pinnedInputs: [
            { resource: fixture.dataset, revision: head.revision, snapshot: head.snapshotId },
          ],
          expectedHeads: {},
        };
        const binding = (
          await pool.query(
            "INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation) SELECT $1,$2,$3,$4,$5,ARRAY['private'],$6,s.security_epoch,i.incarnation FROM moria.instance_state s,moria.indexing_instance i RETURNING id",
            [
              fixture.environment,
              fixture.dataset,
              spec,
              {
                environment: fixture.environment,
                principal: fixture.principal,
                application: fixture.application,
                sessionHash: createHash('sha256').update(token).digest('hex'),
              },
              [`${fixture.dataset}:read`, 'filesystem:read'],
              state.epoch,
            ],
          )
        ).rows[0].id;
        const output = await promisify(execFile)(
          fixture.harness,
          ['--ignored', '--exact', 'catalog_publication', '--nocapture', '--test-threads=1'],
          {
            env: {
              ...readEnv,
              MORIA_NATIVE_CATALOG_FIXTURE: JSON.stringify({
                root: db.root,
                token,
                binding,
                mode: 'read',
                expectedRows: rows.length,
                writerRows: { [fixture.dataset]: rows },
                nativeRef: head.metadataLocation,
                snapshots: [
                  {
                    resource: fixture.dataset,
                    warehouseId: namespace.warehouseId,
                    tableId: namespace.tableId,
                    revision: namespace.revision,
                    tableUuid: fixture.icebergUuid,
                    metadata: 'unused',
                    snapshotId: head.snapshotId,
                  },
                ],
                s3Bindings: [restoredStorage],
              }),
            },
            timeout: 120000,
            maxBuffer: 1024 * 1024,
          },
        );
        assert.match(output.stdout, /1 passed; 0 failed; 0 ignored;/);
        const execution = (
          await pool.query('SELECT id FROM moria.runtime_executions WHERE binding=$1', [binding])
        ).rows[0].id;
        await fixture.command(['execution', 'cancel'], { id: execution }, readEnv);
      };
      return {
        pool,
        catalog,
        restored,
        env: targetEnv,
        readEnv,
        restoredStorage,
        read,
        close: () => pool.end(),
      };
    } catch (error) {
      await pool.end();
      throw error;
    }
  };
  return { capture, restore };
}
