import { schema } from '../../database/src/schema.js';
import { randomUUID } from 'node:crypto';
import type { Pool, PoolClient } from 'pg';
import { MoriaError } from '../../../packages/contracts/types.js';
import { fingerprint } from '../../../packages/compiler/src/index.js';

/** Trusted internal persistence API, not an authorization boundary or HTTP service. */
export class Catalog {
  constructor(
    readonly pool: Pool,
    readonly schemaMode: 'apply' | 'verify' = 'apply',
  ) {}
  async dataProvenance(
    client: PoolClient,
    environment: string,
    resource: string,
    operation = 'read',
  ) {
    try {
      const row = (
        await client.query(
          `SELECT moria.runtime_data_origins($1,$2) AS origins,
        moria.runtime_data_labels($1,$2) AS markings,moria.runtime_data_resource($1,$2) AS resource`,
          [environment, resource],
        )
      ).rows[0] as { origins: string[]; markings: string[]; resource: string };
      await client.query(
        'SELECT moria.runtime_data_operational($1,origin,$3) FROM unnest($2::text[]) origin',
        [environment, row.origins, operation],
      );
      return row;
    } catch (error) {
      if (error instanceof Error && error.message === 'NOT_FOUND')
        throw new MoriaError('NOT_FOUND', 'Published resource is unavailable');
      if (error instanceof Error && error.message === 'AMBIGUOUS_RESOURCE')
        throw new MoriaError(
          'FORBIDDEN',
          'Runtime bindings must identify ambiguous resources by UUID',
        );
      if (error instanceof Error && error.message === 'RESOURCE_LIMIT')
        throw new MoriaError(
          'RESOURCE_LIMIT',
          'Resource provenance exceeds the admitted control budget',
        );
      throw error;
    }
  }
  async datasetClassification(
    client: PoolClient,
    sessionHash: string,
    resource: string,
  ): Promise<string[]> {
    return (
      await client.query('SELECT moria.runtime_dataset_labels($1,$2) AS markings', [
        sessionHash,
        resource,
      ])
    ).rows[0].markings;
  }
  /** Admission changes policy before the caller captures its immutable work authority. */
  async admitDataProvenance(
    client: PoolClient,
    sessionHash: string,
    targets: string[],
    sources: string[],
    operation: 'write' | 'index',
  ) {
    return (
      await client.query('SELECT moria.runtime_data_admit($1,$2,$3,$4)::text AS epoch', [
        sessionHash,
        targets,
        sources,
        operation,
      ])
    ).rows[0].epoch as string;
  }
  async migrate(): Promise<void> {
    await schema(this.pool, this.schemaMode);
  }
  /** repeatable-read is reserved for protected snapshot reads with append-only
   * cursor metadata. Data/security mutations retain the serializable default. */
  async transaction<T>(
    fn: (client: PoolClient) => Promise<T>,
    environment?: string,
    isolation: 'serializable' | 'repeatable-read' = 'serializable',
    lockMode: 'exclusive' | 'shared' = 'exclusive',
    filesystemLock: 'exclusive' | 'shared' = 'shared',
  ): Promise<T> {
    const lockKeys = [
      ...(environment === undefined
        ? []
        : [{ key: `moria/environment/${environment}`, mode: lockMode }]),
      { key: 'moria/filesystem', mode: filesystemLock },
    ];
    for (let attempt = 0; attempt < 3; attempt++) {
      const client = await this.pool.connect();
      const locked: { key: string; mode: 'shared' | 'exclusive' }[] = [];
      let began = false,
        timeout: string | undefined;
      try {
        // Queue cooperating environment writers BEFORE taking the serializable
        // snapshot. Row-lock waiting inside SERIALIZABLE otherwise repeatedly
        // wakes with an obsolete snapshot under concurrent load.
        if (lockKeys.length) {
          timeout = (await client.query('SHOW lock_timeout')).rows[0].lock_timeout;
          await client.query("SET lock_timeout='30s'");
          // Shared readers also queue before taking a snapshot. Otherwise an
          // intervening writer can invalidate their later FOR SHARE row lock.
          for (const lockKey of lockKeys) {
            await client.query(
              lockKey.mode === 'shared'
                ? 'SELECT pg_advisory_lock_shared(hashtextextended($1,0))'
                : 'SELECT pg_advisory_lock(hashtextextended($1,0))',
              [lockKey.key],
            );
            locked.push(lockKey);
          }
          await client.query("SELECT set_config('lock_timeout',$1,false)", [timeout]);
          timeout = undefined;
        }
        await client.query(
          isolation === 'repeatable-read'
            ? 'BEGIN ISOLATION LEVEL REPEATABLE READ'
            : 'BEGIN ISOLATION LEVEL SERIALIZABLE',
        );
        began = true;
        const result = await fn(client);
        await client.query('COMMIT');
        began = false;
        return result;
      } catch (error) {
        if (began) await client.query('ROLLBACK');
        if (!['40001', '40P01'].includes((error as any).code)) throw error;
        if (attempt === 2)
          throw new MoriaError(
            'CONCURRENT_MODIFICATION',
            'Transaction contention retry limit exceeded',
          );
      } finally {
        let cleanupError: Error | undefined;
        try {
          for (const lockKey of locked.reverse())
            await client.query(
              lockKey.mode === 'shared'
                ? 'SELECT pg_advisory_unlock_shared(hashtextextended($1,0))'
                : 'SELECT pg_advisory_unlock(hashtextextended($1,0))',
              [lockKey.key],
            );
          if (timeout !== undefined)
            await client.query("SELECT set_config('lock_timeout',$1,false)", [timeout]);
        } catch (error) {
          cleanupError = error as Error;
        }
        client.release(cleanupError);
        if (cleanupError) throw cleanupError;
      }
    }
    throw new MoriaError('CONCURRENT_MODIFICATION', 'Serialization retry exhausted');
  }
  async ensureEnvironment(id: string): Promise<void> {
    await this.pool.query('INSERT INTO moria.environments(id) VALUES($1) ON CONFLICT DO NOTHING', [
      id,
    ]);
  }
  async ensureDataset(environment: string, dataset: string): Promise<void> {
    await this.pool.query(
      'INSERT INTO moria.dataset_heads(environment,dataset) VALUES($1,$2) ON CONFLICT DO NOTHING',
      [environment, dataset],
    );
  }
  async resolveIdentity(domain: string, canonicalKey: string): Promise<string> {
    // Managed domains require their native owner/fence; legacy fixtures cannot bypass a transfer.
    try {
      return (
        await this.pool.query('SELECT moria.runtime_identity_resolve_legacy($1,$2) AS uid', [
          domain,
          canonicalKey,
        ])
      ).rows[0].uid;
    } catch (error) {
      const code = (error as Error).message;
      if (['OWNERSHIP_REQUIRED', 'IDENTITY_ALLOCATION_FAILED'].includes(code))
        throw new MoriaError(code, 'Identity allocation requires its current domain owner.');
      throw error;
    }
  }
  /** One MVCC statement pins a coherent head map across an atomic publication. */
  async heads(
    environment: string,
    datasets: string[],
  ): Promise<
    {
      dataset: string;
      revision: string;
      metadataLocation: string | null;
      snapshotId: string | null;
    }[]
  > {
    const result = await this.pool.query(
      `SELECT dataset,revision,metadata_location AS "metadataLocation",snapshot_id AS "snapshotId"
      FROM moria.dataset_heads WHERE environment=$1 AND dataset=ANY($2::text[]) ORDER BY dataset`,
      [environment, datasets],
    );
    if (result.rowCount !== new Set(datasets).size)
      throw new MoriaError('DATASET_UNKNOWN', 'Requested dataset does not exist');
    return result.rows;
  }
  async claim(
    environment: string,
    execution: string,
    worker: string,
    seconds = 60,
  ): Promise<string> {
    if (!Number.isInteger(seconds) || seconds < 1 || seconds > 3600)
      throw new MoriaError('LEASE_INVALID', 'Lease duration outside allowed range');
    const result = await this.pool.query(
      `INSERT INTO moria.leases(environment,execution,worker,fence,expires_at)
      VALUES($1,$2,$3,1,clock_timestamp()+$4*interval '1 second')
      ON CONFLICT(environment,execution) DO UPDATE SET worker=EXCLUDED.worker,fence=moria.leases.fence+1,expires_at=EXCLUDED.expires_at
      WHERE moria.leases.expires_at<=clock_timestamp() RETURNING fence`,
      [environment, execution, worker, seconds],
    );
    if (!result.rowCount) throw new MoriaError('LEASE_BUSY', 'Execution already has a live worker');
    return result.rows[0].fence;
  }
  /** Renewal cannot resurrect expiry or change the worker's fencing token. */
  async renew(
    environment: string,
    execution: string,
    worker: string,
    fence: string,
    seconds = 60,
  ): Promise<void> {
    if (!Number.isInteger(seconds) || seconds < 1 || seconds > 3600)
      throw new MoriaError('LEASE_INVALID', 'Lease duration outside allowed range');
    const result = await this.pool.query(
      `UPDATE moria.leases SET expires_at=GREATEST(expires_at,clock_timestamp()+$5*interval '1 second')
      WHERE environment=$1 AND execution=$2 AND worker=$3 AND fence=$4 AND expires_at>clock_timestamp()`,
      [environment, execution, worker, fence, seconds],
    );
    if (!result.rowCount)
      throw new MoriaError('LEASE_LOST', 'Worker lease expired or was replaced');
  }
  async revoke(environment: string): Promise<string> {
    const r = await this.pool.query(
      'UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id=$1 RETURNING security_epoch',
      [environment],
    );
    if (!r.rowCount) throw new MoriaError('ENVIRONMENT_UNKNOWN', 'Unknown environment');
    return r.rows[0].security_epoch;
  }
  async publish(
    request: Publication,
    checkCurrent?: (client: PoolClient) => Promise<void>,
  ): Promise<Receipt> {
    if (
      !request.outputs.length ||
      new Set(request.outputs.map((o) => o.dataset)).size !== request.outputs.length
    )
      throw new MoriaError('OUTPUT_INVALID', 'Outputs must be nonempty and unique');
    // Input hash excludes worker lease state so acknowledgment retries retain logical identity.
    const hash = fingerprint({
      outputs: request.outputs,
      lineage: request.lineage,
      checkpoint: request.checkpoint ?? null,
    });
    return this.transaction(async (c) => {
      const env = await c.query('SELECT * FROM moria.environments WHERE id=$1 FOR UPDATE', [
        request.environment,
      ]);
      if (!env.rowCount) throw new MoriaError('ENVIRONMENT_UNKNOWN', 'Unknown environment');
      if (env.rows[0].security_epoch !== request.securityEpoch)
        throw new MoriaError('AUTHORIZATION_CHANGED', 'Current authorization must be rechecked');
      await checkCurrent?.(c);
      const old = await c.query(
        'SELECT * FROM moria.publications WHERE environment=$1 AND execution=$2',
        [request.environment, request.execution],
      );
      if (old.rowCount) {
        if (old.rows[0].request_hash !== hash)
          throw new MoriaError(
            'PUBLICATION_CONFLICT',
            'Execution already committed different outputs',
          );
        return old.rows[0].receipt;
      }
      const lease = await c.query(
        `SELECT 1 FROM moria.leases WHERE environment=$1 AND execution=$2 AND worker=$3 AND fence=$4 AND expires_at>clock_timestamp() FOR UPDATE`,
        [request.environment, request.execution, request.worker, request.fence],
      );
      if (!lease.rowCount)
        throw new MoriaError('LEASE_LOST', 'Publisher is stale or its lease expired');
      const outputs: Receipt['outputs'] = [];
      for (const output of [...request.outputs].sort((a, b) =>
        a.dataset.localeCompare(b.dataset),
      )) {
        const updated = await c.query(
          `UPDATE moria.dataset_heads SET revision=revision+1,metadata_location=$4,snapshot_id=$5
          WHERE environment=$1 AND dataset=$2 AND revision=$3 RETURNING revision`,
          [
            request.environment,
            output.dataset,
            output.expectedRevision,
            output.metadataLocation,
            output.snapshotId,
          ],
        );
        if (!updated.rowCount)
          throw new MoriaError('HEAD_CONFLICT', 'An output head changed or is missing');
        const revision = updated.rows[0].revision;
        await c.query('INSERT INTO moria.snapshot_history VALUES($1,$2,$3,$4,$5,$6,$7)', [
          request.environment,
          output.dataset,
          revision,
          output.metadataLocation,
          output.snapshotId,
          request.execution,
          JSON.stringify(request.lineage),
        ]);
        outputs.push({ dataset: output.dataset, revision, snapshotId: output.snapshotId });
      }
      if (request.checkpoint) {
        const p = request.checkpoint;
        await c.query(
          `INSERT INTO moria.checkpoints VALUES($1,$2,$3,$4,0,'null'::jsonb) ON CONFLICT DO NOTHING`,
          [request.environment, p.connection, p.stream, p.configRevision],
        );
        const updated = await c.query(
          `UPDATE moria.checkpoints SET version=version+1,value=$6
          WHERE environment=$1 AND connection=$2 AND stream=$3 AND config_revision=$4 AND version=$5 RETURNING version`,
          [
            request.environment,
            p.connection,
            p.stream,
            p.configRevision,
            p.expectedVersion,
            JSON.stringify(p.value),
          ],
        );
        if (!updated.rowCount) throw new MoriaError('CHECKPOINT_CONFLICT', 'Checkpoint changed');
      }
      const revision = (
        await c.query(
          'UPDATE moria.environments SET revision=revision+1 WHERE id=$1 RETURNING revision',
          [request.environment],
        )
      ).rows[0].revision;
      const receipt = { revision, outputs };
      for (const output of outputs)
        await c.query('SELECT moria.indexing_enqueue($1,$2,$3,$4)', [
          request.environment,
          output.dataset,
          output.revision,
          revision,
        ]);
      await c.query('INSERT INTO moria.publications VALUES($1,$2,$3,$4)', [
        request.environment,
        request.execution,
        hash,
        JSON.stringify(receipt),
      ]);
      return receipt;
    }, request.environment);
  }
  /** Trusted DB-only callback. Never invoke user code/network effects in this transaction. */
  async action<T>(
    request: ActionRequest,
    apply: (client: PoolClient) => Promise<T>,
    checkCurrent?: (client: PoolClient) => Promise<void>,
    transaction?: PoolClient,
  ): Promise<T> {
    const execute = async (c: PoolClient) => {
      const env = await c.query(
        'SELECT security_epoch FROM moria.environments WHERE id=$1 FOR UPDATE',
        [request.environment],
      );
      if (!env.rowCount || env.rows[0].security_epoch !== request.securityEpoch)
        throw new MoriaError('AUTHORIZATION_CHANGED', 'Current authorization must be rechecked');
      await checkCurrent?.(c);
      const scope = [
        request.environment,
        request.application,
        request.principal,
        request.action,
        request.key,
      ];
      const existing = await c.query(
        'SELECT *,expires_at<=clock_timestamp() AS expired FROM moria.action_keys WHERE environment=$1 AND application=$2 AND principal=$3 AND action=$4 AND key=$5',
        scope,
      );
      const hash = fingerprint(request.input);
      if (existing.rowCount) {
        const old = existing.rows[0];
        if (
          old.input_hash !== hash ||
          (request.explicitRevision && old.action_revision !== request.actionRevision)
        )
          throw new MoriaError(
            'IDEMPOTENCY_CONFLICT',
            'Idempotency key was used for another request',
          );
        if (old.expired || old.result === null)
          throw new MoriaError('RESULT_EXPIRED', 'Stored result expired; key cannot be reused');
        return old.result as T;
      }
      const result = await apply(c);
      if (result === undefined || result === null)
        throw new MoriaError('ACTION_RESULT', 'Action result must be a non-null JSON value');
      await c.query(
        `INSERT INTO moria.action_keys(environment,application,principal,action,key,input_hash,action_revision,result,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,clock_timestamp()+interval '30 days')`,
        [...scope, hash, request.actionRevision, JSON.stringify(result)],
      );
      return result;
    };
    return transaction ? execute(transaction) : this.transaction(execute, request.environment);
  }
}
export interface Publication {
  environment: string;
  execution: string;
  worker: string;
  fence: string;
  securityEpoch: string;
  outputs: {
    dataset: string;
    expectedRevision: string;
    metadataLocation: string;
    snapshotId: string;
  }[];
  lineage: Record<string, string>;
  checkpoint?: {
    connection: string;
    stream: string;
    configRevision: string;
    expectedVersion: string;
    value: unknown;
  };
}
export interface Receipt {
  revision: string;
  outputs: { dataset: string; revision: string; snapshotId: string }[];
}
export interface ActionRequest {
  environment: string;
  application: string;
  principal: string;
  action: string;
  key: string;
  actionRevision: string;
  explicitRevision?: boolean;
  input: unknown;
  securityEpoch: string;
}
