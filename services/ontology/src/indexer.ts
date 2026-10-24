import { ObjectStore, type ObjectSchema, type StageLease } from './store.js';
import type { SessionContext } from './service.js';
import { Authority } from '../../security/src/authority.js';
import { authorize } from '../../../packages/authorization/src/access.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import { fingerprint } from '../../../packages/compiler/src/index.js';
import { icebergRows } from '../../../workers/iceberg/stream.js';
import { withLease } from './lease.js';

/** Trusted release configuration, never request JSON. This initial adapter only
 * supports Dataset sources; restricted-view object sources need policy binding.
 * Serving must use the same target labels.
 * Release resolution and automatic marking propagation remain upstream work. */
export interface IndexBinding {
  source: { kind: 'dataset'; resource: string; markings: string[] };
  target: { resource: string; markings: string[]; schema: ObjectSchema };
}

/** Internal single-attempt coordinator. Callers supervise/renew the worker lease.
 * A crash can leave an unpublished stage; durable scheduling/recovery is pending.
 * No source path or snapshot supplied by the caller is accepted. */
export class IcebergIndexer {
  private readonly binding: IndexBinding;
  constructor(
    readonly store: ObjectStore,
    readonly authority: Authority,
    binding: IndexBinding,
  ) {
    this.binding = structuredClone(binding);
    if (binding.source.kind !== 'dataset')
      throw new MoriaError('SOURCE_INVALID', 'Index input must be a Dataset');
    if (binding.source.markings.some((label) => !binding.target.markings.includes(label)))
      throw new MoriaError('MARKING_DOWNGRADE', 'Target must preserve source markings');
  }
  async migrate(): Promise<void> {
    await this.store.catalog.pool.query(`CREATE TABLE IF NOT EXISTS moria.index_provenance(
      stage uuid PRIMARY KEY REFERENCES moria.source_stages(id),environment text NOT NULL,
      principal text NOT NULL,application text NOT NULL,contract text NOT NULL,
      source_dataset text NOT NULL,source_revision bigint NOT NULL,metadata_location text NOT NULL,snapshot_id text NOT NULL,
      FOREIGN KEY(environment,source_dataset,source_revision) REFERENCES moria.snapshot_history(environment,dataset,revision))`);
  }
  async runSupervised(
    session: SessionContext,
    lease: StageLease,
    options: { signal?: AbortSignal; seconds?: number; timeoutMs?: number } = {},
  ): Promise<{ stage: string; revision: string }> {
    session = structuredClone(session);
    lease = structuredClone(lease);
    return withLease(
      this.store.catalog,
      session.environment,
      lease,
      (signal) => this.run(session, lease, signal),
      options,
    );
  }
  async run(
    session: SessionContext,
    lease: StageLease,
    signal?: AbortSignal,
  ): Promise<{ stage: string; revision: string }> {
    session = structuredClone(session);
    lease = structuredClone(lease);
    const checkCancelled = () => {
      if (signal?.aborted) throw new MoriaError('CANCELLED', 'Indexing cancelled');
    };
    checkCancelled();
    const { source, target } = this.binding;
    const state = await this.authority.current(
      session.environment,
      session.principal,
      session.application,
    );
    authorize(state.identity, source.resource, 'read', source.markings);
    authorize(state.identity, target.resource, 'index', target.markings);
    const check = async (c: Parameters<Authority['assertCurrent']>[0]) => {
      checkCancelled();
      await this.authority.assertCurrent(
        c,
        session.environment,
        session.principal,
        session.application,
        state.epoch,
        session.sessionHash,
      );
      checkCancelled();
    };
    const pin = await this.store.catalog.transaction(
      async (c) => {
        await check(c);
        const result = await c.query(
          `SELECT e.revision,h.revision AS source_revision,h.metadata_location,h.snapshot_id
        FROM moria.environments e JOIN moria.dataset_heads h ON h.environment=e.id
        JOIN moria.snapshot_history s ON s.environment=h.environment AND s.dataset=h.dataset AND s.revision=h.revision
        AND s.metadata_location=h.metadata_location AND s.snapshot_id=h.snapshot_id
        WHERE e.id=$1 AND h.dataset=$2`,
          [session.environment, source.resource],
        );
        if (!result.rowCount)
          throw new MoriaError('SOURCE_UNPUBLISHED', 'Source has no published snapshot');
        return result.rows[0];
      },
      session.environment,
      'repeatable-read',
      'shared',
    );
    const stage = await this.store.stage(
      session.environment,
      target.resource,
      target.schema,
      icebergRows(
        { metadataLocation: pin.metadata_location, snapshotId: pin.snapshot_id },
        { signal },
      ),
      state.epoch,
      pin.revision,
      lease,
      target.markings,
    );
    await this.store.catalog.pool.query(
      `INSERT INTO moria.index_provenance VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [
        stage,
        session.environment,
        session.principal,
        session.application,
        fingerprint(JSON.parse(JSON.stringify(this.binding))),
        source.resource,
        pin.source_revision,
        pin.metadata_location,
        pin.snapshot_id,
      ],
    );
    const revision = await this.store.publishStage(stage, check);
    return { stage, revision };
  }
}
