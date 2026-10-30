import { ObjectStore, type ObjectSchema, type StageLease } from './store.js';
import type { SessionContext } from './service.js';
import { Authority } from '../../security/src/authority.js';
import { authorize } from '../../../packages/authorization/src/access.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import { fingerprint } from '../../../packages/compiler/src/index.js';
import { icebergRows } from '../../../workers/iceberg/stream.js';
import { withLease } from './lease.js';
import { normalizeType } from '../../../packages/contracts/values.js';
import { compareColumns } from '../../../packages/contracts/compatibility.js';

/** Trusted release configuration, never request JSON. This initial adapter only
 * supports Dataset sources; restricted-view object sources need policy binding.
 * Serving must use the same target labels.
 * Release resolution and automatic marking propagation remain upstream work. */
export interface IndexBinding {
  source: { kind: 'dataset'; resource: string; markings: string[] };
  target: { resource: string; markings: string[]; schema: ObjectSchema };
}

/** Trusted binding registration and the legacy single-attempt fixture runner.
 * Distributed execution belongs to services/indexing; legacy callers own recovery.
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
    await this.store.catalog.migrate();
  }
  /** Trusted composition only; workers receive immutable bindings, never request-supplied paths. */
  async registerDistributed(
    session: SessionContext,
    configuration: {
      targetId: string;
      warehouse: string;
      partitions?: number;
      timeoutSeconds?: number;
    },
  ): Promise<void> {
    session = structuredClone(session);
    configuration = structuredClone(configuration);
    const partitions = configuration.partitions ?? 64;
    const timeoutSeconds = configuration.timeoutSeconds ?? 1800;
    if (
      session.execution ||
      !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(configuration.targetId) ||
      (/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(this.binding.target.resource) &&
        this.binding.target.resource !== configuration.targetId) ||
      !configuration.warehouse.startsWith('/') ||
      configuration.warehouse.split('/').some((part) => part === '..') ||
      !Number.isInteger(partitions) ||
      partitions < 1 ||
      partitions > 4096 ||
      (partitions & (partitions - 1)) !== 0 ||
      !Number.isInteger(timeoutSeconds) ||
      timeoutSeconds < 1 ||
      timeoutSeconds > 86400
    )
      throw new MoriaError('INDEXING_INVALID', 'Invalid distributed indexing configuration.');
    const properties = Object.fromEntries(
      Object.entries(this.binding.target.schema.properties).map(([name, type]) => [
        name,
        normalizeType(type, {}, 'column'),
      ]),
    );
    if (
      properties[this.binding.target.schema.primaryKey]?.type !== 'uid' ||
      properties[this.binding.target.schema.primaryKey].nullable
    )
      throw new MoriaError('SCHEMA_INVALID', 'Object key must be non-null UID.');
    await this.store.catalog.transaction(async (c) => {
      const state = await this.authority.current(
        session.environment,
        session.principal,
        session.application,
        c,
      );
      const { source, target } = this.binding;
      authorize(state.identity, source.resource, 'read', source.markings);
      authorize(state.identity, target.resource, 'index', target.markings);
      const sourceLabels = await this.authority.resourceAccess(c, session, source.resource);
      const labels = [
        ...new Set([
          ...target.markings,
          ...(await this.authority.resourceAccess(c, session, target.resource, 'write')),
        ]),
      ].sort();
      if ([...source.markings, ...sourceLabels].some((label) => !labels.includes(label)))
        throw new MoriaError('MARKING_DOWNGRADE', 'Index target must preserve source markings.');
      await this.authority.assertCurrent(
        c,
        session.environment,
        session.principal,
        session.application,
        state.epoch,
        session.sessionHash,
      );
      const instanceEpoch = (await c.query('SELECT security_epoch FROM moria.instance_state'))
        .rows[0].security_epoch;
      const environmentId = (
        await c.query(
          'INSERT INTO moria.indexing_environments(environment) VALUES($1) ON CONFLICT(environment) DO UPDATE SET environment=EXCLUDED.environment RETURNING id',
          [session.environment],
        )
      ).rows[0].id;
      const incarnation = (await c.query('SELECT incarnation FROM moria.indexing_instance')).rows[0]
        .incarnation;
      const previous = (
        await c.query(
          'SELECT object_schema FROM moria.source_generations WHERE environment=$1 AND object_type=$2 ORDER BY revision DESC LIMIT 1',
          [session.environment, target.resource],
        )
      ).rows[0]?.object_schema;
      if (
        previous &&
        (previous.primaryKey !== target.schema.primaryKey ||
          !compareColumns(previous.properties, target.schema.properties).compatible)
      )
        throw new MoriaError(
          'MIGRATION_REQUIRED',
          'Destructive schema changes require an explicit migration.',
        );
      const definition = {
        version: 1,
        valueVersion: 1,
        environmentId,
        incarnation,
        targetId: configuration.targetId,
        warehouse: configuration.warehouse,
        partitions,
        timeoutSeconds,
        schema: { primaryKey: target.schema.primaryKey, properties },
        markings: labels,
      };
      const revision = fingerprint({
        definition,
        context: session,
        epoch: state.epoch,
        instanceEpoch,
      });
      const registered = await c.query(
        `INSERT INTO moria.indexing_bindings(environment,target,target_id,source,revision,definition,context,security_epoch,instance_epoch)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(environment,target) DO UPDATE SET target_id=EXCLUDED.target_id,
        source=EXCLUDED.source,revision=EXCLUDED.revision,definition=EXCLUDED.definition,context=EXCLUDED.context,
        security_epoch=EXCLUDED.security_epoch,instance_epoch=EXCLUDED.instance_epoch,enabled=true WHERE moria.indexing_bindings.target_id=EXCLUDED.target_id`,
        [
          session.environment,
          target.resource,
          configuration.targetId,
          source.resource,
          revision,
          definition,
          session,
          state.epoch,
          instanceEpoch,
        ],
      );
      if (!registered.rowCount)
        throw new MoriaError(
          'BINDING_CONFLICT',
          'A target cannot change immutable object type identity.',
        );
      const head = (
        await c.query(
          'SELECT revision FROM moria.dataset_heads WHERE environment=$1 AND dataset=$2 AND metadata_location IS NOT NULL',
          [session.environment, source.resource],
        )
      ).rows[0];
      if (head)
        await c.query('SELECT moria.indexing_enqueue($1,$2,$3,$4)', [
          session.environment,
          source.resource,
          head.revision,
          (
            await c.query('SELECT revision FROM moria.environments WHERE id=$1', [
              session.environment,
            ])
          ).rows[0].revision,
        ]);
    }, session.environment);
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
      const sourceLabels = await this.authority.resourceAccess(c, session, source.resource);
      const targetLabels = new Set([
        ...target.markings,
        ...(await this.authority.resourceAccess(c, session, target.resource, 'write')),
      ]);
      if (sourceLabels.some((label) => !targetLabels.has(label)))
        throw new MoriaError(
          'MARKING_DOWNGRADE',
          'Index target must preserve current filesystem source markings',
        );
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
