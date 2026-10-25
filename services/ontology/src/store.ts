import type { PoolClient, QueryResult } from 'pg';
import { randomUUID } from 'node:crypto';
import { Catalog, type ActionRequest } from '../../catalog/src/catalog.js';
import { MoriaError, type Value, type ValueType } from '../../../packages/contracts/types.js';
import { validateRow, validateValue } from '../../../packages/contracts/values.js';
import { CursorStore } from './cursors.js';
import { fingerprint } from '../../../packages/compiler/src/index.js';

export interface ObjectSchema {
  primaryKey: string;
  properties: Record<string, ValueType>;
}
export interface StageLease {
  execution: string;
  worker: string;
  fence: string;
}
export type Edit =
  | { op: 'set'; property: string; value: Value }
  | { op: 'clearOverride'; property: string }
  | { op: 'delete' }
  | { op: 'restore' };
export interface StoredObject {
  source: Record<string, Value>;
  effective: Record<string, Value>;
  revision: string;
}

/** Internal version store. Callers must enforce current authorization before use.
 * Source generations and overlays are separate so disappearance cannot erase edits.
 */
export class ObjectStore {
  constructor(readonly catalog: Catalog) {}
  async migrate(): Promise<void> {
    await this.catalog.migrate();
  }
  private async lock(c: PoolClient, environment: string, epoch: string): Promise<string> {
    const result = await c.query(
      'SELECT revision,security_epoch FROM moria.environments WHERE id=$1 FOR UPDATE',
      [environment],
    );
    if (!result.rowCount || result.rows[0].security_epoch !== epoch)
      throw new MoriaError('AUTHORIZATION_CHANGED', 'Current authorization must be rechecked');
    return result.rows[0].revision;
  }
  private async advance(c: PoolClient, environment: string): Promise<string> {
    return (
      await c.query(
        'UPDATE moria.environments SET revision=revision+1 WHERE id=$1 RETURNING revision',
        [environment],
      )
    ).rows[0].revision;
  }
  /** Trusted ingestion only. Reserves UID homes permanently, even on failed builds.
   * Batches commit outside the environment lock; only published pointers are read.
   * Failed/abandoned stages are retained for a future pin-aware garbage collector.
   */
  async stage(
    environment: string,
    objectType: string,
    schema: ObjectSchema,
    input: Iterable<unknown> | AsyncIterable<unknown>,
    epoch: string,
    expectedRevision: string,
    lease?: StageLease,
    requiredMarkings?: string[],
  ): Promise<string> {
    schema = structuredClone(schema);
    if (requiredMarkings !== undefined) {
      if (
        !Array.isArray(requiredMarkings) ||
        requiredMarkings.some((label) => typeof label !== 'string' || !label)
      )
        throw new MoriaError('MARKING_INVALID', 'Invalid required markings');
      requiredMarkings = [...new Set(requiredMarkings)].sort();
    }
    if (lease) {
      lease = structuredClone(lease);
      if (
        typeof lease.execution !== 'string' ||
        !lease.execution ||
        typeof lease.worker !== 'string' ||
        !lease.worker ||
        typeof lease.fence !== 'string' ||
        !/^\d+$/.test(lease.fence)
      )
        throw new MoriaError('LEASE_INVALID', 'Invalid stage lease');
      const live = await this.catalog.pool.query(
        'SELECT 1 FROM moria.leases WHERE environment=$1 AND execution=$2 AND worker=$3 AND fence=$4 AND expires_at>clock_timestamp()',
        [environment, lease.execution, lease.worker, lease.fence],
      );
      if (!live.rowCount)
        throw new MoriaError('LEASE_LOST', 'Staging worker lease expired or was replaced');
    }
    if (
      schema.properties[schema.primaryKey]?.type !== 'uid' ||
      schema.properties[schema.primaryKey].nullable
    )
      throw new MoriaError('SCHEMA_INVALID', 'Object key must be non-null UID');
    const env = (
      await this.catalog.pool.query(
        'SELECT revision,security_epoch FROM moria.environments WHERE id=$1',
        [environment],
      )
    ).rows[0];
    if (!env || env.security_epoch !== epoch)
      throw new MoriaError('AUTHORIZATION_CHANGED', 'Current authorization must be rechecked');
    if (env.revision !== expectedRevision)
      throw new MoriaError('HEAD_CONFLICT', 'Environment advanced before staging');
    const stage = randomUUID();
    const base = (
      await this.catalog.pool.query(
        'SELECT revision FROM moria.source_generations WHERE environment=$1 AND object_type=$2 AND revision<=$3 ORDER BY revision DESC LIMIT 1',
        [environment, objectType, expectedRevision],
      )
    ).rows[0];
    await this.catalog.pool.query(
      "INSERT INTO moria.source_stages(id,environment,object_type,expected_revision,security_epoch,status,object_schema,base_source_revision,lease_binding,required_markings) VALUES($1,$2,$3,$4,$5,'loading',$6,$7,$8,$9)",
      [
        stage,
        environment,
        objectType,
        expectedRevision,
        epoch,
        JSON.stringify(schema),
        base?.revision ?? null,
        lease ? JSON.stringify(lease) : null,
        requiredMarkings ?? null,
      ],
    );
    try {
      let batch: Record<string, Value>[] = [];
      const flush = async () => {
        if (!batch.length) return;
        const ids = batch.map((row) => row[schema.primaryKey] as string);
        if (new Set(ids).size !== ids.length)
          throw new MoriaError('DUPLICATE_UID', 'Canonical rows must contain unique UIDs');
        await this.catalog.transaction(async (c) => {
          const registered = await c.query(
            'SELECT count(*) FROM moria.identities WHERE uid=ANY($1::uuid[])',
            [ids],
          );
          if (Number(registered.rows[0].count) !== ids.length)
            throw new MoriaError('UID_UNREGISTERED', 'Source UID must be registered');
          await c.query(
            'INSERT INTO moria.object_homes SELECT uid,$2 FROM unnest($1::uuid[]) uid ON CONFLICT(uid) DO NOTHING',
            [ids, objectType],
          );
          const homes = await c.query(
            'SELECT object_type FROM moria.object_homes WHERE uid=ANY($1::uuid[])',
            [ids],
          );
          if (homes.rows.some((home) => home.object_type !== objectType))
            throw new MoriaError('UID_HOME_CONFLICT', 'UID belongs to another object type');
          await c.query(
            'INSERT INTO moria.staged_source_rows SELECT $1,uid,value FROM unnest($2::uuid[],$3::jsonb[]) AS input(uid,value)',
            [stage, ids, batch.map((row) => JSON.stringify(row))],
          );
        });
        batch = [];
      };
      for await (const row of input) {
        batch.push(validateRow(row, schema.properties));
        if (batch.length === 1000) await flush();
      }
      await flush();
      // Validate retained edits at the expected revision, outside the write lock.
      // Publication retains strict CAS for schema changes/legacy generations.
      // Same-schema refreshes may accept later schema-checked edits.
      let after: string | null = null;
      for (;;) {
        const overlays: QueryResult<{ uid: string; overrides: Record<string, Value> }> =
          await this.catalog.pool.query(
            `SELECT DISTINCT ON(uid) uid,overrides FROM moria.overlay_versions WHERE environment=$1 AND object_type=$2 AND revision<=$3 AND ($4::uuid IS NULL OR uid>$4) ORDER BY uid,revision DESC LIMIT 1000`,
            [environment, objectType, expectedRevision, after],
          );
        for (const overlay of overlays.rows)
          for (const [property, value] of Object.entries(overlay.overrides)) {
            if (!Object.hasOwn(schema.properties, property))
              throw new MoriaError('MIGRATION_REQUIRED', 'Retained overlay property was removed');
            validateValue(value, schema.properties[property]);
          }
        if (overlays.rows.length < 1000) break;
        after = overlays.rows.at(-1)!.uid;
      }
      // Bulk-loaded generations need planner statistics before first serving.
      // This samples the shared staging table outside the publication lock.
      await this.catalog.pool.query('ANALYZE moria.staged_source_rows');
      await this.catalog.pool.query("UPDATE moria.source_stages SET status='ready' WHERE id=$1", [
        stage,
      ]);
      return stage;
    } catch (error) {
      await this.catalog.pool.query("UPDATE moria.source_stages SET status='failed' WHERE id=$1", [
        stage,
      ]);
      if (
        (error as any).code === '23505' &&
        (error as any).constraint === 'staged_source_rows_pkey'
      )
        throw new MoriaError('DUPLICATE_UID', 'Canonical rows must contain unique UIDs');
      throw error;
    }
  }
  /** Publishes only a sealed stage. Never copies the staged rows under the lock. */
  async publishStage(
    stage: string,
    checkCurrent?: (client: PoolClient) => Promise<void>,
  ): Promise<string> {
    validateValue(stage, { type: 'uid', nullable: false });
    const info = (
      await this.catalog.pool.query('SELECT environment FROM moria.source_stages WHERE id=$1', [
        stage,
      ])
    ).rows[0];
    if (!info) throw new MoriaError('STAGE_INVALID', 'Unknown source stage');
    return this.catalog.transaction(
      (c) => this.publishStageIn(c, stage, checkCurrent),
      info.environment,
    );
  }
  /** Atomically expose related object types from one completed dataset sync. */
  async publishStages(
    stages: string[],
    checkCurrent?: (client: PoolClient) => Promise<void>,
    afterPublish?: (client: PoolClient) => Promise<void>,
  ): Promise<string[]> {
    if (!stages.length || new Set(stages).size !== stages.length)
      throw new MoriaError('STAGE_INVALID', 'Expected unique stages.');
    for (const stage of stages) validateValue(stage, { type: 'uid', nullable: false });
    const info = await this.catalog.pool.query(
      'SELECT environment,object_type FROM moria.source_stages WHERE id=ANY($1::uuid[])',
      [stages],
    );
    if (
      info.rowCount !== stages.length ||
      new Set(info.rows.map((r) => r.environment)).size !== 1 ||
      new Set(info.rows.map((r) => r.object_type)).size !== stages.length
    )
      throw new MoriaError('STAGE_INVALID', 'Stages must cover distinct types in one environment.');
    const environment = info.rows[0].environment;
    return this.catalog.transaction(async (c) => {
      const admission = (
        await c.query('SELECT revision FROM moria.environments WHERE id=$1 FOR UPDATE', [
          environment,
        ])
      ).rows[0].revision;
      const revisions: string[] = [];
      for (const stage of stages)
        revisions.push(await this.publishStageIn(c, stage, checkCurrent, admission));
      await afterPublish?.(c);
      return revisions;
    }, environment);
  }
  private async publishStageIn(
    c: PoolClient,
    stage: string,
    checkCurrent?: (client: PoolClient) => Promise<void>,
    admissionRevision?: string,
  ): Promise<string> {
    const staged = (
      await c.query('SELECT * FROM moria.source_stages WHERE id=$1 FOR UPDATE', [stage])
    ).rows[0];
    const current = await this.lock(c, staged.environment, staged.security_epoch);
    await checkCurrent?.(c);
    if (staged.status === 'published') return staged.published_revision;
    if (staged.status !== 'ready')
      throw new MoriaError('STAGE_INVALID', 'Source stage is not ready');
    if (staged.lease_binding) {
      const lease = staged.lease_binding as StageLease;
      // The binding is persisted at staging, never supplied by the publisher.
      // Lock the lease until COMMIT so a replacement cannot race publication.
      const live = await c.query(
        'SELECT 1 FROM moria.leases WHERE environment=$1 AND execution=$2 AND worker=$3 AND fence=$4 AND expires_at>clock_timestamp() FOR UPDATE',
        [staged.environment, lease.execution, lease.worker, lease.fence],
      );
      if (!live.rowCount)
        throw new MoriaError('LEASE_LOST', 'Staging worker lease expired or was replaced');
    }
    const head = (
      await c.query(
        'SELECT revision,object_schema,required_markings FROM moria.source_generations WHERE environment=$1 AND object_type=$2 ORDER BY revision DESC LIMIT 1',
        [staged.environment, staged.object_type],
      )
    ).rows[0];
    if (
      head?.required_markings?.some((label: string) => !staged.required_markings?.includes(label))
    )
      throw new MoriaError('MARKING_DOWNGRADE', 'Refresh cannot remove active source markings');
    if ((admissionRevision ?? current) !== staged.expected_revision) {
      // Only unchanged-source, identical-schema refreshes can cross edits.
      // Every new edit validates against this persisted active schema below.
      // Legacy metadata and schema migrations retain strict environment CAS.
      if (
        !head?.object_schema ||
        !staged.object_schema ||
        head.revision !== staged.base_source_revision ||
        fingerprint(head.object_schema) !== fingerprint(staged.object_schema)
      )
        throw new MoriaError('HEAD_CONFLICT', 'Source head or schema changed before activation');
    }
    const revision = await this.advance(c, staged.environment);
    await c.query(
      'INSERT INTO moria.source_generations(environment,object_type,revision,stage,object_schema,required_markings) VALUES($1,$2,$3,$4,$5,$6)',
      [
        staged.environment,
        staged.object_type,
        revision,
        stage,
        staged.object_schema,
        staged.required_markings,
      ],
    );
    await c.query(
      "UPDATE moria.source_stages SET status='published',published_revision=$2 WHERE id=$1",
      [stage, revision],
    );
    return revision;
  }
  async activate(
    environment: string,
    objectType: string,
    schema: ObjectSchema,
    input: Iterable<unknown> | AsyncIterable<unknown>,
    epoch: string,
    expectedRevision: string,
  ): Promise<string> {
    return this.publishStage(
      await this.stage(environment, objectType, schema, input, epoch, expectedRevision),
    );
  }
  /** Called inside a protected environment transaction. Legacy generations have
   * no schema metadata and retain the earlier strict-publication behavior. */
  async assertSchema(
    c: PoolClient,
    environment: string,
    objectType: string,
    schema: ObjectSchema,
    servingMarkings?: string[],
  ): Promise<void> {
    const active = (
      await c.query(
        'SELECT object_schema,required_markings FROM moria.source_generations WHERE environment=$1 AND object_type=$2 ORDER BY revision DESC LIMIT 1',
        [environment, objectType],
      )
    ).rows[0];
    if (active?.object_schema && fingerprint(active.object_schema) !== fingerprint(schema))
      throw new MoriaError(
        'SCHEMA_CHANGED',
        'Binding schema differs from the active source schema',
      );
    if (
      servingMarkings !== undefined &&
      active?.required_markings?.some((label: string) => !servingMarkings.includes(label))
    )
      throw new MoriaError('MARKING_DOWNGRADE', 'Serving binding omits active source markings');
  }
  private async source(
    c: PoolClient,
    environment: string,
    objectType: string,
    uid: string,
    revision: string,
  ) {
    // Legacy generations remain readable after the additive migration.
    return c.query(
      `WITH generation AS (SELECT revision,stage FROM moria.source_generations WHERE environment=$1 AND object_type=$2 AND revision<=$4 ORDER BY revision DESC LIMIT 1)
      SELECT r.value FROM generation g JOIN moria.staged_source_rows r ON r.stage=g.stage AND r.uid=$3
      UNION ALL SELECT r.value FROM generation g JOIN moria.source_rows r ON r.environment=$1 AND r.object_type=$2 AND r.revision=g.revision AND r.uid=$3 WHERE g.stage IS NULL`,
      [environment, objectType, uid, revision],
    );
  }
  async read(
    c: PoolClient,
    environment: string,
    objectType: string,
    uid: string,
    revision: string,
  ): Promise<StoredObject | null> {
    const source = await this.source(c, environment, objectType, uid, revision);
    if (!source.rowCount) return null;
    const overlay = await c.query(
      'SELECT overrides,tombstone FROM moria.overlay_versions WHERE environment=$1 AND object_type=$2 AND uid=$3 AND revision<=$4 ORDER BY revision DESC LIMIT 1',
      [environment, objectType, uid, revision],
    );
    if (overlay.rows[0]?.tombstone) return null;
    return {
      source: source.rows[0].value,
      effective: { ...source.rows[0].value, ...overlay.rows[0]?.overrides },
      revision,
    };
  }
  /** before/after guard must be pure and DB-independent; retried on serialization. */
  async edit(
    request: ActionRequest,
    objectType: string,
    uid: string,
    schema: ObjectSchema,
    edits: Edit[],
    guard: (before: StoredObject | null, after: StoredObject | null) => void,
    checkCurrent?: (client: PoolClient) => Promise<void>,
  ): Promise<{ revision: string }> {
    return this.catalog.action(
      request,
      async (c) => {
        const revision = await this.lock(c, request.environment, request.securityEpoch);
        await this.assertSchema(c, request.environment, objectType, schema);
        const before = await this.read(c, request.environment, objectType, uid, revision);
        const source = await this.source(c, request.environment, objectType, uid, revision);
        if (!source.rowCount) throw new MoriaError('NOT_FOUND', 'Object not found');
        const previous = await c.query(
          'SELECT overrides,tombstone FROM moria.overlay_versions WHERE environment=$1 AND object_type=$2 AND uid=$3 ORDER BY revision DESC LIMIT 1',
          [request.environment, objectType, uid],
        );
        const overrides = { ...previous.rows[0]?.overrides };
        let tombstone = previous.rows[0]?.tombstone ?? false;
        for (const edit of edits) {
          if (edit.op === 'delete') {
            tombstone = true;
            continue;
          }
          if (edit.op === 'restore') {
            tombstone = false;
            continue;
          }
          if (
            edit.property === schema.primaryKey ||
            !Object.hasOwn(schema.properties, edit.property)
          )
            throw new MoriaError('EDIT_INVALID', 'Cannot edit unknown property or UID');
          if (edit.op === 'clearOverride') delete overrides[edit.property];
          else
            overrides[edit.property] = validateValue(edit.value, schema.properties[edit.property]);
        }
        const effective = validateRow({ ...source.rows[0].value, ...overrides }, schema.properties);
        guard(before, tombstone ? null : { source: source.rows[0].value, effective, revision });
        const next = await this.advance(c, request.environment);
        await c.query('INSERT INTO moria.overlay_versions VALUES($1,$2,$3,$4,$5,$6)', [
          request.environment,
          objectType,
          uid,
          next,
          JSON.stringify(overrides),
          tombstone,
        ]);
        return { revision: next };
      },
      checkCurrent,
    );
  }
}
