import { randomUUID } from 'node:crypto';
import { Catalog } from '../../../services/catalog/src/catalog.js';
import { Authority } from '../../../services/security/src/authority.js';
import { WorkloadSessions } from '../../../services/security/src/sessions.js';
import { ObjectStore, type ObjectSchema } from '../../../services/ontology/src/store.js';
import type { PoolClient } from 'pg';
import {
  ObjectService,
  type SessionContext,
  type ActionBinding,
} from '../../../services/ontology/src/service.js';
import { IcebergIndexer } from '../../../services/ontology/src/indexer.js';
import { MoriaError, type Expr, type ValueType } from '../../../packages/contracts/types.js';
import { validateValue } from '../../../packages/contracts/values.js';
import { authorize } from '../../../packages/authorization/src/access.js';
import { iceberg } from '../../../workers/iceberg/stage.js';
import { icebergSchema } from '../../../packages/contracts/iceberg.js';
import { filesystemOperations } from '../../../packages/contracts/filesystem.js';

import {
  assetFields,
  assetSchema,
  assetActionInputs,
  builtinObjectTypes,
} from '../../../packages/contracts/bundled-objects.js';
export const fields = assetFields;
export const profiles = ['operator', 'west-editor', 'east-reader'] as const;
export type Profile = (typeof profiles)[number];
const environment = 'local-workspace';
const application = 'local-ui';
const markings = ['workspace'];
const schema = assetSchema,
  properties = schema.properties;
const region: Expr = {
  op: 'or',
  args: [
    { principalAttribute: 'admin' },
    { op: 'eq', args: [{ field: 'region' }, { principalAttribute: 'region' }] },
  ],
};
export const editAction: ActionBinding = {
  resource: 'edit-asset',
  revision: '1',
  markings,
  targetParameter: 'uid',
  inputs: assetActionInputs['edit-asset'],
  condition: { literal: true },
  edits: ['title', 'status', 'notes'].map((property) => ({
    op: 'set',
    property,
    value: { parameter: property },
  })),
};
export const resetAction: ActionBinding = {
  ...editAction,
  resource: 'reset-asset',
  inputs: assetActionInputs['reset-asset'],
  edits: ['title', 'status', 'notes'].map((property) => ({ op: 'clearOverride', property })),
};
export const fieldActions: ActionBinding[] = ['title', 'status', 'notes'].map((property) => ({
  ...editAction,
  resource: `edit-${property}`,
  inputs: assetActionInputs[`edit-${property}`],
  edits: [{ op: 'set', property, value: { parameter: property } }],
}));
export const managementOperations = [
  'instance:manage',
  'functions:invoke',
  ...filesystemOperations.map((operation) => `filesystem:${operation}`),
  ...fieldActions.map((action) => `${action.resource}:invoke`),
  'assets:read',
  'assets:index',
  'assets-source:read',
  'assets-source:write',
  'edit-asset:invoke',
  'reset-asset:invoke',
];

export function validateImport(input: unknown): Record<string, string>[] {
  if (!Array.isArray(input) || input.length < 1 || input.length > 1000)
    throw new MoriaError('INPUT_INVALID', 'Import requires 1–1,000 rows.');
  const seen = new Set<string>();
  return input.map((row, index) => {
    if (
      !row ||
      typeof row !== 'object' ||
      Array.isArray(row) ||
      Object.keys(row).some((key) => !fields.includes(key as (typeof fields)[number]))
    )
      throw new MoriaError(
        'INPUT_INVALID',
        `Row ${index + 1}: expected only id, title, region, status, notes.`,
      );
    for (const field of fields) {
      validateValue(row[field], properties[field], `row ${index + 1}.${field}`);
      if (row[field].length > 2000 || (field !== 'notes' && !row[field].trim()))
        throw new MoriaError('INPUT_INVALID', `Row ${index + 1}: invalid ${field}.`);
    }
    if (!['West', 'East'].includes(row.region))
      throw new MoriaError('INPUT_INVALID', 'Region must be West or East.');
    if (seen.has(row.id)) throw new MoriaError('INPUT_INVALID', `Duplicate id: ${row.id}`);
    seen.add(row.id);
    return { ...row };
  });
}
export async function createWorkspace(catalog: Catalog, warehouse: string) {
  await catalog.migrate();
  await catalog.ensureEnvironment(environment);
  await catalog.ensureDataset(environment, 'assets-source');
  const authority = new Authority(catalog),
    store = new ObjectStore(catalog);
  await authority.migrate();
  await store.migrate();
  const operations = managementOperations;
  if (
    !(
      await catalog.pool.query(
        'SELECT 1 FROM moria.application_scopes WHERE environment=$1 AND application=$2',
        [environment, application],
      )
    ).rowCount
  )
    await authority.setApplication(environment, 'local-bootstrap', application, operations);
  for (const profile of profiles) {
    if (
      (
        await catalog.pool.query(
          'SELECT 1 FROM moria.principals WHERE environment=$1 AND principal=$2',
          [environment, profile],
        )
      ).rowCount
    )
      continue;
    await authority.syncPrincipal(environment, 'local-bootstrap', profile, {
      kind: 'workload',
      enabled: true,
      markings,
      attributes: {
        admin: profile === 'operator',
        region: profile === 'east-reader' ? 'East' : 'West',
      },
    });
    await authority.setGrants(
      environment,
      'local-bootstrap',
      profile,
      application,
      profile === 'operator'
        ? operations
        : profile === 'west-editor'
          ? [
              ...filesystemOperations.map((operation) => `filesystem:${operation}`),
              'assets:read',
              'edit-asset:invoke',
              'reset-asset:invoke',
              ...fieldActions.map((action) => `${action.resource}:invoke`),
            ]
          : ['assets:read', ...filesystemOperations.map((operation) => `filesystem:${operation}`)],
    );
  }
  const service = new ObjectService(store, authority, {
    objectTypeId: builtinObjectTypes.assets,
    schema,
    policies: {
      resource: 'assets',
      markings,
      properties,
      sourceColumns: properties,
      attributes: {
        admin: { type: 'boolean', nullable: false },
        region: { type: 'string', nullable: false },
      },
      row: region,
    },
    writePolicy: region,
  });
  const indexer = new IcebergIndexer(store, authority, {
    source: { kind: 'dataset', resource: 'assets-source', markings },
    target: { resource: 'assets', markings, schema },
  });
  await indexer.migrate();
  const sessions = new WorkloadSessions(authority);
  const context = (principal: Profile) => ({ environment, principal, application });
  const tokens = new Map<Profile, { token: string; expiresAt: string }>();
  const resolveContext = (actor: Profile | SessionContext): SessionContext =>
    typeof actor === 'string' ? context(actor) : actor;
  let importing = false;
  async function managementGuard(actor: Profile | SessionContext) {
    const session = resolveContext(actor);
    if (session.environment !== environment || session.application !== application)
      throw new MoriaError('FORBIDDEN', 'Management application session required.');
    const state = await authority.current(environment, session.principal, application);
    authorize(state.identity, 'instance', 'manage');
    const check = async (c: PoolClient) => {
      await c.query('SELECT 1 FROM moria.environments WHERE id=$1 FOR SHARE', [environment]);
      await authority.assertCurrent(
        c,
        environment,
        session.principal,
        application,
        state.epoch,
        session.sessionHash,
      );
    };
    await catalog.transaction(check, environment);
    return check;
  }
  return {
    service,
    sessions,
    authority,
    managementGuard,
    async requireManagement(actor: Profile | SessionContext) {
      await managementGuard(actor);
    },
    async token(profile: Profile) {
      let entry = tokens.get(profile);
      if (!entry || Date.parse(entry.expiresAt) < Date.now() + 30000) {
        entry = await sessions.mint(context(profile), 'local-ui');
        tokens.set(profile, entry);
      }
      return entry.token;
    },
    async status() {
      const result = await catalog.pool.query(
        'SELECT revision FROM moria.source_generations WHERE environment=$1 AND object_type=$2 ORDER BY revision DESC LIMIT 1',
        [environment, 'assets'],
      );
      return { ready: result.rowCount !== 0, revision: result.rows[0]?.revision ?? '0', importing };
    },
    async ingest(
      actor: Profile | SessionContext,
      input: unknown,
      refreshIdentity?: () => Promise<SessionContext>,
    ) {
      const session = resolveContext(actor);
      if (session.environment !== environment || session.application !== application)
        throw new MoriaError('FORBIDDEN', 'Workspace session required.');
      const rows = validateImport(input);
      const state = await authority.current(environment, session.principal, application);
      authorize(state.identity, 'assets-source', 'write', markings);
      if (typeof actor !== 'string') authorize(state.identity, 'instance', 'manage');
      await catalog.transaction(
        (c) =>
          authority.assertCurrent(
            c,
            environment,
            session.principal,
            application,
            state.epoch,
            session.sessionHash,
          ),
        environment,
      );
      if (importing) throw new MoriaError('RESOURCE_BUSY', 'Another import is in progress.');
      importing = true;
      let published = false;
      try {
        const head = (await catalog.heads(environment, ['assets-source']))[0];
        const resolved = [];
        for (const row of rows)
          resolved.push({ ...row, uid: await catalog.resolveIdentity('local-assets', row.id) });
        const staged = await iceberg({
          operation: 'stage',
          warehouse,
          ...(head.metadataLocation ? { baseMetadataLocation: head.metadataLocation } : {}),
          schema: icebergSchema(properties),
          rows: resolved,
        });
        const execution = randomUUID(),
          worker = 'local-ingest';
        const fence = await catalog.claim(environment, execution, worker, 300);
        if (refreshIdentity) {
          const refreshed = await refreshIdentity();
          if (
            refreshed.principal !== session.principal ||
            refreshed.sessionHash !== session.sessionHash
          )
            throw new MoriaError('UNAUTHENTICATED', 'Session changed during import.');
        }
        const current = await authority.current(environment, session.principal, application);
        authorize(current.identity, 'assets-source', 'write', markings);
        if (typeof actor !== 'string') authorize(current.identity, 'instance', 'manage');
        await catalog.publish(
          {
            environment,
            execution,
            worker,
            fence,
            securityEpoch: current.epoch,
            outputs: [
              {
                dataset: 'assets-source',
                expectedRevision: head.revision,
                metadataLocation: staged.metadataLocation,
                snapshotId: staged.snapshotId,
              },
            ],
            lineage: {},
          },
          (c) =>
            authority.assertCurrent(
              c,
              environment,
              session.principal,
              application,
              current.epoch,
              session.sessionHash,
            ),
        );
        published = true;
        const result = await indexer.runSupervised(session, { execution, worker, fence });
        return { ...result, rows: rows.length };
      } catch (error) {
        if (published)
          throw new MoriaError(
            'INDEX_FAILED',
            'Dataset saved, but indexing failed. The prior objects remain visible; import the file again to retry.',
          );
        throw error;
      } finally {
        importing = false;
      }
    },
  };
}
export type Workspace = Awaited<ReturnType<typeof createWorkspace>>;
