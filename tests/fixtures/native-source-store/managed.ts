import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { execFile } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { promisify } from 'node:util';
import type { managedRetentionDataset } from '../native-retention/dataset.js';
import { publishNativeSource } from '../native-source/setup.js';
import { Authority } from '../../../services/security/src/authority.js';
import { WorkloadSessions } from '../../../services/security/src/sessions.js';
import { filesystemClient } from '../../../packages/platform/src/filesystem.js';
import { createTransport } from '../../../packages/platform/src/transport.js';
import type { ObjectSchema } from '../../../services/ontology/src/store.js';

/** Install a real dependent Source through compiler publication and current service admission. */
export async function prepareManagedSource(
  fixture: Awaited<ReturnType<typeof managedRetentionDataset>>,
  columns: ObjectSchema['properties'],
  options: {
    actions?: boolean;
    profiles?: unknown[];
    beforeApply?: (client: ReturnType<typeof filesystemClient>) => Promise<void>;
  } = {},
) {
  const { db, environment, dataset, principal, application, markings } = fixture;
  const headBefore = (await db.catalog.heads(environment, [dataset]))[0];
  const targetType = 'v3_source/ObjectType/Record';
  const actionNames = options.actions ? (['Edit', 'Delete'] as const) : [];
  const dependent = await publishNativeSource(fixture.baseUrl, fixture.token, [
    {
      path: '.ontology.json',
      text: JSON.stringify({
        version: 1,
        name: 'V3 Source',
        namespace: 'v3_source',
        dependencies: [
          { rootId: fixture.published.root.id, digest: fixture.published.publication.digest },
        ],
      }),
    },
    {
      path: 'record.object.json',
      text: JSON.stringify({
        apiVersion: 'moria/v1alpha2',
        kind: 'ObjectType',
        metadata: { namespace: 'v3_source', name: 'Record' },
        spec: {
          primaryKey: 'uid',
          properties: columns,
          source: {
            resource: 'retention/Dataset/records',
            mapping: Object.fromEntries(Object.keys(columns).map((name) => [name, name])),
          },
        },
      }),
    },
    ...actionNames.map((name) => ({
      path: name + '.action.json',
      text: JSON.stringify({
        apiVersion: 'moria/v1alpha2',
        kind: 'Action',
        metadata: { namespace: 'v3_source', name },
        spec: {
          targetType,
          targetParameter: 'uid',
          inputs: { uid: columns.uid, ...(name === 'Edit' ? { note: columns.note } : {}) },
          condition: { literal: true },
          edits: [
            {
              op: name === 'Edit' ? 'set' : 'delete',
              targetType,
              uid: { parameter: 'uid' },
              ...(name === 'Edit' ? { property: 'note', value: { parameter: 'note' } } : {}),
            },
          ],
        },
      }),
    })),
  ]);
  const target = dependent.resource(targetType);
  const actions = actionNames.map((name) => dependent.resource('v3_source/Action/' + name));
  const authority = new Authority(db.catalog);
  const current = await authority.current(environment, principal, application);
  const operations = [
    ...new Set([
      ...current.identity.grantedOperations,
      ...current.identity.applicationScopes,
      dataset + ':read',
      dataset + ':write',
      target + ':index',
      target + ':read',
      ...actions.map((id) => id + ':invoke'),
    ]),
  ];
  await authority.setApplication(environment, 'fixture', application, operations);
  await authority.setGrants(environment, 'fixture', principal, application, operations);
  const sessions = new WorkloadSessions(authority);
  const registrar = (await sessions.mint({ environment, principal, application }, 'fixture')).token;
  const service = randomUUID();
  await db.pool.query(
    "INSERT INTO moria.runtime_service_authorities(id,environment,principal,application,operations,markings,domains,resource_classes,incarnation) SELECT $1,$2,$3,$4,$5,$6,ARRAY['indexing'],ARRAY['standard'],incarnation FROM moria.indexing_instance",
    [
      service,
      environment,
      principal,
      application,
      [dataset + ':read', target + ':index', 'filesystem:read'],
      markings,
    ],
  );
  await db.pool.query(
    "INSERT INTO moria.runtime_service_targets(environment,resource,domain,binding_revision,resource_class,authority) VALUES($1,$2,'indexing','v3-source-v1','standard',$3)",
    [environment, target, service],
  );
  const artifactNamespace = {
    warehouseId: 'v3-source-artifacts',
    tableId: 'rows',
    revision: 'v1',
  };
  const artifactRoot = join(db.root, 'v3-source-artifacts');
  await mkdir(artifactRoot, { mode: 0o700 });
  fixture.storageBindings.push({
    namespace: artifactNamespace,
    provider: {
      kind: 'posix',
      root: artifactRoot,
      limits: { maxObjectBytes: 128 * 1024 * 1024, chunkBytes: 1024 * 1024 },
    },
  });
  fixture.nativeEnv.MORIA_NATIVE_STORAGE_BINDINGS = JSON.stringify(fixture.storageBindings);
  const configured = await fixture.start([
    {
      environment,
      projectId: dependent.role.projectId,
      reference: 'v3_source/ObjectType/Record',
      configuration: {
        projection: {
          version: 1,
          sourceFields: ['uid', 'tags', 'note'],
          effectiveFields: ['uid', 'tags', 'note'],
          layouts: [{ name: 'uid', sort: [], keyBytes: 256 }],
          readerRoles: [new URL(fixture.nativeEnv.RETENTION_DATABASE!).username],
          budgets: {
            rowBytes: 1048576,
            batchRows: 100,
            batchBytes: 1048576,
            commitReplayRows: 100,
            commitReplayBytes: 1048576,
            ddlTimeMs: 5000,
          },
        },
        artifactNamespace,
        markings,
        partitions: 2,
        timeoutSeconds: 1800,
        serviceBindingRevision: 'v3-source-v1',
      },
    },
    ...(options.profiles ?? []),
  ]);
  const transport = (token: string) =>
    createTransport({
      baseUrl: configured.url,
      fetch: (url, init) =>
        fetch(url, { ...init, headers: { ...init?.headers, authorization: 'Bearer ' + token } }),
    });
  const fs = filesystemClient(transport(registrar));
  await options.beforeApply?.(fs);
  const plan = await fs.plan(dependent.root.id, dependent.publication.digest);
  await fs.apply(plan.id);
  assert.deepEqual((await db.catalog.heads(environment, [dataset]))[0], headBefore);
  const context = (
    await db.pool.query(
      'SELECT context FROM moria.indexing_bindings WHERE environment=$1 AND target=$2',
      [environment, target],
    )
  ).rows[0].context;
  assert.equal(context.serviceAuthority, service);
  assert(context.executionBinding);
  assert.equal(Object.hasOwn(context, 'sessionHash'), false);
  assert.equal(
    (
      await db.pool.query(
        'SELECT count(*) FROM moria.native_indexing_service_definitions WHERE binding=$1',
        [context.executionBinding],
      )
    ).rows[0].count,
    '1',
  );
  assert.equal(
    (
      await db.pool.query(
        'SELECT authority FROM moria.runtime_execution_services WHERE binding=$1',
        [context.executionBinding],
      )
    ).rows[0].authority,
    service,
  );
  const drain = async () => {
    const result = await promisify(execFile)(fixture.runtime, ['drain'], {
      env: fixture.nativeEnv,
      timeout: 120000,
      maxBuffer: 1024 * 1024,
    });
    return result.stdout;
  };
  return {
    target,
    actions,
    dependent,
    baseUrl: configured.url,
    context,
    sessions,
    registrar,
    transport,
    drain,
    service,
  };
}
