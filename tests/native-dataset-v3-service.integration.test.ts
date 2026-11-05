import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { execFile } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { promisify } from 'node:util';
import test from 'node:test';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';
import { publishNativeSource } from './fixtures/native-source/setup.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { filesystemClient } from '../packages/platform/src/filesystem.js';
import { createTransport } from '../packages/platform/src/transport.js';
import { objectClient } from '../packages/osdk/src/runtime.js';
import type { ObjectSchema } from '../services/ontology/src/store.js';

test(
  'accepted permanent Source indexes v3 heads independently of its registrar and retains exact array keys',
  { timeout: 300000 },
  async (t) => {
    const columns: ObjectSchema['properties'] = {
      uid: { type: 'uid', nullable: false },
      tags: { type: 'array', nullable: false, items: { type: 'string', nullable: false } },
      note: { type: 'string', nullable: true },
    };
    const fixture = await managedRetentionDataset({ primaryKey: ['tags'], columns });
    t.after(fixture.close);
    const { db, environment, dataset, principal, application } = fixture;
    const identityDomain = environment + '/v3-service-fixture';
    const owner = await fixture.command(['identity', 'register-domain'], {
      domain: identityDomain,
      cell: fixture.cell,
    });
    const identities = await fixture.command(['identity', 'resolve'], {
      domain: identityDomain,
      keys: Array.from({ length: 16 }, (_, i) => 'row-' + i),
      owner,
    });
    assert.equal(new Set(identities.uids).size, 16);
    const rows = Array.from({ length: 16 }, (_, i) => ({
      uid: identities.uids[i] as string,
      tags: ['key-' + i, 'key-' + i],
      note: i % 2 ? null : 'row-' + i,
    }));
    const first = await fixture.publish(rows, { graph: true });
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
    ]);
    const target = dependent.resource('v3_source/ObjectType/Record');
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
      ]),
    ];
    await authority.setApplication(environment, 'fixture', application, operations);
    await authority.setGrants(environment, 'fixture', principal, application, operations);
    const sessions = new WorkloadSessions(authority);
    const registrar = (await sessions.mint({ environment, principal, application }, 'fixture'))
      .token;
    const service = randomUUID();
    await db.pool.query(
      "INSERT INTO moria.runtime_service_authorities(id,environment,principal,application,operations,markings,domains,resource_classes,incarnation) SELECT $1,$2,$3,$4,$5,ARRAY['private'],ARRAY['indexing'],ARRAY['standard'],incarnation FROM moria.indexing_instance",
      [
        service,
        environment,
        principal,
        application,
        [dataset + ':read', target + ':index', 'filesystem:read'],
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
          markings: ['private'],
          partitions: 2,
          timeoutSeconds: 1800,
          serviceBindingRevision: 'v3-source-v1',
        },
      },
    ]);
    const transport = (token: string) =>
      createTransport({
        baseUrl: configured.url,
        fetch: (url, init) =>
          fetch(url, { ...init, headers: { ...init?.headers, authorization: 'Bearer ' + token } }),
      });
    const fs = filesystemClient(transport(registrar));
    const plan = await fs.plan(dependent.root.id, dependent.publication.digest);
    await fs.apply(plan.id);
    assert.deepEqual((await db.catalog.heads(environment, [dataset]))[0], first.head);
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
    const currentBuild = async () =>
      (
        await db.pool.query(
          "SELECT b.id,a.revision::text AS alias_revision,s.row_count::text,s.key_count::text,s.ordered_keys FROM moria.indexing_builds b JOIN moria.indexing_bindings current_binding ON current_binding.environment=b.environment AND current_binding.target=b.target AND current_binding.revision=b.binding_revision JOIN moria.dataset_heads h ON h.environment=b.environment AND h.dataset=b.source AND h.revision=b.source_revision AND h.metadata_location=b.metadata_location JOIN moria.native_source_row_stores s ON s.id=b.id JOIN moria.native_source_generation_aliases a ON a.store=s.id WHERE b.environment=$1 AND b.target=$2 AND b.status='active'",
          [environment, target],
        )
      ).rows;
    await drain();
    const firstBuild = await currentBuild();
    if (firstBuild.length !== 1) {
      const blocked = (
        await db.pool.query(
          'SELECT error FROM moria.indexing_blocked_intents WHERE environment=$1 AND target=$2 AND source_revision=$3',
          [environment, target, first.head.revision],
        )
      ).rows;
      t.diagnostic(
        JSON.stringify({
          phase: 'actualPermanentV3Admission',
          acceptedActiveBuilds: firstBuild.length,
          blocked,
          datasetHeadUnchanged: true,
        }),
      );
    }
    assert.equal(
      firstBuild.length,
      1,
      'actual accepted permanent service must produce a current managed v3 Source store',
    );
    assert.equal(firstBuild[0].row_count, '16');
    assert.equal(firstBuild[0].key_count, '16');
    assert.deepEqual(firstBuild[0].ordered_keys, ['tags']);
    const delegated = (
      await db.pool.query(
        'SELECT origin_session_hash,proof FROM moria.runtime_indexing_delegations WHERE build=$1',
        [firstBuild[0].id],
      )
    ).rows[0];
    assert.equal(delegated.origin_session_hash, null);
    assert.equal(delegated.proof.originKind, 'service');
    assert.equal(delegated.proof.originBinding, context.executionBinding);
    const keys = (
      await db.pool.query(
        "SELECT encode(key_hash,'hex') AS hash,convert_from(encoded_key,'UTF8')::jsonb AS key,uid FROM moria.native_source_key_map WHERE store=$1",
        [firstBuild[0].id],
      )
    ).rows;
    assert.equal(keys.length, 16);
    const expectedRows = new Map(rows.map((row) => [row.uid, row]));
    for (const row of keys) {
      assert(expectedRows.has(row.uid));
      assert.deepEqual(row.key, [expectedRows.get(row.uid)!.tags]);
      assert.equal(row.hash, createHash('sha256').update(JSON.stringify(row.key)).digest('hex'));
    }
    assert.deepEqual(keys.map((row) => row.uid).sort(), rows.map((row) => row.uid).sort());
    const objects = objectClient<{ uid: string; tags: string[]; note: string | null }>(
      transport(fixture.token),
      target,
      columns,
    );
    for (const row of rows) assert.deepEqual((await objects.get(row.uid)).object, row);
    await sessions.revoke(await sessions.authenticate('Bearer ' + registrar), 'fixture');
    const second = await fixture.publish([], { graph: true });
    await drain();
    const secondBuild = await currentBuild();
    assert.equal(secondBuild.length, 1);
    assert.notEqual(secondBuild[0].id, firstBuild[0].id);
    assert.equal(secondBuild[0].row_count, '0');
    assert.equal(secondBuild[0].key_count, '0');
    assert.equal((await objects.query()).objects.length, 0);
    assert.equal(
      (
        await db.pool.query('SELECT count(*)::text FROM moria.runtime_object_base($1,$2,$3)', [
          environment,
          target,
          firstBuild[0].alias_revision,
        ])
      ).rows[0].count,
      '16',
    );
    const secondDelegation = (
      await db.pool.query(
        'SELECT origin_session_hash,proof FROM moria.runtime_indexing_delegations WHERE build=$1',
        [secondBuild[0].id],
      )
    ).rows[0];
    assert.equal(secondDelegation.origin_session_hash, null);
    assert.equal(secondDelegation.proof.originKind, 'service');
    assert.deepEqual((await db.catalog.heads(environment, [dataset]))[0], second.head);
    t.diagnostic(
      JSON.stringify({
        realDependentSourceCompilation: true,
        actualOperatorApproval: true,
        actualServiceAdmissionAndDelegation: true,
        noRegistrarTokenInWorker: true,
        exactArrayKeyMap: true,
        typedObjectGet: true,
        registrarRevocationThenFreshServiceReadmission: true,
        emptyCurrentAndRetainedHistory: true,
        datasetHeadUnchangedByIndexing: true,
      }),
    );
  },
);
