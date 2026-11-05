import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import test from 'node:test';
import { database } from './fixtures/catalog-storage/database.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { IcebergIndexer } from '../services/ontology/src/indexer.js';
import { ObjectStore } from '../services/ontology/src/store.js';
import { setTimeout as delay } from 'node:timers/promises';

test('resource data provenance retains classification, bounds cycles and pre-admits publication authority', async (t) => {
  const db = await database();
  t.after(db.close);
  const environment = randomUUID(),
    source = randomUUID(),
    target = randomUUID();
  await db.catalog.ensureEnvironment(environment);
  for (const id of [source, target]) await db.catalog.ensureDataset(environment, id);
  const authority = new Authority(db.catalog),
    sessions = new WorkloadSessions(authority);
  const operations = [source, target].flatMap((id) => [`${id}:read`, `${id}:write`, `${id}:index`]);
  await authority.setApplication(environment, 'fixture', 'provenance', operations);
  for (const [principal, markings] of [
    ['privileged', ['private', 'secret']],
    ['reader', ['private']],
  ] as const) {
    await authority.syncPrincipal(environment, 'fixture', principal, {
      enabled: true,
      kind: 'workload',
      attributes: {},
      markings: [...markings],
    });
    await authority.setGrants(environment, 'fixture', principal, 'provenance', operations);
  }
  const context = { environment, principal: 'privileged', application: 'provenance' };
  const credential = await sessions.mint(context, 'fixture');
  const privileged = {
    ...context,
    sessionHash: createHash('sha256').update(credential.token).digest('hex'),
  };
  const readerCredential = await sessions.mint({ ...context, principal: 'reader' }, 'fixture');
  const reader = {
    ...context,
    principal: 'reader',
    sessionHash: createHash('sha256').update(readerCredential.token).digest('hex'),
  };
  await t.test(
    'permanent indexing revisions use exact canonical strings for large epochs',
    async () => {
      const definitionHash = 'a'.repeat(64),
        metadataBinding = '00000000-0000-4000-8000-000000000001',
        securityEpoch = '42',
        instanceEpoch = '9007199254740993';
      const expected = `sha256:${createHash('sha256').update(JSON.stringify({ definitionHash, instanceEpoch, metadataBinding, securityEpoch })).digest('hex')}`;
      assert.equal(
        (
          await db.pool.query(
            'SELECT moria.runtime_indexing_service_revision($1,$2,$3,$4) AS revision',
            [definitionHash, metadataBinding, securityEpoch, instanceEpoch],
          )
        ).rows[0].revision,
        expected,
      );
      await assert.rejects(
        db.pool.query('SELECT moria.runtime_indexing_service_revision($1,$2,-1,1)', [
          definitionHash,
          metadataBinding,
        ]),
        /INDEXING_INVALID/,
      );
    },
  );
  const namespace = async (table: string, enabled: boolean, markings: string[]) =>
    db.pool.query(
      `INSERT INTO moria.native_storage_namespaces(environment,warehouse_id,table_id,resource,revision,purpose,enabled,iceberg_uuid,markings)
      VALUES($1,'provenance',$2,$3,'v1','dataset-source',$4,$5,$6)`,
      [environment, table, source, enabled, randomUUID(), markings],
    );
  await namespace('initial', true, ['private']);
  await namespace('never-enabled', false, ['never-enabled']);
  const nativeLabels = async (hash: string) =>
    (
      await db.pool.query("SELECT moria.native_serving_resource_access($1,$2,'read') AS labels", [
        hash,
        target,
      ])
    ).rows[0].labels;
  const nodeLabels = async (session: typeof reader) =>
    db.catalog.transaction((c) => authority.resourceAccess(c, session, target), environment);
  await t.test(
    'authorized dependency is admitted before its immutable binding captures the resulting epoch',
    async () => {
      const oldEpoch = (
        await authority.current(environment, context.principal, context.application)
      ).epoch;
      const spec = {
        pinnedInputs: [{ resource: source, revision: '0', snapshot: 'fixture' }],
        expectedHeads: { [target]: '0' },
      };
      const row = (
        await db.pool.query(
          `INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation)
      SELECT $1,$2,$3,$4,$5,ARRAY['private'],$6,i.security_epoch,c.incarnation FROM moria.instance_state i,moria.indexing_instance c RETURNING security_epoch::text`,
          [environment, target, spec, privileged, operations, oldEpoch],
        )
      ).rows[0];
      assert.ok(BigInt(row.security_epoch) > BigInt(oldEpoch));
      assert.equal(
        row.security_epoch,
        (await authority.current(environment, context.principal, context.application)).epoch,
      );
      assert.deepEqual(await nativeLabels(reader.sessionHash), ['private']);
      assert.deepEqual(await nodeLabels(reader), ['private']);
    },
  );
  await t.test(
    'a valid Dataset publication records blocked index intent for an expired registration, then fresh registration accepts the same head',
    async () => {
      const indexer = new IcebergIndexer(new ObjectStore(db.catalog), authority, {
        source: { kind: 'dataset', resource: source, markings: ['private'] },
        target: {
          resource: target,
          markings: ['private'],
          schema: { primaryKey: 'uid', properties: { uid: { type: 'uid', nullable: false } } },
        },
      });
      const short = await sessions.mint(context, 'fixture');
      const shortHash = createHash('sha256').update(short.token).digest('hex');
      const expires = (
        await db.pool.query(
          "UPDATE moria.workload_sessions SET expires_at=clock_timestamp()+interval '2 seconds' WHERE token_hash=$1 RETURNING expires_at",
          [shortHash],
        )
      ).rows[0].expires_at as Date;
      await indexer.registerDistributed(
        { ...context, sessionHash: shortHash },
        { targetId: target, warehouse: '/trusted-fixture', partitions: 1 },
      );
      await delay(Math.max(0, expires.getTime() - Date.now()) + 100);
      const execution = randomUUID(),
        worker = 'publisher',
        fence = await db.catalog.claim(environment, execution, worker, 60);
      const receipt = await db.catalog.publish({
        environment,
        execution,
        worker,
        fence,
        securityEpoch: (
          await authority.current(environment, context.principal, context.application)
        ).epoch,
        outputs: [
          {
            dataset: source,
            expectedRevision: '0',
            metadataLocation: 'fixture-metadata',
            snapshotId: '100',
          },
        ],
        lineage: {},
      });
      assert.equal(receipt.outputs[0].revision, '1');
      assert.deepEqual(
        (
          await db.pool.query(
            'SELECT error,source_revision::text AS revision FROM moria.indexing_blocked_intents WHERE environment=$1',
            [environment],
          )
        ).rows,
        [{ error: 'UNAUTHENTICATED', revision: '1' }],
      );
      assert.equal(
        (
          await db.pool.query(
            'SELECT count(*)::text FROM moria.indexing_builds WHERE environment=$1',
            [environment],
          )
        ).rows[0].count,
        '0',
      );
      await assert.rejects(
        indexer.registerDistributed(
          { ...context, sessionHash: shortHash },
          { targetId: target, warehouse: '/trusted-fixture', partitions: 1 },
        ),
        /expired or was revoked/,
      );
      await indexer.registerDistributed(privileged, {
        targetId: target,
        warehouse: '/trusted-fixture',
        partitions: 1,
      });
      assert.equal(
        (
          await db.pool.query(
            'SELECT count(*)::text FROM moria.runtime_indexing_delegations d JOIN moria.indexing_builds b ON b.id=d.build WHERE b.environment=$1',
            [environment],
          )
        ).rows[0].count,
        '1',
      );
    },
  );
  await t.test(
    'new namespace classification immediately tightens derived reads in both runtimes and invalidates old epochs',
    async () => {
      const before = (await authority.current(environment, context.principal, context.application))
        .epoch;
      await namespace('secret', true, ['secret']);
      const build = (
        await db.pool.query('SELECT id FROM moria.indexing_builds WHERE environment=$1', [
          environment,
        ])
      ).rows[0].id;
      await assert.rejects(
        db.pool.query('SELECT moria.indexing_check($1)', [build]),
        /AUTHORIZATION_CHANGED/,
      );
      assert.ok(
        BigInt(
          (await authority.current(environment, context.principal, context.application)).epoch,
        ) > BigInt(before),
      );
      await assert.rejects(nativeLabels(reader.sessionHash), /FORBIDDEN/);
      await assert.rejects(nodeLabels(reader), /classification/);
      assert.deepEqual(await nativeLabels(privileged.sessionHash), ['private', 'secret']);
      assert.deepEqual(await nodeLabels(privileged), ['private', 'secret']);
      await db.pool.query(
        "UPDATE moria.native_storage_namespaces SET enabled=false WHERE warehouse_id='provenance' AND table_id='secret'",
      );
      assert.deepEqual(await nativeLabels(privileged.sessionHash), ['private', 'secret']);
      assert.deepEqual(await nodeLabels(privileged), ['private', 'secret']);
      await assert.rejects(nativeLabels(reader.sessionHash), /FORBIDDEN/);
    },
  );
  await t.test(
    'duplicate admission is epoch-stable and cyclic provenance terminates with retained union',
    async () => {
      const before = (await authority.current(environment, context.principal, context.application))
        .epoch;
      await db.catalog.transaction(
        (c) =>
          db.catalog.admitDataProvenance(c, privileged.sessionHash, [target], [source], 'write'),
        environment,
      );
      assert.equal(
        (await authority.current(environment, context.principal, context.application)).epoch,
        before,
      );
      await db.catalog.transaction(
        (c) =>
          db.catalog.admitDataProvenance(c, privileged.sessionHash, [source], [target], 'write'),
        environment,
      );
      assert.deepEqual(await nativeLabels(privileged.sessionHash), ['private', 'secret']);
      await assert.rejects(
        db.pool.query('DELETE FROM moria.resource_data_edges WHERE environment=$1', [environment]),
        /IMMUTABLE_DATA_PROVENANCE/,
      );
      await assert.rejects(
        db.pool.query('DELETE FROM moria.resource_data_labels WHERE environment=$1', [environment]),
        /IMMUTABLE_DATA_PROVENANCE/,
      );
    },
  );
  await t.test(
    'registered producer metadata admits exact declared provenance and rejects authority or output-floor substitution atomically',
    async () => {
      const inputs = [randomUUID(), randomUUID(), randomUUID()],
        outputs = [randomUUID(), randomUUID()],
        publicOutput = randomUUID(),
        producer = randomUUID(),
        service = randomUUID();
      for (const id of [...inputs, ...outputs, publicOutput]) {
        await db.catalog.ensureDataset(environment, id);
        await db.pool.query(
          "INSERT INTO moria.native_storage_namespaces(environment,warehouse_id,table_id,resource,revision,purpose,iceberg_uuid,markings) VALUES($1,'registered',$2,$2,'v1','dataset-source',$3,$4)",
          [environment, id, randomUUID(), id === publicOutput ? [] : ['private']],
        );
        operations.push(`${id}:read`, `${id}:write`);
      }
      await authority.setApplication(environment, 'fixture', 'provenance', operations);
      await authority.setGrants(environment, 'fixture', 'privileged', 'provenance', operations);
      await db.pool.query(
        "INSERT INTO moria.runtime_service_authorities(id,environment,principal,application,operations,markings,domains,resource_classes,incarnation) SELECT $1,$2,'privileged','provenance',$3,ARRAY['private'],ARRAY['transform'],ARRAY['standard'],incarnation FROM moria.indexing_instance",
        [service, environment, operations],
      );
      await db.pool.query(
        "INSERT INTO moria.runtime_service_targets(environment,resource,domain,binding_revision,resource_class,authority) VALUES($1,$2,'transform','v1','standard',$3)",
        [environment, producer, service],
      );
      const baseSpec = {
        version: 1,
        domain: 'transform',
        release: 'a'.repeat(64),
        resourceClass: 'standard',
        timeoutSeconds: 30,
        retryPolicy: 'idempotent',
        inputSchema: { type: 'object', properties: {} },
        pinnedInputs: inputs.map((resource) => ({ resource, revision: '0', snapshot: 'fixture' })),
        expectedHeads: Object.fromEntries(outputs.map((resource) => [resource, '0'])),
      };
      const serviceContext = { ...context, serviceAuthority: service, serviceTargetRevision: 'v1' };
      const insert = async (
        spec: unknown = baseSpec,
        selectedContext: unknown = serviceContext,
      ) => {
        const epoch = (await authority.current(environment, context.principal, context.application))
          .epoch;
        return db.pool.query(
          `INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation)
        SELECT $1,$2,$3,$4,$5,ARRAY['private'],$6,i.security_epoch,c.incarnation FROM moria.instance_state i,moria.indexing_instance c RETURNING id,security_epoch::text`,
          [environment, producer, spec, selectedContext, operations, epoch],
        );
      };
      const before = (await authority.current(environment, context.principal, context.application))
        .epoch;
      for (const invalid of [
        { ...serviceContext, serviceAuthority: randomUUID() },
        { ...serviceContext, serviceTargetRevision: 'stale' },
        { ...serviceContext, principal: 'reader' },
        { ...serviceContext, sessionHash: privileged.sessionHash },
      ])
        await assert.rejects(
          insert(baseSpec, invalid),
          /SERVICE_PURPOSE_REVOKED|AUTHORIZATION_CHANGED/,
        );
      await assert.rejects(
        insert({ ...baseSpec, expectedHeads: { [publicOutput]: '0' } }),
        /MARKING_DOWNGRADE/,
      );
      assert.equal(
        (await authority.current(environment, context.principal, context.application)).epoch,
        before,
      );
      assert.equal(
        (
          await db.pool.query(
            'SELECT count(*)::text AS count FROM moria.resource_data_edges WHERE environment=$1 AND derived=ANY($2)',
            [environment, [...outputs, publicOutput]],
          )
        ).rows[0].count,
        '0',
      );
      const admitted = (await insert()).rows[0];
      assert.equal(BigInt(admitted.security_epoch) - BigInt(before), 6n);
      const replay = () =>
        db.pool.query(
          "SELECT moria.runtime_service_dataset_edges($1,$2,$3,'transform','v1','standard',$4,$5,ARRAY['private'])::text AS epoch",
          [service, environment, producer, inputs, outputs],
        );
      assert.equal((await replay()).rows[0].epoch, admitted.security_epoch);
      assert.equal(
        admitted.security_epoch,
        (await authority.current(environment, context.principal, context.application)).epoch,
      );
      await db.pool.query(
        "INSERT INTO moria.runtime_execution_services(binding,authority,target_revision) VALUES($1,$2,'v1')",
        [admitted.id, service],
      );
      await db.pool.query(
        'UPDATE moria.runtime_service_authorities SET enabled=false WHERE id=$1',
        [service],
      );
      await assert.rejects(replay(), /SERVICE_PURPOSE_REVOKED/);
      assert.equal(
        (
          await db.pool.query(
            'SELECT count(*)::text AS count FROM moria.resource_data_edges WHERE environment=$1 AND derived=ANY($2) AND source=ANY($3)',
            [environment, outputs, inputs],
          )
        ).rows[0].count,
        '6',
      );
    },
  );
});
