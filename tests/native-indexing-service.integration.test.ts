import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { execFile, spawn, type ChildProcess } from 'node:child_process';
import { mkdir, readFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { promisify } from 'node:util';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { database } from './fixtures/catalog-storage/database.js';
import { assertNativeArtifacts } from './fixtures/native-common/artifact.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { createLocalFilesystem } from '../services/filesystem/src/local.js';
import { filesystemOperations } from '../packages/contracts/filesystem.js';
import { fingerprint } from '../packages/compiler/src/index.js';
import { IcebergIndexer } from '../services/ontology/src/indexer.js';
import { ObjectStore, type ObjectSchema } from '../services/ontology/src/store.js';
import { managedDatasetRecovery } from './fixtures/native-recovery/managed.js';
import { prepareSourceStoreProjector } from './fixtures/native-source-store/projector.js';
import { sourceStagingCollection } from './fixtures/native-source-store/staging.js';

for (const [wide, staging] of [
  [false, false],
  [true, false],
  [false, true],
] as const)
  test(
    `permanent indexing purposes refresh unrelated epochs, preserve mapped canonical rows and fail closed on own definition changes (${staging ? 'staging lifecycle' : wide ? 'maximum width' : 'small rows'})`,
    { timeout: 300000 },
    async (t) => {
      const binary = process.env.MORIA_RUNTIME_BINARY ?? '/usr/local/bin/moria-runtime',
        harness = process.env.MORIA_CATALOG_TEST_BINARY ?? '/usr/local/bin/moria-catalog-tests';
      await assertNativeArtifacts(binary, [harness]);
      const db = await database();
      const processes: ChildProcess[] = [];
      t.after(async () => {
        for (const child of processes) child.kill('SIGKILL');
        await Promise.all(
          processes
            .filter((child) => child.exitCode === null && child.signalCode === null)
            .map((child) => new Promise((resolve) => child.once('exit', resolve))),
        );
        await db.close();
      });
      const environment = randomUUID(),
        principal = 'pipeline',
        application = 'service-index',
        context = { environment, principal, application };
      await db.catalog.ensureEnvironment(environment);
      const authority = new Authority(db.catalog),
        sessions = new WorkloadSessions(authority);
      let operations = filesystemOperations.map((op) => `filesystem:${op}`);
      await authority.syncPrincipal(environment, 'fixture', principal, {
        enabled: true,
        kind: 'workload',
        attributes: {},
        markings: ['permanent/Marking/Private', 'permanent/Marking/Secret'],
      });
      await authority.setApplication(environment, 'fixture', application, operations);
      await authority.setGrants(environment, 'fixture', principal, application, operations);
      const issued = await sessions.mint(context, 'fixture'),
        session = await sessions.authenticate(`Bearer ${issued.token}`),
        hash = createHash('sha256').update(issued.token).digest('hex');
      const fs = await createLocalFilesystem(db.catalog, join(db.root, 'filesystem')),
        request = { session };
      const home = await fs.management.ensureHome(request),
        parent = await fs.filesystem.get(request, home.home.id);
      const project = await fs.management.createProject(
        request,
        parent.id,
        'service-index',
        parent.revision,
      );
      const children = await fs.filesystem.list(request, project.entry.id),
        sourceDirectory = children.entries.find((entry) => entry.name === 'source')!;
      const code = (
        await fs.filesystem.store.apply(context, randomUUID(), [
          {
            op: 'create',
            parentId: sourceDirectory.id,
            expectedParentRevision: sourceDirectory.revision,
            name: 'ontology',
            kind: 'directory',
          },
        ])
      ).entries.find((entry) => entry.name === 'ontology')!;
      const write = async (name: string, value: unknown) => {
        const bytes = new TextEncoder().encode(JSON.stringify(value)),
          current = await fs.filesystem.get(request, code.id);
        await fs.filesystem.writeFile(
          request,
          randomUUID(),
          {
            op: 'create',
            parentId: code.id,
            expectedParentRevision: current.revision,
            name,
            content: {
              digest: `sha256:${createHash('sha256').update(bytes).digest('hex')}`,
              size: bytes.length,
              mediaType: 'application/json',
            },
          },
          new Response(bytes).body!,
        );
      };
      const sourceProperty = wide ? 'v'.repeat(100) : 'value',
        population = wide ? 2 : 300,
        declaredKeys = wide ? [] : ['tenant', sourceProperty];
      const sourceColumns = {
          uid: { type: 'uid', nullable: false },
          [sourceProperty]: { type: 'string', nullable: false },
          tenant: { type: 'string', nullable: false },
        } as const,
        schema: ObjectSchema = {
          primaryKey: 'uid',
          properties: { uid: sourceColumns.uid, label: { type: 'string', nullable: false } },
        },
        mapping = { uid: 'uid', label: sourceProperty };
      await write('.ontology.json', {
        version: 1,
        name: 'Permanent native indexing',
        namespace: 'permanent',
      });
      for (const name of ['Private', 'Secret'])
        await write(`${name}.resource.json`, {
          apiVersion: 'moria/v1alpha2',
          kind: 'Marking',
          metadata: { namespace: 'permanent', name },
          spec: { eligibilityGroup: 'fixture', declassifierGroup: 'fixture' },
        });
      const projector = wide || staging ? undefined : await prepareSourceStoreProjector(write, t);
      await write('data.resource.json', {
        apiVersion: 'moria/v1alpha2',
        kind: 'Dataset',
        metadata: { namespace: 'permanent', name: 'data' },
        spec: {
          columns: sourceColumns,
          ...(wide ? {} : { primaryKey: declaredKeys }),
          writeModes: ['replace'],
        },
      });
      await write('records.object.json', {
        apiVersion: 'moria/v1alpha2',
        kind: 'ObjectType',
        metadata: { namespace: 'permanent', name: 'records' },
        spec: {
          primaryKey: 'uid',
          properties: schema.properties,
          source: { resource: 'permanent/Dataset/data', mapping },
        },
      });
      await fs.source.register(
        request,
        code.id,
        (await fs.filesystem.get(request, code.id)).revision,
        'ontology',
      );
      const checkout = await fs.source.checkout(request, code.id),
        publication = await fs.source.publish(request, code.id, checkout.digest),
        plan = await fs.source.plan(request, code.id, publication.digest);
      await fs.source.apply(request, plan.id);
      const source = publication.resources.find(
          (resource) => resource.reference === 'permanent/Dataset/data',
        )!.id,
        target = publication.resources.find(
          (resource) => resource.reference === 'permanent/ObjectType/records',
        )!.id;
      operations = [
        ...operations,
        `${source}:read`,
        `${source}:write`,
        `${target}:index`,
        `${target}:read`,
        `${target}:write`,
        ...['source-store-edit', 'source-store-delete', 'source-store-restore'].map(
          (action) => `${action}:invoke`,
        ),
      ];
      await authority.setApplication(environment, 'fixture', application, operations);
      await authority.setGrants(environment, 'fixture', principal, application, operations);
      await db.catalog.ensureDataset(environment, source);
      const role = `index_service_${randomUUID().replaceAll('-', '')}`,
        password = randomUUID();
      await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
      await db.pool.query(`ALTER ROLE ${role} SET temp_file_limit='256MB'`);
      await db.pool.query(
        `GRANT USAGE ON SCHEMA moria TO ${role};GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role};GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
      );
      const connection = `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`,
        root = join(db.root, 'objects'),
        physical = join(db.root, 'physical'),
        scratch = join(db.root, 'scratch');
      for (const path of [root, physical, scratch]) await mkdir(path);
      const namespace = { warehouseId: 'service', tableId: 'data', revision: 'v1' },
        bindings = [
          {
            namespace,
            provider: {
              kind: 'posix',
              root,
              limits: { maxObjectBytes: 128 * 1024 * 1024, chunkBytes: 1024 * 1024 },
            },
          },
        ];
      const env = {
        PATH: process.env.PATH,
        ...projector?.env,
        MORIA_INDEXING_DATABASE_SECRET_REF: 'INDEX_SERVICE_DATABASE',
        INDEX_SERVICE_DATABASE: connection,
        MORIA_INDEXING_DATABASE_TLS: 'disable',
        MORIA_NATIVE_STORAGE_BINDINGS: JSON.stringify(bindings),
        MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT: scratch,
        MORIA_NATIVE_CLOSURE_LIMITS: JSON.stringify({
          maxEntries: 10000,
          maxReferencedBytes: 1024 * 1024 * 1024,
          maxSpoolBytes: 32 * 1024 * 1024,
        }),
      };
      const native = async (fixture: Record<string, unknown>) => {
        const result = await promisify(execFile)(
          harness,
          ['--ignored', '--exact', 'catalog_publication', '--nocapture', '--test-threads=1'],
          {
            env: {
              ...env,
              MORIA_NATIVE_CATALOG_FIXTURE: JSON.stringify({
                root,
                token: issued.token,
                binding: randomUUID(),
                snapshots: [],
                ...fixture,
              }),
            },
            timeout: 120000,
            maxBuffer: 1024 * 1024,
          },
        );
        assert.match(result.stdout, /1 passed; 0 failed; 0 ignored;/);
        return result.stdout;
      };
      const lookup = join(db.root, 'uids.txt');
      await native({
        mode: 'allocate',
        allocation: {
          domain: `service-${source}`,
          cell: source,
          count: population,
          output: join(db.root, 'ids.jsonl'),
          lookupOutput: lookup,
        },
      });
      const generated = JSON.parse(
          (
            await promisify(execFile)(resolve('.venv/bin/python'), [
              'tests/fixtures/native-indexing-service/generate.py',
              physical,
              lookup,
              ...(wide ? ['--wide'] : []),
            ])
          ).stdout,
        ),
        metadata = generated.metadataName;
      const snapshot = {
        ...generated,
        metadata,
        objects: { ...generated.objects, [metadata]: generated.metadata },
        resource: source,
        warehouseId: namespace.warehouseId,
        tableId: namespace.tableId,
      };
      // Provisioned metadata uses the genuine published Dataset identity/release and the real producer's exact schema.
      await db.pool.query(
        "INSERT INTO moria.native_storage_namespaces(environment,warehouse_id,table_id,resource,revision,purpose,iceberg_uuid,markings) VALUES($1,'service','data',$2,'v1','dataset-source',$3,ARRAY['permanent/Marking/Private'])",
        [environment, source, generated.tableUuid],
      );
      await db.pool.query(
        "INSERT INTO moria.native_dataset_bindings(environment,resource,reference,revision,columns,iceberg_schema,warehouse_id,table_id,namespace_revision,physical_binding_fingerprint,source_digest,primary_key,key_schema_hash) VALUES($1,$2,'permanent/Dataset/data',$3,$4,$5,'service','data','v1',$6,$7,$8,$9)",
        [
          environment,
          source,
          fingerprint({ source, release: publication.digest, schema: generated.icebergSchema }),
          sourceColumns,
          generated.icebergSchema,
          fingerprint({ backend: 'local', root }),
          publication.digest,
          declaredKeys,
          fingerprint({
            version: 1,
            keys: declaredKeys.map((name) => ({ name, descriptor: sourceColumns[name] })),
          }),
        ],
      );
      const serving = {
        objectTypeId: target,
        schema,
        policies: {
          resource: target,
          markings: ['permanent/Marking/Private'],
          properties: schema.properties,
          sourceColumns,
          attributes: {},
          row: { op: 'eq', args: [{ field: 'tenant' }, { literal: 'a' }] },
          objectRead: { literal: true },
        },
      };
      await db.pool.query(
        'INSERT INTO moria.native_serving_bindings(environment,resource,object_type_id,revision,definition) VALUES($1,$2,$2::text::uuid,$3,$4)',
        [environment, target, fingerprint(serving), serving],
      );
      for (const [resource, edits] of [
        ['source-store-edit', [{ op: 'set', property: 'label', value: { parameter: 'label' } }]],
        ['source-store-delete', [{ op: 'delete' }]],
        ['source-store-restore', [{ op: 'restore' }]],
      ] as const) {
        await db.pool.query(
          'INSERT INTO moria.native_serving_actions(environment,resource,object_resource,revision,definition) VALUES($1,$2,$3,$4,$5)',
          [
            environment,
            resource,
            target,
            'v1',
            {
              resource,
              revision: 'v1',
              markings: ['permanent/Marking/Private'],
              targetParameter: 'uid',
              inputs: {
                uid: sourceColumns.uid,
                ...(resource === 'source-store-edit' ? { label: schema.properties.label } : {}),
              },
              condition: { literal: true },
              edits,
            },
          ],
        );
      }
      const projection = {
        version: 1,
        sourceFields: ['uid', sourceProperty, 'tenant'],
        effectiveFields: ['uid', 'label'],
        layouts: [
          { name: 'uid', sort: [], keyBytes: 256 },
          { name: 'label', sort: [{ property: 'label', direction: 'asc' }], keyBytes: 1024 },
        ],
        readerRoles: [role],
        budgets: {
          rowBytes: wide ? 16 * 1024 * 1024 : 65536,
          batchRows: wide ? 1 : 30,
          batchBytes: wide ? 32 * 1024 * 1024 : 1048576,
          commitReplayRows: 100,
          commitReplayBytes: wide ? 16 * 1024 * 1024 : 1048576,
          ddlTimeMs: 30000,
        },
      };
      await db.pool.query(
        "INSERT INTO moria.runtime_projection_bindings(environment,resource,revision,definition) VALUES($1,$2,'p1',$3)",
        [environment, target, projection],
      );
      const indexer = new IcebergIndexer(new ObjectStore(db.catalog), authority, {
        source: { kind: 'dataset', resource: source, markings: ['permanent/Marking/Private'] },
        target: { resource: target, markings: ['permanent/Marking/Private'], schema },
      });
      await indexer.registerDistributed(
        { ...context, sessionHash: hash },
        { targetId: target, warehouse: root, partitions: 1, timeoutSeconds: 240 },
      );
      // Trusted compiled mapping metadata enters the compatibility registrar before native promotion.
      const old = (
        await db.pool.query(
          'SELECT * FROM moria.indexing_bindings WHERE environment=$1 AND target=$2',
          [environment, target],
        )
      ).rows[0];
      old.definition.sourceColumns = sourceColumns;
      old.definition.sourceMapping = mapping;
      old.revision = fingerprint({
        definition: old.definition,
        context: old.context,
        epoch: old.security_epoch,
        instanceEpoch: old.instance_epoch,
      });
      await db.pool.query(
        'UPDATE moria.indexing_bindings SET definition=$3,revision=$4 WHERE environment=$1 AND target=$2',
        [environment, target, old.definition, old.revision],
      );
      const service = randomUUID();
      await db.pool.query(
        "INSERT INTO moria.runtime_service_authorities(id,environment,principal,application,operations,markings,domains,resource_classes,incarnation) SELECT $1,$2,$3,$4,$5,ARRAY['permanent/Marking/Private','permanent/Marking/Secret'],ARRAY['indexing'],ARRAY['standard'],incarnation FROM moria.indexing_instance",
        [
          service,
          environment,
          principal,
          application,
          [`${source}:read`, `${target}:index`, 'filesystem:read'],
        ],
      );
      await db.pool.query(
        "INSERT INTO moria.runtime_service_targets(environment,resource,domain,binding_revision,resource_class,authority) VALUES($1,$2,'indexing','service-v1','standard',$3)",
        [environment, target, service],
      );
      const registration = JSON.parse(
        (
          await promisify(execFile)(
            binary,
            [
              'indexing-service',
              'register-existing',
              JSON.stringify({
                environment,
                target,
                expectedRevision: old.revision,
                serviceTargetRevision: 'service-v1',
              }),
            ],
            { env, timeout: 30000 },
          )
        ).stdout.trim(),
      );
      const server = spawn(binary, ['serve'], {
        env: { ...env, MORIA_NATIVE_LISTEN: '127.0.0.1:0' },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      processes.push(server);
      let serverOutput = '',
        serverError = '',
        base = '';
      server.stdout!.on('data', (b) => (serverOutput += b.toString()));
      server.stderr!.on('data', (b) => (serverError += b.toString()));
      for (let i = 0; i < 500 && !base; i++) {
        for (const line of serverOutput.split('\n')) {
          try {
            const item = JSON.parse(line);
            if (item.event === 'serving') base = `http://${item.address}`;
          } catch {}
        }
        if (server.exitCode !== null) throw new Error(serverError);
        if (!base) await delay(10);
      }
      assert.ok(base, serverError);
      const queryCount = async () => {
        let cursor: string | undefined,
          count = 0;
        do {
          const response = await fetch(`${base}/api/v1/objects/${target}/query`, {
            method: 'POST',
            headers: {
              authorization: `Bearer ${issued.token}`,
              'content-type': 'application/json',
            },
            body: JSON.stringify({
              projection: ['uid', 'label'],
              sort: [{ property: 'label', direction: 'asc' }],
              pageSize: wide ? 1 : 37,
              ...(cursor ? { cursor } : {}),
            }),
          });
          const page = await response.json();
          assert.equal(response.status, 200, JSON.stringify(page));
          count += page.objects.length;
          cursor = page.nextCursor;
        } while (cursor);
        return count;
      };
      assert.equal(
        (
          await db.pool.query(
            'SELECT count(*)::text FROM moria.runtime_executions WHERE binding=$1',
            [registration.metadataBinding],
          )
        ).rows[0].count,
        '0',
      );
      let producerMarkings = ['permanent/Marking/Private'];
      const publish = async (replacementRows?: Record<string, unknown>[]) => {
        const head = (await db.catalog.heads(environment, [source]))[0],
          state = await authority.current(environment, principal, application),
          spec = {
            version: 1,
            domain: 'sync',
            release: 'a'.repeat(64),
            resourceClass: 'standard',
            timeoutSeconds: 240,
            retryPolicy: 'idempotent',
            inputSchema: { type: 'object', properties: {} },
            pinnedInputs: [],
            expectedHeads: { [source]: head.revision },
          };
        const binding = (
          await db.pool.query(
            'INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation) SELECT $1,$2,$3,$4,$5,$7,$6,s.security_epoch,i.incarnation FROM moria.instance_state s,moria.indexing_instance i RETURNING id',
            [
              environment,
              source,
              spec,
              { ...context, sessionHash: hash },
              [`${source}:read`, `${source}:write`, 'filesystem:read'],
              state.epoch,
              producerMarkings,
            ],
          )
        ).rows[0].id;
        await native({
          mode: replacementRows ? 'writer-supplied' : 'valid',
          binding,
          snapshots: [{ ...snapshot, revision: namespace.revision }],
          ...(replacementRows ? { writerRows: { [source]: replacementRows } } : {}),
        });
        return (await db.catalog.heads(environment, [source]))[0];
      };
      let acceptedBuild = '',
        acceptedRevision = '';
      const sourceAlias = async (stage: string) =>
        (
          await db.pool.query(
            'SELECT s.*,a.revision::text AS alias_revision,a.indexed_dataset_revision::text,a.indexed_snapshot,a.indexed_root FROM moria.native_source_row_stores s JOIN moria.native_source_generation_aliases a ON a.store=s.id WHERE s.id=$1',
            [stage],
          )
        ).rows[0];
      await t.test(
        'native registration accepts a genuine mapped source, retains raw policy columns and performs no generic indexing execution',
        async () => {
          await publish();
          const indexed = await native({ mode: 'index' });
          const batches = indexed.split('\n').flatMap((line) => {
            try {
              const value = JSON.parse(line);
              return value.event === 'reducer_batches' ? [value] : [];
            } catch {
              return [];
            }
          });
          assert.equal(batches.length, 1);
          assert.ok(batches.every((item) => Number(item.maxWireBytes) <= 16 * 1024 * 1024));
          if (wide) {
            assert.ok(Number(batches[0].maxWireBytes) > 8 * 1024 * 1024);
            assert.equal(batches[0].batches, '2');
          }
          t.diagnostic(
            JSON.stringify({
              profile: wide ? 'mapped-maximum-width' : 'mapped-small',
              reducerBatches: batches,
            }),
          );
          const rows = (
            await db.pool.query(
              'SELECT r.value,r.source_value FROM moria.staged_source_rows r JOIN moria.source_generations g ON g.stage=r.stage WHERE g.environment=$1 AND g.object_type=$2',
              [environment, target],
            )
          ).rows;
          assert.equal(rows.length, population);
          assert.equal(await queryCount(), population / 2);
          if (wide) {
            const actual = (
              await db.pool.query(
                'SELECT max(octet_length(r.value::text)+octet_length(r.source_value::text))::text AS bytes FROM moria.staged_source_rows r JOIN moria.source_generations g ON g.stage=r.stage WHERE g.environment=$1 AND g.object_type=$2',
                [environment, target],
              )
            ).rows[0].bytes;
            assert.ok(
              Number(actual) > 8 * 1024 * 1024 && Number(actual) <= 16 * 1024 * 1024,
              actual,
            );
          }
          assert.ok(
            rows.every(
              (row) =>
                Object.keys(row.value).sort().join(',') === 'label,uid' &&
                row.source_value.tenant &&
                row.value.label === row.source_value[sourceProperty],
            ),
          );
          const projection = (
            await db.pool.query(
              "SELECT moria.runtime_projection_relation(id) AS relation,source_schema FROM moria.runtime_projections WHERE environment=$1 AND status='active'",
              [environment],
            )
          ).rows[0];
          assert.deepEqual(projection.source_schema, sourceColumns);
          assert.match(projection.relation, /^moria\.runtime_projection_[0-9a-f]{32}$/);
          assert.equal(
            (
              await db.pool.query(
                `SELECT count(*)::text FROM ${projection.relation} WHERE source->>'tenant'='a'`,
              )
            ).rows[0].count,
            String(population / 2),
          );
          acceptedBuild = (
            await db.pool.query(
              "SELECT id FROM moria.indexing_builds WHERE environment=$1 AND status='active'",
              [environment],
            )
          ).rows[0].id;
          const alias = await sourceAlias(acceptedBuild);
          assert.equal(alias.status, 'published');
          assert.equal(alias.row_count, String(population));
          assert.equal(alias.key_count, wide ? '0' : String(population));
          assert.deepEqual(alias.ordered_keys, declaredKeys);
          assert.equal(alias.id, acceptedBuild);
          acceptedRevision = alias.alias_revision;
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*)::text FROM moria.native_source_row_stores WHERE environment=$1',
                [environment],
              )
            ).rows[0].count,
            '1',
          );
          const map = (
            await db.pool.query(
              "SELECT encode(m.key_hash,'hex') AS hash,convert_from(m.encoded_key,'UTF8')::jsonb AS key,m.uid,r.source_value FROM moria.native_source_key_map m JOIN moria.staged_source_rows r ON r.stage=m.store AND r.uid=m.uid WHERE m.store=$1",
              [acceptedBuild],
            )
          ).rows;
          assert.equal(map.length, wide ? 0 : population);
          assert.ok(
            map.every(
              (row) =>
                JSON.stringify(row.key) ===
                  JSON.stringify(declaredKeys.map((name) => row.source_value[name])) &&
                row.uid === row.source_value.uid &&
                row.hash === createHash('sha256').update(JSON.stringify(row.key)).digest('hex'),
            ),
          );
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*)::text FROM moria.runtime_object_base($1,$2,$3)',
                [environment, target, acceptedRevision],
              )
            ).rows[0].count,
            String(population),
          );
          await assert.rejects(
            db.pool.query('SELECT moria.runtime_source_store_open($1,$2,1)', [
              acceptedBuild,
              randomUUID(),
            ]),
            /LEASE_LOST|INDEXING_DELEGATION_INACTIVE|BUILD_NOT_ACTIVE/,
          );
          await assert.rejects(
            db.pool.query(
              'UPDATE moria.native_source_generation_aliases SET source_version=source_version WHERE store=$1',
              [acceptedBuild],
            ),
            /IMMUTABLE_SOURCE_ALIAS/,
          );
          const restricted = await db.pool.connect();
          try {
            await restricted.query('BEGIN');
            await restricted.query(`SET LOCAL ROLE ${role}`);
            await assert.rejects(
              restricted.query(
                'UPDATE moria.native_source_row_stores SET owner=owner WHERE id=$1',
                [acceptedBuild],
              ),
              /SOURCE_STORE_AUTHORITY_REQUIRED/,
            );
          } finally {
            await restricted.query('ROLLBACK');
            restricted.release();
          }
          const forged = await db.pool.connect();
          try {
            await forged.query('BEGIN');
            await assert.rejects(
              forged.query(
                'INSERT INTO moria.source_generations(environment,object_type,revision,stage,object_schema,required_markings) SELECT environment,object_type,revision+1000,stage,object_schema,required_markings FROM moria.source_generations WHERE environment=$1 AND object_type=$2 AND revision=$3',
                [environment, target, acceptedRevision],
              ),
              /CHANGE_REVISION_INVALID/,
            );
          } finally {
            await forged.query('ROLLBACK');
            forged.release();
          }
          assert.equal((await sourceAlias(acceptedBuild)).alias_revision, acceptedRevision);
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*)::text FROM moria.native_source_generation_aliases WHERE environment=$1',
                [environment],
              )
            ).rows[0].count,
            '1',
          );
          await assert.rejects(
            db.pool.query('SELECT moria.runtime_execution_request($1,$2,$3,$4,$5)', [
              registration.metadataBinding,
              randomUUID(),
              {},
              'a'.repeat(64),
              JSON.stringify([{}]),
            ]),
            /INDEXING_TASK_AUTHORITY_REQUIRED/,
          );
        },
      );
      if (staging) {
        await t.test(
          'real closed staging collection preserves published aliases and drains expired private rows through native history once',
          async () => {
            await sourceStagingCollection({
              db,
              runtime: binary,
              env,
              environment,
              target,
              source,
              publishedBuild: acceptedBuild,
              publish,
              queryCount,
            });
          },
        );
        return;
      }
      if (!wide)
        await t.test(
          'clean-host recovery rebinds managed typed Iceberg and retains prior snapshots after another real native publication',
          async () => {
            const rows = (
              await db.pool.query(
                'SELECT r.source_value FROM moria.staged_source_rows r JOIN moria.source_generations g ON g.stage=r.stage WHERE g.environment=$1 AND g.object_type=$2 ORDER BY r.uid',
                [environment, target],
              )
            ).rows.map((row) => row.source_value);
            await managedDatasetRecovery({
              db,
              runtime: binary,
              harness,
              env,
              connection,
              environment,
              principal,
              application,
              operations,
              source,
              snapshot,
              bindings,
              rows,
              markings: ['permanent/Marking/Private', 'permanent/Marking/Secret'],
              sourceStore: { target, stage: acceptedBuild, revision: acceptedRevision },
            });
          },
        );
      await t.test(
        'unrelated environment and instance changes authorize fresh metadata for the next head without replacing old proofs',
        async () => {
          const before = (
              await db.pool.query(
                'SELECT context,revision FROM moria.indexing_bindings WHERE environment=$1 AND target=$2',
                [environment, target],
              )
            ).rows[0],
            proof = (
              await db.pool.query(
                'SELECT proof FROM moria.runtime_indexing_delegations WHERE build=$1',
                [acceptedBuild],
              )
            ).rows[0].proof;
          await authority.syncPrincipal(environment, 'fixture', 'unrelated', {
            enabled: true,
            kind: 'workload',
            attributes: {},
            markings: [],
          });
          await db.pool.query('UPDATE moria.instance_state SET security_epoch=security_epoch+1');
          await publish();
          const after = (
            await db.pool.query(
              'SELECT context,revision FROM moria.indexing_bindings WHERE environment=$1 AND target=$2',
              [environment, target],
            )
          ).rows[0];
          assert.notEqual(after.revision, before.revision);
          assert.notEqual(after.context.executionBinding, before.context.executionBinding);
          assert.equal(after.context.sessionHash, undefined);
          assert.deepEqual(
            (
              await db.pool.query(
                'SELECT proof FROM moria.runtime_indexing_delegations WHERE build=$1',
                [acceptedBuild],
              )
            ).rows[0].proof,
            proof,
          );
          await assert.rejects(
            db.pool.query('SELECT moria.indexing_check($1)', [acceptedBuild]),
            /AUTHORIZATION_CHANGED|INDEXING_DELEGATION_INACTIVE/,
          );
          await native({ mode: 'index' });
          const aliases = (
            await db.pool.query(
              'SELECT a.store,a.revision::text,moria.runtime_source_stage(a.store,a.environment,a.object_type,a.revision) AS actual FROM moria.native_source_generation_aliases a WHERE a.environment=$1 ORDER BY a.revision',
              [environment],
            )
          ).rows;
          assert.equal(aliases.length, 2);
          assert.equal(aliases[0].store, acceptedBuild);
          assert.equal(aliases[0].revision, acceptedRevision);
          assert.ok(aliases.every((alias) => alias.store === alias.actual));
          assert.notEqual(aliases[0].store, aliases[1].store);
          for (const alias of aliases)
            assert.equal(
              (
                await db.pool.query(
                  'SELECT count(*)::text FROM moria.runtime_object_base($1,$2,$3)',
                  [environment, target, alias.revision],
                )
              ).rows[0].count,
              String(population),
            );
          assert.equal((await sourceAlias(acceptedBuild)).status, 'published');
          assert.equal(
            (
              await db.pool.query(
                "SELECT count(*)::text FROM moria.indexing_builds WHERE environment=$1 AND status='active'",
                [environment],
              )
            ).rows[0].count,
            '2',
          );
        },
      );
      if (!wide)
        await t.test(
          'new Source aliases preserve real Action overlays and tombstones across replacement and historical reads',
          async () => {
            const selected = (
              await db.pool.query(
                "SELECT uid,value FROM moria.runtime_object_base($1,$2,$3) WHERE source_value->>'tenant'='a' ORDER BY uid LIMIT 2",
                [environment, target, acceptedRevision],
              )
            ).rows;
            assert.equal(selected.length, 2);
            const invoke = async (action: string, input: unknown) => {
              const response = await fetch(`${base}/api/v1/actions/${action}/invoke`, {
                method: 'POST',
                headers: {
                  authorization: `Bearer ${issued.token}`,
                  'content-type': 'application/json',
                  'idempotency-key': randomUUID(),
                },
                body: JSON.stringify({ input }),
              });
              const receipt = await response.json();
              assert.equal(response.status, 200, JSON.stringify(receipt));
              return receipt;
            };
            const read = async (uid: string) => {
              const response = await fetch(`${base}/api/v1/objects/${target}/${uid}`, {
                headers: { authorization: `Bearer ${issued.token}` },
              });
              return { status: response.status, value: await response.json() };
            };
            await invoke('source-store-edit', {
              uid: selected[0].uid,
              label: 'source-store overlay',
            });
            await invoke('source-store-delete', { uid: selected[1].uid });
            assert.equal(await queryCount(), population / 2 - 1);
            const rawRows = (
              await db.pool.query(
                'SELECT source_value FROM moria.staged_source_rows WHERE stage=$1 ORDER BY uid',
                [acceptedBuild],
              )
            ).rows.map((row) => row.source_value as Record<string, unknown>);
            await publish(rawRows.filter((row) => row.uid !== selected[1].uid));
            await native({ mode: 'index' });
            const missing = (
              await db.pool.query(
                'SELECT g.stage,g.revision::text FROM moria.source_generations g WHERE environment=$1 AND object_type=$2 ORDER BY g.revision DESC LIMIT 1',
                [environment, target],
              )
            ).rows[0];
            assert.equal((await sourceAlias(missing.stage)).key_count, String(population - 1));
            assert.equal(
              (
                await db.pool.query(
                  'SELECT count(*)::text FROM moria.runtime_object_base($1,$2,$3) WHERE uid=$4',
                  [environment, target, missing.revision, selected[1].uid],
                )
              ).rows[0].count,
              '0',
            );
            assert.equal((await read(selected[1].uid)).status, 404);
            await publish(rawRows);
            await native({ mode: 'index' });
            const live = await read(selected[0].uid),
              deleted = await read(selected[1].uid);
            assert.equal(live.status, 200, JSON.stringify(live));
            assert.equal(live.value.object.label, 'source-store overlay');
            assert.equal(deleted.status, 404, JSON.stringify(deleted));
            assert.equal(await queryCount(), population / 2 - 1);
            const original = (
              await db.pool.query(
                'SELECT value FROM moria.runtime_object_base($1,$2,$3) WHERE uid=$4',
                [environment, target, acceptedRevision, selected[0].uid],
              )
            ).rows[0].value;
            assert.deepEqual(original, selected[0].value);
            const store = new ObjectStore(db.catalog),
              c = await db.pool.connect();
            try {
              const old = await store.read(
                c,
                environment,
                target,
                selected[1].uid,
                acceptedRevision,
              );
              assert.ok(old);
              assert.deepEqual(old.effective, selected[1].value);
            } finally {
              c.release();
            }
            const aliases = (
              await db.pool.query(
                'SELECT a.store,a.revision::text FROM moria.native_source_generation_aliases a WHERE a.environment=$1 AND a.object_type=$2 ORDER BY a.revision',
                [environment, target],
              )
            ).rows;
            assert.equal(aliases.length, 4);
            assert.ok(aliases.every((alias) => alias.store !== undefined));
            assert.equal((await sourceAlias(acceptedBuild)).row_count, String(population));
            await assert.rejects(
              db.pool.query('SELECT moria.runtime_source_stage($1,$2,$3,$4)', [
                acceptedBuild,
                randomUUID(),
                target,
                acceptedRevision,
              ]),
              /SOURCE_ALIAS_REQUIRED/,
            );
            const currentAlias = aliases[aliases.length - 1];
            for (let i = 1; i < aliases.length; i++)
              assert.ok(BigInt(aliases[i].revision) > BigInt(aliases[i - 1].revision));
            const currentSource = (
              await db.pool.query(
                'SELECT g.stage,g.revision::text FROM moria.source_generations g WHERE g.environment=$1 AND g.object_type=$2 ORDER BY g.revision DESC LIMIT 1',
                [environment, target],
              )
            ).rows[0];
            assert.deepEqual(
              currentAlias,
              { store: currentSource.stage, revision: currentSource.revision },
              JSON.stringify({ aliases, currentSource }),
            );
            const projectionProof = await projector!.verify({
              db,
              authority,
              environment,
              principal,
              application,
              target,
              source,
              model: publication.resources.find(
                (resource) => resource.reference === 'permanent/Model/SourceEmbedding',
              )!.id,
              token: issued.token,
              base,
              runtime: binary,
              env,
              expectedRows: population - 1,
              deletedUid: selected[1].uid,
              overlayUid: selected[0].uid,
              store: currentAlias.store,
            });
            t.diagnostic(
              JSON.stringify({
                fullObjectSource: projectionProof,
                ordinaryRows: population / 2 - 1,
              }),
            );
            await invoke('source-store-restore', { uid: selected[1].uid });
            await invoke('source-store-edit', {
              uid: selected[0].uid,
              label: selected[0].value.label,
            });
            assert.equal(await queryCount(), population / 2);
          },
        );
      await t.test(
        'a changed own serving definition records blocked lag without metadata refresh or work acceptance',
        async () => {
          const before = (
            await db.pool.query(
              'SELECT revision FROM moria.indexing_bindings WHERE environment=$1 AND target=$2',
              [environment, target],
            )
          ).rows[0].revision;
          const changed = {
            ...serving,
            policies: { ...serving.policies, row: { literal: false } },
          };
          await db.pool.query(
            'UPDATE moria.native_serving_bindings SET definition=$3,revision=$4 WHERE environment=$1 AND resource=$2',
            [environment, target, changed, fingerprint(changed)],
          );
          const head = await publish();
          assert.equal(await queryCount(), 0);
          assert.equal(
            (
              await db.pool.query(
                'SELECT revision FROM moria.indexing_bindings WHERE environment=$1 AND target=$2',
                [environment, target],
              )
            ).rows[0].revision,
            before,
          );
          assert.equal(
            (
              await db.pool.query(
                'SELECT error FROM moria.indexing_blocked_intents WHERE environment=$1 AND source_revision=$2',
                [environment, head.revision],
              )
            ).rows[0].error,
            'INDEXING_AUTHORITY_CHANGED',
          );
        },
      );
      await t.test(
        'broader current policy retains the old generation restriction until a newly captured native build activates',
        async () => {
          const broader = { ...serving, policies: { ...serving.policies, row: { literal: true } } };
          await db.pool.query(
            'UPDATE moria.native_serving_bindings SET definition=$3,revision=$4 WHERE environment=$1 AND resource=$2',
            [environment, target, broader, fingerprint(broader)],
          );
          assert.equal(await queryCount(), population / 2);
          const current = (
            await db.pool.query(
              'SELECT revision FROM moria.indexing_bindings WHERE environment=$1 AND target=$2',
              [environment, target],
            )
          ).rows[0].revision;
          await promisify(execFile)(
            binary,
            [
              'indexing-service',
              'register-existing',
              JSON.stringify({
                environment,
                target,
                expectedRevision: current,
                serviceTargetRevision: 'service-v1',
              }),
            ],
            { env, timeout: 30000 },
          );
          assert.equal(await queryCount(), population / 2);
          await native({ mode: 'index' });
          assert.equal(await queryCount(), population);
        },
      );
      await t.test(
        'a newly classified Dataset saves its valid head while the older explicit index floor remains blocked',
        async () => {
          const before = (
            await db.pool.query(
              'SELECT revision FROM moria.indexing_bindings WHERE environment=$1 AND target=$2',
              [environment, target],
            )
          ).rows[0].revision;
          await db.pool.query(
            "UPDATE moria.native_storage_namespaces SET revision='v2',markings=ARRAY['permanent/Marking/Private','permanent/Marking/Secret'] WHERE warehouse_id='service' AND table_id='data'",
          );
          await db.pool.query(
            "UPDATE moria.native_dataset_bindings SET namespace_revision='v2',revision=$3 WHERE environment=$1 AND resource=$2",
            [
              environment,
              source,
              fingerprint({
                source,
                release: publication.digest,
                classification: ['permanent/Marking/Private', 'permanent/Marking/Secret'],
              }),
            ],
          );
          namespace.revision = 'v2';
          env.MORIA_NATIVE_STORAGE_BINDINGS = JSON.stringify(bindings);
          producerMarkings = ['permanent/Marking/Private', 'permanent/Marking/Secret'];
          const head = await publish();
          assert.equal(
            (
              await db.pool.query(
                'SELECT revision FROM moria.indexing_bindings WHERE environment=$1 AND target=$2',
                [environment, target],
              )
            ).rows[0].revision,
            before,
          );
          assert.equal(
            (
              await db.pool.query(
                'SELECT error FROM moria.indexing_blocked_intents WHERE environment=$1 AND source_revision=$2',
                [environment, head.revision],
              )
            ).rows[0].error,
            'INDEXING_AUTHORITY_CHANGED',
          );
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*)::text FROM moria.indexing_builds WHERE environment=$1 AND source_revision=$2',
                [environment, head.revision],
              )
            ).rows[0].count,
            '0',
          );
        },
      );
      await t.test(
        'disabled registered purpose remains blocked even after further unrelated epoch changes',
        async () => {
          await db.pool.query(
            'UPDATE moria.runtime_service_targets SET enabled=false WHERE authority=$1',
            [service],
          );
          await db.pool.query(
            'UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id=$1',
            [environment],
          );
          const head = await publish();
          assert.equal(
            (
              await db.pool.query(
                'SELECT error FROM moria.indexing_blocked_intents WHERE environment=$1 AND source_revision=$2',
                [environment, head.revision],
              )
            ).rows[0].error,
            'SERVICE_PURPOSE_REVOKED',
          );
        },
      );
      if (!wide)
        await t.test(
          'approved bounded history collection retires old Source payload and exact key maps while retaining Search and current stores',
          async () => {
            const before = (
              await db.pool.query(
                "SELECT id,key_count::text FROM moria.native_source_row_stores WHERE environment=$1 AND status='published'",
                [environment],
              )
            ).rows;
            assert.ok(before.length >= 5);
            const protectedStore = (
              await db.pool.query(
                "SELECT r.source_stage FROM moria.native_search_rebuilds r JOIN moria.native_search_generations g ON g.id=r.generation WHERE g.environment=$1 AND g.status='active'",
                [environment],
              )
            ).rows[0].source_stage;
            const protectedKeys = (await sourceAlias(protectedStore)).key_count;
            // Only fixture-owned timestamps are advanced; live structural Search pins remain intact.
            await db.pool.query(
              "UPDATE moria.read_cursors SET expires_at=clock_timestamp()-interval '1 second' WHERE environment=$1",
              [environment],
            );
            await db.pool.query(
              "UPDATE moria.runtime_history_revisions SET observed_at=clock_timestamp()-interval '3 days' WHERE environment=$1",
              [environment],
            );
            const policy = (
              await db.pool.query('SELECT moria.runtime_history_approve($1,$2,86400,2) AS id', [
                environment,
                'source-history-' + environment,
              ])
            ).rows[0].id;
            const collection = (
              await db.pool.query('SELECT moria.runtime_history_plan($1) AS id', [policy])
            ).rows[0].id;
            assert.ok(
              BigInt(
                (
                  await db.pool.query(
                    'SELECT floor::text FROM moria.runtime_history_collections WHERE id=$1',
                    [collection],
                  )
                ).rows[0].floor,
              ) > BigInt(acceptedRevision),
            );
            assert.equal(
              (await db.pool.query('SELECT moria.runtime_history_step($1) AS value', [policy]))
                .rows[0].value.state,
              'grace',
            );
            await db.pool.query(
              "UPDATE moria.runtime_history_collections SET observed_at=clock_timestamp()-interval '3 days',not_before=clock_timestamp()-interval '1 day' WHERE id=$1",
              [collection],
            );
            let result: any;
            for (let i = 0; i < 100; i++) {
              result = (
                await db.pool.query('SELECT moria.runtime_history_step($1) AS value', [policy])
              ).rows[0].value;
              if (result.state === 'complete') break;
            }
            assert.equal(result.state, 'complete', JSON.stringify(result));
            assert.equal(
              (
                await db.pool.query(
                  'SELECT status FROM moria.native_source_row_stores WHERE id=$1',
                  [acceptedBuild],
                )
              ).rows[0].status,
              'retired',
            );
            assert.equal(
              (
                await db.pool.query(
                  'SELECT count(*)::text FROM moria.native_source_key_map WHERE store=$1',
                  [acceptedBuild],
                )
              ).rows[0].count,
              '0',
            );
            assert.equal(
              (
                await db.pool.query(
                  'SELECT count(*)::text FROM moria.staged_source_rows WHERE stage=$1',
                  [acceptedBuild],
                )
              ).rows[0].count,
              '0',
            );
            await assert.rejects(
              db.pool.query('SELECT moria.runtime_source_stage($1,$2,$3,$4)', [
                acceptedBuild,
                environment,
                target,
                acceptedRevision,
              ]),
              /SOURCE_ALIAS_REQUIRED/,
            );
            assert.equal((await sourceAlias(protectedStore)).status, 'published');
            assert.equal((await sourceAlias(protectedStore)).key_count, protectedKeys);
            assert.equal(
              (
                await db.pool.query(
                  'SELECT count(*)::text FROM moria.native_source_key_map WHERE store=$1',
                  [protectedStore],
                )
              ).rows[0].count,
              protectedKeys,
            );
            assert.equal(await queryCount(), population);
          },
        );
      assert.equal((await readFile(lookup, 'utf8')).trim().split('\n').length, population);
    },
  );
