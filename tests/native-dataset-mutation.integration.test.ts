import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';
import test from 'node:test';
import type { Resource } from '../packages/contracts/types.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { engineImage } from './fixtures/data-engine/image.js';
import { startInstanceEngine } from './fixtures/native-instance/engine.js';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';
import { publishNativeSource } from './fixtures/native-source/setup.js';

const output = 'retention/Dataset/records';
const program = `import pyarrow as pa, uuid
def sync(ctx):
 response=ctx.http('GET','/rows')
 assert response['status']==200
 body=response['body']
 rows=body['rows']
 tags=pa.array([row['tags'] for row in rows],type=pa.list_(pa.field('element',pa.string(),nullable=False)))
 if body['keysOnly']:
  table=pa.table({'tags':tags})
 else:
  table=pa.table({'id':pa.array([uuid.UUID(row['id']).bytes for row in rows],type=pa.uuid()),'tags':tags,'note':pa.array([row['note'] for row in rows],type=pa.string())})
 ctx.write('retention/Dataset/records',table)
 ctx.checkpoint(body['cursor'])
`;

const mutationBudget = {
  maxInputRows: 100,
  maxInputBytes: 1048576,
  maxChangedRows: 100,
  maxChangedLogicalBytes: 1048576,
  maxTouchedFiles: 10,
  maxCompactionRows: 1000,
  maxCompactionBytes: 1048576,
};
type Row = { id: string; tags: string[]; note: string | null };

test(
  'actual Connector sparse publication remains readable as standard Iceberg',
  { timeout: 300000 },
  async (t) => {
    const modes = ['append', 'upsert', 'delete'] as const;
    const fixture = await managedRetentionDataset({
      primaryKey: ['tags'],
      writeModes: ['replace', ...modes],
      columns: {
        id: { type: 'uid', nullable: false },
        tags: { type: 'array', nullable: false, items: { type: 'string', nullable: false } },
        note: { type: 'string', nullable: true },
      },
    });
    const { db, environment, principal, dataset } = fixture;
    const material = randomUUID();
    let engine: Awaited<ReturnType<typeof startInstanceEngine>> | undefined;
    let image: Awaited<ReturnType<typeof engineImage>> | undefined;
    let calls = 0;
    let offered: { rows: Array<Pick<Row, 'tags'> | Row>; keysOnly: boolean } = {
      rows: [],
      keysOnly: false,
    };
    const provider = createServer((request, response) => {
      if (
        request.method !== 'GET' ||
        request.url !== '/rows' ||
        request.headers.authorization !== 'Bearer ' + material
      ) {
        response.writeHead(403).end();
        return;
      }
      response.setHeader('content-type', 'application/json');
      response.end(JSON.stringify({ ...offered, cursor: { offset: ++calls } }));
    });
    t.after(async () => {
      const cleanup = [
        async () => {
          if (engine) await engine.close();
        },
        async () => {
          if (engine) await engine.restoreOwnership(db.root);
        },
        async () => {
          if (!provider.listening) return;
          provider.closeAllConnections();
          await new Promise<void>((done) => provider.close(() => done()));
        },
        async () => {
          if (image) await image.close();
        },
        fixture.close,
      ];
      const errors = [];
      for (const close of cleanup)
        try {
          await close();
        } catch (error) {
          errors.push(error);
        }
      assert.deepEqual(errors, []);
    });

    const one: Row = { id: randomUUID(), tags: ['a', 'a', 'b'], note: null };
    const two: Row = { id: randomUUID(), tags: ['a', 'b', 'a'], note: 'preserve order' };
    const three: Row = { id: randomUUID(), tags: ['c'], note: 'appended' };
    const four: Row = { id: randomUUID(), tags: ['d'], note: null };
    const edited: Row = { ...one, note: 'updated' };
    const first = await fixture.publish([one, two], { graph: true });
    image = await engineImage(program);
    await new Promise<void>((done, fail) => {
      provider.once('error', fail);
      provider.listen(0, '127.0.0.1', done);
    });
    const reference = (mode: (typeof modes)[number]) => 'mutation/Connection/' + mode;
    const implementation: Resource = {
      apiVersion: 'moria/v1alpha2',
      kind: 'ConnectorImplementation',
      metadata: { namespace: 'mutation', name: 'Batch' },
      spec: {
        artifact: image.artifact,
        abiVersion: 'connector/v1',
        capabilities: ['batchRead'],
        configurationSchema: { type: 'object', additionalProperties: false },
      },
    };
    const connections: Resource[] = modes.map((mode) => ({
      apiVersion: 'moria/v1alpha2',
      kind: 'Connection',
      metadata: { namespace: 'mutation', name: mode },
      spec: {
        implementation: 'mutation/ConnectorImplementation/Batch',
        configuration: {},
        secrets: ['mutation/SecretReference/Credential'],
        output,
        mode,
        coverage: 'incremental',
        stream: 'rows',
      },
    }));
    const secret: Resource = {
      apiVersion: 'moria/v1alpha2',
      kind: 'SecretReference',
      metadata: { namespace: 'mutation', name: 'Credential' },
      spec: { binding: 'mutation-credential', consumers: modes.map(reference) },
    };
    const published = await publishNativeSource(
      fixture.baseUrl,
      fixture.token,
      [
        {
          path: '.repository.json',
          text: JSON.stringify({
            version: 1,
            name: 'Sparse mutations',
            namespace: 'mutation',
            runtime: 'python-duckdb-v1',
            dependencies: [
              { rootId: fixture.published.root.id, digest: fixture.published.publication.digest },
            ],
          }),
        },
        { path: 'main.py', text: program },
        ...[...connections, secret].map((value) => ({
          path: value.metadata.name + '.resource.json',
          text: JSON.stringify(value),
        })),
      ],
      'repository',
      [implementation],
    );
    await published.client.apply(
      (await published.client.plan(published.root.id, published.publication.digest)).id,
    );
    const resources = Object.fromEntries(
      modes.map((mode) => [mode, published.resource(reference(mode))]),
    ) as Record<(typeof modes)[number], string>;
    const secretId = published.resource('mutation/SecretReference/Credential');
    const operations = [
      ...Object.values(resources).flatMap((id) => [id + ':read', id + ':sync']),
      published.resource('mutation/ConnectorImplementation/Batch') + ':read',
      dataset + ':read',
      dataset + ':write',
      secretId + ':use',
    ];
    const authority = new Authority(db.catalog);
    const sessions = new WorkloadSessions(authority);
    const serviceApplication = 'mutation-service';
    const mint = async (application: string) => {
      await authority.setApplication(environment, 'fixture', application, operations);
      await authority.setGrants(environment, 'fixture', principal, application, operations);
      return (await sessions.mint({ environment, principal, application }, 'fixture')).token;
    };
    const token = await mint('mutation-client');
    await mint(serviceApplication);
    const owner = randomUUID(),
      host = randomUUID(),
      service = randomUUID();
    await db.pool.query(
      "INSERT INTO moria.runtime_resource_classes(name,maximum_tasks,maximum_environment_tasks) VALUES('engine-spill',2,2)",
    );
    await db.pool.query(
      "INSERT INTO moria.runtime_workers(owner,host_id,incarnation,resource_classes,domains,expires_at) SELECT $1,$2,incarnation,ARRAY['engine-spill'],ARRAY['sync'],clock_timestamp()+interval '1 hour' FROM moria.indexing_instance",
      [owner, host],
    );
    await db.pool.query(
      "INSERT INTO moria.runtime_service_authorities(id,environment,principal,application,operations,markings,domains,resource_classes,incarnation) SELECT $1,$2,$3,$4,$5,ARRAY['private'],ARRAY['sync'],ARRAY['engine-spill'],incarnation FROM moria.indexing_instance",
      [service, environment, principal, serviceApplication, operations],
    );
    const { iceberg_schema: schema } = (
      await db.pool.query(
        'SELECT iceberg_schema FROM moria.native_dataset_bindings WHERE environment=$1 AND resource=$2 AND enabled',
        [environment, dataset],
      )
    ).rows[0];
    const address = provider.address();
    assert.ok(address && typeof address !== 'string');
    const providerEnv = {
      CONNECTOR_ENDPOINT: 'http://127.0.0.1:' + address.port + '/',
      CONNECTOR_MATERIAL_0: material,
    };
    const operatorEnv = { ...fixture.operatorEnv, ...providerEnv };
    const runtimeEnv = {
      ...fixture.nativeEnv,
      ...providerEnv,
      MORIA_PIPELINE_WORKER_OWNER: owner,
      MORIA_PIPELINE_HOST_ID: host,
    };
    for (const mode of modes) {
      const resource = resources[mode],
        destination = randomUUID();
      await db.pool.query(
        "INSERT INTO moria.runtime_service_targets(environment,resource,domain,binding_revision,resource_class,authority) VALUES($1,$2,'sync','v1','engine-spill',$3)",
        [environment, resource, service],
      );
      const credentials = [
        {
          secret: secretId,
          materialRef: 'CONNECTOR_MATERIAL_0',
          header: 'authorization',
          scheme: 'bearer',
        },
      ];
      await db.pool.query(
        "INSERT INTO moria.runtime_pipeline_destinations(id,environment,resource,revision,endpoint_ref,secret_ref,credentials,methods,paths,markings,allowed_markings) VALUES($1,$2,$3,'v1','CONNECTOR_ENDPOINT','CONNECTOR_MATERIAL_0',$4,ARRAY['GET'],ARRAY['/rows'],ARRAY['private'],ARRAY['private'])",
        [destination, environment, resource, JSON.stringify(credentials)],
      );
      const input =
        mode === 'delete'
          ? {
              ...schema,
              fields: schema.fields.filter((field: { name: string }) => field.name === 'tags'),
              'identifier-field-ids': [],
            }
          : schema;
      const profile = {
        serviceAuthority: service,
        serviceBindingRevision: 'v1',
        resourceClass: 'engine-spill',
        hardSpill: true,
        adapter: image.adapter,
        outputs: { [output]: { namespace: fixture.namespace, schema: input, mutationBudget } },
        limits: {
          timeoutSeconds: 120,
          memoryMiB: 512,
          cpuMillis: 60000,
          inputBytes: 134217728,
          outputBytes: 134217728,
        },
        connector: destination,
      };
      await db.pool.query(
        "INSERT INTO moria.runtime_pipeline_profiles(environment,resource,revision,domain,service_authority,service_revision,resource_class,profile) VALUES($1,$2,'v1','sync',$3,'v1','engine-spill',$4)",
        [environment, resource, service, profile],
      );
      const inspection = await fixture.command(
        ['secret', 'inspect-connector'],
        { environment, connection: resource },
        operatorEnv,
      );
      assert.equal(inspection.bindings.length, 1);
      await fixture.command(
        ['secret', 'approve'],
        {
          requestKey: 'mutation-' + mode + '-credential',
          environment,
          binding: inspection.bindings[0],
          materialRevision: 'mutation-v1',
          markings: ['private'],
        },
        operatorEnv,
      );
    }
    const pool = JSON.parse(process.env.MORIA_CONNECTOR_SCRATCH_POOL_JSON ?? 'null');
    assert.equal(pool?.slots.length, 2, 'sparse publication uses actual isolated hard spill');
    engine = await startInstanceEngine({ host, runtimeEnvironment: runtimeEnv, pool });
    const head = async () => (await db.catalog.heads(environment, [dataset]))[0];
    const invoke = async (mode: (typeof modes)[number], key: string, rows: typeof offered.rows) => {
      const resource = resources[mode],
        before = calls;
      offered = { rows, keysOnly: mode === 'delete' };
      const run = () =>
        fixture.command(['pipelines', 'invoke'], { token, resource, key }, runtimeEnv);
      const { id } = await run();
      const deadline = Date.now() + 120000;
      while (true) {
        engine!.check();
        const result = (
          await db.pool.query(
            'SELECT status,error,result FROM moria.runtime_executions WHERE id=$1',
            [id],
          )
        ).rows[0];
        if (result.status === 'succeeded') break;
        assert.ok(!['failed', 'cancelled'].includes(result.status), JSON.stringify(result));
        assert.ok(Date.now() < deadline, 'sparse ' + mode + ' execution did not complete');
        await delay(100);
      }
      assert.equal(calls, before + 1);
      assert.equal((await run()).id, id);
      assert.equal(calls, before + 1);
      const checkpoint = (
        await db.pool.query(
          'SELECT version::text,value FROM moria.checkpoints WHERE environment=$1 AND connection=$2',
          [environment, resource],
        )
      ).rows[0];
      assert.deepEqual(checkpoint.value, { offset: calls });
      t.diagnostic(
        JSON.stringify({
          mode,
          key,
          revision: (await head()).revision,
          checkpoint: checkpoint.version,
        }),
      );
      return id as string;
    };
    const app = await invoke('append', 'append', [three]);
    const up = await invoke('upsert', 'update-and-insert', [edited, four]);
    const del = await invoke('delete', 'delete-one', [{ tags: two.tags }, { tags: ['missing'] }]);
    const result = await head();
    const noop = await invoke('delete', 'no-effect', [{ tags: ['missing'] }]);
    assert.deepEqual(await head(), result);
    const journal = (
      await db.pool.query(
        'SELECT execution,mode,change_kind,basis_revision::text,result_revision::text,mutation_footprint,complete_rows_root FROM moria.runtime_dataset_commit_deltas WHERE dataset=$1 ORDER BY result_revision',
        [dataset],
      )
    ).rows;
    assert.deepEqual(
      journal.map((edge) => [
        edge.execution,
        edge.mode,
        edge.change_kind,
        edge.basis_revision,
        edge.result_revision,
        edge.mutation_footprint?.node.entries,
      ]),
      [
        [first.execution, 'replace', 'replace', '0', '1', undefined],
        [app, 'append', 'mutation', '1', '2', 1],
        [up, 'upsert', 'mutation', '2', '3', 2],
        [del, 'delete', 'mutation', '3', '4', 1],
      ],
    );
    assert.ok(journal[0].complete_rows_root);
    assert.ok(
      journal
        .slice(1)
        .every((edge) => edge.complete_rows_root === null && edge.mutation_footprint.version === 1),
    );
    const receipt = (
      await db.pool.query('SELECT receipt FROM moria.publications WHERE execution=$1', [noop])
    ).rows[0].receipt;
    assert.deepEqual(receipt.outputs, [
      { dataset, revision: '4', snapshotId: result.snapshotId, changed: false },
    ]);
    await fixture.read(first.head);
    const expected = [edited, three, four];
    await fixture.read(result, true, expected);
    const binding = await fixture.bind(
      [{ resource: dataset, revision: result.revision, snapshot: result.snapshotId }],
      {},
    );
    const exportDir = await mkdtemp(join(tmpdir(), 'moria-sparse-iceberg-'));
    t.after(() => rm(exportDir, { recursive: true, force: true }));
    await fixture.invoke({
      root: db.root,
      token: fixture.token,
      binding,
      mode: 'graph-read',
      standardExport: exportDir,
      expectedRows: expected.length,
      writerRows: { [dataset]: expected },
      snapshots: [{ ...fixture.snapshot, snapshotId: result.snapshotId }],
      s3Bindings: [fixture.datasetStorage],
    });
    const { stdout } = await promisify(execFile)(resolve('.venv/bin/python'), [
      resolve('tests/fixtures/native-retention/iceberg_oracle.py'),
      join(exportDir, 'export.json'),
    ]);
    const oracle = JSON.parse(stdout) as {
      pyiceberg: string;
      pyarrow: string;
      snapshot: string;
      sequence: string;
      positionDeleteFiles: number;
      matchedDeletePairs: number;
      rows: Row[];
    };
    assert.equal(oracle.snapshot, result.snapshotId);
    assert.equal(oracle.sequence, '4');
    assert.equal(oracle.positionDeleteFiles, 2);
    assert.equal(oracle.matchedDeletePairs, 2);
    const byId = (a: Row, b: Row) => a.id.localeCompare(b.id);
    assert.deepEqual(oracle.rows.toSorted(byId), expected.toSorted(byId));
    t.diagnostic(
      JSON.stringify({
        independentIceberg: oracle.pyiceberg,
        pyarrow: oracle.pyarrow,
        positionDeleteFiles: oracle.positionDeleteFiles,
        matchedDeletePairs: oracle.matchedDeletePairs,
      }),
    );
  },
);
