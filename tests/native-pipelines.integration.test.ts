import { connectorCustody } from './fixtures/native-connectors/custody.js';
import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { spawn, execFile, type ChildProcess } from 'node:child_process';
import { lstat, mkdir, readFile, readdir, realpath } from 'node:fs/promises';
import { basename, dirname, join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { database } from './fixtures/catalog-storage/database.js';
import { engineImage } from './fixtures/data-engine/image.js';
import { createLocalFilesystem } from '../services/filesystem/src/local.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { filesystemOperations } from '../packages/contracts/filesystem.js';
import type { Resource } from '../packages/contracts/types.js';
import { fingerprint } from '../packages/compiler/src/index.js';
import { pipelinesClient } from '../packages/platform/src/pipelines.js';
import { createTransport } from '../packages/platform/src/transport.js';
import type { ValueType } from '../packages/contracts/value-types.js';
import pg from 'pg';

const execute = promisify(execFile);
async function until(check: () => Promise<boolean>, timeout = 120000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    if (await check()) return;
    await delay(200);
  }
  assert.fail('Actual native pipeline lifecycle barrier was not reached');
}
const program = `import json\ndef transform(ctx):\n c=ctx.connection()\n for alias,name in [('a','inputA'),('b','inputB'),('c','inputC')]:\n  c.register(alias+'_input',ctx.read('engine/Dataset/'+name))\n  c.execute('CREATE TABLE '+alias+'_data AS SELECT * FROM '+alias+'_input')\n c.execute('CREATE TABLE joined AS SELECT a.id,a."group" AS g,CAST(a.amount+b.amount+c.amount AS BIGINT) AS total FROM a_data a JOIN b_data b USING(id) JOIN c_data c USING(id)')\n ctx.write('engine/Dataset/Detail',c.sql('SELECT id,total FROM joined ORDER BY id'))\n ctx.write('engine/Dataset/Summary',c.sql('SELECT g,CAST(count(*) AS BIGINT) AS n,CAST(sum(total) AS BIGINT) AS total FROM joined GROUP BY g ORDER BY g'))\n print(json.dumps({'phase':'joined','total':'9007199254740996'}))\n`;
const verifyProgram = `import json\nimport pyarrow as pa\ndef transform(ctx):\n count=0\n for batch in ctx.read('engine/Dataset/Detail'):\n  for value in batch.column('total'):\n   assert value.as_py()==9007199254740996\n  count+=batch.num_rows\n assert count==6000\n groups=0\n for batch in ctx.read('engine/Dataset/Summary'):\n  for n,total in zip(batch.column('n'),batch.column('total')):\n   assert n.as_py()==600 and total.as_py()==5404319552844597600\n   groups+=1\n assert groups==10\n ctx.write('engine/Dataset/Proof',pa.table({'rows':pa.array([count],type=pa.int64())}))\n print(json.dumps({'detailRows':str(count),'groups':str(groups),'total':'9007199254740996','groupSum':'5404319552844597600'}))\n`;
const typedVerifyProgram = verifyProgram.replace(
  " ctx.write('engine/Dataset/Proof',pa.table({'rows':pa.array([count],type=pa.int64())}))",
  [
    ' import uuid',
    ' c=ctx.connection()',
    " c.register('proof',pa.table({'rows':pa.array([count],type=pa.int64()),'identity':pa.array([uuid.UUID('a1832030-de00-4ba3-9fc2-cc7676092bc2').bytes],type=pa.uuid()),'items':pa.array([[9007199254740993,-1]],type=pa.list_(pa.int64()))}))",
    " ctx.write('engine/Dataset/Proof',c.sql('SELECT * FROM proof'))",
  ].join('\n'),
);
const confirmProgram = `import json,uuid,errno,os
import pyarrow as pa
def transform(ctx):
 rows=0
 for batch in ctx.read('engine/Dataset/Proof'):
  for identity,items in zip(batch.column('identity'),batch.column('items')):
   assert identity.as_py()==uuid.UUID('a1832030-de00-4ba3-9fc2-cc7676092bc2')
   assert items.as_py()==[9007199254740993,-1]
   rows+=1
 assert rows==1
 os.mkdir('/spill/locked',0o700)
 fd=os.open('/spill/locked/full',os.O_CREAT|os.O_WRONLY,0o600)
 filled=0
 try:
  while filled<256*1024*1024:
   filled+=os.write(fd,b'x'*1048576)
 except OSError as error:
  assert error.errno==errno.ENOSPC
 finally:
  os.ftruncate(fd,1024)
  os.close(fd)
 assert 64*1024*1024<filled<128*1024*1024
 os.chmod('/spill/locked/full',0)
 os.chmod('/spill/locked',0)
 ctx.write('engine/Dataset/Confirmed',pa.table({'rows':pa.array([rows],type=pa.int64())}))
 print(json.dumps({'uidArraysVerified':True,'hardSpillBytes':str(filled),'mode000CleanupRequired':True}))
`;

test(
  'native pipelines stream three pinned Iceberg sources through isolated DuckDB and atomically publish two outputs',
  // Cold provisioning and three independent 120-second jobs share this outer watchdog.
  { timeout: 600000 },
  async (t) => {
    const pool = JSON.parse(process.env.MORIA_ENGINE_SCRATCH_POOL_JSON ?? 'null') as {
      root: string;
      slots: { registration: Record<string, unknown>; device: string }[];
    } | null;
    assert.ok(pool?.slots.length === 2, 'A fresh actual dedicated ext4 scratch pool is mandatory');
    const db = await database(),
      image = await engineImage(program),
      children: ChildProcess[] = [];
    const rootProcesses = new Set<string>(),
      rootChildren = new Set<ChildProcess>();
    let workerRssObserved = 0n;
    const hostCpu = async () => {
      let worker = 0n,
        postgres = 0n;
      for (const pid of await readdir('/proc')) {
        if (!/^[0-9]+$/.test(pid)) continue;
        try {
          const stat = await readFile(`/proc/${pid}/stat`, 'utf8'),
            end = stat.lastIndexOf(') '),
            fields = stat.slice(end + 2).split(' '),
            ticks = BigInt(fields[11]) + BigInt(fields[12]);
          if (rootProcesses.has(pid)) worker += ticks;
          if (stat.slice(stat.indexOf('(') + 1, end) === 'postgres') postgres += ticks;
        } catch {}
      }
      return { workerCpuTicks: worker.toString(), postgresCpuTicks: postgres.toString() };
    };
    t.after(async () => {
      const failures: unknown[] = [];
      const cleanup = async (action: () => Promise<unknown>) => {
        try {
          await action();
        } catch (error) {
          failures.push(error);
        }
      };
      for (const pid of rootProcesses)
        await cleanup(() =>
          execute('docker', [
            'exec',
            '--user',
            '0',
            process.env.MORIA_TEST_CONTROLLER!,
            '/app/.venv/bin/python',
            '-c',
            'import os,signal,sys\ntry: os.kill(int(sys.argv[1]),signal.SIGTERM)\nexcept ProcessLookupError: pass',
            pid,
          ]),
        );
      if (rootChildren.size)
        await cleanup(() =>
          until(
            async () =>
              [...rootChildren].every(
                (child) => child.exitCode !== null || child.signalCode !== null,
              ),
            11000,
          ),
        );
      if ([...rootChildren].some((child) => child.exitCode === null && child.signalCode === null))
        for (const pid of rootProcesses)
          await cleanup(() =>
            execute('docker', [
              'exec',
              '--user',
              '0',
              process.env.MORIA_TEST_CONTROLLER!,
              '/app/.venv/bin/python',
              '-c',
              'import os,signal,sys\ntry: os.kill(int(sys.argv[1]),signal.SIGKILL)\nexcept ProcessLookupError: pass',
              pid,
            ]),
          );
      for (const child of children) child.kill('SIGKILL');
      assert.equal(dirname(db.root), tmpdir());
      assert.match(basename(db.root), /^catalog-storage-[A-Za-z0-9]+$/);
      const rootState = await lstat(db.root);
      assert.ok(rootState.isDirectory() && !rootState.isSymbolicLink());
      assert.equal(await realpath(db.root), resolve(db.root));
      assert.equal(await realpath(tmpdir()), resolve(tmpdir()));
      await cleanup(() =>
        execute('docker', [
          'exec',
          '--user',
          '0',
          process.env.MORIA_TEST_CONTROLLER!,
          'chown',
          '-R',
          '--no-dereference',
          '1000:1000',
          db.root,
        ]),
      );
      await cleanup(() => image.close());
      await cleanup(() => db.close());
      assert.deepEqual(
        failures,
        [],
        'All disposable native processes and private fixture files must clean up',
      );
    });
    const verifier = await image.derive('moria-engine-verifier', typedVerifyProgram);
    const confirmer = await image.derive('moria-engine-confirmer', confirmProgram);
    const environment = randomUUID(),
      principal = 'pipeline-workload',
      caller = 'pipeline-user',
      serviceApp = 'pipeline-service',
      owner = randomUUID(),
      host = randomUUID(),
      service = randomUUID();
    await db.catalog.ensureEnvironment(environment);
    const fs = await createLocalFilesystem(db.catalog, join(db.root, 'files')),
      authority = fs.filesystem.security.authority,
      sessions = new WorkloadSessions(authority);
    await authority.syncPrincipal(environment, 'fixture', principal, {
      kind: 'workload',
      enabled: true,
      attributes: {},
      markings: ['private'],
    });
    const fsOps = filesystemOperations.map((op) => `filesystem:${op}`);
    const mint = async (application: string, operations: string[]) => {
      await authority.setApplication(environment, 'fixture', application, operations);
      await authority.setGrants(environment, 'fixture', principal, application, operations);
      const issued = await sessions.mint({ environment, principal, application }, 'fixture');
      return {
        token: issued.token,
        context: { session: await sessions.authenticate(`Bearer ${issued.token}`) },
      };
    };
    const author = await mint('pipeline-author', ['instance:manage', ...fsOps]);
    const home = await fs.management.ensureHome(author.context),
      role = await fs.management.role(author.context, home.home.id),
      projects = await fs.filesystem.store.get(role!.configuration.projects as string);
    await fs.management.createProject(author.context, projects.id, 'pipelines', projects.revision);
    const source = await fs.filesystem.store.resolve(
      `/users/${home.home.name}/projects/pipelines/source`,
    );
    const made = await fs.filesystem.apply(author.context, randomUUID(), [
      {
        op: 'create',
        kind: 'directory',
        parentId: source.id,
        expectedParentRevision: source.revision,
        name: 'logic',
      },
    ]);
    const root = made.entries.find((entry) => entry.name === 'logic')!;
    const file = async (name: string, value: unknown) => {
      const body = Buffer.from(JSON.stringify(value));
      await fs.filesystem.writeFile(
        author.context,
        randomUUID(),
        {
          op: 'create',
          parentId: root.id,
          expectedParentRevision: (await fs.filesystem.store.get(root.id)).revision,
          name,
          content: {
            size: body.length,
            digest: `sha256:${createHash('sha256').update(body).digest('hex')}`,
            mediaType: 'application/json',
          },
        },
        new ReadableStream({
          start(controller) {
            controller.enqueue(body);
            controller.close();
          },
        }),
      );
    };
    await file('.repository.json', {
      version: 1,
      name: 'Native pipelines',
      namespace: 'engine',
      runtime: 'python-duckdb-v1',
    });
    await fs.source.register(
      author.context,
      root.id,
      (await fs.filesystem.store.get(root.id)).revision,
      'repository',
    );
    const long = { type: 'int64' as const, nullable: false },
      text = { type: 'string' as const, nullable: false };
    const columns: Record<string, Record<string, ValueType>> = {
      inputA: { id: long, group: text, amount: long, padding: text },
      inputB: { id: long, group: text, amount: long, padding: text },
      inputC: { id: long, group: text, amount: long, padding: text },
      Detail: { id: long, total: long },
      Summary: { g: text, n: long, total: long },
      Proof: {
        rows: long,
        identity: { type: 'uid', nullable: false },
        items: { type: 'array', nullable: false, items: { type: 'int64', nullable: false } },
      },
      Confirmed: { rows: long },
    };
    const fanout = Array.from(
      { length: 64 },
      (_, index) => `Fanout${index.toString().padStart(2, '0')}`,
    );
    const fanin = Array.from(
      { length: 64 },
      (_, index) => `Fanin${index.toString().padStart(2, '0')}`,
    );
    const maximumOutputs = Array.from(
      { length: 64 },
      (_, index) => `Maximum${index.toString().padStart(2, '0')}`,
    );
    for (const name of [...fanout, ...fanin, ...maximumOutputs])
      columns[name] = { id: long, total: long };
    const definitions: Resource[] = Object.entries(columns).map(([name, columns]) => ({
      apiVersion: 'moria/v1alpha2',
      kind: 'Dataset',
      metadata: { namespace: 'engine', name },
      spec: { columns, primaryKey: [Object.keys(columns)[0]], writeModes: ['replace'] },
    }));
    definitions.push(
      ...[
        {
          name: 'Join',
          artifact: image.artifact,
          inputs: ['inputA', 'inputB', 'inputC'],
          outputs: ['Detail', 'Summary'],
        },
        { name: 'Verify', artifact: verifier, inputs: ['Detail', 'Summary'], outputs: ['Proof'] },
        { name: 'Confirm', artifact: confirmer, inputs: ['Proof'], outputs: ['Confirmed'] },
        {
          name: 'Fanout',
          artifact: image.artifact,
          inputs: ['inputA', 'inputB', 'inputC'],
          outputs: fanout,
        },
        { name: 'Maximum', artifact: image.artifact, inputs: fanin, outputs: maximumOutputs },
      ].map((item) => ({
        apiVersion: 'moria/v1alpha2' as const,
        kind: 'Transform' as const,
        metadata: { namespace: 'engine', name: item.name },
        spec: {
          runtime: 'python-duckdb-v1',
          artifact: item.artifact,
          inputs: item.inputs.map((name) => `engine/Dataset/${name}`),
          outputs: item.outputs.map((name) => `engine/Dataset/${name}`),
          limits: { timeoutSeconds: 120, memoryMiB: 512, cpuMillis: 60000 },
        },
      })),
    );
    for (const definition of definitions.filter((item) => item.kind === 'Dataset'))
      await file(`${definition.metadata.name}.resource.json`, definition);
    const published = await fs.source.publish(
      author.context,
      root.id,
      (await fs.source.checkout(author.context, root.id)).digest,
      definitions.filter((item) => item.kind === 'Transform'),
    );
    await fs.source.apply(
      author.context,
      (await fs.source.plan(author.context, root.id, published.digest)).id,
    );
    const resources = new Map(published.resources.map((item) => [item.reference, item.id]));
    const id = (kind: string, name: string) => resources.get(`engine/${kind}/${name}`)!;
    const operations = [
      ...fsOps,
      ...Object.keys(columns).flatMap((name) => [
        `${id('Dataset', name)}:read`,
        `${id('Dataset', name)}:write`,
      ]),
      ...['Join', 'Verify', 'Confirm', 'Fanout', 'Maximum'].flatMap((name) => [
        `${id('Transform', name)}:read`,
        `${id('Transform', name)}:build`,
      ]),
    ];
    const user = await mint(caller, operations);
    const serviceOperations = operations.filter(
      (operation) => !operation.startsWith('filesystem:'),
    );
    await authority.setApplication(environment, 'fixture', serviceApp, serviceOperations);
    await authority.setGrants(environment, 'fixture', principal, serviceApp, serviceOperations);
    const roleName = `pipeline_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(
      `CREATE ROLE ${roleName} LOGIN PASSWORD '${password}';GRANT USAGE ON SCHEMA moria TO ${roleName};GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${roleName};GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${roleName}`,
    );
    const connection = `postgresql://${roleName}:${password}@127.0.0.1:${db.pool.options.port}/postgres`;
    const nativeRoot = join(db.root, 'native'),
      scratch = join(db.root, 'scratch'),
      physical = join(db.root, 'physical');
    await Promise.all([mkdir(nativeRoot), mkdir(scratch), mkdir(physical)]);
    const generated = JSON.parse(
      (
        await execute(
          resolve('.venv/bin/python'),
          ['tests/fixtures/data-engine/datasets.py', physical, '--fan-in'],
          { maxBuffer: 4 * 1024 * 1024 },
        )
      ).stdout,
    ) as any[];
    const schemas: Record<string, unknown> = {};
    for (const [name, fields] of Object.entries(columns)) {
      await db.catalog.ensureDataset(environment, id('Dataset', name));
      const tableUuid = generated.find((item) => item.name === name)?.tableUuid ?? randomUUID();
      await db.pool.query(
        "INSERT INTO moria.native_storage_namespaces(environment,warehouse_id,table_id,resource,revision,purpose,iceberg_uuid,markings) VALUES($1,'enginewarehouse',$2,$3,'v1','dataset-source',$4,ARRAY['private'])",
        [environment, name, id('Dataset', name), tableUuid],
      );
      schemas[name] = {
        type: 'struct',
        'schema-id': 0,
        fields: Object.entries(fields).map(([name, type], index) => ({
          id: index + 1,
          name,
          required: true,
          type:
            type.type === 'int64'
              ? 'long'
              : type.type === 'uid'
                ? 'uuid'
                : type.type === 'array'
                  ? {
                      type: 'list',
                      'element-id': 100 + index,
                      element: 'long',
                      'element-required': true,
                    }
                  : 'string',
        })),
      };
      const bindingFingerprint = `sha256:${createHash('sha256')
        .update(
          JSON.stringify({
            columns: fields,
            schema: schemas[name],
            namespace: name,
            primaryKey: [Object.keys(fields)[0]],
            writeModes: ['replace'],
          }),
        )
        .digest('hex')}`;
      await db.pool.query(
        "INSERT INTO moria.native_dataset_bindings(environment,resource,reference,revision,columns,iceberg_schema,warehouse_id,table_id,namespace_revision,physical_binding_fingerprint,source_digest,primary_key,write_modes,key_schema_hash) VALUES($1,$2,$3,$4,$5,$6,'enginewarehouse',$7,'v1',$9,$8,$10,ARRAY['replace'],$11)",
        [
          environment,
          id('Dataset', name),
          `engine/Dataset/${name}`,
          bindingFingerprint,
          fields,
          schemas[name],
          name,
          published.digest,
          fingerprint({ backend: 'local', root: nativeRoot }),
          [Object.keys(fields)[0]],
          fingerprint({
            version: 1,
            keys: [{ name: Object.keys(fields)[0], descriptor: fields[Object.keys(fields)[0]] }],
          }),
        ],
      );
    }
    await db.pool.query(
      "INSERT INTO moria.runtime_resource_classes(name,maximum_tasks,maximum_environment_tasks) VALUES('producer',4,4),('engine-spill',2,2)",
    );
    await db.pool.query(
      "INSERT INTO moria.runtime_workers(owner,host_id,incarnation,resource_classes,domains,expires_at) SELECT $1,$2,incarnation,ARRAY['producer','engine-spill'],ARRAY['sync','transform'],clock_timestamp()+interval '1 hour' FROM moria.indexing_instance",
      [owner, host],
    );
    const storage = Object.keys(columns).map((name) => ({
      namespace: { warehouseId: 'enginewarehouse', tableId: name, revision: 'v1' },
      provider: {
        kind: 'posix',
        root: nativeRoot,
        limits: { maxObjectBytes: 134217728, chunkBytes: 1048576 },
      },
    }));
    const env: Record<string, string | undefined> = {
      PATH: process.env.PATH,
      MORIA_INDEXING_DATABASE_SECRET_REF: 'PIPELINE_DATABASE',
      PIPELINE_DATABASE: connection,
      MORIA_INDEXING_DATABASE_TLS: 'disable',
      MORIA_NATIVE_LISTEN: '127.0.0.1:0',
      MORIA_PIPELINE_WORKER_OWNER: owner,
      MORIA_PIPELINE_HOST_ID: host,
      MORIA_NATIVE_STORAGE_BINDINGS: JSON.stringify(storage),
      MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT: scratch,
      MORIA_NATIVE_CLOSURE_LIMITS: JSON.stringify({
        maxEntries: 10000,
        maxReferencedBytes: 536870912,
        maxSpoolBytes: 33554432,
      }),
    };
    const binary =
      process.env.MORIA_RUNTIME_BINARY ??
      process.env.MORIA_INDEXING_BINARY ??
      '/usr/local/bin/moria-indexing';
    const start = (args: string[], input?: unknown, rootUser = false) => {
      const entries = Object.entries(env).filter(
        (entry): entry is [string, string] => entry[1] !== undefined,
      );
      const child = rootUser
        ? spawn(
            'docker',
            [
              'exec',
              '--user',
              '0',
              '-i',
              ...entries.flatMap(([key, value]) => ['--env', `${key}=${value}`]),
              process.env.MORIA_TEST_CONTROLLER!,
              binary,
              ...args,
            ],
            { stdio: ['pipe', 'pipe', 'pipe'] },
          )
        : spawn(binary, args, { env, stdio: ['pipe', 'pipe', 'pipe'] });
      children.push(child);
      if (rootUser && args[0] === 'pipelines') rootChildren.add(child);
      let output = '',
        error = '';
      child.stdout.on('data', (bytes) => {
        output += bytes;
        for (const line of output.split('\n'))
          try {
            const value = JSON.parse(line);
            if (value.event === 'pipeline-worker' && /^[1-9][0-9]*$/.test(value.processId))
              rootProcesses.add(value.processId);
          } catch {}
      });
      child.stderr.on('data', (bytes) => (error += bytes));
      child.stdin.on('error', () => {});
      child.stdin.end(input === undefined ? '' : JSON.stringify(input));
      const done = new Promise<string>((resolve, reject) => {
        child.once('error', reject);
        child.once('exit', (code, signal) =>
          code === 0
            ? resolve(output)
            : reject(new Error(`${args.join(' ')} ${code ?? signal}: ${error}`)),
        );
      });
      void done.catch(() => {});
      return { child, done, output: () => output, error: () => error };
    };
    for (const slot of pool.slots)
      await start(
        ['scratch', 'register'],
        { ...slot.registration, hostId: host, resourceClass: 'engine-spill' },
        true,
      ).done;
    // Each real fixture producer retains its own unchanged 120-second deadline.
    for (let offset = 0; offset < generated.length; offset += 16) {
      const inputGroup = generated.slice(offset, offset + 16);
      const epoch = (await authority.current(environment, principal, caller)).epoch;
      const producerBinding = (
        await db.pool.query(
          "INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation) SELECT $1,$2,$3,$4,$5,ARRAY['private'],$6,i.security_epoch,c.incarnation FROM moria.instance_state i,moria.indexing_instance c RETURNING id",
          [
            environment,
            id('Dataset', inputGroup[0].name),
            {
              version: 1,
              domain: 'sync',
              release: published.digest.replace('sha256:', ''),
              resourceClass: 'producer',
              timeoutSeconds: 120,
              retryPolicy: 'idempotent',
              inputSchema: { type: 'object', properties: {} },
              pinnedInputs: [],
              expectedHeads: Object.fromEntries(
                inputGroup.map((item) => [id('Dataset', item.name), '0']),
              ),
            },
            {
              environment,
              principal,
              application: caller,
              sessionHash: createHash('sha256').update(user.token).digest('hex'),
            },
            inputGroup.flatMap((item) => [
              `${id('Dataset', item.name)}:read`,
              `${id('Dataset', item.name)}:write`,
            ]),
            epoch,
          ],
        )
      ).rows[0].id;
      const producer = spawn(
        process.env.MORIA_CATALOG_TEST_BINARY ?? '/usr/local/bin/moria-catalog-tests',
        ['--ignored', '--exact', 'catalog_publication', '--nocapture', '--test-threads=1'],
        {
          env: {
            ...env,
            MORIA_NATIVE_CATALOG_FIXTURE: JSON.stringify({
              root: nativeRoot,
              token: user.token,
              binding: producerBinding,
              mode: 'valid',
              registeredHost: host,
              registeredOwner: owner,
              snapshots: inputGroup.map((item) => ({
                ...item,
                resource: id('Dataset', item.name),
              })),
            }),
          },
          stdio: ['ignore', 'pipe', 'pipe'],
        },
      );
      children.push(producer);
      let producerError = '';
      producer.stdout.on('data', (bytes) => (producerError += bytes));
      producer.stderr.on('data', (bytes) => (producerError += bytes));
      await new Promise<void>((resolve, reject) => {
        producer.once('error', reject);
        producer.once('exit', (code) =>
          code === 0 ? resolve() : reject(new Error(producerError)),
        );
      });
    }
    const server = start(['serve'], undefined);
    let api = '';
    await until(async () => {
      assert.equal(server.child.exitCode, null, server.error());
      for (const line of server.output().split('\n'))
        try {
          const event = JSON.parse(line);
          if (event.event === 'serving') api = `http://${event.address}`;
        } catch {}
      return !!api;
    });
    const request = async (path: string, method = 'GET', body?: unknown, key?: string) => {
      const response = await fetch(api + path, {
        method,
        headers: {
          authorization: `Bearer ${user.token}`,
          'content-type': 'application/json',
          ...(key ? { 'idempotency-key': key } : {}),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      });
      const value = (await response.json()) as any;
      return { response, value };
    };
    const invoke = async (name: string, key: string, expectedStatus = 202) => {
      const response = await request(
        `/api/v1/pipelines/${id('Transform', name)}/executions`,
        'POST',
        { wait: false },
        key,
      );
      assert.equal(response.response.status, expectedStatus, JSON.stringify(response.value));
      await db.pool.query('SELECT moria.runtime_pipeline_current($1)', [response.value.id]);
      return response.value;
    };
    await t.test(
      'metadata authority and actual storage pins precede any user-code claim',
      async () => {
        const unavailable = await request(
          `/api/v1/pipelines/${id('Transform', 'Join')}/executions`,
          'POST',
          { wait: false },
          'not-configured',
        );
        assert.equal(unavailable.response.status, 503);
        assert.equal(unavailable.value.error.code, 'PIPELINE_PROFILE_REQUIRED');
        await db.pool.query(
          "INSERT INTO moria.runtime_service_authorities(id,environment,principal,application,operations,markings,domains,resource_classes,incarnation) SELECT $1,$2,$3,$4,$5,ARRAY['private'],ARRAY['transform'],ARRAY['engine-spill'],incarnation FROM moria.indexing_instance",
          [service, environment, principal, serviceApp, serviceOperations],
        );
        for (const [name, outputs] of [
          ['Join', ['Detail', 'Summary']],
          ['Verify', ['Proof']],
          ['Confirm', ['Confirmed']],
          ['Fanout', fanout],
          ['Maximum', maximumOutputs],
        ] as const) {
          await db.pool.query(
            "INSERT INTO moria.runtime_service_targets(environment,resource,domain,binding_revision,resource_class,authority) VALUES($1,$2,'transform','v1','engine-spill',$3)",
            [environment, id('Transform', name), service],
          );
          const profile = {
            serviceAuthority: service,
            serviceBindingRevision: 'v1',
            resourceClass: 'engine-spill',
            hardSpill: true,
            adapter: image.adapter,
            outputs: Object.fromEntries(
              outputs.map((name) => [
                `engine/Dataset/${name}`,
                {
                  namespace: { warehouseId: 'enginewarehouse', tableId: name, revision: 'v1' },
                  schema: schemas[name],
                },
              ]),
            ),
            limits: {
              timeoutSeconds: 120,
              memoryMiB: 512,
              cpuMillis: 60000,
              inputBytes: 134217728,
              outputBytes: 134217728,
            },
            connector: null,
          };
          await db.pool.query(
            "INSERT INTO moria.runtime_pipeline_profiles(environment,resource,revision,domain,service_authority,service_revision,resource_class,profile) VALUES($1,$2,'v1','transform',$3,'v1','engine-spill',$4)",
            [environment, id('Transform', name), service, profile],
          );
        }
      },
    );
    await t.test(
      'the accepted sixty-four-output profile fits its complete trusted operation binding before code',
      async () => {
        const accepted = await invoke('Fanout', 'maximum-output-fanout');
        const row = (
          await db.pool.query(
            "SELECT cardinality(b.operations) AS operations,(SELECT count(*)::int FROM jsonb_object_keys(e.spec->'expectedHeads')) AS outputs,jsonb_array_length(e.spec->'pinnedInputs') AS inputs FROM moria.runtime_executions e JOIN moria.runtime_execution_bindings b ON b.id=e.binding WHERE e.id=$1",
            [accepted.id],
          )
        ).rows[0];
        assert.equal(row.outputs, 64);
        assert.equal(row.inputs, 3);
        assert.equal(row.operations, 133);
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.runtime_sandbox_attempts WHERE execution=$1',
              [accepted.id],
            )
          ).rows[0].count,
          '0',
        );
        const cancelled = await request(
          `/api/v1/pipeline-executions/${accepted.id}/cancel`,
          'POST',
          {},
        );
        assert.equal(cancelled.response.status, 200, JSON.stringify(cancelled.value));
        assert.deepEqual(cancelled.value, { id: accepted.id, cancelRequested: true });
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.publications WHERE environment=$1 AND execution=$2',
              [environment, accepted.id],
            )
          ).rows[0].count,
          '0',
        );
      },
    );
    await t.test(
      'the full sixty-four-input and sixty-four-output profile pins real heads within the trusted operation ceiling',
      async () => {
        const accepted = await invoke('Maximum', 'maximum-input-output-fanout');
        const row = (
          await db.pool.query(
            "SELECT cardinality(b.operations) AS operations,(SELECT count(*)::int FROM jsonb_object_keys(e.spec->'expectedHeads')) AS outputs,jsonb_array_length(e.spec->'pinnedInputs') AS inputs FROM moria.runtime_executions e JOIN moria.runtime_execution_bindings b ON b.id=e.binding WHERE e.id=$1",
            [accepted.id],
          )
        ).rows[0];
        assert.deepEqual(row, { operations: 194, outputs: 64, inputs: 64 });
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.runtime_sandbox_attempts WHERE execution=$1',
              [accepted.id],
            )
          ).rows[0].count,
          '0',
        );
        const cancelled = await request(
          `/api/v1/pipeline-executions/${accepted.id}/cancel`,
          'POST',
          {},
        );
        assert.equal(cancelled.response.status, 200, JSON.stringify(cancelled.value));
        assert.deepEqual(cancelled.value, { id: accepted.id, cancelRequested: true });
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.publications WHERE environment=$1 AND execution=$2',
              [environment, accepted.id],
            )
          ).rows[0].count,
          '0',
        );
      },
    );
    const revoked = await invoke('Join', 'revoked-before-worker');
    await t.test(
      'private batched authority retains per-request parity and current revocation',
      async () => {
        const client = await db.pool.connect();
        const requests = [
          { resource: id('Transform', 'Join'), operation: 'read' },
          ...['inputA', 'inputB', 'inputC'].map((name) => ({
            resource: id('Dataset', name),
            operation: 'read',
          })),
          ...['Detail', 'Summary'].flatMap((name) =>
            ['read', 'write'].map((operation) => ({ resource: id('Dataset', name), operation })),
          ),
        ];
        try {
          await client.query('BEGIN');
          const binding = (
            await client.query('SELECT binding FROM moria.runtime_executions WHERE id=$1', [
              revoked.id,
            ])
          ).rows[0].binding;
          const state = (
            await client.query('SELECT moria.runtime_service_binding_guard($1) AS state', [binding])
          ).rows[0].state;
          const batch = async () =>
            (
              await client.query(
                'SELECT moria.native_serving_resource_access_batch_state($1,$2) AS labels',
                [state, JSON.stringify(requests)],
              )
            ).rows[0].labels;
          const expected = [];
          for (const request of requests)
            expected.push(
              (
                await client.query(
                  'SELECT moria.native_serving_resource_access_state($1,$2,$3) AS labels',
                  [state, request.resource, request.operation],
                )
              ).rows[0].labels,
            );
          assert.deepEqual(await batch(), expected);
          const sessionHash = createHash('sha256').update(user.token).digest('hex');
          const sessionBatch = async () =>
            (
              await client.query(
                'SELECT moria.native_serving_resource_access_batch($1,$2) AS labels',
                [sessionHash, JSON.stringify(requests)],
              )
            ).rows[0].labels;
          const sessionExpected = [];
          for (const request of requests)
            sessionExpected.push(
              (
                await client.query(
                  'SELECT moria.native_serving_resource_access($1,$2,$3) AS labels',
                  [sessionHash, request.resource, request.operation],
                )
              ).rows[0].labels,
            );
          assert.deepEqual(await sessionBatch(), sessionExpected);
          const reads = requests.filter(({ operation }) => operation === 'read');
          assert.deepEqual(
            (
              await client.query(
                'SELECT moria.native_serving_resource_access_batch($1,$2,true) AS labels',
                [sessionHash, JSON.stringify(reads)],
              )
            ).rows[0].labels,
            await Promise.all(
              reads.map(
                async ({ resource }) =>
                  (
                    await client.query(
                      "SELECT moria.native_resource_filesystem_access($1,$2,'read') AS labels",
                      [sessionHash, resource],
                    )
                  ).rows[0].labels,
              ),
            ),
          );
          for (const mutation of [
            [
              "UPDATE moria.native_dataset_bindings SET enabled=false,revision='sha256:'||repeat('f',64) WHERE environment=$1 AND resource=$2",
              [environment, id('Dataset', 'inputA')],
              'NOT_FOUND',
            ],
            [
              'UPDATE moria.fs_entries SET deleted=true WHERE environment_id=$1 AND resource_id=$2',
              [environment, id('Dataset', 'inputA')],
              'NOT_FOUND',
            ],
            [
              "INSERT INTO moria.resource_data_labels(environment,resource,marking) VALUES($1,$2,'unavailable-clearance')",
              [environment, id('Dataset', 'inputA')],
              'FORBIDDEN',
            ],
            [
              'INSERT INTO moria.fs_policies(entry_id,grants) SELECT id,$3 FROM moria.fs_entries WHERE environment_id=$1 AND resource_id=$2 AND NOT deleted ON CONFLICT(entry_id) DO UPDATE SET grants=moria.fs_policies.grants||EXCLUDED.grants',
              [
                environment,
                id('Dataset', 'inputA'),
                JSON.stringify([
                  {
                    effect: 'deny',
                    subject: { kind: 'principal', id: state.instanceId },
                    operations: ['read'],
                    inherit: true,
                  },
                ]),
              ],
              'FORBIDDEN',
            ],
          ] as const) {
            await client.query('SAVEPOINT negative');
            const changed = await client.query(mutation[0], [...mutation[1]]);
            assert.equal(changed.rowCount, 1, mutation[0]);
            await assert.rejects(
              batch(),
              (error: any) => error.message === mutation[2],
              mutation[0],
            );
            await client.query('ROLLBACK TO SAVEPOINT negative');
            assert.deepEqual(await batch(), expected);
          }
          await client.query('SAVEPOINT malformed');
          await assert.rejects(
            client.query('SELECT moria.native_serving_resource_access_batch_state($1,$2)', [
              state,
              JSON.stringify([{ ...requests[0], actor: 'forged' }]),
            ]),
            (error: any) => error.message === 'FORBIDDEN',
          );
          await client.query('ROLLBACK TO SAVEPOINT malformed');
          await assert.rejects(
            client.query('SELECT moria.native_serving_resource_access_batch_state($1,$2)', [
              state,
              JSON.stringify(Array(257).fill(requests[0])),
            ]),
            (error: any) => error.message === 'RESOURCE_LIMIT',
          );
        } finally {
          await client.query('ROLLBACK');
          client.release();
        }
        const untrusted = new pg.Client({ connectionString: connection });
        await untrusted.connect();
        try {
          await assert.rejects(
            untrusted.query("SELECT moria.native_serving_resource_access_batch_state('{}','[]')"),
            (error: any) => error.code === '42501',
          );
        } finally {
          await untrusted.end();
        }
      },
    );
    const probeClient = new pg.Client({ connectionString: connection });
    await probeClient.connect();
    try {
      const jit = (
        await probeClient.query(
          "SELECT current_setting('jit') AS setting,pg_jit_available() AS available",
        )
      ).rows[0];
      const timings: Record<string, string[]> = {};
      for (const mode of ['on', 'off']) {
        await probeClient.query('BEGIN');
        await probeClient.query(`SET LOCAL jit=${mode}`);
        timings[mode] = [];
        for (let count = 0; count < 3; count++) {
          const started = performance.now();
          await probeClient.query('SELECT moria.runtime_pipeline_current($1)', [revoked.id]);
          timings[mode].push((performance.now() - started).toFixed(3));
        }
        await probeClient.query('COMMIT');
      }
      console.log(
        'PIPELINE_AUTHORITY_GUARD_PROBE=' +
          JSON.stringify({
            jit,
            timingsMillis: timings,
            scope: 'one dedicated probe connection; worker settings unchanged',
          }),
      );
    } finally {
      await probeClient.end();
    }
    const profileClient = await db.pool.connect();
    try {
      await profileClient.query('BEGIN');
      await profileClient.query("SET LOCAL track_functions='all'");
      await profileClient.query('SELECT moria.runtime_pipeline_current($1)', [revoked.id]);
      const profile = (
        await profileClient.query(
          "SELECT funcname,calls,total_time,self_time FROM pg_stat_xact_user_functions WHERE schemaname='moria' ORDER BY total_time DESC LIMIT 24",
        )
      ).rows;
      console.log('PIPELINE_AUTHORITY_FUNCTION_PROFILE=' + JSON.stringify(profile));
    } finally {
      await profileClient.query('ROLLBACK');
      profileClient.release();
    }
    if (process.env.MORIA_ENGINE_PROFILE_ONLY === '1') return;
    const epochBefore = BigInt(
      (
        await db.pool.query('SELECT security_epoch FROM moria.environments WHERE id=$1', [
          environment,
        ])
      ).rows[0].security_epoch,
    );
    const revokeClient = await db.pool.connect();
    try {
      await revokeClient.query('BEGIN');
      await revokeClient.query(
        'UPDATE moria.runtime_pipeline_profiles SET enabled=false WHERE environment=$1 AND resource=$2',
        [environment, id('Transform', 'Join')],
      );
      await revokeClient.query(
        'UPDATE moria.runtime_pipeline_profiles SET enabled=true WHERE environment=$1 AND resource=$2',
        [environment, id('Transform', 'Join')],
      );
      await revokeClient.query('COMMIT');
    } finally {
      revokeClient.release();
    }
    assert.equal(
      BigInt(
        (
          await db.pool.query('SELECT security_epoch FROM moria.environments WHERE id=$1', [
            environment,
          ])
        ).rows[0].security_epoch,
      ),
      epochBefore + 2n,
    );
    const worker = start(['pipelines', 'worker'], undefined, true);
    await t.test(
      'unobserved profile disable and restore fences accepted work before code starts',
      async () => {
        await until(async () => {
          assert.equal(worker.child.exitCode, null, worker.error());
          return (
            (
              await db.pool.query('SELECT status FROM moria.runtime_executions WHERE id=$1', [
                revoked.id,
              ])
            ).rows[0].status === 'failed'
          );
        });
        assert.equal(
          (
            await db.pool.query(
              'SELECT attempts FROM moria.runtime_execution_tasks WHERE execution=$1',
              [revoked.id],
            )
          ).rows[0].attempts,
          0,
        );
        assert.equal(
          (
            await db.pool.query(
              "SELECT count(*) FROM moria.runtime_execution_events WHERE execution=$1 AND kind='noCodeStart'",
              [revoked.id],
            )
          ).rows[0].count,
          '1',
        );
      },
    );
    const terminal = async (id: string) => {
      let value: any;
      let lastProgress = 0;
      await until(async () => {
        assert.equal(worker.child.exitCode, null, worker.error());
        const result = await request(`/api/v1/pipeline-executions/${id}`);
        const pending = result.response.status === 409;
        if (pending) {
          assert.equal(result.value.error?.code, 'FUNCTION_OBSERVATION_INCOMPLETE');
        } else {
          assert.equal(result.response.status, 200, JSON.stringify(result.value));
          value = result.value;
        }
        if (!pending && Date.now() - lastProgress >= 10000) {
          lastProgress = Date.now();
          console.log(
            'PIPELINE_LIVE_CONTROL=' +
              JSON.stringify({
                id,
                status: value.status,
                hostCpu: await hostCpu(),
                events: (
                  await db.pool.query(
                    "SELECT e.sequence::text,e.kind,e.payload FROM moria.runtime_execution_events e WHERE execution=$1 AND kind IN('enginePhase','engineProgress','brokerFailed') ORDER BY e.sequence DESC LIMIT 4",
                    [id],
                  )
                ).rows,
              }),
          );
        }
        for (const pid of rootProcesses) {
          const status = await readFile(`/proc/${pid}/status`, 'utf8');
          const rss = status.match(/^VmRSS:\s+(\d+) kB$/m);
          if (rss) {
            const bytes = BigInt(rss[1]) * 1024n;
            if (bytes > workerRssObserved) workerRssObserved = bytes;
          }
        }
        return !pending && ['succeeded', 'failed', 'cancelled'].includes(value.status);
      }, 135000);
      if (value.status === 'succeeded')
        assert.equal(
          (
            await db.pool.query(
              'SELECT finished_at<=deadline AS within_deadline FROM moria.runtime_executions WHERE id=$1',
              [id],
            )
          ).rows[0].within_deadline,
          true,
          'Successful publication must precede the accepted 120-second database deadline',
        );
      return value;
    };
    let joined = '';
    await t.test(
      'actual bounded guest reads three wide sources and commits both heads with one receipt',
      async () => {
        const accepted = await invoke('Join', 'join-once');
        joined = accepted.id;
        const result = await terminal(joined);
        assert.equal(
          result.status,
          'succeeded',
          JSON.stringify({
            result,
            hostCpu: await hostCpu(),
            events: (await request(`/api/v1/pipeline-executions/${joined}/events`)).value,
          }),
        );
        assert.equal(result.result.outputs.length, 2);
        const receipts = (
          await db.pool.query('SELECT * FROM moria.publications WHERE execution=$1', [joined])
        ).rows;
        assert.equal(receipts.length, 1);
        const attempts = (
          await db.pool.query(
            'SELECT attempts FROM moria.runtime_execution_tasks WHERE execution=$1',
            [joined],
          )
        ).rows;
        assert.deepEqual(
          attempts.map((row) => row.attempts),
          [1],
        );
        const proofs = (
          await db.pool.query(
            "SELECT resource,proof->>'physicalRows' AS rows FROM moria.runtime_snapshot_candidates WHERE execution=$1 ORDER BY resource",
            [joined],
          )
        ).rows;
        assert.deepEqual(proofs.map((row) => row.rows).sort(), ['10', '6000']);
        const sandbox = (
          await db.pool.query(
            'SELECT state,cleaned,cpu_observed_ns,memory_observed_bytes FROM moria.runtime_sandbox_attempts WHERE execution=$1',
            [joined],
          )
        ).rows[0];
        assert.equal(sandbox.state, 'sealed');
        assert.equal(sandbox.cleaned, true);
        assert.ok(BigInt(sandbox.cpu_observed_ns) > 0n);
        assert.ok(BigInt(sandbox.memory_observed_bytes) > 0n);
        assert.ok(BigInt(sandbox.memory_observed_bytes) <= 512n * 1024n * 1024n);
        const io = (
          await db.pool.query(
            "SELECT payload FROM moria.runtime_execution_events WHERE execution=$1 AND kind='engineIo'",
            [joined],
          )
        ).rows[0].payload;
        assert.ok(BigInt(io.calls) > 32n, JSON.stringify(io));
        assert.equal(io.outputs, 2);
        assert.equal(io.metrics.inputRowsEncoded, '18000');
        assert.equal(io.metrics.outputRowsAppended, '6010');
        const events = (await request(`/api/v1/pipeline-executions/${joined}/events`)).value.events;
        assert.ok(events.length >= 12, JSON.stringify(events));
        const sequences = events.map((event: any) => BigInt(event.sequence));
        assert.deepEqual(
          sequences,
          [...sequences].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0)),
        );
        const after = events[8].sequence;
        const page = (await request(`/api/v1/pipeline-executions/${joined}/events?after=${after}`))
          .value;
        assert.deepEqual(
          page.events.map((event: any) => event.sequence),
          events.slice(9).map((event: any) => event.sequence),
        );
        assert.equal(page.after, events.at(-1).sequence);
        assert.equal((await invoke('Join', 'join-once', 200)).id, joined);
      },
    );
    await t.test(
      'public SDK and CLI replay one native receipt without running code again',
      async () => {
        const sdk = pipelinesClient(
          createTransport({
            baseUrl: api,
            fetch: (input, init) =>
              fetch(input, {
                ...init,
                headers: { ...init?.headers, authorization: `Bearer ${user.token}` },
              }),
          }),
        );
        assert.equal((await sdk.get(joined)).status, 'succeeded');
        assert.equal(
          (await sdk.invoke(id('Transform', 'Join'), { idempotencyKey: 'join-once', wait: false }))
            .id,
          joined,
        );
        const cli = await execute(
          process.execPath,
          ['--import', 'tsx', 'apps/cli/src/main.ts', 'pipelines', 'get', joined, '--url', api],
          { env: { PATH: process.env.PATH, MORIA_API_TOKEN: user.token } },
        );
        assert.equal(JSON.parse(cli.stdout).id, joined);
        assert.equal(
          (
            await db.pool.query(
              'SELECT attempts FROM moria.runtime_execution_tasks WHERE execution=$1',
              [joined],
            )
          ).rows[0].attempts,
          1,
        );
      },
    );
    await t.test(
      'a second actual guest verifies published Parquet values beyond JS precision through native grants',
      async () => {
        const accepted = await invoke('Verify', 'verify-once'),
          result = await terminal(accepted.id);
        assert.equal(
          result.status,
          'succeeded',
          JSON.stringify({
            result,
            events: (await request(`/api/v1/pipeline-executions/${accepted.id}/events`)).value,
          }),
        );
        const events = (await request(`/api/v1/pipeline-executions/${accepted.id}/events`)).value
          .events;
        assert.ok(
          events.some(
            (event: any) =>
              event.type === 'log' && event.payload.message.includes('5404319552844597600'),
          ),
          JSON.stringify(events),
        );
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.runtime_engine_scratch_reservations WHERE cleaned_at IS NULL',
            )
          ).rows[0].count,
          '0',
        );
      },
    );
    await t.test(
      'canonical UUID and scalar arrays survive DuckDB and Parquet; hard spill exhaustion cleans mode000 files before slot reuse',
      async () => {
        const accepted = await invoke('Confirm', 'confirm-once'),
          result = await terminal(accepted.id);
        assert.equal(
          result.status,
          'succeeded',
          JSON.stringify({
            result,
            events: (await request(`/api/v1/pipeline-executions/${accepted.id}/events`)).value,
          }),
        );
        const events = (await request(`/api/v1/pipeline-executions/${accepted.id}/events`)).value
          .events;
        const event = events.find(
          (event: any) => event.type === 'log' && event.payload.message.includes('hardSpillBytes'),
        );
        assert.ok(event, JSON.stringify(events));
        const probe = JSON.parse(event.payload.message);
        assert.equal(probe.uidArraysVerified, true);
        assert.equal(probe.mode000CleanupRequired, true);
        assert.ok(BigInt(probe.hardSpillBytes) > 64n * 1024n * 1024n);
        assert.ok(BigInt(probe.hardSpillBytes) < 128n * 1024n * 1024n);
        const reservations = (
          await db.pool.query(
            'SELECT slot,cleaned_at FROM moria.runtime_engine_scratch_reservations',
          )
        ).rows;
        assert.equal(reservations.length, 3);
        assert.ok(reservations.every((row) => row.cleaned_at !== null));
        assert.equal(new Set(reservations.map((row) => row.slot)).size, 1);
        assert.ok(workerRssObserved > 0n);
        console.log(
          'PIPELINE_PROOF_METRICS=' +
            JSON.stringify({
              workerRssObservedBytes: workerRssObserved.toString(),
              hostCpu: await hostCpu(),
              guestResources: (
                await db.pool.query(
                  'SELECT cpu_observed_ns AS "cpuObservedNs",memory_observed_bytes AS "memoryObservedBytes",cleaned FROM moria.runtime_sandbox_attempts',
                )
              ).rows,
              io: (
                await db.pool.query(
                  "SELECT payload FROM moria.runtime_execution_events WHERE kind='engineIo'",
                )
              ).rows.map((row) => row.payload),
              hardSpillBytes: probe.hardSpillBytes,
              slotReused: true,
            }),
        );
      },
    );
  },
);

test('native Connector custody and checkpoint publication', { timeout: 300000 }, connectorCustody);
