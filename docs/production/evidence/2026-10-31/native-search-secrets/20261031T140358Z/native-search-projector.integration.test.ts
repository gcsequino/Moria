import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { mkdtemp } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { nativeProjectionCapacity } from '../benchmarks/native-projection.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { createLocalFilesystem } from '../services/filesystem/src/local.js';
import { filesystemOperations } from '../packages/contracts/filesystem.js';

test(
  'registered native search projects full Dataset objects and replays a complete action frame',
  { timeout: 240000 },
  async (t) => {
    const markings = ['projector/Marking/Private', 'projector/Marking/Enterprise'];
    const material = randomUUID();
    const output = await mkdtemp(join(tmpdir(), 'moria-search-projector-'));
    await nativeProjectionCapacity({
      rows: 64,
      rowBytes: 4096,
      markings,
      output,
      onVerified: async (fixture) => {
        const { db, environment, source, target, schema, token, nativeEnv, runtime } = fixture;
        const processes: ReturnType<typeof spawn>[] = [],
          exits: Promise<unknown>[] = [];
        let hold = false,
          entered = () => {},
          release = () => {};
        let barrier = new Promise<void>((resolve) => (entered = resolve));
        let held = new Promise<void>((resolve) => (release = resolve));
        const batches: string[][] = [];
        let providerAborted = 0;
        const provider = createServer(async (request, response) => {
          response.on('close', () => {
            if (!response.writableEnded) providerAborted++;
          });
          assert.equal(request.headers.authorization, `Bearer ${material}`);
          let bytes = '';
          for await (const part of request) {
            bytes += part;
            assert.ok(bytes.length <= 1024 * 1024);
          }
          const input = JSON.parse(bytes);
          batches.push(input.input);
          if (hold) {
            entered();
            await held;
          }
          response.setHeader('content-type', 'application/json');
          response.end(
            JSON.stringify({
              data: input.input.map((_: string, index: number) => ({
                index,
                embedding: input.model === 'fixture-embedding-v2' ? [0, 1, 0] : [1, 0, 0],
              })),
              usage: { prompt_tokens: input.input.length, total_tokens: input.input.length },
            }),
          );
        });
        await new Promise<void>((resolve) => provider.listen(0, '127.0.0.1', resolve));
        t.after(async () => {
          release();
          for (const child of processes) child.kill('SIGKILL');
          await Promise.allSettled(exits);
          await new Promise<void>((resolve) => provider.close(() => resolve()));
        });
        const collection = randomUUID(),
          collectionId = randomUUID(),
          serviceId = randomUUID();
        const authority = new Authority(db.catalog);
        const fsOps = filesystemOperations.map((operation) => `filesystem:${operation}`);
        const beforeSource = await authority.current(environment, 'pipeline', 'capacity');
        const sourceOperations = [...beforeSource.identity.grantedOperations, ...fsOps];
        await authority.setApplication(environment, 'fixture', 'capacity', sourceOperations);
        await authority.setGrants(environment, 'fixture', 'pipeline', 'capacity', sourceOperations);
        const fs = await createLocalFilesystem(db.catalog, join(db.root, 'model-source'));
        const session = await new WorkloadSessions(authority).authenticate('Bearer ' + token);
        const context = { session };
        const home = await fs.management.ensureHome(context);
        const homeRole = await fs.management.role(context, home.home.id);
        const projects = await fs.filesystem.store.get(homeRole!.configuration.projects as string);
        await fs.management.createProject(context, projects.id, 'projector', projects.revision);
        const project = await fs.filesystem.store.resolve(
          `/users/${home.home.name}/projects/projector`,
        );
        const sourceFolder = await fs.filesystem.store.resolve(
          `/users/${home.home.name}/projects/projector/source`,
        );
        const made = await fs.filesystem.apply(context, randomUUID(), [
          {
            op: 'create',
            kind: 'directory',
            parentId: sourceFolder.id,
            expectedParentRevision: sourceFolder.revision,
            name: 'models',
          },
        ]);
        const modelRoot = made.entries.find((entry) => entry.name === 'models')!;
        const write = async (name: string, value: unknown) => {
          const bytes = Buffer.from(JSON.stringify(value));
          const prior = await fs.filesystem.store
            .resolve(`/users/${home.home.name}/projects/projector/source/models/${name}`)
            .catch(() => undefined);
          await fs.filesystem.writeFile(
            context,
            randomUUID(),
            prior
              ? {
                  op: 'write',
                  id: prior.id,
                  expectedRevision: prior.revision,
                  content: {
                    size: bytes.length,
                    digest: `sha256:${createHash('sha256').update(bytes).digest('hex')}`,
                    mediaType: 'application/json',
                  },
                }
              : {
                  op: 'create',
                  parentId: modelRoot.id,
                  expectedParentRevision: (await fs.filesystem.store.get(modelRoot.id)).revision,
                  name,
                  content: {
                    size: bytes.length,
                    digest: `sha256:${createHash('sha256').update(bytes).digest('hex')}`,
                    mediaType: 'application/json',
                  },
                },
            new ReadableStream({
              start(controller) {
                controller.enqueue(bytes);
                controller.close();
              },
            }),
          );
        };
        await write('.ontology.json', {
          version: 1,
          name: 'Projector Models',
          namespace: 'projector',
        });
        for (const name of ['Private', 'Enterprise'])
          await write(`${name}.resource.json`, {
            apiVersion: 'moria/v1alpha2',
            kind: 'Marking',
            metadata: { namespace: 'projector', name },
            spec: { eligibilityGroup: 'fixture', declassifierGroup: 'fixture' },
          });
        await write('Credential.resource.json', {
          apiVersion: 'moria/v1alpha2',
          kind: 'SecretReference',
          metadata: { namespace: 'projector', name: 'Credential' },
          spec: { binding: 'projector-embedding', consumers: ['projector/ModelProvider/Provider'] },
        });
        await write('Destination.resource.json', {
          apiVersion: 'moria/v1alpha2',
          kind: 'Policy',
          metadata: { namespace: 'projector', name: 'Destination' },
          spec: {
            type: 'modelDestination',
            allowedDestinations: ['local-model'],
            allowedMarkings: markings,
          },
        });
        await write('Provider.resource.json', {
          apiVersion: 'moria/v1alpha2',
          kind: 'ModelProvider',
          metadata: { namespace: 'projector', name: 'Provider' },
          spec: {
            adapter: 'openai-compatible-v1',
            endpoint: `http://127.0.0.1:${(provider.address() as { port: number }).port}/v1/`,
            destination: 'local-model',
            destinationPolicy: 'projector/Policy/Destination',
            secret: 'projector/SecretReference/Credential',
          },
        });
        const declareModel = (upstream: string, revision: string) =>
          write('Embedding.resource.json', {
            apiVersion: 'moria/v1alpha2',
            kind: 'Model',
            metadata: { namespace: 'projector', name: 'Embedding' },
            spec: {
              provider: 'projector/ModelProvider/Provider',
              upstreamName: upstream,
              versionLabel: revision,
              mutable: false,
              capabilities: ['embeddings'],
              embeddingDimensions: 3,
            },
          });
        await declareModel('fixture-embedding', 'immutable-fixture-v1');
        await fs.source.register(
          context,
          modelRoot.id,
          (await fs.filesystem.store.get(modelRoot.id)).revision,
          'ontology',
        );
        const publishModel = async () => {
          const release = await fs.source.publish(
            context,
            modelRoot.id,
            (await fs.source.checkout(context, modelRoot.id)).digest,
          );
          await fs.source.apply(
            context,
            (await fs.source.plan(context, modelRoot.id, release.digest)).id,
          );
          return release;
        };
        const released = await publishModel();
        const model = released.resources.find(
          (resource) => resource.reference === 'projector/Model/Embedding',
        )!.id;
        const secret = released.resources.find(
          (resource) => resource.reference === 'projector/SecretReference/Credential',
        )!.id;

        const extra = [
          `${collection}:query`,
          `${collection}:project`,
          `${model}:invoke`,
          `${secret}:use`,
          ...fsOps,
          'edit-search:invoke',
        ];
        const current = await authority.current(environment, 'pipeline', 'capacity');
        const operations = [...current.identity.grantedOperations, ...extra];
        await authority.setApplication(environment, 'fixture', 'capacity', operations);
        await authority.setGrants(environment, 'fixture', 'pipeline', 'capacity', operations);
        await db.pool.query(
          'INSERT INTO moria.native_serving_bindings(environment,resource,object_type_id,revision,definition) VALUES($1,$2,$3,$4,$5)',
          [
            environment,
            target,
            target,
            'objects-v1',
            {
              objectTypeId: target,
              schema,
              policies: {
                resource: target,
                markings: markings,
                properties: schema.properties,
                sourceColumns: schema.properties,
                attributes: {
                  region: { type: 'string', nullable: false },
                  admin: { type: 'boolean', nullable: false },
                },
                row: { op: 'eq', args: [{ field: 'region' }, { principalAttribute: 'region' }] },
              },
            },
          ],
        );
        await db.pool.query(
          'INSERT INTO moria.native_serving_actions(environment,resource,object_resource,revision,definition) VALUES($1,$2,$3,$4,$5)',
          [
            environment,
            'edit-search',
            target,
            'action-v1',
            {
              resource: 'edit-search',
              revision: 'action-v1',
              markings: markings,
              targetParameter: 'uids',
              inputs: {
                uids: { type: 'array', nullable: false, items: schema.properties.uid },
                text: schema.properties.payload,
              },
              condition: { literal: true },
              edits: [{ op: 'set', property: 'payload', value: { parameter: 'text' } }],
            },
          ],
        );
        await db.pool.query(
          'INSERT INTO moria.native_model_bindings(environment,resource,revision,definition) VALUES($1,$2,$3,$4)',
          [
            environment,
            model,
            'model-v1',
            {
              endpointRef: 'SEARCH_PROJECTOR_ENDPOINT',
              secretRef: 'SEARCH_PROJECTOR_SECRET',
              model: 'fixture-embedding',
              modelRevision: 'immutable-fixture-v1',
              resourceClass: 'models',
              destination: 'local-model',
              markings: markings,
              destinationPolicy: {
                allowedDestinations: ['local-model'],
                allowedMarkings: markings,
              },
              capabilities: ['embed'],
              dimensions: 3,
            },
          ],
        );
        const env: NodeJS.ProcessEnv = {
          ...nativeEnv,
          SEARCH_PROJECTOR_SECRET: material,
          SEARCH_PROJECTOR_ENDPOINT: `http://127.0.0.1:${(provider.address() as { port: number }).port}/v1/`,
        };
        const start = (args: string[], input?: unknown, more: NodeJS.ProcessEnv = {}) => {
          const child = spawn(runtime, args, {
            env: { ...env, ...more },
            stdio: ['pipe', 'pipe', 'pipe'],
          });
          processes.push(child);
          let out = '',
            error = '';
          child.stdout.on('data', (part) => {
            out += part;
            assert.ok(out.length <= 8 * 1024 * 1024);
          });
          child.stderr.on('data', (part) => {
            error = (error + part).slice(-65536);
          });
          child.stdin.on('error', () => {});
          child.stdin.end(input === undefined ? '' : JSON.stringify(input));
          const done = new Promise<string>((resolve, reject) => {
            child.once('error', reject);
            child.once('exit', (code) =>
              code === 0
                ? resolve(out)
                : reject(new Error(`Native ${args.join(' ')} failed: ${error}\n${out}`)),
            );
          });
          void done.catch(() => {});
          exits.push(done);
          return { child, done, out: () => out, errors: () => error };
        };
        const cli = async (domain: string, operation: string, input: unknown) =>
          JSON.parse(await start([domain, operation], { token, ...(input as object) }).done);
        const databaseRef = env.MORIA_INDEXING_DATABASE_SECRET_REF!;
        const operatorEnv = {
          [databaseRef]: `postgresql://${db.pool.options.user}:${encodeURIComponent(db.pool.options.password as string)}@127.0.0.1:${db.pool.options.port}/postgres`,
        };
        const approveMaterial = async () => {
          const binding = JSON.parse(
            await start(['secret', 'inspect-model'], { environment, model }, operatorEnv).done,
          ).binding;
          return JSON.parse(
            await start(
              ['secret', 'approve'],
              {
                requestKey: randomUUID(),
                environment,
                binding,
                materialRevision: 'embedding-material-v1',
                markings,
              },
              operatorEnv,
            ).done,
          );
        };
        let approval = await approveMaterial();
        await cli('models', 'probe', { resource: model, input: {} });
        batches.length = 0;
        const serviceOperations = [
          `${collection}:project`,
          `${target}:read`,
          `${source}:read`,
          `${model}:invoke`,
          `${secret}:use`,
          'filesystem:read',
        ];
        await authority.syncPrincipal(environment, 'fixture', 'projector', {
          enabled: true,
          kind: 'workload',
          attributes: { region: 'b', admin: false },
          markings: markings,
        });
        await authority.setApplication(
          environment,
          'fixture',
          'projector-service',
          serviceOperations,
        );
        await authority.setGrants(
          environment,
          'fixture',
          'projector',
          'projector-service',
          serviceOperations,
        );
        let serviceInstance = (
          await db.pool.query(
            'SELECT instance_id FROM moria.instance_principal_bindings WHERE environment=$1 AND principal=$2',
            [environment, 'projector'],
          )
        ).rows[0]?.instance_id as string | undefined;
        if (!serviceInstance) {
          serviceInstance = await fs.filesystem.security.createPrincipal('fixture', 'workload');
          await fs.filesystem.security.bindPrincipal(
            'fixture',
            environment,
            'projector',
            serviceInstance,
          );
        }
        const priorPolicy = (
          await db.pool.query(
            'SELECT revision::text,markings,grants FROM moria.fs_policies WHERE entry_id=$1',
            [project.id],
          )
        ).rows[0];
        await fs.filesystem.security.setPolicy(
          'fixture',
          project.id,
          priorPolicy?.revision ?? '0',
          {
            markings: priorPolicy?.markings ?? [],
            grants: [
              ...(priorPolicy?.grants ?? []),
              {
                subject: { kind: 'principal', id: serviceInstance },
                operations: ['read'],
                inherit: true,
                effect: 'allow',
              },
            ],
          },
        );
        await db.pool.query(
          `INSERT INTO moria.runtime_service_authorities(id,environment,principal,application,operations,markings,domains,resource_classes,incarnation) SELECT $1,$2,'projector','projector-service',$3,ARRAY['projector/Marking/Private','projector/Marking/Enterprise'],ARRAY['search'],ARRAY['search'],incarnation FROM moria.indexing_instance`,
          [serviceId, environment, serviceOperations],
        );
        await db.pool.query(
          "INSERT INTO moria.runtime_service_targets(environment,resource,domain,binding_revision,resource_class,authority) VALUES($1,$2,'search','service-v1','search',$3)",
          [environment, collectionId, serviceId],
        );
        await db.pool.query(
          'INSERT INTO moria.native_search_collections(id,environment,resource,revision,definition,partitions) VALUES($1,$2,$3,$4,$5,2)',
          [
            collectionId,
            environment,
            collection,
            'collection-v1',
            {
              source: target,
              textProperty: 'payload',
              embeddingModel: model,
              chunkSize: 1000,
              chunkOverlap: 100,
              metric: 'cosine',
              searchMode: 'exact',
              markings: markings,
            },
          ],
        );
        const projector = (
          await db.pool.query(
            "INSERT INTO moria.native_search_projector_bindings(revision,environment,collection,collection_id,service_id,service_binding_revision,source_resource,text_property,source_dataset,model_resource,model_destination,markings) VALUES('projector-v1',$1,$2,$3,$4,'service-v1',$5,'payload',$6,$7,'local-model',ARRAY['projector/Marking/Private','projector/Marking/Enterprise']) RETURNING id",
            [environment, collection, collectionId, serviceId, target, source, model],
          )
        ).rows[0].id;
        await db.pool.query(
          'UPDATE moria.native_search_collections SET projector_binding=$3 WHERE environment=$1 AND resource=$2',
          [environment, collection, projector],
        );
        const host = randomUUID();
        const worker = async (operation = 'drain') => {
          const owner = randomUUID();
          await db.pool.query(
            "INSERT INTO moria.runtime_workers(owner,host_id,incarnation,resource_classes,domains,expires_at) SELECT $1,$2,incarnation,ARRAY['search'],ARRAY['search'],clock_timestamp()+interval '1 hour' FROM moria.indexing_instance",
            [owner, host],
          );
          return {
            ...start(['search', operation], undefined, {
              MORIA_SEARCH_WORKER_OWNER: owner,
              MORIA_SEARCH_HOST_ID: host,
            }),
            owner,
          };
        };
        const server = start(['serve']);
        let address = '';
        for (let i = 0; i < 500 && !address; i++) {
          for (const line of server.out().split('\n')) {
            try {
              const event = JSON.parse(line);
              if (event.event === 'serving') address = event.address;
            } catch {}
          }
          if (!address) await delay(10);
        }
        assert.ok(address, 'native serving listener');
        const post = async (path: string, input: unknown, key = randomUUID()) => {
          const response = await fetch(`http://${address}${path}`, {
            method: 'POST',
            headers: {
              authorization: `Bearer ${token}`,
              'content-type': 'application/json',
              'idempotency-key': key,
            },
            body: JSON.stringify(input),
          });
          const value = (await response.json()) as any;
          assert.equal(response.status, 200, JSON.stringify(value));
          return value;
        };
        await t.test(
          'accepted Search pins exact credential authority and never revives after an approval pulse',
          async () => {
            const pending = await post(`/api/v1/search/${collection}/rebuild`, {}, randomUUID());
            const pinned = (
              await db.pool.query(
                'SELECT x.model_secret_approval,b.operations FROM moria.native_search_execution_sources x JOIN moria.runtime_executions e ON e.id=x.execution JOIN moria.runtime_execution_bindings b ON b.id=e.binding WHERE x.execution=$1',
                [pending.execution],
              )
            ).rows[0];
            assert.equal(pinned.model_secret_approval.id, approval.id);
            assert.equal(pinned.model_secret_approval.generation, approval.generation);
            assert.ok(pinned.operations.includes(`${secret}:use`));
            await start(['secret', 'disable'], { id: approval.id }, operatorEnv).done;
            await db.pool.query(
              'UPDATE moria.native_secret_approvals SET enabled=true WHERE id=$1',
              [approval.id],
            );
            const before = batches.length;
            await (
              await worker('once')
            ).done;
            const closed = (
              await db.pool.query('SELECT status,error FROM moria.runtime_executions WHERE id=$1', [
                pending.execution,
              ])
            ).rows[0];
            assert.equal(closed.status, 'failed');
            assert.match(
              closed.error,
              /SECRET_APPROVAL_REVOKED|AUTHORIZATION_CHANGED|JOB_LEASE_LOST/,
            );
            assert.equal(batches.length, before, 'old accepted credential cannot reach provider');
            assert.equal(
              (
                await db.pool.query(
                  'SELECT active_generation FROM moria.native_search_collections WHERE id=$1',
                  [collectionId],
                )
              ).rows[0].active_generation,
              null,
            );
            await start(['execution', 'cancel'], { id: pending.execution }).done;
            approval = await approveMaterial();
          },
        );
        const requestKey = randomUUID();
        const accepted = await post(`/api/v1/search/${collection}/rebuild`, {}, requestKey);
        assert.equal(accepted.state.status, 'queued');
        assert.equal(
          (await post(`/api/v1/search/${collection}/status`, { execution: accepted.execution }))
            .state.status,
          'queued',
        );
        assert.equal(
          (await post(`/api/v1/search/${collection}/rebuild`, {}, requestKey)).generation,
          accepted.generation,
        );
        const origin = (
          await db.pool.query(
            'SELECT e.context,d.origin_kind,d.origin_session_hash FROM moria.runtime_executions e JOIN moria.runtime_job_delegations d ON d.execution=e.id WHERE e.id=$1',
            [accepted.execution],
          )
        ).rows[0];
        assert.equal(origin.context.principal, 'projector');
        assert.equal(origin.context.sessionHash, undefined);
        assert.equal(origin.origin_kind, 'service');
        assert.equal(origin.origin_session_hash, null);
        assert.equal(
          (
            await db.pool.query(
              "SELECT count(*) FROM moria.workload_sessions WHERE environment=$1 AND principal='projector'",
              [environment],
            )
          ).rows[0].count,
          '0',
        );
        hold = true;
        const workers = [await worker(), await worker()];
        await Promise.race([
          barrier,
          Promise.all(workers.map((worker) => worker.done)).then(async (logs) => {
            const state = (
              await db.pool.query('SELECT status,error FROM moria.runtime_executions WHERE id=$1', [
                accepted.execution,
              ])
            ).rows[0];
            throw new Error(
              `Projector ended before provider: ${JSON.stringify({ state, logs, errors: workers.map((worker) => worker.errors()) })}`,
            );
          }),
          delay(30000, undefined, { ref: false }).then(() => {
            throw new Error('Projector did not reach embedding provider');
          }),
        ]);
        const before = (
          await db.pool.query(
            'SELECT active_generation FROM moria.native_search_collections WHERE id=$1',
            [collectionId],
          )
        ).rows[0];
        assert.equal(before.active_generation, null);
        const selected = (
          await db.pool.query(
            "SELECT uid FROM moria.staged_source_rows WHERE stage=(SELECT stage FROM moria.source_generations WHERE environment=$1 AND object_type=$2 ORDER BY revision DESC LIMIT 1) AND value->>'region'='a' ORDER BY uid LIMIT 2",
            [environment, target],
          )
        ).rows.map((row) => row.uid);
        const unicode = 'λ🦀中'.repeat(700) + '\n\n' + 'paragraph '.repeat(130);
        const action = await post('/api/v1/actions/edit-search/invoke', {
          input: { uids: selected, text: unicode },
        });
        hold = false;
        release();
        await Promise.all(workers.map((worker) => worker.done));
        const execution = (
          await db.pool.query(
            'SELECT status,error,result FROM moria.runtime_executions WHERE id=$1',
            [accepted.execution],
          )
        ).rows[0];
        assert.equal(execution.status, 'succeeded', JSON.stringify(execution));
        assert.ok(BigInt(execution.result.visibility.revision) >= BigInt(action.revision));
        assert.equal(
          (
            await db.pool.query(
              'SELECT count(*) FROM moria.native_search_heads WHERE generation=$1',
              [accepted.generation],
            )
          ).rows[0].count,
          '64',
          'service projection must include all regions',
        );
        assert.ok(
          batches.some((batch) => batch.length > 1),
          'embedding requests batch multiple objects',
        );
        assert.ok(batches.every((batch) => batch.length <= 64));
        for (const uid of selected) {
          const chunks = (
            await db.pool.query(
              'SELECT e.text,ch.start_offset,ch.end_offset FROM moria.native_search_heads h JOIN moria.native_search_chunks ch ON ch.document=h.document JOIN moria.native_search_embeddings e ON e.id=ch.embedding WHERE h.generation=$1 AND h.uid=$2 ORDER BY ch.ordinal',
              [accepted.generation, uid],
            )
          ).rows;
          let reconstructed = '';
          for (const [index, chunk] of chunks.entries()) {
            assert.ok(chunk.end_offset - chunk.start_offset <= 1000);
            reconstructed += Array.from(chunk.text as string)
              .slice(index ? 100 : 0)
              .join('');
          }
          assert.equal(reconstructed, unicode);
        }
        const result = await post(`/api/v1/search/${collection}/query`, {
          text: 'query',
          topK: 50,
          minimumToken: execution.result.visibility,
        });
        assert.ok(result.hits.length > 0);
        const visible = new Set(
          (
            await db.pool.query(
              "SELECT uid FROM moria.staged_source_rows WHERE stage=(SELECT stage FROM moria.source_generations WHERE environment=$1 AND object_type=$2 ORDER BY revision DESC LIMIT 1) AND value->>'region'='a'",
              [environment, target],
            )
          ).rows.map((row) => row.uid),
        );
        assert.ok(
          result.hits.every((hit: any) => visible.has(hit.uid)),
          'public policy filters full global projection before ranking',
        );
        const fullSourceBinding = (
          await db.pool.query('SELECT spec FROM moria.runtime_executions WHERE id=$1', [
            accepted.execution,
          ])
        ).rows[0].spec;
        assert.equal(fullSourceBinding.pinnedInputs.length, 1);
        assert.equal(fullSourceBinding.pinnedInputs[0].resource, source);
        assert.match(fullSourceBinding.pinnedInputs[0].snapshot, /^[0-9]+$/);
        await t.test(
          'a fresh background service execution atomically replays later changes',
          async () => {
            const oldHeads = (
              await db.pool.query(
                'SELECT uid,document FROM moria.native_search_heads WHERE generation=$1 AND uid=ANY($2::uuid[]) ORDER BY uid',
                [accepted.generation, selected],
              )
            ).rows;
            const previous = (
              await db.pool.query(
                'SELECT covered_revision::text FROM moria.native_search_generations WHERE id=$1',
                [accepted.generation],
              )
            ).rows[0].covered_revision;
            barrier = new Promise<void>((resolve) => (entered = resolve));
            held = new Promise<void>((resolve) => (release = resolve));
            hold = true;
            const changed = await post('/api/v1/actions/edit-search/invoke', {
              input: { uids: selected, text: unicode + ' later commit' },
            });
            const replayWorker = await worker();
            await Promise.race([
              barrier,
              replayWorker.done.then((logs) => {
                throw new Error(`Replay ended before provider: ${logs} ${replayWorker.errors()}`);
              }),
              delay(30000, undefined, { ref: false }).then(() => {
                throw new Error('Replay provider barrier timed out');
              }),
            ]);
            assert.equal(
              (
                await db.pool.query(
                  'SELECT covered_revision::text FROM moria.native_search_generations WHERE id=$1',
                  [accepted.generation],
                )
              ).rows[0].covered_revision,
              previous,
            );
            assert.deepEqual(
              (
                await db.pool.query(
                  'SELECT uid,document FROM moria.native_search_heads WHERE generation=$1 AND uid=ANY($2::uuid[]) ORDER BY uid',
                  [accepted.generation, selected],
                )
              ).rows,
              oldHeads,
            );
            hold = false;
            release();
            await replayWorker.done;
            const replay = (
              await db.pool.query(
                "SELECT e.id,e.status,e.error,e.result,d.origin_kind FROM moria.native_search_execution_sources x JOIN moria.runtime_executions e ON e.id=x.execution JOIN moria.runtime_job_delegations d ON d.execution=e.id WHERE x.generation=$1 AND x.kind='replay' ORDER BY e.created_at DESC LIMIT 1",
                [accepted.generation],
              )
            ).rows[0];
            assert.equal(replay.status, 'succeeded', JSON.stringify(replay));
            assert.notEqual(replay.id, accepted.execution);
            assert.equal(replay.origin_kind, 'service');
            assert.ok(BigInt(replay.result.visibility.revision) >= BigInt(changed.revision));
            const heads = (
              await db.pool.query(
                'SELECT uid,document FROM moria.native_search_heads WHERE generation=$1 AND uid=ANY($2::uuid[]) ORDER BY uid',
                [accepted.generation, selected],
              )
            ).rows;
            assert.ok(heads.every((row, index) => row.document !== oldHeads[index].document));
            assert.equal(
              (
                await db.pool.query(
                  "SELECT expires_at='infinity'::timestamptz AS retained FROM moria.runtime_change_pins WHERE id=$1 AND owner=$1",
                  [accepted.generation],
                )
              ).rows[0].retained,
              true,
            );
            const searched = await post(`/api/v1/search/${collection}/query`, {
              text: 'query',
              topK: 50,
              minimumToken: replay.result.visibility,
            });
            assert.equal(searched.visibility.generation, accepted.generation);
          },
        );
        await t.test(
          'a complete replacement generation reuses persisted embedding bytes',
          async () => {
            const before = batches.length;
            const rebuilt = await cli('search', 'rebuild', {
              resource: collection,
              key: randomUUID(),
            });
            await (
              await worker()
            ).done;
            const state = (
              await db.pool.query('SELECT status,error FROM moria.runtime_executions WHERE id=$1', [
                rebuilt.execution,
              ])
            ).rows[0];
            assert.equal(state.status, 'succeeded', JSON.stringify(state));
            assert.notEqual(rebuilt.generation, accepted.generation);
            assert.equal(
              batches.length,
              before,
              'cached replacement must not invoke the provider again',
            );
            assert.equal(
              (
                await db.pool.query(
                  'SELECT active_generation FROM moria.native_search_collections WHERE id=$1',
                  [collectionId],
                )
              ).rows[0].active_generation,
              rebuilt.generation,
            );
            assert.equal(
              (
                await db.pool.query('SELECT count(*) FROM moria.runtime_change_pins WHERE id=$1', [
                  accepted.generation,
                ])
              ).rows[0].count,
              '0',
            );
            accepted.generation = rebuilt.generation;
          },
        );
        await t.test(
          'a real expired attempt is fenced and a replacement reuses the warm cache',
          async () => {
            const before = batches.length;
            const rebuilt = await post(`/api/v1/search/${collection}/rebuild`, {});
            const blocker = await db.pool.connect();
            await blocker.query('BEGIN');
            await blocker.query(
              'LOCK TABLE moria.native_search_embeddings IN ACCESS EXCLUSIVE MODE',
            );
            const paused = await worker('once');
            let stopped = false;
            try {
              let waiting = false;
              for (let attempt = 0; attempt < 1000 && !waiting; attempt++) {
                waiting = (
                  await db.pool.query(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT id,text_digest,text FROM moria.native_search_embeddings%') AS waiting",
                  )
                ).rows[0].waiting;
                if (!waiting) await delay(5);
              }
              assert.ok(waiting, 'real source cache lookup must reach PostgreSQL relation lock');
              assert.equal(paused.child.kill('SIGSTOP'), true);
              stopped = true;
              const old = (
                await db.pool.query(
                  "SELECT id,fence,lease_until FROM moria.runtime_execution_tasks WHERE execution=$1 AND owner=$2 AND status='running'",
                  [rebuilt.execution, paused.owner],
                )
              ).rows[0];
              assert.ok(old);
              await blocker.query('COMMIT');
              let expired = false;
              for (let attempt = 0; attempt < 360 && !expired; attempt++) {
                expired = (
                  await db.pool.query(
                    'SELECT lease_until<=clock_timestamp() AS expired FROM moria.runtime_execution_tasks WHERE id=$1',
                    [old.id],
                  )
                ).rows[0].expired;
                if (!expired) await delay(100);
              }
              assert.ok(expired, 'actual database-clock lease expiry');
              const replacement = await worker();
              let fenced = false;
              for (let attempt = 0; attempt < 1500 && !fenced; attempt++) {
                fenced = (
                  await db.pool.query(
                    'SELECT EXISTS(SELECT 1 FROM moria.runtime_execution_attempts WHERE task=$1 AND fence>$2 AND owner<>$3) AS fenced',
                    [old.id, old.fence, paused.owner],
                  )
                ).rows[0].fenced;
                if (!fenced) await delay(10);
              }
              assert.ok(
                fenced,
                'registered replacement must claim the real task at a greater fence',
              );
              assert.equal(paused.child.kill('SIGCONT'), true);
              stopped = false;
              await Promise.all([paused.done, replacement.done]);
              const result = await post(`/api/v1/search/${collection}/status`, {
                execution: rebuilt.execution,
              });
              assert.equal(result.state.status, 'succeeded', JSON.stringify(result));
              assert.equal(
                batches.length,
                before,
                'takeover must reuse committed embeddings without provider rebilling',
              );
              accepted.generation = rebuilt.generation;
            } finally {
              if (stopped) paused.child.kill('SIGCONT');
              await blocker.query('ROLLBACK').catch(() => {});
              blocker.release();
            }
          },
        );
        await t.test(
          'a model binding change creates a complete separately pinned generation',
          async () => {
            const before = batches.length;
            const previous = accepted.generation;
            const definition = (
              await db.pool.query(
                'SELECT definition FROM moria.native_model_bindings WHERE environment=$1 AND resource=$2',
                [environment, model],
              )
            ).rows[0].definition;
            await declareModel('fixture-embedding-v2', 'immutable-fixture-v2');
            await publishModel();
            await db.pool.query(
              "UPDATE moria.native_model_bindings SET revision='model-v2',definition=$3 WHERE environment=$1 AND resource=$2",
              [
                environment,
                model,
                {
                  ...definition,
                  model: 'fixture-embedding-v2',
                  modelRevision: 'immutable-fixture-v2',
                },
              ],
            );
            approval = await approveMaterial();
            const invalid = await fetch(`http://${address}/api/v1/search/${collection}/query`, {
              method: 'POST',
              headers: {
                authorization: `Bearer ${token}`,
                'content-type': 'application/json',
                'idempotency-key': randomUUID(),
              },
              body: JSON.stringify({ text: 'query', topK: 5 }),
            });
            assert.equal(invalid.status, 409);
            assert.equal(((await invalid.json()) as any).error.code, 'SEARCH_MODEL_CHANGED');
            assert.equal(
              batches.length,
              before,
              'stale model generation must fail before inference',
            );
            await cli('models', 'probe', { resource: model, input: {} });
            await (
              await worker()
            ).done;
            const active = (
              await db.pool.query(
                'SELECT active_generation FROM moria.native_search_collections WHERE id=$1',
                [collectionId],
              )
            ).rows[0].active_generation;
            assert.notEqual(active, previous);
            accepted.generation = active;
            const pins = (
              await db.pool.query(
                'SELECT DISTINCT e.model_fingerprint,e.embedding::text AS embedding FROM moria.native_search_heads h JOIN moria.native_search_chunks c ON c.document=h.document JOIN moria.native_search_embeddings e ON e.id=c.embedding WHERE h.generation=$1',
                [active],
              )
            ).rows;
            assert.equal(pins.length, 1);
            assert.equal(pins[0].embedding, '[0,1,0]');
            assert.equal(
              (
                await db.pool.query(
                  'SELECT model_revision FROM moria.native_search_generations WHERE id=$1',
                  [active],
                )
              ).rows[0].model_revision,
              'immutable-fixture-v2',
            );
            assert.ok(
              batches.length > before + 1,
              'new model projection must use its own provider vectors',
            );
          },
        );
        await t.test(
          'revoking the exact projector purpose closes held upstream work without visibility',
          async () => {
            const before = (
              await db.pool.query(
                'SELECT covered_revision::text FROM moria.native_search_generations WHERE id=$1',
                [accepted.generation],
              )
            ).rows[0].covered_revision;
            const oldHeads = (
              await db.pool.query(
                'SELECT uid,document FROM moria.native_search_heads WHERE generation=$1 AND uid=ANY($2::uuid[]) ORDER BY uid',
                [accepted.generation, selected],
              )
            ).rows;
            barrier = new Promise<void>((resolve) => (entered = resolve));
            held = new Promise<void>((resolve) => (release = resolve));
            hold = true;
            await post('/api/v1/actions/edit-search/invoke', {
              input: { uids: selected, text: 'new purpose revocation ' + unicode },
            });
            const revoked = await worker();
            await Promise.race([
              barrier,
              revoked.done.then((logs) => {
                throw new Error(
                  `Revocation run ended before provider: ${logs} ${revoked.errors()}`,
                );
              }),
              delay(30000, undefined, { ref: false }).then(() => {
                throw new Error('Revocation provider barrier timed out');
              }),
            ]);
            const abortedBefore = providerAborted;
            await db.pool.query(
              'UPDATE moria.native_search_projector_bindings SET enabled=false WHERE id=$1',
              [projector],
            );
            await Promise.race([
              revoked.done,
              delay(10000, undefined, { ref: false }).then(() => {
                throw new Error('Revoked projector did not stop held provider');
              }),
            ]);
            const failed = (
              await db.pool.query(
                "SELECT e.status,e.error FROM moria.native_search_execution_sources x JOIN moria.runtime_executions e ON e.id=x.execution WHERE x.generation=$1 AND x.kind='replay' ORDER BY e.created_at DESC LIMIT 1",
                [accepted.generation],
              )
            ).rows[0];
            assert.equal(failed.status, 'failed');
            assert.equal(failed.error, 'FULL_SOURCE_PURPOSE_REVOKED');
            for (let attempt = 0; attempt < 300 && providerAborted === abortedBefore; attempt++)
              await delay(10);
            assert.ok(
              providerAborted > abortedBefore,
              'current projector revocation must close the real upstream socket',
            );
            assert.equal(
              (
                await db.pool.query(
                  'SELECT covered_revision::text FROM moria.native_search_generations WHERE id=$1',
                  [accepted.generation],
                )
              ).rows[0].covered_revision,
              before,
            );
            assert.deepEqual(
              (
                await db.pool.query(
                  'SELECT uid,document FROM moria.native_search_heads WHERE generation=$1 AND uid=ANY($2::uuid[]) ORDER BY uid',
                  [accepted.generation, selected],
                )
              ).rows,
              oldHeads,
            );
            hold = false;
            release();
          },
        );
      },
    });
  },
);
