import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { promisify } from 'node:util';
import type { TestContext } from 'node:test';
import type { database } from '../catalog-storage/database.js';
import type { Authority } from '../../../services/security/src/authority.js';

const defaultMarkings = ['permanent/Marking/Private', 'permanent/Marking/Secret'];
/** A real registered projector reads the managed Source alias; ordinary row policy stays narrower. */
export async function prepareSourceStoreProjector(
  write: (name: string, value: unknown) => Promise<void>,
  t: TestContext,
  options: {
    namespace?: string;
    markings?: string[];
    textProperty?: string;
    overlayText?: string;
  } = {},
) {
  const {
    namespace = 'permanent',
    markings = defaultMarkings,
    textProperty = 'label',
    overlayText = 'source-store overlay',
  } = options;
  const received: string[] = [];
  const provider = createServer(async (request, response) => {
    try {
      let bytes = '';
      for await (const part of request) {
        bytes += part;
        assert.ok(Buffer.byteLength(bytes) <= 1024 * 1024);
      }
      const input = JSON.parse(bytes);
      assert.ok(Array.isArray(input.input));
      received.push(...input.input);
      response.setHeader('content-type', 'application/json');
      response.end(
        JSON.stringify({
          data: input.input.map((_: string, index: number) => ({ index, embedding: [1, 0, 0] })),
          usage: { prompt_tokens: input.input.length, total_tokens: input.input.length },
        }),
      );
    } catch (error) {
      response.statusCode = 500;
      response.end(String(error));
    }
  });
  await new Promise<void>((resolve) => provider.listen(0, '127.0.0.1', resolve));
  t.after(async () => {
    provider.closeAllConnections();
    await new Promise<void>((resolve) => provider.close(() => resolve()));
  });
  const endpoint = `http://127.0.0.1:${(provider.address() as { port: number }).port}/v1/`;
  for (const [kind, name, spec] of [
    [
      'Policy',
      'SourceDestination',
      {
        type: 'modelDestination',
        allowedDestinations: ['source-fixture'],
        allowedMarkings: markings,
      },
    ],
    [
      'ModelProvider',
      'SourceProvider',
      {
        adapter: 'openai-compatible-v1',
        endpoint,
        destination: 'source-fixture',
        destinationPolicy: namespace + '/Policy/SourceDestination',
      },
    ],
    [
      'Model',
      'SourceEmbedding',
      {
        provider: namespace + '/ModelProvider/SourceProvider',
        upstreamName: 'source-embedding',
        versionLabel: 'immutable-source-v1',
        mutable: false,
        capabilities: ['embeddings'],
        embeddingDimensions: 3,
      },
    ],
  ] as const)
    await write(`${name}.resource.json`, {
      apiVersion: 'moria/v1alpha2',
      kind,
      metadata: { namespace, name },
      spec,
    });
  const binding = {
    endpointRef: 'SOURCE_STORE_MODEL_ENDPOINT',
    model: 'source-embedding',
    modelRevision: 'immutable-source-v1',
    resourceClass: 'models',
    destination: 'source-fixture',
    markings: [markings[0]],
    destinationPolicy: {
      allowedDestinations: ['source-fixture'],
      allowedMarkings: markings,
    },
    capabilities: ['embed'],
    dimensions: 3,
  };
  return {
    env: { SOURCE_STORE_MODEL_ENDPOINT: endpoint },
    binding,
    async verify(input: {
      db: Awaited<ReturnType<typeof database>>;
      authority: Authority;
      environment: string;
      principal: string;
      application: string;
      target: string;
      source: string;
      model: string;
      token: string;
      base: string;
      runtime: string;
      env: Record<string, string | undefined>;
      expectedRows: number;
      deletedUid: string;
      overlayUid: string;
      store: string;
      installedModel?: boolean;
      afterBuild?: () => Promise<void>;
    }) {
      const {
        db,
        authority,
        environment,
        principal,
        application,
        target,
        source,
        model,
        token,
        base,
        runtime,
        env,
      } = input;
      const run = async (args: string[], value?: unknown) => {
        // The real CLI reads one bounded JSON request from stdin.
        const { spawn } = await import('node:child_process');
        return await new Promise<string>((resolve, reject) => {
          const child = spawn(runtime, args, { env, stdio: ['pipe', 'pipe', 'pipe'] });
          let out = '',
            error = '';
          const timer = setTimeout(() => {
            child.kill('SIGKILL');
            reject(new Error('Source projector command deadline'));
          }, 150000);
          child.stdout.on('data', (chunk) => {
            out += chunk;
            assert.ok(out.length <= 16 * 1024 * 1024);
          });
          child.stderr.on('data', (chunk) => (error = (error + chunk).slice(-65536)));
          child.stdin.on('error', () => {});
          child.once('error', (error) => {
            clearTimeout(timer);
            reject(error);
          });
          child.once('exit', (code) => {
            clearTimeout(timer);
            code === 0 ? resolve(out) : reject(new Error(`${args.join('/')}: ${error}\n${out}`));
          });
          child.stdin.end(value === undefined ? '' : JSON.stringify(value));
        });
      };
      const collection = randomUUID(),
        service = randomUUID(),
        host = randomUUID(),
        owner = randomUUID();
      const serviceOperations = [
        `${collection}:project`,
        `${target}:read`,
        `${source}:read`,
        `${model}:invoke`,
        'filesystem:read',
      ];
      const state = await authority.current(environment, principal, application),
        operations = [...new Set([...state.identity.grantedOperations, ...serviceOperations])];
      await authority.setApplication(environment, 'fixture', application, operations);
      await authority.setGrants(environment, 'fixture', principal, application, operations);
      if (input.installedModel) {
        const installed = (
          await db.pool.query(
            'SELECT definition FROM moria.native_model_bindings WHERE environment=$1 AND resource=$2 AND enabled',
            [environment, model],
          )
        ).rows[0];
        assert.deepEqual(installed?.definition, binding);
      } else {
        await db.pool.query(
          'INSERT INTO moria.native_model_bindings(environment,resource,revision,definition) VALUES($1,$2,$3,$4)',
          [environment, model, 'source-model-v1', binding],
        );
      }
      const probe = JSON.parse(
        await run(['models', 'probe'], { token, resource: model, input: {} }),
      );
      assert.deepEqual(probe.capabilities, ['embed'], JSON.stringify(probe));
      received.length = 0;
      await db.pool.query(
        "INSERT INTO moria.runtime_service_authorities(id,environment,principal,application,operations,markings,domains,resource_classes,incarnation) SELECT $1,$2,$3,$4,$5,$6,ARRAY['search'],ARRAY['search'],incarnation FROM moria.indexing_instance",
        [service, environment, principal, application, serviceOperations, markings],
      );
      await db.pool.query(
        "INSERT INTO moria.runtime_service_targets(environment,resource,domain,binding_revision,resource_class,authority) VALUES($1,$2,'search','source-search-v1','search',$3)",
        [environment, collection, service],
      );
      await db.pool.query(
        'INSERT INTO moria.native_search_collections(id,environment,resource,revision,definition,partitions) VALUES($1,$2,$3,$4,$5,1)',
        [
          collection,
          environment,
          collection,
          'source-collection-v1',
          {
            source: target,
            textProperty,
            embeddingModel: model,
            chunkSize: 1000,
            chunkOverlap: 100,
            metric: 'cosine',
            searchMode: 'exact',
            markings: [markings[0]],
          },
        ],
      );
      const projector = (
        await db.pool.query(
          "INSERT INTO moria.native_search_projector_bindings(revision,environment,collection,collection_id,service_id,service_binding_revision,source_resource,text_property,source_dataset,model_resource,model_destination,markings) VALUES('source-projector-v1',$1,$2,$2::text::uuid,$3,'source-search-v1',$4,$5,$6,$7,'source-fixture',$8) RETURNING id",
          [environment, collection, service, target, textProperty, source, model, [markings[0]]],
        )
      ).rows[0].id;
      await db.pool.query(
        'UPDATE moria.native_search_collections SET projector_binding=$2 WHERE id=$1',
        [collection, projector],
      );
      const head = (
        await db.pool.query(
          'SELECT g.stage,g.revision::text FROM moria.source_generations g WHERE g.environment=$1 AND g.object_type=$2 ORDER BY g.revision DESC LIMIT 1',
          [environment, target],
        )
      ).rows[0];
      assert.equal(head.stage, input.store, JSON.stringify({ expectedStore: input.store, head }));
      const response = await fetch(`${base}/api/v1/search/${collection}/rebuild`, {
          method: 'POST',
          headers: {
            authorization: `Bearer ${token}`,
            'content-type': 'application/json',
            'idempotency-key': randomUUID(),
          },
          body: '{}',
        }),
        accepted = await response.json();
      assert.equal(response.status, 200, JSON.stringify(accepted));
      const pinned = (
        await db.pool.query(
          'SELECT source_stage,source_revision::text FROM moria.native_search_rebuilds WHERE generation=$1',
          [accepted.generation],
        )
      ).rows[0];
      assert.equal(
        pinned.source_stage,
        input.store,
        JSON.stringify({ expectedStore: input.store, head, pinned }),
      );
      assert.equal(pinned.source_revision, head.revision);
      assert.equal(
        (
          await db.pool.query(
            'SELECT store FROM moria.native_source_generation_aliases WHERE environment=$1 AND object_type=$2 AND revision=$3',
            [environment, target, pinned.source_revision],
          )
        ).rows[0].store,
        input.store,
      );
      await db.pool.query(
        "INSERT INTO moria.runtime_workers(owner,host_id,incarnation,resource_classes,domains,expires_at) SELECT $1,$2,incarnation,ARRAY['search'],ARRAY['search'],clock_timestamp()+interval '1 hour' FROM moria.indexing_instance",
        [owner, host],
      );
      await promisify(execFile)(runtime, ['search', 'drain'], {
        env: { ...env, MORIA_SEARCH_WORKER_OWNER: owner, MORIA_SEARCH_HOST_ID: host },
        timeout: 150000,
        maxBuffer: 16 * 1024 * 1024,
      });
      const execution = (
        await db.pool.query(
          'SELECT status,error,result FROM moria.runtime_executions WHERE id=$1',
          [accepted.execution],
        )
      ).rows[0];
      assert.equal(execution.status, 'succeeded', JSON.stringify(execution));
      assert.equal(
        (
          await db.pool.query(
            'SELECT count(*)::text FROM moria.native_search_heads WHERE generation=$1',
            [accepted.generation],
          )
        ).rows[0].count,
        String(input.expectedRows),
      );
      assert.equal(
        (
          await db.pool.query(
            'SELECT count(*)::text FROM moria.native_search_heads WHERE generation=$1 AND uid=$2',
            [accepted.generation, input.deletedUid],
          )
        ).rows[0].count,
        '0',
      );
      const overlay = (
        await db.pool.query(
          'SELECT e.text FROM moria.native_search_heads h JOIN moria.native_search_chunks c ON c.document=h.document JOIN moria.native_search_embeddings e ON e.id=c.embedding WHERE h.generation=$1 AND h.uid=$2',
          [accepted.generation, input.overlayUid],
        )
      ).rows;
      assert.deepEqual(
        overlay.map((row) => row.text),
        [overlayText],
      );
      assert.ok(received.includes(overlayText));
      assert.ok(
        received.length >= input.expectedRows,
        'registered full Source includes rows hidden by ordinary row policy',
      );
      if (input.afterBuild) {
        const embedded = received.length;
        await input.afterBuild();
        await promisify(execFile)(runtime, ['search', 'drain'], {
          env: { ...env, MORIA_SEARCH_WORKER_OWNER: owner, MORIA_SEARCH_HOST_ID: host },
          timeout: 150000,
          maxBuffer: 16 * 1024 * 1024,
        });
        const progress = (
          await db.pool.query(
            "SELECT c.active_generation,g.covered_revision::text,e.revision::text,(SELECT count(*)::int FROM moria.native_search_rebuilds r JOIN moria.native_search_generations candidate ON candidate.id=r.generation WHERE candidate.environment=c.environment AND candidate.resource=c.resource) AS rebuilds,(SELECT count(*)::int FROM moria.native_search_execution_sources x JOIN moria.runtime_executions ex ON ex.id=x.execution WHERE x.generation=g.id AND x.kind='replay' AND ex.status='succeeded') AS replays FROM moria.native_search_collections c JOIN moria.native_search_generations g ON g.id=c.active_generation JOIN moria.environments e ON e.id=c.environment WHERE c.id=$1",
            [collection],
          )
        ).rows[0];
        assert.equal(progress.active_generation, accepted.generation);
        assert.equal(progress.covered_revision, progress.revision);
        assert.equal(progress.rebuilds, 1);
        assert.ok(progress.replays > 0, JSON.stringify(progress));
        assert.equal(received.length, embedded, 'metadata-only Source advances do not embed again');
      }
      return {
        execution: accepted.execution,
        generation: accepted.generation,
        store: pinned.source_stage,
        rows: input.expectedRows,
      };
    },
  };
}
