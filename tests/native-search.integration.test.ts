import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import test from 'node:test';
import { fingerprint } from '../packages/compiler/src/index.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { ObjectStore } from '../services/ontology/src/store.js';
import { database } from './fixtures/catalog-storage/database.js';

test(
  'native exact retrieval ranks a trusted sealed fixture generation after current protection',
  { timeout: 180000 },
  async (t) => {
    const db = await database(),
      environment = randomUUID(),
      objectType = randomUUID(),
      principal = 'search-user',
      application = 'search-app';
    const processes: ReturnType<typeof spawn>[] = [],
      exits: Promise<unknown>[] = [];
    let release = () => {},
      signal = () => {},
      hold = false,
      barrier = new Promise<void>((resolve) => (signal = resolve)),
      held = new Promise<void>((resolve) => (release = resolve)),
      providerCalls = 0;
    const vectors: Record<string, number[]> = {
      'West α': [0.8, 0.6, 0],
      'West β': [0.8, -0.6, 0],
      'West lower': [0.4, 0.9, 0],
    };
    const provider = createServer(async (req, res) => {
      providerCalls++;
      let text = '';
      for await (const chunk of req) text += chunk;
      const input = JSON.parse(text);
      if (hold) {
        signal();
        await held;
      }
      res.setHeader('content-type', 'application/json');
      res.end(
        JSON.stringify({
          data: input.input.map((text: string, index: number) => ({
            index,
            embedding: vectors[text] ?? [1, 0, 0],
          })),
          usage: { prompt_tokens: 1, total_tokens: 1 },
        }),
      );
    });
    await new Promise<void>((resolve) => provider.listen(0, '127.0.0.1', resolve));
    t.after(async () => {
      release();
      for (const child of processes) child.kill('SIGKILL');
      await Promise.allSettled(exits);
      await new Promise<void>((resolve) => provider.close(() => resolve()));
      await db.close();
    });
    await db.catalog.ensureEnvironment(environment);
    const authority = new Authority(db.catalog),
      sessions = new WorkloadSessions(authority),
      store = new ObjectStore(db.catalog);
    const operations = ['objects:read', 'collection:query', 'model:invoke'];
    await authority.syncPrincipal(environment, 'fixture', principal, {
      kind: 'workload',
      enabled: true,
      markings: ['private'],
      attributes: { region: 'West', admin: false },
    });
    await authority.setApplication(environment, 'fixture', application, operations);
    await authority.setGrants(environment, 'fixture', principal, application, operations);
    const issued = await sessions.mint({ environment, principal, application }, 'fixture');
    const properties = {
      uid: { type: 'uid' as const, nullable: false },
      region: { type: 'string' as const, nullable: false },
      text: { type: 'string' as const, nullable: false },
      secret: { type: 'string' as const, nullable: false },
      visible: { type: 'boolean' as const, nullable: false },
    };
    const schema = { primaryKey: 'uid', properties };
    const binding = {
      objectTypeId: objectType,
      schema,
      policies: {
        resource: 'objects',
        markings: ['private'],
        properties,
        sourceColumns: properties,
        attributes: {
          region: properties.region,
          admin: { type: 'boolean' as const, nullable: false },
        },
        row: { op: 'eq', args: [{ field: 'region' }, { principalAttribute: 'region' }] },
        objectRead: { field: 'visible' },
        propertyRead: { secret: { principalAttribute: 'admin' } },
      },
    };
    const rows = await Promise.all(
      ['West α', 'West β', 'West lower', 'East hidden', 'West denied'].map(async (text, index) => ({
        uid: await db.catalog.resolveIdentity(`search-${environment}`, String(index)),
        region: index === 3 ? 'East' : 'West',
        text,
        secret: 'classified',
        visible: index !== 4,
      })),
    );
    const publication = await store.stage(
      environment,
      'objects',
      schema,
      rows,
      (await authority.current(environment, principal, application)).epoch,
      '0',
      undefined,
      ['private'],
    );
    await store.publishStage(publication);
    await db.pool.query(
      'INSERT INTO moria.native_serving_bindings(environment,resource,object_type_id,revision,definition) VALUES($1,$2,$3,$4,$5)',
      [environment, 'objects', objectType, 'objects-v1', binding],
    );
    const model = {
      endpointRef: 'SEARCH_MODEL_ENDPOINT',
      model: 'fixture-embedding',
      modelRevision: 'fixture-v1',
      resourceClass: 'models',
      destination: 'local-model',
      markings: ['private'],
      destinationPolicy: { allowedDestinations: ['local-model'], allowedMarkings: ['private'] },
      capabilities: ['embed'],
      dimensions: 3,
    };
    await db.pool.query(
      'INSERT INTO moria.native_model_bindings(environment,resource,revision,definition) VALUES($1,$2,$3,$4)',
      [environment, 'model', 'v1', model],
    );
    const role = `search_${randomUUID().replaceAll('-', '')}`,
      password = randomUUID();
    await db.pool.query(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    await db.pool.query(`ALTER ROLE ${role} SET temp_file_limit='256MB'`);
    await db.pool.query(
      `GRANT USAGE ON SCHEMA moria TO ${role}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role}`,
    );
    const nativeEnv = {
      PATH: '/usr/local/bin:/usr/bin:/bin',
      MORIA_INDEXING_DATABASE_SECRET_REF: 'SEARCH_DATABASE',
      SEARCH_DATABASE: `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`,
      MORIA_INDEXING_DATABASE_TLS: 'disable',
      SEARCH_MODEL_ENDPOINT: `http://127.0.0.1:${(provider.address() as { port: number }).port}/v1/`,
    };
    const run = (domain: string, command: string, value: unknown) => {
      const child = spawn(
        process.env.MORIA_NATIVE_BINARY ?? '/usr/local/bin/moria-runtime',
        [domain, command],
        { env: nativeEnv, stdio: ['pipe', 'pipe', 'pipe'] },
      );
      processes.push(child);
      exits.push(new Promise((resolve) => child.once('exit', resolve)));
      let stdout = '',
        stderr = '';
      child.stdout.on('data', (chunk) => (stdout += chunk));
      child.stderr.on('data', (chunk) => (stderr += chunk));
      child.stdin.end(JSON.stringify({ token: issued.token, ...(value as any) }));
      const done = new Promise<any>((resolve, reject) => {
        child.once('error', reject);
        child.once('exit', (code) =>
          code === 0 ? resolve(JSON.parse(stdout)) : reject(new Error(stderr)),
        );
      });
      void done.catch(() => {});
      return done;
    };
    await run('models', 'probe', { resource: 'model', key: randomUUID(), input: {} });
    const profile = await run('models', 'capabilities', { resource: 'model' });
    const embedded = await run('models', 'embed', {
      resource: 'model',
      key: randomUUID(),
      input: { input: rows.map((row) => row.text) },
    });
    const collection = {
      source: 'objects',
      textProperty: 'text',
      embeddingModel: 'model',
      chunkSize: 1000,
      chunkOverlap: 100,
      metric: 'cosine',
      searchMode: 'exact',
      markings: ['private'],
    };
    const generation = randomUUID(),
      revision = (
        await db.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [environment])
      ).rows[0].revision;
    const incarnation = (await db.pool.query('SELECT incarnation FROM moria.indexing_instance'))
      .rows[0].incarnation;
    await db.pool.query(
      'INSERT INTO moria.native_search_collections(environment,resource,revision,definition) VALUES($1,$2,$3,$4)',
      [environment, 'collection', 'v1', collection],
    );
    const servingBinding = (
      await db.pool.query(
        'SELECT revision,authority_generation::text AS generation FROM moria.native_serving_bindings WHERE environment=$1 AND resource=$2',
        [environment, 'objects'],
      )
    ).rows[0];
    await db.pool.query(
      `INSERT INTO moria.native_search_generations(id,environment,resource,collection_revision,definition,object_binding_revision,model_fingerprint,model_revision,dimensions,chunker_revision,incarnation,base_revision,covered_revision,status)
    VALUES($1,$2,'collection','v1',$3,$8,$4,$5,3,'unicode1000-overlap100-paragraph-v1',$6,$7,$7,'active')`,
      [
        generation,
        environment,
        collection,
        profile.modelBindingFingerprint,
        model.modelRevision,
        incarnation,
        revision,
        fingerprint([servingBinding.revision, servingBinding.generation]),
      ],
    );
    for (const [index, row] of rows.entries()) {
      const embedding = (
        await db.pool.query(
          'INSERT INTO moria.native_search_embeddings(model_fingerprint,text_digest,text,dimensions,embedding,markings) VALUES($1,$2,$3,3,$4::public.vector,$5) RETURNING id',
          [
            profile.modelBindingFingerprint,
            createHash('sha256').update(row.text).digest(),
            row.text,
            JSON.stringify(embedded.vectors[index]),
            ['private'],
          ],
        )
      ).rows[0].id;
      const document = (
        await db.pool.query(
          'INSERT INTO moria.native_search_documents(generation,uid,source_revision,source_digest,chunk_count) VALUES($1,$2,$3,$4,1) RETURNING id',
          [generation, row.uid, revision, createHash('sha256').update(row.text).digest()],
        )
      ).rows[0].id;
      await db.pool.query('INSERT INTO moria.native_search_heads VALUES($1,$2,$3)', [
        generation,
        row.uid,
        document,
      ]);
      await db.pool.query('INSERT INTO moria.native_search_chunks VALUES($1,0,$2,0,$3,$4)', [
        document,
        `chunk-${row.uid}`,
        Array.from(row.text).length,
        embedding,
      ]);
    }
    await db.pool.query(
      'UPDATE moria.native_search_collections SET active_generation=$3 WHERE environment=$1 AND resource=$2',
      [environment, 'collection', generation],
    );
    const server = spawn(
      process.env.MORIA_NATIVE_BINARY ?? '/usr/local/bin/moria-runtime',
      ['serve'],
      {
        env: nativeEnv,
        stdio: ['ignore', 'pipe', 'pipe'],
      },
    );
    processes.push(server);
    exits.push(new Promise((resolve) => server.once('exit', resolve)));
    const serverAddress = await new Promise<string>((resolve, reject) => {
      let pending = '';
      server.once('error', reject);
      server.once('exit', (code) =>
        reject(new Error(`Native serving exited before listening: ${code}`)),
      );
      server.stdout.on('data', (bytes) => {
        pending += bytes.toString();
        assert.ok(pending.length <= 65536);
        let split;
        while ((split = pending.indexOf('\n')) >= 0) {
          const line = pending.slice(0, split);
          pending = pending.slice(split + 1);
          try {
            const event = JSON.parse(line);
            if (event.event === 'serving') resolve(event.address);
          } catch {}
        }
      });
    });
    const http = async (
      operation: string,
      input: unknown,
      headers: Record<string, string> = {},
    ) => {
      const response = await fetch(
        `http://${serverAddress}/api/v1/search/collection/${operation}`,
        {
          method: 'POST',
          headers: {
            authorization: `Bearer ${issued.token}`,
            'content-type': 'application/json',
            ...headers,
          },
          body: JSON.stringify(input),
        },
      );
      return {
        status: response.status,
        cache: response.headers.get('cache-control'),
        value: (await response.json()) as any,
      };
    };
    const query = (input: unknown = { text: 'query', topK: 2 }) =>
      run('search', 'query', { resource: 'collection', key: randomUUID(), input });
    const expected = rows
      .slice(0, 2)
      .map((row) => row.uid)
      .sort();
    const overlay = async (uid: string, overrides: unknown, tombstone = false) => {
      const client = await db.pool.connect();
      try {
        await client.query('BEGIN');
        const revision = (
          await client.query(
            'UPDATE moria.environments SET revision=revision+1 WHERE id=$1 RETURNING revision',
            [environment],
          )
        ).rows[0].revision;
        await client.query(
          'INSERT INTO moria.overlay_versions(environment,object_type,uid,revision,overrides,tombstone) VALUES($1,$2,$3,$4,$5,$6)',
          [environment, 'objects', uid, revision, overrides, tombstone],
        );
        await client.query('COMMIT');
      } finally {
        client.release();
      }
    };
    await t.test(
      'public native retrieval shares protected results and rejects invalid authority before billing',
      async () => {
        const result = await http(
          'query',
          { text: 'query', topK: 2 },
          { 'idempotency-key': randomUUID() },
        );
        assert.equal(result.status, 200, JSON.stringify(result.value));
        assert.equal(result.cache, 'no-store');
        assert.deepEqual(
          result.value.hits.map((hit: any) => hit.uid),
          expected,
        );
        const visibility = await http('visibility', {});
        assert.equal(visibility.status, 200);
        assert.deepEqual(visibility.value.visibility, result.value.visibility);
        const calls = providerCalls;
        assert.equal((await http('query', { text: 'query', topK: 2 })).status, 400);
        assert.equal(
          (await http('query', { text: 'query', topK: 51 }, { 'idempotency-key': randomUUID() }))
            .status,
          400,
        );
        assert.equal(
          (
            await http(
              'query',
              { text: 'query', topK: 2 },
              { authorization: 'Bearer invalid', 'idempotency-key': randomUUID() },
            )
          ).status,
          401,
        );
        assert.equal(
          (
            await http(
              'query',
              { text: 'query', topK: 2 },
              { origin: 'https://untrusted.invalid', 'idempotency-key': randomUUID() },
            )
          ).status,
          403,
        );
        assert.equal((await http('visibility', { actor: principal })).status, 400);
        assert.equal(providerCalls, calls);
      },
    );
    await t.test(
      'actual pgvector cosine ranks complete authorized rows with a stable tie',
      async () => {
        const result = await query();
        assert.deepEqual(
          result.hits.map((hit: any) => hit.uid),
          expected,
        );
        assert.ok(result.hits.every((hit: any) => Math.abs(hit.score - 0.8) < 1e-6));
        assert.equal(result.visibility.revision, revision);
        assert.equal(result.modelRevision, 'fixture-v1');
        assert.deepEqual(
          (await run('search', 'visibility', { resource: 'collection' })).visibility,
          result.visibility,
        );
        assert.deepEqual(
          (await query({ text: 'query', topK: 2, minimumToken: result.visibility })).hits,
          result.hits,
        );
      },
    );
    await t.test(
      'selected-property denial and invalid topK fail before model dispatch',
      async () => {
        const before = providerCalls;
        collection.textProperty = 'secret';
        await db.pool.query(
          'UPDATE moria.native_search_collections SET definition=$2 WHERE environment=$1',
          [environment, collection],
        );
        await db.pool.query(
          'UPDATE moria.native_search_generations SET definition=$2 WHERE id=$1',
          [generation, collection],
        );
        await assert.rejects(query(), /PROPERTY_FORBIDDEN/);
        assert.equal(providerCalls, before);
        collection.textProperty = 'text';
        await db.pool.query(
          'UPDATE moria.native_search_collections SET definition=$2 WHERE environment=$1',
          [environment, collection],
        );
        await db.pool.query(
          'UPDATE moria.native_search_generations SET definition=$2 WHERE id=$1',
          [generation, collection],
        );
        for (const topK of [0, 51])
          await assert.rejects(query({ text: 'query', topK }), /SEARCH_INPUT_INVALID/);
        assert.equal(providerCalls, before);
      },
    );
    await t.test('current tombstones and effective restrictions filter before topK', async () => {
      await overlay(rows[0].uid, {}, true);
      let result = await query();
      assert.equal(result.hits.length, 2);
      assert.ok(!result.hits.some((hit: any) => hit.uid === rows[0].uid));
      assert.equal(result.hits[0].uid, rows[1].uid);
      await overlay(rows[0].uid, {});
      await overlay(rows[1].uid, { visible: false });
      result = await query();
      assert.ok(!result.hits.some((hit: any) => hit.uid === rows[1].uid));
      assert.equal(result.hits[0].uid, rows[0].uid);
      await overlay(rows[1].uid, {});
    });
    await t.test(
      'retained embedding labels precede ranking and cache bytes cannot be rewritten',
      async () => {
        const row = (
          await db.pool.query(
            'SELECT ch.document,ch.embedding FROM moria.native_search_heads h JOIN moria.native_search_chunks ch ON ch.document=h.document WHERE h.generation=$1 AND h.uid=$2',
            [generation, rows[0].uid],
          )
        ).rows[0];
        const retained = (
          await db.pool.query(
            "INSERT INTO moria.native_search_embeddings(model_fingerprint,text_digest,text,dimensions,embedding,markings) SELECT model_fingerprint,text_digest,text,dimensions,'[1,0,0]'::public.vector,ARRAY['private','secret'] FROM moria.native_search_embeddings WHERE id=$1 RETURNING id",
            [row.embedding],
          )
        ).rows[0].id;
        await db.pool.query(
          'UPDATE moria.native_search_chunks SET embedding=$2 WHERE document=$1',
          [row.document, retained],
        );
        const result = await query();
        assert.equal(result.hits.length, 2);
        assert.ok(!result.hits.some((hit: any) => hit.uid === rows[0].uid));
        assert.equal(result.hits[0].uid, rows[1].uid);
        await db.pool.query(
          'UPDATE moria.native_search_chunks SET embedding=$2 WHERE document=$1',
          [row.document, row.embedding],
        );
        await assert.rejects(
          db.pool.query(
            "UPDATE moria.native_search_embeddings SET embedding='[1,0,0]'::public.vector WHERE id=$1",
            [row.embedding],
          ),
          /SEARCH_EMBEDDING_RESERVED/,
        );
        await assert.rejects(
          db.pool.query(
            'UPDATE moria.native_search_embeddings SET markings=ARRAY[]::text[] WHERE id=$1',
            [row.embedding],
          ),
          /SEARCH_EMBEDDING_RESERVED/,
        );
        await assert.rejects(
          db.pool.query('DELETE FROM moria.native_search_embeddings WHERE id=$1', [retained]),
          /SEARCH_EMBEDDING_RESERVED/,
        );
        assert.deepEqual(
          (await query()).hits.map((hit: any) => hit.uid),
          expected,
        );
      },
    );
    await t.test('current canonical policy still applies across source replacement', async () => {
      const publish = async (data: typeof rows) =>
        store.publishStage(
          await store.stage(
            environment,
            'objects',
            schema,
            data,
            (await authority.current(environment, principal, application)).epoch,
            (
              await db.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [
                environment,
              ])
            ).rows[0].revision,
            undefined,
            ['private'],
          ),
        );
      await publish(
        rows.map((row, index) => ({ ...row, region: index === 0 ? 'East' : row.region })),
      );
      const result = await query();
      assert.equal(result.hits.length, 2);
      assert.ok(!result.hits.some((hit: any) => hit.uid === rows[0].uid));
      assert.equal(result.hits[0].uid, rows[1].uid);
      await publish(rows);
      assert.deepEqual(
        (await query()).hits.map((hit: any) => hit.uid),
        expected,
      );
    });
    await t.test(
      'a different model binding cannot mix query vectors with the active generation',
      async () => {
        const before = providerCalls;
        model.modelRevision = 'fixture-v2';
        await db.pool.query(
          'UPDATE moria.native_model_bindings SET definition=$2 WHERE environment=$1',
          [environment, model],
        );
        await assert.rejects(query(), /SEARCH_MODEL_CHANGED/);
        assert.equal(providerCalls, before);
        model.modelRevision = 'fixture-v1';
        await db.pool.query(
          'UPDATE moria.native_model_bindings SET definition=$2 WHERE environment=$1',
          [environment, model],
        );
      },
    );
    await t.test('visibility tokens bind incarnation and generation', async () => {
      const token = (await run('search', 'visibility', { resource: 'collection' })).visibility;
      const before = providerCalls;
      await assert.rejects(
        query({ text: 'query', topK: 2, minimumToken: { ...token, incarnation: randomUUID() } }),
        /SEARCH_TOKEN_INVALID/,
      );
      await assert.rejects(
        query({ text: 'query', topK: 2, minimumToken: { ...token, generation: randomUUID() } }),
        /SEARCH_GENERATION_CHANGED/,
      );
      assert.equal(providerCalls, before);
    });
    await t.test('revocation during actual inference withholds ranked documents', async () => {
      hold = true;
      barrier = new Promise((resolve) => (signal = resolve));
      held = new Promise((resolve) => (release = resolve));
      const pending = query();
      await barrier;
      await authority.setGrants(environment, 'fixture', principal, application, []);
      release();
      await assert.rejects(pending);
      hold = false;
      await authority.setGrants(environment, 'fixture', principal, application, operations);
    });
  },
);
