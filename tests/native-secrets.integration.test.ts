import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { join } from 'node:path';
import test from 'node:test';
import { Pool } from 'pg';
import { database } from './fixtures/catalog-storage/database.js';
import { createLocalFilesystem } from '../services/filesystem/src/local.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { filesystemOperations } from '../packages/contracts/filesystem.js';
import type { Resource } from '../packages/contracts/types.js';

test(
  'Source secrets require exact operator custody and current actor authority',
  { timeout: 180000 },
  async (t) => {
    const db = await database(),
      children = new Set<ChildProcess>(),
      exits: Promise<unknown>[] = [];
    let calls = 0,
      hold = false,
      entered = () => {},
      release = () => {};
    let barrier = Promise.resolve(),
      held = Promise.resolve();
    const material = randomUUID(),
      label = 'secrets/Marking/Private';
    const provider = createServer(async (req, res) => {
      calls++;
      try {
        assert.equal(req.headers.authorization, `Bearer ${material}`);
        for await (const _ of req) {
          /* Consume the bounded native request. */
        }
        if (hold) {
          entered();
          await held;
        }
        res.setHeader('content-type', 'application/json');
        res.end(
          JSON.stringify({
            choices: [
              {
                message: { role: 'assistant', content: 'approved response' },
                finish_reason: 'stop',
              },
            ],
            usage: { prompt_tokens: 1, completion_tokens: 1 },
          }),
        );
      } catch (error) {
        res.destroy(error instanceof Error ? error : undefined);
      }
    });
    await new Promise<void>((resolve) => provider.listen(0, '127.0.0.1', resolve));
    t.after(async () => {
      release();
      for (const child of children) child.kill('SIGKILL');
      await Promise.allSettled(exits);
      await new Promise<void>((resolve) => provider.close(() => resolve()));
      await db.close();
    });
    const endpoint = `http://127.0.0.1:${(provider.address() as { port: number }).port}/v1/`;
    const environment = randomUUID(),
      principal = 'secret-user',
      application = 'secret-client';
    await db.catalog.ensureEnvironment(environment);
    const fs = await createLocalFilesystem(db.catalog, join(db.root, 'files'));
    const authority = fs.filesystem.security.authority,
      sessions = new WorkloadSessions(authority);
    const fsOps = filesystemOperations.map((op) => `filesystem:${op}`);
    await authority.syncPrincipal(environment, 'fixture', principal, {
      kind: 'workload',
      enabled: true,
      attributes: {},
      markings: [label],
    });
    const actor = async (app: string, operations: string[]) => {
      await authority.setApplication(environment, 'fixture', app, operations);
      await authority.setGrants(environment, 'fixture', principal, app, operations);
      const issued = await sessions.mint({ environment, principal, application: app }, 'fixture');
      return {
        token: issued.token,
        context: { session: await sessions.authenticate('Bearer ' + issued.token) },
      };
    };
    const author = await actor('secret-author', ['instance:manage', ...fsOps]);
    const home = await fs.management.ensureHome(author.context),
      homeRole = await fs.management.role(author.context, home.home.id);
    const projects = await fs.filesystem.store.get(homeRole!.configuration.projects as string);
    await fs.management.createProject(author.context, projects.id, 'secrets', projects.revision);
    const source = await fs.filesystem.store.resolve(
      `/users/${home.home.name}/projects/secrets/source`,
    );
    const made = await fs.filesystem.apply(author.context, randomUUID(), [
      {
        op: 'create',
        kind: 'directory',
        parentId: source.id,
        expectedParentRevision: source.revision,
        name: 'models',
      },
    ]);
    const root = made.entries.find((entry) => entry.name === 'models')!;
    const write = async (name: string, value: unknown) => {
      const bytes = Buffer.from(JSON.stringify(value));
      const content = {
        size: bytes.length,
        digest: `sha256:${createHash('sha256').update(bytes).digest('hex')}`,
        mediaType: 'application/json',
      };
      const old = await fs.filesystem.store
        .resolve(`/users/${home.home.name}/projects/secrets/source/models/${name}`)
        .catch(() => undefined);
      await fs.filesystem.writeFile(
        author.context,
        randomUUID(),
        old
          ? { op: 'write', id: old.id, expectedRevision: old.revision, content }
          : {
              op: 'create',
              parentId: root.id,
              expectedParentRevision: (await fs.filesystem.store.get(root.id)).revision,
              name,
              content,
            },
        new ReadableStream({
          start(controller) {
            controller.enqueue(bytes);
            controller.close();
          },
        }),
      );
    };
    await write('.ontology.json', { version: 1, name: 'Secret Models', namespace: 'secrets' });
    await fs.source.register(
      author.context,
      root.id,
      (await fs.filesystem.store.get(root.id)).revision,
      'ontology',
    );
    const definitions: Resource[] = [
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'Marking',
        metadata: { namespace: 'secrets', name: 'Private' },
        spec: { eligibilityGroup: 'fixture', declassifierGroup: 'fixture' },
      },
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'SecretReference',
        metadata: { namespace: 'secrets', name: 'Credential' },
        spec: {
          binding: 'logical-provider-credential',
          consumers: ['secrets/ModelProvider/Provider'],
        },
      },
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'Policy',
        metadata: { namespace: 'secrets', name: 'Local' },
        spec: {
          type: 'modelDestination',
          allowedDestinations: ['local-secret-provider'],
          allowedMarkings: [label],
        },
      },
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'ModelProvider',
        metadata: { namespace: 'secrets', name: 'Provider' },
        spec: {
          adapter: 'openai-compatible-v1',
          endpoint,
          destination: 'local-secret-provider',
          destinationPolicy: 'secrets/Policy/Local',
          secret: 'secrets/SecretReference/Credential',
        },
      },
      {
        apiVersion: 'moria/v1alpha2',
        kind: 'Model',
        metadata: { namespace: 'secrets', name: 'Chat' },
        spec: {
          provider: 'secrets/ModelProvider/Provider',
          upstreamName: 'secret-fixture',
          versionLabel: 'v1',
          mutable: false,
          capabilities: ['chat'],
        },
      },
    ];
    let resources = new Map<string, string>(),
      releaseDigest = '';
    let dependentModel: string | undefined;
    const publish = async () => {
      for (const definition of definitions)
        await write(`${definition.metadata.name}.resource.json`, definition);
      const publication = await fs.source.publish(
        author.context,
        root.id,
        (await fs.source.checkout(author.context, root.id)).digest,
        [],
      );
      await fs.source.apply(
        author.context,
        (await fs.source.plan(author.context, root.id, publication.digest)).id,
      );
      resources = new Map(
        publication.resources.map((resource) => [resource.reference, resource.id]),
      );
      releaseDigest = publication.digest;
    };
    await publish();
    const model = resources.get('secrets/Model/Chat')!,
      secret = resources.get('secrets/SecretReference/Credential')!;
    const ops = [`${model}:invoke`, `${secret}:use`, ...fsOps],
      user = await actor(application, ops);
    const route = {
      endpointRef: 'SECRET_MODEL_ENDPOINT',
      secretRef: 'SECRET_MODEL_MATERIAL',
      model: 'secret-fixture',
      modelRevision: 'v1',
      resourceClass: 'models',
      destination: 'local-secret-provider',
      markings: [],
      destinationPolicy: {
        allowedDestinations: ['local-secret-provider'],
        allowedMarkings: [label],
      },
      capabilities: ['chat'],
    };
    await db.pool.query(
      'INSERT INTO moria.native_model_bindings(environment,resource,revision,definition) VALUES($1,$2,$3,$4)',
      [environment, model, 'v1', route],
    );
    const role = 'secret_' + randomUUID().replaceAll('-', ''),
      password = randomUUID();
    await db.pool.query(
      `CREATE ROLE ${role} LOGIN PASSWORD '${password}'; GRANT USAGE ON SCHEMA moria TO ${role}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role}`,
    );
    const nativeEnv = {
      PATH: process.env.PATH,
      MORIA_INDEXING_DATABASE_SECRET_REF: 'SECRET_DATABASE',
      MORIA_INDEXING_DATABASE_TLS: 'disable',
      SECRET_DATABASE: `postgresql://${role}:${password}@127.0.0.1:${db.pool.options.port}/postgres`,
      SECRET_MODEL_ENDPOINT: endpoint,
      SECRET_MODEL_MATERIAL: material,
    };
    const operatorEnv = {
      ...nativeEnv,
      SECRET_DATABASE: `postgresql://${db.pool.options.user}:${encodeURIComponent(db.pool.options.password as string)}@127.0.0.1:${db.pool.options.port}/postgres`,
    };
    const launch = (domain: string, command: string, input: unknown, operator = false) => {
      const child = spawn(
        process.env.MORIA_RUNTIME_BINARY ??
          process.env.MORIA_NATIVE_BINARY ??
          '/usr/local/bin/moria-runtime',
        [domain, command],
        { env: operator ? operatorEnv : nativeEnv, stdio: ['pipe', 'pipe', 'pipe'] },
      );
      children.add(child);
      const done = new Promise<any>((resolve, reject) => {
        let out = '',
          error = '';
        const timer = setTimeout(() => child.kill('SIGKILL'), 30000);
        child.stdout.on('data', (part) => (out += part));
        child.stderr.on('data', (part) => (error += part));
        child.stdin.on('error', () => {});
        child.once('error', reject);
        child.once('exit', (code) => {
          clearTimeout(timer);
          children.delete(child);
          assert.ok(!out.includes(material) && !error.includes(material));
          if (code !== 0) reject(new Error(error));
          else {
            try {
              resolve(JSON.parse(out));
            } catch (failure) {
              reject(failure);
            }
          }
        });
      });
      exits.push(done);
      void done.catch(() => {});
      child.stdin.end(JSON.stringify(input));
      return done;
    };
    const invoke = (key = randomUUID()) =>
      launch('models', 'chat', {
        token: user.token,
        resource: model,
        key,
        input: { messages: [{ role: 'user', content: 'hello' }], maxTokens: 20 },
      });
    const inspected = async () =>
      (await launch('secret', 'inspect-model', { environment, model }, true)).binding;
    const approve = async (revision = 'material-v1', binding?: unknown) =>
      launch(
        'secret',
        'approve',
        {
          requestKey: randomUUID(),
          environment,
          binding: binding ?? (await inspected()),
          materialRevision: revision,
          markings: [label],
        },
        true,
      );
    let approval: any;
    await t.test(
      'a Source declaration alone grants no material, and ordinary callers cannot approve custody',
      async () => {
        await assert.rejects(invoke(), /SECRET_APPROVAL_REQUIRED/);
        assert.equal(calls, 0);
        await assert.rejects(
          launch('secret', 'approve', {
            requestKey: randomUUID(),
            environment,
            binding: await inspected(),
            materialRevision: 'material-v1',
            markings: [label],
          }),
          /RECOVERY_OPERATOR_REQUIRED/,
        );
        approval = await approve();
        const ordinary = new Pool({ connectionString: nativeEnv.SECRET_DATABASE });
        try {
          await assert.rejects(
            ordinary.query('UPDATE moria.native_secret_approvals SET enabled=false WHERE id=$1', [
              approval.id,
            ]),
            /RECOVERY_OPERATOR_REQUIRED/,
          );
          await assert.rejects(
            ordinary.query(
              "INSERT INTO moria.native_secret_approvals SELECT (jsonb_populate_record(NULL::moria.native_secret_approvals,to_jsonb(a)||jsonb_build_object('id',$2::text,'request_key',$3::text))).* FROM moria.native_secret_approvals a WHERE id=$1",
              [approval.id, randomUUID(), randomUUID()],
            ),
            /RECOVERY_OPERATOR_REQUIRED/,
          );
        } finally {
          await ordinary.end();
        }
        await assert.rejects(
          approve('material-v1', { ...(await inspected()), consumer: randomUUID() }),
          /SECRET_CONSUMER_DENIED/,
        );
        await assert.rejects(
          approve('material-v1', { ...(await inspected()), materialRef: 'PATH' }),
          /SECRET_APPROVAL_REVOKED/,
        );
        assert.equal(calls, 0);
      },
    );
    await t.test(
      'actual caller needs SecretReference.use independently of Model.invoke',
      async () => {
        await authority.setGrants(
          environment,
          'fixture',
          principal,
          application,
          ops.filter((op) => op !== `${secret}:use`),
        );
        await assert.rejects(invoke(), /FORBIDDEN/);
        assert.equal(calls, 0);
        await authority.setGrants(environment, 'fixture', principal, application, ops);
        const probed = await launch('models', 'probe', { token: user.token, resource: model });
        assert.equal(probed.capabilities.includes('chat'), true);
        const result = await invoke();
        assert.equal(result.content, 'approved response');
        assert.deepEqual(result.markings, [label]);
      },
    );
    await t.test('Secret use does not grant independent filesystem browsing', async () => {
      const minimum = [`${model}:invoke`, `${secret}:use`];
      await authority.setApplication(environment, 'fixture', application, minimum);
      await authority.setGrants(environment, 'fixture', principal, application, minimum);
      const ordinary = new Pool({ connectionString: nativeEnv.SECRET_DATABASE });
      try {
        assert.equal((await invoke()).content, 'approved response');
        const entry = (
          await fs.filesystem.store.resolve(
            `/users/${home.home.name}/projects/secrets/source/models/Credential.resource.json`,
          )
        ).id;
        await assert.rejects(
          ordinary.query("SELECT moria.native_filesystem_access($1,$2,'read')", [
            createHash('sha256').update(user.token).digest('hex'),
            entry,
          ]),
          /FORBIDDEN/,
        );
      } finally {
        await ordinary.end();
        await authority.setApplication(environment, 'fixture', application, ops);
        await authority.setGrants(environment, 'fixture', principal, application, ops);
      }
    });
    await t.test(
      'material semantic revision invalidates old capability evidence without returning material',
      async () => {
        const before = await launch('models', 'capabilities', {
          token: user.token,
          resource: model,
        });
        approval = await approve('material-v2');
        const after = await launch('models', 'capabilities', {
          token: user.token,
          resource: model,
        });
        assert.notEqual(after.modelBindingFingerprint, before.modelBindingFingerprint);
        assert.deepEqual(after.capabilities, []);
        await assert.rejects(invoke(), /UNSUPPORTED_CAPABILITY/);
        await launch('models', 'probe', { token: user.token, resource: model });
        assert.equal((await invoke()).content, 'approved response');
        approval = await approve('material-v2');
        const unchanged = await launch('models', 'capabilities', {
          token: user.token,
          resource: model,
        });
        assert.equal(unchanged.modelBindingFingerprint, after.modelBindingFingerprint);
        assert.equal(unchanged.capabilities.includes('chat'), true);
        const stored = await db.pool.query(
          'SELECT row_to_json(a) AS approval FROM moria.native_secret_approvals a',
        );
        assert.ok(!JSON.stringify(stored.rows).includes(material));
      },
    );
    await t.test('disable and reenable cannot revive an accepted in-flight grant', async () => {
      hold = true;
      barrier = new Promise<void>((resolve) => (entered = resolve));
      held = new Promise<void>((resolve) => (release = resolve));
      const pending = invoke();
      await Promise.race([
        barrier,
        pending.then(() => assert.fail('held provider completed before barrier')),
      ]);
      await launch('secret', 'disable', { id: approval.id }, true);
      await db.pool.query('UPDATE moria.native_secret_approvals SET enabled=true WHERE id=$1', [
        approval.id,
      ]);
      release();
      await assert.rejects(pending, /AUTHORIZATION_CHANGED|SECRET_APPROVAL_REVOKED/);
      hold = false;
      const state = await db.pool.query(
        'SELECT generation::text FROM moria.native_secret_approvals WHERE id=$1',
        [approval.id],
      );
      assert.equal(state.rows[0].generation, '3');
      approval = await approve('material-v2');
      assert.equal((await invoke()).content, 'approved response');
    });
    await t.test('changed logical declaration requires new explicit custody approval', async () => {
      const declaration = definitions.find((value) => value.kind === 'SecretReference')!;
      (declaration.spec as { binding: string }).binding = 'different-logical-binding';
      await publish();
      const before = calls;
      await assert.rejects(invoke(), /SECRET_APPROVAL_REQUIRED/);
      assert.equal(calls, before);
      approval = await approve('material-v2');
      assert.equal((await invoke()).content, 'approved response');
    });
    await t.test(
      'stale operator Model routes cannot survive a changed actual declaration',
      async () => {
        const declaration = definitions.find((value) => value.kind === 'Model')!;
        (declaration.spec as { upstreamName: string }).upstreamName = 'different-upstream';
        await publish();
        const before = calls;
        await assert.rejects(invoke(), /MODEL_BINDING_INVALID/);
        assert.equal(calls, before);
        (declaration.spec as { upstreamName: string }).upstreamName = 'secret-fixture';
        await publish();
        assert.equal((await invoke()).content, 'approved response');
      },
    );
    await t.test(
      'approved private material cannot flow to a public destination policy',
      async () => {
        const declaration = definitions.find((value) => value.kind === 'Policy')!;
        (declaration.spec as { allowedMarkings: string[] }).allowedMarkings = [];
        route.destinationPolicy.allowedMarkings = [];
        await publish();
        await db.pool.query(
          'UPDATE moria.native_model_bindings SET definition=$3 WHERE environment=$1 AND resource=$2',
          [environment, model, route],
        );
        approval = await approve('material-v2');
        const before = calls;
        await assert.rejects(invoke(), /DESTINATION_FORBIDDEN/);
        assert.equal(calls, before);
        (declaration.spec as { allowedMarkings: string[] }).allowedMarkings = [label];
        route.destinationPolicy.allowedMarkings = [label];
        await publish();
        await db.pool.query(
          'UPDATE moria.native_model_bindings SET definition=$3 WHERE environment=$1 AND resource=$2',
          [environment, model, route],
        );
        approval = await approve('material-v2');
        assert.equal((await invoke()).content, 'approved response');
      },
    );
    await t.test('a dependent Model uses the actual separately owned Secret release', async () => {
      const made = await fs.filesystem.apply(author.context, randomUUID(), [
        {
          op: 'create',
          kind: 'directory',
          parentId: source.id,
          expectedParentRevision: (await fs.filesystem.store.get(source.id)).revision,
          name: 'dependent',
        },
      ]);
      const dependent = made.entries.find((entry) => entry.name === 'dependent')!;
      const files = [
        [
          '.ontology.json',
          {
            version: 1,
            name: 'Dependent Model',
            namespace: 'clientmodels',
            dependencies: [{ rootId: root.id, digest: releaseDigest }],
          },
        ],
        [
          'Chat.resource.json',
          {
            apiVersion: 'moria/v1alpha2',
            kind: 'Model',
            metadata: { namespace: 'clientmodels', name: 'Chat' },
            spec: {
              provider: 'secrets/ModelProvider/Provider',
              upstreamName: 'secret-fixture',
              versionLabel: 'v1',
              mutable: false,
              capabilities: ['chat'],
            },
          },
        ],
      ] as const;
      for (const [name, value] of files) {
        const bytes = Buffer.from(JSON.stringify(value));
        await fs.filesystem.writeFile(
          author.context,
          randomUUID(),
          {
            op: 'create',
            parentId: dependent.id,
            expectedParentRevision: (await fs.filesystem.store.get(dependent.id)).revision,
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
      }
      await fs.source.register(
        author.context,
        dependent.id,
        (await fs.filesystem.store.get(dependent.id)).revision,
        'ontology',
      );
      const publication = await fs.source.publish(
        author.context,
        dependent.id,
        (await fs.source.checkout(author.context, dependent.id)).digest,
      );
      await fs.source.apply(
        author.context,
        (await fs.source.plan(author.context, dependent.id, publication.digest)).id,
      );
      dependentModel = publication.resources.find(
        (resource) => resource.reference === 'clientmodels/Model/Chat',
      )!.id;
      const currentOps = [...ops, `${dependentModel}:invoke`];
      await authority.setApplication(environment, 'fixture', application, currentOps);
      await authority.setGrants(environment, 'fixture', principal, application, currentOps);
      await db.pool.query(
        'INSERT INTO moria.native_model_bindings(environment,resource,revision,definition) VALUES($1,$2,$3,$4)',
        [environment, dependentModel, 'v1', route],
      );
      const dependentBinding = (
        await launch('secret', 'inspect-model', { environment, model: dependentModel }, true)
      ).binding;
      assert.equal(dependentBinding.release, publication.digest);
      assert.notEqual(dependentBinding.release, releaseDigest);
      await approve('material-v2', dependentBinding);
      await launch('models', 'probe', { token: user.token, resource: dependentModel });
      const result = await launch('models', 'chat', {
        token: user.token,
        resource: dependentModel,
        key: randomUUID(),
        input: { messages: [{ role: 'user', content: 'hello' }], maxTokens: 20 },
      });
      assert.equal(result.content, 'approved response');
      assert.deepEqual(result.markings, [label]);
    });
    await t.test(
      'actual Secret declaration file classification gates use before dispatch',
      async () => {
        const entry = (
          await fs.filesystem.store.resolve(
            `/users/${home.home.name}/projects/secrets/source/models/Credential.resource.json`,
          )
        ).id;
        await fs.filesystem.security.setPolicy('fixture', entry, '0', {
          markings: [{ environment, marking: 'secret-denied' }],
          grants: [],
        });
        const before = calls;
        await assert.rejects(invoke(), /FORBIDDEN/);
        await assert.rejects(
          launch('models', 'chat', {
            token: user.token,
            resource: dependentModel,
            key: randomUUID(),
            input: { messages: [{ role: 'user', content: 'hello' }], maxTokens: 20 },
          }),
          /FORBIDDEN/,
        );
        assert.equal(calls, before);
      },
    );
  },
);
