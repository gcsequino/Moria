import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import test from 'node:test';
import { database } from './fixtures/catalog-storage/database.js';
import { Authority } from '../services/security/src/authority.js';
import { FilesystemAuthority } from '../services/security/src/filesystem.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { FilesystemStore } from '../services/filesystem/src/store.js';
import { FilesystemService } from '../services/filesystem/src/service.js';
import { SourceService } from '../services/filesystem/src/source.js';
import { createAuthorizedLocalStorage } from '../services/storage/src/authority.js';
import { filesystemRoot, filesystemOperations } from '../packages/contracts/filesystem.js';
import type { Resource } from '../packages/contracts/types.js';

test(
  'managed source roots checkout, publish, plan and activate coherent definitions',
  { timeout: 120000 },
  async (t) => {
    const db = await database();
    t.after(db.close);
    const authority = new Authority(db.catalog),
      security = new FilesystemAuthority(authority),
      store = new FilesystemStore(db.catalog);
    const environment = 'source-test',
      application = 'source-app',
      principal = 'author';
    const operations = filesystemOperations.map((op) => `filesystem:${op}`);
    await db.catalog.ensureEnvironment(environment);
    await authority.setApplication(environment, 'operator', application, operations);
    await authority.syncPrincipal(environment, 'operator', principal, {
      kind: 'workload',
      enabled: true,
      attributes: {},
      markings: ['protected'],
    });
    await authority.setGrants(environment, 'operator', principal, application, operations);
    const instanceId = await security.createPrincipal('operator', 'workload');
    await security.bindPrincipal('operator', environment, principal, instanceId);
    await security.setPolicy('operator', filesystemRoot, '0', {
      markings: [],
      grants: [
        {
          subject: { kind: 'principal', id: instanceId },
          operations: [...filesystemOperations],
          inherit: true,
          effect: 'allow',
        },
      ],
    });
    const token = await new WorkloadSessions(authority).mint(
      { environment, principal, application },
      'operator',
    );
    const context = {
      session: await new WorkloadSessions(authority).authenticate(`Bearer ${token.token}`),
    };
    const binding = {
      root: join(db.root, 'content'),
      warehouseId: 'filesystem',
      tableId: 'source',
      maxObjectBytes: 1024 * 1024,
      chunkBytes: 1048576,
    };
    await mkdir(binding.root, { recursive: true });
    const filesystem = new FilesystemService(store, security, {
      ...binding,
      maxFileBytes: 1024 * 1024,
      storage: (scope, current) => createAuthorizedLocalStorage(binding, scope, current),
    });
    const source = new SourceService(filesystem);
    const projectId = randomUUID();
    const root = await store.apply({ environment, principal, application }, 'project', [
      {
        op: 'create',
        parentId: filesystemRoot,
        expectedParentRevision: '1',
        name: 'project',
        kind: 'directory',
        scope: { projectId },
      },
    ]);
    const project = root.entries.find((entry) => entry.name === 'project')!;
    async function directory(parentId: string, name: string) {
      const receipt = await filesystem.apply(context, randomUUID(), [
        {
          op: 'create',
          parentId,
          expectedParentRevision: (await store.get(parentId)).revision,
          name,
          kind: 'directory',
        },
      ]);
      return receipt.entries.find((entry) => entry.parentId === parentId && entry.name === name)!;
    }
    async function file(parentId: string, name: string, value: unknown) {
      const bytes = new TextEncoder().encode(
        typeof value === 'string' ? value : JSON.stringify(value),
      );
      const receipt = await filesystem.writeFile(
        context,
        randomUUID(),
        {
          op: 'create',
          parentId,
          expectedParentRevision: (await store.get(parentId)).revision,
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
      return receipt.entries.find((entry) => entry.name === name)!;
    }
    const properties = { id: { type: 'uid', nullable: false } };
    const dataset: Resource = {
      apiVersion: 'moria/v1alpha2',
      kind: 'Dataset',
      metadata: { namespace: 'stations', name: 'source' },
      spec: { columns: properties, writeModes: ['replace'] },
    };
    const object: Resource = {
      apiVersion: 'moria/v1alpha2',
      kind: 'ObjectType',
      metadata: { namespace: 'stations', name: 'Station' },
      spec: {
        primaryKey: 'id',
        properties,
        source: { resource: 'stations/Dataset/source', mapping: { id: 'id' } },
      },
    };
    const ontology = await directory(project.id, 'observations');
    await file(ontology.id, '.ontology.json', {
      version: 1,
      name: 'Observations',
      namespace: 'stations',
    });
    const datasetFile = await file(ontology.id, 'source.resource.json', dataset);
    const objectFile = await file(ontology.id, 'station.object.json', object);
    const role = await source.register(
      context,
      ontology.id,
      (await store.get(ontology.id)).revision,
      'ontology',
    );
    let firstDigest = '',
      objectId = '';

    await t.test(
      'registered roots preserve metadata dotfiles and allocate stable exported resource identities',
      async () => {
        const checkout = await source.checkout(context, ontology.id);
        assert.ok(checkout.files.some((item) => item.path === '.ontology.json'));
        const release = await source.publish(context, ontology.id, checkout.digest);
        firstDigest = release.digest;
        objectId = release.resources.find(
          (resource) => resource.reference === 'stations/ObjectType/Station',
        )!.id;
        assert.deepEqual(await source.publish(context, ontology.id, checkout.digest), release);
        assert.equal(
          (await source.getRelease(context, release.digest)).snapshot.role.deploymentUnitId,
          role.deploymentUnitId,
        );
        const plan = await source.plan(context, ontology.id, release.digest);
        const applied = await source.apply(context, plan.id);
        assert.equal(applied.revision, '1');
        assert.deepEqual(await source.apply(context, plan.id), applied);
        const resolved = await source.resource(context, 'stations/ObjectType/Station', projectId);
        assert.equal(resolved.resourceId, objectId);
        assert.deepEqual(await source.resource(context, objectId), resolved);
        assert.equal((await filesystem.get(context, resolved.entryId)).kind, 'resource');
        const datasetResource = await source.resource(
          context,
          'stations/Dataset/source',
          projectId,
        );
        await security.setPolicy('operator', datasetResource.entryId, '0', {
          markings: [{ environment, marking: 'protected' }],
          grants: [],
        });
        await authority.syncPrincipal(environment, 'operator', principal, {
          kind: 'workload',
          enabled: true,
          attributes: {},
          markings: [],
        });
        await assert.rejects(source.resource(context, objectId), { code: 'NOT_FOUND' });
        await authority.syncPrincipal(environment, 'operator', principal, {
          kind: 'workload',
          enabled: true,
          attributes: {},
          markings: ['protected'],
        });
        assert.equal((await source.resource(context, objectId)).resourceId, objectId);
        const current = await store.get(objectFile.id),
          parent = await store.get(ontology.id);
        await filesystem.apply(context, 'rename-source', [
          {
            op: 'move',
            id: current.id,
            expectedRevision: current.revision,
            parentId: parent.id,
            expectedParentRevision: parent.revision,
            expectedSourceParentRevision: parent.revision,
            name: 'renamed.object.json',
          },
        ]);
        const changed = await source.publish(
          context,
          ontology.id,
          (await source.checkout(context, ontology.id)).digest,
        );
        assert.notEqual(changed.digest, release.digest);
        assert.equal(
          changed.resources.find(
            (resource) => resource.reference === 'stations/ObjectType/Station',
          )!.id,
          objectId,
        );
      },
    );

    await t.test(
      'sibling roots are independent and duplicate/nested source registration is rejected',
      async () => {
        const another = await directory(project.id, 'maintenance');
        await file(another.id, '.ontology.json', {
          version: 1,
          name: 'Maintenance',
          namespace: 'maintenance',
        });
        await source.register(
          context,
          another.id,
          (await store.get(another.id)).revision,
          'ontology',
        );
        const nested = await directory(ontology.id, 'nested');
        await file(nested.id, '.ontology.json', {
          version: 1,
          name: 'Nested',
          namespace: 'nested',
        });
        await assert.rejects(
          source.register(context, nested.id, (await store.get(nested.id)).revision, 'ontology'),
          { code: 'SOURCE_INVALID' },
        );
        const duplicate = await directory(project.id, 'duplicate');
        await file(duplicate.id, '.ontology.json', {
          version: 1,
          name: 'Duplicate',
          namespace: 'stations',
        });
        await assert.rejects(
          source.register(
            context,
            duplicate.id,
            (await store.get(duplicate.id)).revision,
            'ontology',
          ),
          { code: 'FILESYSTEM_CONFLICT' },
        );
        const descriptor = (await store.list(nested.id))[0];
        await filesystem.apply(context, 'remove-nested-descriptor', [
          {
            op: 'delete',
            id: descriptor.id,
            expectedRevision: descriptor.revision,
            expectedParentRevision: (await store.get(nested.id)).revision,
          },
        ]);
        await filesystem.apply(context, 'remove-nested', [
          {
            op: 'delete',
            id: nested.id,
            expectedRevision: (await store.get(nested.id)).revision,
            expectedParentRevision: (await store.get(ontology.id)).revision,
          },
        ]);
      },
    );

    await t.test(
      'plans bind release, principal, current policy and expected active revision',
      async () => {
        const plan = await source.plan(context, ontology.id, firstDigest);
        await security.setPolicy('operator', datasetFile.id, '0', {
          markings: [{ environment, marking: 'protected' }],
          grants: [],
        });
        await assert.rejects(source.apply(context, plan.id), { code: 'AUTHORIZATION_CHANGED' });
        const first = await source.plan(context, ontology.id, firstDigest),
          second = await source.plan(context, ontology.id, firstDigest);
        await source.apply(context, first.id);
        await assert.rejects(source.apply(context, second.id), { code: 'FILESYSTEM_CONFLICT' });
        const expired = await source.plan(context, ontology.id, firstDigest);
        await db.pool.query(
          "UPDATE moria.fs_release_plans SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
          [expired.id],
        );
        await assert.rejects(source.apply(context, expired.id), { code: 'PLAN_EXPIRED' });
      },
    );

    await t.test(
      'missing declarations are retained until explicit dependency-checked deletion',
      async () => {
        const current = await store.get(objectFile.id);
        await filesystem.apply(context, 'omit-type', [
          {
            op: 'delete',
            id: current.id,
            expectedRevision: current.revision,
            expectedParentRevision: (await store.get(ontology.id)).revision,
          },
        ]);
        const release = await source.publish(
          context,
          ontology.id,
          (await source.checkout(context, ontology.id)).digest,
        );
        const retained = await source.apply(
          context,
          (await source.plan(context, ontology.id, release.digest)).id,
        );
        assert.ok(retained.resources.includes('stations/ObjectType/Station'));
        await assert.rejects(
          source.plan(context, ontology.id, release.digest, ['stations/Dataset/source']),
          { code: 'VALIDATION_FAILED' },
        );
        const removed = await source.apply(
          context,
          (await source.plan(context, ontology.id, release.digest, ['stations/ObjectType/Station']))
            .id,
        );
        assert.ok(!removed.resources.includes('stations/ObjectType/Station'));
        await assert.rejects(source.resource(context, objectId), { code: 'NOT_FOUND' });
        await assert.rejects(
          db.catalog.transaction(
            (client) => authority.resourceAccess(client, context.session, objectId),
            environment,
            'repeatable-read',
            'shared',
          ),
          { code: 'NOT_FOUND' },
        );
        assert.equal(
          (
            await db.pool.query(
              'SELECT resource_id::text FROM moria.fs_resource_identities WHERE root_id=$1 AND reference=$2',
              [ontology.id, 'stations/ObjectType/Station'],
            )
          ).rows[0].resource_id,
          objectId,
        );
        await source.apply(context, (await source.plan(context, ontology.id, firstDigest)).id);
        assert.equal((await source.resource(context, objectId)).resourceId, objectId);
      },
    );

    async function isolatedOntology(namespace: string, parentId = project.id) {
      const root = await directory(parentId, namespace),
        declarations = await directory(root.id, 'declarations');
      await file(root.id, '.ontology.json', { version: 1, name: namespace, namespace });
      await file(root.id, 'data.resource.json', {
        ...dataset,
        metadata: { namespace, name: 'source' },
      });
      const definition = {
        ...object,
        metadata: { namespace, name: 'Station' },
        spec: {
          ...object.spec,
          source: { resource: `${namespace}/Dataset/source`, mapping: { id: 'id' } },
        },
      };
      const declaration = await file(declarations.id, 'station.object.json', definition);
      await source.register(context, root.id, (await store.get(root.id)).revision, 'ontology');
      const publish = () =>
        source
          .checkout(context, root.id)
          .then((snapshot) => source.publish(context, root.id, snapshot.digest));
      const first = await publish(),
        initialPlan = await source.plan(context, root.id, first.digest);
      await source.apply(context, initialPlan.id);
      const resource = await source.resource(
        context,
        first.resources.find((item) => item.reference === `${namespace}/ObjectType/Station`)!.id,
      );
      return { root, declarations, declaration, definition, publish, first, resource, initialPlan };
    }
    await t.test('replacement retains future restrictions on its original source', async () => {
      const unit = await isolatedOntology('replacement'),
        holding = await directory(project.id, 'old-replacement-source');
      await filesystem.apply(context, randomUUID(), [
        {
          op: 'move',
          id: unit.declaration.id,
          expectedRevision: unit.declaration.revision,
          parentId: holding.id,
          expectedParentRevision: holding.revision,
          expectedSourceParentRevision: (await store.get(unit.declarations.id)).revision,
          name: 'previous.object.json',
        },
      ]);
      await file(unit.declarations.id, 'current.object.json', unit.definition);
      const replacement = await unit.publish();
      await source.apply(
        context,
        (await source.plan(context, unit.root.id, replacement.digest)).id,
      );
      await security.setPolicy('operator', holding.id, '0', {
        markings: [{ environment, marking: 'protected' }],
        grants: [],
      });
      assert.ok(
        (await source.resource(context, unit.resource.resourceId)).markings.some(
          (label) => label.marking === 'protected',
        ),
      );
    });
    await t.test(
      'omitted definitions retain future restrictions on their declaration directory',
      async () => {
        const unit = await isolatedOntology('omission');
        await filesystem.apply(context, randomUUID(), [
          {
            op: 'delete',
            id: unit.declaration.id,
            expectedRevision: unit.declaration.revision,
            expectedParentRevision: (await store.get(unit.declarations.id)).revision,
          },
        ]);
        const omitted = await unit.publish();
        await source.apply(context, (await source.plan(context, unit.root.id, omitted.digest)).id);
        await security.setPolicy('operator', unit.declarations.id, '0', {
          markings: [{ environment, marking: 'protected' }],
          grants: [],
        });
        assert.ok(
          (await source.resource(context, unit.resource.resourceId)).markings.some(
            (label) => label.marking === 'protected',
          ),
        );
      },
    );
    for (const operation of ['publish', 'delete'] as const)
      await t.test(`activation respects a resource-level ${operation} deny`, async () => {
        const unit = await isolatedOntology(`deny-${operation}`);
        await security.setPolicy('operator', unit.resource.entryId, '0', {
          markings: [],
          grants: [
            {
              subject: { kind: 'principal', id: instanceId },
              operations: [operation],
              effect: 'deny',
              inherit: false,
            },
          ],
        });
        await file(unit.root.id, 'README.md', 'Changed source envelope');
        const release = await unit.publish();
        await assert.rejects(
          async () =>
            source.apply(
              context,
              (
                await source.plan(
                  context,
                  unit.root.id,
                  release.digest,
                  operation === 'delete' ? [`deny-${operation}/ObjectType/Station`] : [],
                )
              ).id,
            ),
          { code: 'FORBIDDEN' },
        );
        if (operation === 'publish')
          await assert.rejects(source.apply(context, unit.initialPlan.id), { code: 'FORBIDDEN' });
        assert.equal(
          (await source.resource(context, unit.resource.resourceId)).digest,
          unit.first.digest,
        );
      });

    async function anotherProject(name: string) {
      const receipt = await store.apply({ environment, principal, application }, randomUUID(), [
        {
          op: 'create',
          kind: 'directory',
          parentId: filesystemRoot,
          expectedParentRevision: (await store.get(filesystemRoot)).revision,
          name,
          scope: { projectId: randomUUID() },
        },
      ]);
      return receipt.entries.find((entry) => entry.name === name)!;
    }
    await t.test(
      'identical namespaces in independent projects activate without identity collisions',
      async () => {
        const first = await isolatedOntology('shared_namespace'),
          secondProject = await anotherProject('second-project'),
          second = await isolatedOntology('shared_namespace', secondProject.id);
        assert.notEqual(first.resource.resourceId, second.resource.resourceId);
        assert.equal((await filesystem.get(context, first.resource.entryId)).projectId, projectId);
        assert.equal(
          (await filesystem.get(context, second.resource.entryId)).projectId,
          secondProject.projectId,
        );
        assert.equal(
          (await source.resource(context, 'shared_namespace/ObjectType/Station', projectId))
            .resourceId,
          first.resource.resourceId,
        );
        assert.equal(
          (
            await source.resource(
              context,
              'shared_namespace/ObjectType/Station',
              secondProject.projectId!,
            )
          ).resourceId,
          second.resource.resourceId,
        );
        await assert.rejects(
          async () => source.resource(context, 'shared_namespace/ObjectType/Station'),
          {
            code: 'SOURCE_INVALID',
          },
        );
        await assert.rejects(
          db.catalog.transaction(
            (client) =>
              authority.resourceAccess(
                client,
                context.session,
                'shared_namespace/ObjectType/Station',
              ),
            environment,
            'repeatable-read',
            'shared',
          ),
          { code: 'FORBIDDEN' },
        );
        const firstDataset = await source.resource(
          context,
          first.first.resources.find(
            (item) => item.reference === 'shared_namespace/Dataset/source',
          )!.id,
        );
        await security.setPolicy('operator', firstDataset.entryId, '0', {
          markings: [{ environment, marking: 'protected' }],
          grants: [],
        });
        assert.ok(
          (await source.resource(context, first.resource.resourceId)).markings.some(
            (label) => label.marking === 'protected',
          ),
        );
        assert.ok(
          !(await source.resource(context, second.resource.resourceId)).markings.some(
            (label) => label.marking === 'protected',
          ),
        );
      },
    );
    await t.test(
      'pinned cross-project inputs retain their owning project classifications',
      async () => {
        const imported = await isolatedOntology('foreign'),
          other = await anotherProject('import-project'),
          root = await directory(other.id, 'consumer');
        await file(root.id, '.ontology.json', {
          version: 1,
          name: 'Consumer',
          namespace: 'consumer',
          dependencies: [{ rootId: imported.root.id, digest: imported.first.digest }],
        });
        await file(root.id, 'station.object.json', {
          ...object,
          metadata: { namespace: 'consumer', name: 'Station' },
          spec: {
            ...object.spec,
            source: { resource: 'foreign/Dataset/source', mapping: { id: 'id' } },
          },
        });
        await source.register(context, root.id, (await store.get(root.id)).revision, 'ontology');
        const release = await source.publish(
          context,
          root.id,
          (await source.checkout(context, root.id)).digest,
        );
        await source.apply(context, (await source.plan(context, root.id, release.digest)).id);
        const upstream = await source.resource(
            context,
            imported.first.resources.find((item) => item.reference === 'foreign/Dataset/source')!
              .id,
          ),
          downstream = release.resources.find(
            (item) => item.reference === 'consumer/ObjectType/Station',
          )!;
        await security.setPolicy('operator', upstream.entryId, '0', {
          markings: [{ environment, marking: 'protected' }],
          grants: [],
        });
        assert.ok(
          (await source.resource(context, downstream.id)).markings.some(
            (item) => item.marking === 'protected',
          ),
        );
      },
    );
    await t.test(
      'identical imported names from different projects require explicit disambiguation',
      async () => {
        const roots = (
          await db.pool.query(
            "SELECT entry_id,latest_release FROM moria.fs_roles WHERE namespace='shared_namespace'",
          )
        ).rows;
        assert.equal(roots.length, 2);
        const root = await directory(project.id, 'ambiguous-imports');
        await file(root.id, '.ontology.json', {
          version: 1,
          name: 'Ambiguous',
          namespace: 'ambiguous_imports',
          dependencies: roots.map((item) => ({
            rootId: item.entry_id,
            digest: item.latest_release,
          })),
        });
        await source.register(context, root.id, (await store.get(root.id)).revision, 'ontology');
        const release = await source.publish(
          context,
          root.id,
          (await source.checkout(context, root.id)).digest,
        );
        await assert.rejects(source.plan(context, root.id, release.digest), {
          code: 'SOURCE_INVALID',
        });
      },
    );
    await t.test(
      'active transitive dependencies prevent retirement, and retired consumers release that protection',
      async () => {
        const dependency = await isolatedOntology('chain_source');
        async function consumer(name: string, rootId: string, digest: string) {
          const root = await directory(project.id, name);
          await file(root.id, '.ontology.json', {
            version: 1,
            name,
            namespace: name,
            dependencies: [{ rootId, digest }],
          });
          const role = await source.register(
            context,
            root.id,
            (await store.get(root.id)).revision,
            'ontology',
          );
          const release = await source.publish(
            context,
            root.id,
            (await source.checkout(context, root.id)).digest,
          );
          return { root, role, release };
        }
        const bridge = await consumer('chain_bridge', dependency.root.id, dependency.first.digest),
          active = await consumer('chain_consumer', bridge.root.id, bridge.release.digest);
        await source.apply(
          context,
          (await source.plan(context, active.root.id, active.release.digest)).id,
        );
        await assert.rejects(source.retire(context, dependency.root.id, '1'), {
          code: 'SOURCE_INVALID',
        });
        await source.retire(context, active.root.id, active.role.revision);
        await source.retire(context, dependency.root.id, '1');
      },
    );
    await t.test('same-project pinned source dependencies remain usable', async () => {
      const root = await directory(project.id, 'same-project-consumer');
      await file(root.id, '.ontology.json', {
        version: 1,
        name: 'Consumer',
        namespace: 'same_project_consumer',
        dependencies: [{ rootId: ontology.id, digest: firstDigest }],
      });
      await source.register(context, root.id, (await store.get(root.id)).revision, 'ontology');
      const release = await source.publish(
        context,
        root.id,
        (await source.checkout(context, root.id)).digest,
      );
      const activated = await source.apply(
        context,
        (await source.plan(context, root.id, release.digest)).id,
      );
      assert.ok(activated.resources.includes('_project/Namespace/same_project_consumer'));
    });

    await t.test(
      'cross-project transform outputs require active writable placements and inherit producer restrictions',
      async () => {
        const targets = await isolatedOntology('targets');
        await file(targets.root.id, 'sink.resource.json', {
          ...dataset,
          metadata: { namespace: 'targets', name: 'sink' },
        });
        const targetRelease = await targets.publish(),
          other = await anotherProject('transform-project'),
          root = await directory(other.id, 'jobs');
        await file(root.id, '.repository.json', {
          version: 1,
          name: 'Jobs',
          namespace: 'jobs',
          runtime: 'python-duckdb-v1',
          dependencies: [{ rootId: targets.root.id, digest: targetRelease.digest }],
        });
        await source.register(context, root.id, (await store.get(root.id)).revision, 'repository');
        const transform: Resource = {
          apiVersion: 'moria/v1alpha2',
          kind: 'Transform',
          metadata: { namespace: 'jobs', name: 'copy' },
          spec: {
            artifact: { uri: 'oci://fixture/transforms', digest: `sha256:${'b'.repeat(64)}` },
            runtime: 'python-duckdb-v1',
            inputs: ['targets/Dataset/source'],
            outputs: ['targets/Dataset/sink'],
            limits: { timeoutSeconds: 60, memoryMiB: 512, cpuMillis: 1000 },
          },
        };
        const release = await source.publish(
          context,
          root.id,
          (await source.checkout(context, root.id)).digest,
          [transform],
        );
        await assert.rejects(
          source.apply(context, (await source.plan(context, root.id, release.digest)).id),
          { code: 'SOURCE_INVALID' },
        );
        assert.equal(
          (
            await db.pool.query('SELECT 1 FROM moria.fs_active_releases WHERE root_id=$1', [
              root.id,
            ])
          ).rowCount,
          0,
        );
        await source.apply(
          context,
          (await source.plan(context, targets.root.id, targetRelease.digest)).id,
        );
        const sink = await source.resource(
          context,
          targetRelease.resources.find((item) => item.reference === 'targets/Dataset/sink')!.id,
        );
        const revision = await security.setPolicy('operator', sink.entryId, '0', {
          markings: [],
          grants: [
            {
              subject: { kind: 'principal', id: instanceId },
              operations: ['write'],
              inherit: true,
              effect: 'deny',
            },
          ],
        });
        await assert.rejects(
          source.apply(context, (await source.plan(context, root.id, release.digest)).id),
          { code: 'FORBIDDEN' },
        );
        assert.equal(
          (
            await db.pool.query('SELECT 1 FROM moria.fs_active_releases WHERE root_id=$1', [
              root.id,
            ])
          ).rowCount,
          0,
        );
        await security.setPolicy('operator', sink.entryId, revision, { markings: [], grants: [] });
        await source.apply(context, (await source.plan(context, root.id, release.digest)).id);
        await security.setPolicy('operator', root.id, '0', {
          markings: [{ environment, marking: 'protected' }],
          grants: [],
        });
        assert.ok(
          (await source.resource(context, sink.resourceId)).markings.some(
            (item) => item.marking === 'protected',
          ),
        );
      },
    );
    await t.test(
      'code repositories publish CI export manifests without loading source modules',
      async () => {
        const repository = await directory(project.id, 'logic');
        await file(repository.id, '.repository.json', {
          version: 1,
          name: 'Logic',
          namespace: 'logic',
          runtime: 'typescript-v1',
        });
        await file(repository.id, 'main.ts', 'throw new Error("control plane must not run code");');
        const repositoryRole = await source.register(
          context,
          repository.id,
          (await store.get(repository.id)).revision,
          'repository',
        );
        const fn: Resource = {
          apiVersion: 'moria/v1alpha2',
          kind: 'Function',
          metadata: { namespace: 'logic', name: 'calculate' },
          spec: {
            artifact: { uri: 'oci://fixture/functions', digest: `sha256:${'a'.repeat(64)}` },
            runtime: 'typescript-v1',
            inputs: { value: { type: 'string', nullable: false } },
            outputs: { value: { type: 'string', nullable: false } },
            effect: 'readOnly',
            limits: { timeoutSeconds: 30, memoryMiB: 512, cpuMillis: 1000 },
          },
        };
        const release = await source.publish(
          context,
          repository.id,
          (await source.checkout(context, repository.id)).digest,
          [fn],
        );
        assert.ok(
          release.resources.some((resource) => resource.reference === 'logic/Function/calculate'),
        );
        const stale = await source.checkout(context, repository.id);
        await file(repository.id, 'another.ts', 'export const example = 42;');
        await assert.rejects(source.publish(context, repository.id, stale.digest, [fn]), {
          code: 'FILESYSTEM_CONFLICT',
        });
        await assert.rejects(
          filesystem.apply(context, randomUUID(), [
            {
              op: 'move',
              id: repository.id,
              expectedRevision: (await store.get(repository.id)).revision,
              parentId: ontology.id,
              expectedParentRevision: (await store.get(ontology.id)).revision,
              expectedSourceParentRevision: (await store.get(project.id)).revision,
              name: 'nested',
            },
          ]),
          { code: 'SOURCE_INVALID' },
        );
        const activated = await source.apply(
          context,
          (await source.plan(context, repository.id, release.digest)).id,
        );
        assert.ok(activated.resources.includes('logic/Function/calculate'));
        await source.retire(context, repository.id, repositoryRole.revision);
        await assert.rejects(source.resource(context, 'logic/Function/calculate', projectId), {
          code: 'NOT_FOUND',
        });
        await assert.rejects(source.checkout(context, repository.id), { code: 'NOT_FOUND' });
      },
    );
  },
);
