import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { Catalog } from '../services/catalog/src/catalog.js';
import { FilesystemStore } from '../services/filesystem/src/store.js';
import {
  filesystemRoot,
  type FileContent,
  type FilesystemChange,
  type FilesystemEntry,
} from '../packages/contracts/filesystem.js';
import { database } from './fixtures/catalog-storage/database.js';

const actor = {
  environment: 'filesystem-test',
  principal: 'filesystem-user',
  application: 'filesystem-test',
};
const bytes: FileContent = {
  location: { version: 1, warehouseId: 'source', tableId: 'files', objectId: 'revision-one' },
  digest: `sha256:${'a'.repeat(64)}`,
  size: 12,
  mediaType: 'application/json',
};
const next = (revision: string) => (BigInt(revision) + 1n).toString();

test(
  'filesystem metadata uses real transactions, durable versions and scoped replay',
  { timeout: 120000 },
  async (t) => {
    const db = await database();
    t.after(db.close);
    const store = new FilesystemStore(db.catalog);
    const apply = (changes: FilesystemChange[], key = randomUUID()) =>
      store.apply(actor, key, changes);
    async function create(
      parentId: string,
      name: string,
      extra: Partial<Extract<FilesystemChange, { op: 'create' }>> = {},
    ) {
      const parent = await store.get(parentId);
      const receipt = await apply([
        {
          op: 'create',
          parentId,
          expectedParentRevision: parent.revision,
          name,
          kind: 'directory',
          ...extra,
        },
      ]);
      return receipt.entries.find((entry) => entry.parentId === parentId && entry.name === name)!;
    }
    async function move(current: FilesystemEntry, parentId: string, name: string) {
      return apply([
        {
          op: 'move',
          id: current.id,
          expectedRevision: current.revision,
          parentId,
          name,
          expectedParentRevision: (await store.get(parentId)).revision,
          expectedSourceParentRevision: (await store.get(current.parentId!)).revision,
        },
      ]);
    }
    async function remove(current: FilesystemEntry) {
      return apply([
        {
          op: 'delete',
          id: current.id,
          expectedRevision: current.revision,
          expectedParentRevision: (await store.get(current.parentId!)).revision,
        },
      ]);
    }

    await t.test('files keep content and identity through writes and renames', async () => {
      const source = await create(filesystemRoot, 'source');
      const file = await create(source.id, '.ontology.json', { kind: 'file', content: bytes });
      assert.equal((await store.resolve('/source/.ontology.json')).id, file.id);
      assert.deepEqual(await store.list(source.id), [file]);
      const replacement = {
        ...bytes,
        location: { ...bytes.location, objectId: 'revision-two' },
        digest: `sha256:${'b'.repeat(64)}`,
      };
      const written = await apply([
        { op: 'write', id: file.id, expectedRevision: file.revision, content: replacement },
      ]);
      const updated = written.entries[0];
      assert.equal((await store.get(source.id)).revision, '2');
      await move(updated, source.id, 'ontology.json');
      const recreated = new FilesystemStore(new Catalog(db.pool));
      assert.equal((await recreated.resolve('/source/ontology.json')).id, file.id);
      assert.deepEqual((await recreated.version(file.id, '1')).content, bytes);
      assert.deepEqual((await recreated.version(file.id, '2')).content, replacement);
      await assert.rejects(store.resolve('/source/.ontology.json'), { code: 'NOT_FOUND' });
      const renamed = await store.get(file.id);
      await remove(renamed);
      await assert.rejects(store.get(file.id), { code: 'NOT_FOUND' });
      assert.equal((await store.version(file.id, next(renamed.revision))).deleted, true);
      const reused = await create(source.id, 'ontology.json', { kind: 'file', content: bytes });
      assert.notEqual(reused.id, file.id);
      assert.equal((await store.version(file.id, '1')).name, '.ontology.json');
    });

    await t.test(
      'a failed batch rolls back entry heads, parent versions and replay records',
      async () => {
        const parent = await create(filesystemRoot, 'atomic');
        const key = randomUUID();
        const change: FilesystemChange = {
          op: 'create',
          parentId: parent.id,
          expectedParentRevision: parent.revision,
          name: 'same',
          kind: 'directory',
        };
        await assert.rejects(
          apply([change, { ...change, expectedParentRevision: next(parent.revision) }], key),
          { code: 'FILESYSTEM_CONFLICT' },
        );
        assert.deepEqual(await store.list(parent.id), []);
        assert.equal((await store.get(parent.id)).revision, parent.revision);
        await assert.rejects(store.version(parent.id, next(parent.revision)), {
          code: 'NOT_FOUND',
        });
        await apply([change], key);
        assert.equal((await store.list(parent.id)).length, 1);
        await assert.rejects(create(parent.id, 'same', { kind: 'file', content: bytes }), {
          code: 'FILESYSTEM_CONFLICT',
        });
      },
    );

    await t.test(
      'concurrent writers conflict and lost-ack retries commit exactly one mutation',
      async () => {
        const parent = await create(filesystemRoot, 'concurrent');
        const file = await create(parent.id, 'file.json', { kind: 'file', content: bytes });
        const change: FilesystemChange = {
          op: 'write',
          id: file.id,
          expectedRevision: file.revision,
          content: bytes,
        };
        const results = await Promise.allSettled([apply([change]), apply([change])]);
        assert.equal(results.filter((result) => result.status === 'fulfilled').length, 1);
        const rejected = results.find((result) => result.status === 'rejected');
        assert.ok(rejected?.status === 'rejected');
        assert.equal(rejected.reason.code, 'FILESYSTEM_CONFLICT');
        const key = randomUUID(),
          currentParent = await store.get(parent.id);
        const creation: FilesystemChange = {
          op: 'create',
          parentId: parent.id,
          expectedParentRevision: currentParent.revision,
          name: 'replayed',
          kind: 'directory',
        };
        const receipts = await Promise.all([apply([creation], key), apply([creation], key)]);
        assert.deepEqual(receipts[0], receipts[1]);
        assert.deepEqual(
          await new FilesystemStore(db.catalog).apply(actor, key, [creation]),
          receipts[0],
        );
        await assert.rejects(apply([{ ...creation, name: 'different' }], key), {
          code: 'IDEMPOTENCY_CONFLICT',
        });
        assert.equal((await store.get(parent.id)).revision, next(currentParent.revision));
        const delegatedParent = await store.get(parent.id);
        await store.apply({ ...actor, delegation: ['another-user'] }, key, [
          { ...creation, name: 'delegated', expectedParentRevision: delegatedParent.revision },
        ]);
      },
    );

    await t.test(
      'moves are atomic, reject cycles and enforce both parent preconditions',
      async () => {
        const first = await create(filesystemRoot, 'first');
        const second = await create(filesystemRoot, 'second');
        const child = await create(first.id, 'child');
        const nested = await create(child.id, 'nested');
        await assert.rejects(move(await store.get(child.id), nested.id, 'cycle'), {
          code: 'FILESYSTEM_INVALID',
        });
        await assert.rejects(move(await store.get(first.id), first.id, 'self'), {
          code: 'FILESYSTEM_INVALID',
        });
        await assert.rejects(remove(await store.get(first.id)), { code: 'FILESYSTEM_INVALID' });
        await assert.rejects(
          apply([
            {
              op: 'move',
              id: filesystemRoot,
              expectedRevision: (await store.get(filesystemRoot)).revision,
              parentId: second.id,
              expectedParentRevision: (await store.get(second.id)).revision,
              expectedSourceParentRevision: '1',
              name: 'root',
            },
          ]),
          { code: 'FILESYSTEM_INVALID' },
        );
        const liveChild = await store.get(child.id),
          sourceParent = await store.get(first.id),
          destination = await store.get(second.id);
        await assert.rejects(
          apply([
            {
              op: 'move',
              id: child.id,
              expectedRevision: liveChild.revision,
              parentId: second.id,
              name: 'moved',
              expectedParentRevision: destination.revision,
              expectedSourceParentRevision: '1',
            },
          ]),
          { code: 'FILESYSTEM_CONFLICT' },
        );
        await move(liveChild, second.id, 'moved');
        assert.equal((await store.resolve('/second/moved/nested')).id, nested.id);
        assert.equal((await store.get(first.id)).revision, next(sourceParent.revision));
        assert.equal((await store.get(second.id)).revision, next(destination.revision));
      },
    );

    await t.test(
      'runtime placements inherit scope, stay unique and cannot bypass domain retirement',
      async () => {
        await db.catalog.ensureEnvironment('filesystem-dev');
        const project = await create(filesystemRoot, 'project', {
          scope: { projectId: 'project-a' },
        });
        const environment = await create(project.id, 'dev', {
          scope: { projectId: 'project-a', environmentId: 'filesystem-dev' },
        });
        const folder = await create(environment.id, 'datasets');
        const resourceId = randomUUID();
        const resource = await create(folder.id, 'stations', { kind: 'resource', resourceId });
        assert.equal(resource.projectId, 'project-a');
        assert.equal(resource.environmentId, 'filesystem-dev');
        await assert.rejects(
          create(environment.id, 'duplicate', { kind: 'resource', resourceId }),
          { code: 'FILESYSTEM_CONFLICT' },
        );
        await assert.rejects(move(resource, filesystemRoot, 'stations'), {
          code: 'FILESYSTEM_INVALID',
        });
        await assert.rejects(remove(resource), { code: 'UNSUPPORTED_CAPABILITY' });
        await assert.rejects(create(project.id, 'other', { scope: { projectId: 'project-b' } }), {
          code: 'FILESYSTEM_INVALID',
        });
        await assert.rejects(
          create(filesystemRoot, 'orphan', { kind: 'resource', resourceId: randomUUID() }),
          { code: 'FILESYSTEM_INVALID' },
        );
        await move(await store.get(project.id), filesystemRoot, 'renamed-project');
        assert.equal(
          (await store.resolve('/renamed-project/dev/datasets/stations')).id,
          resource.id,
        );
        assert.equal((await store.get(resource.id)).resourceId, resourceId);
      },
    );

    await t.test(
      'invalid paths, descriptors and changes fail before persisting; inputs are captured',
      async () => {
        for (const path of [
          'relative',
          '//source',
          '/source/',
          '/source/../escape',
          '/source\\escape',
          '/e\u0301',
          '/\ud800',
        ])
          await assert.rejects(store.resolve(path), { code: 'FILESYSTEM_INVALID' });
        const parent = await create(filesystemRoot, 'validation');
        for (const entryName of [
          '.',
          '..',
          'a/b',
          'a\\b',
          '\0',
          'e\u0301',
          'x'.repeat(129),
          '\ud800',
        ])
          await assert.rejects(create(parent.id, entryName), { code: 'FILESYSTEM_INVALID' });
        await assert.rejects(
          create(parent.id, 'bad', { kind: 'file', content: { ...bytes, size: -1 } }),
          { code: 'FILESYSTEM_INVALID' },
        );
        await assert.rejects(
          apply([
            { op: 'delete', id: parent.id, expectedRevision: '01', expectedParentRevision: '1' },
          ]),
          { code: 'FILESYSTEM_INVALID' },
        );
        await assert.rejects(store.apply(actor, 'empty', []), { code: 'FILESYSTEM_INVALID' });
        await assert.rejects(store.apply(actor, 'too-many', Array(101).fill({ op: 'delete' })), {
          code: 'FILESYSTEM_INVALID',
        });
        const changes: FilesystemChange[] = [
          {
            op: 'create',
            parentId: parent.id,
            expectedParentRevision: parent.revision,
            name: 'captured',
            kind: 'directory',
          },
        ];
        const pending = apply(changes);
        (changes[0] as Extract<FilesystemChange, { op: 'create' }>).name = 'changed';
        await pending;
        assert.equal((await store.list(parent.id))[0].name, 'captured');
      },
    );

    await t.test('path limits are enforced on creation and subtree moves', async () => {
      let current = await store.get(filesystemRoot);
      for (let depth = 0; depth < 64; depth++) current = await create(current.id, 'd');
      assert.equal((await store.resolve('/' + Array(64).fill('d').join('/'))).id, current.id);
      await assert.rejects(create(current.id, 'too-deep'), { code: 'FILESYSTEM_INVALID' });
      const subtree = await create(filesystemRoot, 'subtree');
      await create(subtree.id, 'descendant');
      await assert.rejects(move(await store.get(subtree.id), current.parentId!, 'subtree'), {
        code: 'FILESYSTEM_INVALID',
      });
    });
  },
);
