import assert from 'node:assert/strict';
import test from 'node:test';
import { createHash, randomUUID } from 'node:crypto';
import { join } from 'node:path';
import { database } from './fixtures/catalog-storage/database.js';
import { createLocalFilesystem } from '../services/filesystem/src/local.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import {
  filesystemOperations,
  type FilesystemChange,
  type FilesystemEntry,
  type FilesystemReceipt,
} from '../packages/contracts/filesystem.js';

interface Expected {
  id: string;
  parentId: string | null;
  name: string;
  kind: 'directory' | 'file';
  revision: number;
  deleted: boolean;
  body?: Uint8Array;
}
const content = (body: Uint8Array) => ({
  size: body.length,
  digest: `sha256:${createHash('sha256').update(body).digest('hex')}`,
  mediaType: 'application/octet-stream',
});
const stream = (body: Uint8Array) =>
  new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(body);
      controller.close();
    },
  });

for (const initialSeed of [0x143825, 0xabc721, 0x76e902])
  test(
    `seed ${initialSeed.toString(16)}: mixed filesystem operations agree with an independent directory journal`,
    { timeout: 120000 },
    async (t) => {
      const db = await database();
      t.after(db.close);
      const runtime = await createLocalFilesystem(db.catalog, join(db.root, 'bytes')),
        fs = runtime.filesystem,
        authority = fs.security.authority;
      const environment = 'exercise',
        application = 'exercise',
        principal = 'owner',
        operations = [...filesystemOperations.map((op) => `filesystem:${op}`), 'instance:manage'];
      await db.catalog.ensureEnvironment(environment);
      await authority.setApplication(environment, 'operator', application, operations);
      await authority.syncPrincipal(environment, 'operator', principal, {
        kind: 'workload',
        enabled: true,
        attributes: {},
        markings: [],
      });
      await authority.setGrants(environment, 'operator', principal, application, operations);
      const sessions = new WorkloadSessions(authority),
        credential = await sessions.mint({ environment, application, principal }, 'operator'),
        context = { session: await sessions.authenticate(`Bearer ${credential.token}`) };
      const home = await runtime.management.ensureHome(context),
        receipt = await fs.apply(context, randomUUID(), [
          {
            op: 'create',
            kind: 'directory',
            parentId: home.home.id,
            expectedParentRevision: home.home.revision,
            name: 'exercise',
          },
        ]);
      const root = receipt.entries.find((entry) => entry.name === 'exercise')!;
      const entries = new Map<string, Expected>(),
        history = new Map<string, Map<number, Expected>>();
      let seed = initialSeed,
        counter = 0;
      const random = (maximum: number) => {
        seed ^= seed << 13;
        seed ^= seed >>> 17;
        seed ^= seed << 5;
        return (seed >>> 0) % maximum;
      };
      const pick = <T>(values: T[]) => values[random(values.length)];
      const remember = (ids: string[]) => {
        for (const id of new Set(ids)) {
          const entry = entries.get(id)!;
          if (!history.has(id)) history.set(id, new Map());
          history.get(id)!.set(entry.revision, structuredClone(entry));
        }
      };
      entries.set(root.id, {
        id: root.id,
        parentId: root.parentId,
        name: 'exercise',
        kind: 'directory',
        revision: 1,
        deleted: false,
      });
      remember([root.id]);
      const assertEntry = (actual: FilesystemEntry, expected: Expected) => {
        assert.deepEqual(
          {
            id: actual.id,
            parentId: actual.parentId,
            name: actual.name,
            kind: actual.kind,
            revision: actual.revision,
            deleted: actual.deleted,
          },
          {
            id: expected.id,
            parentId: expected.parentId,
            name: expected.name,
            kind: expected.kind,
            revision: String(expected.revision),
            deleted: expected.deleted,
          },
        );
        if (expected.body) assert.equal(actual.content?.digest, content(expected.body).digest);
      };
      const assertReceipt = (result: FilesystemReceipt) => {
        for (const entry of result.entries) assertEntry(entry, entries.get(entry.id)!);
      };
      async function verify() {
        const live = [...entries.values()].filter((entry) => !entry.deleted);
        for (const directory of live.filter((entry) => entry.kind === 'directory')) {
          const result = await fs.list(context, directory.id);
          assert.equal(result.nextCursor, undefined);
          const expected = live
            .filter((entry) => entry.parentId === directory.id)
            .sort((a, b) => a.name.localeCompare(b.name));
          assert.deepEqual(
            result.entries.map((entry) => entry.id),
            expected.map((entry) => entry.id),
          );
          for (const entry of result.entries) assertEntry(entry, entries.get(entry.id)!);
        }
        for (const entry of live) {
          assertEntry(await fs.get(context, entry.id), entry);
          if (entry.body)
            assert.deepEqual(
              new Uint8Array(
                await new Response((await fs.readFile(context, entry.id)).stream).arrayBuffer(),
              ),
              entry.body,
            );
        }
      }
      for (let step = 0; step < 140; step++) {
        const live = [...entries.values()].filter((entry) => !entry.deleted),
          directories = live.filter((entry) => entry.kind === 'directory'),
          files = live.filter((entry) => entry.kind === 'file');
        const leaves = live.filter(
          (entry) => entry.id !== root.id && !live.some((child) => child.parentId === entry.id),
        );
        const operation = random(6),
          name = `n${String(++counter).padStart(4, '0')}`,
          key = randomUUID();
        let result: FilesystemReceipt | undefined, change: FilesystemChange | undefined;
        if (operation < 2 || live.length === 1) {
          const parent = pick(directories),
            kind = operation === 0 ? 'directory' : 'file',
            body = new Uint8Array(random(128)).fill(random(256));
          if (kind === 'file')
            result = await fs.writeFile(
              context,
              key,
              {
                op: 'create',
                parentId: parent.id,
                expectedParentRevision: String(parent.revision),
                name,
                content: content(body),
              },
              stream(body),
            );
          else {
            change = {
              op: 'create',
              kind: 'directory',
              parentId: parent.id,
              expectedParentRevision: String(parent.revision),
              name,
            };
            result = await fs.apply(context, key, [change]);
          }
          const created = result.entries.find(
            (entry) => entry.parentId === parent.id && entry.name === name,
          )!;
          entries.set(created.id, {
            id: created.id,
            parentId: parent.id,
            name,
            kind,
            revision: 1,
            deleted: false,
            ...(kind === 'file' ? { body } : {}),
          });
          parent.revision++;
          remember([created.id, parent.id]);
        } else if (operation === 2 && files.length) {
          const file = pick(files),
            body = new Uint8Array(random(128)).fill(random(256));
          result = await fs.writeFile(
            context,
            key,
            {
              op: 'write',
              id: file.id,
              expectedRevision: String(file.revision),
              content: content(body),
            },
            stream(body),
          );
          file.revision++;
          file.body = body;
          remember([file.id]);
        } else if (operation === 3 && leaves.length) {
          const entry = pick(leaves),
            parent = pick(directories.filter((item) => item.id !== entry.id)),
            oldParent = entries.get(entry.parentId!)!;
          change = {
            op: 'move',
            id: entry.id,
            expectedRevision: String(entry.revision),
            parentId: parent.id,
            expectedParentRevision: String(parent.revision),
            expectedSourceParentRevision: String(oldParent.revision),
            name,
          };
          result = await fs.apply(context, key, [change]);
          entry.parentId = parent.id;
          entry.name = name;
          entry.revision++;
          oldParent.revision++;
          if (parent.id !== oldParent.id) parent.revision++;
          remember([entry.id, parent.id, oldParent.id]);
        } else if (operation === 4 && leaves.length) {
          const entry = pick(leaves),
            parent = entries.get(entry.parentId!)!;
          change = {
            op: 'delete',
            id: entry.id,
            expectedRevision: String(entry.revision),
            expectedParentRevision: String(parent.revision),
          };
          result = await fs.apply(context, key, [change]);
          entry.deleted = true;
          entry.revision++;
          parent.revision++;
          remember([entry.id, parent.id]);
          await assert.rejects(fs.get(context, entry.id), { code: 'NOT_FOUND' });
        } else if (files.length) {
          const file = pick(files),
            before = await fs.get(context, file.id),
            body = new Uint8Array([9, 8, 7]);
          await assert.rejects(
            fs.writeFile(
              context,
              key,
              {
                op: 'write',
                id: file.id,
                expectedRevision: String(file.revision + 1),
                content: content(body),
              },
              stream(body),
            ),
            { code: 'FILESYSTEM_CONFLICT' },
          );
          assert.deepEqual(await fs.get(context, file.id), before);
        }
        if (result) {
          assertReceipt(result);
          if (change) assert.deepEqual(await fs.apply(context, key, [change]), result);
        }
        if (step % 35 === 0) await verify();
      }
      await verify();
      await db.pool.query(
        "UPDATE moria.fs_content_candidates SET created_at=clock_timestamp()-interval '25 hours'",
      );
      let removed = 0;
      for (let pass = 0; pass < 20; pass++) {
        const result = await fs.collect(context);
        removed += result.removed;
        if (result.examined < 50) break;
      }
      assert.ok(removed > 0);
      for (const [id, versions] of history) {
        const actual: FilesystemEntry[] = [];
        let before: string | undefined;
        do {
          const page = await fs.history(context, id, before);
          actual.push(...page.entries);
          before = page.nextRevision;
        } while (before);
        assert.equal(actual.length, versions.size);
        for (const version of actual) {
          const expected = versions.get(Number(version.revision))!;
          assertEntry(version, expected);
          if (expected.body)
            assert.deepEqual(
              new Uint8Array(
                await new Response(
                  (await fs.readFile(context, id, version.revision)).stream,
                ).arrayBuffer(),
              ),
              expected.body,
            );
        }
      }
      t.diagnostic(
        '140 deterministic operations; current directory membership, revisions, replay, failed writes, retained bytes and orphan collection verified.',
      );
    },
  );
