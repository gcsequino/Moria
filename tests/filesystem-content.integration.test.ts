import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import test from 'node:test';
import { database } from './fixtures/catalog-storage/database.js';
import { startS3 } from './fixtures/storage-s3/server.js';
import { FilesystemStore } from '../services/filesystem/src/store.js';
import { FilesystemService } from '../services/filesystem/src/service.js';
import { Authority } from '../services/security/src/authority.js';
import { FilesystemAuthority } from '../services/security/src/filesystem.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { configuredFilesystem } from '../services/filesystem/src/local.js';
import {
  createAuthorizedLocalStorage,
  createAuthorizedS3Storage,
} from '../services/storage/src/authority.js';
import {
  filesystemRoot,
  filesystemOperations,
  type FilesystemFileWrite,
} from '../packages/contracts/filesystem.js';

function stream(bytes: Uint8Array) {
  let offset = 0;
  return new ReadableStream<Uint8Array>(
    {
      pull(controller) {
        if (offset === bytes.length) {
          controller.close();
          return;
        }
        const next = bytes.subarray(offset, offset + 32);
        offset += next.byteLength;
        controller.enqueue(next);
      },
    },
    { highWaterMark: 0 },
  );
}
const descriptor = (bytes: Uint8Array) => ({
  size: bytes.byteLength,
  digest: `sha256:${createHash('sha256').update(bytes).digest('hex')}`,
  mediaType: 'application/json',
});

for (const provider of ['local', 's3'] as const) {
  test(
    `${provider}: protected file content verifies integrity, replays and revokes during transfer`,
    { timeout: 120000 },
    async (t) => {
      const db = await database();
      t.after(db.close);
      const s3 = provider === 's3' ? await startS3() : undefined;
      if (s3) t.after(s3.close);
      const authority = new Authority(db.catalog),
        security = new FilesystemAuthority(authority),
        store = new FilesystemStore(db.catalog);
      const environment = `files-${provider}`,
        application = 'files-app',
        principal = 'writer';
      await db.catalog.ensureEnvironment(environment);
      const operations = [
        ...filesystemOperations.map((op) => `filesystem:${op}`),
        'instance:manage',
      ];
      await authority.setApplication(environment, 'operator', application, operations);
      await authority.syncPrincipal(environment, 'operator', principal, {
        kind: 'workload',
        enabled: true,
        attributes: {},
        markings: [],
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
      const sessions = new WorkloadSessions(authority),
        token = await sessions.mint({ environment, application, principal }, 'operator');
      const context = { session: await sessions.authenticate(`Bearer ${token.token}`) };
      const renewSession = async () => {
        const renewed = await sessions.mint({ environment, application, principal }, 'operator');
        context.session = await sessions.authenticate(`Bearer ${renewed.token}`);
      };
      const binding = {
        root: join(db.root, 'files'),
        warehouseId: 'filesystem',
        tableId: 'content',
        maxObjectBytes: 1024 * 1024,
        chunkBytes: 64,
        endpoint: s3?.endpoint ?? '',
        credentials: s3?.credentials ?? { accessKeyId: '', secretAccessKey: '' },
        bucket: s3?.bucket ?? '',
        prefix: 'files',
        region: 'us-east-1',
      };
      await mkdir(binding.root, { recursive: true });
      let afterAuthorization: (() => void) | undefined;
      const service = new FilesystemService(store, security, {
        warehouseId: binding.warehouseId,
        tableId: binding.tableId,
        maxFileBytes: 1024,
        storage: (scope, current) => {
          const checkCurrent: typeof current.checkCurrent = async (...args) => {
            const allowed = await current.checkCurrent(...args);
            afterAuthorization?.();
            return allowed;
          };
          return provider === 'local'
            ? createAuthorizedLocalStorage(binding, scope, { checkCurrent })
            : createAuthorizedS3Storage(binding, scope, { checkCurrent });
        },
      });
      const body = new TextEncoder().encode(
        JSON.stringify({ description: 'synthetic filesystem content', values: Array(30).fill(42) }),
      );
      const request: FilesystemFileWrite = {
        op: 'create',
        parentId: filesystemRoot,
        expectedParentRevision: (await store.get(filesystemRoot)).revision,
        name: 'file.json',
        content: descriptor(body),
      };
      let fileId = '';

      await t.test(
        'verified bytes publish once and identical retries retain the original receipt',
        async () => {
          const receipt = await service.writeFile(context, 'upload', request, stream(body));
          fileId = receipt.entries.find((entry) => entry.name === 'file.json')!.id;
          assert.deepEqual(
            await service.writeFile(context, 'upload', request, stream(body)),
            receipt,
          );
          const read = await service.readFile(context, fileId);
          assert.deepEqual(new Uint8Array(await new Response(read.stream).arrayBuffer()), body);
          const different = new TextEncoder().encode('{}');
          await assert.rejects(
            service.writeFile(
              context,
              'upload',
              { ...request, content: descriptor(different) },
              stream(different),
            ),
            { code: 'IDEMPOTENCY_CONFLICT' },
          );
          assert.equal((await store.list(filesystemRoot)).length, 1);
        },
      );

      await t.test(
        'digest/size failures, stale revisions and oversized requests preserve the current head',
        async () => {
          const current = await store.get(fileId),
            bytes = new TextEncoder().encode('{"changed":true}');
          const write: FilesystemFileWrite = {
            op: 'write',
            id: fileId,
            expectedRevision: current.revision,
            content: descriptor(bytes),
          };
          await assert.rejects(
            service.writeFile(
              context,
              'bad-digest',
              { ...write, content: { ...write.content, digest: `sha256:${'0'.repeat(64)}` } },
              stream(bytes),
            ),
            { code: 'CONTENT_MISMATCH' },
          );
          await assert.rejects(
            service.writeFile(
              context,
              'bad-size',
              { ...write, content: { ...write.content, size: bytes.length + 1 } },
              stream(bytes),
            ),
            { code: 'CONTENT_MISMATCH' },
          );
          await assert.rejects(
            service.writeFile(
              context,
              'too-big',
              { ...write, content: { ...write.content, size: 1025 } },
              stream(bytes),
            ),
            { code: 'LIMIT_EXCEEDED' },
          );
          assert.equal((await store.get(fileId)).revision, current.revision);
          await service.writeFile(context, 'write', write, stream(bytes));
          await assert.rejects(service.writeFile(context, 'stale', write, stream(bytes)), {
            code: 'FILESYSTEM_CONFLICT',
          });
          const original = await service.readFile(context, fileId, current.revision);
          assert.deepEqual(new Uint8Array(await new Response(original.stream).arrayBuffer()), body);
        },
      );

      await t.test(
        'revocation blocks the next read demand and cancels an in-progress upload',
        async () => {
          const opened = await service.readFile(context, fileId, '1'),
            reader = opened.stream.getReader();
          assert.ok(!(await reader.read()).done);
          await security.setEnabled('operator', instanceId, false);
          await assert.rejects(reader.read());
          reader.releaseLock();
          await security.setEnabled('operator', instanceId, true);
          await assert.rejects(service.readFile(context, fileId), { code: 'UNAUTHENTICATED' });
          await renewSession();
          const parent = await store.get(filesystemRoot),
            key = randomUUID();
          let cancelled = false,
            sent = false;
          const source = new ReadableStream<Uint8Array>(
            {
              async pull(controller) {
                if (sent) {
                  controller.close();
                  return;
                }
                sent = true;
                await security.setEnabled('operator', instanceId, false);
                controller.enqueue(body.subarray(0, 32));
              },
              cancel() {
                cancelled = true;
              },
            },
            { highWaterMark: 0 },
          );
          await assert.rejects(
            service.writeFile(
              context,
              key,
              { ...request, name: 'revoked.json', expectedParentRevision: parent.revision },
              source,
            ),
          );
          assert.ok(cancelled);
          assert.equal(source.locked, false);
          assert.equal((await store.get(filesystemRoot)).revision, parent.revision);
          await security.setEnabled('operator', instanceId, true);
          await assert.rejects(service.readFile(context, fileId), { code: 'UNAUTHENTICATED' });
          await renewSession();
        },
      );

      await t.test(
        'orphan collection preserves every retained file revision and removes failed or replay candidates',
        async () => {
          await db.pool.query(
            "UPDATE moria.fs_content_candidates SET created_at=clock_timestamp()-interval '25 hours'",
          );
          const before = (
            await db.pool.query(
              'SELECT count(*)::integer AS count FROM moria.fs_content_candidates',
            )
          ).rows[0].count;
          assert.ok(before > 2);
          const result = await service.collect(context);
          assert.equal(result.examined, before);
          assert.ok(result.retained > 0);
          assert.ok(result.removed > 0);
          const files = (await service.list(context, filesystemRoot)).entries.filter(
            (entry) => entry.kind === 'file',
          );
          for (const file of files) {
            for (const version of (await service.history(context, file.id)).entries) {
              const read = await service.readFile(context, version.id, version.revision);
              assert.equal(
                (await new Response(read.stream).arrayBuffer()).byteLength,
                version.content!.size,
              );
            }
          }
          assert.deepEqual(await service.collect(context), {
            removed: 0,
            retained: 0,
            examined: 0,
          });
        },
      );

      await t.test('empty files and explicit transfer cancellation remain supported', async () => {
        const empty = new Uint8Array(),
          parent = await store.get(filesystemRoot);
        const receipt = await service.writeFile(
          context,
          'empty',
          {
            ...request,
            name: 'empty.json',
            expectedParentRevision: parent.revision,
            content: descriptor(empty),
          },
          stream(empty),
        );
        const entry = receipt.entries.find((item) => item.name === 'empty.json')!;
        const opened = await service.readFile(context, entry.id);
        assert.equal((await new Response(opened.stream).arrayBuffer()).byteLength, 0);
        const controller = new AbortController();
        controller.abort();
        await assert.rejects(
          service.writeFile(
            context,
            'aborted',
            {
              op: 'write',
              id: entry.id,
              expectedRevision: entry.revision,
              content: descriptor(empty),
            },
            stream(empty),
            controller.signal,
          ),
        );
        assert.equal((await store.get(entry.id)).revision, entry.revision);
      });
      await t.test(
        'competing uploads publish one head while concurrent identical retries share one receipt',
        async () => {
          const initial = new Uint8Array([0]),
            parent = await store.get(filesystemRoot);
          const created = await service.writeFile(
            context,
            randomUUID(),
            {
              op: 'create',
              parentId: parent.id,
              expectedParentRevision: parent.revision,
              name: 'concurrent.bin',
              content: descriptor(initial),
            },
            stream(initial),
          );
          const entry = created.entries.find((item) => item.name === 'concurrent.bin')!;
          const bodies = Array.from({ length: 12 }, (_, index) => new Uint8Array([index + 1]));
          const outcomes = await Promise.allSettled(
            bodies.map((bytes) =>
              service.writeFile(
                context,
                randomUUID(),
                {
                  op: 'write',
                  id: entry.id,
                  expectedRevision: entry.revision,
                  content: descriptor(bytes),
                },
                stream(bytes),
              ),
            ),
          );
          assert.equal(outcomes.filter((outcome) => outcome.status === 'fulfilled').length, 1);
          for (const outcome of outcomes)
            if (outcome.status === 'rejected')
              assert.equal(outcome.reason.code, 'FILESYSTEM_CONFLICT');
          const winner = outcomes.findIndex((outcome) => outcome.status === 'fulfilled'),
            head = await store.get(entry.id);
          assert.equal(head.revision, '2');
          assert.deepEqual(
            new Uint8Array(
              await new Response((await service.readFile(context, entry.id)).stream).arrayBuffer(),
            ),
            bodies[winner],
          );
          assert.deepEqual(
            new Uint8Array(
              await new Response(
                (await service.readFile(context, entry.id, '1')).stream,
              ).arrayBuffer(),
            ),
            initial,
          );
          const current = await store.get(filesystemRoot),
            key = randomUUID(),
            request: FilesystemFileWrite = {
              op: 'create',
              parentId: current.id,
              expectedParentRevision: current.revision,
              name: 'retry-race.bin',
              content: descriptor(initial),
            };
          const receipts = await Promise.all(
            Array.from({ length: 8 }, () =>
              service.writeFile(context, key, request, stream(initial)),
            ),
          );
          for (const receipt of receipts) assert.deepEqual(receipt, receipts[0]);
          assert.equal(
            (await store.get(filesystemRoot)).revision,
            String(BigInt(current.revision) + 1n),
          );
        },
      );

      await t.test(
        'a producer cannot mutate already-verified bytes during an authorization await',
        async () => {
          const original = new Uint8Array([1, 2, 3, 4]),
            expected = new Uint8Array(original);
          let delivered = false;
          afterAuthorization = () => {
            if (delivered) original.fill(9);
          };
          try {
            const input = new ReadableStream<Uint8Array>(
              {
                pull(controller) {
                  if (delivered) controller.close();
                  else {
                    controller.enqueue(original);
                    delivered = true;
                  }
                },
              },
              { highWaterMark: 0 },
            );
            const receipt = await service.writeFile(
              context,
              randomUUID(),
              {
                op: 'create',
                parentId: filesystemRoot,
                expectedParentRevision: (await store.get(filesystemRoot)).revision,
                name: 'mutable.bin',
                content: descriptor(expected),
              },
              input,
            );
            const entry = receipt.entries.find((entry) => entry.name === 'mutable.bin')!,
              opened = await service.readFile(context, entry.id);
            assert.deepEqual(
              new Uint8Array(await new Response(opened.stream).arrayBuffer()),
              expected,
            );
          } finally {
            afterAuthorization = undefined;
          }
        },
      );

      if (provider === 's3')
        await t.test(
          'configured S3 instances share bytes and reject storage relocation',
          async () => {
            await assert.rejects(
              configuredFilesystem(db.catalog, binding.root, {
                MORIA_FILESYSTEM_BACKEND: 'unknown',
              }),
              { code: 'FILESYSTEM_INVALID' },
            );
            await assert.rejects(
              configuredFilesystem(db.catalog, binding.root, { MORIA_FILESYSTEM_BACKEND: 's3' }),
              { code: 'FILESYSTEM_INVALID' },
            );
            const env = {
              MORIA_FILESYSTEM_BACKEND: 's3',
              MORIA_FILESYSTEM_S3_ENDPOINT: binding.endpoint,
              MORIA_FILESYSTEM_S3_BUCKET: binding.bucket,
              MORIA_FILESYSTEM_S3_REGION: binding.region,
              MORIA_FILESYSTEM_S3_PREFIX: binding.prefix,
              MORIA_FILESYSTEM_S3_ACCESS_KEY_ID: binding.credentials.accessKeyId,
              MORIA_FILESYSTEM_S3_SECRET_ACCESS_KEY: binding.credentials.secretAccessKey,
            };
            await configuredFilesystem(db.catalog, binding.root, env);
            const second = await configuredFilesystem(db.catalog, binding.root, env),
              empty = (await store.list(filesystemRoot)).find(
                (entry) => entry.name === 'empty.json',
              )!;
            assert.equal(
              (
                await new Response(
                  (await second.filesystem.readFile(context, empty.id)).stream,
                ).arrayBuffer()
              ).byteLength,
              0,
            );
            await assert.rejects(
              configuredFilesystem(db.catalog, binding.root, {
                ...env,
                MORIA_FILESYSTEM_S3_PREFIX: 'another-instance',
              }),
              { code: 'FILESYSTEM_CONFLICT' },
            );
          },
        );
    },
  );
}
