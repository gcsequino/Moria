import assert from 'node:assert/strict';
import { createHash, randomUUID, randomBytes } from 'node:crypto';
import { mkdir, readFile, writeFile, symlink, access, readdir } from 'node:fs/promises';
import { request as httpRequest } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import { filesystemCommand } from '../apps/cli/src/filesystem.js';
import { join } from 'node:path';
import test from 'node:test';
import { database } from './fixtures/catalog-storage/database.js';
import { createWorkspace } from '../apps/local/src/workspace.js';
import { createLocalServer } from '../apps/local/src/server.js';
import type { AppAuth } from '../services/app-host/src/auth.js';
import { AppHost } from '../services/app-host/src/store.js';
import { createLocalFilesystem } from '../services/filesystem/src/local.js';
import { filesystemClient } from '../packages/platform/src/filesystem.js';
import { createTransport, ApiError } from '../packages/platform/src/transport.js';

test(
  'filesystem platform API client provides scoped authoring and coherent source deployment',
  { timeout: 120000 },
  async (t) => {
    const db = await database();
    t.after(db.close);
    const workspace = await createWorkspace(db.catalog, join(db.root, 'iceberg'));
    const runtime = await createLocalFilesystem(db.catalog, join(db.root, 'files'));
    await createLocalFilesystem(db.catalog, join(db.root, 'files'));
    await assert.rejects(createLocalFilesystem(db.catalog, join(db.root, 'other-files')), {
      code: 'FILESYSTEM_CONFLICT',
    });
    const key = randomBytes(32).toString('base64url');
    const server = createLocalServer(
      workspace,
      key,
      new AppHost(join(db.root, 'apps')),
      undefined,
      undefined,
      undefined,
      undefined,
      runtime,
    );
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    t.after(
      () =>
        new Promise<void>((resolve, reject) =>
          server.close((error) => (error ? reject(error) : resolve())),
        ),
    );
    const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
    const client = (profile: string) =>
      filesystemClient(
        createTransport({
          baseUrl: origin,
          fetch: (url, init) =>
            fetch(url, {
              ...init,
              headers: { ...init?.headers, 'x-moria-key': key, 'x-moria-profile': profile },
            }),
        }),
      );
    const owner = client('operator'),
      reader = client('west-editor'),
      outsider = client('east-reader');
    const home = await owner.home(),
      readerHome = await reader.home(),
      outsiderHome = await outsider.home();

    await t.test(
      'non-browser bearer writes authenticate on a browser-enabled management host',
      async () => {
        const browserAuth = {
          allowRecovery: true,
          sessions: { binding: { origin: 'http://127.0.0.1' } },
          handle: async () => false,
        } as unknown as AppAuth;
        const protectedServer = createLocalServer(
          workspace,
          key,
          new AppHost(join(db.root, 'apps')),
          undefined,
          browserAuth,
          undefined,
          undefined,
          runtime,
        );
        await new Promise<void>((resolve) => protectedServer.listen(0, '127.0.0.1', resolve));
        try {
          const port = (protectedServer.address() as { port: number }).port;
          const token = await workspace.token('operator');
          const response = await fetch(`http://127.0.0.1:${port}/api/management/v1/fs/resolve`, {
            method: 'POST',
            headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
            body: JSON.stringify({ path: `/users/${home.home.name}` }),
          });
          assert.equal(response.status, 200, await response.clone().text());
          assert.equal((await response.json()).id, home.home.id);
          const denied = await fetch(`http://127.0.0.1:${port}/api/management/v1/fs/resolve`, {
            method: 'POST',
            headers: {
              authorization: `Bearer ${'a'.repeat(43)}`,
              'content-type': 'application/json',
            },
            body: JSON.stringify({ path: `/users/${home.home.name}` }),
          });
          assert.equal(denied.status, 401);
        } finally {
          await new Promise<void>((resolve) => protectedServer.close(() => resolve()));
        }
      },
    );

    await t.test(
      'verified local profiles and workload sessions cannot impersonate each other',
      async () => {
        assert.notEqual(home.principalId, readerHome.principalId);
        await assert.rejects(
          reader.get(outsiderHome.home.id),
          (error) => error instanceof ApiError && error.status === 404,
        );
        const unauthenticated = await fetch(`${origin}/api/management/v1/fs/home`);
        assert.equal(unauthenticated.status, 401);
        const forged = await fetch(`${origin}/api/management/v1/fs/home`, {
          headers: { origin, 'x-moria-key': key, 'x-moria-profile': 'operator' },
        });
        assert.equal(forged.status, 401);
        const bearer = await workspace.token('west-editor');
        const authorized = await fetch(`${origin}/api/management/v1/fs/home`, {
          headers: { authorization: `Bearer ${bearer}`, 'x-moria-profile': 'operator' },
        });
        assert.equal(authorized.status, 200);
        assert.equal(
          ((await authorized.json()) as { principalId: string }).principalId,
          readerHome.principalId,
        );
        const malformed = await fetch(`${origin}/api/management/v1/fs/entries/not-a-uuid`, {
          headers: { 'x-moria-key': key },
        });
        assert.equal(malformed.status, 400);
        const original = await reader.policy(readerHome.home.id);
        const denied = await reader.setPolicy(readerHome.home.id, original.revision, {
          markings: original.markings,
          grants: [
            ...original.grants,
            {
              subject: { kind: 'principal', id: readerHome.principalId },
              operations: ['read'],
              inherit: false,
              effect: 'deny',
            },
          ],
        });
        await assert.rejects(
          reader.home(),
          (error) => error instanceof ApiError && error.status === 404,
        );
        assert.equal((await reader.resolve('/groups')).name, 'groups');
        await reader.setPolicy(readerHome.home.id, denied.revision, {
          markings: original.markings,
          grants: original.grants,
        });
      },
    );

    await t.test(
      'ordinary authors can create projects and versioned source files without instance administration',
      async () => {
        const projects = await reader.resolve(`/users/${readerHome.home.name}/projects`);
        const project = await reader.createProject(projects.id, 'observatory', projects.revision);
        const source = await reader.resolve(
          `/users/${readerHome.home.name}/projects/observatory/source`,
        );
        assert.equal(source.projectId, project.projectId);
        const receipt = await reader.changes(
          [
            {
              op: 'create',
              parentId: source.id,
              expectedParentRevision: source.revision,
              name: 'ontology',
              kind: 'directory',
            },
          ],
          { idempotencyKey: 'create-ontology' },
        );
        const ontology = receipt.entries.find((entry) => entry.name === 'ontology')!;
        const bytes = new TextEncoder().encode(
          JSON.stringify({ version: 1, name: 'Observatory', namespace: 'observatory' }),
        );
        const digest = `sha256:${createHash('sha256').update(bytes).digest('hex')}`;
        const written = await reader.writeFile(
          {
            op: 'create',
            parentId: ontology.id,
            expectedParentRevision: ontology.revision,
            name: '.ontology.json',
            content: { digest, size: bytes.length, mediaType: 'application/json' },
          },
          bytes,
          { idempotencyKey: 'write-descriptor' },
        );
        const descriptor = written.entries.find((entry) => entry.name === '.ontology.json')!;
        const read = await reader.readFile(descriptor.id);
        assert.match(read.headers.get('content-disposition') ?? '', /^attachment;/);
        assert.equal(read.headers.get('content-security-policy'), 'sandbox');
        assert.equal(read.headers.get('x-moria-digest'), digest);
        assert.deepEqual(new Uint8Array(await read.arrayBuffer()), bytes);
        await assert.rejects(
          outsider.readFile(descriptor.id),
          (error) => error instanceof ApiError && error.status === 404,
        );
        const role = await reader.register(
          ontology.id,
          'ontology',
          (await reader.get(ontology.id)).revision,
        );
        assert.equal(role.namespace, 'observatory');
        const checkout = await reader.checkout(ontology.id);
        assert.equal(checkout.files[0].path, '.ontology.json');
        const published = await reader.publish(ontology.id, checkout.digest);
        const plan = await reader.plan(ontology.id, published.digest);
        const activated = await reader.apply(plan.id);
        assert.equal(activated.digest, published.digest);
        assert.deepEqual(await reader.apply(plan.id), activated);
        assert.equal(activated.runtimeReady, false);
        assert.equal(
          (
            await reader.resource('_project/Namespace/observatory', {
              projectId: project.projectId,
            })
          ).resourceId,
          published.resources[0].id,
        );
        await assert.rejects(
          reader.resource('_project/Namespace/observatory'),
          (error) => error instanceof ApiError && error.status === 400,
        );
        for (const request of [
          () => outsider.role(descriptor.id),
          () => outsider.role(randomUUID()),
          () => outsider.policy(descriptor.id),
          () => outsider.policy(randomUUID()),
          () => outsider.resource(published.resources[0].id),
          () => outsider.resource(randomUUID()),
          () => outsider.checkout(ontology.id),
          () => outsider.checkout(randomUUID()),
          () => outsider.release(published.digest),
          () => outsider.release(`sha256:${'0'.repeat(64)}`),
        ])
          await assert.rejects(request(), {
            status: 404,
            code: 'NOT_FOUND',
            message: 'Entry not found',
          });
      },
    );

    await t.test(
      'disconnecting during an HTTP upload leaves no visible file and cleans private staging',
      async () => {
        const parent = await reader.get(readerHome.home.id),
          bytes = new Uint8Array(65536).fill(7),
          staging = join(db.root, 'files', 'filesystem', 'content', 'staging');
        const query = new URLSearchParams({
          name: 'interrupted.bin',
          expectedRevision: parent.revision,
        });
        const upload = httpRequest(
          `${origin}/api/management/v1/fs/entries/${parent.id}/files?${query}`,
          {
            method: 'POST',
            headers: {
              'x-moria-key': key,
              'x-moria-profile': 'west-editor',
              'idempotency-key': randomUUID(),
              'content-type': 'application/octet-stream',
              'content-length': bytes.length,
              'x-moria-size': String(bytes.length),
              'x-moria-digest': `sha256:${createHash('sha256').update(bytes).digest('hex')}`,
            },
          },
        );
        upload.on('error', () => {});
        try {
          upload.write(bytes.subarray(0, 1024));
          for (let attempt = 0; attempt < 500 && !(await readdir(staging)).length; attempt++)
            await delay(10);
          assert.equal((await readdir(staging)).length, 1);
        } finally {
          const closed = new Promise<void>((resolve) => upload.once('close', resolve));
          upload.destroy();
          await closed;
        }
        for (let attempt = 0; attempt < 500 && (await readdir(staging)).length; attempt++)
          await delay(10);
        assert.deepEqual(await readdir(staging), []);
        assert.equal((await reader.get(parent.id)).revision, parent.revision);
        await assert.rejects(
          reader.resolve(`/users/${readerHome.home.name}/interrupted.bin`),
          (error) => error instanceof ApiError && error.status === 404,
        );
        assert.equal((await reader.home()).home.id, parent.id);
      },
    );

    await t.test(
      'CLI round trips a local source checkout and rejects stale or escaped files',
      async () => {
        const credentials = join(db.root, 'cli');
        await mkdir(credentials);
        await writeFile(join(credentials, 'credentials.json'), JSON.stringify({ accessKey: key }));
        const options = { url: origin, directory: credentials, profile: 'west-editor' };
        const path = `/users/${readerHome.home.name}/projects/observatory/source/ontology`;
        const local = join(db.root, 'checkout');
        await filesystemCommand(['checkout', path, local], options);
        assert.match(await readFile(join(local, '.ontology.json'), 'utf8'), /observatory/);
        await assert.rejects(access(join(local, '.moria-checkout-incomplete')));
        await writeFile(join(local, 'README.md'), 'local source\n');
        await filesystemCommand(['push', local], options);
        const remote = await reader.resolve(`${path}/README.md`);
        assert.equal(await (await reader.readFile(remote.id)).text(), 'local source\n');
        const published = (await filesystemCommand(['publish', path], options)) as {
          digest: string;
        };
        const plan = (await filesystemCommand(['plan', path, published.digest], options)) as {
          id: string;
        };
        assert.equal(
          ((await filesystemCommand(['apply', plan.id], options)) as { digest: string }).digest,
          published.digest,
        );
        const stale = join(db.root, 'stale');
        await filesystemCommand(['checkout', path, stale], options);
        await writeFile(join(local, 'README.md'), 'updated\n');
        await filesystemCommand(['push', local], options);
        await assert.rejects(filesystemCommand(['push', stale], options), {
          code: 'FILESYSTEM_CONFLICT',
        });
        await symlink(join(credentials, 'credentials.json'), join(local, 'escaped.json'));
        await assert.rejects(filesystemCommand(['push', local], options), /symlinks/);
      },
    );

    await t.test(
      'strict JSON, query and mutation-key boundaries reject ambiguous requests',
      async () => {
        const route = `${origin}/api/management/v1/fs`;
        const headers = { 'x-moria-key': key, 'content-type': 'application/json' };
        const bad = await fetch(`${route}/resolve`, {
          method: 'POST',
          headers,
          body: JSON.stringify({ path: '/', principal: 'operator' }),
        });
        assert.equal(bad.status, 400);
        const duplicate = await fetch(
          `${route}/entries/${home.home.id}/children?cursor=a&cursor=b`,
          { headers },
        );
        assert.equal(duplicate.status, 400);
        const missingKey = await fetch(`${route}/changes`, {
          method: 'POST',
          headers,
          body: JSON.stringify({ changes: [] }),
        });
        assert.equal(missingKey.status, 400);
        const error = (await missingKey.json()) as {
          error: { code: string; correlationId: string };
        };
        assert.equal(error.error.code, 'IDEMPOTENCY_KEY_INVALID');
        assert.ok(error.error.correlationId);
      },
    );
  },
);
