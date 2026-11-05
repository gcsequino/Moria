import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import { publishedResources } from './fixtures/catalog-storage/source.js';
import test from 'node:test';
import { FilesystemStore } from '../services/filesystem/src/store.js';
import { FilesystemService } from '../services/filesystem/src/service.js';
import {
  FilesystemAuthority,
  type FilesystemContext,
} from '../services/security/src/filesystem.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import {
  filesystemRoot,
  filesystemOperations,
  type FilesystemGrant,
  type FilesystemMarking,
  type FilesystemChange,
  type FileContent,
} from '../packages/contracts/filesystem.js';
import { database } from './fixtures/catalog-storage/database.js';
import { ObjectStore } from '../services/ontology/src/store.js';
import { ObjectService } from '../services/ontology/src/service.js';
import { IcebergIndexer } from '../services/ontology/src/indexer.js';

const environment = 'filesystem-security',
  application = 'filesystem-app';
const operations = filesystemOperations.map((operation) => `filesystem:${operation}`);
const content: FileContent = {
  location: { version: 1, warehouseId: 'source', tableId: 'files', objectId: 'fixture' },
  digest: `sha256:${'a'.repeat(64)}`,
  size: 2,
  mediaType: 'application/json',
};

test(
  'protected filesystem composes current identities, inherited policy and atomic mutations',
  { timeout: 120000 },
  async (t) => {
    const db = await database();
    t.after(db.close);
    const authority = new Authority(db.catalog),
      security = new FilesystemAuthority(authority);
    const store = new FilesystemStore(db.catalog),
      service = new FilesystemService(store, security),
      sessions = new WorkloadSessions(authority);
    await db.catalog.ensureEnvironment(environment);
    await authority.setApplication(environment, 'operator', application, operations);
    const identities: Record<string, string> = {},
      contexts: Record<string, FilesystemContext> = {};
    for (const principal of ['owner', 'reader', 'agent', 'outsider']) {
      await authority.syncPrincipal(environment, 'operator', principal, {
        kind: 'workload',
        enabled: true,
        markings: principal === 'reader' || principal === 'outsider' ? [] : ['sensitive'],
        attributes: { groups: principal === 'reader' ? ['analysts'] : [] },
      });
      await authority.setGrants(environment, 'operator', principal, application, operations);
      identities[principal] = await security.createPrincipal('operator', 'workload');
      await security.bindPrincipal('operator', environment, principal, identities[principal]);
      const token = await sessions.mint({ environment, principal, application }, 'operator');
      contexts[principal] = { session: await sessions.authenticate(`Bearer ${token.token}`) };
    }
    const grant = (
      principal: string,
      ops = [...filesystemOperations],
      inherit = true,
      effect: 'allow' | 'deny' = 'allow',
    ): FilesystemGrant => ({
      subject: { kind: 'principal', id: identities[principal] },
      operations: ops,
      inherit,
      effect,
    });
    const group: FilesystemGrant = {
      subject: { kind: 'group', id: 'analysts', environment },
      operations: ['read', 'list'],
      inherit: true,
      effect: 'allow',
    };
    async function policy(
      id: string,
      grants: FilesystemGrant[],
      markings: FilesystemMarking[] = [],
    ) {
      const old = await db.pool.query('SELECT revision FROM moria.fs_policies WHERE entry_id=$1', [
        id,
      ]);
      return security.setPolicy('operator', id, old.rows[0]?.revision ?? '0', { grants, markings });
    }
    await policy(filesystemRoot, [
      grant('owner'),
      grant('reader', ['read', 'list'], false),
      grant('agent', ['read', 'list'], false),
      grant('outsider', ['list'], false),
    ]);
    async function create(
      parentId: string,
      name: string,
      file = false,
      scope?: { projectId: string; environmentId?: string },
    ) {
      const parent = await store.get(parentId);
      const change: FilesystemChange = {
        op: 'create',
        parentId,
        expectedParentRevision: parent.revision,
        name,
        kind: file ? 'file' : 'directory',
        ...(file ? { content } : {}),
        ...(scope ? { scope } : {}),
      };
      const receipt = await store.apply(
        { environment, principal: 'operator', application },
        randomUUID(),
        [change],
      );
      return receipt.entries.find((entry) => entry.parentId === parentId && entry.name === name)!;
    }
    const team = await create(filesystemRoot, 'team');
    await policy(team.id, [group, grant('agent')]);
    const file = await create(team.id, 'definition.json', true);
    const privateFolder = await create(filesystemRoot, 'private');
    const hidden = await create(privateFolder.id, '.secret.json', true);

    await t.test(
      'ID lookup, paths, versions and listings agree without revealing hidden entries',
      async () => {
        assert.equal((await service.get(contexts.reader, file.id)).id, file.id);
        assert.equal((await service.resolve(contexts.reader, '/team/definition.json')).id, file.id);
        assert.equal((await service.version(contexts.reader, file.id, '1')).id, file.id);
        assert.deepEqual(
          (await service.list(contexts.reader, filesystemRoot)).entries.map((entry) => entry.id),
          [team.id],
        );
        await assert.rejects(service.get(contexts.reader, hidden.id), { code: 'NOT_FOUND' });
        await assert.rejects(service.resolve(contexts.reader, '/private/.secret.json'), {
          code: 'NOT_FOUND',
        });
        await assert.rejects(service.version(contexts.reader, hidden.id, '1'), {
          code: 'NOT_FOUND',
        });
        await assert.rejects(service.get(contexts.outsider, file.id), { code: 'NOT_FOUND' });
        assert.deepEqual((await service.list(contexts.outsider, filesystemRoot)).entries, []);
        await policy(file.id, [grant('reader', ['read'], false, 'deny')]);
        await assert.rejects(service.get(contexts.reader, file.id), { code: 'NOT_FOUND' });
        await policy(file.id, []);
        assert.equal((await service.get(contexts.reader, file.id)).id, file.id);
      },
    );

    await t.test(
      'markings tighten existing descendants, old versions and current group eligibility',
      async () => {
        const marked = await create(team.id, 'marked');
        const markedFile = await create(marked.id, 'protected.json', true);
        assert.equal((await service.get(contexts.reader, markedFile.id)).id, markedFile.id);
        await policy(marked.id, [], [{ environment, marking: 'sensitive' }]);
        await assert.rejects(service.get(contexts.reader, markedFile.id), { code: 'NOT_FOUND' });
        await assert.rejects(service.version(contexts.reader, markedFile.id, '1'), {
          code: 'NOT_FOUND',
        });
        assert.equal((await service.get(contexts.owner, markedFile.id)).id, markedFile.id);
        await assert.rejects(policy(marked.id, []), { code: 'UNSUPPORTED_CAPABILITY' });
        await authority.syncPrincipal(environment, 'operator', 'reader', {
          kind: 'workload',
          enabled: true,
          markings: [],
          attributes: { groups: [] },
        });
        await assert.rejects(service.get(contexts.reader, file.id), { code: 'NOT_FOUND' });
        await authority.syncPrincipal(environment, 'operator', 'reader', {
          kind: 'workload',
          enabled: true,
          markings: [],
          attributes: { groups: ['analysts'] },
        });
      },
    );

    await t.test(
      'moving marked content retains restrictions, including receipt replay and tombstone history',
      async () => {
        const source = await create(team.id, 'classified'),
          destination = await create(team.id, 'shared');
        await policy(source.id, [], [{ environment, marking: 'sensitive' }]);
        const entry = await create(source.id, 'move.json', true);
        const key = randomUUID();
        const changes: FilesystemChange[] = [
          {
            op: 'move',
            id: entry.id,
            expectedRevision: entry.revision,
            parentId: destination.id,
            name: 'moved.json',
            expectedParentRevision: (await store.get(destination.id)).revision,
            expectedSourceParentRevision: (await store.get(source.id)).revision,
          },
        ];
        const receipt = await service.apply(contexts.owner, key, changes);
        assert.deepEqual(await service.apply(contexts.owner, key, changes), receipt);
        await assert.rejects(service.get(contexts.reader, entry.id), { code: 'NOT_FOUND' });
        await assert.rejects(service.version(contexts.reader, entry.id, '1'), {
          code: 'NOT_FOUND',
        });
        const current = await store.get(entry.id);
        const deletion: FilesystemChange[] = [
          {
            op: 'delete',
            id: entry.id,
            expectedRevision: current.revision,
            expectedParentRevision: (await store.get(destination.id)).revision,
          },
        ];
        const deleted = await service.apply(contexts.owner, 'delete-marked', deletion);
        assert.deepEqual(await service.apply(contexts.owner, 'delete-marked', deletion), deleted);
        assert.equal((await service.version(contexts.owner, entry.id, '1')).id, entry.id);
        await assert.rejects(service.version(contexts.reader, entry.id, '1'), {
          code: 'NOT_FOUND',
        });
        const audits = await db.pool.query(
          "SELECT 1 FROM moria.instance_audit WHERE operation='retainMoveMarkings' AND target=$1",
          [entry.id],
        );
        assert.equal(audits.rowCount, 1);
      },
    );

    await t.test(
      'application scopes, base permissions and every delegated identity remain required',
      async () => {
        const delegated = {
          session: contexts.agent.session,
          delegation: [contexts.reader.session],
        };
        assert.equal((await service.get(delegated, file.id)).id, file.id);
        await policy(file.id, [grant('reader', ['read'], false, 'deny')]);
        assert.equal((await service.get(contexts.agent, file.id)).id, file.id);
        await assert.rejects(service.get(delegated, file.id), { code: 'NOT_FOUND' });
        await policy(file.id, []);
        await authority.setGrants(environment, 'operator', 'reader', application, []);
        await assert.rejects(service.get(contexts.reader, file.id), { code: 'NOT_FOUND' });
        await authority.setGrants(environment, 'operator', 'reader', application, operations);
        await authority.setApplication(
          environment,
          'operator',
          application,
          operations.filter((operation) => operation !== 'filesystem:read'),
        );
        await assert.rejects(service.get(contexts.owner, file.id), { code: 'NOT_FOUND' });
        await authority.setApplication(environment, 'operator', application, operations);
        await assert.rejects(
          service.get({ session: { environment, principal: 'owner', application } }, file.id),
          { code: 'UNAUTHENTICATED' },
        );
        await assert.rejects(
          security.bindPrincipal('operator', environment, 'reader', identities.owner),
          { code: 'FILESYSTEM_CONFLICT' },
        );
        await sessions.revoke(contexts.reader.session, 'operator');
        await assert.rejects(service.get(delegated, file.id), { code: 'UNAUTHENTICATED' });
        const token = await sessions.mint(
          { environment, principal: 'reader', application },
          'operator',
        );
        contexts.reader = { session: await sessions.authenticate(`Bearer ${token.token}`) };
      },
    );

    await t.test(
      'listing continuations are opaque, identity-bound, policy-bound and preserve expiry',
      async () => {
        const parent = await create(filesystemRoot, 'pages');
        await policy(parent.id, [group]);
        let revision = (await store.get(parent.id)).revision;
        const changes: FilesystemChange[] = [];
        for (let index = 0; index < 100; index++) {
          changes.push({
            op: 'create',
            parentId: parent.id,
            expectedParentRevision: revision,
            name: `item-${index.toString().padStart(3, '0')}`,
            kind: 'directory',
          });
          revision = (BigInt(revision) + 1n).toString();
        }
        await store.apply(
          { environment, principal: 'operator', application },
          randomUUID(),
          changes,
        );
        await create(parent.id, 'item-100');
        const first = await service.list(contexts.reader, parent.id);
        assert.equal(first.entries.length, 100);
        assert.ok(first.nextCursor);
        const second = await service.list(contexts.reader, parent.id, first.nextCursor);
        assert.equal(second.entries.length, 1);
        assert.equal(second.nextCursor, undefined);
        await assert.rejects(service.list(contexts.owner, parent.id, first.nextCursor), {
          code: 'CURSOR_INVALID',
        });
        await create(parent.id, 'item-101');
        await assert.rejects(service.list(contexts.reader, parent.id, first.nextCursor), {
          code: 'CURSOR_INVALIDATED',
        });
        const fresh = await service.list(contexts.reader, parent.id);
        await policy(parent.id, [group]);
        await assert.rejects(service.list(contexts.reader, parent.id, fresh.nextCursor), {
          code: 'CURSOR_INVALIDATED',
        });
        const expired = await service.list(contexts.reader, parent.id);
        await db.pool.query(
          "UPDATE moria.read_cursors SET expires_at=clock_timestamp()-interval '1 second'",
        );
        await assert.rejects(service.list(contexts.reader, parent.id, expired.nextCursor), {
          code: 'CURSOR_EXPIRED',
        });
      },
    );

    await t.test(
      'queued operations see committed revocation and marking changes before their snapshot',
      async () => {
        const blocker = await db.pool.connect();
        const target = await create(team.id, 'queued');
        try {
          await blocker.query(
            "SELECT pg_advisory_lock(hashtextextended('moria/environment/' || $1,0))",
            [environment],
          );
          const mutation = service.apply(contexts.agent, 'disabled-before-snapshot', [
            {
              op: 'create',
              parentId: target.id,
              expectedParentRevision: target.revision,
              name: 'denied',
              kind: 'directory',
            },
          ]);
          const outcome = assert.rejects(mutation, { code: 'FORBIDDEN' });
          await security.setEnabled('operator', identities.agent, false);
          await blocker.query(
            "SELECT pg_advisory_unlock(hashtextextended('moria/environment/' || $1,0))",
            [environment],
          );
          try {
            await outcome;
            assert.deepEqual(await store.list(target.id), []);
          } finally {
            await security.setEnabled('operator', identities.agent, true);
            await assert.rejects(service.get(contexts.agent, target.id), {
              code: 'UNAUTHENTICATED',
            });
            const renewed = await sessions.mint(
              { environment, principal: 'agent', application },
              'operator',
            );
            contexts.agent = { session: await sessions.authenticate(`Bearer ${renewed.token}`) };
          }
          await blocker.query(
            "SELECT pg_advisory_lock(hashtextextended('moria/environment/' || $1,0))",
            [environment],
          );
          const read = service.get(contexts.reader, target.id);
          const denied = assert.rejects(read, { code: 'NOT_FOUND' });
          await policy(target.id, [], [{ environment, marking: 'sensitive' }]);
          await blocker.query(
            "SELECT pg_advisory_unlock(hashtextextended('moria/environment/' || $1,0))",
            [environment],
          );
          await denied;
        } finally {
          blocker.release(true);
        }
      },
    );

    await t.test(
      'ordinary mutations cannot register scopes, inject content or bypass current replay permission',
      async () => {
        const parent = await store.get(team.id);
        const change: FilesystemChange[] = [
          {
            op: 'create',
            parentId: parent.id,
            expectedParentRevision: parent.revision,
            name: 'created',
            kind: 'directory',
          },
        ];
        await service.apply(contexts.agent, 'create-authorized', change);
        await authority.setGrants(
          environment,
          'operator',
          'agent',
          application,
          operations.filter((operation) => operation !== 'filesystem:create'),
        );
        await assert.rejects(service.apply(contexts.agent, 'create-authorized', change), {
          code: 'FORBIDDEN',
        });
        await authority.setGrants(environment, 'operator', 'agent', application, operations);
        const live = await store.get(team.id);
        await assert.rejects(
          service.apply(contexts.owner, 'inject', [
            {
              op: 'create',
              parentId: team.id,
              expectedParentRevision: live.revision,
              name: 'injected',
              kind: 'file',
              content,
            },
          ]),
          { code: 'UNSUPPORTED_CAPABILITY' },
        );
        await assert.rejects(
          service.apply(contexts.owner, 'write', [
            { op: 'write', id: file.id, expectedRevision: file.revision, content },
          ]),
          { code: 'UNSUPPORTED_CAPABILITY' },
        );
        await assert.rejects(
          service.apply(contexts.owner, 'scope', [
            {
              op: 'create',
              parentId: team.id,
              expectedParentRevision: live.revision,
              name: 'scope',
              kind: 'directory',
              scope: { projectId: 'other' },
            },
          ]),
          { code: 'UNSUPPORTED_CAPABILITY' },
        );
      },
    );

    await t.test(
      'file copies retain provenance when a source is marked or deleted later',
      async () => {
        const original = await create(team.id, 'copy-source.json', true),
          parent = await store.get(team.id);
        const request = {
          id: original.id,
          expectedRevision: original.revision,
          parentId: parent.id,
          expectedParentRevision: parent.revision,
          name: 'copied.json',
        };
        const receipt = await service.copy(contexts.owner, 'copy', request),
          copied = receipt.entries.find((entry) => entry.name === 'copied.json')!;
        assert.deepEqual(await service.copy(contexts.owner, 'copy', request), receipt);
        assert.equal(
          (await service.get(contexts.reader, copied.id)).content?.digest,
          content.digest,
        );
        await policy(original.id, [], [{ environment, marking: 'sensitive' }]);
        await assert.rejects(service.get(contexts.reader, copied.id), { code: 'NOT_FOUND' });
        await assert.rejects(service.history(contexts.reader, copied.id), { code: 'NOT_FOUND' });
        await service.apply(contexts.owner, randomUUID(), [
          {
            op: 'delete',
            id: original.id,
            expectedRevision: original.revision,
            expectedParentRevision: (await store.get(team.id)).revision,
          },
        ]);
        await assert.rejects(service.get(contexts.reader, copied.id), { code: 'NOT_FOUND' });
        assert.equal((await service.history(contexts.owner, original.id)).entries[0].deleted, true);
      },
    );

    await t.test(
      'runtime-resource IDs enforce current inherited markings through domain access',
      async () => {
        const [entry] = await publishedResources(
          db.catalog,
          join(db.root, 'source-files'),
          environment,
          [
            {
              apiVersion: 'moria/v1alpha2',
              kind: 'Dataset',
              metadata: { namespace: 'fixture', name: 'dataset' },
              spec: { columns: { uid: { type: 'uid', nullable: false } }, writeModes: ['replace'] },
            },
          ],
        );
        const scope = await store.get(entry.parentId!);
        const resourceId = entry.resourceId!;
        await policy(scope.id, [grant('reader', ['read'])]);
        const domainOperations = [...operations, `${resourceId}:read`];
        await authority.setApplication(environment, 'operator', application, domainOperations);
        await authority.setGrants(environment, 'operator', 'reader', application, domainOperations);
        const objectStore = new ObjectStore(db.catalog),
          properties = { uid: { type: 'uid' as const, nullable: false } };
        const schema = { primaryKey: 'uid', properties },
          state = await authority.current(environment, 'reader', application);
        await objectStore.activate(
          environment,
          resourceId,
          schema,
          [],
          state.epoch,
          (
            await db.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [
              environment,
            ])
          ).rows[0].revision,
        );
        const domain = new ObjectService(objectStore, authority, {
          schema,
          policies: {
            resource: resourceId,
            markings: [],
            properties,
            sourceColumns: properties,
            attributes: {},
          },
        });
        assert.deepEqual((await domain.query(contexts.reader.session, {})).objects, []);
        const check = () =>
          db.catalog.transaction(
            (client) => authority.resourceAccess(client, contexts.reader.session, resourceId),
            environment,
            'repeatable-read',
            'shared',
          );
        await check();
        await policy(
          scope.id,
          [grant('reader', ['read'])],
          [{ environment, marking: 'sensitive' }],
        );
        await assert.rejects(check(), { code: 'FORBIDDEN' });
        await assert.rejects(domain.query(contexts.reader.session, {}), { code: 'FORBIDDEN' });
        const indexOperations = [...domainOperations, 'unbound-target:index'];
        await authority.setApplication(environment, 'operator', application, indexOperations);
        await authority.setGrants(environment, 'operator', 'owner', application, indexOperations);
        const indexer = new IcebergIndexer(objectStore, authority, {
          source: { kind: 'dataset', resource: resourceId, markings: [] },
          target: { resource: 'unbound-target', markings: [], schema },
        });
        await assert.rejects(
          indexer.run(contexts.owner.session, {
            execution: 'filesystem-index',
            worker: 'worker',
            fence: '1',
          }),
          { code: 'MARKING_DOWNGRADE' },
        );
      },
    );
  },
);
