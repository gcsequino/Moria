import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { database } from './fixtures/catalog-storage/database.js';
import { Authority } from '../services/security/src/authority.js';
import {
  FilesystemAuthority,
  type FilesystemContext,
} from '../services/security/src/filesystem.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { FilesystemStore } from '../services/filesystem/src/store.js';
import { FilesystemService } from '../services/filesystem/src/service.js';
import { FilesystemManagement } from '../services/filesystem/src/management.js';
import { filesystemOperations } from '../packages/contracts/filesystem.js';

test(
  'managed homes and group projects preserve current identity and private agent configuration',
  { timeout: 120000 },
  async (t) => {
    const db = await database();
    t.after(db.close);
    const authority = new Authority(db.catalog),
      security = new FilesystemAuthority(authority),
      store = new FilesystemStore(db.catalog);
    const service = new FilesystemService(store, security),
      management = new FilesystemManagement(service),
      sessions = new WorkloadSessions(authority);
    const environment = 'homes',
      application = 'homes-app',
      operations = filesystemOperations.map((op) => `filesystem:${op}`);
    await db.catalog.ensureEnvironment(environment);
    await authority.setApplication(environment, 'operator', application, [
      ...operations,
      'instance:manage',
    ]);
    async function context(
      principal: string,
      attributes: Record<string, any>,
      admin = false,
    ): Promise<FilesystemContext> {
      await authority.syncPrincipal(environment, 'operator', principal, {
        kind: 'workload',
        enabled: true,
        markings: [],
        attributes,
      });
      await authority.setGrants(
        environment,
        'operator',
        principal,
        application,
        admin ? [...operations, 'instance:manage'] : operations,
      );
      const token = await sessions.mint({ environment, application, principal }, 'operator');
      return { session: await sessions.authenticate(`Bearer ${token.token}`) };
    }
    const owner = await context('owner', {}, true),
      reader = await context('reader', { groups: ['analysts'] }),
      outsider = await context('outsider', {});
    const ownerHome = await management.ensureHome(owner),
      readerHome = await management.ensureHome(reader),
      outsiderHome = await management.ensureHome(outsider);
    const agent = await context('agent', { agent: true, managerId: ownerHome.principalId });
    const agentHome = await management.ensureHome(agent);

    await t.test(
      'home provisioning is idempotent and discovery reveals only accessible homes',
      async () => {
        const epoch = (await db.pool.query('SELECT security_epoch FROM moria.instance_state'))
          .rows[0].security_epoch;
        assert.deepEqual(await management.ensureHome(reader), readerHome);
        assert.equal(
          (await db.pool.query('SELECT security_epoch FROM moria.instance_state')).rows[0]
            .security_epoch,
          epoch,
        );
        const users = await service.resolve(reader, '/users');
        assert.deepEqual(
          (await service.list(reader, users.id)).entries.map((entry) => entry.id),
          [readerHome.home.id],
        );
        await assert.rejects(service.get(reader, outsiderHome.home.id), { code: 'NOT_FOUND' });
        assert.equal((await service.get(owner, outsiderHome.home.id)).id, outsiderHome.home.id);
        await authority.setGrants(environment, 'operator', 'owner', application, operations);
        await assert.rejects(service.get(owner, outsiderHome.home.id), { code: 'NOT_FOUND' });
        await authority.setGrants(environment, 'operator', 'owner', application, [
          ...operations,
          'instance:manage',
        ]);
      },
    );

    await t.test(
      'agents can write memory while configuration and registration remain manager-controlled',
      async () => {
        const role = await management.role(agent, agentHome.home.id);
        assert.equal(role.kind, 'agent');
        assert.equal(role.configuration.managerId, ownerHome.principalId);
        const configuration = await store.get(role.configuration.configuration),
          memory = await store.get(role.configuration.memory);
        await assert.rejects(
          service.apply(agent, 'change-instructions', [
            {
              op: 'create',
              parentId: configuration.id,
              expectedParentRevision: configuration.revision,
              name: 'instructions',
              kind: 'directory',
            },
          ]),
          { code: 'FORBIDDEN' },
        );
        await service.apply(agent, 'remember', [
          {
            op: 'create',
            parentId: memory.id,
            expectedParentRevision: memory.revision,
            name: 'notes',
            kind: 'directory',
          },
        ]);
        await service.apply(owner, 'configure-agent', [
          {
            op: 'create',
            parentId: configuration.id,
            expectedParentRevision: configuration.revision,
            name: 'instructions',
            kind: 'directory',
          },
        ]);
        const currentMemory = await store.get(memory.id);
        await assert.rejects(
          management.createProject(agent, memory.id, 'self-upgrade', currentMemory.revision),
          { code: 'FORBIDDEN' },
        );
        await assert.rejects(
          service.get({ session: agent.session, delegation: [reader.session] }, memory.id),
          { code: 'NOT_FOUND' },
        );
      },
    );

    await t.test(
      'managed configuration cannot acquire agent memory permissions through a move',
      async () => {
        const role = await management.role(owner, agentHome.home.id),
          configuration = await store.get(role.configuration.configuration),
          memory = await store.get(role.configuration.memory);
        await assert.rejects(
          service.apply(owner, randomUUID(), [
            {
              op: 'move',
              id: configuration.id,
              expectedRevision: configuration.revision,
              parentId: memory.id,
              expectedParentRevision: memory.revision,
              expectedSourceParentRevision: (await store.get(agentHome.home.id)).revision,
              name: 'configuration',
            },
          ]),
          { code: 'FILESYSTEM_INVALID' },
        );
        assert.equal((await store.get(configuration.id)).parentId, agentHome.home.id);
        const home = await store.get(agentHome.home.id);
        await service.apply(owner, randomUUID(), [
          {
            op: 'move',
            id: configuration.id,
            expectedRevision: configuration.revision,
            parentId: home.id,
            expectedParentRevision: home.revision,
            expectedSourceParentRevision: home.revision,
            name: 'settings',
          },
        ]);
        await assert.rejects(
          service.apply(agent, randomUUID(), [
            {
              op: 'create',
              kind: 'directory',
              parentId: configuration.id,
              expectedParentRevision: (await store.get(configuration.id)).revision,
              name: 'unauthorized',
            },
          ]),
          { code: 'FORBIDDEN' },
        );
      },
    );

    await t.test(
      'group membership gates shared projects and ordinary users cannot create groups',
      async () => {
        const groups = await service.resolve(owner, '/groups');
        const group = await management.createGroup(owner, 'analysis', 'analysts', groups.revision);
        const projects = await service.resolve(reader, '/groups/analysis/projects');
        const project = await management.createProject(
          reader,
          projects.id,
          'observatory',
          projects.revision,
        );
        const source = await service.resolve(
          reader,
          '/groups/analysis/projects/observatory/source',
        );
        assert.equal(source.projectId, project.projectId);
        assert.equal((await management.role(reader, project.entry.id)).kind, 'project');
        await assert.rejects(service.get(outsider, group.entry.id), { code: 'NOT_FOUND' });
        await assert.rejects(
          management.createGroup(
            reader,
            'unauthorized',
            'another',
            (await store.get(groups.id)).revision,
          ),
          { code: 'FORBIDDEN' },
        );
        const policy = await management.policy(owner, group.entry.id);
        await assert.rejects(
          management.setPolicy(reader, group.entry.id, policy.revision, {
            markings: [],
            grants: [],
          }),
          { code: 'FORBIDDEN' },
        );
        await authority.syncPrincipal(environment, 'operator', 'reader', {
          kind: 'workload',
          enabled: true,
          markings: [],
          attributes: { groups: [] },
        });
        await assert.rejects(service.get(reader, project.entry.id), { code: 'NOT_FOUND' });
      },
    );

    await t.test(
      'home identity survives renames and instance disable blocks every home operation',
      async () => {
        const current = await store.get(outsiderHome.home.id),
          users = await store.get(current.parentId!);
        await service.apply(owner, randomUUID(), [
          {
            op: 'move',
            id: current.id,
            expectedRevision: current.revision,
            parentId: users.id,
            expectedParentRevision: users.revision,
            expectedSourceParentRevision: users.revision,
            name: 'renamed-home',
          },
        ]);
        assert.equal((await management.ensureHome(outsider)).home.id, current.id);
        await assert.rejects(management.setEnabled(reader, outsiderHome.principalId, false), {
          code: 'FORBIDDEN',
        });
        await management.setEnabled(owner, outsiderHome.principalId, false);
        await assert.rejects(management.ensureHome(outsider), { code: 'FORBIDDEN' });
        await assert.rejects(service.get(outsider, current.id), { code: 'FORBIDDEN' });
      },
    );
  },
);
