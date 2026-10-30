import { randomUUID } from 'node:crypto';
import type { PoolClient } from 'pg';
import { FilesystemService } from './service.js';
import type { FilesystemContext, FilesystemSnapshot } from '../../security/src/filesystem.js';
import { authorize } from '../../../packages/authorization/src/access.js';
import { fingerprint } from '../../../packages/compiler/src/index.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import {
  filesystemRoot,
  filesystemOperations,
  type FilesystemActor,
  type FilesystemEntry,
  type FilesystemGrant,
  type FilesystemPolicy,
} from '../../../packages/contracts/filesystem.js';

export class FilesystemManagement {
  constructor(readonly filesystem: FilesystemService) {}

  setEnabled(input: FilesystemContext, id: string, enabled: boolean) {
    if (
      typeof id !== 'string' ||
      !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(id) ||
      typeof enabled !== 'boolean'
    )
      throw new MoriaError('FILESYSTEM_INVALID', 'Invalid principal state');
    const context = structuredClone(input);
    return this.filesystem.store.catalog.transaction(
      async (client) => {
        const security = this.filesystem.security,
          state = await security.current(client, context);
        for (const member of state.members) authorize(member.state.identity, 'instance', 'manage');
        await security.finish(client, state);
        if (
          !(
            await client.query('UPDATE moria.instance_principals SET enabled=$2 WHERE id=$1', [
              id,
              enabled,
            ])
          ).rowCount
        )
          throw new MoriaError('NOT_FOUND', 'Instance principal not found');
        await client.query(
          `UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id IN (SELECT environment FROM moria.instance_principal_bindings WHERE instance_id=$1)`,
          [id],
        );
        await security.audit(
          client,
          state.members[0].id,
          enabled ? 'enablePrincipal' : 'disablePrincipal',
          id,
        );
        return { id, enabled };
      },
      context.session.environment,
      'serializable',
      'exclusive',
      'exclusive',
    );
  }

  private async directory(
    client: PoolClient,
    actor: FilesystemActor,
    parentId: string,
    name: string,
    projectId?: string,
  ) {
    const found = await client.query(
      'SELECT id FROM moria.fs_entries WHERE parent_id=$1 AND name=$2 AND NOT deleted',
      [parentId, name],
    );
    if (found.rowCount) {
      const existing = await this.filesystem.store.get(found.rows[0].id, client);
      if (existing.kind !== 'directory' || (projectId && existing.projectId !== projectId))
        throw new MoriaError('FILESYSTEM_CONFLICT', 'Managed directory binding differs');
      return existing;
    }
    const parent = await this.filesystem.store.get(parentId, client);
    const result = await this.filesystem.store.apply(
      actor,
      randomUUID(),
      [
        {
          op: 'create',
          parentId,
          expectedParentRevision: parent.revision,
          name,
          kind: 'directory',
          ...(projectId ? { scope: { projectId } } : {}),
        },
      ],
      {
        client,
        environment: actor.environment,
        current: async () => {},
        change: async () => {},
        receipt: async () => {},
      },
    );
    return result.entries.find((entry) => entry.parentId === parentId && entry.name === name)!;
  }

  private async grant(client: PoolClient, actor: string, id: string, grant: FilesystemGrant) {
    const policy = (
      await client.query(
        'SELECT revision,markings,grants FROM moria.fs_policies WHERE entry_id=$1',
        [id],
      )
    ).rows[0];
    if (
      (policy?.grants ?? []).some(
        (existing: FilesystemGrant) =>
          fingerprint(existing.subject) === fingerprint(grant.subject) &&
          existing.effect === grant.effect &&
          existing.inherit === grant.inherit &&
          grant.operations.every((op) => existing.operations.includes(op)),
      )
    )
      return;
    await this.filesystem.security.setPolicy(
      actor,
      id,
      policy?.revision ?? '0',
      { markings: policy?.markings ?? [], grants: [...(policy?.grants ?? []), grant] },
      client,
    );
  }

  /** Provision only the verified primary identity; authentication supplies all identity fields. */
  ensureHome(input: FilesystemContext) {
    const context = structuredClone(input),
      session = context.session;
    if (context.delegation?.length)
      throw new MoriaError('FORBIDDEN', 'Provision the primary identity outside delegation');
    return this.filesystem.store.catalog.transaction(
      async (client) => {
        const base = await this.filesystem.security.authority.current(
          session.environment,
          session.principal,
          session.application,
          client,
        );
        if (!session.sessionHash)
          throw new MoriaError('UNAUTHENTICATED', 'A verified session is required');
        await this.filesystem.security.authority.assertCurrent(
          client,
          session.environment,
          session.principal,
          session.application,
          base.epoch,
          session.sessionHash,
        );
        authorize(base.identity, 'filesystem', 'read');
        const principal = (
          await client.query(
            `SELECT p.kind,b.instance_id,i.enabled,i.home_entry_id FROM moria.principals p
        LEFT JOIN moria.instance_principal_bindings b USING(environment,principal)
        LEFT JOIN moria.instance_principals i ON i.id=b.instance_id WHERE p.environment=$1 AND p.principal=$2`,
            [session.environment, session.principal],
          )
        ).rows[0];
        if (!principal || (principal.instance_id && !principal.enabled))
          throw new MoriaError('FORBIDDEN', 'Instance identity is unavailable');
        const id: string = principal.instance_id ?? randomUUID();
        if (!principal.instance_id) {
          await client.query('INSERT INTO moria.instance_principals(id,kind) VALUES($1,$2)', [
            id,
            principal.kind,
          ]);
          await client.query('INSERT INTO moria.instance_principal_bindings VALUES($1,$2,$3)', [
            session.environment,
            session.principal,
            id,
          ]);
          await this.filesystem.security.audit(client, session.principal, 'provisionPrincipal', id);
        }
        if (principal.home_entry_id) {
          await this.filesystem.security.authority.assertCurrent(
            client,
            session.environment,
            session.principal,
            session.application,
            base.epoch,
            session.sessionHash,
          );
          return {
            principalId: id,
            home: await this.filesystem.store.get(principal.home_entry_id, client),
          };
        }
        const actor: FilesystemActor = {
          environment: session.environment,
          principal: session.principal,
          application: session.application,
        };
        const roots: Record<string, FilesystemEntry> = {};
        for (const name of ['system', 'groups', 'users']) {
          roots[name] = await this.directory(client, actor, filesystemRoot, name);
          await client.query(
            `INSERT INTO moria.fs_roles(entry_id,kind,configuration) VALUES($1,'system',$2) ON CONFLICT(entry_id) DO NOTHING`,
            [roots[name].id, JSON.stringify({ name })],
          );
        }
        const allowance = (
          operations: FilesystemGrant['operations'],
          inherit: boolean,
        ): FilesystemGrant => ({
          subject: { kind: 'principal', id },
          operations,
          inherit,
          effect: 'allow',
        });
        await this.grant(client, id, filesystemRoot, {
          subject: { kind: 'administrators' },
          operations: [...filesystemOperations],
          inherit: true,
          effect: 'allow',
        });
        for (const target of [filesystemRoot, roots.users.id, roots.groups.id])
          await this.grant(client, id, target, allowance(['read', 'list'], false));
        const home = await this.directory(client, actor, roots.users.id, id),
          agent = base.attributes.agent === true;
        let managerId: string | undefined;
        if (agent) {
          const requested = base.attributes.managerId;
          if (
            typeof requested !== 'string' ||
            !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(requested) ||
            !(
              await client.query('SELECT 1 FROM moria.instance_principals WHERE id=$1', [requested])
            ).rowCount
          )
            throw new MoriaError(
              'FILESYSTEM_INVALID',
              'Agents require an explicit registered manager',
            );
          managerId = requested;
        }
        await this.grant(
          client,
          id,
          home.id,
          allowance(agent ? ['read', 'list'] : [...filesystemOperations], true),
        );
        if (managerId)
          await this.grant(client, id, home.id, {
            subject: { kind: 'principal', id: managerId },
            operations: [...filesystemOperations],
            inherit: true,
            effect: 'allow',
          });
        const configuration = await this.directory(client, actor, home.id, 'configuration'),
          memory = await this.directory(client, actor, home.id, 'memory'),
          runs = await this.directory(client, actor, home.id, 'runs');
        if (agent)
          await this.grant(
            client,
            id,
            memory.id,
            allowance(['read', 'list', 'create', 'write', 'move', 'delete', 'export'], true),
          );
        const projects = agent
          ? undefined
          : await this.directory(client, actor, home.id, 'projects');
        await client.query(
          `INSERT INTO moria.fs_roles(entry_id,kind,configuration) VALUES($1,$2,$3)`,
          [
            home.id,
            agent ? 'agent' : 'user',
            JSON.stringify({
              principalId: id,
              managerId: managerId ?? null,
              configuration: configuration.id,
              memory: memory.id,
              runs: runs.id,
              projects: projects?.id ?? null,
            }),
          ],
        );
        await client.query('UPDATE moria.instance_principals SET home_entry_id=$2 WHERE id=$1', [
          id,
          home.id,
        ]);
        await this.filesystem.security.authority.assertCurrent(
          client,
          session.environment,
          session.principal,
          session.application,
          base.epoch,
          session.sessionHash,
        );
        return { principalId: id, home: await this.filesystem.store.get(home.id, client) };
      },
      session.environment,
      'serializable',
      'exclusive',
      'exclusive',
    );
  }

  private write<T>(
    input: FilesystemContext,
    action: (client: PoolClient, state: FilesystemSnapshot) => Promise<T>,
  ) {
    const context = structuredClone(input);
    return this.filesystem.store.catalog.transaction(
      async (client) => {
        const state = await this.filesystem.security.current(client, context),
          result = await action(client, state);
        await this.filesystem.security.finish(client, state);
        return result;
      },
      context.session.environment,
      'serializable',
      'exclusive',
      'exclusive',
    );
  }

  createProject(
    context: FilesystemContext,
    parentId: string,
    name: string,
    expectedRevision: string,
  ) {
    return this.write(context, async (client, state) => {
      await this.filesystem.security.check(client, state, parentId, 'register');
      const parent = await this.filesystem.store.get(parentId, client);
      if (parent.revision !== expectedRevision || parent.projectId)
        throw new MoriaError(
          'FILESYSTEM_CONFLICT',
          'Project parent changed or is already project-scoped',
        );
      const projectId = randomUUID(),
        session = state.members[0].session,
        actor = {
          environment: session.environment,
          principal: session.principal,
          application: session.application,
        };
      const project = await this.directory(client, actor, parentId, name, projectId);
      for (const name of ['source', 'environments']) {
        const child = await this.directory(client, actor, project.id, name);
        await client.query(
          `INSERT INTO moria.fs_roles(entry_id,kind,configuration) VALUES($1,'system',$2)`,
          [child.id, JSON.stringify({ managed: 'project', name })],
        );
      }
      await client.query(
        `INSERT INTO moria.fs_roles(entry_id,kind,project_id,configuration) VALUES($1,'project',$2,$3)`,
        [project.id, projectId, JSON.stringify({ name })],
      );
      await this.filesystem.security.audit(
        client,
        state.members[0].id,
        'createProject',
        project.id,
      );
      return { projectId, entry: await this.filesystem.store.get(project.id, client) };
    });
  }

  createGroup(
    context: FilesystemContext,
    name: string,
    groupKey: string,
    expectedRevision: string,
  ) {
    if (typeof groupKey !== 'string' || !groupKey || groupKey.length > 128)
      throw new MoriaError('FILESYSTEM_INVALID', 'Invalid authoritative group key');
    return this.write(context, async (client, state) => {
      const session = state.members[0].session;
      for (const member of state.members) authorize(member.state.identity, 'instance', 'manage');
      const groups = await this.filesystem.store.resolve('/groups', client);
      if (groups.revision !== expectedRevision)
        throw new MoriaError('FILESYSTEM_CONFLICT', 'Group parent changed');
      const entry = await this.directory(
          client,
          {
            environment: session.environment,
            principal: session.principal,
            application: session.application,
          },
          groups.id,
          name,
        ),
        id = randomUUID();
      await client.query('INSERT INTO moria.fs_groups VALUES($1,$2,$3,$4)', [
        id,
        session.environment,
        groupKey,
        entry.id,
      ]);
      await client.query(
        `INSERT INTO moria.fs_roles(entry_id,kind,configuration) VALUES($1,'group',$2)`,
        [entry.id, JSON.stringify({ id, environment: session.environment, groupKey })],
      );
      await this.grant(client, state.members[0].id, entry.id, {
        subject: { kind: 'group', id: groupKey, environment: session.environment },
        operations: filesystemOperations.filter((op) => op !== 'administer'),
        inherit: true,
        effect: 'allow',
      });
      await this.directory(
        client,
        {
          environment: session.environment,
          principal: session.principal,
          application: session.application,
        },
        entry.id,
        'projects',
      );
      return { id, entry: await this.filesystem.store.get(entry.id, client) };
    });
  }

  policy(context: FilesystemContext, id: string) {
    return this.filesystem.protectedRead(context, async (client, state) => {
      await this.filesystem.visible(client, state, id, 'administer');
      return (
        (
          await client.query(
            'SELECT revision,markings,grants,retained_markings AS "retainedMarkings" FROM moria.fs_policies WHERE entry_id=$1',
            [id],
          )
        ).rows[0] ?? { revision: '0', markings: [], grants: [], retainedMarkings: [] }
      );
    });
  }

  setPolicy(
    context: FilesystemContext,
    id: string,
    expectedRevision: string,
    policy: FilesystemPolicy,
  ) {
    return this.write(context, async (client, state) => {
      await this.filesystem.security.check(client, state, id, 'administer');
      return {
        revision: await this.filesystem.security.setPolicy(
          state.members[0].id,
          id,
          expectedRevision,
          policy,
          client,
        ),
      };
    });
  }

  role(context: FilesystemContext, id: string) {
    return this.filesystem.protectedRead(context, async (client, state) => {
      await this.filesystem.visible(client, state, id, 'read');
      return (
        (
          await client.query(
            'SELECT kind,project_id::text AS "projectId",deployment_unit_id::text AS "deploymentUnitId",namespace,configuration,revision::text,latest_release AS "latestRelease",retired FROM moria.fs_roles WHERE entry_id=$1',
            [id],
          )
        ).rows[0] ?? null
      );
    });
  }
}
