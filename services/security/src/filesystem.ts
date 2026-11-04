import { randomUUID } from 'node:crypto';
import type { PoolClient } from 'pg';
import type { Authority, SecuritySnapshot } from './authority.js';
import { authorize } from '../../../packages/authorization/src/access.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import {
  filesystemOperations,
  type FilesystemMarking,
  type FilesystemOperation,
  type FilesystemPolicy,
} from '../../../packages/contracts/filesystem.js';
import type { SessionContext } from '../../ontology/src/service.js';
import { executionGuard, type ExecutionConstraint } from './execution.js';

export interface FilesystemContext {
  session: SessionContext;
  /** Verified delegation contexts supplied by trusted session composition, never request JSON. */
  delegation?: SessionContext[];
}
export interface FilesystemSnapshot {
  domainAccess?: boolean;
  execution?: ExecutionConstraint;
  writes?: string[];
  instanceEpoch: string;
  members: { id: string; session: SessionContext; state: SecuritySnapshot }[];
}

function invalid(message: string): never {
  throw new MoriaError('FILESYSTEM_INVALID', message);
}
function uuid(value: unknown) {
  if (typeof value !== 'string' || !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(value))
    invalid('Invalid instance principal or entry ID');
}
function text(value: unknown) {
  if (typeof value !== 'string' || !value || value.length > 128 || /[\x00-\x1f\x7f]/.test(value))
    invalid('Invalid security identity');
}
function fields(value: unknown, keys: string[]): asserts value is Record<string, any> {
  if (
    !value ||
    typeof value !== 'object' ||
    Array.isArray(value) ||
    Object.keys(value).length !== keys.length ||
    keys.some((key) => !Object.hasOwn(value, key))
  )
    invalid('Invalid policy fields');
}
function validatePolicy(value: unknown): asserts value is FilesystemPolicy {
  fields(value, ['markings', 'grants']);
  if (
    !Array.isArray(value.markings) ||
    value.markings.length > 64 ||
    !Array.isArray(value.grants) ||
    value.grants.length > 256
  )
    invalid('Policy exceeds bounds');
  for (const marking of value.markings) {
    fields(marking, ['environment', 'marking']);
    text(marking.environment);
    text(marking.marking);
  }
  for (const grant of value.grants) {
    fields(grant, ['subject', 'operations', 'inherit', 'effect']);
    if (grant.subject?.kind === 'principal') {
      fields(grant.subject, ['kind', 'id']);
      uuid(grant.subject.id);
    } else if (grant.subject?.kind === 'group') {
      fields(grant.subject, ['kind', 'id', 'environment']);
      text(grant.subject.id);
      text(grant.subject.environment);
    } else if (grant.subject?.kind === 'administrators') fields(grant.subject, ['kind']);
    else invalid('Invalid grant subject');
    if (
      !Array.isArray(grant.operations) ||
      !grant.operations.length ||
      grant.operations.length > filesystemOperations.length ||
      grant.operations.some(
        (op: unknown) => !filesystemOperations.includes(op as FilesystemOperation),
      ) ||
      typeof grant.inherit !== 'boolean' ||
      !['allow', 'deny'].includes(grant.effect)
    )
      invalid('Invalid grant');
  }
}
function union(markings: FilesystemMarking[]): FilesystemMarking[] {
  return [
    ...new Map(
      markings.map((item) => [JSON.stringify([item.environment, item.marking]), item]),
    ).values(),
  ];
}

/** Security administration is trusted operator composition, matching Authority's
 * sync/grant methods. No administration method is exposed as a user endpoint. */
export class FilesystemAuthority {
  constructor(readonly authority: Authority) {}

  async audit(client: PoolClient, actor: string, operation: string, target: string) {
    const epoch = (
      await client.query(
        'UPDATE moria.instance_state SET security_epoch=security_epoch+1 RETURNING security_epoch',
      )
    ).rows[0].security_epoch;
    await client.query(
      'INSERT INTO moria.instance_audit(actor,operation,target,epoch) VALUES($1,$2,$3,$4)',
      [actor, operation, target, epoch],
    );
    return epoch as string;
  }

  async createPrincipal(actor: string, kind: 'human' | 'workload'): Promise<string> {
    text(actor);
    if (!['human', 'workload'].includes(kind)) invalid('Invalid principal kind');
    return this.authority.catalog.transaction(
      async (client) => {
        const id = randomUUID();
        await client.query('INSERT INTO moria.instance_principals(id,kind) VALUES($1,$2)', [
          id,
          kind,
        ]);
        await this.audit(client, actor, 'createPrincipal', id);
        return id;
      },
      undefined,
      'serializable',
      'exclusive',
      'exclusive',
    );
  }

  async bindPrincipal(
    actor: string,
    environment: string,
    principal: string,
    instanceId: string,
  ): Promise<void> {
    text(actor);
    text(environment);
    text(principal);
    uuid(instanceId);
    await this.authority.catalog.transaction(
      async (client) => {
        const identity = await client.query(
          `SELECT 1 FROM moria.principals p JOIN moria.instance_principals i ON i.id=$3 AND i.kind=p.kind
        WHERE p.environment=$1 AND p.principal=$2`,
          [environment, principal, instanceId],
        );
        if (!identity.rowCount)
          invalid('Principal binding must preserve authenticated identity kind');
        const existing = await client.query(
          'SELECT instance_id FROM moria.instance_principal_bindings WHERE environment=$1 AND principal=$2',
          [environment, principal],
        );
        if (existing.rowCount) {
          if (existing.rows[0].instance_id !== instanceId)
            throw new MoriaError('FILESYSTEM_CONFLICT', 'Principal bindings cannot be reassigned');
          return;
        }
        await client.query('INSERT INTO moria.instance_principal_bindings VALUES($1,$2,$3)', [
          environment,
          principal,
          instanceId,
        ]);
        await this.audit(client, actor, 'bindPrincipal', instanceId);
      },
      environment,
      'serializable',
      'exclusive',
      'exclusive',
    );
  }

  async setEnabled(actor: string, instanceId: string, enabled: boolean): Promise<void> {
    text(actor);
    uuid(instanceId);
    if (typeof enabled !== 'boolean') invalid('Expected enabled state');
    await this.authority.catalog.transaction(
      async (client) => {
        const result = await client.query(
          'UPDATE moria.instance_principals SET enabled=$2 WHERE id=$1',
          [instanceId, enabled],
        );
        if (!result.rowCount) throw new MoriaError('NOT_FOUND', 'Instance principal not found');
        await client.query(
          `UPDATE moria.environments SET security_epoch=security_epoch+1
          WHERE id IN (SELECT environment FROM moria.instance_principal_bindings WHERE instance_id=$1)`,
          [instanceId],
        );
        await this.audit(
          client,
          actor,
          enabled ? 'enablePrincipal' : 'disablePrincipal',
          instanceId,
        );
      },
      undefined,
      'serializable',
      'exclusive',
      'exclusive',
    );
  }

  async effectiveMarkings(client: PoolClient, id: string): Promise<FilesystemMarking[]> {
    const result = await client.query(
      `WITH RECURSIVE base_edges AS (
      SELECT id AS entry_id,parent_id AS source_id FROM moria.fs_entries WHERE parent_id IS NOT NULL
      UNION SELECT entry_id,source_id FROM moria.fs_provenance
      UNION SELECT a.entry_id,b.entry_id FROM moria.fs_resource_dependencies d
        JOIN moria.fs_resource_bindings a ON a.environment=d.environment AND a.project_id=d.project_id AND a.reference=d.reference
        JOIN moria.fs_resource_bindings b ON b.environment=d.environment AND b.project_id=d.source_project_id AND b.reference=d.source_reference
    ), origins AS (
      SELECT $1::uuid AS id UNION SELECT e.source_id FROM base_edges e JOIN origins o ON e.entry_id=o.id
    ), releases AS (
      SELECT b.entry_id,r.digest,r.snapshot,r.markings FROM (
        SELECT entry_id,digest FROM moria.fs_resource_bindings
        UNION SELECT entry_id,digest FROM moria.fs_resource_release_history
      ) b JOIN moria.fs_source_releases r ON r.digest=b.digest
        WHERE b.entry_id IN (SELECT id FROM origins)
      UNION SELECT r.entry_id,d.digest,d.snapshot,d.markings FROM releases r
        CROSS JOIN LATERAL jsonb_array_elements(r.snapshot->'descriptor'->'dependencies') dependency
        JOIN moria.fs_source_releases d ON d.digest=dependency->>'digest'
    ), edges AS (
      SELECT entry_id,source_id FROM base_edges
      UNION SELECT r.entry_id,(r.snapshot->>'rootId')::uuid FROM releases r
      UNION SELECT r.entry_id,(file->'entry'->>'id')::uuid FROM releases r
        CROSS JOIN LATERAL jsonb_array_elements(r.snapshot->'files') file
    ), closure AS (
      SELECT $1::uuid AS id UNION SELECT e.source_id FROM edges e JOIN closure c ON e.entry_id=c.id
    ) SELECT COALESCE(p.markings,'[]') || COALESCE(p.retained_markings,'[]') AS markings
      FROM closure c LEFT JOIN moria.fs_policies p ON p.entry_id=c.id
      UNION ALL SELECT markings FROM releases WHERE entry_id IN (SELECT id FROM closure)`,
      [id],
    );
    return union(result.rows.flatMap((row) => row.markings));
  }

  async setPolicy(
    actor: string,
    id: string,
    expectedRevision: string,
    input: FilesystemPolicy,
    transaction?: PoolClient,
  ): Promise<string> {
    text(actor);
    uuid(id);
    if (
      typeof expectedRevision !== 'string' ||
      !/^(0|[1-9][0-9]{0,18})$/.test(expectedRevision) ||
      BigInt(expectedRevision) >= 9223372036854775807n
    )
      invalid('Invalid policy revision');
    const policy = structuredClone(input);
    validatePolicy(policy);
    const apply = async (client: PoolClient) => {
      const entry = await client.query(
        'SELECT 1 FROM moria.fs_entries WHERE id=$1 AND NOT deleted',
        [id],
      );
      if (!entry.rowCount) throw new MoriaError('NOT_FOUND', 'Entry not found');
      const old = await client.query(
        'SELECT revision,markings FROM moria.fs_policies WHERE entry_id=$1',
        [id],
      );
      if ((old.rows[0]?.revision ?? '0') !== expectedRevision)
        throw new MoriaError('FILESYSTEM_CONFLICT', 'Policy revision changed');
      const newLabels = new Set(
        policy.markings.map((item) => JSON.stringify([item.environment, item.marking])),
      );
      if (
        (old.rows[0]?.markings ?? []).some(
          (item: FilesystemMarking) =>
            !newLabels.has(JSON.stringify([item.environment, item.marking])),
        )
      )
        throw new MoriaError(
          'UNSUPPORTED_CAPABILITY',
          'Marking removal requires an explicit declassification protocol',
        );
      const environments = [
        ...new Set([
          ...policy.markings.map((item) => item.environment),
          ...policy.grants.flatMap((grant) =>
            grant.subject.kind === 'group' ? [grant.subject.environment] : [],
          ),
        ]),
      ];
      if (
        (
          await client.query('SELECT id FROM moria.environments WHERE id=ANY($1::text[])', [
            environments,
          ])
        ).rowCount !== environments.length
      )
        invalid('Unknown marking or group environment');
      const principals = [
        ...new Set(
          policy.grants.flatMap((grant) =>
            grant.subject.kind === 'principal' ? [grant.subject.id] : [],
          ),
        ),
      ];
      if (
        (
          await client.query('SELECT id FROM moria.instance_principals WHERE id=ANY($1::uuid[])', [
            principals,
          ])
        ).rowCount !== principals.length
      )
        invalid('Unknown instance principal');
      const result = await client.query(
        `INSERT INTO moria.fs_policies(entry_id,revision,markings,grants) VALUES($1,1,$2,$3)
        ON CONFLICT(entry_id) DO UPDATE SET revision=fs_policies.revision+1,markings=EXCLUDED.markings,grants=EXCLUDED.grants RETURNING revision`,
        [id, JSON.stringify(union(policy.markings)), JSON.stringify(policy.grants)],
      );
      await this.audit(client, actor, 'setFilesystemPolicy', id);
      return result.rows[0].revision;
    };
    return transaction
      ? apply(transaction)
      : this.authority.catalog.transaction(
          apply,
          undefined,
          'serializable',
          'exclusive',
          'exclusive',
        );
  }

  async current(client: PoolClient, context: FilesystemContext): Promise<FilesystemSnapshot> {
    await executionGuard(client, context.session);
    const sessions = [context.session, ...(context.delegation ?? [])];
    if (!context.session || sessions.length > 8) invalid('Invalid delegation chain');
    const members: FilesystemSnapshot['members'] = [];
    for (const session of sessions) {
      if (!session?.sessionHash)
        throw new MoriaError('UNAUTHENTICATED', 'A verified session is required');
      if (
        session.environment !== context.session.environment ||
        session.application !== context.session.application
      )
        throw new MoriaError('FORBIDDEN', 'Delegation scope differs');
      const state = await this.authority.current(
        session.environment,
        session.principal,
        session.application,
        client,
      );
      await this.authority.assertCurrent(
        client,
        session.environment,
        session.principal,
        session.application,
        state.epoch,
        session.sessionHash,
      );
      const instance = await client.query(
        `SELECT i.id FROM moria.instance_principal_bindings b
        JOIN moria.instance_principals i ON i.id=b.instance_id AND i.enabled
        JOIN moria.principals p ON p.environment=b.environment AND p.principal=b.principal AND p.kind=i.kind
        WHERE b.environment=$1 AND b.principal=$2`,
        [session.environment, session.principal],
      );
      if (!instance.rowCount) throw new MoriaError('FORBIDDEN', 'Instance identity is unavailable');
      if (members.some((member) => member.id === instance.rows[0].id))
        invalid('Delegation contains duplicate identities');
      members.push({ id: instance.rows[0].id, session, state });
    }
    return {
      execution: context.session.execution,
      writes: [],
      instanceEpoch: (await client.query('SELECT security_epoch FROM moria.instance_state')).rows[0]
        .security_epoch,
      members,
    };
  }

  async finish(client: PoolClient, snapshot: FilesystemSnapshot): Promise<void> {
    if (snapshot.execution)
      await executionGuard(client, {
        ...snapshot.members[0].session,
        execution: snapshot.execution,
      });
    if (snapshot.execution)
      for (const id of snapshot.writes ?? [])
        await executionGuard(
          client,
          snapshot.members[0].session,
          snapshot.execution.proposal ? 'proposal' : 'write',
          await this.effectiveMarkings(client, id),
          false,
        );
    for (const member of snapshot.members)
      await this.authority.assertCurrent(
        client,
        member.session.environment,
        member.session.principal,
        member.session.application,
        member.state.epoch,
        member.session.sessionHash,
      );
    if (snapshot.execution && snapshot.writes?.length && !snapshot.domainAccess) {
      const row = (
        await client.query('SELECT reads FROM moria.function_invocations WHERE id=$1', [
          snapshot.execution.id,
        ])
      ).rows[0];
      for (const read of row.reads) {
        for (const file of read.files ?? []) {
          if (!snapshot.writes.includes(file.id)) continue;
          const current = (
            await client.query('SELECT revision::text FROM moria.fs_entries WHERE id=$1', [file.id])
          ).rows[0];
          if (current) file.revision = current.revision;
        }
      }
      await client.query('UPDATE moria.function_invocations SET reads=$2 WHERE id=$1', [
        snapshot.execution.id,
        JSON.stringify(row.reads),
      ]);
    }
  }

  async ancestry(
    client: PoolClient,
    id: string,
  ): Promise<
    {
      id: string;
      environment: string | null;
      markings: FilesystemMarking[];
      grants: FilesystemPolicy['grants'];
    }[]
  > {
    uuid(id);
    return (
      await client.query(
        `WITH RECURSIVE ancestry AS (
      SELECT id,parent_id,environment_id FROM moria.fs_entries WHERE id=$1
      UNION ALL SELECT e.id,e.parent_id,e.environment_id FROM moria.fs_entries e JOIN ancestry a ON e.id=a.parent_id
    ) SELECT a.id,a.environment_id AS environment,COALESCE(p.markings,'[]') || COALESCE(p.retained_markings,'[]') AS markings,
      COALESCE(p.grants,'[]') AS grants FROM ancestry a LEFT JOIN moria.fs_policies p ON p.entry_id=a.id`,
        [id],
      )
    ).rows;
  }

  async check(
    client: PoolClient,
    snapshot: FilesystemSnapshot,
    id: string,
    operation: FilesystemOperation,
    includeInactive = false,
  ): Promise<void> {
    const ancestry = await this.ancestry(client, id);
    if (!ancestry.length) throw new MoriaError('NOT_FOUND', 'Entry not found');
    if (
      (
        await client.query(
          `SELECT 1 FROM moria.fs_resource_bindings b JOIN moria.fs_roles r ON r.entry_id=b.root_id WHERE b.entry_id=$1 AND ((NOT b.active AND NOT $2) OR r.retired)`,
          [id, includeInactive],
        )
      ).rowCount
    )
      throw new MoriaError('NOT_FOUND', 'Resource is inactive');
    const markings = await this.effectiveMarkings(client, id);
    if (snapshot.execution && !snapshot.domainAccess && !['read', 'list'].includes(operation))
      await executionGuard(
        client,
        snapshot.members[0].session,
        snapshot.execution.proposal ? 'proposal' : 'write',
        markings,
      );
    if (snapshot.execution && !snapshot.domainAccess && !['read', 'list'].includes(operation))
      snapshot.writes?.push(id);
    for (const member of snapshot.members) {
      if (
        ancestry.some(
          (node) => node.environment !== null && node.environment !== member.session.environment,
        ) ||
        markings.some((item) => item.environment !== member.session.environment)
      )
        throw new MoriaError('FORBIDDEN', 'Operation is not authorized');
      authorize(
        member.state.identity,
        'filesystem',
        operation,
        markings.map((item) => item.marking),
      );
      const grants = ancestry.flatMap((node) =>
        node.grants.filter(
          (grant) => (node.id === id || grant.inherit) && grant.operations.includes(operation),
        ),
      );
      const matches = grants.filter((grant) =>
        grant.subject.kind === 'administrators'
          ? member.state.identity.grantedOperations.has('instance:manage') &&
            member.state.identity.applicationScopes.has('instance:manage')
          : grant.subject.kind === 'principal'
            ? grant.subject.id === member.id
            : grant.subject.environment === member.session.environment &&
              Array.isArray(member.state.attributes.groups) &&
              member.state.attributes.groups.includes(grant.subject.id),
      );
      if (
        !matches.some((grant) => grant.effect === 'allow') ||
        matches.some((grant) => grant.effect === 'deny')
      )
        throw new MoriaError('FORBIDDEN', 'Operation is not authorized');
      try {
        await client.query('SELECT moria.native_compiler_artifact_access($1,$2)', [
          member.session.sessionHash ?? null,
          id,
        ]);
      } catch (error) {
        const code = (error as { message?: string }).message;
        if (
          code &&
          [
            'FORBIDDEN',
            'NOT_FOUND',
            'UNAUTHENTICATED',
            'IDENTITY_STALE',
            'RESOURCE_LIMIT',
          ].includes(code)
        )
          throw new MoriaError(code, 'Compiler artifact source access is unavailable');
        throw error;
      }
    }
    if (snapshot.execution)
      await client.query(
        "UPDATE moria.function_invocations SET markings=COALESCE((SELECT jsonb_agg(DISTINCT value) FROM jsonb_array_elements(markings || $2::jsonb)),'[]') WHERE id=$1",
        [snapshot.execution.id, JSON.stringify(markings)],
      );
  }

  async retain(client: PoolClient, actor: string, id: string): Promise<void> {
    const markings = await this.effectiveMarkings(client, id);
    if (!markings.length) return;
    await client.query(
      `INSERT INTO moria.fs_policies(entry_id,retained_markings) VALUES($1,$2)
      ON CONFLICT(entry_id) DO UPDATE SET retained_markings=EXCLUDED.retained_markings`,
      [id, JSON.stringify(markings)],
    );
    await this.audit(client, actor, 'retainMoveMarkings', id);
  }
}
