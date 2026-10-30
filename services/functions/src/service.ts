import { randomUUID } from 'node:crypto';
import type { PoolClient } from 'pg';
import { authorize } from '../../../packages/authorization/src/access.js';
import { fingerprint, valueFingerprint } from '../../../packages/compiler/src/index.js';
import {
  MoriaError,
  type ValueType,
  type Value,
  type TypeSchema,
  VALUE_VERSION,
} from '../../../packages/contracts/types.js';
import {
  validateRow,
  canonicalJson,
  normalizeType,
  bindOntologyObjects,
} from '../../../packages/contracts/values.js';
import type { FunctionInvocation, FunctionEvent } from '../../../packages/contracts/functions.js';
import type { FilesystemMarking } from '../../../packages/contracts/filesystem.js';
import type { FilesystemContext } from '../../security/src/filesystem.js';
import type { FilesystemRuntime } from '../../filesystem/src/local.js';
import type {
  ObjectService,
  SessionContext,
  PreparedFunctionEdits,
} from '../../ontology/src/service.js';
import { functionLimits, type FunctionWorker } from './worker.js';
import type { FunctionBindings, FunctionRequest, FunctionResponse } from './platform.js';
import { executionGuard } from '../../security/src/execution.js';
import { resolveObject } from '../../ontology/src/service.js';
import { captureAccess } from '../../../packages/authorization/src/flow.js';

const uuid = (value: unknown): value is string =>
  typeof value === 'string' && /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(value);
const terminal = (status: string) => ['succeeded', 'failed', 'cancelled'].includes(status);
const actor = (context: FilesystemContext) =>
  fingerprint([
    context.session.environment,
    context.session.application,
    context.session.principal,
  ]);
const mergeMarkings = (...groups: FilesystemMarking[][]) => [
  ...new Map(
    groups.flat().map((label) => [`${label.environment}\0${label.marking}`, label]),
  ).values(),
];
interface ReadObservation {
  objectType?: string;
  action?: string;
  invocation?: { id: string; after?: string; metadata?: boolean };
  filesystem?: FunctionRequest;
  files?: { id: string; revision: string }[];
  instanceEpoch?: string;
  path: string;
  method: string;
  input: unknown;
  hash: string;
  revision: string;
  epoch: string;
  resource: string;
  markings: string[];
}

/** One attempt per invocation. Recovery fences lost workers and never re-executes user code. */
export class Functions {
  private active = new Map<string, { controller: AbortController; task: Promise<void> }>();
  private stopped = false;
  private workerTimer: ReturnType<typeof setInterval> | undefined;
  private polling: Promise<void> | undefined;
  private maintenance = new Map<string, Promise<void>>();
  constructor(
    readonly runtime: FilesystemRuntime,
    readonly objects: ReadonlyMap<string, ObjectService> = new Map(),
    private worker?: FunctionWorker,
    readonly bindings: FunctionBindings = {},
  ) {}
  private get fs() {
    return this.runtime.filesystem;
  }
  /** Explicit trusted worker role; API processes never start a sandbox backend. */
  startWorker() {
    if (this.workerTimer || this.stopped) return;
    if (!this.worker)
      throw new MoriaError('FUNCTION_WORKER_UNAVAILABLE', 'Function worker backend required');
    const poll = () => {
      if (!this.polling && !this.stopped)
        this.polling = this.tick()
          .catch(() => {})
          .finally(() => {
            this.polling = undefined;
          });
    };
    this.workerTimer = setInterval(poll, 100);
    poll();
  }
  private async tick() {
    await this.fs.store.catalog.transaction(async (client) => {
      const expired = (
        await client.query(
          "SELECT id,deadline<=clock_timestamp() AS timed_out FROM moria.function_invocations WHERE status IN ('accepted','running') AND (lease_until<=clock_timestamp() OR deadline<=clock_timestamp()) LIMIT 4 FOR UPDATE SKIP LOCKED",
        )
      ).rows;
      for (const row of expired) {
        await client.query(
          "UPDATE moria.function_invocations SET status='failed',error=$2,finished_at=clock_timestamp() WHERE id=$1",
          [
            row.id,
            JSON.stringify({
              code: row.timed_out ? 'FUNCTION_TIMEOUT' : 'FUNCTION_WORKER_LOST',
              message: 'Function worker lease or deadline expired',
            }),
          ],
        );
        await this.event(client, row.id, 'state', 'failed');
      }
      const children = (
        await client.query(
          "WITH RECURSIVE stopped AS (SELECT c.id FROM moria.function_invocations c JOIN moria.function_invocations p ON c.parent_id=p.id WHERE p.status IN ('failed','cancelled') AND c.status IN ('accepted','running') UNION SELECT c.id FROM moria.function_invocations c JOIN stopped p ON c.parent_id=p.id WHERE c.status IN ('accepted','running')) UPDATE moria.function_invocations SET status='cancelled',finished_at=clock_timestamp() WHERE id IN (SELECT id FROM stopped) RETURNING id",
        )
      ).rows;
      for (const child of children) await this.event(client, child.id, 'state', 'cancelled');
    });
    if (this.stopped) return;
    const accepted = (
      await this.fs.store.catalog.pool.query(
        "SELECT id,context FROM moria.function_invocations WHERE status='accepted' AND lease_until>clock_timestamp() AND deadline>clock_timestamp() ORDER BY created_at LIMIT 4",
      )
    ).rows;
    for (const row of accepted) {
      if (this.active.size >= 4 || this.active.has(row.id)) continue;
      const controller = new AbortController();
      const task = this.execute(row.context, row.id, controller)
        .catch(() => {})
        .finally(() => this.active.delete(row.id));
      this.active.set(row.id, { controller, task });
    }
    const abandoned = (
      await this.fs.store.catalog.pool.query(
        "SELECT id FROM moria.function_invocations WHERE status IN ('succeeded','failed','cancelled') AND NOT worker_cleaned ORDER BY created_at LIMIT 4",
      )
    ).rows;
    for (const row of abandoned) {
      if (this.maintenance.has(row.id) || this.active.has(row.id)) continue;
      const task = this.worker!.remove(row.id)
        .then(async () => {
          await this.fs.store.catalog.pool.query(
            'UPDATE moria.function_invocations SET worker_cleaned=true WHERE id=$1',
            [row.id],
          );
        })
        .catch(() => {})
        .finally(() => this.maintenance.delete(row.id));
      this.maintenance.set(row.id, task);
    }
  }
  private transaction<T>(context: FilesystemContext, run: (client: PoolClient) => Promise<T>) {
    return this.fs.store.catalog.transaction(run, context.session.environment);
  }
  private async authorize(
    client: PoolClient,
    context: FilesystemContext,
    entryId: string,
    markings: { environment: string; marking: string }[] = [],
  ) {
    if (context.delegation?.length)
      throw new MoriaError('UNSUPPORTED_CAPABILITY', 'Function delegation is not enabled');
    const state = await this.fs.security.current(client, context);
    await this.fs.visible(client, state, entryId, 'read');
    for (const member of state.members) {
      if (markings.some((label) => label.environment !== member.session.environment))
        throw new MoriaError('FORBIDDEN', 'Operation is not authorized');
      authorize(
        member.state.identity,
        'functions',
        'invoke',
        markings.map((label) => label.marking),
      );
    }
    await this.fs.security.finish(client, state);
    return state.members[0].state.identity;
  }
  private async event(client: PoolClient, id: string, type: 'state' | 'log', message: string) {
    if (Buffer.byteLength(message) > 4096)
      throw new MoriaError('FUNCTION_PROTOCOL', 'Function log exceeds bounds');
    const sequence = (
      await client.query(
        'UPDATE moria.function_invocations SET event_sequence=event_sequence+1 WHERE id=$1 AND event_sequence<$2 RETURNING event_sequence',
        [id, type === 'log' ? 124 : 128],
      )
    ).rows[0];
    if (!sequence) throw new MoriaError('FUNCTION_PROTOCOL', 'Function event limit exceeded');
    await client.query(
      'INSERT INTO moria.function_events(invocation_id,sequence,type,message) VALUES($1,$2,$3,$4)',
      [id, sequence.event_sequence, type, message],
    );
  }
  private async row(client: PoolClient, context: FilesystemContext, id: string) {
    if (!uuid(id)) throw new MoriaError('FUNCTION_INVALID', 'Invalid invocation ID');
    const row = (
      await client.query(
        'SELECT * FROM moria.function_invocations WHERE id=$1 AND environment=$2 AND actor_hash=$3',
        [id, context.session.environment, actor(context)],
      )
    ).rows[0];
    if (!row) throw new MoriaError('NOT_FOUND', 'Invocation not found');
    row.calls = (
      await client.query(
        'SELECT key,path,status FROM moria.function_calls WHERE invocation_id=$1 ORDER BY created_at,key',
        [id],
      )
    ).rows;
    const identity = await this.authorize(client, context, row.entry_id, row.markings);
    if (row.value_version !== VALUE_VERSION)
      throw new MoriaError('UNSUPPORTED_VALUE_VERSION', 'Unsupported invocation value version');
    for (const grant of row.read_grants) {
      authorize(identity, grant.resource, grant.operation);
      const source = await this.fs.security.authority.resourceAccess(
        client,
        context.session,
        grant.resource,
        grant.operation,
      );
      const services = [
        ...this.objects.values(),
        ...[...(this.bindings.actions?.values() ?? [])].map((binding) => binding.service),
      ];
      const current = services
        .filter((service) => service.binding.policies.resource === grant.resource)
        .flatMap((service) => service.binding.policies.markings);
      row.markings = mergeMarkings(
        row.markings,
        [...source, ...current].map((marking) => ({
          environment: context.session.environment,
          marking,
        })),
      );
    }
    await this.authorize(client, context, row.entry_id, row.markings);
    row.markings = mergeMarkings(
      row.markings,
      await this.fs.security.effectiveMarkings(client, row.entry_id),
    );
    return row;
  }
  private public(row: any): FunctionInvocation {
    return {
      valueVersion: row.value_version,
      id: row.id,
      resourceId: row.resource_id,
      release: row.release,
      artifact: row.definition.spec.artifact,
      attemptId: row.attempt_id,
      correlationId: row.correlation_id,
      status: row.status,
      effect: row.definition.spec.effect,
      createdAt: row.created_at.toISOString(),
      finishedAt: row.finished_at?.toISOString() ?? null,
      markings: row.markings,
      ...(row.result ? { result: row.result } : {}),
      ...(row.error ? { error: row.error } : {}),
      ...(row.calls ? { calls: row.calls } : {}),
      ...(row.parent_id ? { parentId: row.parent_id } : {}),
      ...(row.prepare_only ? { prepareOnly: true } : {}),
      ...(row.proposal_commit ? { proposalCommit: row.proposal_commit } : {}),
    };
  }
  async invoke(
    inputContext: FilesystemContext,
    resourceId: string,
    key: string,
    input: unknown,
    prepareOnly = false,
  ): Promise<FunctionInvocation> {
    const context = structuredClone(inputContext);
    const parentExecution = context.session.execution;
    delete context.session.execution;
    if (
      !uuid(resourceId) ||
      typeof key !== 'string' ||
      !/^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$/.test(key)
    )
      throw new MoriaError('FUNCTION_INVALID', 'Resource UUID and idempotency key required');
    const serialized = canonicalJson(input);
    if (Buffer.byteLength(serialized) > 32768)
      throw new MoriaError('FUNCTION_INVALID', 'Function input exceeds bounds');
    const payload = JSON.parse(serialized);
    const hashInput = (input: unknown, fields: Record<string, TypeSchema>) =>
      valueFingerprint(validateRow(input, fields, 'value'), { type: 'object', properties: fields });
    // Recheck the resolved release inside acceptance so publication cannot switch code.
    const resolved = await this.runtime.source.resource(context, resourceId);
    if (resolved.resource.kind !== 'Function' || resolved.resource.spec.runtime !== 'typescript-v1')
      throw new MoriaError('FUNCTION_INVALID', 'Expected an active TypeScript Function');
    const definition = resolved.resource,
      spec = definition.spec;
    if (prepareOnly && spec.effect !== 'editProposal')
      throw new MoriaError(
        'FUNCTION_EFFECT',
        'Action-backed functions must produce edit proposals',
      );
    const accepted = await this.transaction(context, async (client) => {
      await this.authorize(client, context, resolved.entryId, resolved.markings);
      let parent: any;
      if (parentExecution) {
        await executionGuard(client, { ...context.session, execution: parentExecution });
        parent = await this.row(client, context, parentExecution.id);
        if (
          parent.definition.spec.effect !== 'readWrite' &&
          spec.effect !== 'readOnly' &&
          !(
            prepareOnly &&
            parent.definition.spec.effect === 'editProposal' &&
            spec.effect === 'editProposal'
          )
        )
          throw new MoriaError(
            'FUNCTION_EFFECT',
            'Parent effect does not permit this child function',
          );
        const ancestors = (
          await client.query(
            'WITH RECURSIVE chain AS (SELECT id,parent_id,resource_id FROM moria.function_invocations WHERE id=$1 UNION ALL SELECT p.id,p.parent_id,p.resource_id FROM moria.function_invocations p JOIN chain c ON p.id=c.parent_id) SELECT * FROM chain',
            [parent.id],
          )
        ).rows;
        if (ancestors.length >= 4 || ancestors.some((row) => row.resource_id === resourceId))
          throw new MoriaError(
            'FUNCTION_RECURSION',
            'Nested invocation depth or cycle limit exceeded',
          );
      }
      const existing = (
        await client.query(
          'SELECT * FROM moria.function_invocations WHERE environment=$1 AND resource_id=$2 AND actor_hash=$3 AND request_key=$4',
          [context.session.environment, resourceId, actor(context), key],
        )
      ).rows[0];
      if (existing) {
        if (existing.prepare_only !== prepareOnly)
          throw new MoriaError(
            'IDEMPOTENCY_CONFLICT',
            'Invocation key binds a different proposal mode',
          );
        let replayHash: string;
        try {
          replayHash = hashInput(payload, existing.definition.spec.inputs);
        } catch {
          throw new MoriaError('IDEMPOTENCY_CONFLICT', 'Invocation key binds different input');
        }
        if (existing.input_hash !== replayHash)
          throw new MoriaError(
            'IDEMPOTENCY_CONFLICT',
            'Invocation key already binds different input',
          );
        if (existing.finished_at && Date.now() - existing.finished_at.getTime() > 30 * 86400000)
          throw new MoriaError(
            'RESULT_EXPIRED',
            'Invocation result expired; its key cannot be reused',
          );
        return { id: existing.id, created: false };
      }
      const normalized = validateRow(payload, spec.inputs as Record<string, TypeSchema>, 'value');
      for (const fields of [spec.inputs, spec.outputs]) {
        const visit = (type: ReturnType<typeof normalizeType>) => {
          if (type.type === 'ontologyObject') this.objectBinding(type.objectType!);
          if (type.items) visit(type.items);
          for (const child of Object.values(type.properties ?? {})) visit(child);
        };
        visit(normalizeType({ type: 'object', properties: fields }));
      }
      const references = await this.referenceReads(context, normalized, spec.inputs, client);
      if (references.failed) throw references.error;
      const observations = [...(parent?.reads ?? []), ...references.observations];
      if (observations.length > 256)
        throw new MoriaError('FUNCTION_LIMIT', 'Read evidence exceeds invocation bounds');
      const referenceLabels = await this.checkReads(client, context, references.observations);
      await this.authorize(
        client,
        context,
        resolved.entryId,
        mergeMarkings(
          resolved.markings,
          parent?.markings ?? [],
          referenceLabels,
          references.markings,
        ),
      );
      const inputHash = hashInput(normalized, spec.inputs);
      functionLimits(spec.limits);
      if (this.stopped) throw new MoriaError('FUNCTION_BUSY', 'Function supervisor is stopping');
      const binding = (
        await client.query(
          'SELECT digest FROM moria.fs_resource_bindings WHERE entry_id=$1 AND active',
          [resolved.entryId],
        )
      ).rows[0];
      if (binding?.digest !== resolved.digest)
        throw new MoriaError(
          'CONCURRENT_MODIFICATION',
          'Function release changed before acceptance',
        );
      if (
        Number(
          (
            await client.query(
              "SELECT count(*) AS count FROM moria.function_invocations WHERE environment=$1 AND status IN ('accepted','running') AND lease_until>clock_timestamp()",
              [context.session.environment],
            )
          ).rows[0].count,
        ) >= 4
      )
        throw new MoriaError('FUNCTION_BUSY', 'Environment invocation capacity reached');
      const id = randomUUID();
      await client.query(
        `INSERT INTO moria.function_invocations(id,environment,resource_id,entry_id,release,definition,actor_hash,context,request_key,input_hash,input,attempt_id,fence,correlation_id,status,lease_until,deadline,markings,parent_id,reads,prepare_only,point_reads_only)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'accepted',clock_timestamp()+interval '10 seconds',LEAST(clock_timestamp()+$15*interval '1 second',COALESCE($18::timestamptz,'infinity')),$16,$17,$19,$20,$21)`,
        [
          id,
          context.session.environment,
          resourceId,
          resolved.entryId,
          resolved.digest,
          JSON.stringify(definition),
          actor(context),
          JSON.stringify(context),
          key,
          inputHash,
          JSON.stringify(normalized),
          randomUUID(),
          randomUUID(),
          randomUUID(),
          spec.limits.timeoutSeconds,
          JSON.stringify(
            mergeMarkings(
              resolved.markings,
              parent?.markings ?? [],
              referenceLabels,
              references.markings,
            ),
          ),
          parent?.id ?? null,
          parent?.deadline ?? null,
          JSON.stringify(observations),
          prepareOnly,
          spec.effect === 'editProposal' || parent?.point_reads_only === true,
        ],
      );
      if (parent || references.reads.length)
        await client.query('UPDATE moria.function_invocations SET read_grants=$2 WHERE id=$1', [
          id,
          JSON.stringify([...(parent?.read_grants ?? []), ...references.reads]),
        ]);
      await this.event(client, id, 'state', 'accepted');
      return { id, created: true };
    });
    return this.transaction(context, async (client) =>
      this.public({ ...(await this.row(client, context, accepted.id)), result: null }),
    );
  }
  async prepareAction(
    session: SessionContext,
    resourceId: string,
    key: string,
    input: unknown,
  ): Promise<PreparedFunctionEdits> {
    const context: FilesystemContext = { session: { ...session } };
    delete context.session.execution;
    const accepted = await this.invoke({ session }, resourceId, 'action-' + key, input, true);
    const result = await this.wait(context, accepted.id);
    if (!terminal(result.status))
      throw new MoriaError('FUNCTION_IN_PROGRESS', 'Proposal function is still running', {
        invocationId: result.id,
      });
    if (result.status !== 'succeeded')
      throw new MoriaError(
        result.error?.code ?? 'FUNCTION_FAILED',
        'Proposal function did not succeed',
        { invocationId: result.id },
      );
    const hash = fingerprint(result.result);
    return {
      invocationId: result.id,
      values: result.result! as Record<string, Value>,
      check: async (client, versions = true) => {
        const row = await this.row(client, context, result.id);
        if (!row.prepare_only || row.status !== 'succeeded' || fingerprint(row.result) !== hash)
          throw new MoriaError('FUNCTION_INPUT_CHANGED', 'Prepared function changed');
        if (Date.now() - row.finished_at.getTime() > 30 * 86400000)
          throw new MoriaError('RESULT_EXPIRED', 'Prepared function expired');
        const labels = mergeMarkings(
          row.markings,
          versions && !row.proposal_commit
            ? await this.checkReads(client, context, row.reads, 0, true)
            : [],
        );
        await this.authorize(client, context, row.entry_id, labels);
        return labels;
      },
      committed: async (client, commit) => {
        const row = await this.row(client, context, result.id);
        if (row.proposal_commit) return;
        const reads: ReadObservation[] = [];
        for (const read of row.reads as ReadObservation[])
          reads.push(
            read.filesystem || read.invocation
              ? read
              : (await this.read(context, read, client)).observation,
          );
        await client.query(
          'UPDATE moria.function_invocations SET reads=$2,proposal_commit=$3 WHERE id=$1',
          [result.id, JSON.stringify(reads), JSON.stringify(commit)],
        );
      },
    };
  }
  private objectBinding(objectType: string) {
    const service = resolveObject(this.objects, objectType);
    if (!service) throw new MoriaError('OBJECT_TYPE_UNBOUND', 'Ontology object type is not bound');
    return service;
  }
  private async referenceReads(
    context: FilesystemContext,
    value: unknown,
    fields: Record<string, TypeSchema>,
    client?: PoolClient,
    signal?: AbortSignal,
  ) {
    const observations: ReadObservation[] = [];
    const seen = new Set<string>();
    const captured = await captureAccess(async () =>
      bindOntologyObjects(value, { type: 'object', properties: fields }, async (identity) => {
        signal?.throwIfAborted();
        const key = identity.objectType + ':' + identity.uid;
        if (seen.has(key)) return identity;
        if (seen.size >= 256)
          throw new MoriaError('FUNCTION_LIMIT', 'Object references exceed invocation bounds');
        seen.add(key);
        this.objectBinding(identity.objectType);
        const result = await this.read(
          context,
          {
            objectType: identity.objectType,
            path: `/api/v1/objects/${identity.objectType}/${identity.uid}?properties=`,
            method: 'GET',
            input: null,
          },
          client,
        );
        signal?.throwIfAborted();
        observations.push(result.observation);
        if (result.error) throw result.error;
        return identity;
      }),
    );
    return { ...captured, observations };
  }
  private async read(
    context: FilesystemContext,
    request: any,
    transaction?: PoolClient,
  ): Promise<{ value: any; observation: ReadObservation; error?: MoriaError }> {
    if (
      typeof request.path !== 'string' ||
      !request.path.startsWith('/api/v1/objects/') ||
      request.path.length > 4096
    )
      throw new MoriaError('FORBIDDEN', 'Only declared OSDK object reads are supported');
    const url = new URL(request.path, 'http://moria.invalid'),
      parts = url.pathname.split('/').filter(Boolean).map(decodeURIComponent);
    if (url.origin !== 'http://moria.invalid' || parts.length !== 5)
      throw new MoriaError('FORBIDDEN', 'Unsupported OSDK route');
    const service = request.action
      ? this.bindings.actions?.get(request.action)?.service
      : resolveObject(this.objects, parts[3]);
    if (!service) throw new MoriaError('NOT_FOUND', 'Object binding not found');
    if (request.objectType && service.binding.objectTypeId !== request.objectType)
      throw new MoriaError('FUNCTION_INPUT_CHANGED', 'Ontology object type binding changed');
    const state = await this.fs.security.authority.current(
      context.session.environment,
      context.session.principal,
      context.session.application,
      transaction,
    );
    let value: unknown, error: MoriaError | undefined;
    const before = (
      await (transaction ?? this.fs.store.catalog.pool).query(
        'SELECT revision FROM moria.environments WHERE id=$1',
        [context.session.environment],
      )
    ).rows[0].revision;
    if (transaction && request.method === 'POST')
      throw new MoriaError('FUNCTION_EFFECT', 'Edit proposals require point reads');
    try {
      if (request.method === 'POST' && parts[4] === 'query' && !url.search)
        value = await service.query(context.session, request.input);
      else if (
        request.method === 'GET' &&
        request.input === null &&
        [...url.searchParams.keys()].every((key) => key === 'properties') &&
        url.searchParams.getAll('properties').length <= 1
      )
        value = await service.read(
          context.session,
          parts[4],
          url.searchParams.has('properties')
            ? url.searchParams.get('properties') === ''
              ? []
              : url.searchParams.get('properties')!.split(',')
            : undefined,
          transaction,
        );
      else
        throw new MoriaError(
          'FORBIDDEN',
          'Functions cannot invoke mutations or internal endpoints',
        );
    } catch (failure) {
      if (!(failure instanceof MoriaError) || failure.code !== 'NOT_FOUND') throw failure;
      error = failure;
      value = { error: { code: failure.code, message: failure.message } };
    }
    if (Buffer.byteLength(JSON.stringify(value)) > 32768)
      throw new MoriaError('FUNCTION_LIMIT', 'OSDK response exceeds invocation bounds');
    const { nextCursor: _, revision: _revision, ...comparable } = value as any;
    return {
      value,
      error,
      observation: {
        ...(service.binding.objectTypeId ? { objectType: service.binding.objectTypeId } : {}),
        ...(request.action ? { action: request.action } : {}),
        path: request.path,
        method: request.method,
        input: request.input,
        hash: fingerprint(comparable),
        revision: (value as any).revision ?? before,
        epoch: state.epoch,
        resource: service.binding.policies.resource,
        markings: service.binding.policies.markings,
      },
    };
  }
  private async verifyReads(
    context: FilesystemContext,
    reads: ReadObservation[],
    running = false,
    ancestry: string[] = [],
    signal: AbortSignal = AbortSignal.timeout(30000),
  ) {
    const verified: ReadObservation[] = [];
    for (const observation of reads) {
      signal.throwIfAborted();
      if (observation.invocation) {
        const value = observation.invocation.metadata
          ? await this.transaction(context, async (client) =>
              this.public(await this.row(client, context, observation.invocation!.id)),
            )
          : await this.inspect(
              context,
              observation.invocation.id,
              async (client, row) => {
                if (observation.invocation!.after === undefined) return this.public(row);
                const rows = (
                  await client.query(
                    'SELECT sequence::text,type,message,created_at FROM moria.function_events WHERE invocation_id=$1 AND sequence>$2 ORDER BY sequence LIMIT 100',
                    [row.id, observation.invocation!.after],
                  )
                ).rows;
                return {
                  events: rows.map((row) => ({
                    sequence: row.sequence,
                    type: row.type,
                    message: row.message,
                    createdAt: row.created_at.toISOString(),
                  })),
                };
              },
              0,
              ancestry,
            );
        const comparable = observation.invocation.metadata
          ? this.invocationMetadata(value as FunctionInvocation)
          : value;
        if (!running && fingerprint(comparable) !== observation.hash)
          throw new MoriaError('FUNCTION_INPUT_CHANGED', 'Nested invocation result changed');
        verified.push({
          ...observation,
          hash: fingerprint(comparable),
          epoch: (
            await this.fs.security.authority.current(
              context.session.environment,
              context.session.principal,
              context.session.application,
            )
          ).epoch,
        });
        continue;
      }
      const current = observation.filesystem
        ? await this.readFilesystem(context, observation.filesystem, signal)
        : await this.read(context, observation);
      signal.throwIfAborted();
      if (
        (!running ||
          current.observation.epoch !== observation.epoch ||
          current.observation.instanceEpoch !== observation.instanceEpoch) &&
        current.observation.hash !== observation.hash
      )
        throw new MoriaError(
          'FUNCTION_INPUT_CHANGED',
          'Function read evidence changed; result is withheld',
        );
      verified.push(current.observation);
    }
    return verified;
  }
  private async checkReads(
    client: PoolClient,
    context: FilesystemContext,
    reads: ReadObservation[],
    depth = 0,
    pointOnly = false,
  ) {
    if (depth >= 8)
      throw new MoriaError('FUNCTION_RECURSION', 'Invocation evidence depth exceeded');
    const env = (
      await client.query('SELECT revision,security_epoch FROM moria.environments WHERE id=$1', [
        context.session.environment,
      ])
    ).rows[0];
    if (
      reads.some(
        (read) =>
          (!read.filesystem && !read.invocation && read.revision !== env.revision) ||
          read.epoch !== env.security_epoch,
      )
    )
      throw new MoriaError(
        'FUNCTION_INPUT_CHANGED',
        'Function read evidence changed; result is withheld',
      );
    const markings: FilesystemMarking[] = [];
    for (const read of reads) {
      if (read.invocation) {
        const child = await this.row(client, context, read.invocation.id);
        markings.push(...child.markings);
        if (!read.invocation.metadata)
          markings.push(
            ...(await this.checkReads(client, context, child.reads, depth + 1, pointOnly)),
          );
        continue;
      }
      if (read.filesystem) {
        const epoch = (await client.query('SELECT security_epoch FROM moria.instance_state'))
          .rows[0].security_epoch;
        if (epoch !== read.instanceEpoch)
          throw new MoriaError('FUNCTION_INPUT_CHANGED', 'Filesystem authority changed');
        const state = await this.fs.security.current(client, context);
        for (const file of read.files ?? []) {
          await this.fs.visible(client, state, file.id, 'read');
          if ((await this.fs.store.get(file.id, client)).revision !== file.revision)
            throw new MoriaError('FUNCTION_INPUT_CHANGED', 'Filesystem read changed');
          markings.push(...(await this.fs.security.effectiveMarkings(client, file.id)));
        }
        continue;
      }
      if (pointOnly && read.method !== 'GET')
        throw new MoriaError('FUNCTION_EFFECT', 'Edit proposals require point-read dependencies');
      if (pointOnly) {
        const current = await this.read(context, read, client);
        if (current.observation.hash !== read.hash)
          throw new MoriaError('FUNCTION_INPUT_CHANGED', 'Proposal point-read changed');
      }
      const name = new URL(read.path, 'http://moria.invalid').pathname.split('/')[4];
      const binding = read.action
        ? this.bindings.actions?.get(read.action)?.service
        : resolveObject(this.objects, decodeURIComponent(name));
      if (
        !binding ||
        binding.binding.policies.resource !== read.resource ||
        (read.objectType && binding.binding.objectTypeId !== read.objectType)
      )
        throw new MoriaError('FUNCTION_INPUT_CHANGED', 'Object binding changed');
      const source = await this.fs.security.authority.resourceAccess(
        client,
        context.session,
        read.resource,
      );
      markings.push(
        ...[...source, ...read.markings, ...binding.binding.policies.markings].map((marking) => ({
          environment: context.session.environment,
          marking,
        })),
      );
    }
    return markings;
  }
  private async readFilesystem(
    context: FilesystemContext,
    request: FunctionRequest,
    signal: AbortSignal = AbortSignal.timeout(30000),
  ) {
    if (!this.bindings.filesystem) throw new MoriaError('NOT_FOUND', 'Filesystem API is not bound');
    const before = (
      await this.fs.store.catalog.pool.query('SELECT security_epoch FROM moria.instance_state')
    ).rows[0].security_epoch;
    const value = await this.bindings.filesystem(context, request, signal);
    const bytes = Buffer.from(value.body, 'base64');
    let data: any = bytes.toString('base64');
    if (value.headers['content-type']?.startsWith('application/json')) {
      data = JSON.parse(bytes.toString('utf8'));
      if (data && typeof data === 'object') {
        const { nextCursor: _, ...rest } = data;
        data = rest;
      }
    }
    const ids = new Set<string>();
    const path = request.path.split('?')[0].split('/');
    if (uuid(path[6])) ids.add(path[6]);
    const collect = (item: any, depth = 0) => {
      if (!item || typeof item !== 'object' || depth > 16) return;
      for (const [key, child] of Object.entries(item)) {
        if (['id', 'entryId', 'rootId', 'parentId'].includes(key) && uuid(child)) ids.add(child);
        else if (child && typeof child === 'object') collect(child, depth + 1);
      }
    };
    collect(data);
    const files: { id: string; revision: string }[] = [];
    for (const id of ids)
      try {
        const file = await this.fs.get(context, id);
        files.push({ id, revision: file.revision });
      } catch (error) {
        if ((error as MoriaError).code !== 'NOT_FOUND') throw error;
      }
    const state = await this.fs.security.authority.current(
      context.session.environment,
      context.session.principal,
      context.session.application,
    );
    const instanceEpoch = (
      await this.fs.store.catalog.pool.query('SELECT security_epoch FROM moria.instance_state')
    ).rows[0].security_epoch;
    if (instanceEpoch !== before)
      throw new MoriaError('AUTHORIZATION_CHANGED', 'Filesystem authority changed during read');
    return {
      value,
      observation: {
        filesystem: request,
        files,
        instanceEpoch,
        path: request.path,
        method: request.method,
        input: null,
        hash: fingerprint({ status: value.status, data }),
        revision: '',
        epoch: state.epoch,
        resource: '',
        markings: [],
      } satisfies ReadObservation,
    };
  }
  private response(value: unknown, status = 200): FunctionResponse {
    return {
      status,
      headers: { 'content-type': 'application/json' },
      body: Buffer.from(JSON.stringify(value)).toString('base64'),
    };
  }
  private async http(
    context: FilesystemContext,
    record: any,
    message: any,
    reads: ReadObservation[],
    signal: AbortSignal,
  ): Promise<FunctionResponse> {
    let callKey: string | undefined,
      dispatched = false;
    try {
      if (
        typeof message.path !== 'string' ||
        message.path.length > 4096 ||
        !message.path.startsWith('/api/') ||
        /[\\#\r\n]/.test(message.path)
      )
        throw new MoriaError('FORBIDDEN', 'Only supported platform paths are allowed');
      const url = new URL(message.path, 'http://moria.invalid');
      if (url.origin !== 'http://moria.invalid' || url.pathname !== message.path.split('?')[0])
        throw new MoriaError('FORBIDDEN', 'Noncanonical platform path');
      if (
        !['GET', 'POST'].includes(message.method) ||
        !message.headers ||
        typeof message.headers !== 'object' ||
        Array.isArray(message.headers)
      )
        throw new MoriaError('FUNCTION_PROTOCOL', 'Invalid platform request');
      const headers: Record<string, string> = {};
      for (const [key, value] of Object.entries(message.headers)) {
        if (
          !['content-type', 'idempotency-key', 'x-moria-size', 'x-moria-digest'].includes(key) ||
          typeof value !== 'string' ||
          value.length > 256 ||
          /[\r\n]/.test(value)
        )
          throw new MoriaError('FORBIDDEN', 'Unsupported platform header');
        headers[key] = value;
      }
      if (
        message.body !== null &&
        (typeof message.body !== 'string' ||
          !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(message.body))
      )
        throw new MoriaError('FUNCTION_PROTOCOL', 'Invalid platform body');
      const bytes = message.body === null ? null : Buffer.from(message.body, 'base64');
      if ((bytes?.length ?? 0) > 32768 || (message.method === 'GET' && bytes))
        throw new MoriaError('FUNCTION_LIMIT', 'Platform body exceeds bounds');
      const input =
        bytes && headers['content-type']?.startsWith('application/json')
          ? JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes))
          : null;
      const parts = url.pathname.split('/').filter(Boolean).map(decodeURIComponent);
      const filesystem = parts.slice(0, 4).join('/') === 'api/management/v1/fs';
      const action =
        parts.length === 5 &&
        parts.slice(0, 3).join('/') === 'api/v1/actions' &&
        parts[4] === 'invoke' &&
        message.method === 'POST' &&
        !url.search;
      const object = parts.length === 5 && parts.slice(0, 3).join('/') === 'api/v1/objects';
      const nested =
        parts.length === 5 &&
        parts.slice(0, 3).join('/') === 'api/v1/functions' &&
        parts[4] === 'invoke' &&
        message.method === 'POST' &&
        !url.search;
      const inspection = parts.slice(0, 3).join('/') === 'api/v1/invocations';
      const write =
        action ||
        nested ||
        (inspection && message.method === 'POST') ||
        (filesystem &&
          message.method === 'POST' &&
          !['resolve', 'resources', 'checkout', 'read'].includes(parts.at(-1)!));
      const key = headers['idempotency-key'] ?? message.id;
      if (write && (typeof key !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$/.test(key)))
        throw new MoriaError('IDEMPOTENCY_KEY_INVALID', 'Stable call key required');
      const scopedKey = fingerprint([record.id, key]).slice(7);
      headers['idempotency-key'] = scopedKey;
      const request: FunctionRequest = {
        path: message.path,
        method: message.method,
        headers,
        body: message.body,
      };
      const session = { ...context.session, execution: { id: record.id, fence: record.fence } };
      const inheritedCount = reads.length;
      const callContext = { ...context, session };
      await this.transaction(context, async (client) => {
        await this.row(client, context, record.id);
        await executionGuard(client, session);
      });
      if (signal.aborted) throw signal.reason;
      const nestedRead =
        nested &&
        (await this.runtime.source.resource(context, parts[3])).resource.spec.effect === 'readOnly';
      if (
        write &&
        record.definition.spec.effect !== 'readWrite' &&
        !(action && record.definition.spec.effect === 'editProposal') &&
        !nestedRead
      )
        throw new MoriaError('FUNCTION_EFFECT', 'Function effect does not allow this mutation');
      if (write) {
        const verified = await this.verifyReads(context, reads, false, [], signal);
        reads.splice(0, reads.length, ...verified);
        const existing = await this.transaction(context, async (client) => {
          await executionGuard(client, session);
          const row = await this.row(client, context, record.id);
          const labels = mergeMarkings(row.markings, await this.checkReads(client, context, reads));
          await this.authorize(client, context, row.entry_id, labels);
          await client.query(
            'UPDATE moria.function_invocations SET reads=$2,markings=$3 WHERE id=$1',
            [record.id, JSON.stringify(reads), JSON.stringify(labels)],
          );
          const hash = fingerprint(request);
          const old = (
            await client.query(
              'SELECT * FROM moria.function_calls WHERE invocation_id=$1 AND key=$2',
              [record.id, key],
            )
          ).rows[0];
          if (old) {
            if (old.request_hash !== hash)
              throw new MoriaError('IDEMPOTENCY_CONFLICT', 'Call key binds different input');
            return old;
          }
          await client.query(
            "INSERT INTO moria.function_calls(invocation_id,key,path,request_hash,request,status) VALUES($1,$2,$3,$4,$5,'started')",
            [record.id, key, request.path, hash, JSON.stringify(request)],
          );
        });
        if (existing) {
          if (existing.response) return existing.response;
          throw new MoriaError(
            'CALL_OUTCOME_UNKNOWN',
            'Call has an unresolved outcome; it will not be repeated',
          );
        }
        callKey = key;
      }
      let response: FunctionResponse, observation: ReadObservation | undefined;
      if (object) {
        if (record.point_reads_only && request.method === 'POST')
          throw new MoriaError('FUNCTION_EFFECT', 'Edit proposals require point-read dependencies');
        const result = await this.read(context, {
          path: request.path,
          method: request.method,
          input,
        });
        response = this.response(result.value, result.error ? 404 : 200);
        observation = result.observation;
      } else if (action) {
        if (record.prepare_only)
          throw new MoriaError(
            'FUNCTION_EFFECT',
            'Return typed property edits for the owning action',
          );
        const binding = this.bindings.actions?.get(parts[3]);
        if (!binding) throw new MoriaError('NOT_FOUND', 'Action binding not found');
        if (!input || Object.keys(input).join(',') !== 'input')
          throw new MoriaError('INPUT_INVALID', 'Expected an input object');
        const parameters = validateRow(input.input, binding.action.inputs);
        const target = parameters[binding.action.targetParameter];
        const observed = await this.read(context, {
          action: parts[3],
          path:
            '/api/v1/objects/' +
            encodeURIComponent(binding.service.binding.policies.resource) +
            '/' +
            target,
          method: 'GET',
          input: null,
        });
        await this.recordRead(context, record, reads, observed.observation);
        if (observed.error) throw observed.error;
        await this.inherit(
          context,
          record.id,
          binding.action.markings.map((marking) => ({ environment: session.environment, marking })),
        );
        if (record.definition.spec.effect === 'editProposal') {
          if (reads.some((read) => !read.filesystem && read.method === 'POST'))
            throw new MoriaError(
              'FUNCTION_EFFECT',
              'Edit proposals require point-read dependencies',
            );
          validateRow(input.input, binding.action.inputs);
          const state = await this.fs.security.authority.current(
            session.environment,
            session.principal,
            session.application,
          );
          authorize(state.identity, binding.action.resource, 'invoke', binding.action.markings);
          if (binding.action.function) {
            const inherited = reads.length;
            const prepared = await this.prepareAction(
              session,
              binding.action.function,
              fingerprint([binding.action.resource, scopedKey]).slice(7),
              input.input,
            );
            record.preparedActions.set(key, prepared);
            const value = await this.get(context, prepared.invocationId);
            await this.inherit(context, record.id, value.markings);
            await this.consumeInvocation(context, record, reads, value, inherited);
          }
          response = this.response({
            proposed: true,
            revision: (
              await this.fs.store.catalog.pool.query(
                'SELECT revision FROM moria.environments WHERE id=$1',
                [session.environment],
              )
            ).rows[0].revision,
          });
          await this.fs.store.catalog.pool.query(
            'UPDATE moria.function_calls SET binding_hash=$3 WHERE invocation_id=$1 AND key=$2',
            [
              record.id,
              key,
              fingerprint({ action: binding.action, binding: binding.service.binding }),
            ],
          );
        } else {
          dispatched = true;
          const result = await binding.service.invoke(
            session,
            binding.action,
            scopedKey,
            input.input,
            {
              guard: async (client) => {
                await this.checkReads(client, context, reads);
              },
            },
          );
          response = this.response(result);
          await this.fs.store.catalog.pool.query(
            "UPDATE moria.function_calls SET status='succeeded',response=$3 WHERE invocation_id=$1 AND key=$2",
            [record.id, key, JSON.stringify(response)],
          );
          await this.refreshAfterWrite(context, reads, result.revision);
        }
      } else if (filesystem) {
        if (!this.bindings.filesystem)
          throw new MoriaError('NOT_FOUND', 'Filesystem API is not bound');
        if (write) {
          const ids = new Set<string>();
          if (['entries', 'sources'].includes(parts[4]) && uuid(parts[5])) ids.add(parts[5]);
          if (['changes', 'copy', 'projects'].includes(parts[4])) {
            const collect = (value: any, depth = 0) => {
              if (!value || typeof value !== 'object' || depth > 8) return;
              for (const [key, child] of Object.entries(value)) {
                if (['id', 'parentId'].includes(key) && uuid(child)) ids.add(child);
                else if (child && typeof child === 'object') collect(child, depth + 1);
              }
            };
            collect(input);
          }
          if (ids.size > 100) throw new MoriaError('FUNCTION_LIMIT', 'Too many filesystem targets');
          for (const id of ids) {
            const metadata = await this.readFilesystem(context, {
              path: '/api/management/v1/fs/entries/' + id,
              method: 'GET',
              headers: {},
              body: null,
            });
            if (metadata.value.status >= 400) return metadata.value;
            await this.recordRead(context, record, reads, metadata.observation);
          }
          if (['principals', 'collect', 'groups'].includes(parts.at(-1)!))
            await this.transaction(context, async (client) => {
              await executionGuard(client, session, 'write');
            });
          dispatched = true;
          response = await this.bindings.filesystem(callContext, request, signal);
          if (response.status < 400)
            await this.refreshAfterWrite(context, reads, undefined, record.id);
        } else {
          const read = await this.readFilesystem(callContext, request);
          response = read.value;
          observation = read.observation;
        }
      } else if (nested) {
        if (
          !input ||
          Object.keys(input).some((key) => !['input', 'wait'].includes(key)) ||
          !Object.hasOwn(input, 'input') ||
          (input.wait !== undefined && typeof input.wait !== 'boolean')
        )
          throw new MoriaError('INPUT_INVALID', 'Invalid invocation body');
        dispatched = true;
        const child = await this.invoke(callContext, parts[3], scopedKey, input.input);
        const result = input.wait === false ? child : await this.wait(context, child.id);
        response = this.response(
          result,
          ['accepted', 'running'].includes(result.status) ? 202 : 200,
        );
        await this.inherit(context, record.id, result.markings);
        if (terminal(result.status))
          await this.consumeInvocation(context, record, reads, result, inheritedCount);
        observation = {
          invocation: { id: result.id, metadata: true },
          path: request.path,
          method: 'GET',
          input: null,
          hash: fingerprint(this.invocationMetadata(result)),
          revision: '',
          epoch: (
            await this.fs.security.authority.current(
              session.environment,
              session.principal,
              session.application,
            )
          ).epoch,
          resource: '',
          markings: [],
        };
      } else if (
        inspection &&
        (!url.search ||
          (parts[4] === 'events' &&
            [...url.searchParams.keys()].every((key) => key === 'after') &&
            url.searchParams.getAll('after').length <= 1))
      ) {
        if (parts[3] === record.id)
          throw new MoriaError(
            'FUNCTION_RECURSION',
            'A function cannot inspect its own invocation',
          );
        if (message.method === 'GET' && parts.length === 4) {
          const result = await this.get(context, parts[3]);
          await this.inherit(context, record.id, result.markings);
          await this.consumeInvocation(context, record, reads, result);
          response = this.response(result);
          observation = {
            invocation: { id: result.id, metadata: true },
            path: request.path,
            method: 'GET',
            input: null,
            hash: fingerprint(this.invocationMetadata(result)),
            revision: '',
            epoch: (
              await this.fs.security.authority.current(
                session.environment,
                session.principal,
                session.application,
              )
            ).epoch,
            resource: '',
            markings: [],
          };
        } else if (message.method === 'GET' && parts.length === 5 && parts[4] === 'events') {
          const result = await this.get(context, parts[3]);
          await this.inherit(context, record.id, result.markings);
          const value = {
            events: await this.events(context, parts[3], url.searchParams.get('after') ?? '0'),
          };
          await this.consumeInvocation(context, record, reads, result);
          response = this.response(value);
          observation = {
            invocation: { id: parts[3], metadata: true },
            path: request.path,
            method: 'GET',
            input: null,
            hash: fingerprint(this.invocationMetadata(result)),
            revision: '',
            epoch: (
              await this.fs.security.authority.current(
                session.environment,
                session.principal,
                session.application,
              )
            ).epoch,
            resource: '',
            markings: [],
          };
        } else if (
          message.method === 'POST' &&
          parts.length === 5 &&
          parts[4] === 'cancel' &&
          input &&
          !Object.keys(input).length
        ) {
          dispatched = true;
          response = this.response(await this.cancel(callContext, parts[3]));
        } else throw new MoriaError('NOT_FOUND', 'Platform endpoint not found');
      } else throw new MoriaError('NOT_FOUND', 'Platform endpoint not found');
      if (Buffer.byteLength(JSON.stringify(response)) > 49152)
        throw new MoriaError('FUNCTION_LIMIT', 'Platform response exceeds bounds');
      if (observation) {
        await this.recordRead(context, record, reads, observation);
      }
      if (write)
        await this.fs.store.catalog.pool.query(
          'UPDATE moria.function_calls SET status=$3,response=$4 WHERE invocation_id=$1 AND key=$2',
          [
            record.id,
            key,
            action && record.definition.spec.effect === 'editProposal'
              ? 'proposed'
              : response.status < 400
                ? 'succeeded'
                : 'failed',
            JSON.stringify(response),
          ],
        );
      if (write)
        await this.fs.store.catalog.pool.query(
          'UPDATE moria.function_invocations SET reads=$2 WHERE id=$1',
          [record.id, JSON.stringify(reads)],
        );
      return response;
    } catch (error) {
      const code = (error as MoriaError).code ?? 'FUNCTION_PROTOCOL';
      const status = /FORBIDDEN|DOWNGRADE|EFFECT/.test(code)
        ? 403
        : /NOT_FOUND/.test(code)
          ? 404
          : /CONFLICT|CHANGED|UNKNOWN|LOST|RECURSION/.test(code)
            ? 409
            : 400;
      const response = this.response(
        {
          error: {
            code,
            message: error instanceof MoriaError ? error.message : 'Invalid platform request',
          },
        },
        status,
      );
      if (callKey && !dispatched)
        await this.fs.store.catalog.pool.query(
          "UPDATE moria.function_calls SET status='failed',response=$3 WHERE invocation_id=$1 AND key=$2 AND status='started'",
          [record.id, callKey, JSON.stringify(response)],
        );
      return response;
    }
  }
  private async inherit(context: FilesystemContext, id: string, markings: FilesystemMarking[]) {
    await this.transaction(context, async (client) => {
      const row = await this.row(client, context, id);
      const merged = mergeMarkings(row.markings, markings);
      await client.query('UPDATE moria.function_invocations SET markings=$2 WHERE id=$1', [
        id,
        JSON.stringify(merged),
      ]);
    });
  }
  private async recordRead(
    context: FilesystemContext,
    record: any,
    reads: ReadObservation[],
    observation: ReadObservation | ReadObservation[],
  ) {
    const observations = Array.isArray(observation) ? observation : [observation];
    if (reads.length + observations.length > 256)
      throw new MoriaError('FUNCTION_LIMIT', 'Read evidence exceeds invocation bounds');
    await this.transaction(context, async (client) => {
      const row = await this.row(client, context, record.id);
      await executionGuard(client, {
        ...context.session,
        execution: { id: record.id, fence: record.fence },
      });
      const markings = mergeMarkings(
        row.markings,
        await this.checkReads(client, context, observations),
      );
      await this.authorize(client, context, row.entry_id, markings);
      reads.push(...observations);
      await client.query('UPDATE moria.function_invocations SET reads=$2,markings=$3 WHERE id=$1', [
        record.id,
        JSON.stringify(reads),
        JSON.stringify(markings),
      ]);
    });
  }
  private async consumeInvocation(
    context: FilesystemContext,
    record: any,
    reads: ReadObservation[],
    value: FunctionInvocation,
    inherited = 0,
  ) {
    if (record.point_reads_only && value.effect !== 'readOnly' && !value.prepareOnly)
      throw new MoriaError(
        'FUNCTION_EFFECT',
        'Edit proposals can consume only read-only function results',
      );
    const child = await this.transaction(context, (client) => this.row(client, context, value.id));
    if (child.reads.length < inherited)
      throw new MoriaError('FUNCTION_INPUT_CHANGED', 'Nested input evidence disappeared');
    if (inherited) reads.splice(0, inherited, ...child.reads.slice(0, inherited));
    await this.recordRead(context, record, reads, child.reads.slice(inherited));
  }
  private invocationMetadata(value: FunctionInvocation) {
    return {
      valueVersion: value.valueVersion,
      id: value.id,
      resourceId: value.resourceId,
      release: value.release,
      artifact: value.artifact,
      effect: value.effect,
    };
  }
  private async refreshAfterWrite(
    context: FilesystemContext,
    reads: ReadObservation[],
    revision?: string,
    filesystemInvocation?: string,
  ) {
    const committed: ReadObservation[] = filesystemInvocation
      ? (
          await this.fs.store.catalog.pool.query(
            'SELECT reads FROM moria.function_invocations WHERE id=$1',
            [filesystemInvocation],
          )
        ).rows[0].reads
      : reads;
    const refreshed: ReadObservation[] = [];
    for (const read of reads) {
      if (read.invocation) {
        refreshed.push(read);
        continue;
      }
      const current = read.filesystem
        ? await this.readFilesystem(context, read.filesystem)
        : await this.read(context, read);
      const anchor = committed[refreshed.length];
      if (read.filesystem) {
        if (
          (anchor.files ?? []).some(
            (file) =>
              current.observation.files?.find((item) => item.id === file.id)?.revision !==
              file.revision,
          ) ||
          (current.observation.hash !== read.hash &&
            !(read.files ?? []).some(
              (file) =>
                anchor.files?.find((item) => item.id === file.id)?.revision !== file.revision,
            ))
        )
          throw new MoriaError('FUNCTION_INPUT_CHANGED', 'Filesystem changed after committed call');
      } else if (!revision && current.observation.hash !== read.hash)
        throw new MoriaError('FUNCTION_INPUT_CHANGED', 'Object changed after filesystem call');
      if (revision && !read.filesystem && current.observation.revision !== revision)
        throw new MoriaError('FUNCTION_INPUT_CHANGED', 'Concurrent change followed committed call');
      refreshed.push(current.observation);
    }
    reads.splice(0, reads.length, ...refreshed);
  }
  private async execute(context: FilesystemContext, id: string, controller: AbortController) {
    let timer: ReturnType<typeof setTimeout> | undefined,
      heartbeat: ReturnType<typeof setInterval> | undefined,
      renewal = Promise.resolve();
    let record: any;
    const reads: ReadObservation[] = [];
    try {
      record = await this.transaction(context, async (client) => {
        const row = await this.row(client, context, id);
        const claimed = (
          await client.query(
            "UPDATE moria.function_invocations SET status='running' WHERE id=$1 AND status='accepted' AND lease_until>clock_timestamp() RETURNING *",
            [id],
          )
        ).rows[0];
        if (!claimed)
          throw new MoriaError('FUNCTION_WORKER_LOST', 'Invocation could not be claimed');
        await this.event(client, id, 'state', 'running');
        return claimed;
      });
      reads.push(...record.reads);
      record.preparedActions = new Map<string, PreparedFunctionEdits>();
      const remaining = record.deadline.getTime() - Date.now();
      timer = setTimeout(
        () => controller.abort(new MoriaError('FUNCTION_TIMEOUT', 'Function deadline exceeded')),
        Math.max(1, remaining),
      );
      heartbeat = setInterval(() => {
        renewal = renewal
          .then(async () => {
            const updated = await this.fs.store.catalog.pool.query(
              "UPDATE moria.function_invocations SET lease_until=clock_timestamp()+interval '10 seconds' WHERE id=$1 AND fence=$2 AND status='running' AND lease_until>clock_timestamp() AND deadline>clock_timestamp()",
              [id, record.fence],
            );
            if (!updated.rowCount)
              throw new MoriaError('FUNCTION_WORKER_LOST', 'Invocation lease lost');
          })
          .catch((error) => controller.abort(error));
      }, 1000);
      const result = await this.worker!.execute(
        id,
        record.definition.spec.artifact,
        record.definition.spec.limits,
        {
          version: 1,
          valueVersion: record.value_version,
          type: 'start',
          invocationId: id,
          attemptId: record.attempt_id,
          input: record.input,
          inputs: record.definition.spec.inputs,
          outputs: record.definition.spec.outputs,
          effect: record.definition.spec.effect,
        },
        async (request: any) => {
          const captured = await captureAccess(async () => {
            if (request.type === 'http')
              return this.http(context, record, request, reads, controller.signal);
            if (record.point_reads_only && request.method !== 'GET')
              throw new MoriaError(
                'FUNCTION_EFFECT',
                'Edit proposals require point-read dependencies',
              );
            await this.transaction(context, async (client) => {
              await this.row(client, context, id);
            });
            const { value, observation, error } = await this.read(context, request);
            await this.transaction(context, async (client) => {
              const row = await this.row(client, context, id);
              if (
                row.status !== 'running' ||
                row.fence !== record.fence ||
                controller.signal.aborted
              )
                throw new MoriaError('CANCELLED', 'Invocation stopped');
              const labels = mergeMarkings(
                row.markings,
                await this.checkReads(client, context, [observation]),
              );
              await this.authorize(client, context, row.entry_id, labels);
              reads.push(observation);
              await client.query(
                'UPDATE moria.function_invocations SET reads=$2,markings=$3 WHERE id=$1',
                [id, JSON.stringify(reads), JSON.stringify(labels)],
              );
            });
            if (error) throw error;
            return value;
          });
          await this.inherit(context, id, captured.markings);
          await this.fs.store.catalog.pool.query(
            "UPDATE moria.function_invocations SET read_grants=COALESCE((SELECT jsonb_agg(DISTINCT value) FROM jsonb_array_elements(read_grants || $2::jsonb)),'[]') WHERE id=$1",
            [id, JSON.stringify(captured.reads)],
          );
          await this.transaction(context, (client) => this.row(client, context, id));
          if (captured.failed)
            throw captured.error ?? new MoriaError('FUNCTION_FAILED', 'Platform call failed');
          return captured.value;
        },
        async (message) => {
          await this.transaction(context, async (client) => {
            const row = await this.row(client, context, id);
            if (row.status !== 'running') throw new MoriaError('CANCELLED', 'Invocation stopped');
            await this.event(client, id, 'log', message);
          });
        },
        controller.signal,
      );
      if (controller.signal.aborted) throw controller.signal.reason;
      if (Buffer.byteLength(JSON.stringify(result) ?? '') > 32768)
        throw new MoriaError('FUNCTION_OUTPUT_INVALID', 'Function output exceeds bounds');
      const validated = validateRow(result, record.definition.spec.outputs, 'value');
      const references = await this.referenceReads(
        context,
        validated,
        record.definition.spec.outputs,
        undefined,
        controller.signal,
      );
      if (references.observations.length || references.markings.length || references.reads.length) {
        await this.inherit(context, id, references.markings);
        await this.fs.store.catalog.pool.query(
          "UPDATE moria.function_invocations SET read_grants=COALESCE((SELECT jsonb_agg(DISTINCT value) FROM jsonb_array_elements(read_grants || $2::jsonb)),'[]') WHERE id=$1",
          [id, JSON.stringify(references.reads)],
        );
        await this.recordRead(context, record, reads, references.observations);
      }
      if (references.failed) throw references.error;
      const verified = await this.verifyReads(context, reads, false, [], controller.signal);
      await this.transaction(context, async (client) => {
        const row = await this.row(client, context, id);
        const labels = mergeMarkings(
          row.markings,
          await this.checkReads(client, context, verified),
        );
        await this.authorize(client, context, row.entry_id, labels);
        const proposals = (
          await client.query(
            "SELECT * FROM moria.function_calls WHERE invocation_id=$1 AND status='proposed' ORDER BY created_at,key",
            [id],
          )
        ).rows;
        if (proposals.length) {
          await this.checkReads(client, context, verified, 0, true);
          for (const proposal of proposals)
            await record.preparedActions.get(proposal.key)?.check(client);
          await client.query("SET LOCAL statement_timeout='5s'");
          if (record.definition.spec.effect !== 'editProposal')
            throw new MoriaError('FUNCTION_EFFECT', 'Unexpected edit proposals');
          if (verified.some((read) => !read.filesystem && read.method === 'POST'))
            throw new MoriaError('FUNCTION_EFFECT', 'Edit proposals require point reads');
          for (const proposal of proposals) {
            const name = new URL(proposal.path, 'http://moria.invalid').pathname.split('/')[4];
            const binding = this.bindings.actions?.get(decodeURIComponent(name));
            if (
              !binding ||
              proposal.binding_hash !==
                fingerprint({ action: binding.action, binding: binding.service.binding })
            )
              throw new MoriaError('CONCURRENT_MODIFICATION', 'Proposal action binding changed');
            const input = JSON.parse(Buffer.from(proposal.request.body, 'base64').toString('utf8'));
            const prepared: PreparedFunctionEdits | undefined = record.preparedActions.get(
              proposal.key,
            );
            if (binding.action.function && !prepared)
              throw new MoriaError('FUNCTION_WORKER_LOST', 'Prepared action state is unavailable');
            const action = prepared
              ? {
                  ...binding.action,
                  function: undefined,
                  edits: Object.entries(prepared.values).map(([property, value]) => ({
                    op: 'set' as const,
                    property,
                    value: { literal: value },
                  })),
                }
              : binding.action;
            const result = await binding.service.invoke(
              { ...context.session, execution: { id, fence: record.fence, proposal: true } },
              action,
              proposal.request.headers['idempotency-key'],
              input.input,
              { client, proposal: true, prepared, preparedChecked: true },
            );
            await client.query(
              "UPDATE moria.function_calls SET status='committed',response=$3 WHERE invocation_id=$1 AND key=$2",
              [id, proposal.key, JSON.stringify(this.response(result))],
            );
          }
          for (let index = 0; index < verified.length; index++) {
            if (!verified[index].filesystem && !verified[index].invocation)
              verified[index] = (await this.read(context, verified[index], client)).observation;
          }
        }
        const updated = await client.query(
          "UPDATE moria.function_invocations SET status='succeeded',result=$3,markings=COALESCE((SELECT jsonb_agg(DISTINCT value) FROM jsonb_array_elements(markings || $4::jsonb)),'[]'),reads=$5,finished_at=clock_timestamp() WHERE id=$1 AND fence=$2 AND status='running' AND lease_until>clock_timestamp() AND deadline>clock_timestamp()",
          [
            id,
            record.fence,
            JSON.stringify(validated),
            JSON.stringify(labels),
            JSON.stringify(verified),
          ],
        );
        if (!updated.rowCount)
          throw new MoriaError('FUNCTION_WORKER_LOST', 'Invocation cannot publish');
        await this.event(client, id, 'state', 'succeeded');
      });
    } catch (error) {
      const code = controller.signal.aborted
        ? (controller.signal.reason as MoriaError)?.code
        : (error as MoriaError)?.code;
      await this.transaction(context, async (client) => {
        const updated = await client.query(
          "UPDATE moria.function_invocations SET status='failed',error=$2,finished_at=clock_timestamp() WHERE id=$1 AND ((status='accepted' AND $3::uuid IS NULL) OR (status='running' AND fence=$3)) RETURNING id",
          [
            id,
            JSON.stringify({
              code: typeof code === 'string' ? code : 'FUNCTION_FAILED',
              message: 'Function invocation failed',
            }),
            record?.fence ?? null,
          ],
        );
        if (updated.rowCount) await this.event(client, id, 'state', 'failed');
      });
    } finally {
      if (timer) clearTimeout(timer);
      if (heartbeat) clearInterval(heartbeat);
      await renewal;
      if (record)
        try {
          await this.worker!.remove(id);
          await this.fs.store.catalog.pool.query(
            "UPDATE moria.function_invocations SET worker_cleaned=true WHERE id=$1 AND status IN ('succeeded','failed','cancelled')",
            [id],
          );
        } catch {
          /* A surviving worker is fenced; the worker maintenance loop retries cleanup. */
        }
    }
  }
  private async inspect<T>(
    context: FilesystemContext,
    id: string,
    read: (client: PoolClient, row: any) => Promise<T>,
    attempt = 0,
    ancestry: string[] = [],
  ): Promise<T> {
    if (ancestry.includes(id) || ancestry.length >= 8)
      throw new MoriaError(
        'FUNCTION_RECURSION',
        'Invocation evidence cycle or depth limit exceeded',
      );
    const snapshot = await this.transaction(context, async (client) => {
      const row = await this.row(client, context, id);
      if (
        !terminal(row.status) &&
        (row.lease_until.getTime() <= Date.now() || row.deadline.getTime() <= Date.now())
      ) {
        const error = {
          code: row.deadline.getTime() <= Date.now() ? 'FUNCTION_TIMEOUT' : 'FUNCTION_WORKER_LOST',
          message: 'Function worker lease or deadline expired',
        };
        await client.query(
          "UPDATE moria.function_invocations SET status='failed',error=$2,finished_at=clock_timestamp() WHERE id=$1",
          [id, JSON.stringify(error)],
        );
        await this.event(client, id, 'state', 'failed');
        Object.assign(row, { status: 'failed', error, finished_at: new Date() });
      }
      const environment = (
        await client.query(
          'SELECT revision,security_epoch,(SELECT security_epoch FROM moria.instance_state) AS instance_epoch FROM moria.environments WHERE id=$1',
          [context.session.environment],
        )
      ).rows[0];
      if (
        !terminal(row.status) &&
        row.reads.every(
          (observation: ReadObservation) =>
            observation.epoch === environment.security_epoch &&
            (observation.filesystem
              ? observation.instanceEpoch === environment.instance_epoch
              : observation.invocation || observation.revision === environment.revision),
        )
      ) {
        row.markings = mergeMarkings(
          row.markings,
          await this.checkReads(client, context, row.reads),
        );
        await this.authorize(client, context, row.entry_id, row.markings);
        await client.query('UPDATE moria.function_invocations SET markings=$2 WHERE id=$1', [
          id,
          JSON.stringify(row.markings),
        ]);
        return { row, inspected: true, value: await read(client, row) };
      }
      return { row, inspected: false, value: undefined };
    });
    if (snapshot.inspected) return snapshot.value as T;
    const initial = snapshot.row;
    let verified: ReadObservation[];
    try {
      verified = await this.verifyReads(context, initial.reads, !terminal(initial.status), [
        ...ancestry,
        id,
      ]);
    } catch (error) {
      const latest = await this.transaction(context, (client) => this.row(client, context, id));
      if (
        attempt < 3 &&
        (initial.status !== latest.status ||
          fingerprint(initial.reads) !== fingerprint(latest.reads))
      )
        return this.inspect(context, id, read, attempt + 1, ancestry);
      throw error;
    }
    try {
      return await this.transaction(context, async (client) => {
        const row = await this.row(client, context, id);
        if (row.status !== initial.status)
          throw new MoriaError(
            'FUNCTION_STATE_CHANGED',
            'Invocation state changed; retry inspection',
          );
        if (fingerprint(row.reads) !== fingerprint(initial.reads))
          throw new MoriaError(
            'FUNCTION_STATE_CHANGED',
            'Invocation reads changed; retry inspection',
          );
        row.markings = mergeMarkings(
          row.markings,
          await this.checkReads(client, context, verified),
        );
        await this.authorize(client, context, row.entry_id, row.markings);
        await client.query(
          "UPDATE moria.function_invocations SET markings=$2,reads=CASE WHEN status IN ('succeeded','failed','cancelled') THEN $3::jsonb ELSE reads END WHERE id=$1",
          [id, JSON.stringify(row.markings), JSON.stringify(verified)],
        );
        if (terminal(row.status) && Date.now() - row.finished_at.getTime() > 30 * 86400000)
          throw new MoriaError(
            'RESULT_EXPIRED',
            'Invocation result expired; its key cannot be reused',
          );
        return read(client, row);
      });
    } catch (error) {
      if (
        error instanceof MoriaError &&
        (error.code === 'FUNCTION_STATE_CHANGED' ||
          error.code === 'AUTHORIZATION_CHANGED' ||
          (!terminal(initial.status) && error.code === 'FUNCTION_INPUT_CHANGED')) &&
        attempt < 3
      )
        return this.inspect(context, id, read, attempt + 1, ancestry);
      throw error;
    }
  }
  get(context: FilesystemContext, id: string): Promise<FunctionInvocation> {
    return this.inspect(context, id, async (_client, row) => this.public(row));
  }
  async events(context: FilesystemContext, id: string, after = '0'): Promise<FunctionEvent[]> {
    if (!/^(0|[1-9][0-9]{0,3})$/.test(after))
      throw new MoriaError('FUNCTION_INVALID', 'Invalid event cursor');
    return this.inspect(context, id, async (client) => {
      return (
        await client.query(
          'SELECT sequence::text,type,message,created_at FROM moria.function_events WHERE invocation_id=$1 AND sequence>$2 ORDER BY sequence LIMIT 100',
          [id, after],
        )
      ).rows.map((row) => ({
        sequence: row.sequence,
        type: row.type,
        message: row.message,
        createdAt: row.created_at.toISOString(),
      }));
    });
  }
  async cancel(context: FilesystemContext, id: string) {
    const result = await this.transaction(context, async (client) => {
      const row = await this.row(client, context, id);
      await executionGuard(client, context.session, 'write', row.markings);
      if (!terminal(row.status)) {
        await client.query(
          "UPDATE moria.function_invocations SET status='cancelled',finished_at=clock_timestamp() WHERE id=$1",
          [id],
        );
        await this.event(client, id, 'state', 'cancelled');
        const children = (
          await client.query(
            "WITH RECURSIVE children AS (SELECT id FROM moria.function_invocations WHERE parent_id=$1 UNION ALL SELECT c.id FROM moria.function_invocations c JOIN children p ON c.parent_id=p.id) UPDATE moria.function_invocations SET status='cancelled',finished_at=clock_timestamp() WHERE id IN (SELECT id FROM children) AND status IN ('accepted','running') RETURNING id",
            [id],
          )
        ).rows;
        for (const child of children) await this.event(client, child.id, 'state', 'cancelled');
      }
      return this.public({
        ...row,
        status: terminal(row.status) ? row.status : 'cancelled',
        finished_at: row.finished_at ?? new Date(),
        result: null,
      });
    });
    this.active.get(id)?.controller.abort(new MoriaError('CANCELLED', 'Invocation cancelled'));
    return result;
  }
  async wait(context: FilesystemContext, id: string, milliseconds = 25000) {
    const until = Date.now() + milliseconds;
    do {
      try {
        const result = await this.get(context, id);
        if (terminal(result.status)) return result;
      } catch (error) {
        if (
          !(error instanceof MoriaError) ||
          !['FUNCTION_STATE_CHANGED', 'CONCURRENT_MODIFICATION'].includes(error.code)
        )
          throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    } while (Date.now() < until);
    return this.get(context, id);
  }
  async close() {
    this.stopped = true;
    if (this.workerTimer) clearInterval(this.workerTimer);
    await this.polling;
    for (const { controller } of this.active.values())
      controller.abort(new MoriaError('FUNCTION_WORKER_LOST', 'Function supervisor stopped'));
    await Promise.allSettled([...this.active.values()].map(({ task }) => task));
    await Promise.allSettled([...this.maintenance.values()]);
  }
}
