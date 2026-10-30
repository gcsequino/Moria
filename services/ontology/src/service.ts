import { Authority, type SecuritySnapshot } from '../../security/src/authority.js';
import { ObjectStore, type ObjectSchema, type Edit } from './store.js';
import { objectReader, type ObjectPolicies } from '../../../packages/authorization/src/objects.js';
import { authorize } from '../../../packages/authorization/src/access.js';
import { compileExpression } from '../../../packages/authorization/src/expressions.js';
import {
  MoriaError,
  type Expr,
  type Value,
  type ValueType,
} from '../../../packages/contracts/types.js';
import { validateRow, validateValue } from '../../../packages/contracts/values.js';
import { CursorStore, type CursorBinding } from './cursors.js';
import { parseQuery, queryFields, queryRows } from './query.js';
import { fingerprint } from '../../../packages/compiler/src/index.js';
import type { PoolClient } from 'pg';
import { executionGuard, type ExecutionConstraint } from '../../security/src/execution.js';

/** Must be supplied by a verified session, not parsed from client JSON. */
export interface SessionContext {
  environment: string;
  principal: string;
  application: string;
  sessionHash?: string;
  execution?: ExecutionConstraint;
}
export interface ObjectBinding {
  /** Immutable ontology identity shared with the generated app contract. */
  objectTypeId?: string;
  /** Additional trusted application gates, checked in the same policy snapshot. */
  requiredOperations?: { resource: string; operation: string }[];
  schema: ObjectSchema;
  policies: ObjectPolicies;
  writePolicy?: Expr;
  propertyWrite?: Record<string, Expr>;
}
export interface ActionBinding {
  function?: string;
  resource: string;
  revision: string;
  markings: string[];
  targetParameter: string;
  inputs: Record<string, ValueType>;
  condition: Expr;
  edits: (
    | { op: 'set'; property: string; value: { parameter: string } | { literal: Value } }
    | { op: 'clearOverride'; property: string }
  )[];
}
export interface PreparedFunctionEdits {
  invocationId: string;
  values: Record<string, Value>;
  check(
    client: PoolClient,
    versions?: boolean,
  ): Promise<{ environment: string; marking: string }[]>;
  committed(client: PoolClient, result: { revision: string }): Promise<void>;
}

/** UUID routes resolve by immutable identity; names remain supported for existing callers. */
export function resolveObject(
  objects: ReadonlyMap<string, ObjectService>,
  reference: string,
): ObjectService | undefined {
  if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(reference))
    return objects.get(reference);
  const candidates = [...new Set(objects.values())].filter(
    (service) => service.binding.objectTypeId === reference,
  );
  if (candidates.length > 1)
    throw new MoriaError('OBJECT_TYPE_UNBOUND', 'Ambiguous ontology object type binding');
  return candidates[0];
}

/** Integrated point/query/action path. Human OIDC, release resolution and
 * delegated sessions are not implemented by this internal service. */
export class ObjectService {
  functions?: {
    prepareAction(
      session: SessionContext,
      resourceId: string,
      key: string,
      input: unknown,
    ): Promise<PreparedFunctionEdits>;
  };
  readonly #binding: ObjectBinding;
  readonly #store: ObjectStore;
  readonly #authority: Authority;
  constructor(store: ObjectStore, authority: Authority, binding: ObjectBinding) {
    this.#binding = structuredClone(binding);
    this.#store = store;
    this.#authority = authority;
    if (binding.objectTypeId !== undefined)
      validateValue(binding.objectTypeId, { type: 'uid', nullable: false });
  }
  get binding(): ObjectBinding {
    return structuredClone(this.#binding);
  }
  requiring(operations: { resource: string; operation: string }[]): ObjectService {
    const service = new ObjectService(this.#store, this.#authority, {
      ...this.#binding,
      requiredOperations: [...(this.#binding.requiredOperations ?? []), ...operations],
    });
    service.functions = this.functions;
    return service;
  }
  private async current(
    session: SessionContext,
    transaction?: PoolClient,
  ): Promise<SecuritySnapshot> {
    const state = await this.#authority.current(
      session.environment,
      session.principal,
      session.application,
      transaction,
    );
    for (const required of this.#binding.requiredOperations ?? [])
      authorize(state.identity, required.resource, required.operation);
    return state;
  }
  private assertCurrent(c: PoolClient, session: SessionContext, epoch: string) {
    return this.#authority.assertCurrent(
      c,
      session.environment,
      session.principal,
      session.application,
      epoch,
      session.sessionHash,
    );
  }
  /** The owner releases results only after the final check; callers cannot omit it. */
  private protectedRead<T>(
    session: SessionContext,
    state: SecuritySnapshot,
    evaluate: (c: PoolClient, env: { revision: string; expires_at: Date }) => Promise<T>,
    transaction?: PoolClient,
  ): Promise<T> {
    const execute = async (c: PoolClient) => {
      if (!transaction) await c.query("SET LOCAL statement_timeout='30s'");
      await this.#authority.resourceAccess(c, session, this.#binding.policies.resource);
      const env = (
        await c.query(
          "SELECT revision,security_epoch,clock_timestamp()+interval '15 minutes' AS expires_at FROM moria.environments WHERE id=$1 FOR SHARE",
          [session.environment],
        )
      ).rows[0];
      if (!env || env.security_epoch !== state.epoch)
        throw new MoriaError('AUTHORIZATION_CHANGED', 'Current authorization must be rechecked');
      await this.assertCurrent(c, session, state.epoch);
      await this.#store.assertSchema(
        c,
        session.environment,
        this.#binding.policies.resource,
        this.#binding.schema,
        this.#binding.policies.markings,
      );
      const result = await evaluate(c, env);
      await this.assertCurrent(c, session, state.epoch);
      return result;
    };
    return transaction
      ? execute(transaction)
      : this.#store.catalog.transaction(execute, session.environment, 'repeatable-read', 'shared');
  }

  async query(
    session: SessionContext,
    input: unknown,
  ): Promise<{ revision: string; objects: Record<string, Value>[]; nextCursor?: string }> {
    session = { ...session };
    const query = parseQuery(input),
      { policies, schema } = this.#binding;
    const state = await this.current(session);
    const project = objectReader(policies).prepare(
      state.identity,
      state.attributes,
      query.projection,
      [schema.primaryKey, ...queryFields(query.filter)],
    );
    // Validate even for empty populations, before touching object data.
    const predicate =
      query.filter && compileExpression(query.filter, { fields: policies.properties });
    const cursors = new CursorStore(this.#store.catalog);
    const binding: CursorBinding = {
      environment: session.environment,
      principal: session.principal,
      application: session.application,
      delegation: [],
      contract: fingerprint(JSON.parse(JSON.stringify(this.#binding))),
      securityEpoch: state.epoch,
      query: {
        resource: policies.resource,
        filter: query.filter ?? null,
        projection: query.projection ?? null,
        pageSize: query.pageSize,
        order: 'uid-asc',
      },
    };
    return this.protectedRead(session, state, async (c, env) => {
      const pin = query.cursor
        ? await cursors.resumeIn(c, query.cursor, binding)
        : {
            revision: env.revision,
            currentRevision: env.revision,
            after: null,
            expiresAt: env.expires_at.toISOString(),
          };
      const after =
        pin.after === null
          ? null
          : (validateValue(pin.after, { type: 'uid', nullable: false }) as string);
      const rows = await queryRows(
        c,
        session.environment,
        policies,
        state.attributes,
        pin.revision,
        env.revision,
        after,
        query,
      );
      const verified = rows.rows.map((row) => {
        const object = project(
          { source: row.source, effective: row.effective },
          { source: row.current_source, effective: row.current_effective },
        );
        // Fail closed on an SQL/reference mismatch rather than silently leaking.
        if (!object || (predicate && !predicate.allows({ fields: row.effective })))
          throw new MoriaError('POLICY_MISMATCH', 'Query authorization verification failed');
        return object;
      });
      const nextCursor =
        rows.rows.length > query.pageSize
          ? await cursors.saveIn(c, binding, pin, rows.rows[query.pageSize - 1].uid)
          : undefined;
      return {
        revision: pin.revision,
        objects: verified.slice(0, query.pageSize),
        ...(nextCursor ? { nextCursor } : {}),
      };
    });
  }

  async read(
    session: SessionContext,
    uid: string,
    projection?: string[],
    transaction?: PoolClient,
  ): Promise<{ revision: string; object: Record<string, Value> }> {
    session = { ...session };
    validateValue(uid, { type: 'uid', nullable: false });
    const state = await this.current(session, transaction);
    const project = objectReader(this.#binding.policies).prepare(
      state.identity,
      state.attributes,
      projection,
    );
    return this.protectedRead(
      session,
      state,
      async (c, env) => {
        const row = await this.#store.read(
          c,
          session.environment,
          this.#binding.policies.resource,
          uid,
          env.revision,
        );
        const object = project(row, row);
        if (!object) throw new MoriaError('NOT_FOUND', 'Object not found');
        return { revision: env.revision, object };
      },
      transaction,
    );
  }

  async invoke(
    session: SessionContext,
    action: ActionBinding,
    key: string,
    input: unknown,
    options: {
      client?: PoolClient;
      proposal?: boolean;
      guard?: (client: PoolClient) => Promise<void>;
      prepared?: PreparedFunctionEdits;
      preparedChecked?: boolean;
    } = {},
  ): Promise<{ revision: string }> {
    session = { ...session };
    action = structuredClone(action);
    if (!key || key.length > 256)
      throw new MoriaError('IDEMPOTENCY_KEY_INVALID', 'A bounded idempotency key is required');
    if (action.function) {
      if (options.client)
        throw new MoriaError(
          'FUNCTION_EFFECT',
          'Function-backed actions cannot run user code inside another action transaction',
        );
      const parameters = validateRow(input, action.inputs);
      const state = await this.#authority.current(
        session.environment,
        session.principal,
        session.application,
      );
      authorize(state.identity, action.resource, 'invoke', action.markings);
      for (const required of this.#binding.requiredOperations ?? [])
        authorize(state.identity, required.resource, required.operation);
      const existing = (
        await this.#store.catalog.pool.query(
          'SELECT 1 FROM moria.action_keys WHERE environment=$1 AND application=$2 AND principal=$3 AND action=$4 AND key=$5',
          [session.environment, session.application, session.principal, action.resource, key],
        )
      ).rowCount;
      if (existing)
        return this.invoke(
          session,
          { ...action, function: undefined, edits: [] },
          key,
          input,
          options,
        );
      if (!this.functions)
        throw new MoriaError(
          'UNSUPPORTED_CAPABILITY',
          'Function proposal runtime is not configured',
        );
      const prepared = await this.functions.prepareAction(
        session,
        action.function,
        fingerprint([action.resource, key]).slice(7),
        parameters,
      );
      const edits = Object.entries(prepared.values).map(([property, value]) => ({
        op: 'set' as const,
        property,
        value: { literal: value },
      }));
      return this.invoke(session, { ...action, function: undefined, edits }, key, input, {
        ...options,
        prepared,
      });
    }
    if ((session.execution || options.prepared) && !options.client)
      return this.#store.catalog.transaction(async (client) => {
        const result = await this.invoke(session, action, key, input, { ...options, client });
        await executionGuard(client, session);
        return result;
      }, session.environment);
    const parameters = validateRow(input, action.inputs);
    const uid = validateValue(parameters[action.targetParameter], {
      type: 'uid',
      nullable: false,
    }) as string;
    const state = await this.#authority.current(
      session.environment,
      session.principal,
      session.application,
    );
    authorize(state.identity, action.resource, 'invoke', action.markings);
    for (const required of this.#binding.requiredOperations ?? [])
      authorize(state.identity, required.resource, required.operation);
    const { policies, schema } = this.#binding;
    const fields: string[] = [];
    const inspect = (expression: Expr): void => {
      if ('field' in expression) fields.push(expression.field);
      else if ('op' in expression) expression.args.forEach(inspect);
    };
    inspect(action.condition);
    const project = objectReader(policies).prepare(
      state.identity,
      state.attributes,
      undefined,
      fields,
    );
    const write =
      this.#binding.writePolicy &&
      compileExpression(this.#binding.writePolicy, {
        fields: schema.properties,
        attributes: policies.attributes,
      });
    const condition = compileExpression(action.condition, {
      fields: schema.properties,
      parameters: action.inputs,
      attributes: policies.attributes,
    });
    const edits: Edit[] = action.edits.map((edit) => {
      if (!Object.hasOwn(schema.properties, edit.property) || edit.property === schema.primaryKey)
        throw new MoriaError('EDIT_INVALID', 'Unknown or immutable property');
      const expression = this.#binding.propertyWrite?.[edit.property];
      if (
        expression &&
        !compileExpression(expression, { attributes: policies.attributes }).allows({
          attributes: state.attributes,
        })
      )
        throw new MoriaError('PROPERTY_FORBIDDEN', 'Property is not writable');
      if (edit.op === 'clearOverride') return edit;
      const value =
        'parameter' in edit.value ? parameters[edit.value.parameter] : edit.value.literal;
      return {
        op: 'set',
        property: edit.property,
        value: validateValue(value, schema.properties[edit.property]),
      };
    });
    const result = await this.#store.edit(
      {
        ...session,
        action: action.resource,
        actionRevision: action.revision,
        key,
        input: parameters,
        securityEpoch: state.epoch,
      },
      policies.resource,
      uid,
      schema,
      edits,
      (before, after) => {
        if (!project(before, before)) throw new MoriaError('NOT_FOUND', 'Object not found');
        if (!after || !project(after, after))
          throw new MoriaError('FORBIDDEN', 'After-state restrictions failed');
        for (const row of [before!, after])
          if (write && !write.allows({ fields: row.effective, attributes: state.attributes }))
            throw new MoriaError('FORBIDDEN', 'Object write policy denied');
        if (
          !condition.allows({ fields: before!.effective, parameters, attributes: state.attributes })
        )
          throw new MoriaError('ACTION_CONDITION_FAILED', 'Action condition failed');
      },
      async (c) => {
        const labels = [
          ...policies.markings,
          ...(await this.#authority.resourceAccess(c, session, policies.resource, 'write')),
        ];
        await this.#authority.resourceAccess(c, session, action.resource, 'write');
        await executionGuard(
          c,
          session,
          options.proposal ? 'proposal' : 'write',
          labels.map((marking) => ({ environment: session.environment, marking })),
        );
        await options.guard?.(c);
        const source = await options.prepared?.check(c, !options.preparedChecked);
        if (
          source?.some(
            (label) => label.environment !== session.environment || !labels.includes(label.marking),
          )
        )
          throw new MoriaError(
            'MARKING_DOWNGRADE',
            'Action destination omits function input markings',
          );
        await this.#authority.assertCurrent(
          c,
          session.environment,
          session.principal,
          session.application,
          state.epoch,
          session.sessionHash,
        );
        await this.#store.assertSchema(
          c,
          session.environment,
          policies.resource,
          schema,
          policies.markings,
        );
      },
      options.client,
    );
    if (options.prepared) await options.prepared.committed(options.client!, result);
    return result;
  }
}
