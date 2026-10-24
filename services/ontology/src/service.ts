import { Authority } from '../../security/src/authority.js';
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

/** Must be supplied by a verified session, not parsed from client JSON. */
export interface SessionContext {
  environment: string;
  principal: string;
  application: string;
  sessionHash?: string;
}
export interface ObjectBinding {
  /** Additional trusted application gates, checked in the same policy snapshot. */
  requiredOperations?: { resource: string; operation: string }[];
  schema: ObjectSchema;
  policies: ObjectPolicies;
  writePolicy?: Expr;
  propertyWrite?: Record<string, Expr>;
}
export interface ActionBinding {
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

/** Integrated point/query/action path. Human OIDC, release resolution and
 * delegated sessions are not implemented by this internal service. */
export class ObjectService {
  constructor(
    readonly store: ObjectStore,
    readonly authority: Authority,
    readonly binding: ObjectBinding,
  ) {}
  async query(
    session: SessionContext,
    input: unknown,
  ): Promise<{ revision: string; objects: Record<string, Value>[]; nextCursor?: string }> {
    const query = parseQuery(input),
      { policies, schema } = this.binding;
    const state = await this.authority.current(
      session.environment,
      session.principal,
      session.application,
    );
    for (const required of this.binding.requiredOperations ?? [])
      authorize(state.identity, required.resource, required.operation);
    const project = objectReader(policies).prepare(
      state.identity,
      state.attributes,
      query.projection,
      [schema.primaryKey, ...queryFields(query.filter)],
    );
    // Validate even for empty populations, before touching object data.
    const predicate =
      query.filter && compileExpression(query.filter, { fields: policies.properties });
    const cursors = new CursorStore(this.store.catalog);
    const binding: CursorBinding = {
      environment: session.environment,
      principal: session.principal,
      application: session.application,
      delegation: [],
      contract: fingerprint(JSON.parse(JSON.stringify(this.binding))),
      securityEpoch: state.epoch,
      query: {
        resource: policies.resource,
        filter: query.filter ?? null,
        projection: query.projection ?? null,
        pageSize: query.pageSize,
        order: 'uid-asc',
      },
    };
    return this.store.catalog.transaction(
      async (c) => {
        await c.query("SET LOCAL statement_timeout='30s'");
        const env = (
          await c.query(
            "SELECT revision,security_epoch,clock_timestamp()+interval '15 minutes' AS expires_at FROM moria.environments WHERE id=$1 FOR SHARE",
            [session.environment],
          )
        ).rows[0];
        if (!env || env.security_epoch !== state.epoch)
          throw new MoriaError('AUTHORIZATION_CHANGED', 'Current authorization must be rechecked');
        await this.authority.assertCurrent(
          c,
          session.environment,
          session.principal,
          session.application,
          state.epoch,
          session.sessionHash,
        );
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
        await this.store.assertSchema(
          c,
          session.environment,
          policies.resource,
          schema,
          policies.markings,
        );
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
        await this.authority.assertCurrent(
          c,
          session.environment,
          session.principal,
          session.application,
          state.epoch,
          session.sessionHash,
        );
        const nextCursor =
          rows.rows.length > query.pageSize
            ? await cursors.saveIn(c, binding, pin, rows.rows[query.pageSize - 1].uid)
            : undefined;
        return {
          revision: pin.revision,
          objects: verified.slice(0, query.pageSize),
          ...(nextCursor ? { nextCursor } : {}),
        };
      },
      session.environment,
      'repeatable-read',
      'shared',
    );
  }
  async read(
    session: SessionContext,
    uid: string,
    projection?: string[],
  ): Promise<{ revision: string; object: Record<string, Value> }> {
    validateValue(uid, { type: 'uid', nullable: false });
    const state = await this.authority.current(
      session.environment,
      session.principal,
      session.application,
    );
    for (const required of this.binding.requiredOperations ?? [])
      authorize(state.identity, required.resource, required.operation);
    const project = objectReader(this.binding.policies).prepare(
      state.identity,
      state.attributes,
      projection,
    );
    return this.store.catalog.transaction(
      async (c) => {
        const env = (
          await c.query(
            'SELECT revision,security_epoch FROM moria.environments WHERE id=$1 FOR SHARE',
            [session.environment],
          )
        ).rows[0];
        if (!env || env.security_epoch !== state.epoch)
          throw new MoriaError('AUTHORIZATION_CHANGED', 'Current authorization must be rechecked');
        await this.authority.assertCurrent(
          c,
          session.environment,
          session.principal,
          session.application,
          state.epoch,
          session.sessionHash,
        );
        const row = await this.store.read(
          c,
          session.environment,
          this.binding.policies.resource,
          uid,
          env.revision,
        );
        await this.store.assertSchema(
          c,
          session.environment,
          this.binding.policies.resource,
          this.binding.schema,
          this.binding.policies.markings,
        );
        const object = project(row, row);
        if (!object) throw new MoriaError('NOT_FOUND', 'Object not found');
        return { revision: env.revision, object };
      },
      session.environment,
      'repeatable-read',
      'shared',
    );
  }
  async invoke(
    session: SessionContext,
    action: ActionBinding,
    key: string,
    input: unknown,
  ): Promise<{ revision: string }> {
    if (!key || key.length > 256)
      throw new MoriaError('IDEMPOTENCY_KEY_INVALID', 'A bounded idempotency key is required');
    const parameters = validateRow(input, action.inputs);
    const uid = validateValue(parameters[action.targetParameter], {
      type: 'uid',
      nullable: false,
    }) as string;
    const state = await this.authority.current(
      session.environment,
      session.principal,
      session.application,
    );
    authorize(state.identity, action.resource, 'invoke', action.markings);
    for (const required of this.binding.requiredOperations ?? [])
      authorize(state.identity, required.resource, required.operation);
    const { policies, schema } = this.binding;
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
      this.binding.writePolicy &&
      compileExpression(this.binding.writePolicy, {
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
      const expression = this.binding.propertyWrite?.[edit.property];
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
    return this.store.edit(
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
        await this.authority.assertCurrent(
          c,
          session.environment,
          session.principal,
          session.application,
          state.epoch,
          session.sessionHash,
        );
        await this.store.assertSchema(
          c,
          session.environment,
          policies.resource,
          schema,
          policies.markings,
        );
      },
    );
  }
}
