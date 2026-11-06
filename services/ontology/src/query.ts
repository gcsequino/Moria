import type { PoolClient } from 'pg';
import { compileExpression } from '../../../packages/authorization/src/expressions.js';
import type { ObjectPolicies } from '../../../packages/authorization/src/objects.js';
import {
  MoriaError,
  type Expr,
  type Value,
  type ValueType,
} from '../../../packages/contracts/types.js';

export interface ObjectQuery {
  filter?: Expr;
  projection?: string[];
  pageSize: number;
  cursor?: string;
}
export function parseQuery(input: unknown): ObjectQuery {
  const fail = (): never => {
    throw new MoriaError('QUERY_INVALID', 'Invalid object query');
  };
  if (!input || typeof input !== 'object' || Array.isArray(input)) return fail();
  const q = input as Record<string, unknown>;
  if (Object.hasOwn(q, 'sort'))
    throw new MoriaError(
      'UNSUPPORTED_CAPABILITY',
      'Only UID ascending order is currently supported',
    );
  if (Object.keys(q).some((k) => !['filter', 'projection', 'pageSize', 'cursor'].includes(k)))
    return fail();
  const pageSize = q.pageSize ?? 100;
  if (
    typeof pageSize !== 'number' ||
    !Number.isInteger(pageSize) ||
    pageSize < 1 ||
    pageSize > 1000
  )
    return fail();
  if (
    q.projection !== undefined &&
    (!Array.isArray(q.projection) ||
      q.projection.length > 1000 ||
      q.projection.some((x) => typeof x !== 'string') ||
      new Set(q.projection).size !== q.projection.length)
  )
    return fail();
  if (
    q.cursor !== undefined &&
    (typeof q.cursor !== 'string' || !/^[A-Za-z0-9_-]{43}$/.test(q.cursor))
  )
    return fail();
  let nodes = 0;
  function expression(v: unknown, depth = 0): void {
    if (++nodes > 256 || depth > 16 || !v || typeof v !== 'object' || Array.isArray(v))
      return fail();
    const e = v as Record<string, unknown>,
      keys = Object.keys(e);
    if (keys.length === 1 && Object.hasOwn(e, 'literal')) {
      const scalar = (x: unknown) =>
        x === null ||
        typeof x === 'string' ||
        typeof x === 'boolean' ||
        (typeof x === 'number' && Number.isFinite(x));
      if (
        Array.isArray(e.literal)
          ? e.literal.length > 100 || !e.literal.every(scalar)
          : !scalar(e.literal)
      )
        return fail();
      return;
    }
    if (keys.length === 1 && Object.hasOwn(e, 'field') && typeof e.field === 'string') return;
    if (
      keys.length === 2 &&
      Object.hasOwn(e, 'op') &&
      Object.hasOwn(e, 'args') &&
      [
        'and',
        'or',
        'not',
        'eq',
        'ne',
        'lt',
        'lte',
        'gt',
        'gte',
        'in',
        'contains',
        'isNull',
      ].includes(e.op as string) &&
      Array.isArray(e.args)
    ) {
      for (const a of e.args) expression(a, depth + 1);
      return;
    }
    return fail();
  }
  if (q.filter !== undefined) expression(q.filter);
  return {
    pageSize,
    filter: q.filter as Expr | undefined,
    projection: q.projection as string[] | undefined,
    cursor: q.cursor as string | undefined,
  };
}
export function queryFields(filter?: Expr): string[] {
  if (!filter) return [];
  return 'field' in filter
    ? [filter.field]
    : 'op' in filter
      ? filter.args.flatMap(queryFields)
      : [];
}

/** SQL evaluates both pinned/current restrictions before LIMIT. JSON keys and all
 * user values are parameters; SQL identifiers and types are generated internally.
 * Initial ordering is UID ascending; custom sorts/aggregations are not supported.
 */
export async function queryRows(
  c: PoolClient,
  environment: string,
  policy: ObjectPolicies,
  attributes: Record<string, Value>,
  revision: string,
  currentRevision: string,
  after: string | null,
  query: ObjectQuery,
) {
  // Resolve immutable generation pointers first, in the same MVCC transaction.
  // Constant stage keys let PostgreSQL use the (stage,uid) index for ordered LIMIT.
  const heads = await c.query(
    `SELECT pins.kind,h.revision,h.stage FROM (VALUES('pinned',$3::bigint),('current',$4::bigint)) pins(kind,revision)
    JOIN LATERAL (SELECT revision,moria.runtime_source_stage(stage,environment,object_type,revision) AS stage FROM moria.source_generations WHERE environment=$1 AND object_type=$2 AND revision<=pins.revision ORDER BY revision DESC LIMIT 1) h ON TRUE`,
    [environment, policy.resource, revision, currentRevision],
  );
  const pinned = heads.rows.find((h) => h.kind === 'pinned'),
    live = heads.rows.find((h) => h.kind === 'current');
  if (!pinned || !live) return { rows: [] };
  const values: unknown[] = [
    environment,
    policy.resource,
    revision,
    currentRevision,
    after,
    query.pageSize + 1,
  ];
  const bind = (v: unknown) => {
    values.push(v);
    return `$${values.length}`;
  };
  const pinnedTable = pinned.stage ? 'moria.staged_source_rows' : 'moria.source_rows';
  const currentTable = live.stage ? 'moria.staged_source_rows' : 'moria.source_rows';
  const pinnedConstraint = pinned.stage
    ? `p.stage=${bind(pinned.stage)}::uuid`
    : `p.environment=$1 AND p.object_type=$2 AND p.revision=${bind(pinned.revision)}::bigint`;
  const currentConstraint = live.stage
    ? `r.stage=${bind(live.stage)}::uuid`
    : `r.environment=$1 AND r.object_type=$2 AND r.revision=${bind(live.revision)}::bigint`;
  const sqlType = (type: ValueType): string => {
    switch (type.type) {
      case 'uid':
        return 'uuid';
      case 'string':
        return 'text';
      case 'boolean':
        return 'boolean';
      case 'int64':
        return 'bigint';
      case 'float64':
        return 'double precision';
      case 'decimal':
        return 'numeric';
      case 'date':
        return 'date';
      case 'timestamp':
        return 'timestamptz';
      case 'array':
        return sqlType(type.items!) + '[]';
    }
  };
  const extract = (json: string, name: string, type: ValueType): string => {
    const key = bind(name);
    if (type.type === 'array')
      return `(CASE WHEN (${json}->${key}::text) IS NULL OR (${json}->${key}::text)='null'::jsonb THEN NULL ELSE ARRAY(SELECT element::${sqlType(type.items!)} FROM jsonb_array_elements_text(${json}->${key}::text) AS elements(element)) END)`;
    return `((${json}->>${key}::text)::${sqlType(type)})`;
  };
  function predicate(
    expr: Expr | undefined,
    fields: Record<string, ValueType>,
    json: string,
  ): string {
    if (!expr) return 'TRUE';
    const names = Object.keys(fields),
      columns = Object.fromEntries(names.map((name, i) => [name, `f${i}`]));
    const compiled = compileExpression(expr, { fields, attributes: policy.attributes }).sql(
      { attributes },
      columns,
    );
    const offset = values.length;
    values.push(...compiled.values);
    return compiled.text
      .replace(/\$(\d+)/g, (_, n) => `$${Number(n) + offset}`)
      .replace(/"f(\d+)"/g, (_, n) => extract(json, names[Number(n)], fields[names[Number(n)]]));
  }
  const effective = "(p.value||COALESCE(po.overrides,'{}'::jsonb))",
    current = "(cur.value||COALESCE(co.overrides,'{}'::jsonb))";
  const predicates = [
    predicate(policy.row, policy.sourceColumns, 'p.value'),
    predicate(policy.row, policy.sourceColumns, 'cur.value'),
    predicate(policy.objectRead, policy.properties, effective),
    predicate(policy.objectRead, policy.properties, current),
    predicate(query.filter, policy.properties, effective),
  ];
  // OFFSET 0 preserves the dependent current-UID lookup. Flattening this lateral
  // join let the planner scan/sort a whole newer generation before LIMIT when
  // pinned and current stages differed (measured multi-second refresh stalls).
  return c.query(
    `SELECT p.uid,p.value AS source,${effective} AS effective,cur.value AS current_source,${current} AS current_effective
    FROM ${pinnedTable} p
    JOIN LATERAL (SELECT r.value FROM ${currentTable} r WHERE ${currentConstraint} AND r.uid=p.uid OFFSET 0) cur ON TRUE
    LEFT JOIN LATERAL (SELECT overrides,tombstone FROM moria.overlay_versions WHERE environment=$1 AND object_type=$2 AND uid=p.uid AND revision<=$3 ORDER BY revision DESC LIMIT 1) po ON TRUE
    LEFT JOIN LATERAL (SELECT overrides,tombstone FROM moria.overlay_versions WHERE environment=$1 AND object_type=$2 AND uid=p.uid AND revision<=$4 ORDER BY revision DESC LIMIT 1) co ON TRUE
    WHERE ${pinnedConstraint} AND ($5::uuid IS NULL OR p.uid>$5) AND NOT COALESCE(po.tombstone,false) AND NOT COALESCE(co.tombstone,false) AND ${predicates.map((p) => `(${p}) IS TRUE`).join(' AND ')}
    ORDER BY p.uid ASC LIMIT $6`,
    values,
  );
}
