import { MoriaError, type Expr, type Value, type ValueType } from '../../contracts/types.js';
import { validDate, timestampMicros, validateValue } from '../../contracts/values.js';

export interface ExpressionTypes {
  fields?: Record<string, ValueType>;
  parameters?: Record<string, ValueType>;
  attributes?: Record<string, ValueType>;
}
export interface ExpressionValues {
  fields?: Record<string, Value>;
  parameters?: Record<string, Value>;
  attributes?: Record<string, Value>;
}
type TypeName = ValueType['type'] | 'null';
interface Inferred {
  type: TypeName;
  items?: Inferred;
  scale?: number;
}
const bool: Inferred = { type: 'boolean' };
const fail = (message: string): never => {
  throw new MoriaError('POLICY_INVALID', message);
};
const scalar = (value: unknown): Inferred => ({
  type:
    value === null
      ? 'null'
      : typeof value === 'number'
        ? 'float64'
        : typeof value === 'boolean'
          ? 'boolean'
          : 'string',
});
function same(a: Inferred, b: Inferred): boolean {
  return (
    a.type === 'null' ||
    b.type === 'null' ||
    (a.type === b.type && (a.type !== 'array' || same(a.items!, b.items!)))
  );
}
function literalCompatible(e: Expr, t: Inferred): boolean {
  if (!('literal' in e) || e.literal === null) return 'literal' in e;
  if (typeof e.literal !== 'string') return false;
  switch (t.type) {
    case 'int64':
      return (
        /^-?(0|[1-9][0-9]*)$/.test(e.literal) &&
        BigInt(e.literal) >= -(2n ** 63n) &&
        BigInt(e.literal) < 2n ** 63n
      );
    case 'decimal':
      return (
        /^-?(0|[1-9][0-9]*)(\.[0-9]+)?$/.test(e.literal) &&
        (e.literal.split('.')[1]?.length ?? 0) <= (t.scale ?? 0)
      );
    case 'uid':
      return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(e.literal);
    case 'date':
      return validDate(e.literal);
    case 'timestamp':
      try {
        timestampMicros(e.literal);
        return true;
      } catch {
        return false;
      }
    default:
      return false;
  }
}

export function compileExpression(expression: Expr, types: ExpressionTypes) {
  const lookup = (group: Record<string, ValueType> | undefined, name: string) =>
    group && Object.hasOwn(group, name) ? group[name] : undefined;
  let nodes = 0;
  const inferred = new Map<Expr, Inferred>();
  function inspect(e: Expr, depth = 0): Inferred {
    if (++nodes > 256 || depth > 16) fail('Expression exceeds complexity limits');
    let result: Inferred;
    if ('literal' in e) {
      if (Array.isArray(e.literal)) {
        if (e.literal.length > 100) fail('Literal list exceeds 100 values');
        const element = e.literal.find((x) => x !== null);
        const item = scalar(element ?? null);
        if (!e.literal.every((x) => same(item, scalar(x)))) fail('Mixed literal array types');
        result = { type: 'array', items: item };
      } else {
        if (typeof e.literal === 'number' && !Number.isFinite(e.literal))
          fail('Non-finite literal');
        result = scalar(e.literal);
      }
    } else if ('field' in e)
      result = lookup(types.fields, e.field) ?? fail(`Unknown or unavailable field ${e.field}`);
    else if ('parameter' in e)
      result =
        lookup(types.parameters, e.parameter) ??
        fail(`Unknown or unavailable parameter ${e.parameter}`);
    else if ('principalAttribute' in e)
      result =
        lookup(types.attributes, e.principalAttribute) ??
        fail(`Unknown principal attribute ${e.principalAttribute}`);
    else {
      const arity = ['not', 'isNull'].includes(e.op) ? 1 : ['and', 'or'].includes(e.op) ? null : 2;
      if ((arity !== null && e.args.length !== arity) || (arity === null && e.args.length < 2))
        fail('Invalid operator arity');
      const args = e.args.map((a) => inspect(a, depth + 1));
      const [a, b] = args;
      if (['and', 'or', 'not'].includes(e.op)) {
        if (args.some((x) => x.type !== 'boolean' && x.type !== 'null'))
          fail('Boolean operator needs boolean operands');
      } else if (e.op === 'isNull') {
        /* any typed value */
      } else if (e.op === 'in' || e.op === 'contains') {
        const list = e.op === 'in' ? b : a,
          val = e.op === 'in' ? a : b;
        if (list.type !== 'array' || !same(list.items!, val))
          fail('Membership needs a compatible scalar and array');
      } else if (['eq', 'ne', 'lt', 'lte', 'gt', 'gte'].includes(e.op)) {
        if (!same(a, b) && !literalCompatible(e.args[0], b) && !literalCompatible(e.args[1], a))
          fail('Comparison types differ');
        if (a.type === 'null' || b.type === 'null') fail('Use isNull rather than comparing null');
        if (
          ['lt', 'lte', 'gt', 'gte'].includes(e.op) &&
          [a.type, b.type].some((x) => ['array', 'boolean'].includes(x))
        )
          fail('Ordering unsupported for these operands');
      } else fail(`Unsupported operator ${e.op}`);
      result = bool;
    }
    inferred.set(e, result);
    return result;
  }
  if (inspect(expression).type !== 'boolean') fail('Policy root must return boolean');
  function compare(a: Value, b: Value, type: Inferred): number {
    if (a === null || b === null) return a === b ? 0 : a === null ? 1 : -1;
    if (type.type === 'timestamp') {
      const x = timestampMicros(a as string),
        y = timestampMicros(b as string);
      return x < y ? -1 : x > y ? 1 : 0;
    }
    if (type.type === 'int64')
      return BigInt(a as string) < BigInt(b as string)
        ? -1
        : BigInt(a as string) > BigInt(b as string)
          ? 1
          : 0;
    if (type.type === 'decimal') {
      const numeric = (v: Value) => {
        const str = String(v);
        const [whole, fraction = ''] = str.replace(/^-/, '').split('.');
        return (
          BigInt(whole + fraction.padEnd(type.scale ?? 0, '0')) * (str.startsWith('-') ? -1n : 1n)
        );
      };
      const x = numeric(a),
        y = numeric(b);
      return x < y ? -1 : x > y ? 1 : 0;
    }
    if (Array.isArray(a) && Array.isArray(b))
      return a.length === b.length && a.every((v, i) => compare(v, b[i], type.items!) === 0)
        ? 0
        : 1;
    if (typeof a === 'string' && typeof b === 'string') {
      // Compare Unicode code points, not JavaScript UTF-16 units.
      const x = Array.from(a, (c) => c.codePointAt(0)!),
        y = Array.from(b, (c) => c.codePointAt(0)!);
      for (let i = 0; i < Math.min(x.length, y.length); i++)
        if (x[i] !== y[i]) return x[i] < y[i] ? -1 : 1;
      return x.length < y.length ? -1 : x.length > y.length ? 1 : 0;
    }
    return a! < b! ? -1 : a! > b! ? 1 : 0;
  }
  function comparisonType(e: Expr & { op: string; args: Expr[] }): Inferred {
    const a = inferred.get(e.args[0])!,
      b = inferred.get(e.args[1])!;
    return 'literal' in e.args[0] && !same(a, b) ? b : a;
  }
  function evaluate(values: ExpressionValues): boolean | null {
    validateInputs(values);
    function ev(e: Expr): Value {
      if ('literal' in e) return e.literal;
      if ('field' in e)
        return Object.hasOwn(values.fields ?? {}, e.field) ? values.fields![e.field] : null;
      if ('parameter' in e)
        return Object.hasOwn(values.parameters ?? {}, e.parameter)
          ? values.parameters![e.parameter]
          : null;
      if ('principalAttribute' in e)
        return Object.hasOwn(values.attributes ?? {}, e.principalAttribute)
          ? values.attributes![e.principalAttribute]
          : null;
      const args = e.args.map(ev),
        [a, b] = args;
      if (e.op === 'and') return args.includes(false) ? false : args.includes(null) ? null : true;
      if (e.op === 'or') return args.includes(true) ? true : args.includes(null) ? null : false;
      if (e.op === 'not') return a === null ? null : !a;
      if (e.op === 'isNull') return a === null;
      if (e.op === 'in' || e.op === 'contains') {
        const list = (e.op === 'in' ? b : a) as Value[],
          val = e.op === 'in' ? a : b;
        const listType = inferred.get(e.args[e.op === 'in' ? 1 : 0])!;
        if (list === null) return null;
        if (!list.length) return false;
        if (val === null) return null;
        if (list.some((x) => x !== null && compare(x, val, listType.items!) === 0)) return true;
        return list.includes(null) ? null : false;
      }
      if (a === null || b === null) return null;
      const c = compare(a, b, comparisonType(e));
      switch (e.op) {
        case 'eq':
          return c === 0;
        case 'ne':
          return c !== 0;
        case 'lt':
          return c < 0;
        case 'lte':
          return c <= 0;
        case 'gt':
          return c > 0;
        case 'gte':
          return c >= 0;
        default:
          return fail('Unknown operator');
      }
    }
    return ev(expression) as boolean | null;
  }
  function sql(
    values: Omit<ExpressionValues, 'fields'>,
    fieldColumns: Record<string, string>,
  ): { text: string; values: unknown[] } {
    validateInputs(values);
    const params: unknown[] = [];
    const sqlType = (t: Inferred): string =>
      ({
        string: 'text',
        uid: 'uuid',
        boolean: 'boolean',
        int64: 'bigint',
        float64: 'double precision',
        decimal: 'numeric',
        date: 'date',
        timestamp: 'timestamptz',
        null: 'text',
        array: '',
      })[t.type] || `${sqlType(t.items!)}[]`;
    const bind = (value: unknown, type: Inferred) => {
      params.push(value);
      return `$${params.length}::${sqlType(type)}`;
    };
    function emit(e: Expr, expected?: Inferred): string {
      const t = expected ?? inferred.get(e)!;
      if ('literal' in e) return bind(e.literal, t);
      if ('parameter' in e)
        return bind(
          Object.hasOwn(values.parameters ?? {}, e.parameter)
            ? values.parameters![e.parameter]
            : null,
          t,
        );
      if ('principalAttribute' in e)
        return bind(
          Object.hasOwn(values.attributes ?? {}, e.principalAttribute)
            ? values.attributes![e.principalAttribute]
            : null,
          t,
        );
      if ('field' in e) {
        const column = fieldColumns[e.field];
        if (!column || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(column))
          fail('Field column is not a safe internal identifier');
        return `"${column}"`;
      }
      if (e.op === 'and' || e.op === 'or')
        return `(${e.args.map((x) => emit(x)).join(` ${e.op.toUpperCase()} `)})`;
      if (e.op === 'not') return `(NOT ${emit(e.args[0])})`;
      if (e.op === 'isNull') return `(${emit(e.args[0])} IS NULL)`;
      if (e.op === 'in' || e.op === 'contains') {
        const [val, list] = e.op === 'in' ? e.args : [e.args[1], e.args[0]];
        return `(${emit(val)} = ANY(${emit(list)}))`;
      }
      const type = comparisonType(e),
        op = ({ eq: '=', ne: '<>', lt: '<', lte: '<=', gt: '>', gte: '>=' } as any)[e.op];
      const left = emit(e.args[0], type),
        right = emit(e.args[1], type);
      // PostgreSQL UTF8 byte order under C collation agrees with Unicode code-point order.
      return type.type === 'string'
        ? `((${left} COLLATE "C") ${op} (${right} COLLATE "C"))`
        : `(${left} ${op} ${right})`;
    }
    return { text: emit(expression), values: params };
  }
  function validateInputs(values: ExpressionValues): void {
    for (const group of ['fields', 'parameters', 'attributes'] as const)
      for (const [name, value] of Object.entries(values[group] ?? {})) {
        const type = types[group]?.[name];
        if (type)
          validateValue(
            value,
            { ...type, nullable: group === 'attributes' || type.nullable },
            `${group}.${name}`,
          );
      }
  }
  return { evaluate, allows: (values: ExpressionValues) => evaluate(values) === true, sql };
}
