import { MoriaError } from './errors.js';
import {
  VALUE_VERSION,
  ONTOLOGY_OBJECT,
  type ObjectIdentity,
  type Value,
  type ValueType,
  type TypeSchema,
  type ResolvedType,
  type JsonValue,
} from './value-types.js';

const plain = (value: unknown): value is Record<string, any> =>
  !!value &&
  typeof value === 'object' &&
  !Array.isArray(value) &&
  Object.getOwnPropertySymbols(value).length === 0 &&
  [Object.prototype, null].includes(Object.getPrototypeOf(value));
const identifier = (name: string) =>
  /^[A-Za-z_][A-Za-z0-9_-]{0,99}$/.test(name) &&
  !['__proto__', 'prototype', 'constructor'].includes(name);

export function normalizeType(
  input: unknown,
  definitions: Record<string, TypeSchema> = {},
  profile: 'value' | 'column' = 'value',
): ResolvedType {
  let nodes = 0;
  const fail = (): never => {
    throw new MoriaError('TYPE_INVALID', 'Invalid or unsupported value type');
  };
  if (!plain(definitions) || !['value', 'column'].includes(profile)) return fail();
  function visit(type: unknown, stack: string[] = [], depth = 0): ResolvedType {
    if (++nodes > 10000 || depth > 20 || !plain(type)) return fail();
    const keys = (allowed: string[]) => {
      if (Object.keys(type).some((key) => !allowed.includes(key))) fail();
    };
    if (Object.hasOwn(type, 'ref')) {
      keys(['ref']);
      if (
        typeof type.ref !== 'string' ||
        !Object.hasOwn(definitions, type.ref) ||
        stack.includes(type.ref)
      )
        return fail();
      return visit(definitions[type.ref], [...stack, type.ref], depth + 1);
    }
    if (!['object', 'list'].includes(type.type) && typeof type.nullable !== 'boolean')
      return fail();
    if (type.nullable !== undefined && typeof type.nullable !== 'boolean') return fail();
    const nullable = type.nullable ?? false;
    if (type.type === 'ontologyObject') {
      keys(['type', 'nullable', 'objectType']);
      if (
        typeof type.objectType !== 'string' ||
        !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(type.objectType)
      )
        return fail();
      return { type: 'ontologyObject', nullable, objectType: type.objectType };
    }
    if (type.type === 'object') {
      keys(['type', 'nullable', 'properties', 'optional']);
      if (
        !plain(type.properties) ||
        (type.optional !== undefined &&
          (!Array.isArray(type.optional) ||
            new Set(type.optional).size !== type.optional.length ||
            type.optional.some(
              (key: unknown) => typeof key !== 'string' || !Object.hasOwn(type.properties, key),
            )))
      )
        return fail();
      const properties: Record<string, ResolvedType> = {};
      for (const [key, value] of Object.entries(type.properties).sort(([a], [b]) =>
        a < b ? -1 : a > b ? 1 : 0,
      )) {
        if (!identifier(key)) return fail();
        properties[key] = visit(value, stack, depth + 1);
      }
      return {
        type: 'object',
        nullable,
        properties,
        ...(type.optional?.length ? { optional: [...type.optional].sort() } : {}),
      };
    }
    if (type.type === 'array' || type.type === 'list') {
      keys(['type', 'nullable', 'items']);
      return { type: 'array', nullable, items: visit(type.items, stack, depth + 1) };
    }
    if (type.type === 'decimal') {
      keys(['type', 'nullable', 'precision', 'scale']);
      if (
        !Number.isInteger(type.precision) ||
        !Number.isInteger(type.scale) ||
        type.precision < 1 ||
        type.precision > 38 ||
        type.scale < 0 ||
        type.scale > type.precision
      )
        return fail();
      return { type: 'decimal', nullable, precision: type.precision, scale: type.scale };
    }
    keys(['type', 'nullable']);
    if (!['string', 'boolean', 'int64', 'float64', 'date', 'timestamp', 'uid'].includes(type.type))
      return fail();
    return { type: type.type, nullable };
  }
  const result = visit(input);
  if (
    profile === 'column' &&
    (!plain(input) ||
      ![
        'string',
        'boolean',
        'int64',
        'float64',
        'decimal',
        'date',
        'timestamp',
        'uid',
        'array',
      ].includes(input.type))
  )
    return fail();
  if (
    profile === 'column' &&
    (result.type === 'object' ||
      (result.type === 'array' &&
        (result.items!.nullable ||
          ['array', 'object', 'ontologyObject'].includes(result.items!.type))))
  )
    return fail();
  return result;
}

export function validDate(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value) || value.startsWith('0000')) return false;
  const date = new Date(`${value}T00:00:00.000Z`);
  return Number.isFinite(date.getTime()) && date.toISOString().slice(0, 10) === value;
}
export function timestampMicros(value: string): bigint {
  const match = /^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?Z$/.exec(value);
  if (
    !match ||
    !validDate(match[1]) ||
    Number(match[2]) > 23 ||
    Number(match[3]) > 59 ||
    Number(match[4]) > 59
  )
    throw new MoriaError(
      'VALUE_INVALID',
      'Expected a UTC timestamp with at most six fractional digits',
    );
  return (
    BigInt(Date.parse(`${match[1]}T${match[2]}:${match[3]}:${match[4]}Z`)) * 1000n +
    BigInt((match[5] ?? '').padEnd(6, '0'))
  );
}
function decode(
  value: unknown,
  type: ResolvedType,
  path: string,
  budget: { nodes: number; native?: boolean },
): JsonValue {
  const bad = (): never => {
    throw new MoriaError('VALUE_INVALID', `Invalid ${type.type} at ${path}`);
  };
  if (++budget.nodes > 100000) return bad();
  if (value === null) return type.nullable ? null : bad();
  if (type.type === 'ontologyObject') {
    if (budget.native && value && typeof value === 'object' && ONTOLOGY_OBJECT in value)
      value = (value as any)[ONTOLOGY_OBJECT];
    if (
      !plain(value) ||
      Object.keys(value).sort().join(',') !== 'objectType,uid' ||
      value.objectType !== type.objectType
    )
      return bad();
    return {
      objectType: type.objectType!,
      uid: decode(value.uid, { type: 'uid', nullable: false }, path + '.uid', budget) as string,
    };
  }
  if (type.type === 'object') {
    if (!plain(value) || Object.keys(value).some((key) => !Object.hasOwn(type.properties!, key)))
      return bad();
    const result: Record<string, JsonValue> = {};
    for (const [key, field] of Object.entries(type.properties!)) {
      if (!Object.hasOwn(value, key)) {
        if (type.optional?.includes(key)) continue;
        return bad();
      }
      result[key] = decode(value[key], field, `${path}.${key}`, budget);
    }
    return result;
  }
  if (type.type === 'array') {
    if (!Array.isArray(value) || value.length > 100000) return bad();
    const result: JsonValue[] = [];
    for (let i = 0; i < value.length; i++) {
      if (!Object.hasOwn(value, i)) return bad();
      result.push(decode(value[i], type.items!, `${path}[${i}]`, budget));
    }
    return result;
  }
  if (type.type === 'boolean') return typeof value === 'boolean' ? value : bad();
  if (type.type === 'float64')
    return typeof value === 'number' && Number.isFinite(value)
      ? Object.is(value, -0)
        ? 0
        : value
      : bad();
  if (typeof value !== 'string') return bad();
  switch (type.type) {
    case 'string':
      return value.isWellFormed() && !value.includes('\0') ? value : bad();
    case 'uid':
      return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value)
        ? value
        : bad();
    case 'int64':
      return value.length <= 20 &&
        /^-?(0|[1-9][0-9]*)$/.test(value) &&
        BigInt(value) >= -(2n ** 63n) &&
        BigInt(value) < 2n ** 63n
        ? value === '-0'
          ? '0'
          : value
        : bad();
    case 'decimal': {
      if (value.length > 80 || !/^-?(0|[1-9][0-9]*)(\.[0-9]+)?$/.test(value)) return bad();
      const [whole, fraction = ''] = value.replace(/^-/, '').split('.');
      const scale = type.scale!,
        precision = type.precision!;
      if (
        !Number.isInteger(scale) ||
        !Number.isInteger(precision) ||
        scale < 0 ||
        precision < 1 ||
        precision > 38 ||
        scale > precision ||
        fraction.length > scale ||
        whole.replace(/^0+/, '').length > precision - scale
      )
        return bad();
      const normalized = whole + (scale ? '.' + fraction.padEnd(scale, '0') : '');
      return value.startsWith('-') && /[1-9]/.test(normalized) ? '-' + normalized : normalized;
    }
    case 'date':
      return validDate(value) ? value : bad();
    case 'timestamp':
      timestampMicros(value);
      return value.slice(0, 19) + '.' + (value.match(/\.(\d+)Z$/)?.[1] ?? '').padEnd(6, '0') + 'Z';
    default:
      return bad();
  }
}
/** Canonical JSON wire values. Int64/decimal remain exact strings in TypeScript. */
export function decodeValue(
  value: unknown,
  type: TypeSchema,
  definitions: Record<string, TypeSchema> = {},
): JsonValue {
  return decode(value, normalizeType(type, definitions), 'value', { nodes: 0 });
}
export function encodeValue(
  value: unknown,
  type: TypeSchema,
  definitions: Record<string, TypeSchema> = {},
): JsonValue {
  return decode(value, normalizeType(type, definitions), 'value', { nodes: 0, native: true });
}

/** Bind only declared ontology values; ordinary records with similar fields remain records. */
export async function bindOntologyObjects(
  value: unknown,
  schema: TypeSchema,
  bind: (identity: ObjectIdentity) => unknown | Promise<unknown>,
  definitions: Record<string, TypeSchema> = {},
): Promise<any> {
  const type = normalizeType(schema, definitions);
  const wire = encodeValue(value, schema, definitions);
  async function visit(value: JsonValue, type: ResolvedType): Promise<any> {
    if (value === null) return null;
    if (type.type === 'ontologyObject') return bind(value as unknown as ObjectIdentity);
    if (type.type === 'array') {
      const result = [];
      for (const item of value as JsonValue[]) result.push(await visit(item, type.items!));
      return result;
    }
    if (type.type === 'object') {
      const result: Record<string, unknown> = {};
      for (const [key, item] of Object.entries(value as Record<string, JsonValue>))
        result[key] = await visit(item, type.properties![key]);
      return result;
    }
    return value;
  }
  return visit(wire, type);
}
export function validateValue(value: unknown, type: ValueType, path = 'value'): Value {
  return decode(value, normalizeType(type, {}, 'column'), path, { nodes: 0 }) as Value;
}
export function validateRow(
  row: unknown,
  columns: Record<string, ValueType>,
  profile?: 'column',
): Record<string, Value>;
export function validateRow(
  row: unknown,
  columns: Record<string, TypeSchema>,
  profile: 'value',
): Record<string, JsonValue>;
export function validateRow(
  row: unknown,
  columns: Record<string, TypeSchema>,
  profile: 'value' | 'column' = 'column',
): Record<string, JsonValue> {
  if (!plain(columns) || !['column', 'value'].includes(profile))
    throw new MoriaError('TYPE_INVALID', 'Invalid row schema');
  if (!plain(row)) throw new MoriaError('ROW_INVALID', 'Expected an object row');
  for (const key of Object.keys(row))
    if (!Object.hasOwn(columns, key)) throw new MoriaError('ROW_INVALID', `Unknown column ${key}`);
  const type = normalizeType({ type: 'object', properties: columns });
  if (profile === 'column')
    for (const field of Object.values(columns)) normalizeType(field, {}, 'column');
  const input = { ...row };
  for (const [key, field] of Object.entries(type.properties!))
    if (!Object.hasOwn(input, key) && field.nullable) input[key] = null;
  return decode(input, type, 'row', { nodes: 0, native: true }) as Record<string, JsonValue>;
}

/** Deterministic, strict JSON serialization shared by fingerprints and transports. */
export function canonicalJson(value: unknown): string {
  const active = new Set<object>();
  let nodes = 0;
  function visit(value: unknown, depth = 0): string {
    if (++nodes > 1000000 || depth > 64)
      throw new MoriaError('INVALID_JSON', 'JSON complexity exceeds bounds');
    if (typeof value === 'number' && !Number.isFinite(value))
      throw new MoriaError('INVALID_JSON', 'Non-finite number');
    if (value === null || typeof value !== 'object') {
      let text: string | undefined;
      try {
        text = JSON.stringify(value);
      } catch {
        throw new MoriaError('INVALID_JSON', 'Value is not JSON');
      }
      if (text === undefined) throw new MoriaError('INVALID_JSON', 'Value is not JSON');
      return text;
    }
    if (ONTOLOGY_OBJECT in value) {
      const identity = (value as any)[ONTOLOGY_OBJECT];
      const normalized = encodeValue(value, {
        type: 'ontologyObject',
        objectType: identity?.objectType,
        nullable: false,
      });
      return visit(normalized, depth + 1);
    }
    if (active.has(value) || Object.getOwnPropertySymbols(value).length)
      throw new MoriaError('INVALID_JSON', 'Expected an acyclic JSON value');
    active.add(value);
    try {
      if (Array.isArray(value)) {
        for (let i = 0; i < value.length; i++)
          if (!Object.hasOwn(value, i))
            throw new MoriaError('INVALID_JSON', 'Sparse arrays are not JSON values');
        return `[${value.map((item) => visit(item, depth + 1)).join(',')}]`;
      }
      if (!plain(value)) throw new MoriaError('INVALID_JSON', 'Expected a plain JSON object');
      return (
        '{' +
        Object.keys(value)
          .sort()
          .map((key) => JSON.stringify(key) + ':' + visit(value[key], depth + 1))
          .join(',') +
        '}'
      );
    } finally {
      active.delete(value);
    }
  }
  return visit(value);
}
/** Float bits avoid language-specific decimal/exponent printing in typed fingerprints. */
export function canonicalValue(
  value: unknown,
  schema: TypeSchema,
  definitions: Record<string, TypeSchema> = {},
): string {
  const type = normalizeType(schema, definitions);
  const normalized = decode(value, type, 'value', { nodes: 0 });
  function typed(value: JsonValue, type: ResolvedType): any {
    if (value === null) return null;
    if (type.type === 'float64') {
      const bytes = new ArrayBuffer(8);
      new DataView(bytes).setFloat64(0, value as number, false);
      return {
        $float64: [...new Uint8Array(bytes)]
          .map((byte) => byte.toString(16).padStart(2, '0'))
          .join(''),
      };
    }
    if (type.type === 'array')
      return (value as JsonValue[]).map((item) => typed(item, type.items!));
    if (type.type === 'object')
      return Object.fromEntries(
        Object.entries(value as Record<string, JsonValue>).map(([key, item]) => [
          key,
          typed(item, type.properties![key]),
        ]),
      );
    return value;
  }
  return canonicalJson({ version: VALUE_VERSION, schema: type, value: typed(normalized, type) });
}
