import { MoriaError, type Value, type ValueType } from './types.js';

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
/** Strict wire values, normalized only where the value contract requires it. */
export function validateValue(value: unknown, type: ValueType, path = 'value'): Value {
  const bad = (): never => {
    throw new MoriaError('VALUE_INVALID', `Invalid ${type.type} at ${path}`);
  };
  if (value === null) return type.nullable ? null : bad();
  if (type.type === 'array') {
    if (!Array.isArray(value) || !type.items || type.items.type === 'array' || type.items.nullable)
      return bad();
    return value.map((v, i) => validateValue(v, type.items!, `${path}[${i}]`)) as Value;
  }
  if (type.type === 'boolean') return typeof value === 'boolean' ? value : bad();
  if (type.type === 'float64')
    return typeof value === 'number' && Number.isFinite(value) ? value : bad();
  if (typeof value !== 'string') return bad();
  switch (type.type) {
    case 'string':
      return value.isWellFormed() && !value.includes('\0') ? value : bad();
    case 'uid':
      return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value)
        ? value
        : bad();
    case 'int64':
      return /^-?(0|[1-9][0-9]*)$/.test(value) &&
        BigInt(value) >= -(2n ** 63n) &&
        BigInt(value) < 2n ** 63n
        ? value
        : bad();
    case 'decimal': {
      if (!/^-?(0|[1-9][0-9]*)(\.[0-9]+)?$/.test(value)) return bad();
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
      return value;
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
export function validateRow(
  row: unknown,
  columns: Record<string, ValueType>,
): Record<string, Value> {
  if (!row || typeof row !== 'object' || Array.isArray(row))
    throw new MoriaError('ROW_INVALID', 'Expected an object row');
  for (const key of Object.keys(row))
    if (!Object.hasOwn(columns, key)) throw new MoriaError('ROW_INVALID', `Unknown column ${key}`);
  return Object.fromEntries(
    Object.entries(columns).map(([key, type]) => [
      key,
      validateValue(Object.hasOwn(row, key) ? (row as any)[key] : null, type, key),
    ]),
  );
}
