import { normalizeType } from './values.js';
import type { TypeSchema, ResolvedType, ValueType } from './value-types.js';

/** Whether every valid source wire value can be decoded by the target schema. */
export function acceptsType(
  source: TypeSchema,
  target: TypeSchema,
  sourceDefinitions: Record<string, TypeSchema> = {},
  targetDefinitions: Record<string, TypeSchema> = sourceDefinitions,
): boolean {
  function accepts(source: ResolvedType, target: ResolvedType): boolean {
    if (source.type !== target.type || (source.nullable && !target.nullable)) return false;
    if (source.type === 'ontologyObject') return source.objectType === target.objectType;
    if (source.type === 'decimal')
      return source.scale === target.scale && source.precision! <= target.precision!;
    if (source.type === 'array') return accepts(source.items!, target.items!);
    if (source.type === 'object') {
      for (const [key, type] of Object.entries(source.properties!)) {
        if (!Object.hasOwn(target.properties!, key) || !accepts(type, target.properties![key]))
          return false;
        if (source.optional?.includes(key) && !target.optional?.includes(key)) return false;
      }
      return Object.keys(target.properties!).every(
        (key) => Object.hasOwn(source.properties!, key) || target.optional?.includes(key),
      );
    }
    return true;
  }
  return accepts(
    normalizeType(source, sourceDefinitions),
    normalizeType(target, targetDefinitions),
  );
}

/** Column additions can fill nulls; destructive/type changes require an explicit migration. */
export function compareColumns(
  before: Record<string, ValueType>,
  after: Record<string, ValueType>,
) {
  const changes: {
    field: string;
    kind: 'addNullable' | 'addRequired' | 'remove' | 'typeChange';
    migration: boolean;
  }[] = [];
  for (const field of [...new Set([...Object.keys(before), ...Object.keys(after)])].sort()) {
    const old = Object.hasOwn(before, field)
      ? normalizeType(before[field], {}, 'column')
      : undefined;
    const current = Object.hasOwn(after, field)
      ? normalizeType(after[field], {}, 'column')
      : undefined;
    if (!old && current)
      changes.push({
        field,
        kind: current.nullable ? 'addNullable' : 'addRequired',
        migration: !current.nullable,
      });
    else if (!current) changes.push({ field, kind: 'remove', migration: true });
    else if (JSON.stringify(old) !== JSON.stringify(current))
      changes.push({ field, kind: 'typeChange', migration: true });
  }
  return { compatible: changes.every((change) => !change.migration), changes };
}
