import { normalizeType } from './values.js';
import type { ValueType, ResolvedType } from './value-types.js';

/** Initial field IDs are deterministic; later Iceberg evolution must preserve persisted IDs. */
export function icebergSchema(columns: Record<string, ValueType>) {
  const properties = normalizeType({ type: 'object', properties: columns }).properties!;
  let next = 1;
  function physical(type: ResolvedType): any {
    if (type.type === 'array')
      return {
        type: 'list',
        'element-id': next++,
        'element-required': !type.items!.nullable,
        element: physical(type.items!),
      };
    if (type.type === 'decimal') return `decimal(${type.precision}, ${type.scale})`;
    return (
      {
        string: 'string',
        boolean: 'boolean',
        int64: 'long',
        float64: 'double',
        date: 'date',
        timestamp: 'timestamptz',
        uid: 'uuid',
      } as Record<string, string>
    )[type.type];
  }
  return {
    type: 'struct',
    'schema-id': 0,
    fields: Object.entries(properties).map(([name, type]) => {
      normalizeType(columns[name], {}, 'column');
      return { id: next++, name, required: !type.nullable, type: physical(type) };
    }),
  };
}
