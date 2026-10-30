import type { ValueType, TypeSchema } from '../packages/contracts/value-types.js';

const decimal: ValueType = { type: 'decimal', nullable: false, precision: 8, scale: 2 };
const stored: ValueType = {
  type: 'array',
  nullable: true,
  items: { type: 'uid', nullable: false },
};
const envelope: TypeSchema = {
  type: 'list',
  items: { type: 'object', properties: { count: { type: 'int64', nullable: true } } },
};
// @ts-expect-error decimal metadata is required
const incomplete: ValueType = { type: 'decimal', nullable: false };
// @ts-expect-error metadata for another scalar is not allowed
const extra: ValueType = { type: 'string', nullable: false, scale: 2 };
const nullableItems: ValueType = {
  type: 'array',
  nullable: false,
  // @ts-expect-error stored arrays cannot contain nullable elements
  items: { type: 'string', nullable: true },
};
const nested: ValueType = {
  type: 'array',
  nullable: false,
  // @ts-expect-error stored arrays cannot contain records
  items: { type: 'object', properties: {} },
};
// @ts-expect-error array descriptors explicitly declare nullability
const missingNullability: TypeSchema = {
  type: 'array',
  items: { type: 'string', nullable: false },
};
void [decimal, stored, envelope, incomplete, extra, nullableItems, nested, missingNullability];
