export const VALUE_VERSION = 1;
export const ONTOLOGY_OBJECT = Symbol.for('moria.ontology-object.v1');
export interface ObjectIdentity {
  objectType: string;
  uid: string;
}
export type Scalar = string | number | boolean | null;
/** Values supported by dataset columns, object properties and policy expressions. */
export type Value = Scalar | Scalar[];
export type JsonValue = Scalar | JsonValue[] | { [key: string]: JsonValue };
export type ScalarType =
  | {
      type: 'string' | 'boolean' | 'int64' | 'float64' | 'date' | 'timestamp' | 'uid';
      nullable: boolean;
      precision?: never;
      scale?: never;
      items?: never;
    }
  | { type: 'decimal'; nullable: boolean; precision: number; scale: number; items?: never };
export type ValueType =
  | ScalarType
  | {
      type: 'array';
      nullable: boolean;
      items: ScalarType & { nullable: false };
      precision?: never;
      scale?: never;
    };
/** API/function envelopes share scalar semantics with stored values. */
export type TypeSchema =
  | ScalarType
  | { type: 'ontologyObject'; objectType: string; nullable: boolean }
  | { ref: string }
  | {
      type: 'object';
      properties: Record<string, TypeSchema>;
      optional?: string[];
      nullable?: boolean;
    }
  | { type: 'list'; items: TypeSchema; nullable?: boolean }
  | { type: 'array'; items: TypeSchema; nullable: boolean };
export interface ResolvedType {
  type: ValueType['type'] | 'object' | 'ontologyObject';
  objectType?: string;
  nullable: boolean;
  precision?: number;
  scale?: number;
  items?: ResolvedType;
  properties?: Record<string, ResolvedType>;
  optional?: string[];
}
