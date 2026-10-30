import type { Value } from './value-types.js';
export type {
  Scalar,
  ScalarType,
  ObjectIdentity,
  Value,
  ValueType,
  JsonValue,
  TypeSchema,
  ResolvedType,
} from './value-types.js';
export { VALUE_VERSION, ONTOLOGY_OBJECT } from './value-types.js';
export { MoriaError } from './errors.js';
export type Expr =
  | { literal: Value }
  | { field: string }
  | { parameter: string }
  | { principalAttribute: string }
  | { op: string; args: Expr[] };
export interface Resource {
  apiVersion: 'moria/v1alpha2';
  kind: string;
  metadata: { namespace: string; name: string; markings?: string[] };
  // This is the JSON boundary. The compiler validates shape before interpreting spec.
  spec: Record<string, any>;
}
export interface Manifest {
  apiVersion: 'moria/v1alpha2';
  projectId: string;
  deploymentUnitId: string;
  resources: Resource[];
  dependencies?: { package: string; digest: string }[];
}
export interface Diagnostic {
  code: string;
  path: string;
  message: string;
}
export const resourceRef = (r: Resource): string =>
  `${r.metadata.namespace}/${r.kind}/${r.metadata.name}`;
