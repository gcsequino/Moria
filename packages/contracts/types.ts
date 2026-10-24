export type Scalar = string | number | boolean | null;
export type Value = Scalar | Scalar[];
export type ValueType = {
  type:
    'string' | 'boolean' | 'int64' | 'float64' | 'decimal' | 'date' | 'timestamp' | 'uid' | 'array';
  nullable: boolean;
  precision?: number;
  scale?: number;
  items?: ValueType;
};
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
export class MoriaError extends Error {
  constructor(
    public code: string,
    message: string,
    public details: unknown = undefined,
  ) {
    super(message);
    this.name = 'MoriaError';
  }
}
export const resourceRef = (r: Resource): string =>
  `${r.metadata.namespace}/${r.kind}/${r.metadata.name}`;
