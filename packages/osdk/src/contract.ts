import type { ValueType, TypeSchema } from '../../contracts/types.js';
export type ApiType = TypeSchema;
export interface AppContract {
  version: 1;
  app: string;
  types: Record<string, ApiType>;
  objects: Record<string, { id: string; type: string; properties: Record<string, ValueType> }>;
  actions: Record<string, { id?: string; inputs: Record<string, ValueType> }>;
  endpoints: Record<
    string,
    { path: string; method: 'GET' | 'POST'; input?: ApiType; output: ApiType }
  >;
}
