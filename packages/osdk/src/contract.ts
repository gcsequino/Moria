import type { ValueType } from '../../contracts/types.js';
export type ApiType =
  | ValueType
  | { ref: string }
  | { type: 'object'; properties: Record<string, ApiType>; optional?: string[]; nullable?: boolean }
  | { type: 'list'; items: ApiType; nullable?: boolean };
export interface AppContract {
  version: 1;
  app: string;
  types: Record<string, ApiType>;
  objects: Record<string, { type: string; properties: Record<string, ValueType> }>;
  actions: Record<string, { inputs: Record<string, ValueType> }>;
  endpoints: Record<
    string,
    { path: string; method: 'GET' | 'POST'; input?: ApiType; output: ApiType }
  >;
}
