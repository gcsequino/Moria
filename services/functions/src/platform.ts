import type { FilesystemContext } from '../../security/src/filesystem.js';
import type { ObjectService, ActionBinding } from '../../ontology/src/service.js';

export interface FunctionRequest {
  path: string;
  method: string;
  headers: Record<string, string>;
  body: string | null;
}
export interface FunctionResponse {
  status: number;
  headers: Record<string, string>;
  body: string;
}
export interface FunctionBindings {
  actions?: ReadonlyMap<string, { service: ObjectService; action: ActionBinding }>;
  filesystem?: (
    context: FilesystemContext,
    request: FunctionRequest,
    signal: AbortSignal,
  ) => Promise<FunctionResponse>;
}
