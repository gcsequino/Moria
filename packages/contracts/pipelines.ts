import type { JsonValue } from './types.js';
import type { ValueType } from './value-types.js';

export interface PipelineExecution<Result = JsonValue> {
  valueVersion: 1;
  id: string;
  resourceId: string;
  release: string;
  status: 'queued' | 'running' | 'preparing' | 'succeeded' | 'failed' | 'cancelled';
  result: Result | null;
  error: string | null;
  markings: { environment: string; marking: string }[];
}

export interface PipelineEvent {
  sequence: string;
  type: string;
  payload: JsonValue;
  markings: { environment: string; marking: string }[];
}

export interface ConnectorSchemaProposal {
  version: 1;
  stream: string;
  columns: Record<string, ValueType>;
  primaryKey: string[];
}

export interface ConnectorDiscoveryResult {
  schemaDiscovery: ConnectorSchemaProposal;
}
