import type { JsonValue } from './types.js';

export interface PipelineExecution {
  valueVersion: 1;
  id: string;
  resourceId: string;
  release: string;
  status: 'queued' | 'running' | 'preparing' | 'succeeded' | 'failed' | 'cancelled';
  result: JsonValue | null;
  error: string | null;
  markings: { environment: string; marking: string }[];
}

export interface PipelineEvent {
  sequence: string;
  type: string;
  payload: JsonValue;
  markings: { environment: string; marking: string }[];
}
