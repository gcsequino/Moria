import type { JsonValue } from './types.js';

export interface AgentExecution {
  valueVersion: 1;
  id: string;
  resourceId: string;
  release: string;
  attemptId: string;
  correlationId: string;
  status: 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled';
  markings: { environment: string; marking: string }[];
  modelCalls: { id: string; status: number; usage: JsonValue | null; error: string | null }[];
  result?: Record<string, JsonValue>;
  error?: { code: string; message: string };
}

export interface AgentEvent {
  sequence: string;
  type: string;
  payload: JsonValue;
  markings: { environment: string; marking: string }[];
}
