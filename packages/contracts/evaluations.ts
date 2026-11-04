import type { JsonValue } from './types.js';
import type { AgentEvent } from './agents.js';

export interface EvaluationRun {
  valueVersion: 1;
  id: string;
  resourceId: string;
  release: string;
  status: 'queued' | 'running' | 'completed' | 'failed' | 'cancelled';
  loaded: boolean;
  caseCount: number | null;
  markings: { environment: string; marking: string }[];
  summary: EvaluationSummary | null;
  error: string | null;
}
export interface EvaluationSummary {
  total: string;
  passed: string;
  failed: string;
  errors: string;
  meanScore: number | null;
  scoreStandardDeviation: number | null;
}
export interface EvaluationCase {
  caseId: string;
  repeat: number;
  status: 'pending' | 'running' | 'pass' | 'fail' | 'error';
  agentExecution: string | null;
  evaluatorExecution: string | null;
  score: number | null;
  message: string | null;
  error: string | null;
  durationMillis: string | null;
  markings: { environment: string; marking: string }[];
}
export interface EvaluationComparison {
  comparable: boolean;
  reason: string | null;
  summaries: JsonValue | null;
  markings: { environment: string; marking: string }[];
}
export type EvaluationEvent = AgentEvent;
