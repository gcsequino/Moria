import type { JsonValue } from './types.js';

export interface FunctionInvocation {
  valueVersion: 1;
  id: string;
  resourceId: string;
  release: string;
  artifact: { uri: string; digest: string };
  attemptId: string;
  correlationId: string;
  status: 'accepted' | 'running' | 'succeeded' | 'failed' | 'cancelled';
  effect: 'readOnly' | 'editProposal' | 'readWrite';
  createdAt: string;
  finishedAt: string | null;
  markings: { environment: string; marking: string }[];
  parentId?: string;
  prepareOnly?: boolean;
  proposalCommit?: { revision: string };
  calls?: {
    key: string;
    path: string;
    status: 'started' | 'succeeded' | 'failed' | 'proposed' | 'committed';
  }[];
  result?: Record<string, JsonValue>;
  error?: { code: string; message: string };
}

export interface FunctionEvent {
  sequence: string;
  type: 'state' | 'log';
  message: string;
  createdAt: string;
}
