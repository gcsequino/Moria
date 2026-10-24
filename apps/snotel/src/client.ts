import { createClient } from './generated/index.js';
import type { Observation } from './generated/index.js';
export type { Station, Observation, Status } from './generated/index.js';
export const sdk = createClient({
  onUnauthenticated: () => window.dispatchEvent(new Event('moria:unauthenticated')),
});
export function segments(rows: Observation[], field: 'snowDepth' | 'swe'): Observation[][] {
  const result: Observation[][] = [];
  let current: Observation[] = [];
  for (const row of rows) {
    if (row[field] === null) {
      if (current.length) result.push(current);
      current = [];
    } else current.push(row);
  }
  if (current.length) result.push(current);
  return result;
}
