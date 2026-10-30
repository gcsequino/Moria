import type { Catalog } from '../../catalog/src/catalog.js';
import { Jobs, type JobHandler } from './jobs.js';

/** Trusted in-process composition only; callers cannot upload handlers through the API. */
export async function createPlatformJobs(
  catalog: Catalog,
  handlers: Record<string, JobHandler> = {},
) {
  const jobs = new Jobs(catalog, handlers);
  await jobs.migrate();
  return { jobs };
}

export type ExecutionFactory = (catalog: Catalog) => Promise<{ jobs: Jobs }>;
