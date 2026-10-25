import pg from 'pg';
import { Server } from 'node:net';
import { runProcess, type ProcessRole } from '../../../apps/local/src/runtime.js';
import { Jobs, type JobContext } from '../../../services/execution/src/jobs.js';

// Intercept only the dispatch boundary, leaving all real SQL and transactions intact.
let resumeAdvance: (() => void) | undefined;
let pauseAdvance = process.argv[3] === 'pause';
const query = pg.Client.prototype.query;
pg.Client.prototype.query = function (this: pg.Client, ...args: unknown[]) {
  if (
    pauseAdvance &&
    typeof args[0] === 'string' &&
    args[0].startsWith('UPDATE moria.job_schedules SET next_run=')
  ) {
    pauseAdvance = false;
    return new Promise<void>((resolve) => {
      resumeAdvance = resolve;
      process.send?.({ event: 'before-advance' });
    }).then(() => Reflect.apply(query, this, args));
  }
  return Reflect.apply(query, this, args);
} as typeof query;
process.on('message', (message: { command: string }) => {
  if (message.command === 'advance') resumeAdvance?.();
});

class ObservedJobs extends Jobs {
  override async scheduleTick() {
    await super.scheduleTick();
    process.send?.({ event: 'schedule-tick' });
  }
  override start(role: 'combined' | 'scheduler' | 'worker' = 'combined') {
    process.send?.({ event: 'jobs-start' });
    super.start(role);
  }
  override async stop() {
    process.send?.({ event: 'jobs-stop' });
    await super.stop();
  }
  override async workerTick() {
    await super.workerTick();
    process.send?.({ event: 'tick' });
  }
}

let release: (() => void) | undefined;
const handler = async (job: JobContext) => {
  await new Promise<void>((resolve) => {
    release = resolve;
    process.send?.({ event: 'started', id: job.id });
  });
  try {
    // Publication and acknowledgment share the real fenced transaction.
    await runtime!.jobs.catalog.transaction(async (client) => {
      await job.guard(client);
      await client.query('INSERT INTO public.process_publications(id) VALUES($1)', [job.id]);
      await job.complete(client, { synthetic: true });
    });
    process.send?.({ event: 'published', id: job.id });
  } catch (error) {
    process.send?.({ event: 'rejected', id: job.id });
    throw error;
  }
};
const runtime = await runProcess(process.argv[2] as ProcessRole, async (catalog) => {
  const jobs = new ObservedJobs(catalog, { sample: handler, periodic: handler });
  await jobs.migrate();
  return { jobs };
});
if (!runtime) {
  process.exit(1);
} else {
  process.on('message', async (message: { command: string }) => {
    if (message.command === 'release') release?.();
    if (message.command === 'configure') {
      await runtime.jobs.schedule('periodic', false, 120);
      process.send?.({ event: 'configured' });
    }
  });
  process.once('SIGTERM', async () => {
    process.send?.({ event: 'draining' });
    await runtime.close();
    process.disconnect?.();
  });
  process.send?.({ event: 'ready' });
}
