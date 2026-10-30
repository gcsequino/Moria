import { Server } from 'node:net';
import { runProcess, type ProcessRole } from '../../../apps/local/src/runtime.js';
import { Jobs, type JobContext } from '../../../services/execution/src/jobs.js';
import { createWeatherService } from '../../../services/weather/src/service.js';

// Only this fixture delays listen; production entry points have no test controls.
const pausedListen = Number(process.argv[3] ?? 0);
if (pausedListen) {
  const listen = Server.prototype.listen;
  let calls = 0;
  Server.prototype.listen = function (this: Server, ...args: unknown[]) {
    calls += 1;
    process.send?.({ event: 'listen-called' });
    if (calls !== pausedListen) return Reflect.apply(listen, this, args);
    process.send?.({ event: 'listen-pending' });
    const resume = (message: { command: string }) => {
      if (message.command !== 'resume-listen') return;
      process.removeListener('message', resume);
      Reflect.apply(listen, this, args);
    };
    process.on('message', resume);
    return this;
  } as typeof listen;
  process.once('SIGTERM', () => process.send?.({ event: 'startup-signal' }));
}

class ObservedJobs extends Jobs {
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
const runtime = await runProcess(
  process.argv[2] as Exclude<ProcessRole, 'function-worker'>,
  async (catalog) => {
    const jobs = new ObservedJobs(catalog, { snotel: handler, weather: handler });
    await jobs.migrate();
    return { jobs, weather: await createWeatherService(catalog) };
  },
);
if (!runtime) {
  if (pausedListen && !process.exitCode) {
    process.disconnect?.();
  } else {
    process.exit(1);
  }
} else {
  process.on('message', async (message: { command: string }) => {
    if (message.command === 'release') release?.();
  });
  process.once('SIGTERM', async () => {
    process.send?.({ event: 'draining' });
    await runtime.close();
    process.disconnect?.();
  });
  process.send?.({ event: 'ready' });
}
