import { runProcess } from '../../../apps/local/src/runtime.js';

const runtime = await runProcess('api');
if (!runtime) {
  process.exitCode = 1;
  process.disconnect?.();
} else {
  process.send?.('ready');
  process.once('SIGTERM', async () => {
    await runtime.close();
    process.disconnect?.();
  });
}
