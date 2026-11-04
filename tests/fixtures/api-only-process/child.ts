import { registerHooks } from 'node:module';
process.env.MORIA_LOCAL_RUNTIME = 'legacy';

// Exercise real startup in an empty working directory. A UI build dependency
// must never be loaded, even if a future bootstrap stops writing artifacts.
registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier === 'vite' || specifier.includes('/vite/')) {
      throw new Error('API-only startup attempted to load the UI builder.');
    }
    return nextResolve(specifier, context);
  },
});

if (process.argv[2] === 'entrypoint') {
  await import('../../../apps/local/src/api.js');
} else {
  const { runProcess } = await import('../../../apps/local/src/runtime.js');
  await runProcess('api');
}
process.disconnect?.();
