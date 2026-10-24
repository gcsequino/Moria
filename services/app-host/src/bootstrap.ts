import { resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { rm, readFile, writeFile } from 'node:fs/promises';
import { AppHost } from './store.js';

export function localAppHost(directory = process.env.MORIA_LOCAL_DIR ?? '.local/workspace') {
  return new AppHost(resolve(directory, 'apps'));
}
export async function bootstrapManagement(host: AppHost, recover = false) {
  return bootstrapBundled(host, 'management', recover);
}
export async function bootstrapBundled(
  host: AppHost,
  app: 'management' | 'snotel',
  recover = false,
) {
  if (!recover) {
    try {
      const active = await host.active(app);
      await host.verify(app, active.current);
      return active;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    }
  }
  const { build } = await import('vite');
  const output = resolve(host.root, `.build-${randomUUID()}`);
  try {
    await build({
      configFile: resolve(`apps/${app}/vite.config.ts`),
      logLevel: 'silent',
      build: { outDir: output },
    });
    const contract = JSON.parse(
      await readFile(resolve(`apps/${app}/src/generated/contract.json`), 'utf8'),
    );
    await writeFile(
      resolve(output, 'moria-workflow.json'),
      JSON.stringify({
        version: 1,
        environment: app === 'management' ? 'local-workspace' : 'snotel',
        contract,
      }),
    );
    const artifact = await host.publish(app, output, recover);
    return await host.promote(app, artifact.version, recover);
  } finally {
    // output is a freshly generated direct child of the operator-selected host root.
    await rm(output, { recursive: true, force: true });
  }
}
