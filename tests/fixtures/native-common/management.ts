import { build } from 'vite';
import configuration from '../../../apps/management/vite.config.js';
/** The acceptance instance hosts the same management bundle shipped to users. */
export async function buildNativeManagement(outDir: string) {
  await build({
    ...configuration,
    configFile: false,
    logLevel: 'silent',
    build: { outDir, emptyOutDir: true },
  });
}
