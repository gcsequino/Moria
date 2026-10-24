import { defineConfig } from 'vite';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const contract = JSON.parse(
  readFileSync(new URL('./src/generated/contract.json', import.meta.url), 'utf8'),
);
export default defineConfig({
  root: fileURLToPath(new URL('.', import.meta.url)),
  base: './',
  esbuild: { jsx: 'automatic' },
  build: { outDir: 'dist', emptyOutDir: true },
  plugins: [
    {
      name: 'moria-workflow-declaration',
      generateBundle() {
        this.emitFile({
          type: 'asset',
          fileName: 'moria-workflow.json',
          source: JSON.stringify({ version: 1, environment: 'snotel', contract }),
        });
      },
    },
  ],
});
