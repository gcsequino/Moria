import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { createContext, runInContext } from 'node:vm';
import { webcrypto } from 'node:crypto';
import { build } from 'vite';
import { compileManifest } from '../packages/compiler/src/index.js';
import { compileSource } from '../packages/compiler/src/source.js';

test('production browser compiler bundle agrees with Node with dynamic code generation disabled', async () => {
  const result = await build({
    configFile: false,
    logLevel: 'silent',
    build: {
      write: false,
      minify: true,
      lib: {
        entry: resolve('packages/compiler/src/browser.ts'),
        name: 'MoriaCompiler',
        formats: ['iife'],
      },
    },
  });
  const output = Array.isArray(result) ? result[0] : result;
  assert('output' in output);
  const chunk = output.output.find((value) => value.type === 'chunk');
  assert(chunk && chunk.type === 'chunk');
  const context = createContext(
    { crypto: webcrypto, TextEncoder, TextDecoder },
    { codeGeneration: { strings: false, wasm: false } },
  );
  runInContext(chunk.code, context);
  assert.throws(() => runInContext('new Function("return 1")()', context), /Code generation/);
  const fixture = JSON.parse(readFileSync('packages/contracts/compiler-conformance.json', 'utf8'));
  const browserCall = (method: string, ...args: unknown[]) => {
    context.serialized = JSON.stringify(args);
    return runInContext(`MoriaCompiler.${method}(...JSON.parse(serialized))`, context);
  };
  for (const item of fixture.cases) {
    const options = { principalAttributes: item.attributes };
    if (item.valid) {
      const browser = await browserCall('compileManifest', item.manifest, options);
      assert.deepEqual(
        JSON.parse(JSON.stringify(browser)),
        compileManifest(item.manifest, options),
        item.name,
      );
    } else
      await assert.rejects(
        browserCall('compileManifest', item.manifest, options),
        { code: 'VALIDATION_FAILED' },
        item.name,
      );
  }
  const source = {
    kind: 'ontology' as const,
    projectId: fixture.cases[0].manifest.projectId,
    deploymentUnitId: fixture.cases[0].manifest.deploymentUnitId,
    files: [
      {
        path: '.ontology.json',
        text: JSON.stringify({ version: 1, name: 'Unicode source 𠮷', namespace: 'Example' }),
      },
    ],
  };
  assert.deepEqual(
    JSON.parse(JSON.stringify(await browserCall('compileSource', source))),
    compileSource(source),
  );
});
