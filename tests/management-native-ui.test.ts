import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { build, type Plugin } from 'vite';
import { resolve } from 'node:path';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { ResourceInspector } from '../apps/management/src/resources.js';

test('management active browser graph contains native clients and no retired workspace facade or server dependencies', async () => {
  const inputs: string[] = [];
  const graph: Plugin = {
    name: 'native-management-caller-audit',
    generateBundle() {
      inputs.push(...this.getModuleIds());
    },
  };
  await build({
    configFile: false,
    logLevel: 'silent',
    root: resolve('apps/management'),
    plugins: [graph],
    build: { write: false, minify: false },
  });
  assert(inputs.some((path) => path.endsWith('packages/platform/src/filesystem.ts')));
  assert(inputs.some((path) => path.endsWith('packages/platform/src/functions.ts')));
  assert(inputs.some((path) => path.endsWith('packages/platform/src/agents.ts')));
  assert(inputs.some((path) => path.endsWith('packages/platform/src/evaluations.ts')));
  assert(inputs.some((path) => path.endsWith('packages/platform/src/pipelines.ts')));
  assert(
    !inputs.some((path) =>
      /apps\/management\/src\/(?:client|platform|lineage|operational-health)\.tsx?$/.test(path),
    ),
  );
  assert(!inputs.some((path) => path.includes('apps/management/src/generated/')));
  assert(
    !inputs.some(
      (path) =>
        path.includes('services/catalog/') ||
        path.includes('services/ontology/') ||
        path.includes('services/execution/'),
    ),
  );
  const source = await readFile('apps/management/src/main.tsx', 'utf8');
  assert(!source.includes('endpoints.workspace'));
});
test('resource views route only the resolved domain and preserve exact-value guidance', () => {
  const resource = {
    apiVersion: 'moria/v1alpha2' as const,
    kind: 'ObjectType',
    metadata: { namespace: 'example', name: 'Record' },
    spec: {},
  };
  const objects = renderToStaticMarkup(
    createElement(ResourceInspector, {
      resourceId: '00000000-0000-4000-8000-000000000001',
      resource,
    }),
  );
  assert(objects.includes('Query objects / retry'));
  assert(objects.includes('exact JSON strings'));
  assert(!objects.includes('Invoke function'));
  const declaration = renderToStaticMarkup(
    createElement(ResourceInspector, {
      resourceId: '00000000-0000-4000-8000-000000000002',
      resource: { ...resource, kind: 'Grant' },
    }),
  );
  assert(!declaration.includes('Query objects / retry'));
  assert(!declaration.includes('Run / retry'));
  assert(declaration.includes('published definition'));
});
