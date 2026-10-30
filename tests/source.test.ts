import test from 'node:test';
import assert from 'node:assert/strict';
import { compileSource, sourceDescriptor } from '../packages/compiler/src/source.js';
import type { Resource } from '../packages/contracts/types.js';

const projectId = '22222222-2222-4222-8222-222222222222',
  deploymentUnitId = '33333333-3333-4333-8333-333333333333';
const uid = { type: 'uid', nullable: false };
const dataset: Resource = {
  apiVersion: 'moria/v1alpha2',
  kind: 'Dataset',
  metadata: { namespace: 'sample', name: 'stations' },
  spec: { columns: { id: uid }, writeModes: ['replace'] },
};
const object: Resource = {
  apiVersion: 'moria/v1alpha2',
  kind: 'ObjectType',
  metadata: { namespace: 'sample', name: 'Station' },
  spec: {
    primaryKey: 'id',
    properties: { id: uid },
    source: { resource: 'sample/Dataset/stations', mapping: { id: 'id' } },
  },
};
const input = {
  kind: 'ontology' as const,
  projectId,
  deploymentUnitId,
  files: [
    {
      path: '.ontology.json',
      text: JSON.stringify({ version: 1, name: 'Sample', namespace: 'sample' }),
    },
    { path: 'datasets/stations.resource.json', text: JSON.stringify(dataset) },
    { path: 'objects/station.object.json', text: JSON.stringify(object) },
  ],
};
test('ontology directory assembly reuses the semantic compiler and preserves separate namespaces', () => {
  const result = compileSource(input);
  assert.deepEqual(result.owned, [
    '_project/Namespace/sample',
    'sample/Dataset/stations',
    'sample/ObjectType/Station',
  ]);
  assert.equal(result.compiled.manifest.projectId, projectId);
  const second = structuredClone(input);
  second.files = second.files.map((file) => ({
    ...file,
    text: file.text.replaceAll('sample', 'another'),
  }));
  assert.ok(compileSource(second).owned.includes('another/ObjectType/Station'));
});
test('source assembly rejects missing/nested roots, wrong declaration placement and missing dependencies', () => {
  assert.throws(() => compileSource({ ...input, files: input.files.slice(1) }), {
    code: 'SOURCE_INVALID',
  });
  assert.throws(
    () =>
      compileSource({
        ...input,
        files: [...input.files, { path: 'nested/.ontology.json', text: '{}' }],
      }),
    { code: 'SOURCE_INVALID' },
  );
  assert.throws(
    () =>
      compileSource({
        ...input,
        files: input.files.map((file) => ({
          ...file,
          path: file.path.replace('.object.json', '.resource.json'),
        })),
      }),
    { code: 'SOURCE_INVALID' },
  );
  assert.throws(
    () =>
      compileSource({
        ...input,
        files: input.files.filter((file) => !file.path.includes('stations.resource')),
      }),
    { code: 'VALIDATION_FAILED' },
  );
  assert.throws(
    () => compileSource({ ...input, files: [...input.files, { path: '../escape', text: '{}' }] }),
    { code: 'SOURCE_INVALID' },
  );
  assert.throws(
    () =>
      sourceDescriptor(
        'repository',
        '{"version":1,"name":"Spark","namespace":"spark","runtime":"scala-spark"}',
      ),
    { code: 'SOURCE_INVALID' },
  );
});
test('repository export compilation validates existing runtime declarations without executing code', () => {
  const resource: Resource = {
    apiVersion: 'moria/v1alpha2',
    kind: 'Function',
    metadata: { namespace: 'logic', name: 'calculate' },
    spec: {
      artifact: { uri: 'oci://local/functions', digest: `sha256:${'a'.repeat(64)}` },
      runtime: 'typescript-v1',
      inputs: { value: { type: 'string', nullable: false } },
      outputs: { value: { type: 'string', nullable: false } },
      effect: 'readOnly',
      limits: { timeoutSeconds: 30, memoryMiB: 512, cpuMillis: 1000 },
    },
  };
  const repository = {
    kind: 'repository' as const,
    projectId,
    deploymentUnitId,
    files: [
      {
        path: '.repository.json',
        text: JSON.stringify({
          version: 1,
          name: 'Logic',
          namespace: 'logic',
          runtime: 'typescript-v1',
        }),
      },
      { path: 'src/main.ts', text: 'throw new Error("must never execute");' },
    ],
    exports: [resource],
  };
  const result = compileSource(repository);
  assert.ok(result.owned.includes('logic/Function/calculate'));
  const connector: Resource = {
    apiVersion: 'moria/v1alpha2',
    kind: 'ConnectorImplementation',
    metadata: { namespace: 'logic', name: 'connector' },
    spec: {
      artifact: resource.spec.artifact,
      abiVersion: 'connector/v1',
      capabilities: ['batchRead'],
      configurationSchema: { type: 'object' },
    },
  };
  assert.ok(
    compileSource({ ...repository, exports: [connector] }).owned.includes(
      'logic/ConnectorImplementation/connector',
    ),
  );
  assert.throws(() => compileSource({ ...repository, exports: [null as unknown as Resource] }), {
    code: 'SOURCE_INVALID',
  });
  assert.throws(
    () =>
      compileSource({
        ...repository,
        exports: [],
        files: [
          ...repository.files,
          { path: 'connector.resource.json', text: JSON.stringify(connector) },
        ],
      }),
    { code: 'SOURCE_INVALID' },
  );
  assert.throws(
    () => compileSource({ ...repository, exports: [{ ...resource, kind: 'ObjectType' }] }),
    { code: 'SOURCE_INVALID' },
  );
});
