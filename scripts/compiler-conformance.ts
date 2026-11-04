import { readFileSync, writeFileSync } from 'node:fs';
import { validateManifest } from '../packages/compiler/src/index.js';
import type { Manifest, Resource } from '../packages/contracts/types.js';
const manifest = JSON.parse(
  readFileSync(new URL('../examples/maintenance.json', import.meta.url), 'utf8'),
) as Manifest;
const attributes = JSON.parse(
  readFileSync(new URL('../examples/principal-attributes.json', import.meta.url), 'utf8'),
);
const cases: { name: string; manifest: Manifest; attributes: unknown; valid: boolean }[] = [];
function add(name: string, change?: (manifest: Manifest) => void, context: unknown = attributes) {
  const value = structuredClone(manifest);
  change?.(value);
  cases.push({
    name,
    manifest: value,
    attributes: context,
    valid: validateManifest(value, { principalAttributes: context as any }).length === 0,
  });
}
function resource(value: Manifest, kind: string): Resource {
  return value.resources.find((resource) => resource.kind === kind)!;
}
add('all36DeclaredResources');
add('unknownAttribute', undefined, {});
add('invalidUnusedAttributeType', undefined, { bad: { type: 'arbitrary' } });
add('viewCannotProduceTransformInput', (value) => {
  resource(value, 'Transform').spec.inputs = ['maintenance/RestrictedView/RegionalOrders'];
});
add('transformCycle', (value) => {
  const spec = resource(value, 'Transform').spec;
  spec.inputs = spec.outputs;
});
add('duplicateResource', (value) => {
  value.resources.push(value.resources[0]);
});
add('unresolvedExternalPackage', (value) => {
  value.dependencies = [{ package: 'unknown', digest: 'sha256:' + 'a'.repeat(64) }];
});
add('missingMappedColumn', (value) => {
  resource(value, 'ObjectType').spec.source.mapping.status = 'missing';
});
add('invalidObjectPrimaryKey', (value) => {
  resource(value, 'ObjectType').spec.properties.id.type = 'string';
});
add('uidCannotBeEdited', (value) => {
  resource(value, 'Action').spec.edits[0].property = 'id';
});
add('rowPolicyUnknownField', (value) => {
  value.resources.find(
    (resource) => resource.kind === 'Policy' && resource.spec.type === 'row',
  )!.spec.expression = { op: 'eq', args: [{ field: 'missing' }, { literal: 'West' }] };
});
add('propertyPolicyCannotReadRow', (value) => {
  value.resources.find(
    (resource) => resource.kind === 'Policy' && resource.spec.type === 'propertyRead',
  )!.spec.expression = { op: 'eq', args: [{ field: 'region' }, { literal: 'West' }] };
});
add('unknownMarking', (value) => {
  resource(value, 'ObjectType').metadata.markings = ['maintenance/Marking/missing'];
});
add('immutableModelWithoutVersion', (value) => {
  const spec = resource(value, 'Model').spec;
  spec.mutable = false;
  delete spec.versionLabel;
});
add('retrievalUnknownTextProperty', (value) => {
  resource(value, 'RetrievalCollection').spec.textProperty = 'missing';
});
add('retrievalOverlapExceedsSize', (value) => {
  const spec = resource(value, 'RetrievalCollection').spec;
  spec.chunkOverlap = spec.chunkSize;
});
add('applicationFrontendWrongIdentity', (value) => {
  const spec = resource(value, 'Application').spec;
  spec.mode = 'workload';
  spec.frontend = { baseUrl: 'https://example.test' };
});
add('globalReferenceWrongKind', (value) => {
  resource(value, 'Agent').spec.model = 'maintenance/Dataset/WorkOrders';
});
if (!cases[0].valid || cases.slice(1).some((value) => value.valid))
  throw new Error('Compiler conformance reference expectations drifted');
writeFileSync(
  new URL('../packages/contracts/compiler-conformance.json', import.meta.url),
  JSON.stringify({ version: 1, cases }, null, 2) + '\n',
);
