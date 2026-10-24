import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { compileManifest, validateManifest, fingerprint } from '../packages/compiler/src/index.js';
import type { Manifest } from '../packages/contracts/types.js';
const fixture = JSON.parse(
  readFileSync(new URL('../examples/maintenance.json', import.meta.url), 'utf8'),
) as Manifest;
const change = (kind: string, fn: (r: any) => void) => {
  const m = structuredClone(fixture);
  fn(m.resources.find((r) => r.kind === kind));
  return m;
};
const context = {
  principalAttributes: JSON.parse(
    readFileSync(new URL('../examples/principal-attributes.json', import.meta.url), 'utf8'),
  ),
};
test('policy context validation binds fields, attributes and action parameters', () => {
  assert.equal(compileManifest(fixture, context).policyTypesChecked, true);
  assert.equal(compileManifest(fixture).policyTypesChecked, false);
  const missing = change('RestrictedView', () => {});
  missing.resources.find((r) => r.kind === 'Policy' && r.spec.type === 'row')!.spec.expression = {
    op: 'eq',
    args: [{ field: 'missing' }, { literal: 'East' }],
  };
  assert.ok(validateManifest(missing, context).some((d) => d.code === 'POLICY_INVALID'));
  const property = structuredClone(fixture);
  property.resources.find(
    (r) => r.kind === 'Policy' && r.spec.type === 'propertyRead',
  )!.spec.expression = { op: 'eq', args: [{ field: 'region' }, { literal: 'East' }] };
  assert.ok(validateManifest(property, context).some((d) => d.code === 'POLICY_INVALID'));
  assert.ok(
    validateManifest(fixture, { principalAttributes: {} }).some((d) => d.code === 'POLICY_INVALID'),
  );
  assert.ok(
    validateManifest(fixture, { principalAttributes: { bad: { type: 'arbitrary' } as any } }).some(
      (d) => d.code === 'ATTRIBUTE_SCHEMA_INVALID',
    ),
  );
});
test('all declared kinds compile with deterministic fingerprint', () => {
  assert.equal(compileManifest(fixture).order.length, 36);
  assert.equal(fingerprint({ b: 1, a: 2 }), fingerprint({ a: 2, b: 1 }));
});
test('restricted views are terminal transform inputs (SEC-03)', () => {
  const m = change(
    'Transform',
    (r) => (r.spec.inputs = ['maintenance/RestrictedView/RegionalOrders']),
  );
  assert.ok(validateManifest(m).some((d) => d.code === 'REFERENCE_KIND'));
});
test('a restricted view remains a legal object source', () =>
  assert.equal(validateManifest(fixture).length, 0));
test('transform cycles are rejected', () =>
  assert.ok(
    validateManifest(change('Transform', (r) => (r.spec.inputs = r.spec.outputs))).some(
      (d) => d.code === 'TRANSFORM_CYCLE',
    ),
  ));
test('duplicate definitions rejected', () => {
  const m = structuredClone(fixture);
  m.resources.push(m.resources[0]);
  assert.ok(validateManifest(m).some((d) => d.code === 'DUPLICATE_RESOURCE'));
});
test('schema fixtures fail strict validation', () => {
  const cases = JSON.parse(
    readFileSync(new URL('../docs/spec/examples/invalid-resources.json', import.meta.url), 'utf8'),
  );
  for (const c of cases) {
    const m = structuredClone(fixture);
    m.resources.push(c.resource);
    assert.ok(
      validateManifest(m).some((d) => d.code === 'SCHEMA_INVALID'),
      c.name,
    );
  }
});
test('no external dependency may be silently ignored', () => {
  const m = structuredClone(fixture);
  m.dependencies = [{ package: 'domain', digest: 'sha256:' + 'a'.repeat(64) }];
  assert.ok(validateManifest(m).some((d) => d.code === 'DEPENDENCY_RESOLUTION_REQUIRED'));
});
test('schema mapping and identity type mismatches rejected', () => {
  assert.ok(
    validateManifest(change('ObjectType', (r) => (r.spec.source.mapping.status = 'missing'))).some(
      (d) => d.code === 'MAPPING_INVALID',
    ),
  );
  assert.ok(
    validateManifest(change('ObjectType', (r) => (r.spec.properties.id.type = 'string'))).some(
      (d) => d.code === 'PRIMARY_KEY_INVALID',
    ),
  );
});
test('UID cannot be an action edit', () =>
  assert.ok(
    validateManifest(change('Action', (r) => (r.spec.edits[0].property = 'id'))).some(
      (d) => d.code === 'EDIT_INVALID',
    ),
  ));
