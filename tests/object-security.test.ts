import { test } from 'node:test';
import assert from 'node:assert/strict';
import { objectReader, type ObjectPolicies } from '../packages/authorization/src/objects.js';
import type { Identity } from '../packages/authorization/src/access.js';

const type = { type: 'string', nullable: false } as const;
const policy: ObjectPolicies = {
  resource: 'orders',
  markings: ['sensitive'],
  properties: { uid: type, region: type, note: type },
  sourceColumns: { region: type },
  attributes: { region: type, auditor: { type: 'boolean', nullable: false } },
  row: { op: 'eq', args: [{ field: 'region' }, { principalAttribute: 'region' }] },
  propertyRead: { note: { principalAttribute: 'auditor' } },
};
const identity: Identity = {
  principal: 'east',
  application: 'queue',
  environment: 'dev',
  grantedOperations: new Set(['orders:read']),
  applicationScopes: new Set(['orders:read']),
  markings: new Set(['sensitive']),
};
const east = {
  source: { region: 'East' },
  effective: { uid: '1', region: 'East', note: 'secret' },
};
const west = {
  source: { region: 'West' },
  effective: { uid: '1', region: 'East', note: 'secret' },
};
test('restricted canonical rows survive editable overlays and pinned reads recheck current visibility', () => {
  const read = objectReader(policy).prepare(identity, { region: 'East', auditor: false });
  assert.deepEqual(read(east, east), { uid: '1', region: 'East' });
  assert.equal(read(east, west), null);
  assert.equal(read(west, east), null);
  assert.equal(read(east, null), null);
});
test('property denial is checked before any rows and property policies cannot inspect row values', () => {
  const reader = objectReader(policy);
  assert.throws(() => reader.prepare(identity, { region: 'East', auditor: false }, ['note']), {
    code: 'PROPERTY_FORBIDDEN',
  });
  assert.throws(
    () => reader.prepare(identity, { region: 'East', auditor: false }, undefined, ['note']),
    { code: 'PROPERTY_FORBIDDEN' },
  );
  assert.throws(
    () =>
      objectReader({
        ...policy,
        propertyRead: { note: { op: 'eq', args: [{ field: 'region' }, { literal: 'East' }] } },
      }),
    { code: 'POLICY_INVALID' },
  );
});
test('delegated object reads require all current attributes and intersect policy results', () => {
  const reader = objectReader(policy),
    delegated = { ...identity, delegated: { ...identity, principal: 'west' } };
  assert.throws(() => reader.prepare(delegated, { region: 'East', auditor: true }), {
    code: 'FORBIDDEN',
  });
  const read = reader.prepare(
    delegated,
    { region: 'East', auditor: true },
    undefined,
    [],
    [{ region: 'West', auditor: false }],
  );
  assert.equal(read(east, east), null);
});
