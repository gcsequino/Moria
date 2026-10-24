import { test } from 'node:test';
import assert from 'node:assert/strict';
import { compileExpression } from '../packages/authorization/src/expressions.js';
import {
  authorize,
  authorizeDestination,
  type Identity,
} from '../packages/authorization/src/access.js';
import type { Expr } from '../packages/contracts/types.js';
const str = { type: 'string' as const, nullable: true };
const region: Expr = { op: 'eq', args: [{ field: 'region' }, { principalAttribute: 'region' }] };
const types = { fields: { region: str }, attributes: { region: str } };
test('row policy denies absent attributes even underneath NOT', () => {
  assert.equal(compileExpression(region, types).allows({ fields: { region: 'East' } }), false);
  assert.equal(
    compileExpression({ op: 'not', args: [region] }, types).allows({ fields: { region: 'East' } }),
    false,
  );
});
test('SQL binds malicious strings rather than interpolating them', () => {
  const p = compileExpression(region, types);
  const value = "East'; DROP TABLE objects; --";
  const sql = p.sql({ attributes: { region: value } }, { region: 'region' });
  assert.ok(!sql.text.includes(value));
  assert.deepEqual(sql.values, [value]);
  assert.throws(() => p.sql({}, { region: 'x; drop table users' }));
});
test('SQL three-valued boolean operations agree with the reference truth tables', () => {
  for (const a of [true, false, null])
    for (const b of [true, false, null]) {
      const env = {
        attributes: {
          a: { type: 'boolean' as const, nullable: true },
          b: { type: 'boolean' as const, nullable: true },
        },
      };
      const args: Expr[] = [{ principalAttribute: 'a' }, { principalAttribute: 'b' }];
      assert.equal(
        compileExpression({ op: 'and', args }, env).evaluate({ attributes: { a, b } }),
        a === false || b === false ? false : a === null || b === null ? null : true,
      );
      assert.equal(
        compileExpression({ op: 'or', args }, env).evaluate({ attributes: { a, b } }),
        a === true || b === true ? true : a === null || b === null ? null : false,
      );
    }
});
test('int64 comparisons do not lose precision or sort lexically', () => {
  const t = { fields: { n: { type: 'int64' as const, nullable: false } } };
  const p = compileExpression(
    { op: 'gt', args: [{ field: 'n' }, { literal: '9007199254740992' }] },
    t,
  );
  assert.equal(p.allows({ fields: { n: '9007199254740993' } }), true);
  assert.equal(p.allows({ fields: { n: '10' } }), false);
});
test('Unicode ordering uses code points rather than UTF16 order', () => {
  const p = compileExpression(
    { op: 'gt', args: [{ field: 'region' }, { literal: '\uE000' }] },
    types,
  );
  assert.equal(p.allows({ fields: { region: '😀' } }), true);
});
test('invalid expression types, operators and complexity fail closed', () => {
  assert.throws(() => compileExpression({ op: 'eval', args: [{ literal: true }] }, {}));
  assert.throws(() => compileExpression({ field: 'x' }, {}));
  assert.throws(() =>
    compileExpression({ op: 'eq', args: [{ field: 'region' }, { literal: null }] }, types),
  );
  let e: Expr = { literal: true };
  for (let i = 0; i < 20; i++) e = { op: 'not', args: [e] };
  assert.throws(() => compileExpression(e, {}));
});
const identity = (): Identity => ({
  principal: 'user',
  application: 'app',
  environment: 'dev',
  grantedOperations: new Set(['objects:read']),
  applicationScopes: new Set(['objects:read']),
  markings: new Set(['A', 'B']),
});
test('grants, scopes and all markings are independently required', () => {
  const i = identity();
  authorize(i, 'objects', 'read', ['A', 'B']);
  i.markings.delete('B');
  assert.throws(() => authorize(i, 'objects', 'read', ['A', 'B']));
  i.markings.add('B');
  i.applicationScopes.clear();
  assert.throws(() => authorize(i, 'objects', 'read'));
});
test('delegated identity cannot exceed initiating user permissions', () => {
  const i = identity();
  i.delegated = identity();
  i.delegated.grantedOperations.clear();
  assert.throws(() => authorize(i, 'objects', 'read'));
});
test('model destination cannot drop one of the execution markings', () => {
  const i = identity();
  assert.throws(() =>
    authorizeDestination(
      i,
      new Set(['A', 'B']),
      { allowedDestinations: ['local'], allowedMarkings: ['A'] },
      'local',
    ),
  );
  authorizeDestination(
    i,
    new Set(['A']),
    { allowedDestinations: ['local'], allowedMarkings: ['A'] },
    'local',
  );
  assert.throws(() => authorizeDestination(i, new Set(), undefined, 'local'));
});
