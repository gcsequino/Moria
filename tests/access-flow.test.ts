import test from 'node:test';
import assert from 'node:assert/strict';
import { authorize, type Identity } from '../packages/authorization/src/access.js';
import { captureAccess, accessMarkings } from '../packages/authorization/src/flow.js';

test('access flow stays isolated across concurrent calls and retains authorized error inputs', async () => {
  const identity: Identity = {
    environment: 'env',
    principal: 'user',
    application: 'app',
    grantedOperations: new Set(['objects:read']),
    applicationScopes: new Set(['objects:read']),
    markings: new Set(['private']),
  };
  const [failed, other] = await Promise.all([
    captureAccess(async () => {
      authorize(identity, 'objects', 'read', ['private']);
      await Promise.resolve();
      throw undefined;
    }),
    captureAccess(async () => {
      await Promise.resolve();
      authorize(identity, 'objects', 'read');
      return accessMarkings();
    }),
  ]);
  assert.equal(failed.failed, true);
  assert.deepEqual(failed.markings, [{ environment: 'env', marking: 'private' }]);
  assert.deepEqual(failed.reads, [{ resource: 'objects', operation: 'read' }]);
  assert.deepEqual(other.value, []);
  assert.deepEqual(accessMarkings(), []);
  const denied = await captureAccess(async () =>
    authorize(identity, 'objects', 'write', ['private']),
  );
  assert.equal(denied.failed, true);
  assert.deepEqual(denied.markings, []);
});
