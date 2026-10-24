import test from 'node:test';
import assert from 'node:assert/strict';
import { publicHosting } from '../services/app-host/src/public-hosting.js';

test('public hosting is opt-in and rejects ambiguous origins and colliding listeners', () => {
  assert.deepEqual(publicHosting({}), []);
  assert.deepEqual(publicHosting({ MORIA_PUBLIC_ORIGIN: 'https://management.example.test' }), [
    { app: 'management', origin: 'https://management.example.test', port: 3212 },
  ]);
  for (const origin of [
    'http://example.test',
    'https://example.test/',
    'https://example.test/path',
    'https://user:password@example.test',
    'https://example.test?query=1',
  ])
    assert.throws(() => publicHosting({ MORIA_PUBLIC_ORIGIN: origin }));
  assert.throws(() =>
    publicHosting({ MORIA_PUBLIC_ORIGIN: 'https://example.test', MORIA_PUBLIC_PORT: '3210' }),
  );
  assert.throws(() =>
    publicHosting({
      MORIA_PUBLIC_ORIGIN: 'https://example.test',
      MORIA_SNOW_PUBLIC_ORIGIN: 'https://example.test',
    }),
  );
});
