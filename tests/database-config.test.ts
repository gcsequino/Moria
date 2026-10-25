import { test } from 'node:test';
import assert from 'node:assert/strict';
import { externalDatabase, integer } from '../services/database/src/config.js';

test('external configuration validates secret references and pool budgets without disclosing values', () => {
  const env = {
    MORIA_DATABASE_SECRET_REF: 'TEST_DATABASE',
    TEST_DATABASE: 'postgresql://runtime:synthetic-only@localhost/moria',
  };
  const config = externalDatabase(env);
  assert.equal(config.user, 'runtime');
  assert.equal(config.max, 10);
  assert.deepEqual(config.ssl, { rejectUnauthorized: true });
  for (const value of [
    '',
    'not-a-url synthetic-only',
    env.TEST_DATABASE + '?sslkey=/forbidden',
    'http://runtime:synthetic-only@localhost/moria',
  ]) {
    assert.throws(
      () => externalDatabase({ ...env, TEST_DATABASE: value }),
      (error: unknown) => {
        assert.ok(error instanceof Error);
        assert.ok(!error.message.includes('synthetic-only'));
        assert.ok(!error.message.includes('/forbidden'));
        return true;
      },
    );
  }
  assert.throws(
    () => externalDatabase({ ...env, MORIA_DATABASE_REPLICAS: '2' }),
    /budget exceeded/,
  );
  assert.throws(() => externalDatabase({ ...env, MORIA_DATABASE_POOL_MAX: 'NaN' }), /integer/);
  assert.throws(() => externalDatabase(env, 'migration'), /MORIA_MIGRATION_SECRET_REF/);
  assert.throws(() => integer({ PORT: '1e4' }, 'PORT', 3210, 1024, 65535), /integer/);
});

test('external pool minimum reserves both bundled handler connection paths', () => {
  const env = {
    MORIA_DATABASE_SECRET_REF: 'TEST_DATABASE',
    MORIA_MIGRATION_SECRET_REF: 'TEST_DATABASE',
    TEST_DATABASE: 'postgresql://runtime:synthetic-only@localhost/moria',
    MORIA_DATABASE_POOL_MAX: '4',
    MORIA_DATABASE_CONNECTION_BUDGET: '5',
  };
  for (const max of ['0', '1', '2', '3']) {
    assert.throws(
      () => externalDatabase({ ...env, MORIA_DATABASE_POOL_MAX: max }),
      /MORIA_DATABASE_POOL_MAX must be an integer from 4 to 100/,
    );
  }
  assert.equal(externalDatabase(env).max, 4);
  assert.equal(externalDatabase(env, 'migration').max, 1);
  assert.throws(
    () => externalDatabase({ ...env, MORIA_DATABASE_CONNECTION_BUDGET: '4' }),
    /MORIA_DATABASE_CONNECTION_BUDGET must be an integer from 5/,
  );
  assert.throws(
    () => externalDatabase({ ...env, MORIA_DATABASE_REPLICAS: '2' }),
    /budget exceeded/,
  );
  assert.equal(
    externalDatabase({
      ...env,
      MORIA_DATABASE_REPLICAS: '2',
      MORIA_DATABASE_CONNECTION_BUDGET: '9',
    }).max,
    4,
  );
});

test('external URL hosts normalize IPv6 and preserve DNS, IPv4, ports and TLS defaults', () => {
  for (const [authority, host, port] of [
    ['[::1]', '::1', 5432],
    ['[::1]:5544', '::1', 5544],
    ['database.example:5545', 'database.example', 5545],
    ['127.0.0.1:5546', '127.0.0.1', 5546],
    ['localhost', 'localhost', 5432],
    ['127.0.0.1', '127.0.0.1', 5432],
  ] as const) {
    const config = externalDatabase({
      MORIA_DATABASE_SECRET_REF: 'TEST_DATABASE',
      TEST_DATABASE: `postgresql://runtime:synthetic-only@${authority}/moria`,
    });
    assert.equal(config.host, host);
    assert.equal(config.port, port);
    assert.deepEqual(config.ssl, { rejectUnauthorized: true });
  }
});
