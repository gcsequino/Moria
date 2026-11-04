import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import { promisify } from 'node:util';
import test from 'node:test';
import { compileExpression } from '../packages/authorization/src/expressions.js';
import type { Expr } from '../packages/contracts/types.js';
import { database } from './fixtures/catalog-storage/database.js';

test(
  'native policy evaluation agrees with TypeScript and real PostgreSQL',
  { timeout: 120000 },
  async (t) => {
    const vectors = JSON.parse(
      await readFile(
        new URL('../packages/contracts/policy-conformance.json', import.meta.url),
        'utf8',
      ),
    );
    assert.equal(vectors.version, 1);
    assert(vectors.valid.length > 0);
    assert(vectors.invalid.length > 0);
    for (const item of vectors.valid) {
      assert.equal(
        compileExpression(item.request.expression as Expr, item.request.types).evaluate(
          item.request.values,
        ),
        item.result,
        `${item.name}: TypeScript reference`,
      );
    }
    const fixture = await database();
    t.after(fixture.close);
    const url = new URL(`postgresql://127.0.0.1:${fixture.pool.options.port}/postgres`);
    url.username = String(fixture.pool.options.user);
    assert.equal(typeof fixture.pool.options.password, 'string');
    url.password = fixture.pool.options.password as string;
    const { stdout } = await promisify(execFile)(
      process.env.MORIA_POLICY_TEST_BINARY ?? '/usr/local/bin/moria-policy-tests',
      ['--ignored', '--exact', 'sql_policy_parity', '--nocapture'],
      {
        env: {
          PATH: '/usr/local/bin:/usr/bin:/bin',
          MORIA_POLICY_TEST_DATABASE_URL: url.toString(),
        },
        timeout: 60000,
        maxBuffer: 1024 * 1024,
      },
    );
    assert.match(stdout, /test result: ok\. 1 passed; 0 failed; 0 ignored;/);
    assert(
      stdout.includes(
        `Verified ${vectors.valid.length} valid policies through native evaluation and real PostgreSQL; ${vectors.invalid.length} invalid policies rejected.`,
      ),
    );
  },
);
