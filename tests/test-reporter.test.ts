import assert from 'node:assert/strict';
import test from 'node:test';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import type { TestEvent } from 'node:test/reporters';
import { requireTestCases } from '../scripts/test-reporter.js';

test('the reporter preserves ordinary test events', async () => {
  const event = { type: 'test:pass', data: { name: 'a real case' } } as TestEvent;
  async function* source() {
    yield event;
  }
  const received = [];
  for await (const value of requireTestCases(source())) received.push(value);
  assert.deepEqual(received, [event]);
});

test('the real test command rejects an early zero exit and accepts executed cases', async () => {
  const run = (name: string) =>
    promisify(execFile)(
      process.execPath,
      [
        '--import',
        'tsx',
        '--test',
        '--test-reporter=./scripts/test-reporter.ts',
        `tests/fixtures/test-reporter/${name}.test.ts`,
      ],
      { env: { ...process.env, NODE_TEST_CONTEXT: undefined } },
    );
  await assert.rejects(run('early-exit'), (error: unknown) => {
    const failure = error as { code: number; stderr: string };
    return (
      failure.code !== 0 && failure.stderr.includes('Test module exited without reporting cases')
    );
  });
  assert.match((await run('passing')).stdout, /reporter fixture executes its case/);
});
