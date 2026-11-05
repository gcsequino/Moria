import assert from 'node:assert/strict';
import { spawn, execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createInterface } from 'node:readline';
import test from 'node:test';

test(
  'function runner uses the versioned pipe ABI and native TypeScript with OSDK transport',
  { timeout: 10000 },
  async (t) => {
    const directory = await mkdtemp(join(tmpdir(), 'function-runner-'));
    t.after(() => rm(directory, { recursive: true, force: true }));
    const file = join(directory, 'function.ts');
    await writeFile(
      file,
      `export default async (input:{value:number},context:any)=>{context.log('start');const response=await context.fetch('/api/v1/objects/records/record',{method:'GET'});return {value:input.value+(await response.json()).object.value};}`,
    );
    const runtime = join(directory, 'runtime');
    await promisify(execFile)(process.execPath, ['scripts/build-function-runtime.mjs', runtime]);
    const child = spawn(process.execPath, [join(runtime, 'runner.mjs'), file], {
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    t.after(() => {
      child.kill('SIGKILL');
    });
    const closed = new Promise<number | null>((resolve, reject) => {
      child.once('error', reject);
      child.once('close', resolve);
    });
    const messages: any[] = [];
    const nonce = '00000000-0000-4000-8000-000000000001';
    let errors = '';
    child.stderr.on('data', (chunk) => {
      errors += String(chunk);
    });
    createInterface({ input: child.stdout }).on('line', (line) => {
      const message = JSON.parse(line);
      messages.push(message);
      if (message.type === 'osdk')
        child.stdin.write(
          JSON.stringify({
            version: 1,
            type: 'osdkResult',
            id: message.id,
            value: { object: { value: 2 } },
          }) + '\n',
        );
      if (message.type === 'result')
        child.stdin.write(JSON.stringify({ version: 1, type: 'complete', nonce }) + '\n');
    });
    child.stdin.write(
      JSON.stringify({
        version: 1,
        type: 'start',
        valueVersion: 1,
        invocationId: 'test-invocation',
        input: { value: 40 },
        inputs: { value: { type: 'float64', nullable: false } },
        outputs: { value: { type: 'float64', nullable: false } },
      }) + '\n',
    );
    assert.equal(await closed, 0, errors);
    assert.deepEqual(
      messages.map((message) => message.type),
      ['log', 'osdk', 'result', 'completed'],
    );
    assert.ok(messages.every((message) => message.version === 1));
    assert.deepEqual(messages[2].value, { value: 42 });
    assert.deepEqual(messages[3], { version: 1, type: 'completed', nonce });
    assert.equal(messages[1].path, '/api/v1/objects/records/record');
    assert.equal(messages[1].input, null);
    await t.test(
      'completion requires an exact valid challenge before a flushed acknowledgement',
      async () => {
        const simple = join(directory, 'simple.ts');
        await writeFile(simple, 'export default () => ({value:1});');
        for (const completion of [
          { version: 1, type: 'complete' },
          { version: 1, type: 'complete', nonce: 'not-a-uuid' },
          { version: 1, type: 'complete', nonce: true },
          { version: 1, type: 'complete', nonce, extra: true },
        ]) {
          const invalid = spawn(process.execPath, [join(runtime, 'runner.mjs'), simple], {
            stdio: ['pipe', 'pipe', 'pipe'],
          });
          t.after(() => invalid.kill('SIGKILL'));
          const exit = new Promise<number | null>((resolve, reject) => {
            invalid.once('error', reject);
            invalid.once('close', resolve);
          });
          const output: any[] = [];
          invalid.stderr.resume();
          createInterface({ input: invalid.stdout }).on('line', (line) => {
            const message = JSON.parse(line);
            output.push(message);
            if (message.type === 'result') invalid.stdin.write(JSON.stringify(completion) + '\n');
          });
          invalid.stdin.write(
            JSON.stringify({
              version: 1,
              type: 'start',
              valueVersion: 1,
              invocationId: 'invalid-completion',
              input: {},
              inputs: {},
              outputs: { value: { type: 'float64', nullable: false } },
            }) + '\n',
          );
          assert.notEqual(await exit, 0);
          assert.equal(output.filter((message) => message.type === 'completed').length, 0);
        }
      },
    );
  },
);
