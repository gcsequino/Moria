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
        child.stdin.write(JSON.stringify({ version: 1, type: 'complete' }) + '\n');
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
      ['log', 'osdk', 'result'],
    );
    assert.ok(messages.every((message) => message.version === 1));
    assert.deepEqual(messages.at(-1).value, { value: 42 });
    assert.equal(messages[1].path, '/api/v1/objects/records/record');
    assert.equal(messages[1].input, null);
  },
);
