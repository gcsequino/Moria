import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';

import { agentImage } from '../agents/image.js';
import { instanceAgentProgram, instanceEvaluatorProgram } from './logic.js';

const execute = promisify(execFile);

export async function instanceLogicImages() {
  const image = await agentImage({ programSource: instanceAgentProgram });
  const directory = await mkdtemp(join(tmpdir(), 'moria-instance-evaluator-'));
  const repository = image.artifact.uri.slice(6).replace(/\/[^/]+$/, '/instance-evaluator');
  const close = async () => {
    await execute('docker', ['image', 'rm', '--force', repository]).catch(() => {});
    await image.close();
    await rm(directory, { recursive: true, force: true });
  };
  try {
    await writeFile(join(directory, 'evaluator.mjs'), instanceEvaluatorProgram);
    await writeFile(
      join(directory, 'Dockerfile'),
      `FROM ${image.child.uri.slice(6)}@${image.child.digest}\nCOPY evaluator.mjs /evaluator.mjs\nCMD ["node","/function/runtime/runner.mjs","/evaluator.mjs"]\n`,
    );
    await execute(
      'docker',
      ['build', '--network=none', '--pull=false', '--tag', repository, directory],
      {
        timeout: 180000,
        maxBuffer: 1048576,
      },
    );
    const pushed = await execute('docker', ['push', repository], { timeout: 180000 });
    const digest = pushed.stdout.match(/digest: (sha256:[0-9a-f]{64})/)?.[1];
    assert.ok(digest);
    return {
      agent: image.artifact,
      adapter: image.adapter,
      evaluator: { uri: `oci://${repository}`, digest },
      agentSource: image.programSource,
      evaluatorSource: instanceEvaluatorProgram,
      close,
    };
  } catch (error) {
    await close();
    throw error;
  }
}
