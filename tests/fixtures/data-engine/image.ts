import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { copyFile, cp, mkdtemp, rm, writeFile, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { functionImage } from '../functions/image.js';

const execute = promisify(execFile);
export async function engineImage(program: string) {
  const registry = await functionImage();
  const root = await mkdtemp(join(tmpdir(), 'moria-engine-image-'));
  const repositories: string[] = [];
  const docker = async (...args: string[]) =>
    (await execute('docker', args, { timeout: 180000, maxBuffer: 1048576 })).stdout;
  const close = async () => {
    for (const name of repositories) await docker('image', 'rm', '--force', name).catch(() => {});
    await registry.close();
    await rm(root, { recursive: true, force: true });
  };
  const build = async (name: string, Dockerfile: string) => {
    const repository = registry.artifact.uri.slice(6).replace(/\/[^/]+$/, `/${name}`);
    repositories.push(repository);
    await writeFile(join(root, 'Dockerfile'), Dockerfile);
    await docker('build', '--network=none', '--pull=false', '--tag', repository, root);
    const output = await docker('push', repository);
    const digest = output.match(/digest: (sha256:[0-9a-f]{64})/)?.[1];
    assert.ok(digest);
    return { uri: `oci://${repository}`, digest };
  };
  try {
    for (const name of ['requirements.lock', 'protocol.py', 'runner.py'])
      await copyFile(
        new URL(`../../../workers/data-engine/${name}`, import.meta.url),
        join(root, name),
      );
    assert.ok(
      (await readFile(join(root, 'requirements.lock'))).equals(
        await readFile('/opt/moria-engine/requirements.lock'),
      ),
      'Locked engine dependencies are stale; rebuild the test image',
    );
    await cp('/opt/moria-engine/site-packages', join(root, 'site-packages'), {
      recursive: true,
      verbatimSymlinks: true,
    });
    const Dockerfile = await readFile(
      new URL('../../../workers/data-engine/Dockerfile', import.meta.url),
      'utf8',
    );
    const install =
      'RUN pip install --no-cache-dir --only-binary=:all: --require-hashes -r requirements.lock';
    assert.equal(
      Dockerfile.split(install).length,
      2,
      'Expected the exact locked engine dependency step',
    );
    const adapter = await build(
      'moria-engine-adapter',
      Dockerfile.replace(install, 'COPY site-packages/ /usr/local/lib/python3.11/site-packages/'),
    );
    await rm(join(root, 'site-packages'), { recursive: true });
    await writeFile(join(root, 'main.py'), program);
    const artifact = await build(
      'moria-engine-program',
      `FROM ${adapter.uri.slice(6)}@${adapter.digest}\nCOPY main.py /program/main.py\n`,
    );
    const derive = async (name: string, source: string) => {
      await writeFile(join(root, 'main.py'), source);
      return build(
        name,
        `FROM ${adapter.uri.slice(6)}@${adapter.digest}\nCOPY main.py /program/main.py\n`,
      );
    };
    return { adapter, artifact, derive, close };
  } catch (error) {
    await close();
    throw error;
  }
}
