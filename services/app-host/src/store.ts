import { createHash, randomUUID } from 'node:crypto';
import { lstat, mkdir, open, readFile, readdir, rename, rm, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { MoriaError } from '../../../packages/contracts/types.js';

export interface Artifact {
  app: string;
  version: string;
  files: Record<string, string>;
}
export interface Deployment {
  current: string;
  previous?: string;
  promotedAt: string;
}
const digest = (value: string | Buffer) => createHash('sha256').update(value).digest('hex');
export function appId(id: string) {
  if (!/^[a-z][a-z0-9-]{0,47}$/.test(id) || /^(con|prn|aux|nul|com[0-9]|lpt[0-9])$/.test(id))
    throw new MoriaError('APP_INVALID', 'App IDs must be lowercase names, up to 48 characters.');
  return id;
}
function versionId(version: string) {
  if (!/^[a-f0-9]{64}$/.test(version))
    throw new MoriaError('APP_INVALID', 'Expected a full SHA-256 version.');
  return version;
}
export function safeFile(path: string) {
  return (
    path.length <= 240 &&
    path
      .split('/')
      .every(
        (part) =>
          /^[a-zA-Z0-9_][a-zA-Z0-9_.-]*$/.test(part) &&
          !part.endsWith('.') &&
          !/^(con|prn|aux|nul|com[0-9]|lpt[0-9])(?:\.|$)/i.test(part),
      ) &&
    /\.(html|js|mjs|css|json|svg|png|jpg|jpeg|webp|gif|ico|woff|woff2|txt|wasm)$/.test(path) &&
    path !== 'artifact.json'
  );
}
async function collect(root: string, prefix = '', result = new Map<string, Buffer>()) {
  for (const entry of await readdir(join(root, prefix), { withFileTypes: true })) {
    const path = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isSymbolicLink())
      throw new MoriaError('APP_INVALID', 'Artifact symlinks are forbidden.');
    if (entry.isDirectory()) {
      if (!/^[a-zA-Z0-9_][a-zA-Z0-9_-]*$/.test(entry.name) || path.split('/').length > 8)
        throw new MoriaError('APP_INVALID', 'Invalid artifact directory.');
      await collect(root, path, result);
    } else {
      if (!entry.isFile() || !safeFile(path))
        throw new MoriaError('APP_INVALID', `Unsupported artifact path: ${path}`);
      const stat = await lstat(join(root, path));
      if (stat.size > 16 * 1024 * 1024 || result.size >= 2000)
        throw new MoriaError('APP_INVALID', 'Artifact file/count limit exceeded.');
      result.set(path, await readFile(join(root, path)));
      if ([...result.values()].reduce((size, value) => size + value.length, 0) > 64 * 1024 * 1024)
        throw new MoriaError('APP_INVALID', 'Artifact exceeds 64 MiB.');
    }
  }
  return result;
}
/** Trusted local operator store. No submitted scripts or build commands execute here. */
export class AppHost {
  constructor(readonly root: string) {}
  private directory(id: string) {
    return resolve(this.root, appId(id));
  }
  private release(id: string, version: string) {
    return join(this.directory(id), 'releases', versionId(version));
  }
  private async locked<T>(id: string, action: () => Promise<T>): Promise<T> {
    const directory = this.directory(id);
    await mkdir(directory, { recursive: true });
    const lock = join(directory, 'mutation.lock');
    let handle;
    try {
      handle = await open(lock, 'wx');
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'EEXIST')
        throw new MoriaError(
          'APP_BUSY',
          'Another app mutation holds the lock. Use apps unlock after a crashed CLI process.',
        );
      throw error;
    }
    try {
      await handle.writeFile(String(process.pid));
      await handle.close();
      return await action();
    } finally {
      await handle.close();
      await rm(lock, { force: true });
    }
  }
  async unlock(id: string) {
    const lock = join(this.directory(id), 'mutation.lock');
    const pid = Number(await readFile(lock, 'utf8'));
    if (!Number.isInteger(pid) || pid < 1)
      throw new MoriaError(
        'APP_BUSY',
        'Incomplete lock; verify no publisher is running before removing mutation.lock manually.',
      );
    try {
      process.kill(pid, 0);
      throw new MoriaError('APP_BUSY', 'The lock owner is still running.');
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error;
    }
    await rm(lock);
  }
  async publish(id: string, source: string, repair = false): Promise<Artifact> {
    appId(id);
    const stat = await lstat(source);
    if (!stat.isDirectory() || stat.isSymbolicLink())
      throw new MoriaError('APP_INVALID', 'Build directory must be a real directory.');
    const entries = await collect(source);
    if (!entries.has('index.html'))
      throw new MoriaError('APP_INVALID', 'Artifact requires index.html.');
    const files = Object.fromEntries(
      [...entries]
        .sort(([a], [b]) => a.localeCompare(b, 'en'))
        .map(([name, bytes]) => [name, digest(bytes)]),
    );
    const artifact = { app: id, version: digest(JSON.stringify({ app: id, files })), files };
    return this.locked(id, async () => {
      const target = this.release(id, artifact.version);
      try {
        await this.verify(id, artifact.version);
        return artifact;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT' && !repair) throw error;
        if (repair) {
          try {
            await rename(target, join(this.directory(id), `quarantine-${randomUUID()}`));
          } catch (moveError) {
            if ((moveError as NodeJS.ErrnoException).code !== 'ENOENT') throw moveError;
          }
        }
      }
      const stage = join(this.directory(id), `staging-${randomUUID()}`);
      await mkdir(stage, { recursive: true });
      for (const [name, bytes] of entries) {
        const path = join(stage, name);
        await mkdir(dirname(path), { recursive: true });
        await writeFile(path, bytes, { flag: 'wx' });
      }
      await writeFile(join(stage, 'artifact.json'), JSON.stringify(artifact), { flag: 'wx' });
      await mkdir(dirname(target), { recursive: true });
      await rename(stage, target);
      return artifact;
    });
  }
  async artifact(id: string, version: string): Promise<Artifact> {
    const value = JSON.parse(
      await readFile(join(this.release(id, version), 'artifact.json'), 'utf8'),
    ) as Artifact;
    if (
      value.app !== id ||
      value.version !== version ||
      !value.files ||
      !Object.hasOwn(value.files, 'index.html') ||
      Object.entries(value.files).some(
        ([name, hash]) => !safeFile(name) || !/^[a-f0-9]{64}$/.test(hash),
      ) ||
      digest(JSON.stringify({ app: value.app, files: value.files })) !== version
    )
      throw new MoriaError('APP_CORRUPT', 'Artifact manifest failed integrity validation.');
    return value;
  }
  async file(id: string, version: string, name: string) {
    if (!safeFile(name)) throw new MoriaError('NOT_FOUND', 'Asset not found.');
    const artifact = await this.artifact(id, version);
    if (!Object.hasOwn(artifact.files, name)) throw new MoriaError('NOT_FOUND', 'Asset not found.');
    const path = join(this.release(id, version), name);
    const stat = await lstat(path);
    if (!stat.isFile() || stat.isSymbolicLink())
      throw new MoriaError('APP_CORRUPT', 'Invalid stored asset.');
    const bytes = await readFile(path);
    if (digest(bytes) !== artifact.files[name])
      throw new MoriaError('APP_CORRUPT', 'Asset failed integrity validation.');
    return bytes;
  }
  async verify(id: string, version: string) {
    const artifact = await this.artifact(id, version);
    for (const name of Object.keys(artifact.files)) await this.file(id, version, name);
    return artifact;
  }
  async active(id: string): Promise<Deployment> {
    const value = JSON.parse(
      await readFile(join(this.directory(id), 'active.json'), 'utf8'),
    ) as Deployment;
    versionId(value.current);
    if (value.previous) versionId(value.previous);
    return value;
  }
  async promote(id: string, version: string, recovery = false): Promise<Deployment> {
    return this.locked(id, () => this.promoteLocked(id, version, recovery));
  }
  private async promoteLocked(id: string, version: string, recovery = false): Promise<Deployment> {
    await this.verify(id, version);
    let old: Deployment | undefined;
    try {
      old = await this.active(id);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT' && !recovery) throw error;
    }
    if (old?.current === version) return old;
    const value = {
      current: version,
      ...(old ? { previous: old.current } : {}),
      promotedAt: new Date().toISOString(),
    };
    const temp = join(this.directory(id), `pointer-${randomUUID()}.json`);
    await writeFile(temp, JSON.stringify(value), { flag: 'wx' });
    await rename(temp, join(this.directory(id), 'active.json'));
    return value;
  }
  async rollback(id: string) {
    return this.locked(id, async () => {
      const active = await this.active(id);
      if (!active.previous)
        throw new MoriaError('APP_INVALID', 'No previous version is available.');
      return this.promoteLocked(id, active.previous);
    });
  }
  async list() {
    await mkdir(this.root, { recursive: true });
    const result = [];
    for (const entry of await readdir(this.root, { withFileTypes: true })) {
      if (!entry.isDirectory() || !/^[a-z][a-z0-9-]{0,47}$/.test(entry.name)) continue;
      let deployment, error;
      try {
        deployment = await this.active(entry.name);
        await this.verify(entry.name, deployment.current);
      } catch (e) {
        error = e instanceof Error ? e.message : 'Unavailable';
      }
      const releases = await readdir(join(this.directory(entry.name), 'releases')).catch(
        () => [] as string[],
      );
      result.push({
        app: entry.name,
        deployment,
        versions: releases.filter((v) => /^[a-f0-9]{64}$/.test(v)),
        error,
      });
    }
    return result;
  }
}
