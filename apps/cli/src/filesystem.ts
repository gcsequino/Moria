import { createHash, randomUUID } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { createWriteStream } from 'node:fs';
import {
  readFile,
  writeFile,
  mkdir,
  lstat,
  readdir,
  realpath,
  rename,
  unlink,
  link,
} from 'node:fs/promises';
import { resolve, join, dirname, sep, posix } from 'node:path';
import { Readable, Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { createTransport } from '../../../packages/platform/src/transport.js';
import { filesystemClient } from '../../../packages/platform/src/filesystem.js';
import {
  sourcePath,
  compileSource,
  sourceDescriptor,
} from '../../../packages/compiler/src/source.js';
import { fingerprint } from '../../../packages/compiler/src/index.js';
import type {
  SourceCheckout,
  FilesystemEntry,
  FilesystemReceipt,
  SourceKind,
  CompiledSourceArtifact,
} from '../../../packages/contracts/filesystem.js';
import { MoriaError, type Manifest, type Resource } from '../../../packages/contracts/types.js';

export interface FilesystemOptions {
  directory?: string;
  url?: string;
  profile?: string;
  cursor?: string;
  out?: string;
  kind?: string;
  namespace?: string;
  runtime?: string;
  exports?: string;
  config?: string;
  project?: string;
  revision?: string;
}
type Client = ReturnType<typeof filesystemClient>;
interface CheckoutBinding {
  version: 1;
  origin: string;
  checkout: SourceCheckout;
}
const lockName = '.moria.lock.json';
function invalid(message: string): never {
  throw new MoriaError('USAGE', message);
}
function json(text: string, label: string): any {
  try {
    return JSON.parse(text);
  } catch {
    return invalid(`Invalid ${label}`);
  }
}
function descriptor(bytes: Uint8Array, name: string) {
  return {
    size: bytes.byteLength,
    digest: `sha256:${createHash('sha256').update(bytes).digest('hex')}`,
    mediaType: name.endsWith('.json')
      ? 'application/json'
      : /\.(?:ts|tsx|js|py|scala|md|txt|toml|yaml|yml|sql|sbt)$/.test(name)
        ? 'text/plain'
        : 'application/octet-stream',
  };
}
export async function filesystemConnection(values: FilesystemOptions) {
  const origin =
    values.url ?? process.env.MORIA_URL ?? `http://127.0.0.1:${process.env.MORIA_PORT ?? '3210'}`;
  const url = new URL(origin),
    local = ['127.0.0.1', '[::1]', 'localhost'].includes(url.hostname);
  if (
    url.username ||
    url.password ||
    url.pathname !== '/' ||
    url.search ||
    url.hash ||
    !['http:', 'https:'].includes(url.protocol)
  )
    invalid('Use an instance HTTP(S) origin without credentials or a path');
  const token = process.env.MORIA_API_TOKEN;
  if (!local && (url.protocol !== 'https:' || !token))
    invalid('Remote filesystem access requires HTTPS and MORIA_API_TOKEN');
  let accessKey: string | undefined;
  if (!token) {
    try {
      accessKey = JSON.parse(
        await readFile(
          resolve(
            values.directory ?? process.env.MORIA_LOCAL_DIR ?? '.local/workspace',
            'credentials.json',
          ),
          'utf8',
        ),
      ).accessKey;
    } catch {
      invalid(
        'Local instance credentials are unavailable; start the instance or set MORIA_API_TOKEN',
      );
    }
    if (typeof accessKey !== 'string' || !/^[A-Za-z0-9_-]{43}$/.test(accessKey))
      invalid('Invalid local instance credentials');
  }
  const transport = createTransport({
    baseUrl: origin,
    fetch: (path, init) =>
      fetch(path, {
        ...init,
        headers: {
          ...init?.headers,
          ...(token
            ? { authorization: `Bearer ${token}` }
            : { 'x-moria-key': accessKey!, 'x-moria-profile': values.profile ?? 'operator' }),
        },
      }),
  });
  const client = filesystemClient(transport);
  return { origin, client, transport };
}
async function entry(client: Client, path: string): Promise<FilesystemEntry> {
  if (!path) invalid('A filesystem path or entry ID is required');
  if (path === '~' || path.startsWith('~/')) {
    const home = await client.home();
    path = `/users/${home.home.name}${path.slice(1)}`;
  }
  return path.startsWith('/') ? client.resolve(path) : client.get(path);
}
async function directoryTarget(client: Client, path: string) {
  if (!path || path === '/' || !path.includes('/'))
    invalid('An absolute destination path is required');
  return { parent: await entry(client, posix.dirname(path)), name: posix.basename(path) };
}
async function upload(
  client: Client,
  parentId: string,
  name: string,
  bytes: Uint8Array,
  existing?: FilesystemEntry,
) {
  if (bytes.length > 64 * 1024 * 1024)
    throw new MoriaError('LIMIT_EXCEEDED', 'File exceeds 64 MiB');
  const content = descriptor(bytes, name);
  return client.writeFile(
    existing
      ? { op: 'write', id: existing.id, expectedRevision: existing.revision, content }
      : {
          op: 'create',
          parentId,
          expectedParentRevision: (await client.get(parentId)).revision,
          name,
          content,
        },
    bytes,
    { idempotencyKey: randomUUID() },
  );
}
async function download(
  client: Client,
  id: string,
  revision: string | undefined,
  target: string,
  expected?: FilesystemEntry['content'],
) {
  const response = await client.readFile(id, revision);
  if (!response.body) throw new MoriaError('INVALID_RESPONSE', 'File stream is missing');
  const hash = createHash('sha256');
  let size = 0;
  const temporary = join(dirname(target), `.moria-download-${randomUUID()}`);
  try {
    await pipeline(
      Readable.fromWeb(response.body as import('node:stream/web').ReadableStream),
      new Transform({
        transform(chunk, _, done) {
          size += chunk.length;
          if (size > 64 * 1024 * 1024 || (expected && size > expected.size)) {
            done(new MoriaError('CONTENT_MISMATCH', 'File exceeds its declared bound'));
            return;
          }
          hash.update(chunk);
          done(null, chunk);
        },
      }),
      createWriteStream(temporary, { flags: 'wx', mode: 0o600 }),
    );
    const digest = `sha256:${hash.digest('hex')}`;
    if (expected && (size !== expected.size || digest !== expected.digest))
      throw new MoriaError(
        'CONTENT_MISMATCH',
        'Downloaded source differs from the checkout manifest',
      );
    await link(temporary, target);
    return { path: target, size, digest };
  } finally {
    await unlink(temporary).catch((error) => {
      if (error.code !== 'ENOENT') throw error;
    });
  }
}
async function writeBinding(directory: string, binding: CheckoutBinding, replace = false) {
  const path = join(directory, lockName);
  if (!replace) {
    await writeFile(path, JSON.stringify(binding, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
    return;
  }
  const temporary = join(directory, `.moria-lock-${randomUUID()}`);
  await writeFile(temporary, JSON.stringify(binding, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
  await rename(temporary, path);
}
async function files(directory: string): Promise<string[]> {
  let result: string[];
  let repository = false;
  try {
    execFileSync('git', ['-C', directory, 'rev-parse', '--show-toplevel'], { stdio: 'ignore' });
    repository = true;
  } catch {
    /* A plain checkout does not require Git. */
  }
  if (repository) {
    result = execFileSync('git', ['-C', directory, 'ls-files', '-z'], {
      maxBuffer: 8 * 1024 * 1024,
    })
      .toString('utf8')
      .split('\0')
      .filter(Boolean);
  } else {
    result = [];
    const walk = async (parent: string, prefix = '') => {
      for (const child of await readdir(parent, { withFileTypes: true })) {
        if (
          [
            '.git',
            'node_modules',
            '.venv',
            'dist',
            'target',
            lockName,
            '.moria-checkout-incomplete',
          ].includes(child.name)
        )
          continue;
        const path = prefix ? `${prefix}/${child.name}` : child.name;
        if (child.isSymbolicLink()) invalid('Source symlinks are forbidden');
        if (child.isDirectory()) await walk(join(parent, child.name), path);
        else if (child.isFile()) result.push(path);
        else invalid('Source entries must be regular files or directories');
        if (result.length > 2000)
          throw new MoriaError('LIMIT_EXCEEDED', 'Source unit exceeds 2,000 files');
      }
    };
    await walk(directory);
  }
  return result.filter((path) => path !== lockName && path !== '.moria-checkout-incomplete').sort();
}
function snapshotDigest(snapshot: SourceCheckout) {
  const compare = (a: { path: string }, b: { path: string }) =>
    Buffer.compare(Buffer.from(a.path), Buffer.from(b.path));
  return fingerprint({
    rootId: snapshot.rootId,
    projectId: snapshot.role.projectId,
    deploymentUnitId: snapshot.role.deploymentUnitId,
    namespace: snapshot.role.namespace,
    files: [...snapshot.files].sort(compare).map(({ path, entry }) => ({
      path,
      id: entry.id,
      revision: entry.revision,
      digest: entry.content!.digest,
    })),
    directories: [...snapshot.directories].sort(compare),
  });
}
async function push(client: Client, origin: string, directory: string) {
  const root = await realpath(resolve(directory));
  const lockPath = join(root, lockName);
  if (!(await lstat(lockPath)).isFile()) invalid('Checkout binding must be a regular file');
  const binding = json(await readFile(lockPath, 'utf8'), 'checkout binding') as CheckoutBinding;
  if (binding.version !== 1 || binding.origin !== origin || !binding.checkout?.rootId)
    invalid('Checkout belongs to another instance or has an invalid binding');
  const current = await client.checkout(binding.checkout.rootId);
  if (current.digest !== binding.checkout.digest)
    throw new MoriaError(
      'FILESYSTEM_CONFLICT',
      'Remote source changed; reconcile a fresh checkout before pushing',
    );
  const expected = structuredClone(current),
    byId = new Map(current.files.map((file) => [file.entry.id, file]));
  const intended = new Map<string, (typeof current.files)[number]>();
  for (const file of binding.checkout.files) {
    sourcePath(file.path);
    if (!byId.has(file.entry.id) || intended.has(file.path))
      invalid('Checkout identity binding is invalid');
    intended.set(file.path, byId.get(file.entry.id)!);
  }
  const parents = new Map(expected.directories.map((item) => [item.path, item.id]));
  const update = (receipt: FilesystemReceipt) => {
    for (const entry of receipt.entries) {
      const file = expected.files.find((item) => item.entry.id === entry.id);
      if (file) file.entry = entry;
      const directory = expected.directories.find((item) => item.id === entry.id);
      if (directory) directory.revision = entry.revision;
    }
  };
  const parent = async (path: string) => {
    if (path === '.' || path === '') return expected.rootId;
    let prefix = '';
    for (const part of path.split('/')) {
      const before = prefix;
      prefix = prefix ? `${prefix}/${part}` : part;
      if (!parents.has(prefix)) {
        const parentId = parents.get(before)!;
        const receipt = await client.changes(
          [
            {
              op: 'create',
              parentId,
              expectedParentRevision: (await client.get(parentId)).revision,
              name: part,
              kind: 'directory',
            },
          ],
          { idempotencyKey: randomUUID() },
        );
        update(receipt);
        const created = receipt.entries.find(
          (entry) => entry.parentId === parentId && entry.name === part,
        )!;
        parents.set(prefix, created.id);
        expected.directories.push({ path: prefix, id: created.id, revision: created.revision });
      }
    }
    return parents.get(path)!;
  };
  let total = 0;
  const paths = await files(root);
  for (const path of paths) {
    sourcePath(path);
    const local = resolve(root, ...path.split('/'));
    if (!(await realpath(local)).startsWith(root + sep) || !(await lstat(local)).isFile())
      invalid('Source files must stay inside the checkout and cannot be symlinks');
    const stat = await lstat(local);
    total += stat.size;
    if (total > 64 * 1024 * 1024 || stat.size > 64 * 1024 * 1024)
      throw new MoriaError('LIMIT_EXCEEDED', 'Source unit exceeds 64 MiB');
    const bytes = await readFile(local),
      digest = descriptor(bytes, path).digest,
      original = intended.get(path);
    const targetParent = await parent(posix.dirname(path));
    if (original && original.path !== path) {
      const sourceParent = await client.get(original.entry.parentId!),
        destination = await client.get(targetParent);
      const receipt = await client.changes(
        [
          {
            op: 'move',
            id: original.entry.id,
            expectedRevision: original.entry.revision,
            parentId: targetParent,
            expectedParentRevision: destination.revision,
            expectedSourceParentRevision: sourceParent.revision,
            name: posix.basename(path),
          },
        ],
        { idempotencyKey: randomUUID() },
      );
      update(receipt);
      expected.files.find((item) => item.entry.id === original.entry.id)!.path = path;
    }
    const entry = original
      ? expected.files.find((item) => item.entry.id === original.entry.id)!.entry
      : undefined;
    if (entry?.content?.digest === digest) continue;
    const receipt = await upload(client, targetParent, posix.basename(path), bytes, entry);
    update(receipt);
    if (!entry)
      expected.files.push({
        path,
        entry: receipt.entries.find(
          (item) => item.parentId === targetParent && item.name === posix.basename(path),
        )!,
      });
  }
  const final = await client.checkout(expected.rootId);
  if (snapshotDigest(expected) !== final.digest)
    throw new MoriaError(
      'FILESYSTEM_CONFLICT',
      'Remote source changed during upload; active releases remain unchanged',
    );
  await writeBinding(root, { version: 1, origin, checkout: final }, true);
  return {
    rootId: final.rootId,
    sourceDigest: final.digest,
    files: paths.length,
    retainedMissingFiles: final.files
      .filter((file) => !paths.includes(file.path))
      .map((file) => file.path),
  };
}
export async function publishCompiled(
  client: Client,
  rootId: string,
  exports: Resource[] | undefined,
) {
  const checkout = await client.checkout(rootId);
  if (snapshotDigest(checkout) !== checkout.digest)
    throw new MoriaError('CONTENT_MISMATCH', 'Source checkout digest is invalid');
  const registered = await client.compilerContext();
  if (registered.fingerprint !== fingerprint(registered.principalAttributes))
    throw new MoriaError('CONTENT_MISMATCH', 'Compiler context fingerprint is invalid');
  const sourceFiles: { path: string; text: string }[] = [];
  let total = 0;
  for (const file of checkout.files) {
    sourcePath(file.path);
    const content = file.entry.content;
    if (!content || content.size > 64 * 1024 * 1024)
      throw new MoriaError('SOURCE_INVALID', 'Invalid qualified source file');
    const required =
      /(?:\.(?:object|link|action|resource)\.json|\.ontology\.json|\.repository\.json)$/.test(
        file.path,
      );
    if (required && content.size > 16 * 1024 * 1024)
      throw new MoriaError('LIMIT_EXCEEDED', 'Source JSON exceeds16MiB');
    const response = await client.readFile(file.entry.id, file.entry.revision);
    if (!response.body) throw new MoriaError('INVALID_RESPONSE', 'Source bytes are unavailable');
    const hash = createHash('sha256');
    let size = 0;
    const parts: Uint8Array[] = [];
    for await (const chunk of Readable.fromWeb(
      response.body as import('node:stream/web').ReadableStream,
    )) {
      size += chunk.length;
      total += chunk.length;
      if (size > content.size || total > 64 * 1024 * 1024)
        throw new MoriaError('CONTENT_MISMATCH', 'Source exceeds its declared bounds');
      hash.update(chunk);
      if (required) parts.push(new Uint8Array(chunk));
    }
    if (size !== content.size || `sha256:${hash.digest('hex')}` !== content.digest)
      throw new MoriaError('CONTENT_MISMATCH', 'Source bytes differ from the checkout');
    let text = '';
    if (required) {
      const bytes = new Uint8Array(size);
      let offset = 0;
      for (const part of parts) {
        bytes.set(part, offset);
        offset += part.length;
      }
      try {
        text = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
      } catch {
        throw new MoriaError('SOURCE_INVALID', 'Source JSON is not UTF-8');
      }
    }
    sourceFiles.push({ path: file.path, text });
  }
  const descriptorFile = sourceFiles.find((file) => file.path === `.${checkout.role.kind}.json`);
  if (!descriptorFile) throw new MoriaError('SOURCE_INVALID', 'Source descriptor is missing');
  const descriptor = sourceDescriptor(checkout.role.kind, descriptorFile.text);
  const imports: Manifest[] = [];
  for (const dependency of descriptor.dependencies) {
    const release = (await client.release(dependency.digest)) as {
      rootId: string;
      digest: string;
      compiled: { manifest: Manifest; fingerprint: string };
    };
    if (
      release.rootId !== dependency.rootId ||
      release.digest !== dependency.digest ||
      fingerprint(release.compiled.manifest) !== release.compiled.fingerprint
    )
      throw new MoriaError('SOURCE_INVALID', 'Pinned source import differs');
    imports.push(release.compiled.manifest);
  }
  const compilerContext = { principalAttributes: registered.principalAttributes };
  const assembled = compileSource(
    {
      kind: checkout.role.kind,
      projectId: checkout.role.projectId,
      deploymentUnitId: checkout.role.deploymentUnitId,
      files: sourceFiles,
      exports,
      imports,
    },
    compilerContext,
  );
  const artifact: CompiledSourceArtifact = {
    version: 1,
    rootId,
    sourceDigest: checkout.digest,
    ...assembled,
    compilerContext,
  };
  const bytes = new TextEncoder().encode(JSON.stringify(artifact));
  if (bytes.length > 16 * 1024 * 1024)
    throw new MoriaError('LIMIT_EXCEEDED', 'Compiler artifact exceeds16MiB');
  const home = await client.home();
  const homeRole = await client.role(home.home.id);
  const parentId = homeRole?.configuration.configuration;
  if (typeof parentId !== 'string')
    throw new MoriaError('SOURCE_INVALID', 'A managed compiler artifact directory is required');
  const parent = await client.get(parentId);
  const name = `compiled-${rootId}-${randomUUID()}.json`;
  const receipt = await client.writeCompilerArtifact(
    rootId,
    checkout.digest,
    parentId,
    name,
    parent.revision,
    bytes,
    { idempotencyKey: randomUUID() },
  );
  const file = receipt.entries.find((entry) => entry.parentId === parentId && entry.name === name);
  if (!file?.content)
    throw new MoriaError('INVALID_RESPONSE', 'Compiler artifact receipt is incomplete');
  return client.publishArtifact(rootId, checkout.digest, {
    entryId: file.id,
    revision: file.revision,
    digest: file.content.digest,
  });
}

export async function filesystemCommand(args: string[], values: FilesystemOptions) {
  const [command, target, argument, ...extra] = args;
  if (extra.length) invalid('Too many filesystem arguments');
  const { origin, client } = await filesystemConnection(values);
  switch (command) {
    case 'collect':
      return client.collect();
    case 'enable':
    case 'disable':
      if (!target) invalid('fs enable|disable <instance-principal-id>');
      return client.setEnabled(target, command === 'enable');
    case 'retire': {
      const root = await entry(client, target),
        role = await client.role(root.id);
      if (!role) invalid('Source root is not registered');
      return client.retire(root.id, role.revision);
    }
    case 'resource':
      if (!target) invalid('fs resource <resource-reference-or-id>');
      return client.resource(target, { projectId: values.project });
    case 'history':
      if (!target) invalid('fs history <path-or-id> [--cursor <before-revision>]');
      return client.history(
        target.startsWith('/') || target.startsWith('~')
          ? (await entry(client, target)).id
          : target,
        values.cursor,
      );
    case 'cp': {
      if (!argument) invalid('fs cp <source-file> <destination>');
      const source = await entry(client, target),
        destination = await directoryTarget(client, argument);
      return client.copy(
        {
          id: source.id,
          expectedRevision: source.revision,
          parentId: destination.parent.id,
          expectedParentRevision: destination.parent.revision,
          name: destination.name,
        },
        { idempotencyKey: randomUUID() },
      );
    }
    case 'home':
      if (target) invalid('fs home takes no arguments');
      return client.home();
    case 'ls':
      return client.list((await entry(client, target ?? '~')).id, values.cursor);
    case 'stat': {
      const current = await entry(client, target);
      return { ...current, role: await client.role(current.id) };
    }
    case 'mkdir': {
      const destination = await directoryTarget(client, target);
      return client.changes(
        [
          {
            op: 'create',
            parentId: destination.parent.id,
            expectedParentRevision: destination.parent.revision,
            name: destination.name,
            kind: 'directory',
          },
        ],
        { idempotencyKey: randomUUID() },
      );
    }
    case 'rm': {
      const current = await entry(client, target);
      return client.changes(
        [
          {
            op: 'delete',
            id: current.id,
            expectedRevision: current.revision,
            expectedParentRevision: (await client.get(current.parentId!)).revision,
          },
        ],
        { idempotencyKey: randomUUID() },
      );
    }
    case 'mv': {
      if (!argument) invalid('fs mv <source> <destination>');
      const current = await entry(client, target),
        destination = await directoryTarget(client, argument),
        parent = await client.get(current.parentId!);
      return client.changes(
        [
          {
            op: 'move',
            id: current.id,
            expectedRevision: current.revision,
            parentId: destination.parent.id,
            expectedParentRevision: destination.parent.revision,
            expectedSourceParentRevision: parent.revision,
            name: destination.name,
          },
        ],
        { idempotencyKey: randomUUID() },
      );
    }
    case 'get': {
      const current = values.revision
          ? await client.version(
              target.startsWith('/') || target.startsWith('~')
                ? (await entry(client, target)).id
                : target,
              values.revision,
            )
          : await entry(client, target),
        out = values.out ?? argument;
      if (!out || current.kind !== 'file') invalid('fs get <file> --out <new-local-file>');
      return download(client, current.id, current.revision, resolve(out), current.content);
    }
    case 'put': {
      if (!target || !argument) invalid('fs put <local-file> <destination>');
      const stat = await lstat(target);
      if (!stat.isFile() || stat.size > 64 * 1024 * 1024)
        invalid('Upload requires a regular file of at most 64 MiB');
      const destination = await directoryTarget(client, argument);
      let existing: FilesystemEntry | undefined;
      try {
        existing = await entry(client, argument);
      } catch (error) {
        if ((error as { code?: string }).code !== 'NOT_FOUND') throw error;
      }
      return upload(
        client,
        destination.parent.id,
        destination.name,
        await readFile(target),
        existing,
      );
    }
    case 'project': {
      if (!argument) invalid('fs project <parent> <name>');
      const parent = await entry(client, target);
      return client.createProject(parent.id, argument, parent.revision);
    }
    case 'group': {
      if (!target || !argument) invalid('fs group <name> <authoritative-group-key>');
      const groups = await client.resolve('/groups');
      return client.createGroup(target, argument, groups.revision);
    }
    case 'init': {
      if (!values.kind || !['ontology', 'repository'].includes(values.kind) || !values.namespace)
        invalid(
          'fs init <path> --kind ontology|repository --namespace <namespace> [--runtime typescript-v1|python-duckdb-v1]',
        );
      const destination = await directoryTarget(client, target);
      const receipt = await client.changes(
        [
          {
            op: 'create',
            parentId: destination.parent.id,
            expectedParentRevision: destination.parent.revision,
            name: destination.name,
            kind: 'directory',
          },
        ],
        { idempotencyKey: randomUUID() },
      );
      const root = receipt.entries.find(
        (entry) => entry.name === destination.name && entry.parentId === destination.parent.id,
      )!;
      const kind = values.kind as SourceKind,
        text =
          JSON.stringify(
            {
              version: 1,
              name: destination.name,
              namespace: values.namespace,
              ...(kind === 'repository' ? { runtime: values.runtime ?? 'typescript-v1' } : {}),
            },
            null,
            2,
          ) + '\n';
      await upload(client, root.id, `.${kind}.json`, new TextEncoder().encode(text));
      return client.register(root.id, kind, (await client.get(root.id)).revision);
    }
    case 'register': {
      if (!values.kind || !['ontology', 'repository'].includes(values.kind))
        invalid('--kind ontology|repository is required');
      const root = await entry(client, target);
      return client.register(root.id, values.kind as SourceKind, root.revision);
    }
    case 'checkout':
    case 'bind': {
      if (!argument) invalid(`fs ${command} <remote-root> <local-directory>`);
      const root = await entry(client, target),
        checkout = await client.checkout(root.id),
        local = resolve(argument);
      if (command === 'checkout') {
        await mkdir(local, { mode: 0o700 });
        await writeFile(
          join(local, '.moria-checkout-incomplete'),
          'Checkout is incomplete until .moria.lock.json is written.\n',
          { flag: 'wx', mode: 0o600 },
        );
        for (const directory of checkout.directories)
          if (directory.path) {
            sourcePath(directory.path);
            await mkdir(join(local, directory.path), { recursive: true, mode: 0o700 });
          }
        for (const file of checkout.files) {
          sourcePath(file.path);
          if (file.path === lockName || file.path === '.moria-checkout-incomplete')
            invalid('Source contains a reserved checkout file');
          await mkdir(dirname(join(local, file.path)), { recursive: true, mode: 0o700 });
          await download(
            client,
            file.entry.id,
            file.entry.revision,
            join(local, file.path),
            file.entry.content,
          );
        }
      } else if (!(await lstat(local)).isDirectory())
        invalid('Binding requires an existing local directory');
      let replace = false;
      if (command === 'bind') {
        try {
          const lock = join(local, lockName);
          if (!(await lstat(lock)).isFile()) invalid('Checkout binding must be a regular file');
          const prior = json(await readFile(lock, 'utf8'), 'checkout binding') as CheckoutBinding;
          if (prior.origin !== origin || prior.checkout?.rootId !== checkout.rootId)
            invalid('Refusing to replace another source binding');
          replace = true;
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
        }
      }
      await writeBinding(local, { version: 1, origin, checkout }, replace);
      if (command === 'checkout') await unlink(join(local, '.moria-checkout-incomplete'));
      return {
        directory: local,
        rootId: root.id,
        sourceDigest: checkout.digest,
        files: checkout.files.length,
      };
    }
    case 'push':
      if (!target) invalid('fs push <local-checkout>');
      return push(client, origin, target);
    case 'publish': {
      const root = await entry(client, target),
        exports = values.exports
          ? json(await readFile(values.exports, 'utf8'), 'export manifest')
          : undefined;
      if (exports !== undefined && !Array.isArray(exports))
        invalid('Export manifest must be a resource array');
      return publishCompiled(client, root.id, exports);
    }
    case 'plan':
      if (!argument) invalid('fs plan <source-root> <release-digest>');
      return client.plan(
        (await entry(client, target)).id,
        argument,
        values.config ? json(await readFile(values.config, 'utf8'), 'deletion list') : [],
      );
    case 'apply':
      if (!target) invalid('fs apply <plan-id>');
      return client.apply(target);
    case 'policy': {
      const current = await entry(client, target),
        policy = await client.policy(current.id);
      return argument
        ? client.setPolicy(
            current.id,
            policy.revision,
            json(await readFile(argument, 'utf8'), 'filesystem policy'),
          )
        : policy;
    }
    default:
      return invalid(
        'Expected fs home|ls|stat|mkdir|mv|rm|get|put|project|group|init|register|checkout|bind|push|publish|plan|apply|policy',
      );
  }
}
