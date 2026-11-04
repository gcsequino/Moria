import { compileSource, digest, fingerprint } from '../../compiler/src/browser.js';
import { sourceDescriptor, sourcePath } from '../../compiler/src/source-core.js';
import { MoriaError, type Manifest } from '../../contracts/types.js';
import type { CompiledSourceArtifact } from '../../contracts/filesystem.js';
import type { filesystemClient } from './filesystem.js';

type Client = ReturnType<typeof filesystemClient>;
function mismatch(message: string): never {
  throw new MoriaError('CONTENT_MISMATCH', message);
}
const encoder = new TextEncoder();
function comparePath(a: { path: string }, b: { path: string }) {
  const left = encoder.encode(a.path),
    right = encoder.encode(b.path);
  for (let i = 0; i < Math.min(left.length, right.length); i++)
    if (left[i] !== right[i]) return left[i] - right[i];
  return left.length - right.length;
}

/** Reads only qualified immutable versions; the native publisher rechecks all authority and the source CAS. */
export async function publishBrowserSource(
  client: Client,
  rootId: string,
  phase: (message: string) => void = () => {},
) {
  phase('Reading pinned source files and compiler context…');
  const checkout = await client.checkout(rootId);
  if (
    checkout.rootId !== rootId ||
    checkout.files.length > 2000 ||
    checkout.directories.length > 2000
  )
    mismatch('Invalid source checkout');
  const expected = await fingerprint({
    rootId,
    projectId: checkout.role.projectId,
    deploymentUnitId: checkout.role.deploymentUnitId,
    namespace: checkout.role.namespace,
    files: [...checkout.files].sort(comparePath).map(({ path, entry }) => ({
      path,
      id: entry.id,
      revision: entry.revision,
      digest: entry.content?.digest,
    })),
    directories: [...checkout.directories].sort(comparePath),
  });
  if (checkout.digest !== expected) mismatch('Source checkout digest is invalid');
  const registered = await client.compilerContext();
  if (registered.fingerprint !== (await fingerprint(registered.principalAttributes)))
    mismatch('Compiler context fingerprint is invalid');
  const files: { path: string; text: string }[] = [];
  let total = 0;
  for (const file of checkout.files) {
    sourcePath(file.path);
    const content = file.entry.content;
    if (
      !content ||
      !Number.isSafeInteger(content.size) ||
      content.size < 0 ||
      content.size > 64 * 1024 * 1024 ||
      total + content.size > 64 * 1024 * 1024
    )
      mismatch('Source exceeds its declared bounds');
    const required =
      /(?:\.(?:object|link|action|resource)\.json|\.ontology\.json|\.repository\.json)$/.test(
        file.path,
      );
    if (required && content.size > 16 * 1024 * 1024) mismatch('Source JSON exceeds 16 MiB');
    const response = await client.readFile(file.entry.id, file.entry.revision);
    if (!response.body) mismatch('Source bytes are unavailable');
    const reader = response.body!.getReader(),
      bytes = new Uint8Array(content.size);
    let size = 0;
    try {
      while (true) {
        const part = await reader.read();
        if (part.done) break;
        if (size + part.value.length > bytes.length) mismatch('Source exceeds its declared size');
        bytes.set(part.value, size);
        size += part.value.length;
      }
    } finally {
      await reader.cancel();
      reader.releaseLock();
    }
    total += size;
    if (size !== content.size || (await digest(bytes)) !== content.digest)
      mismatch('Source bytes differ from the checkout');
    let text = '';
    if (required) {
      try {
        text = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
      } catch {
        throw new MoriaError('SOURCE_INVALID', 'Source JSON is not UTF-8');
      }
    }
    files.push({ path: file.path, text });
  }
  const descriptorFile = files.find((file) => file.path === `.${checkout.role.kind}.json`);
  if (!descriptorFile) throw new MoriaError('SOURCE_INVALID', 'Source descriptor is missing');
  const descriptor = sourceDescriptor(checkout.role.kind, descriptorFile.text),
    imports: Manifest[] = [];
  for (const dependency of descriptor.dependencies) {
    const release = (await client.release(dependency.digest)) as {
      rootId: string;
      digest: string;
      compiled: { manifest: Manifest; fingerprint: string };
    };
    if (
      release.rootId !== dependency.rootId ||
      release.digest !== dependency.digest ||
      (await fingerprint(release.compiled.manifest)) !== release.compiled.fingerprint
    )
      mismatch('Pinned source import differs');
    imports.push(release.compiled.manifest);
  }
  phase('Compiling source; native configuration validation and publication are pending…');
  const compilerContext = { principalAttributes: registered.principalAttributes };
  const assembled = await compileSource(
    {
      kind: checkout.role.kind,
      projectId: checkout.role.projectId,
      deploymentUnitId: checkout.role.deploymentUnitId,
      files,
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
  const bytes = encoder.encode(JSON.stringify(artifact));
  if (bytes.length > 16 * 1024 * 1024)
    throw new MoriaError('LIMIT_EXCEEDED', 'Compiler artifact exceeds 16 MiB');
  const home = await client.home(),
    role = await client.role(home.home.id),
    parentId = role?.configuration.configuration;
  if (typeof parentId !== 'string')
    throw new MoriaError('SOURCE_INVALID', 'A managed compiler artifact directory is required');
  const parent = await client.get(parentId),
    name = `compiled-${rootId}-${crypto.randomUUID()}.json`;
  const receipt = await client.writeCompilerArtifact(
    rootId,
    checkout.digest,
    parentId,
    name,
    parent.revision,
    bytes,
    { idempotencyKey: crypto.randomUUID() },
  );
  const file = receipt.entries.find((entry) => entry.parentId === parentId && entry.name === name);
  if (!file?.content)
    throw new MoriaError('INVALID_RESPONSE', 'Compiler artifact receipt is incomplete');
  phase('Validating and publishing with current native authority…');
  return client.publishArtifact(rootId, checkout.digest, {
    entryId: file.id,
    revision: file.revision,
    digest: file.content.digest,
  });
}
