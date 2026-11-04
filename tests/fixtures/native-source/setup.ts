import { randomUUID, createHash } from 'node:crypto';
import { filesystemClient } from '../../../packages/platform/src/filesystem.js';
import { createTransport } from '../../../packages/platform/src/transport.js';
import { publishBrowserSource } from '../../../packages/platform/src/source.js';
import type { SourceKind } from '../../../packages/contracts/filesystem.js';
import type { Resource } from '../../../packages/contracts/types.js';
import { publishCompiled } from '../../../apps/cli/src/filesystem.js';

/** Actual protected source/file/compiler-artifact publication against a native HTTP instance. */
export async function publishNativeSource(
  baseUrl: string,
  token: string,
  files: { path: string; text: string }[],
  kind: SourceKind = 'ontology',
  exports?: Resource[],
) {
  const transport = createTransport({
    baseUrl,
    fetch: (url, init) =>
      fetch(url, { ...init, headers: { ...init?.headers, authorization: `Bearer ${token}` } }),
  });
  const client = filesystemClient(transport);
  const home = await client.home();
  const project = await client.createProject(
    home.home.id,
    `source-${randomUUID()}`,
    (await client.get(home.home.id)).revision,
  );
  const source = (await client.list(project.entry.id)).entries.find(
    (entry) => entry.name === 'source',
  )!;
  const receipt = await client.changes(
    [
      {
        op: 'create',
        parentId: source.id,
        expectedParentRevision: source.revision,
        name: kind,
        kind: 'directory',
      },
    ],
    { idempotencyKey: randomUUID() },
  );
  const root = receipt.entries.find(
    (entry) => entry.parentId === source.id && entry.name === kind,
  )!;
  const directories = new Map<string, string>([['', root.id]]);
  for (const file of files) {
    const parts = file.path.split('/');
    let path = '';
    for (const name of parts.slice(0, -1)) {
      const next = path ? `${path}/${name}` : name;
      if (!directories.has(next)) {
        const parent = await client.get(directories.get(path)!);
        const receipt = await client.changes(
          [
            {
              op: 'create',
              parentId: parent.id,
              expectedParentRevision: parent.revision,
              name,
              kind: 'directory',
            },
          ],
          { idempotencyKey: randomUUID() },
        );
        directories.set(
          next,
          receipt.entries.find((entry) => entry.parentId === parent.id && entry.name === name)!.id,
        );
      }
      path = next;
    }
    const bytes = new TextEncoder().encode(file.text),
      parent = await client.get(directories.get(path)!);
    await client.writeFile(
      {
        op: 'create',
        parentId: parent.id,
        expectedParentRevision: parent.revision,
        name: parts.at(-1)!,
        content: {
          size: bytes.length,
          digest: `sha256:${createHash('sha256').update(bytes).digest('hex')}`,
          mediaType: 'application/json',
        },
      },
      bytes,
      { idempotencyKey: randomUUID() },
    );
  }
  const role = await client.register(root.id, kind, (await client.get(root.id)).revision);
  const publication =
    exports === undefined
      ? await publishBrowserSource(client, root.id)
      : await publishCompiled(client, root.id, exports);
  return {
    client,
    transport,
    root: await client.get(root.id),
    role,
    project,
    publication,
    resource: (reference: string) => {
      const resource = publication.resources.find((resource) => resource.reference === reference);
      if (!resource) throw new Error(`Missing published reference ${reference}`);
      return resource.id;
    },
  };
}
