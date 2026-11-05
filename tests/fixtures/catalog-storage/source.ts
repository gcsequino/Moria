import assert from 'node:assert/strict';
import { randomUUID, createHash } from 'node:crypto';
import type { Catalog } from '../../../services/catalog/src/catalog.js';
import { createLocalFilesystem } from '../../../services/filesystem/src/local.js';
import { WorkloadSessions } from '../../../services/security/src/sessions.js';
import { filesystemOperations } from '../../../packages/contracts/filesystem.js';
import { resourceRef, type Resource } from '../../../packages/contracts/types.js';

/** Publish actual Source definitions so positive domain fixtures have live resource bindings. */
export async function publishedResources(
  catalog: Catalog,
  storageRoot: string,
  environment: string,
  definitions: Resource[],
) {
  assert.ok(definitions.length);
  const runtime = await createLocalFilesystem(catalog, storageRoot),
    authority = runtime.filesystem.security.authority,
    principal = `source-author-${randomUUID()}`,
    application = principal,
    operations = filesystemOperations.map((operation) => `filesystem:${operation}`);
  await authority.setApplication(environment, 'fixture', application, operations);
  await authority.syncPrincipal(environment, 'fixture', principal, {
    kind: 'workload',
    enabled: true,
    markings: [],
    attributes: {},
  });
  await authority.setGrants(environment, 'fixture', principal, application, operations);
  const sessions = new WorkloadSessions(authority),
    token = await sessions.mint({ environment, principal, application }, 'fixture'),
    context = { session: await sessions.authenticate(`Bearer ${token.token}`) },
    home = await runtime.management.ensureHome(context),
    role = await runtime.management.role(context, home.home.id),
    projects = await runtime.filesystem.get(context, role!.configuration.projects as string),
    project = await runtime.management.createProject(
      context,
      projects.id,
      'fixture',
      projects.revision,
    ),
    source = await runtime.filesystem.store.resolve(
      `/users/${home.home.name}/projects/${project.entry.name}/source`,
    ),
    created = await runtime.filesystem.apply(context, randomUUID(), [
      {
        op: 'create',
        parentId: source.id,
        expectedParentRevision: source.revision,
        name: 'ontology',
        kind: 'directory',
      },
    ]),
    sourceRoot = created.entries.find((entry) => entry.name === 'ontology')!;
  const files = [
    {
      name: '.ontology.json',
      value: { version: 1, name: 'Fixture', namespace: definitions[0].metadata.namespace },
    },
    ...definitions.map((value, index) => ({ name: `${index}.resource.json`, value })),
  ];
  for (const file of files) {
    const bytes = new TextEncoder().encode(JSON.stringify(file.value));
    await runtime.filesystem.writeFile(
      context,
      randomUUID(),
      {
        op: 'create',
        parentId: sourceRoot.id,
        expectedParentRevision: (await runtime.filesystem.store.get(sourceRoot.id)).revision,
        name: file.name,
        content: {
          size: bytes.length,
          digest: `sha256:${createHash('sha256').update(bytes).digest('hex')}`,
          mediaType: 'application/json',
        },
      },
      new Blob([bytes]).stream(),
    );
  }
  await runtime.source.register(
    context,
    sourceRoot.id,
    (await runtime.filesystem.store.get(sourceRoot.id)).revision,
    'ontology',
  );
  const release = await runtime.source.publish(
    context,
    sourceRoot.id,
    (await runtime.source.checkout(context, sourceRoot.id)).digest,
  );
  await runtime.source.apply(
    context,
    (await runtime.source.plan(context, sourceRoot.id, release.digest)).id,
  );
  return Promise.all(
    definitions.map(async (definition) => {
      const bound = await runtime.source.resource(
        context,
        resourceRef(definition),
        project.entry.projectId!,
      );
      return runtime.filesystem.store.get(bound.entryId);
    }),
  );
}
