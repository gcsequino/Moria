import { randomUUID, createHash } from 'node:crypto';
import { join } from 'node:path';
import { database } from '../catalog-storage/database.js';
import { createLocalFilesystem } from '../../../services/filesystem/src/local.js';
import { Functions } from '../../../services/functions/src/service.js';
import { WorkloadSessions } from '../../../services/security/src/sessions.js';
import { filesystemOperations } from '../../../packages/contracts/filesystem.js';
import { ObjectService, type ActionBinding } from '../../../services/ontology/src/service.js';
import { functionFilesystem } from '../../../apps/api/src/function-platform.js';
import { ObjectStore } from '../../../services/ontology/src/store.js';
import type { Resource } from '../../../packages/contracts/types.js';
import type { FunctionWorker } from '../../../services/functions/src/worker.js';
import { DockerFunctionWorker } from '../../../services/functions/src/docker.js';

export async function functionPlatform(
  artifact: { uri: string; digest: string },
  worker: FunctionWorker = new DockerFunctionWorker(),
) {
  const db = await database(),
    runtime = await createLocalFilesystem(db.catalog, join(db.root, 'files')),
    authority = runtime.filesystem.security.authority;
  const environment = 'functions',
    application = 'functions-app',
    principal = 'author',
    operations = [
      'instance:manage',
      'functions:invoke',
      'records:read',
      'edit-record:invoke',
      'reject-record:invoke',
      'function-record:invoke',
      ...filesystemOperations.map((operation) => `filesystem:${operation}`),
    ];
  await db.catalog.ensureEnvironment(environment);
  await authority.setApplication(environment, 'operator', application, operations);
  const sessions = new WorkloadSessions(authority);
  async function identity(principal: string) {
    await authority.syncPrincipal(environment, 'operator', principal, {
      kind: 'workload',
      enabled: true,
      attributes: {},
      markings: ['protected'],
    });
    await authority.setGrants(environment, 'operator', principal, application, operations);
    const token = await sessions.mint({ environment, application, principal }, 'operator');
    const context = { session: await sessions.authenticate(`Bearer ${token.token}`) };
    const home = await runtime.management.ensureHome(context);
    return { context, home, token: token.token };
  }
  const owner = await identity(principal),
    other = await identity('other');
  const role = await runtime.management.role(owner.context, owner.home.home.id);
  const projects = await runtime.filesystem.get(
    owner.context,
    role!.configuration.projects as string,
  );
  const project = await runtime.management.createProject(
    owner.context,
    projects.id,
    'functions',
    projects.revision,
  );
  const sourceDirectory = await runtime.filesystem.store.resolve(
    `/users/${owner.home.home.name}/projects/functions/source`,
  );
  const created = await runtime.filesystem.apply(owner.context, randomUUID(), [
    {
      op: 'create',
      kind: 'directory',
      parentId: sourceDirectory.id,
      expectedParentRevision: sourceDirectory.revision,
      name: 'logic',
    },
  ]);
  const root = created.entries.find((entry) => entry.name === 'logic')!;
  async function file(name: string, value: unknown) {
    const bytes = new TextEncoder().encode(JSON.stringify(value));
    await runtime.filesystem.writeFile(
      owner.context,
      randomUUID(),
      {
        op: 'create',
        parentId: root.id,
        expectedParentRevision: (await runtime.filesystem.store.get(root.id)).revision,
        name,
        content: {
          size: bytes.length,
          digest: `sha256:${createHash('sha256').update(bytes).digest('hex')}`,
          mediaType: 'application/json',
        },
      },
      new ReadableStream({
        start(controller) {
          controller.enqueue(bytes);
          controller.close();
        },
      }),
    );
  }
  await file('.repository.json', {
    version: 1,
    name: 'Logic',
    namespace: 'logic',
    runtime: 'typescript-v1',
  });
  await runtime.source.register(
    owner.context,
    root.id,
    (await runtime.filesystem.store.get(root.id)).revision,
    'repository',
  );
  const declaration: Resource = {
    apiVersion: 'moria/v1alpha2',
    kind: 'Function',
    metadata: { namespace: 'logic', name: 'calculate' },
    spec: {
      artifact,
      runtime: 'typescript-v1',
      inputs: {
        mode: { type: 'string', nullable: false },
        value: { type: 'float64', nullable: false },
        uid: { type: 'uid', nullable: true },
      },
      outputs: { value: { type: 'float64', nullable: false } },
      effect: 'readOnly',
      limits: { timeoutSeconds: 20, memoryMiB: 128, cpuMillis: 5000 },
    },
  };
  async function publish(definition = declaration) {
    const publication = await runtime.source.publish(
      owner.context,
      root.id,
      (await runtime.source.checkout(owner.context, root.id)).digest,
      [definition],
    );
    await runtime.source.apply(
      owner.context,
      (await runtime.source.plan(owner.context, root.id, publication.digest)).id,
    );
    return publication.resources.find(
      (resource) => resource.reference === 'logic/Function/calculate',
    )!.id;
  }
  const resourceId = await publish();
  const objectStore = new ObjectStore(db.catalog),
    properties = {
      uid: { type: 'uid' as const, nullable: false },
      value: { type: 'float64' as const, nullable: false },
    },
    schema = { primaryKey: 'uid', properties };
  const state = await authority.current(environment, principal, application);
  await objectStore.activate(
    environment,
    'records',
    schema,
    [],
    state.epoch,
    (await db.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [environment]))
      .rows[0].revision,
  );
  let records = new ObjectService(objectStore, authority, {
    objectTypeId: '00000000-0000-4000-8000-000000000004',
    schema,
    policies: {
      resource: 'records',
      markings: [],
      properties,
      sourceColumns: properties,
      attributes: {},
    },
  });
  const edit: ActionBinding = {
    resource: 'edit-record',
    revision: '1',
    markings: [],
    targetParameter: 'uid',
    inputs: { uid: { type: 'uid', nullable: false }, value: { type: 'float64', nullable: false } },
    condition: { literal: true },
    edits: [{ op: 'set', property: 'value', value: { parameter: 'value' } }],
  };
  const actions = new Map([
    ['edit-record', { service: records, action: edit }],
    [
      'reject-record',
      {
        service: records,
        action: { ...edit, resource: 'reject-record', condition: { literal: false } },
      },
    ],
  ]);
  const objects = new Map([['records', records]]);
  const functions = new Functions(runtime, objects, worker, {
    actions,
    filesystem: functionFilesystem(runtime),
  });
  records.functions = functions;
  function configureRecords(
    change: (binding: import('../../../services/ontology/src/service.js').ObjectBinding) => void,
  ) {
    const binding = records.binding;
    change(binding);
    records = new ObjectService(objectStore, authority, binding);
    records.functions = functions;
    objects.set('records', records);
    for (const action of actions.values()) action.service = records;
  }
  functions.startWorker();
  const close = async () => {
    await functions.close();
    await db.close();
  };
  return {
    ...db,
    directory: db.root,
    close,
    runtime,
    authority,
    sessions,
    functions,
    owner,
    other,
    environment,
    application,
    operations,
    resourceId,
    root,
    declaration,
    publish,
    get records() {
      return records;
    },
    objects,
    configureRecords,
    objectStore,
    actions,
  };
}
