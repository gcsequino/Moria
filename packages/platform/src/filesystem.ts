import type { Transport, RequestOptions, ActionOptions } from './transport.js';
import type {
  FilesystemEntry,
  FilesystemChange,
  FilesystemReceipt,
  FilesystemPolicy,
  FilesystemFileWrite,
  SourceKind,
  SourceRole,
  SourceCheckout,
  SourcePublication,
  SourcePlan,
  SourceActivation,
  FilesystemMarking,
  SourceCompilerContext,
  SourceCompilerArtifactRef,
  SourceRuntimeStatus,
} from '../../contracts/filesystem.js';

export function filesystemClient(transport: Transport) {
  const route = '/api/management/v1/fs',
    target = (id: string) => `${route}/entries/${encodeURIComponent(id)}`;
  return {
    setEnabled: (id: string, enabled: boolean, settings?: RequestOptions) =>
      transport.request<{ id: string; enabled: boolean }>(
        `${route}/principals`,
        'POST',
        { id, enabled },
        settings,
      ),
    retire: (id: string, expectedRevision: string, settings?: RequestOptions) =>
      transport.request<{ entryId: string; retired: boolean }>(
        `${route}/sources/${encodeURIComponent(id)}/retire`,
        'POST',
        { expectedRevision },
        settings,
      ),
    collect: (settings?: RequestOptions) =>
      transport.request<{
        removed: number;
        retained: number;
        examined: number;
        stagingRemoved?: number;
      }>(`${route}/collect`, 'POST', {}, settings),
    home: (settings?: RequestOptions) =>
      transport.request<{ principalId: string; home: FilesystemEntry }>(
        `${route}/home`,
        'GET',
        undefined,
        settings,
      ),
    resolve: (path: string, settings?: RequestOptions) =>
      transport.request<FilesystemEntry>(`${route}/resolve`, 'POST', { path }, settings),
    get: (id: string, settings?: RequestOptions) =>
      transport.request<FilesystemEntry>(target(id), 'GET', undefined, settings),
    resource: (reference: string, settings?: RequestOptions & { projectId?: string }) =>
      transport.request<{
        entryId: string;
        resourceId: string;
        digest: string;
        resource: unknown;
        markings: FilesystemMarking[];
      }>(
        `${route}/resources`,
        'POST',
        { reference, ...(settings?.projectId ? { projectId: settings.projectId } : {}) },
        settings,
      ),
    version: (id: string, revision: string, settings?: RequestOptions) =>
      transport.request<FilesystemEntry>(
        `${target(id)}/version?${new URLSearchParams({ revision })}`,
        'GET',
        undefined,
        settings,
      ),
    history: (id: string, before?: string, settings?: RequestOptions) =>
      transport.request<{ entries: FilesystemEntry[]; nextRevision?: string }>(
        `${target(id)}/history${before ? `?${new URLSearchParams({ before })}` : ''}`,
        'GET',
        undefined,
        settings,
      ),
    list: (id: string, cursor?: string, settings?: RequestOptions) =>
      transport.request<{ entries: FilesystemEntry[]; nextCursor?: string }>(
        `${target(id)}/children${cursor ? `?${new URLSearchParams({ cursor })}` : ''}`,
        'GET',
        undefined,
        settings,
      ),
    changes: (changes: FilesystemChange[], settings: ActionOptions) =>
      transport.request<FilesystemReceipt>(`${route}/changes`, 'POST', { changes }, settings),
    copy: (
      request: {
        id: string;
        expectedRevision: string;
        parentId: string;
        expectedParentRevision: string;
        name: string;
      },
      settings: ActionOptions,
    ) => transport.request<FilesystemReceipt>(`${route}/copy`, 'POST', request, settings),
    role: (id: string, settings?: RequestOptions) =>
      transport.request<null | {
        kind: string;
        projectId: string | null;
        deploymentUnitId: string | null;
        namespace: string | null;
        configuration: Record<string, unknown>;
        revision: string;
        latestRelease: string | null;
        retired: boolean;
      }>(`${target(id)}/role`, 'GET', undefined, settings),
    policy: (id: string, settings?: RequestOptions) =>
      transport.request<
        FilesystemPolicy & { revision: string; retainedMarkings: FilesystemPolicy['markings'] }
      >(`${target(id)}/policy`, 'GET', undefined, settings),
    setPolicy: (
      id: string,
      expectedRevision: string,
      policy: FilesystemPolicy,
      settings?: RequestOptions,
    ) =>
      transport.request<{ revision: string }>(
        `${target(id)}/policy`,
        'POST',
        { expectedRevision, policy },
        settings,
      ),
    createProject: (
      parentId: string,
      name: string,
      expectedRevision: string,
      settings?: RequestOptions,
    ) =>
      transport.request<{ projectId: string; entry: FilesystemEntry }>(
        `${route}/projects`,
        'POST',
        { parentId, name, expectedRevision },
        settings,
      ),
    createGroup: (
      name: string,
      groupKey: string,
      expectedRevision: string,
      settings?: RequestOptions,
    ) =>
      transport.request<{ id: string; entry: FilesystemEntry }>(
        `${route}/groups`,
        'POST',
        { name, groupKey, expectedRevision },
        settings,
      ),
    async writeFile(input: FilesystemFileWrite, bytes: Uint8Array, settings: ActionOptions) {
      const query =
        input.op === 'create'
          ? new URLSearchParams({
              name: input.name,
              expectedRevision: input.expectedParentRevision,
            })
          : new URLSearchParams({ expectedRevision: input.expectedRevision });
      const path =
        input.op === 'create' ? `${target(input.parentId)}/files` : `${target(input.id)}/content`;
      const response = await transport.response(
        `${path}?${query}`,
        'POST',
        new Uint8Array(bytes),
        {
          'content-type': input.content.mediaType,
          'x-moria-size': String(input.content.size),
          'x-moria-digest': input.content.digest,
        },
        settings,
      );
      return (await response.json()) as FilesystemReceipt;
    },
    readFile: (id: string, revision?: string, settings?: RequestOptions) =>
      transport.response(
        `${target(id)}/content${revision ? `?${new URLSearchParams({ revision })}` : ''}`,
        'GET',
        undefined,
        {},
        settings,
      ),
    register: (id: string, kind: SourceKind, expectedRevision: string, settings?: RequestOptions) =>
      transport.request<SourceRole>(
        `${route}/sources/${encodeURIComponent(id)}/register`,
        'POST',
        { kind, expectedRevision },
        settings,
      ),
    checkout: (id: string, settings?: RequestOptions) =>
      transport.request<SourceCheckout>(
        `${route}/sources/${encodeURIComponent(id)}/checkout`,
        'POST',
        {},
        settings,
      ),
    publish: (
      id: string,
      expectedSourceDigest: string,
      exports?: unknown[],
      settings?: RequestOptions,
    ) =>
      transport.request<SourcePublication>(
        `${route}/sources/${encodeURIComponent(id)}/publish`,
        'POST',
        { expectedSourceDigest, ...(exports ? { exports } : {}) },
        settings,
      ),
    compilerContext: (settings?: RequestOptions) =>
      transport.request<SourceCompilerContext>(
        `${route}/compiler-context`,
        'GET',
        undefined,
        settings,
      ),
    runtimeStatus: (id: string, settings?: RequestOptions) =>
      transport.request<SourceRuntimeStatus>(
        `${route}/sources/${encodeURIComponent(id)}/runtime`,
        'GET',
        undefined,
        settings,
      ),
    publishArtifact: (
      id: string,
      expectedSourceDigest: string,
      artifact: SourceCompilerArtifactRef,
      settings?: RequestOptions,
    ) =>
      transport.request<SourcePublication>(
        `${route}/sources/${encodeURIComponent(id)}/publish`,
        'POST',
        { expectedSourceDigest, artifact },
        settings,
      ),
    async writeCompilerArtifact(
      rootId: string,
      sourceDigest: string,
      parentId: string,
      name: string,
      expectedRevision: string,
      bytes: Uint8Array,
      settings: ActionOptions,
    ) {
      if (bytes.length > 16 * 1024 * 1024) throw new Error('Compiler artifact exceeds16MiB');
      const digest = await crypto.subtle.digest('SHA-256', new Uint8Array(bytes));
      const hash = `sha256:${Array.from(new Uint8Array(digest), (value) => value.toString(16).padStart(2, '0')).join('')}`;
      const response = await transport.response(
        `${route}/sources/${encodeURIComponent(rootId)}/artifacts?${new URLSearchParams({ sourceDigest, parentId, name, expectedRevision })}`,
        'POST',
        new Uint8Array(bytes),
        {
          'content-type': 'application/json',
          'x-moria-size': String(bytes.length),
          'x-moria-digest': hash,
        },
        settings,
      );
      return (await response.json()) as FilesystemReceipt;
    },
    plan: (id: string, digest: string, deletions: string[] = [], settings?: RequestOptions) =>
      transport.request<SourcePlan>(
        `${route}/sources/${encodeURIComponent(id)}/plan`,
        'POST',
        { digest, deletions },
        settings,
      ),
    apply: (planId: string, settings?: RequestOptions) =>
      transport.request<SourceActivation>(
        `${route}/plans/${encodeURIComponent(planId)}/apply`,
        'POST',
        {},
        settings,
      ),
    release: (digest: string, settings?: RequestOptions) =>
      transport.request<unknown>(`${route}/releases/read`, 'POST', { digest }, settings),
  };
}
