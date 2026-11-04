import type { StorageLocation } from './storage.js';
import type { Manifest, ValueType } from './types.js';

export const filesystemRoot = '00000000-0000-4000-8000-000000000001';

export interface FilesystemScope {
  projectId: string;
  environmentId?: string;
}

export interface FileContent {
  location: StorageLocation;
  digest: string;
  size: number;
  mediaType: string;
}

export interface FilesystemEntry {
  id: string;
  parentId: string | null;
  name: string;
  kind: 'directory' | 'file' | 'resource';
  revision: string;
  projectId: string | null;
  environmentId: string | null;
  resourceId: string | null;
  content: FileContent | null;
  deleted: boolean;
}

export interface FilesystemActor {
  environment: string;
  principal: string;
  application: string;
  delegation?: string[];
}

export type FilesystemChange =
  | {
      op: 'create';
      parentId: string;
      expectedParentRevision: string;
      name: string;
      kind: FilesystemEntry['kind'];
      scope?: FilesystemScope;
      resourceId?: string;
      content?: FileContent;
    }
  | { op: 'write'; id: string; expectedRevision: string; content: FileContent }
  | {
      op: 'move';
      id: string;
      expectedRevision: string;
      parentId: string;
      expectedParentRevision: string;
      expectedSourceParentRevision: string;
      name: string;
    }
  | { op: 'delete'; id: string; expectedRevision: string; expectedParentRevision: string };

export interface FilesystemReceipt {
  mutationId: string;
  entries: FilesystemEntry[];
}

export const filesystemOperations = [
  'read',
  'list',
  'create',
  'write',
  'move',
  'delete',
  'export',
  'publish',
  'register',
  'administer',
] as const;
export type FilesystemOperation = (typeof filesystemOperations)[number];

export interface FilesystemMarking {
  environment: string;
  marking: string;
}

export interface FilesystemGrant {
  subject:
    | { kind: 'administrators' }
    | { kind: 'principal'; id: string }
    | { kind: 'group'; id: string; environment: string };
  operations: FilesystemOperation[];
  inherit: boolean;
  effect: 'allow' | 'deny';
}

export interface FilesystemPolicy {
  markings: FilesystemMarking[];
  grants: FilesystemGrant[];
}

export type FilesystemFileWrite =
  | {
      op: 'create';
      parentId: string;
      expectedParentRevision: string;
      name: string;
      content: Omit<FileContent, 'location'>;
    }
  | { op: 'write'; id: string; expectedRevision: string; content: Omit<FileContent, 'location'> };

export type SourceKind = 'ontology' | 'repository';
export interface SourceDescriptor {
  version: 1;
  name: string;
  namespace: string;
  runtime?: 'typescript-v1' | 'python-duckdb-v1';
  dependencies: { rootId: string; digest: string }[];
}
export interface SourceRole {
  entryId: string;
  kind: SourceKind;
  projectId: string;
  deploymentUnitId: string;
  namespace: string;
  configuration: SourceDescriptor;
  revision: string;
  latestRelease: string | null;
}
export interface SourceCheckout {
  version: 1;
  rootId: string;
  role: SourceRole;
  digest: string;
  files: { path: string; entry: FilesystemEntry }[];
  directories: { path: string; id: string; revision: string }[];
}
export interface SourcePublication {
  rootId: string;
  digest: string;
  sourceDigest: string;
  resources: { reference: string; id: string }[];
}
export interface SourceCompilerContext {
  revision: string;
  fingerprint: string;
  principalAttributes: Record<string, ValueType>;
}
export interface SourceCompilerArtifactRef {
  entryId: string;
  revision: string;
  digest: string;
}
export interface CompiledSourceArtifact {
  version: 1;
  rootId: string;
  sourceDigest: string;
  descriptor: SourceDescriptor;
  compiled: {
    manifest: Manifest;
    fingerprint: string;
    order: string[];
    policyTypesChecked: boolean;
    attributeTypesFingerprint?: string;
  };
  owned: string[];
  compilerContext: { principalAttributes: Record<string, ValueType> };
}
export interface SourceRuntimeStatus {
  /** Operational readiness is deliberately hidden from code-only publishers. */
  withheld?: boolean;
  reason?: string;
  version: 1;
  configured: boolean;
  ready: boolean;
  managedCellReady?: boolean;
  authority?: 'caller';
  configurationReady: boolean;
  profileFingerprint: string;
  components: {
    reference: string;
    kind: string;
    status: 'configured' | 'pending';
    reasons: string[];
    readinessReasons?: string[];
    readiness?: unknown;
  }[];
}
export interface SourcePlan {
  id: string;
  rootId: string;
  digest: string;
  environment: string;
  expectedRevision: string;
  resources: string[];
  deletions: string[];
  expiresAt: string;
  runtime?: SourceRuntimeStatus;
}
export interface SourceActivation {
  rootId: string;
  environment: string;
  digest: string;
  revision: string;
  resources: string[];
  runtimeReady: boolean;
  runtimeConfigured?: boolean;
  runtime?: SourceRuntimeStatus;
}
