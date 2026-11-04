import { canonicalJson as canonical } from '../../contracts/values.js';
import { MoriaError, resourceRef, type Manifest, type Resource } from '../../contracts/types.js';

import type { SourceKind, SourceDescriptor } from '../../contracts/filesystem.js';
export type { SourceKind, SourceDescriptor } from '../../contracts/filesystem.js';
export interface SourceFile {
  path: string;
  text: string;
}

function invalid(message: string): never {
  throw new MoriaError('SOURCE_INVALID', message);
}
function parse(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return invalid('Invalid source JSON');
  }
}
export function sourcePath(path: unknown): asserts path is string {
  if (
    typeof path !== 'string' ||
    !path ||
    new TextDecoder('utf-8').decode(new TextEncoder().encode(path)) !== path ||
    new TextEncoder().encode(path).length > 4096 ||
    path.split('/').length > 64 ||
    path
      .split('/')
      .some(
        (part) =>
          !part ||
          part === '.' ||
          part === '..' ||
          part !== part.normalize('NFC') ||
          new TextEncoder().encode(part).length > 128 ||
          /[\x00-\x1f\x7f\\]/.test(part),
      )
  )
    invalid('Invalid source path');
}
export function sourceDescriptor(kind: SourceKind, text: string): SourceDescriptor {
  const value = parse(text) as Record<string, unknown>;
  if (
    !value ||
    typeof value !== 'object' ||
    Array.isArray(value) ||
    Object.keys(value).some(
      (key) => !['version', 'name', 'namespace', 'runtime', 'dependencies'].includes(key),
    ) ||
    value.version !== 1 ||
    typeof value.name !== 'string' ||
    !value.name ||
    value.name.length > 128 ||
    typeof value.namespace !== 'string' ||
    !/^[A-Za-z_][A-Za-z0-9_-]{0,127}$/.test(value.namespace)
  )
    invalid('Invalid source descriptor');
  if (
    kind === 'repository'
      ? !['typescript-v1', 'python-duckdb-v1'].includes(value.runtime as string)
      : value.runtime !== undefined
  )
    invalid('Unsupported source runtime');
  const dependencies = value.dependencies ?? [];
  if (!Array.isArray(dependencies) || dependencies.length > 16)
    invalid('Too many source dependencies');
  const seen = new Set<string>();
  for (const dependency of dependencies) {
    if (
      !dependency ||
      typeof dependency !== 'object' ||
      Object.keys(dependency).sort().join(',') !== 'digest,rootId' ||
      typeof dependency.rootId !== 'string' ||
      !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(dependency.rootId) ||
      typeof dependency.digest !== 'string' ||
      !/^sha256:[0-9a-f]{64}$/.test(dependency.digest) ||
      seen.has(dependency.rootId)
    )
      invalid('Invalid source dependency');
    seen.add(dependency.rootId);
  }
  return {
    version: 1,
    name: value.name,
    namespace: value.namespace,
    ...(value.runtime ? { runtime: value.runtime as SourceDescriptor['runtime'] } : {}),
    dependencies,
  };
}

/** Source is data here. Repository exports are emitted by CI; no source module is loaded. */
export function assembleSource(input: {
  kind: SourceKind;
  projectId: string;
  deploymentUnitId: string;
  files: SourceFile[];
  exports?: Resource[];
  imports?: Manifest[];
}) {
  if (
    !['ontology', 'repository'].includes(input.kind) ||
    !Array.isArray(input.files) ||
    input.files.length > 2000 ||
    (input.exports !== undefined &&
      (!Array.isArray(input.exports) || input.exports.length > 2000)) ||
    (input.imports !== undefined && (!Array.isArray(input.imports) || input.imports.length > 16))
  )
    invalid('Invalid source unit');
  const files = new Map<string, string>();
  for (const file of input.files) {
    sourcePath(file.path);
    if (
      files.has(file.path) ||
      typeof file.text !== 'string' ||
      new TextEncoder().encode(file.text).length > 16 * 1024 * 1024
    )
      invalid('Invalid or duplicate source file');
    if (
      (file.path.endsWith('.ontology.json') || file.path.endsWith('.repository.json')) &&
      file.path !== `.${input.kind}.json`
    )
      invalid('Nested source roots are unsupported');
    files.set(file.path, file.text);
  }
  const descriptorText = files.get(`.${input.kind}.json`);
  if (descriptorText === undefined) invalid('Source descriptor is required');
  const descriptor = sourceDescriptor(input.kind, descriptorText);
  const resources: Resource[] = [
    {
      apiVersion: 'moria/v1alpha2',
      kind: 'Namespace',
      metadata: { namespace: '_project', name: descriptor.namespace },
      spec: { description: descriptor.name },
    },
  ];
  for (const [path, text] of files) {
    const extension = /\.(object|link|action|resource)\.json$/.exec(path)?.[1];
    if (!extension) continue;
    const resource = parse(text) as Resource;
    if (
      !resource ||
      typeof resource !== 'object' ||
      resource.metadata?.namespace !== descriptor.namespace
    )
      invalid('Resource must belong to its source namespace');
    const expected = { object: 'ObjectType', link: 'LinkType', action: 'Action' }[
      extension as 'object' | 'link' | 'action'
    ];
    if (expected && (input.kind !== 'ontology' || resource.kind !== expected))
      invalid('Ontology declarations require their ontology root and matching file kind');
    if (
      extension === 'resource' &&
      [
        'Namespace',
        'ObjectType',
        'LinkType',
        'Action',
        'Function',
        'Transform',
        'Agent',
        'AgentAdapter',
        'ConnectorImplementation',
      ].includes(resource.kind)
    )
      invalid('Resource requires its ontology file or repository export');
    resources.push(resource);
  }
  if (input.kind === 'ontology' && input.exports?.length)
    invalid('Ontology code must be imported from a repository');
  for (const resource of input.exports ?? []) {
    if (
      !resource ||
      typeof resource !== 'object' ||
      Array.isArray(resource) ||
      input.kind !== 'repository' ||
      !['Function', 'Transform', 'Agent', 'AgentAdapter', 'ConnectorImplementation'].includes(
        resource.kind,
      ) ||
      resource.metadata?.namespace !== descriptor.namespace
    )
      invalid('Invalid repository export');
    if (
      (resource.kind === 'Function' && descriptor.runtime !== 'typescript-v1') ||
      (resource.kind === 'Transform' && descriptor.runtime !== 'python-duckdb-v1') ||
      (['Agent', 'AgentAdapter'].includes(resource.kind) && descriptor.runtime !== 'typescript-v1')
    )
      invalid('Export runtime differs from repository');
    resources.push(resource);
  }
  const owned = resources.map(resourceRef),
    combined = [...resources];
  if (new Set(owned).size !== owned.length) invalid('Duplicate source resource');
  for (const imported of input.imports ?? [])
    for (const resource of imported.resources) {
      const old = combined.find((item) => resourceRef(item) === resourceRef(resource));
      if (old && (owned.includes(resourceRef(resource)) || canonical(old) !== canonical(resource)))
        invalid('Dependency conflicts with a declared resource');
      if (!old) combined.push(resource);
    }
  const manifest: Manifest = {
    apiVersion: 'moria/v1alpha2',
    projectId: input.projectId,
    deploymentUnitId: input.deploymentUnitId,
    resources: combined,
  };
  return { descriptor, manifest, owned };
}
