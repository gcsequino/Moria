import { MoriaError, resourceRef, type Manifest } from '../../contracts/types.js';
import { canonicalJson } from '../../contracts/values.js';
import { validateManifestWith, type CompilerContext, type CompiledManifest } from './core.js';
import { assembleSource } from './source-core.js';

export async function fingerprint(value: unknown): Promise<string> {
  return digest(new TextEncoder().encode(canonicalJson(value)));
}
export async function digest(bytes: Uint8Array): Promise<string> {
  const hash = await crypto.subtle.digest('SHA-256', new Uint8Array(bytes));
  return (
    'sha256:' +
    Array.from(new Uint8Array(hash), (byte) => byte.toString(16).padStart(2, '0')).join('')
  );
}
/** Dynamic Connector configuration schemas are checked by the native publisher before publication. */
export async function compileManifest(
  input: unknown,
  context?: CompilerContext,
): Promise<CompiledManifest> {
  const diagnostics = validateManifestWith(input, context, () => true);
  if (diagnostics.length)
    throw new MoriaError('VALIDATION_FAILED', 'Manifest validation failed', diagnostics);
  const manifest = input as Manifest;
  return {
    manifest,
    fingerprint: await fingerprint(manifest),
    order: manifest.resources.map(resourceRef).sort(),
    policyTypesChecked: !!context,
    ...(context
      ? { attributeTypesFingerprint: await fingerprint(context.principalAttributes) }
      : {}),
  };
}
export async function compileSource(
  input: Parameters<typeof assembleSource>[0],
  context?: CompilerContext,
) {
  const { descriptor, manifest, owned } = assembleSource(input);
  return { descriptor, compiled: await compileManifest(manifest, context), owned };
}
