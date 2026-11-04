import { compileManifest, type CompilerContext } from './index.js';
import { assembleSource } from './source-core.js';
export { sourcePath, sourceDescriptor } from './source-core.js';
export type { SourceFile } from './source-core.js';
export type { SourceKind, SourceDescriptor } from '../../contracts/filesystem.js';
export function compileSource(
  input: Parameters<typeof assembleSource>[0],
  context?: CompilerContext,
) {
  const { descriptor, manifest, owned } = assembleSource(input);
  return { descriptor, compiled: compileManifest(manifest, context), owned };
}
