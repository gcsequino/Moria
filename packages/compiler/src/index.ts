import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { MoriaError, resourceRef, type Manifest, type TypeSchema } from '../../contracts/types.js';
import { canonicalJson as canonical, canonicalValue } from '../../contracts/values.js';
import { validateManifestWith, type CompilerContext, type CompiledManifest } from './core.js';
export type { CompilerContext, CompiledManifest } from './core.js';
export { canonical };
const require = createRequire(import.meta.url);
const Ajv = require('ajv/dist/2020').default;
const ajv = new Ajv({ allErrors: true, strict: false, validateFormats: true });
require('ajv-formats')(ajv);
for (const name of ['values', 'resources', 'manifest'])
  ajv.addSchema(
    JSON.parse(
      readFileSync(
        new URL('../../contracts/schemas/' + name + '.schema.json', import.meta.url),
        'utf8',
      ),
    ),
  );
export const fingerprint = (value: unknown): string =>
  'sha256:' + createHash('sha256').update(canonical(value)).digest('hex');
export const valueFingerprint = (
  value: unknown,
  type: TypeSchema,
  definitions: Record<string, TypeSchema> = {},
): string =>
  'sha256:' +
  createHash('sha256')
    .update(canonicalValue(value, type, definitions))
    .digest('hex');
export function validateManifest(input: unknown, context?: CompilerContext) {
  return validateManifestWith(input, context, (schema, configuration) =>
    ajv.compile(schema)(configuration),
  );
}
export function compileManifest(input: unknown, context?: CompilerContext): CompiledManifest {
  const diagnostics = validateManifest(input, context);
  if (diagnostics.length)
    throw new MoriaError('VALIDATION_FAILED', 'Manifest validation failed', diagnostics);
  const manifest = input as Manifest;
  return {
    manifest,
    fingerprint: fingerprint(manifest),
    order: manifest.resources.map(resourceRef).sort(),
    policyTypesChecked: !!context,
    ...(context ? { attributeTypesFingerprint: fingerprint(context.principalAttributes) } : {}),
  };
}
