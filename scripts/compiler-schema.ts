import { readFileSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { format, resolveConfig } from 'prettier';
const require = createRequire(import.meta.url);
const Ajv = require('ajv/dist/2020').default;
const standalone = require('ajv/dist/standalone').default;
const ajv = new Ajv({
  allErrors: true,
  strict: false,
  validateFormats: true,
  inlineRefs: false,
  code: { source: true, esm: true },
});
require('ajv-formats')(ajv);
const load = (name: string) =>
  JSON.parse(
    readFileSync(
      new URL(`../packages/contracts/schemas/${name}.schema.json`, import.meta.url),
      'utf8',
    ),
  );
const values = load('values'),
  resources = load('resources'),
  manifest = load('manifest');
ajv.addSchema(values);
ajv.addSchema(resources);
ajv.addSchema(manifest);
const attributeId = 'https://spec.example.invalid/moria/compiler/principal-attributes';
ajv.addSchema({
  $id: attributeId,
  type: 'object',
  additionalProperties: { $ref: `${resources.$id}#/$defs/ValueType` },
});
let output = standalone(ajv, {
  validateShape: manifest.$id,
  validateAttributes: attributeId,
}) as string;
const imports = new Map<string, string>();
output = output.replace(/require\("([^"\n]+)"\)/g, (_whole: string, path: string) => {
  let name = imports.get(path);
  if (!name) {
    name = `runtime${imports.size}`;
    imports.set(path, name);
  }
  return name;
});
output =
  [...imports]
    .map(
      ([path, name]) =>
        `import * as ${name}Module from ${JSON.stringify(path.endsWith('.js') ? path : path + '.js')};\nconst ${name} = ${name}Module.default && typeof ${name}Module.default === 'object' ? ${name}Module.default : ${name}Module;`,
    )
    .join('\n') +
  '\n' +
  output +
  '\n';
const target = new URL('../packages/compiler/src/schema-validation.generated.js', import.meta.url);
output = await format(output, {
  ...(await resolveConfig(target.pathname)),
  filepath: target.pathname,
});
if (process.argv.includes('--check')) {
  if (readFileSync(target, 'utf8') !== output)
    throw new Error('Compiler schema validator is stale');
} else writeFileSync(target, output);
