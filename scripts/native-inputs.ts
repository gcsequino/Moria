import { readdirSync } from 'node:fs';

export const nativeRoot = new URL('../services/runtime/', import.meta.url);

export function nativeInputs(): string[] {
  const paths = ['Cargo.toml', 'Cargo.lock', 'build.rs', 'schema.json'];
  function walk(directory: string, suffix: string) {
    for (const file of readdirSync(new URL(directory + '/', nativeRoot), { withFileTypes: true })) {
      const path = `${directory}/${file.name}`;
      if (file.isSymbolicLink()) throw new Error('Native source inputs cannot be symlinks.');
      if (file.isDirectory()) walk(path, suffix);
      else if (file.name.endsWith(suffix)) paths.push(path);
    }
  }
  walk('src', '.rs');
  walk('protocol', '.proto');
  for (const entry of readdirSync(new URL('../../tests/fixtures/', nativeRoot), {
    withFileTypes: true,
  }))
    if (entry.name.startsWith('native-') && entry.isDirectory())
      walk(`../../tests/fixtures/${entry.name}`, '.rs');
  for (const name of readdirSync(new URL('../../packages/contracts/', nativeRoot)))
    if (name.endsWith('-conformance.json')) paths.push(`../../packages/contracts/${name}`);
  for (const name of ['manifest.schema.json', 'resources.schema.json', 'values.schema.json'])
    paths.push(`../../packages/contracts/schemas/${name}`);
  return paths.sort();
}
