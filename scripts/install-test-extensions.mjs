import { copyFileSync, mkdirSync, readdirSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, resolve } from 'node:path';

if (process.platform !== 'linux' || process.arch !== 'x64')
  throw new Error('Pinned native PostgreSQL extensions require Linux x64.');
const require = createRequire(import.meta.url);
const embedded = createRequire(require.resolve('embedded-postgres'));
const root = resolve(dirname(embedded.resolve('@embedded-postgres/linux-x64')), '../native');
const destination = resolve(root, 'share/postgresql/extension');
mkdirSync(destination, { recursive: true });
copyFileSync('/opt/moria-pgvector/vector.so', resolve(root, 'lib/postgresql/vector.so'));
for (const file of readdirSync('/opt/moria-pgvector/extension')) {
  if (!/^vector(?:\.control|--[0-9.]+(?:--[0-9.]+)?\.sql)$/.test(file))
    throw new Error('Unexpected native extension artifact.');
  copyFileSync(resolve('/opt/moria-pgvector/extension', file), resolve(destination, file));
}
