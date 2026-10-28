import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync, unlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { architectureViolations } from '../scripts/check-architecture.js';

test('production protected-operation ownership is complete', () => {
  assert.deepEqual(architectureViolations(process.cwd()), []);
});

test('architecture checks reject direct, indirect and unregistered capability access', async (t) => {
  const root = mkdtempSync(join(tmpdir(), 'moria-boundary-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const write = (path: string, text: string) => {
    mkdirSync(dirname(join(root, path)), { recursive: true });
    writeFileSync(join(root, path), text);
  };
  write('package.json', '{"type":"module"}');
  write(
    'tsconfig.json',
    '{"compilerOptions":{"target":"ES2024","module":"NodeNext","moduleResolution":"NodeNext"},"include":["**/*.ts"]}',
  );
  write('services/ontology/src/store.ts', 'export class ObjectStore { read() { return 1; } }');
  const service =
    "import type { ObjectStore } from './store.js'; export class ObjectService { constructor(readonly store: ObjectStore) {} read() {} query() {} invoke() {} requiring() {} }";
  write('services/ontology/src/service.ts', service);
  for (const path of [
    'apps/api/src/server.ts',
    'apps/local/src/server.ts',
    'services/app-host/src/scoped.ts',
    'tests/sql.integration.test.ts',
    'tests/api-only-process.integration.test.ts',
    'tests/platform-composition.test.ts',
  ])
    write(path, '');
  assert.deepEqual(architectureViolations(root), []);
  const cases = [
    [
      'direct aliased store import',
      'apps/api/src/new.ts',
      "import { ObjectStore as Raw } from '../../../services/ontology/src/store.js'; new Raw().read();",
      /ObjectStore/,
    ],
    [
      'hidden store through service',
      'apps/api/src/server.ts',
      "import type { ObjectService } from '../../../services/ontology/src/service.js'; export const leak = (service: ObjectService) => service.store.read();",
      /ObjectStore/,
    ],
    [
      'literal require cannot erase capability types',
      'apps/api/src/new.ts',
      "const { ObjectStore } = require('../../../services/ontology/src/store.js'); new ObjectStore().read();",
      /ObjectStore/,
    ],
    [
      'absolute require cannot bypass the owner',
      'apps/api/src/new.ts',
      `require(${JSON.stringify(join(root, 'services/ontology/src/store.ts'))});`,
      /ObjectStore/,
    ],
    [
      'source file URL cannot bypass the owner',
      'apps/api/src/new.ts',
      `import(${JSON.stringify(new URL('file://' + join(root, 'services/ontology/src/store.ts')).href)});`,
      /ObjectStore/,
    ],
    [
      'literal dynamic import cannot bypass the owner',
      'apps/api/src/new.ts',
      "import('../../../services/ontology/src/store.js');",
      /ObjectStore/,
    ],
    [
      'new unregistered service caller',
      'apps/api/src/new.ts',
      "import type { ObjectService } from '../../../services/ontology/src/service.js'; export const read = (service: ObjectService) => service.read();",
      /ObjectService/,
    ],
    ['runtime database import', 'apps/api/src/new.ts', "import('pg');", /runtime module pg/],
    [
      'computed import',
      'apps/api/src/new.ts',
      'export const load = (path: string) => import(path);',
      /Computed capability imports/,
    ],
    [
      'new unregistered public operation',
      'services/ontology/src/service.ts',
      service.replace('invoke() {}', 'invoke() {} leak() {}'),
      /Public object operations/,
    ],
  ] as const;
  for (const [label, path, content, error] of cases) {
    await t.test(label, () => {
      write(path, content);
      assert.match(architectureViolations(root).join('\n'), error);
      if (path === 'services/ontology/src/service.ts') write(path, service);
      else if (path === 'apps/api/src/server.ts') write(path, '');
      else unlinkSync(join(root, path));
    });
  }
  await t.test('re-export cannot launder a raw store', () => {
    write(
      'packages/bridge.ts',
      "export { ObjectStore as Wrapped } from '../services/ontology/src/store.js';",
    );
    write(
      'apps/api/src/new.ts',
      "import { Wrapped } from '../../../packages/bridge.js'; new Wrapped().read();",
    );
    assert.match(architectureViolations(root).join('\n'), /packages\/bridge.ts.*ObjectStore/);
    unlinkSync(join(root, 'packages/bridge.ts'));
    unlinkSync(join(root, 'apps/api/src/new.ts'));
  });
  await t.test('type-only contracts do not grant persistence access', () => {
    write(
      'apps/api/src/new.ts',
      "import type { ObjectStore } from '../../../services/ontology/src/store.js'; export type Schema = Pick<ObjectStore, 'read'>;",
    );
    assert.deepEqual(architectureViolations(root), []);
    unlinkSync(join(root, 'apps/api/src/new.ts'));
  });
  await t.test('unsupported production source cannot hide outside tsconfig', () => {
    write('apps/api/src/unregistered.mts', 'export const value = 1;');
    assert.throws(() => architectureViolations(root), /Cannot parse/);
    unlinkSync(join(root, 'apps/api/src/unregistered.mts'));
  });
  await t.test('missing registered contract test fails', () => {
    unlinkSync(join(root, 'tests/sql.integration.test.ts'));
    assert.match(architectureViolations(root).join('\n'), /Missing contract test/);
  });
});
