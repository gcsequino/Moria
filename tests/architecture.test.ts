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
    '{"compilerOptions":{"target":"ES2024","module":"NodeNext","moduleResolution":"NodeNext","allowJs":true},"include":["**/*.ts","**/*.mjs"]}',
  );
  write('services/ontology/src/store.ts', 'export class ObjectStore { read() { return 1; } }');
  const service =
    "import type { ObjectStore } from './store.js'; export class ObjectService { constructor(readonly store: ObjectStore) {} read() {} query() {} invoke() {} requiring() {} }";
  write('services/ontology/src/service.ts', service);
  for (const path of [
    'apps/api/src/server.ts',
    'apps/local/src/server.ts',
    'services/app-host/src/scoped.ts',
    'services/app-host/src/static-server.ts',
    'apps/api/src/function-platform.ts',
    'apps/cli/src/filesystem.ts',
    'services/functions/src/docker.ts',
    'workers/functions/runner.mjs',
    'tests/functions-runner.test.ts',
    'tests/functions.integration.test.ts',
    'tests/functions-platform.integration.test.ts',
    'tests/filesystem-http.integration.test.ts',
    'tests/http-boundary.test.ts',
    'tests/static-host-process.integration.test.ts',
    'apps/local/src/development-database.ts',
    'tests/catalog-storage-database.test.ts',
    'workers/iceberg/stage.ts',
    'workers/iceberg/stream.ts',
    'workers/iceberg/scoped-read.ts',
    'services/catalog/src/storage-publication.ts',
    'services/catalog/src/snapshot-publication.ts',
    'tests/process-lifecycle.test.ts',
    'tests/catalog-storage.integration.test.ts',
    'tests/catalog-storage-publication.integration.test.ts',
    'tests/catalog-reference-recovery.integration.test.ts',
    'tests/sql.integration.test.ts',
    'tests/api-only-process.integration.test.ts',
    'tests/platform-composition.test.ts',
  ])
    write(path, '');
  assert.deepEqual(architectureViolations(root), []);
  write('node_modules/@types/node/package.json', '{"types":"http.d.ts"}');
  write(
    'node_modules/@types/node/http.d.ts',
    'export interface Server { on(event: string, callback: () => void): this; emit(event: string): boolean; removeAllListeners(): this; listeners(event: string): unknown[]; requestTimeout: number; }',
  );
  write(
    'services/http/src/server.ts',
    'export function createHttpServer() {} export function browserProfile() {}',
  );
  write('services/process/src/python.ts', 'export function openPython() {}');
  write(
    'node_modules/@types/node/process.d.ts',
    'export declare const process: { kill(pid: number, signal?: number | string): void; getBuiltinModule(name: string): any };',
  );
  write(
    'node_modules/@types/node/child_process.d.ts',
    'export interface ChildProcess { kill(signal?: string): void; }',
  );
  const cases = [
    [
      'builtin module loader',
      'apps/api/src/new.ts',
      "import { process } from '../../../node_modules/@types/node/process.js'; process.getBuiltinModule('child_process').spawn('python');",
      /Builtin capability loaders/,
    ],
    [
      'aliased builtin module loader',
      'apps/api/src/new.ts',
      "import { process } from '../../../node_modules/@types/node/process.js'; const { getBuiltinModule: load } = process; load('child_process').spawn('python');",
      /Builtin capability loaders/,
    ],

    [
      'raw spawn alias',
      'apps/api/src/new.ts',
      "import { spawn as raw } from 'node:child_process'; raw('python');",
      /runtime module node:child_process/,
    ],
    [
      'process re-export',
      'apps/api/src/new.ts',
      "export { fork } from 'child_process';",
      /runtime module child_process/,
    ],
    [
      'dynamic process import',
      'apps/api/src/new.ts',
      "import('node:child_process');",
      /runtime module node:child_process/,
    ],
    [
      'required process import',
      'apps/api/src/new.ts',
      "require('child_process');",
      /runtime module child_process/,
    ],
    [
      'unregistered lifecycle caller',
      'apps/api/src/new.ts',
      "import { openPython } from '../../../services/process/src/python.js'; openPython();",
      /registered boundary|boundary loader/,
    ],
    [
      'indirect lifecycle loader',
      'packages/process-bridge.ts',
      "export { openPython } from '../services/process/src/python.js';",
      /boundary loader/,
    ],
    [
      'raw process termination',
      'apps/api/src/new.ts',
      "import { process as raw } from '../../../node_modules/@types/node/process.js'; raw.kill(1, 'SIGTERM');",
      /Process termination/,
    ],
    [
      'raw child termination',
      'apps/api/src/new.ts',
      "import type { ChildProcess } from '../../../node_modules/@types/node/child_process.js'; export const stop = (child: ChildProcess) => child.kill('SIGKILL');",
      /Process termination/,
    ],

    [
      'raw HTTP listener alias',
      'apps/api/src/new.ts',
      "import { createServer as raw } from 'node:http'; raw();",
      /runtime module node:http/,
    ],
    ['raw HTTP re-export', 'apps/api/src/new.ts', "export * from 'http';", /runtime module http/],
    [
      'dynamic HTTPS listener',
      'apps/api/src/new.ts',
      "import('node:https');",
      /runtime module node:https/,
    ],
    [
      'unregistered boundary listener',
      'apps/api/src/new.ts',
      "import { createHttpServer } from '../../../services/http/src/server.js'; createHttpServer();",
      /registered boundary|boundary loader/,
    ],
    [
      'unregistered profile',
      'apps/api/src/new.ts',
      "import { browserProfile } from '../../../services/http/src/server.js'; browserProfile();",
      /registered boundary|boundary loader/,
    ],
    [
      'request event bypass',
      'apps/api/src/new.ts',
      "import type { Server } from '../../../node_modules/@types/node/http.js'; export const attach = (server: Server) => server.on('request', () => {});",
      /HTTP request listeners/,
    ],
    [
      'request emit bypass',
      'apps/api/src/new.ts',
      "import type { Server } from '../../../node_modules/@types/node/http.js'; export const attach = (server: Server) => server.emit('request');",
      /HTTP request listeners/,
    ],
    [
      'request listener removal',
      'apps/api/src/new.ts',
      "import type { Server } from '../../../node_modules/@types/node/http.js'; export const attach = (server: Server) => server.removeAllListeners();",
      /HTTP request listeners/,
    ],
    [
      'request listener retrieval',
      'apps/api/src/new.ts',
      "import type { Server } from '../../../node_modules/@types/node/http.js'; export const leak = (server: Server) => server.listeners('request');",
      /HTTP request listeners/,
    ],
    [
      'timeout override',
      'apps/api/src/new.ts',
      "import type { Server } from '../../../node_modules/@types/node/http.js'; export const disable = (server: Server) => server.requestTimeout = 0;",
      /HTTP timeout and limit/,
    ],
    [
      'outbound client exception cannot construct servers',
      'services/storage/src/s3.ts',
      "import { Agent, createServer } from 'node:http'; createServer();",
      /runtime module node:http/,
    ],

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
