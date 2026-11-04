/** Reviewed implementation owners; changes to this file require invariant review. */
export const protectedClasses: Record<string, readonly string[]> = {
  'services/security/src/authority.ts#Authority': [
    'services/filesystem/src/local.ts',
    'services/filesystem/src/management.ts',
    'services/filesystem/src/service.ts',
    'services/functions/src/service.ts',
    'services/security/src/filesystem.ts',
    'services/security/src/authority.ts',
    'services/security/src/sessions.ts',
    'services/security/src/browser-sessions.ts',
    'services/ontology/src/service.ts',
    'services/ontology/src/indexer.ts',
    'services/catalog/src/storage.ts',
    'services/snotel/src/service.ts',
    'apps/local/src/workspace.ts',
    'apps/local/src/native-runtime.ts',
  ],
  'services/catalog/src/catalog.ts#Catalog': [
    'services/filesystem/src/local.ts',
    'services/filesystem/src/management.ts',
    'services/filesystem/src/service.ts',
    'services/filesystem/src/source.ts',
    'services/filesystem/src/store.ts',
    'services/functions/src/service.ts',
    'services/security/src/filesystem.ts',
    'services/catalog/src/catalog.ts',
    'services/catalog/src/storage.ts',
    'services/security/src/authority.ts',
    'services/security/src/sessions.ts',
    'services/security/src/browser-sessions.ts',
    'services/ontology/src/store.ts',
    'services/ontology/src/service.ts',
    'services/ontology/src/cursors.ts',
    'services/ontology/src/indexer.ts',
    'services/ontology/src/lease.ts',
    'services/execution/src/jobs.ts',
    'services/execution/src/snow-jobs.ts',
    'services/weather/src/service.ts',
    'services/snotel/src/service.ts',
    'services/lineage/src/service.ts',
    'apps/local/src/workspace.ts',
    'apps/local/src/runtime.ts',
    'apps/local/src/native-runtime.ts',
  ],
  'services/ontology/src/store.ts#ObjectStore': [
    'services/ontology/src/store.ts',
    'services/ontology/src/service.ts',
    'services/ontology/src/indexer.ts',
    'services/snotel/src/service.ts',
    'apps/local/src/workspace.ts',
  ],
  'services/ontology/src/cursors.ts#CursorStore': [
    'services/filesystem/src/service.ts',
    'services/ontology/src/cursors.ts',
    'services/ontology/src/store.ts',
    'services/ontology/src/service.ts',
  ],
  'services/ontology/src/service.ts#ObjectService': [
    'apps/local/src/runtime.ts',
    'services/functions/src/service.ts',
    'services/ontology/src/service.ts',
    'apps/api/src/server.ts',
    'apps/local/src/server.ts',
    'apps/local/src/workspace.ts',
    'services/snotel/src/service.ts',
  ],
};

export const protectedFunctions: Record<string, readonly string[]> = {
  'services/process/src/python.ts#openPython': [
    'workers/iceberg/stage.ts',
    'workers/iceberg/stream.ts',
    'workers/iceberg/scoped-read.ts',
    'services/catalog/src/storage-publication.ts',
    'services/catalog/src/snapshot-publication.ts',
  ],
  'services/http/src/server.ts#cliKeyMatches': [
    'services/app-host/src/auth.ts',
    'services/http/src/server.ts',
  ],
  'services/http/src/server.ts#appHeaders': [
    'services/app-host/src/http.ts',
    'services/http/src/server.ts',
  ],
  'services/http/src/server.ts#requireBrowserOrigin': [
    'services/app-host/src/auth.ts',
    'services/http/src/server.ts',
  ],
  'services/http/src/server.ts#requireRecoveryAllowed': [
    'services/app-host/src/auth.ts',
    'services/http/src/server.ts',
  ],
  'services/http/src/server.ts#createHttpServer': [
    'apps/api/src/function-platform.ts',
    'apps/api/src/server.ts',
    'apps/local/src/server.ts',
    'services/app-host/src/scoped.ts',
    'services/app-host/src/static-server.ts',
    'apps/local/src/native-runtime.ts',
  ],
  'services/http/src/server.ts#browserProfile': [
    'apps/local/src/server.ts',
    'services/app-host/src/scoped.ts',
    'apps/local/src/native-runtime.ts',
  ],
  'services/http/src/server.ts#dispatchHttp': [
    'apps/local/src/server.ts',
    'services/app-host/src/scoped.ts',
  ],
  'services/ontology/src/query.ts#queryRows': ['services/ontology/src/service.ts'],
  'apps/api/src/server.ts#createApi': [
    'apps/api/src/filesystem.ts',
    'apps/api/src/functions.ts',
    'apps/local/src/server.ts',
    'services/app-host/src/scoped.ts',
  ],
};

export const objectOperations = ['query', 'read', 'invoke'] as const;
export const objectServiceMethods = [...objectOperations, 'requiring'] as const;

/** Every live dispatcher has existing real HTTP authority tests; new dispatchers need review. */
export const objectDispatchers = {
  'apps/api/src/server.ts': ['tests/sql.integration.test.ts'],
  'apps/local/src/server.ts': [
    'tests/api-only-process.integration.test.ts',
    'tests/platform-composition.test.ts',
  ],
  'services/app-host/src/scoped.ts': ['tests/platform-composition.test.ts'],
} as const;

export const protectedModules: Record<string, readonly string[]> = {
  'node:child_process': [
    'services/process/src/python.ts',
    'services/functions/src/docker.ts',
    'apps/cli/src/filesystem.ts',
    'apps/local/src/native-runtime.ts',
  ],
  child_process: [
    'services/process/src/python.ts',
    'services/functions/src/docker.ts',
    'apps/cli/src/filesystem.ts',
  ],
  'embedded-postgres': ['apps/local/src/development-database.ts'],
  'node:http2': [],
  http2: [],
  'node:net': [],
  net: [],
  'node:tls': [],
  tls: [],
  'node:http': ['services/http/src/server.ts'],
  http: ['services/http/src/server.ts'],
  'node:https': ['services/http/src/server.ts'],
  https: ['services/http/src/server.ts'],
  pg: [
    'apps/local/src/runtime.ts',
    'apps/local/src/migrate.ts',
    'apps/local/src/development-database.ts',
    'apps/local/src/native-runtime.ts',
  ],
  'node:module': ['packages/compiler/src/index.ts'],
  module: [],
};

export const httpListeners = {
  'apps/local/src/native-runtime.ts': ['tests/native-local-runtime.integration.test.ts'],
  'apps/api/src/function-platform.ts': [
    'tests/functions-platform.integration.test.ts',
    'tests/functions.integration.test.ts',
  ],
  'apps/api/src/server.ts': ['tests/http-boundary.test.ts', 'tests/sql.integration.test.ts'],
  'apps/local/src/server.ts': [
    'tests/http-boundary.test.ts',
    'tests/platform-composition.test.ts',
    'tests/api-only-process.integration.test.ts',
  ],
  'services/app-host/src/scoped.ts': [
    'tests/http-boundary.test.ts',
    'tests/platform-composition.test.ts',
  ],
  'services/app-host/src/static-server.ts': [
    'tests/http-boundary.test.ts',
    'tests/static-host-process.integration.test.ts',
  ],
} as const;

// These imports provide outbound clients only; namespace/dynamic imports are not exceptions.
export const httpClientImports: Record<string, Record<string, readonly string[]>> = {
  'services/functions/src/docker.ts': { 'node:http': ['request'] },
  'services/storage/src/s3.ts': { 'node:http': ['Agent'], 'node:https': ['Agent'] },
  'services/snotel/src/connector.ts': { 'node:https': ['get'] },
};

export const processAdapters = {
  'apps/local/src/native-runtime.ts': ['tests/native-local-runtime.integration.test.ts'],
  'services/functions/src/docker.ts': [
    'tests/functions.integration.test.ts',
    'tests/functions-docker-compat.integration.test.ts',
  ],
  'apps/cli/src/filesystem.ts': ['tests/filesystem-http.integration.test.ts'],
  'apps/local/src/development-database.ts': [
    'tests/catalog-storage-database.test.ts',
    'tests/platform-composition.test.ts',
  ],
  'workers/iceberg/stage.ts': ['tests/process-lifecycle.test.ts', 'tests/sql.integration.test.ts'],
  'workers/iceberg/stream.ts': ['tests/process-lifecycle.test.ts', 'tests/sql.integration.test.ts'],
  'workers/iceberg/scoped-read.ts': [
    'tests/process-lifecycle.test.ts',
    'tests/catalog-storage.integration.test.ts',
  ],
  'services/catalog/src/storage-publication.ts': [
    'tests/process-lifecycle.test.ts',
    'tests/catalog-storage-publication.integration.test.ts',
  ],
  'services/catalog/src/snapshot-publication.ts': [
    'tests/process-lifecycle.test.ts',
    'tests/catalog-reference-recovery.integration.test.ts',
  ],
} as const;

/** Runs only inside the isolated, digest-pinned function image. */
export const sandboxEntrypoints: Record<string, readonly string[]> = {
  'workers/functions/runner.mjs': [
    'tests/functions-runner.test.ts',
    'tests/functions.integration.test.ts',
  ],
};

/** Native child supervision; user code remains owned by its isolated runtime. */
export const processLifecycleOwners = [
  'services/process/src/python.ts',
  'services/functions/src/docker.ts',
  'apps/local/src/native-runtime.ts',
] as const;
