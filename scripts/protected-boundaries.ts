/** Reviewed implementation owners; changes to this file require invariant review. */
export const protectedClasses: Record<string, readonly string[]> = {
  'services/security/src/authority.ts#Authority': [
    'services/security/src/authority.ts',
    'services/security/src/sessions.ts',
    'services/security/src/browser-sessions.ts',
    'services/ontology/src/service.ts',
    'services/ontology/src/indexer.ts',
    'services/catalog/src/storage.ts',
    'services/snotel/src/service.ts',
    'apps/local/src/workspace.ts',
  ],
  'services/catalog/src/catalog.ts#Catalog': [
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
  ],
  'services/ontology/src/store.ts#ObjectStore': [
    'services/ontology/src/store.ts',
    'services/ontology/src/service.ts',
    'services/ontology/src/indexer.ts',
    'services/snotel/src/service.ts',
    'apps/local/src/workspace.ts',
  ],
  'services/ontology/src/cursors.ts#CursorStore': [
    'services/ontology/src/cursors.ts',
    'services/ontology/src/store.ts',
    'services/ontology/src/service.ts',
  ],
  'services/ontology/src/service.ts#ObjectService': [
    'services/ontology/src/service.ts',
    'apps/api/src/server.ts',
    'apps/local/src/server.ts',
    'apps/local/src/workspace.ts',
    'services/snotel/src/service.ts',
  ],
};

export const protectedFunctions: Record<string, readonly string[]> = {
  'services/ontology/src/query.ts#queryRows': ['services/ontology/src/service.ts'],
  'apps/api/src/server.ts#createApi': [
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
  pg: [
    'apps/local/src/runtime.ts',
    'apps/local/src/migrate.ts',
    'apps/local/src/development-database.ts',
  ],
  'node:module': ['packages/compiler/src/index.ts'],
  module: [],
};
