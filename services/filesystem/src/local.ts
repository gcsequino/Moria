import { mkdir } from 'node:fs/promises';
import { Catalog } from '../../catalog/src/catalog.js';
import { Authority } from '../../security/src/authority.js';
import { FilesystemAuthority } from '../../security/src/filesystem.js';
import {
  createAuthorizedLocalStorage,
  createAuthorizedS3Storage,
} from '../../storage/src/authority.js';
import { collectLocalStaging, createLocalStorage } from '../../storage/src/local.js';
import { createS3Storage, collectS3Staging, type S3StorageBinding } from '../../storage/src/s3.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import { fingerprint } from '../../../packages/compiler/src/index.js';
import type { FilesystemStorageBinding } from './content.js';
import { FilesystemStore } from './store.js';
import { FilesystemService } from './service.js';
import { FilesystemManagement } from './management.js';
import { SourceService } from './source.js';

export async function createLocalFilesystem(catalog: Catalog, root: string) {
  await mkdir(root, { recursive: true, mode: 0o700 });
  const binding = {
    root,
    warehouseId: 'filesystem',
    tableId: 'content',
    maxObjectBytes: 64 * 1024 * 1024,
    chunkBytes: 1048576,
  };
  await createLocalStorage(binding);
  await bindStorage(catalog, { backend: 'local', root });
  return createFilesystemRuntime(catalog, {
    ...binding,
    maxFileBytes: binding.maxObjectBytes,
    storage: (scope, current) => createAuthorizedLocalStorage(binding, scope, current),
    collectStaging: () => collectLocalStaging(binding),
  });
}

async function bindStorage(catalog: Catalog, configuration: unknown) {
  const hash = fingerprint(configuration);
  await catalog.transaction(
    async (client) => {
      await client.query(
        "INSERT INTO moria.fs_storage_bindings VALUES('filesystem','content',$1) ON CONFLICT DO NOTHING",
        [hash],
      );
      if (
        (
          await client.query(
            "SELECT binding_hash FROM moria.fs_storage_bindings WHERE warehouse_id='filesystem' AND table_id='content'",
          )
        ).rows[0].binding_hash !== hash
      )
        throw new MoriaError(
          'FILESYSTEM_CONFLICT',
          'Filesystem storage location differs from the persisted instance binding',
        );
    },
    undefined,
    'serializable',
    'exclusive',
    'exclusive',
  );
}

function createFilesystemRuntime(catalog: Catalog, binding: FilesystemStorageBinding) {
  const security = new FilesystemAuthority(new Authority(catalog));
  const filesystem = new FilesystemService(new FilesystemStore(catalog), security, binding);
  return {
    filesystem,
    management: new FilesystemManagement(filesystem),
    source: new SourceService(filesystem),
  };
}

export async function configuredFilesystem(
  catalog: Catalog,
  root: string,
  env: Record<string, string | undefined> = process.env,
) {
  const mode = env.MORIA_FILESYSTEM_BACKEND ?? 'local';
  if (mode === 'local') return createLocalFilesystem(catalog, root);
  if (mode !== 's3')
    throw new MoriaError('FILESYSTEM_INVALID', 'MORIA_FILESYSTEM_BACKEND must be local or s3');
  const required = (name: string) => {
    const value = env[name];
    if (!value)
      throw new MoriaError('FILESYSTEM_INVALID', `${name} is required for filesystem S3 storage`);
    return value;
  };
  const binding: S3StorageBinding = {
    warehouseId: 'filesystem',
    tableId: 'content',
    maxObjectBytes: 64 * 1024 * 1024,
    chunkBytes: 1048576,
    endpoint: required('MORIA_FILESYSTEM_S3_ENDPOINT'),
    bucket: required('MORIA_FILESYSTEM_S3_BUCKET'),
    region: required('MORIA_FILESYSTEM_S3_REGION'),
    prefix: env.MORIA_FILESYSTEM_S3_PREFIX ?? 'moria',
    credentials: {
      accessKeyId: required('MORIA_FILESYSTEM_S3_ACCESS_KEY_ID'),
      secretAccessKey: required('MORIA_FILESYSTEM_S3_SECRET_ACCESS_KEY'),
      ...(env.MORIA_FILESYSTEM_S3_SESSION_TOKEN
        ? { sessionToken: env.MORIA_FILESYSTEM_S3_SESSION_TOKEN }
        : {}),
    },
  };
  await createS3Storage(binding);
  await bindStorage(catalog, {
    backend: 's3',
    endpoint: new URL(binding.endpoint).href,
    bucket: binding.bucket,
    region: binding.region,
    prefix: binding.prefix,
  });
  return createFilesystemRuntime(catalog, {
    ...binding,
    maxFileBytes: binding.maxObjectBytes,
    storage: (scope, current) => createAuthorizedS3Storage(binding, scope, current),
    collectStaging: () => collectS3Staging(binding),
  });
}
export type FilesystemRuntime = Awaited<ReturnType<typeof createLocalFilesystem>>;
