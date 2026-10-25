import pg from 'pg';
import { readFile } from 'node:fs/promises';
import { Catalog } from '../../../services/catalog/src/catalog.js';
import { CatalogStorage } from '../../../services/catalog/src/storage.js';
import { Authority } from '../../../services/security/src/authority.js';
import { metadataStream } from '../../../services/catalog/src/storage-publication.js';
import {
  createAuthorizedLocalStorage,
  createAuthorizedS3Storage,
} from '../../../services/storage/src/authority.js';

// Only synthetic disposable-test connection settings are passed in the environment.
const input = JSON.parse(process.env.MORIA_REFERENCE_RECOVERY_TEST!);
delete process.env.MORIA_REFERENCE_RECOVERY_TEST;
const pool = new pg.Pool(input.database);
try {
  const service = new CatalogStorage(new Authority(new Catalog(pool)), [
    {
      ...input.binding,
      storage: (scope, authority) =>
        input.provider === 'local'
          ? createAuthorizedLocalStorage(input.config, scope, authority)
          : createAuthorizedS3Storage(input.config, scope, authority),
    },
  ]);
  if (input.retry) {
    const metadata = await readFile(input.retry.metadata);
    const objects = await Promise.all(
      Object.entries(input.retry.objects as Record<string, string>).map(async ([name, path]) => {
        const bytes = await readFile(path);
        return { name, size: bytes.length, stream: metadataStream(bytes) };
      }),
    );
    const result = await service.publishSnapshot(input.auth, {
      ...input.retry.request,
      outputs: [
        {
          ...input.retry.request.outputs[0],
          size: metadata.length,
          stream: metadataStream(metadata),
          objects,
        },
      ],
    });
    process.stdout.write(JSON.stringify(result.receipt));
  } else {
    const { rows, ...provenance } = await service.readRows(input.auth, input.binding.dataset);
    const values = [];
    for await (const row of rows) values.push(row);
    process.stdout.write(JSON.stringify({ ...provenance, rows: values }));
  }
} catch (error) {
  process.stdout.write(JSON.stringify({ code: (error as { code?: string }).code ?? 'ERROR' }));
  process.exitCode = 1;
} finally {
  await pool.end();
}
