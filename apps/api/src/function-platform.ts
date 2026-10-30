import { createHttpServer } from '../../../services/http/src/server.js';
import type { FilesystemRuntime } from '../../../services/filesystem/src/local.js';
import type { FunctionBindings } from '../../../services/functions/src/platform.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import { filesystemHandler } from './filesystem.js';

/** Private supervisor transport; guest code has no host network or credential. */
export function functionFilesystem(
  runtime: FilesystemRuntime,
): NonNullable<FunctionBindings['filesystem']> {
  const handle = filesystemHandler(runtime);
  return async (context, input, signal) => {
    const server = createHttpServer({ kind: 'workload' }, async (request, response) => {
      await handle(request, response, context)
        .then((handled) => {
          if (!handled) {
            response.writeHead(404);
            response.end();
          }
        })
        .catch(() => response.destroy());
    });
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(0, '127.0.0.1', resolve);
    });
    try {
      const port = (server.address() as { port: number }).port;
      const response = await fetch(`http://127.0.0.1:${port}${input.path}`, {
        method: input.method,
        headers: input.headers,
        ...(input.body === null ? {} : { body: Buffer.from(input.body, 'base64') }),
        redirect: 'error',
        signal,
      });
      const reader = response.body?.getReader(),
        chunks: Uint8Array[] = [];
      let size = 0;
      if (reader)
        try {
          while (true) {
            const next = await reader.read();
            if (next.done) break;
            size += next.value.byteLength;
            if (size > 32768)
              throw new MoriaError('FUNCTION_LIMIT', 'Platform response exceeds bounds');
            chunks.push(next.value);
          }
        } finally {
          await reader.cancel().catch(() => {});
          reader.releaseLock();
        }
      return {
        status: response.status,
        headers: Object.fromEntries(response.headers),
        body: Buffer.concat(chunks).toString('base64'),
      };
    } finally {
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  };
}
