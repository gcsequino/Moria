import { createServer } from 'node:http';
import { MoriaError } from '../../../packages/contracts/types.js';
import { serveApp } from './http.js';
import { AppHost, appId } from './store.js';

/** Trusted loopback static delivery; no backend composition or artifact mutation. */
export async function startStaticServer(host: AppHost, app: string, port: number) {
  appId(app);
  if (!Number.isInteger(port) || port < 1024 || port > 65535)
    throw new MoriaError('USAGE', 'Static port must be from 1024 to 65535.');
  await host.verify(app, (await host.active(app)).current);
  const server = createServer(async (request, response) => {
    try {
      if (
        request.headers.host !== `127.0.0.1:${port}` ||
        (request.headers.origin && request.headers.origin !== `http://127.0.0.1:${port}`)
      ) {
        response.writeHead(403);
        response.end();
        return;
      }
      if (!(await serveApp(host, app, request, response))) {
        response.writeHead(404);
        response.end();
      }
    } catch (error) {
      if (response.headersSent || response.destroyed) {
        response.destroy();
        return;
      }
      const invalidUrl = (error as { code?: string }).code === 'ERR_INVALID_URL';
      response.writeHead(invalidUrl ? 400 : 500, {
        'content-type': 'text/plain; charset=utf-8',
        'cache-control': 'no-store',
        connection: 'close',
      });
      response.end(invalidUrl ? 'Bad request.' : 'Static request failed.');
    }
  });
  let stopping = false;
  const stop = () => {
    stopping = true;
    server.close();
    // Static delivery has no mutations to drain; also release incomplete requests.
    server.closeAllConnections();
  };
  const cleanup = () => {
    process.removeListener('SIGINT', stop);
    process.removeListener('SIGTERM', stop);
  };
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
  server.once('close', cleanup);
  try {
    await new Promise<void>((ready, reject) => {
      server.once('error', reject);
      server.listen(port, '127.0.0.1', () => {
        server.removeListener('error', reject);
        ready();
      });
    });
    if (stopping) stop();
    return server;
  } catch (error) {
    stop();
    cleanup();
    throw error;
  }
}
