import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import type { Socket } from 'node:net';
import type { startS3 } from '../storage-s3/server.js';

type Provider = Awaited<ReturnType<typeof startS3>>;
export function versionTrace() {
  let selected = '';
  const entries: {
    method: string;
    requestVersion: string | null;
    status?: number;
    responseVersion?: string;
  }[] = [];
  return {
    entries,
    select(path: string) {
      assert(path.startsWith('/'));
      selected = path;
      entries.length = 0;
    },
    wrap: async (source: Provider): Promise<Provider> => {
      const target = new URL(source.endpoint);
      const sockets = new Set<Socket>();
      const proxy = createServer((incoming, outgoing) => {
        const url = new URL(incoming.url!, source.endpoint);
        const entry =
          url.pathname === selected
            ? { method: incoming.method!, requestVersion: url.searchParams.get('versionId') }
            : undefined;
        if (entry) {
          assert(entries.length < 128);
          entries.push(entry);
        }
        const upstream = request(
          {
            hostname: target.hostname,
            port: target.port,
            path: incoming.url,
            method: incoming.method,
            headers: incoming.headers,
          },
          (response) => {
            if (entry) {
              Object.assign(entry, {
                status: response.statusCode,
                responseVersion: response.headers['x-amz-version-id'],
              });
            }
            outgoing.writeHead(response.statusCode!, response.headers);
            response.pipe(outgoing);
          },
        );
        upstream.on('error', () => {
          outgoing.writeHead(502);
          outgoing.end();
        });
        incoming.on('error', () => upstream.destroy());
        incoming.pipe(upstream);
      });
      proxy.on('connection', (socket) => {
        sockets.add(socket);
        socket.on('close', () => sockets.delete(socket));
      });
      await new Promise<void>((resolve) => proxy.listen(0, '127.0.0.1', resolve));
      return {
        ...source,
        endpoint: `http://127.0.0.1:${(proxy.address() as { port: number }).port}`,
        close: async () => {
          for (const socket of sockets) socket.destroy();
          await new Promise<void>((resolve) => proxy.close(() => resolve()));
          await source.close();
        },
      };
    },
  };
}
