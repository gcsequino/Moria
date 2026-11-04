import { createServer, request } from 'node:http';
import type { Socket } from 'node:net';

/** Forward actual signed MinIO traffic and withhold a completed multipart response. */
export async function completeProxy(endpoint: string) {
  const destination = new URL(endpoint),
    sockets = new Set<Socket>();
  let signal!: () => void;
  const committed = new Promise<void>((resolve) => (signal = resolve));
  const server = createServer((incoming, outgoing) => {
    const upstream = request(
      {
        hostname: destination.hostname,
        port: destination.port,
        path: incoming.url,
        method: incoming.method,
        headers: incoming.headers,
      },
      (response) => {
        const complete =
          incoming.method === 'POST' &&
          new URL(incoming.url!, endpoint).searchParams.has('uploadId') &&
          response.statusCode === 200;
        if (complete) {
          let bytes = 0;
          response.on('data', (chunk) => {
            bytes += chunk.length;
            if (bytes > 64 * 1024)
              upstream.destroy(new Error('Provider body exceeds fixture bound'));
          });
          response.on('end', signal);
        } else {
          outgoing.writeHead(response.statusCode!, response.headers);
          response.pipe(outgoing);
        }
      },
    );
    upstream.on('socket', (socket) => socket.setNoDelay(true));
    upstream.on('error', () => outgoing.destroy());
    incoming.pipe(upstream);
  });
  server.on('connection', (socket) => {
    socket.setNoDelay(true);
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  return {
    endpoint: `http://127.0.0.1:${(server.address() as { port: number }).port}`,
    committed,
    async close() {
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}
