import { createServer, request, type IncomingMessage } from 'node:http';

/** Real upstream requests, with faults only at the network acknowledgment boundary. */
export async function proxyS3(
  endpoint: string,
  after: (
    request: IncomingMessage,
    status: number,
    response: IncomingMessage,
  ) => Promise<'drop' | void> = async () => {},
) {
  const upstream = new URL(endpoint);
  const server = createServer((incoming, outgoing) => {
    const forwarded = request(
      {
        hostname: upstream.hostname,
        port: upstream.port,
        method: incoming.method,
        path: incoming.url,
        headers: incoming.headers,
      },
      (response) => {
        void (async () => {
          const action = await after(incoming, response.statusCode!, response);
          if (action === 'drop') {
            response.resume();
            response.once('end', () => outgoing.destroy());
            return;
          }
          outgoing.writeHead(response.statusCode!, response.headers);
          response.pipe(outgoing);
        })().catch(() => {
          response.destroy();
          outgoing.destroy();
        });
      },
    );
    forwarded.on('error', () => outgoing.destroy());
    outgoing.on('close', () => forwarded.destroy());
    incoming.pipe(forwarded);
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') {
    throw new Error('Proxy startup failed');
  }
  return {
    endpoint: `http://127.0.0.1:${address.port}`,
    async close() {
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}
