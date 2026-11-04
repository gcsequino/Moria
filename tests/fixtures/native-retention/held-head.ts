import { createServer, request } from 'node:http';
export async function heldHeadProxy(target: string) {
  let armed: string | undefined,
    release: () => void = () => {},
    observed: () => void = () => {};
  let seen = Promise.resolve();
  const deletes: string[] = [];
  const server = createServer((incoming, outgoing) => {
    if (incoming.method === 'DELETE') deletes.push(incoming.url ?? '');
    const remote = request(
      new URL(incoming.url ?? '/', target),
      { method: incoming.method, headers: incoming.headers },
      async (response) => {
        if (incoming.method === 'HEAD' && new URL(incoming.url ?? '/', target).pathname === armed) {
          armed = undefined;
          observed();
          await new Promise<void>((resolve) => {
            release = resolve;
          });
        }
        if (outgoing.destroyed) {
          response.resume();
          return;
        }
        outgoing.writeHead(response.statusCode ?? 502, response.headers);
        response.pipe(outgoing);
      },
    );
    remote.on('error', () => {
      if (!outgoing.headersSent) outgoing.writeHead(502);
      outgoing.end();
    });
    incoming.pipe(remote);
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Missing proxy address');
  return {
    endpoint: `http://127.0.0.1:${address.port}`,
    deletes,
    arm(path: string) {
      armed = path;
      seen = new Promise<void>((resolve) => {
        observed = resolve;
      });
    },
    async wait() {
      await Promise.race([
        seen,
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error('HEAD checkpoint timed out')), 10000).unref(),
        ),
      ]);
    },
    release() {
      release();
    },
    async close() {
      release();
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}
