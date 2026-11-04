import { createServer, connect, type Socket } from 'node:net';

/** Real PostgreSQL transport: hold a mutating SELECT response after server commit. */
export async function requestCommitProxy(port: number, functionName = 'runtime_execution_request') {
  return postgresProxy(port, functionName, false);
}

/** Hold only the actual COMMIT response after a selected transactional publication. */
export async function transactionCommitProxy(
  port: number,
  functionName = 'runtime_execution_complete',
) {
  return postgresProxy(port, functionName, true);
}

async function postgresProxy(port: number, functionName: string, transaction: boolean) {
  const checkpoints = { matches: 0, commitRequests: 0, commitResponses: 0, ready: [] as string[] };
  const errors: { code: string; functions: string[] }[] = [];
  const sockets = new Set<Socket>();
  let signal!: () => void;
  const committed = new Promise<void>((resolve) => (signal = resolve));
  const server = createServer((client) => {
    const backend = connect(port, '127.0.0.1');
    sockets.add(client);
    sockets.add(backend);
    let startup = true,
      active = false,
      held = false,
      commitSent = false,
      commitObserved = false;
    let input = Buffer.alloc(0),
      output = Buffer.alloc(0);
    const close = () => {
      client.destroy();
      backend.destroy();
      sockets.delete(client);
      sockets.delete(backend);
    };
    for (const socket of [client, backend]) {
      socket.setNoDelay(true);
      socket.on('error', close);
      socket.on('close', close);
    }
    client.on('data', (bytes) => {
      input = Buffer.concat([input, typeof bytes === 'string' ? Buffer.from(bytes) : bytes]);
      while (input.length >= (startup ? 4 : 5)) {
        const length = startup ? input.readUInt32BE(0) : input.readUInt32BE(1) + 1;
        if (length < 4 || length > 1024 * 1024) return close();
        if (input.length < length) return;
        const frame = input.subarray(0, length);
        input = input.subarray(length);
        if (!startup && frame[0] === 80 && frame.includes(Buffer.from(functionName))) {
          active = true;
          checkpoints.matches++;
        }
        if (
          !startup &&
          active &&
          transaction &&
          frame[0] === 81 &&
          /^COMMIT\x00$/i.test(frame.subarray(5).toString())
        ) {
          commitSent = true;
          checkpoints.commitRequests++;
        }
        startup = false;
        backend.write(frame);
      }
    });
    backend.on('data', (bytes) => {
      output = Buffer.concat([output, typeof bytes === 'string' ? Buffer.from(bytes) : bytes]);
      while (output.length >= 5) {
        const length = output.readUInt32BE(1) + 1;
        if (length < 5 || length > 1024 * 1024) return close();
        if (output.length < length) return;
        const frame = output.subarray(0, length);
        output = output.subarray(length);
        if (frame[0] === 69 && errors.length < 64) {
          const fields = new Map<string, string>();
          for (let offset = 5; offset < frame.length - 1;) {
            const tag = String.fromCharCode(frame[offset++]);
            const end = frame.indexOf(0, offset);
            if (end < 0) break;
            if (tag === 'C' || tag === 'W') fields.set(tag, frame.subarray(offset, end).toString());
            offset = end + 1;
          }
          const code = fields.get('C') ?? '';
          if (/^[0-9A-Z]{5}$/.test(code))
            errors.push({
              code,
              functions: [
                ...(fields.get('W') ?? '').matchAll(/^PL\/pgSQL function ([A-Za-z0-9_.]+)\(/gm),
              ]
                .slice(0, 8)
                .map((match) => match[1]),
            });
        }
        if (!transaction && active && frame[0] === 68) held = true;
        if (transaction && commitSent) {
          held = true;
          if (frame[0] === 67 && frame.subarray(5).toString() === 'COMMIT\0') {
            commitObserved = true;
            checkpoints.commitResponses++;
          }
        }
        if (held && frame[0] === 90 && frame[5] === 73 && (!transaction || commitObserved))
          signal();
        if (active && frame[0] === 90) checkpoints.ready.push(String.fromCharCode(frame[5]));
        if (!held) client.write(frame);
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  let closing: Promise<void> | undefined;
  return {
    port: (server.address() as { port: number }).port,
    committed,
    checkpoints,
    errors,
    async close() {
      if (!closing) {
        for (const socket of sockets) socket.destroy();
        closing = new Promise<void>((resolve, reject) =>
          server.close((error) => (error ? reject(error) : resolve())),
        );
      }
      await closing;
    },
  };
}
