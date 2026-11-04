import { createServer, connect, type Socket } from 'node:net';

/** Real PostgreSQL transport: withhold only the activation COMMIT acknowledgment. */
export async function commitProxy(port: number, functionName = 'moria.indexing_activate') {
  const sockets = new Set<Socket>();
  let committed!: () => void;
  const commit = new Promise<void>((resolve) => {
    committed = resolve;
  });
  const server = createServer((client) => {
    const backend = connect(port, '127.0.0.1');
    sockets.add(client);
    sockets.add(backend);
    let startup = true,
      input = Buffer.alloc(0),
      output = Buffer.alloc(0),
      activation = false,
      held = false;
    const close = () => {
      client.destroy();
      backend.destroy();
      sockets.delete(client);
      sockets.delete(backend);
    };
    client.on('error', close);
    backend.on('error', close);
    client.on('close', close);
    backend.on('close', close);
    client.on('data', (bytes) => {
      input = Buffer.concat([input, typeof bytes === 'string' ? Buffer.from(bytes) : bytes]);
      while (input.length >= (startup ? 4 : 5)) {
        const length = startup ? input.readUInt32BE(0) : input.readUInt32BE(1) + 1;
        if (length < 4 || length > 16 * 1024 * 1024) return close();
        if (input.length < length) return;
        const frame = input.subarray(0, length);
        input = input.subarray(length);
        if (!startup && frame[0] === 80 && frame.includes(Buffer.from(functionName)))
          activation = true;
        startup = false;
        backend.write(frame);
      }
    });
    backend.on('data', (bytes) => {
      output = Buffer.concat([output, typeof bytes === 'string' ? Buffer.from(bytes) : bytes]);
      while (output.length >= 5) {
        const length = output.readUInt32BE(1) + 1;
        if (length < 5 || length > 16 * 1024 * 1024) return close();
        if (output.length < length) return;
        const frame = output.subarray(0, length);
        output = output.subarray(length);
        if (activation && frame[0] === 67 && frame.subarray(5).equals(Buffer.from('COMMIT\0'))) {
          held = true;
          committed();
        }
        if (!held) client.write(frame);
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  return {
    port: (server.address() as { port: number }).port,
    commit,
    close: async () => {
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}
