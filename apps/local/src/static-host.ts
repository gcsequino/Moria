import { parseArgs } from 'node:util';
import { localAppHost } from '../../../services/app-host/src/bootstrap.js';
import { startStaticServer } from '../../../services/app-host/src/static-server.js';

try {
  const { values } = parseArgs({
    options: {
      app: { type: 'string' },
      directory: { type: 'string' },
      port: { type: 'string' },
    },
    allowPositionals: false,
    strict: true,
  });
  if (!values.app || !values.directory?.trim() || !/^\d+$/.test(values.port ?? ''))
    throw new Error(
      'Required: --app <trusted-app-id> --directory <instance-dir> --port <1024-65535>.',
    );
  const server = await startStaticServer(
    localAppHost(values.directory),
    values.app,
    Number(values.port),
  );
  if (server.listening)
    console.log(`Moria static-only ${values.app}: http://127.0.0.1:${Number(values.port)}/`);
} catch (error) {
  const code = (error as NodeJS.ErrnoException).code;
  // Do not print filesystem contents or arbitrary parser inputs.
  console.error(
    code === 'EADDRINUSE'
      ? 'Static startup failed: port is already in use.'
      : 'Static startup failed: require --app, --directory and --port (1024-65535), and an existing verified promoted artifact. No artifacts were built or changed.',
  );
  process.exitCode = 1;
}
