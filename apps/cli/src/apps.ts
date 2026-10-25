import { createServer } from 'node:http';
import { localAppHost, bootstrapBundled } from '../../../services/app-host/src/bootstrap.js';
import { serveApp } from '../../../services/app-host/src/http.js';
import { appId } from '../../../services/app-host/src/store.js';
import { MoriaError } from '../../../packages/contracts/types.js';

export async function appCommand(args: string[], values: { directory?: string; port?: string }) {
  const [command, id, argument, ...extra] = args;
  const host = localAppHost(values.directory);
  if (id === 'snotel' && ['bootstrap', 'recover', 'serve'].includes(command))
    throw new MoriaError('USAGE', 'The bundled Snow POC is retired.');
  if (extra.length) throw new MoriaError('USAGE', 'Too many arguments.');
  switch (command) {
    case 'bootstrap':
    case 'recover':
      if ((id && id !== 'management') || argument)
        throw new MoriaError('USAGE', 'bootstrap/recover [management] restores a bundled app.');
      return bootstrapBundled(host, (id ?? 'management') as 'management', command === 'recover');
    case 'list':
      if (id) throw new MoriaError('USAGE', 'list takes no arguments.');
      return host.list();
    case 'publish':
      if (!id || !argument) throw new MoriaError('USAGE', 'apps publish <id> <built-directory>');
      return host.publish(id, argument);
    case 'promote':
      if (!id || !argument) throw new MoriaError('USAGE', 'apps promote <id> <version>');
      return host.promote(id, argument);
    case 'rollback':
    case 'verify':
    case 'unlock':
      if (!id || argument) throw new MoriaError('USAGE', `apps ${command} <id>`);
      if (command === 'rollback') return host.rollback(id);
      if (command === 'unlock') {
        await host.unlock(id);
        return { unlocked: id };
      }
      return host.verify(id, (await host.active(id)).current);
    case 'serve': {
      if (!id || argument || id === 'management')
        throw new MoriaError(
          'USAGE',
          'apps serve <custom-app-id> --port 3211; use pnpm start for management.',
        );
      appId(id);
      const port = Number(values.port ?? '3211');
      if (
        !Number.isInteger(port) ||
        port < 1024 ||
        port > 65535 ||
        port === Number(process.env.MORIA_PORT ?? 3210)
      )
        throw new MoriaError('USAGE', 'Choose a separate custom-app port from 1024 to 65535.');
      await host.verify(id, (await host.active(id)).current);
      const server = createServer(async (request, response) => {
        if (
          request.headers.host !== `127.0.0.1:${port}` ||
          (request.headers.origin && request.headers.origin !== `http://127.0.0.1:${port}`)
        ) {
          response.writeHead(403);
          response.end();
          return;
        }
        if (!(await serveApp(host, id, request, response))) {
          response.writeHead(404);
          response.end();
        }
      });
      await new Promise<void>((ready, reject) => {
        server.once('error', reject);
        server.listen(port, '127.0.0.1', ready);
      });
      process.once('SIGINT', () => server.close());
      process.once('SIGTERM', () => server.close());
      return {
        app: id,
        url: `http://127.0.0.1:${port}`,
        mode: 'static-only',
        note: 'Separate origin; no management credentials or API access granted.',
      };
    }
    default:
      throw new MoriaError(
        'USAGE',
        'apps bootstrap|recover|list|publish|promote|rollback|verify|serve|unlock',
      );
  }
}
