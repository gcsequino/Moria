import type { Functions } from '../../functions/src/service.js';
import { createHttpServer, browserProfile, dispatchHttp, sendJson } from '../../http/src/server.js';
import { AppHost } from './store.js';
import { serveApp } from './http.js';
import { createApi } from '../../../apps/api/src/server.js';
import { WorkloadSessions } from '../../security/src/sessions.js';
import type { ObjectService, SessionContext } from '../../ontology/src/service.js';
import { AppAuth } from './auth.js';
import { MoriaError } from '../../../packages/contracts/types.js';

/** Trusted server binding. Artifact manifests and browser headers cannot expand it. */
export function createScopedAppServer(options: {
  app: string;
  entryPath?: string;
  host: AppHost;
  key: string;
  sessions: WorkloadSessions;
  token: () => Promise<string>;
  auth?: AppAuth;
  publicOrigin?: string;
  objects: Map<string, ObjectService>;
  functions?: Functions;
  status: (session?: SessionContext) => Promise<unknown>;
}) {
  const profile = browserProfile('scoped', options.key, options.auth, options.publicOrigin);
  const api = createApi(options.auth?.sessions ?? options.sessions, {
    objects: options.objects,
    functions: options.functions,
    actions: new Map(),
  });
  const served = (path: string) =>
    path.startsWith('/api/v1/objects/') ||
    Boolean(options.functions && /^\/api\/v1\/(functions|invocations)(?:\/|$)/.test(path));
  const server = createHttpServer(profile, async (req, res, { origin, cli }) => {
    const send = (status: number, value: unknown) => sendJson(res, status, value);
    if (options.auth && (await options.auth.handle(req, res))) return;
    if (await serveApp(options.host, options.app, req, res, options.entryPath)) return;
    if (options.auth) {
      req.headers.authorization = `Bearer ${options.auth.cookie(req)}`;
      const path = new URL(req.url ?? '/', origin).pathname;
      if (path === '/api/v1/status' && req.method === 'GET') {
        const session = await options.auth.sessions.authenticate(req.headers.authorization);
        send(200, await options.status(session));
      } else if (served(path)) await dispatchHttp(api, req, res);
      else send(404, { error: { code: 'NOT_FOUND', message: 'Endpoint not found.' } });
      return;
    }
    if (!cli)
      throw new MoriaError(
        'UNAUTHENTICATED',
        'Open this app using its own access link from pnpm start.',
      );
    const path = new URL(req.url ?? '/', 'http://localhost').pathname;
    if (path === '/api/v1/status' && req.method === 'GET') {
      send(200, await options.status());
      return;
    }
    if (!served(path)) {
      send(404, { error: { code: 'NOT_FOUND', message: 'Endpoint not found.' } });
      return;
    }
    // Caller-supplied bearer/profile/application values are never used as authority.
    req.headers.authorization = `Bearer ${await options.token()}`;
    await dispatchHttp(api, req, res);
  });
  return server;
}
