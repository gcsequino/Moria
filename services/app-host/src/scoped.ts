import { createServer } from 'node:http';
import { timingSafeEqual } from 'node:crypto';
import { AppHost } from './store.js';
import { serveApp } from './http.js';
import { createApi } from '../../../apps/api/src/server.js';
import { WorkloadSessions } from '../../security/src/sessions.js';
import { ObjectService, type SessionContext } from '../../ontology/src/service.js';
import { AppAuth } from './auth.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import type { Functions } from '../../functions/src/service.js';

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
  if (
    options.publicOrigin &&
    (!options.auth ||
      options.auth.allowRecovery ||
      options.auth.sessions.binding.origin !== options.publicOrigin)
  )
    throw new Error('Public hosting requires matching browser auth with local-only recovery.');
  const api = createApi(options.auth?.sessions ?? options.sessions, {
    objects: options.objects,
    actions: new Map(),
    functions: options.functions,
  });
  const served = (path: string) =>
    path.startsWith('/api/v1/objects/') ||
    Boolean(options.functions && /^\/api\/v1\/(functions|invocations)(?:\/|$)/.test(path));
  const server = createServer(async (req, res) => {
    const send = (status: number, data: unknown) => {
      res.writeHead(status, {
        'content-type': 'application/json',
        'cache-control': 'no-store',
        'x-content-type-options': 'nosniff',
      });
      res.end(JSON.stringify(data));
    };
    try {
      const address = server.address(),
        host = `127.0.0.1:${typeof address === 'object' && address ? address.port : 0}`;
      const origin = options.publicOrigin ?? `http://${host}`;
      if (options.publicOrigin && req.headers['x-forwarded-proto'] !== 'https')
        throw new MoriaError(
          'FORBIDDEN',
          'Public requests require HTTPS through the trusted proxy.',
        );
      if (options.publicOrigin) res.setHeader('strict-transport-security', 'max-age=31536000');
      if (
        req.headers.host !== new URL(origin).host ||
        (req.headers.origin && req.headers.origin !== origin)
      )
        throw new MoriaError('FORBIDDEN', 'Origin is not allowed.');
      if (options.auth && (await options.auth.handle(req, res))) return;
      if (await serveApp(options.host, options.app, req, res, options.entryPath)) return;
      if (options.auth) {
        if (req.method !== 'GET' && req.headers.origin !== origin)
          throw new MoriaError('FORBIDDEN', 'An exact same-origin request is required.');
        req.headers.authorization = `Bearer ${options.auth.cookie(req)}`;
        const path = new URL(req.url ?? '/', `http://${host}`).pathname;
        if (path === '/api/v1/status' && req.method === 'GET') {
          const session = await options.auth.sessions.authenticate(req.headers.authorization);
          send(200, await options.status(session));
        } else if (served(path)) api.emit('request', req, res);
        else send(404, { error: { code: 'NOT_FOUND', message: 'Endpoint not found.' } });
        return;
      }
      const key = req.headers['x-moria-app-key'];
      if (
        typeof key !== 'string' ||
        Buffer.byteLength(key) !== Buffer.byteLength(options.key) ||
        !timingSafeEqual(Buffer.from(key), Buffer.from(options.key))
      )
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
      api.emit('request', req, res);
    } catch (error) {
      req.resume();
      const e =
        error instanceof MoriaError ? error : new MoriaError('INTERNAL', 'App request failed.');
      send(e.code === 'UNAUTHENTICATED' ? 401 : e.code === 'FORBIDDEN' ? 403 : 500, {
        error: { code: e.code, message: e.message },
      });
    }
  });
  server.requestTimeout = 30000;
  server.headersTimeout = 10000;
  return server;
}
