import { createHttpServer, body, sendJson as send } from '../../../services/http/src/server.js';
export { body } from '../../../services/http/src/server.js';
import {
  ObjectService,
  type ActionBinding,
  type SessionContext,
} from '../../../services/ontology/src/service.js';
import { MoriaError } from '../../../packages/contracts/types.js';

interface Routes {
  objects: Map<string, ObjectService>;
  actions: Map<string, { service: ObjectService; action: ActionBinding }>;
}
/** Narrow workload API. No CORS/browser authentication, administration or raw
 * storage endpoints. Local callers should bind this server to loopback. */
export function createApi(
  sessions: { authenticate(header: string | undefined): Promise<SessionContext> },
  routes: Routes,
) {
  const server = createHttpServer({ kind: 'workload' }, async (request, response) => {
    const session = await sessions.authenticate(request.headers.authorization);
    const url = new URL(request.url ?? '/', 'http://localhost');
    let segments: string[];
    try {
      segments = url.pathname.split('/').filter(Boolean).map(decodeURIComponent);
    } catch {
      throw new MoriaError('INPUT_INVALID', 'Invalid URL encoding');
    }
    // Canonical serving prefix. Keep /v1 as a prototype compatibility alias;
    // both paths share session checks, cursors and action idempotency scopes.
    if (segments[0] === 'api') segments.shift();
    if (segments[0] !== 'v1') throw new MoriaError('NOT_FOUND', 'Endpoint not found');
    if (
      request.method === 'POST' &&
      segments[1] === 'objects' &&
      segments.length === 4 &&
      segments[3] === 'query'
    ) {
      if (url.search) throw new MoriaError('INPUT_INVALID', 'Unsupported query parameter');
      const service = routes.objects.get(segments[2]);
      if (!service) throw new MoriaError('NOT_FOUND', 'Resource not found');
      send(response, 200, await service.query(session, await body(request)));
      return;
    }
    if (request.method === 'GET' && segments[1] === 'objects' && segments.length === 4) {
      if ([...url.searchParams.keys()].some((key) => key !== 'properties'))
        throw new MoriaError('INPUT_INVALID', 'Unsupported query parameter');
      if (url.searchParams.getAll('properties').length > 1)
        throw new MoriaError('INPUT_INVALID', 'Duplicate properties parameter');
      const service = routes.objects.get(segments[2]);
      if (!service) throw new MoriaError('NOT_FOUND', 'Resource not found');
      const properties = url.searchParams.has('properties')
        ? url.searchParams.get('properties')!.split(',')
        : undefined;
      send(response, 200, await service.read(session, segments[3], properties));
      return;
    }
    if (
      request.method === 'POST' &&
      segments[1] === 'actions' &&
      segments.length === 4 &&
      segments[3] === 'invoke'
    ) {
      if (url.search) throw new MoriaError('INPUT_INVALID', 'Unsupported query parameter');
      const route = routes.actions.get(segments[2]);
      if (!route) throw new MoriaError('NOT_FOUND', 'Resource not found');
      const payload = await body(request);
      if (
        !payload ||
        typeof payload !== 'object' ||
        Array.isArray(payload) ||
        Object.keys(payload).length !== 1 ||
        !Object.hasOwn(payload, 'input')
      )
        throw new MoriaError('INPUT_INVALID', 'Expected only an input object');
      const key = request.headers['idempotency-key'];
      if (typeof key !== 'string')
        throw new MoriaError('IDEMPOTENCY_KEY_INVALID', 'Idempotency-Key header required');
      send(response, 200, await route.service.invoke(session, route.action, key, payload.input));
      return;
    }
    throw new MoriaError('NOT_FOUND', 'Endpoint not found');
  });
  return server;
}
