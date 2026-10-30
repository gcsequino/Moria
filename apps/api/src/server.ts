import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { randomUUID } from 'node:crypto';
import {
  ObjectService,
  resolveObject,
  type ActionBinding,
  type SessionContext,
} from '../../../services/ontology/src/service.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import { functionsHandler } from './functions.js';
import type { Functions } from '../../../services/functions/src/service.js';

interface Routes {
  functions?: Functions;
  objects: Map<string, ObjectService>;
  actions: Map<string, { service: ObjectService; action: ActionBinding }>;
}
export async function body(request: IncomingMessage): Promise<any> {
  if (!/^application\/json(?:;|$)/i.test(request.headers['content-type'] ?? ''))
    throw new MoriaError('CONTENT_TYPE', 'JSON request body required');
  if (request.headers['content-encoding'] && request.headers['content-encoding'] !== 'identity')
    throw new MoriaError('CONTENT_ENCODING', 'Encoded request bodies are unsupported');
  return new Promise((resolve, reject) => {
    let size = 0,
      tooLarge = false;
    const parts: Buffer[] = [];
    request.on('data', (part: Buffer) => {
      if (tooLarge) return;
      size += part.length;
      if (size > 65536) {
        tooLarge = true;
        parts.length = 0;
        reject(new MoriaError('BODY_TOO_LARGE', 'Request body limit exceeded'));
        return;
      }
      parts.push(part);
    });
    request.once('error', () =>
      reject(new MoriaError('INPUT_INVALID', 'Request body interrupted')),
    );
    request.once('end', () => {
      if (tooLarge) return;
      try {
        resolve(JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(parts))));
      } catch {
        reject(new MoriaError('INPUT_INVALID', 'Invalid UTF-8 JSON body'));
      }
    });
  });
}
function send(response: ServerResponse, status: number, value: unknown) {
  response.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'x-content-type-options': 'nosniff',
  });
  response.end(JSON.stringify(value));
}
/** Narrow workload API. No CORS/browser authentication, administration or raw
 * storage endpoints. Local callers should bind this server to loopback. */
export function createApi(
  sessions: { authenticate(header: string | undefined): Promise<SessionContext> },
  routes: Routes,
) {
  const handleFunctions = routes.functions ? functionsHandler(routes.functions) : undefined;
  const server = createServer(async (request, response) => {
    const requestId = randomUUID();
    try {
      const session = await sessions.authenticate(request.headers.authorization);
      if (handleFunctions && (await handleFunctions(request, response, { session }))) return;
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
        const service = resolveObject(routes.objects, segments[2]);
        if (!service) throw new MoriaError('NOT_FOUND', 'Resource not found');
        send(response, 200, await service.query(session, await body(request)));
        return;
      }
      if (request.method === 'GET' && segments[1] === 'objects' && segments.length === 4) {
        if ([...url.searchParams.keys()].some((key) => key !== 'properties'))
          throw new MoriaError('INPUT_INVALID', 'Unsupported query parameter');
        if (url.searchParams.getAll('properties').length > 1)
          throw new MoriaError('INPUT_INVALID', 'Duplicate properties parameter');
        const service = resolveObject(routes.objects, segments[2]);
        if (!service) throw new MoriaError('NOT_FOUND', 'Resource not found');
        const properties = url.searchParams.has('properties')
          ? url.searchParams.get('properties') === ''
            ? []
            : url.searchParams.get('properties')!.split(',')
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
    } catch (error) {
      const databaseCode = (error as { code?: string })?.code;
      const e =
        error instanceof MoriaError
          ? error
          : databaseCode === '57014'
            ? new MoriaError('QUERY_TIMEOUT', 'Query time limit exceeded')
            : databaseCode === '55P03'
              ? new MoriaError('RESOURCE_BUSY', 'Database lock wait limit exceeded')
              : new MoriaError('INTERNAL', 'Request failed');
      const status =
        e.code === 'UNAUTHENTICATED'
          ? 401
          : e.code === 'NOT_FOUND'
            ? 404
            : ['FORBIDDEN', 'PROPERTY_FORBIDDEN'].includes(e.code)
              ? 403
              : e.code === 'BODY_TOO_LARGE'
                ? 413
                : e.code === 'UNSUPPORTED_CAPABILITY'
                  ? 422
                  : e.code === 'CURSOR_EXPIRED'
                    ? 410
                    : e.code === 'QUERY_TIMEOUT'
                      ? 504
                      : [
                            'RESOURCE_BUSY',
                            'CONCURRENT_MODIFICATION',
                            'FUNCTION_IN_PROGRESS',
                          ].includes(e.code)
                        ? 503
                        : [
                              'AUTHORIZATION_CHANGED',
                              'SCHEMA_CHANGED',
                              'MARKING_DOWNGRADE',
                              'CURSOR_INVALIDATED',
                              'ACTION_CONDITION_FAILED',
                              'IDEMPOTENCY_CONFLICT',
                              'RESULT_EXPIRED',
                            ].includes(e.code)
                          ? 409
                          : ['INTERNAL', 'POLICY_MISMATCH'].includes(e.code)
                            ? 500
                            : 400;
      if (!response.headersSent) {
        response.setHeader('connection', 'close');
        request.resume();
        send(response, status, {
          error: {
            code: e.code,
            message: e.message,
            requestId,
            correlationId: requestId,
            retryable: [
              'RESOURCE_BUSY',
              'CONCURRENT_MODIFICATION',
              'QUERY_TIMEOUT',
              'FUNCTION_IN_PROGRESS',
            ].includes(e.code),
            details:
              typeof (e.details as any)?.invocationId === 'string' &&
              /^[0-9a-f-]{36}$/.test((e.details as any).invocationId)
                ? { invocationId: (e.details as any).invocationId }
                : {},
          },
        });
      } else response.destroy();
    }
  });
  server.requestTimeout = 30000;
  server.headersTimeout = 10000;
  return server;
}
