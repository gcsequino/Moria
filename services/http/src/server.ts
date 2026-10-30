import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { randomUUID, timingSafeEqual } from 'node:crypto';
import { MoriaError } from '../../../packages/contracts/types.js';

type BrowserAuth = {
  allowRecovery: boolean;
  sessions: { binding: { origin: string } };
};
type Profile =
  | { kind: 'workload' | 'static' }
  | { kind: 'management' | 'scoped'; cookieAuth: boolean; key: string; publicOrigin?: string };
export interface HttpContext {
  readonly origin: string;
  readonly cli: boolean;
  readonly requestId: string;
}
type Handler = (
  request: IncomingMessage,
  response: ServerResponse,
  context: HttpContext,
) => Promise<void>;
const mediaTypes = new WeakMap<ServerResponse, string>();
const browserProfiles = new WeakSet<object>();
const admitted = new WeakMap<IncomingMessage, { response: ServerResponse; context: HttpContext }>();
const dispatchers = new WeakMap<
  Server,
  (request: IncomingMessage, response: ServerResponse) => Promise<void>
>();

export function browserProfile(
  kind: 'management' | 'scoped',
  key: string,
  auth?: BrowserAuth,
  publicOrigin?: string,
): Profile {
  if (publicOrigin) {
    const url = new URL(publicOrigin);
    if (
      url.protocol !== 'https:' ||
      url.origin !== publicOrigin ||
      url.username ||
      url.password ||
      !auth ||
      auth.allowRecovery ||
      auth.sessions.binding.origin !== publicOrigin
    )
      throw new Error('Public hosting requires matching browser auth with local-only recovery.');
  }
  const profile = Object.freeze({ kind, key, cookieAuth: !!auth, publicOrigin });
  browserProfiles.add(profile);
  return profile;
}

const loopback = (address: string | undefined) =>
  address === '127.0.0.1' || address === '::1' || address === '::ffff:127.0.0.1';
function keyMatches(value: string | string[] | undefined, key: string): boolean {
  return (
    typeof value === 'string' &&
    Buffer.byteLength(value) === Buffer.byteLength(key) &&
    timingSafeEqual(Buffer.from(value), Buffer.from(key))
  );
}
export function cliKeyMatches(request: IncomingMessage, header: string, key: string) {
  return (
    !request.headers.origin &&
    !request.headers['sec-fetch-site'] &&
    keyMatches(request.headers[header], key)
  );
}
export function requireBrowserOrigin(
  request: IncomingMessage,
  origin: string,
  cli = false,
  allowRecovery = false,
) {
  const path = new URL(request.url ?? '/', origin).pathname;
  const recoveryTicket = allowRecovery && path === '/api/auth/recovery-ticket';
  if (
    !['GET', 'HEAD'].includes(request.method ?? '') &&
    !cli &&
    !recoveryTicket &&
    request.headers.origin !== origin
  )
    throw new MoriaError('FORBIDDEN', 'An exact same-origin request is required.');
}
export function requireRecoveryAllowed(path: string, allowed: boolean) {
  if (!allowed && ['/api/auth/recovery-ticket', '/api/auth/recover'].includes(path))
    throw new MoriaError('FORBIDDEN', 'Recovery is available only through the local listener.');
}
export function appHeaders(response: ServerResponse) {
  response.setHeader('x-content-type-options', 'nosniff');
  response.setHeader('referrer-policy', 'no-referrer');
  response.setHeader(
    'content-security-policy',
    "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'self'; form-action 'self'; worker-src 'none'",
  );
}
export function sendJson(response: ServerResponse, status: number, value: unknown) {
  if (response.headersSent || response.destroyed) {
    response.destroy();
    return;
  }
  const text = JSON.stringify(value);
  response.writeHead(status, {
    'content-type': mediaTypes.get(response) ?? 'application/json',
    'cache-control': 'no-store',
    'x-content-type-options': 'nosniff',
  });
  response.end(text);
}
function failure(
  request: IncomingMessage,
  response: ServerResponse,
  profile: Profile,
  error: unknown,
  requestId: string,
) {
  request.resume();
  if (response.headersSent || response.destroyed) {
    response.destroy();
    return;
  }
  const code = (error as { code?: string })?.code;
  const e =
    error instanceof MoriaError
      ? error
      : code === '57014'
        ? new MoriaError('QUERY_TIMEOUT', 'Query time limit exceeded')
        : code === '55P03'
          ? new MoriaError('RESOURCE_BUSY', 'Database lock wait limit exceeded')
          : code === 'ERR_INVALID_URL' || error instanceof URIError
            ? new MoriaError('INPUT_INVALID', 'Invalid request URL')
            : new MoriaError('INTERNAL', 'Request failed');
  response.setHeader('connection', 'close');
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
                  : profile.kind === 'management' &&
                      ['RESOURCE_BUSY', 'WORKLOAD_BUSY', 'IDEMPOTENCY_CONFLICT'].includes(e.code)
                    ? 409
                    : ['RESOURCE_BUSY', 'CONCURRENT_MODIFICATION', 'FUNCTION_IN_PROGRESS'].includes(
                          e.code,
                        )
                      ? 503
                      : [
                            'AUTHORIZATION_CHANGED',
                            'SCHEMA_CHANGED',
                            'MARKING_DOWNGRADE',
                            'CURSOR_INVALIDATED',
                            'ACTION_CONDITION_FAILED',
                            'IDEMPOTENCY_CONFLICT',
                            'RESULT_EXPIRED',
                            'WORKLOAD_BUSY',
                          ].includes(e.code)
                        ? 409
                        : ['INTERNAL', 'POLICY_MISMATCH', 'INDEX_FAILED'].includes(e.code)
                          ? 500
                          : 400;
  if (profile.kind === 'static') {
    response.writeHead(status, {
      'content-type': 'text/plain; charset=utf-8',
      'cache-control': 'no-store',
    });
    response.end(status === 403 ? '' : status === 400 ? 'Bad request.' : 'Static request failed.');
  } else
    sendJson(response, status, {
      error: {
        code: e.code,
        message: e.message,
        requestId,
        correlationId: requestId,
        retryable: [
          'RESOURCE_BUSY',
          'CONCURRENT_MODIFICATION',
          'FUNCTION_IN_PROGRESS',
          'QUERY_TIMEOUT',
        ].includes(e.code),
        details:
          typeof (e.details as any)?.invocationId === 'string' &&
          /^[0-9a-f-]{36}$/.test((e.details as any).invocationId)
            ? { invocationId: (e.details as any).invocationId }
            : {},
      },
    });
}

/** Every native listener and internal workload dispatcher passes through this owner. */
export function createHttpServer(configured: Profile, handler: Handler): Server {
  if (
    !['workload', 'static', 'management', 'scoped'].includes(configured.kind) ||
    (['management', 'scoped'].includes(configured.kind) && !browserProfiles.has(configured))
  )
    throw new Error('An explicit trusted HTTP profile is required.');
  const profile = Object.freeze({ ...configured });
  const run = async (request: IncomingMessage, response: ServerResponse) => {
    mediaTypes.set(
      response,
      profile.kind === 'workload' ? 'application/json; charset=utf-8' : 'application/json',
    );
    const inherited = admitted.get(request);
    const requestId = inherited?.context.requestId ?? randomUUID();
    try {
      let context: HttpContext;
      if (
        profile.kind === 'workload' &&
        inherited?.response === response &&
        !response.writableEnded
      )
        context = inherited.context;
      else {
        const address = server.address();
        if (!address || typeof address === 'string')
          throw new MoriaError('FORBIDDEN', 'An admitted listener request is required.');
        const publicOrigin = 'publicOrigin' in profile ? profile.publicOrigin : undefined;
        const origin = publicOrigin ?? `http://127.0.0.1:${address.port}`;
        if (!loopback(request.socket.remoteAddress) || !loopback(request.socket.localAddress))
          throw new MoriaError('FORBIDDEN', 'This listener requires trusted loopback ingress.');
        if (publicOrigin && request.headers['x-forwarded-proto'] !== 'https')
          throw new MoriaError(
            'FORBIDDEN',
            'Public requests require HTTPS through the trusted proxy.',
          );
        if (publicOrigin) response.setHeader('strict-transport-security', 'max-age=31536000');
        if (
          request.headers.host !== new URL(origin).host ||
          (request.headers.origin && request.headers.origin !== origin) ||
          (profile.kind === 'workload' && request.headers.origin)
        )
          throw new MoriaError('FORBIDDEN', 'Origin is not allowed.');
        const url = new URL(request.url ?? '/', origin);
        if (!request.url?.startsWith('/') || request.url.startsWith('//') || url.origin !== origin)
          throw new MoriaError('INPUT_INVALID', 'Invalid request URL');
        decodeURI(url.pathname);
        const fileUpload =
          request.method === 'POST' &&
          /^\/(?:api\/)?management\/v1\/fs\/entries\/[^/]+\/(?:files|content)$/.test(url.pathname);
        if (
          Number(request.headers['content-length'] ?? 0) >
          (fileUpload && profile.kind !== 'static' ? 64 * 1024 * 1024 : 65536)
        )
          throw new MoriaError('BODY_TOO_LARGE', 'Request body limit exceeded');
        const cli =
          !publicOrigin &&
          'key' in profile &&
          cliKeyMatches(
            request,
            profile.kind === 'management' ? 'x-moria-key' : 'x-moria-app-key',
            profile.key,
          );
        const platformBearer =
          profile.kind === 'management' &&
          !request.headers.cookie &&
          !request.headers.origin &&
          !request.headers['sec-fetch-site'] &&
          /^Bearer [A-Za-z0-9_-]{43}$/.test(request.headers.authorization ?? '') &&
          /^\/(?:(?:api\/)?management\/v1\/fs|api\/v1\/(?:functions|invocations))(?:\/|$)/.test(
            url.pathname,
          );
        if ('cookieAuth' in profile && profile.cookieAuth && !platformBearer)
          requireBrowserOrigin(request, origin, cli, !publicOrigin);
        requireRecoveryAllowed(url.pathname, !publicOrigin);
        context = Object.freeze({ origin, cli, requestId });
        admitted.set(request, { response, context });
      }
      appHeaders(response);
      await handler(request, response, context);
    } catch (error) {
      failure(request, response, profile, error, requestId);
    }
  };
  const server = createServer((request, response) => {
    void run(request, response);
  });
  server.requestTimeout = 30000;
  server.headersTimeout = 10000;
  if (profile.kind === 'workload') dispatchers.set(server, run);
  return server;
}

export async function dispatchHttp(
  server: Server,
  request: IncomingMessage,
  response: ServerResponse,
) {
  const entry = admitted.get(request);
  const run = dispatchers.get(server);
  if (!run || entry?.response !== response || response.writableEnded)
    throw new MoriaError('FORBIDDEN', 'Request has not passed the transport boundary.');
  await run(request, response);
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
      } else parts.push(part);
    });
    request.once('error', () =>
      reject(new MoriaError('INPUT_INVALID', 'Request body interrupted')),
    );
    request.once('aborted', () =>
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
