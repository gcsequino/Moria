/** Browser-safe runtime copied into generated SDKs. No credentials or server imports. */
import { canonicalJson } from '../../contracts/values.js';
export interface ClientOptions {
  /** Same origin by default. Cross-origin cookies are deliberately unsupported. */
  baseUrl?: string;
  fetch?: typeof fetch;
  onUnauthenticated?: () => void;
}
export interface RequestOptions {
  signal?: AbortSignal;
  idempotencyKey?: string;
}
export interface ActionOptions extends RequestOptions {
  idempotencyKey: string;
}
export class ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string,
    message: string,
    public readonly correlationId?: string,
    public readonly retryable = false,
    public readonly details?: unknown,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}
export interface AuthState {
  user: null | { principal: string; name: string; recovery: boolean; expiresAt: string };
  providers: { id: string; label: string }[];
}
export function createTransport(options: ClientOptions = {}) {
  const base = options.baseUrl ?? '';
  if (base) {
    const url = new URL(base);
    if (
      !['https:', 'http:'].includes(url.protocol) ||
      url.origin !== base ||
      url.username ||
      url.password
    )
      throw new Error('baseUrl must be an HTTP(S) origin without a path or trailing slash.');
    if (typeof location !== 'undefined' && url.origin !== location.origin)
      throw new Error('Browser SDK calls must use the current application origin.');
  }
  const fetcher = options.fetch ?? globalThis.fetch.bind(globalThis);
  async function send(
    path: string,
    method: 'GET' | 'POST',
    payload: BodyInit | undefined,
    headers: Record<string, string>,
    settings: RequestOptions = {},
  ): Promise<Response> {
    if (!path.startsWith('/api/') || path.includes('://')) throw new Error('Invalid SDK route.');
    const response = await fetcher(base + path, {
      method,
      credentials: 'same-origin',
      redirect: 'error',
      signal: settings.signal,
      headers: {
        ...headers,
        ...(settings.idempotencyKey ? { 'idempotency-key': settings.idempotencyKey } : {}),
      },
      ...(payload === undefined ? {} : { body: payload }),
    });
    if (response.status === 401) options.onUnauthenticated?.();
    return response;
  }
  async function decode<T>(response: Response): Promise<T> {
    let data: any;
    try {
      data = JSON.parse(
        new TextDecoder('utf-8', { fatal: true }).decode(await response.arrayBuffer()),
      );
    } catch {
      throw new ApiError(
        response.status,
        'INVALID_RESPONSE',
        `Expected JSON from Moria (${response.status}).`,
      );
    }
    if (!response.ok)
      throw new ApiError(
        response.status,
        data?.error?.code ?? 'HTTP_ERROR',
        data?.error?.message ?? `Request failed (${response.status}).`,
        data?.error?.correlationId ?? data?.error?.requestId,
        data?.error?.retryable === true,
        data?.error?.details,
      );
    return data as T;
  }
  async function request<T>(
    path: string,
    method: 'GET' | 'POST',
    input?: unknown,
    settings: RequestOptions = {},
  ): Promise<T> {
    return decode<T>(
      await send(
        path,
        method,
        input === undefined ? undefined : canonicalJson(input),
        { 'content-type': 'application/json' },
        settings,
      ),
    );
  }
  return {
    request,
    async response(
      path: string,
      method: 'GET' | 'POST',
      payload?: BodyInit,
      headers: Record<string, string> = {},
      settings: RequestOptions = {},
    ) {
      const response = await send(path, method, payload, headers, settings);
      if (!response.ok) await decode(response);
      return response;
    },
    auth: {
      session: (settings?: RequestOptions) =>
        request<AuthState>('/api/auth/session', 'GET', undefined, settings),
      login: (provider: string, settings?: RequestOptions) =>
        request<{ url: string }>('/api/auth/login', 'POST', { provider }, settings),
      logout: (settings?: RequestOptions) =>
        request<{ signedOut: boolean }>('/api/auth/logout', 'POST', {}, settings),
      recover: (ticket: string, settings?: RequestOptions) =>
        request<{ recovered: boolean }>('/api/auth/recover', 'POST', { ticket }, settings),
    },
  };
}
export type Transport = ReturnType<typeof createTransport>;
