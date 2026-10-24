/** Browser-safe runtime copied into generated SDKs. No credentials or server imports. */
export interface ClientOptions {
  /** Same origin by default. Cross-origin cookies are deliberately unsupported. */
  baseUrl?: string;
  fetch?: typeof fetch;
  onUnauthenticated?: () => void;
}
export interface RequestOptions {
  signal?: AbortSignal;
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
export interface Page<T> {
  revision: string;
  objects: T[];
  nextCursor?: string;
}
export interface ObjectResult<T> {
  revision: string;
  object: T;
}
export interface ActionResult {
  revision: string;
}
type Key<T> = Extract<keyof T, string>;
export type Filter<T> =
  | {
      [K in Key<T>]: {
        op: 'eq' | 'ne' | 'lt' | 'lte' | 'gt' | 'gte';
        args: [{ field: K }, { literal: T[K] }];
      };
    }[Key<T>]
  | { [K in Key<T>]: { op: 'in'; args: [{ field: K }, { literal: T[K][] }] } }[Key<T>]
  | { op: 'isNull'; args: [{ field: Key<T> }] }
  | { op: 'and' | 'or'; args: Filter<T>[] }
  | { op: 'not'; args: [Filter<T>] };
export interface Query<T, K extends Key<T> = Key<T>> {
  filter?: Filter<T>;
  projection?: readonly K[];
  pageSize?: number;
  cursor?: string;
}
export interface AllOptions extends RequestOptions {
  maxRows?: number;
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
  async function request<T>(
    path: string,
    method: 'GET' | 'POST',
    input?: unknown,
    settings: RequestOptions & { idempotencyKey?: string } = {},
  ): Promise<T> {
    if (!path.startsWith('/api/') || path.includes('://')) throw new Error('Invalid SDK route.');
    const response = await fetcher(base + path, {
      method,
      credentials: 'same-origin',
      redirect: 'error',
      signal: settings.signal,
      headers: {
        'content-type': 'application/json',
        ...(settings.idempotencyKey ? { 'idempotency-key': settings.idempotencyKey } : {}),
      },
      ...(input === undefined ? {} : { body: JSON.stringify(input) }),
    });
    if (response.status === 401) options.onUnauthenticated?.();
    let data: any;
    try {
      data = await response.json();
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
  return {
    request,
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
export function objectClient<T>(transport: Transport, resource: string, fields: readonly Key<T>[]) {
  const route = '/api/v1/objects/' + encodeURIComponent(resource);
  // Explicit projection: the server rejects inaccessible fields instead of silently
  // omitting them while the TypeScript result promises they exist.
  async function query<K extends Key<T> = Key<T>>(
    input: Query<T, K> = {},
    settings?: RequestOptions,
  ) {
    return transport.request<Page<Pick<T, K>>>(
      route + '/query',
      'POST',
      { ...input, projection: input.projection ?? fields },
      settings,
    );
  }
  async function* pages<K extends Key<T> = Key<T>>(
    input: Query<T, K> = {},
    settings?: RequestOptions,
  ): AsyncGenerator<Page<Pick<T, K>>> {
    let cursor = input.cursor;
    const seen = new Set<string>();
    do {
      const page = await query({ ...input, ...(cursor ? { cursor } : {}) }, settings);
      yield page;
      cursor = page.nextCursor;
      if (cursor && seen.has(cursor))
        throw new ApiError(502, 'CURSOR_LOOP', 'Server repeated a cursor.');
      if (cursor) seen.add(cursor);
    } while (cursor);
  }
  return {
    query,
    get: <K extends Key<T> = Key<T>>(
      uid: string,
      properties?: readonly K[],
      settings?: RequestOptions,
    ) =>
      transport.request<ObjectResult<Pick<T, K>>>(
        route +
          '/' +
          encodeURIComponent(uid) +
          '?properties=' +
          encodeURIComponent((properties ?? fields).join(',')),
        'GET',
        undefined,
        settings,
      ),
    pages,
    async all<K extends Key<T> = Key<T>>(
      input: Query<T, K> = {},
      settings: AllOptions = {},
    ): Promise<Pick<T, K>[]> {
      const max = settings.maxRows ?? 20000;
      if (!Number.isInteger(max) || max < 1) throw new Error('maxRows must be a positive integer.');
      const rows: Pick<T, K>[] = [];
      for await (const page of pages(input, settings)) {
        if (rows.length + page.objects.length > max)
          throw new ApiError(413, 'CLIENT_ROW_LIMIT', 'Query exceeds client row limit.');
        rows.push(...page.objects);
      }
      return rows;
    },
  };
}
export function actionClient<I>(transport: Transport, resource: string) {
  return (input: I, settings: ActionOptions): Promise<ActionResult> => {
    if (!settings?.idempotencyKey)
      throw new Error(
        'An explicit idempotencyKey is required. Reuse it when retrying the same operation.',
      );
    return transport.request(
      '/api/v1/actions/' + encodeURIComponent(resource) + '/invoke',
      'POST',
      { input },
      settings,
    );
  };
}
