/** Ontology client helpers; HTTP transport is shared with the platform API client. */
import { ApiError, type Transport, type RequestOptions, type ActionOptions } from './transport.js';
import {
  decodeValue,
  encodeValue,
  normalizeType,
  validateRow,
  validateValue,
  bindOntologyObjects,
} from './values.js';
import {
  ONTOLOGY_OBJECT,
  type TypeSchema,
  type ValueType,
  type ObjectIdentity,
} from './value-types.js';
export { VALUE_VERSION } from './value-types.js';
export { decodeValue, encodeValue, canonicalValue } from './values.js';
export type { TypeSchema, ValueType, ScalarType, JsonValue } from './value-types.js';
export { createTransport, ApiError } from './transport.js';
export type {
  ClientOptions,
  RequestOptions,
  ActionOptions,
  AuthState,
  Transport,
} from './transport.js';
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
  proposed?: boolean;
  created?: { objectType: string; uid: string; version: string }[];
  changed?: { objectType: string; uid: string; version: string; deleted: boolean }[];
}

/** An identity handle; every property fetch uses the current authorized transport. */
export interface OntologyObject<T extends string, P> {
  readonly objectType: T;
  readonly uid: string;
  readonly [ONTOLOGY_OBJECT]: ObjectIdentity & { objectType: T };
  get<K extends Extract<keyof P, string> = Extract<keyof P, string>>(
    properties?: readonly K[],
    settings?: RequestOptions,
  ): Promise<ObjectResult<Pick<P, K>>>;
}

export function bindObject(
  client: { objects: Record<string, { objectType: string; object(uid: string): unknown }> },
  identity: ObjectIdentity,
): unknown {
  const candidates = Object.values(client.objects).filter(
    (value) => value.objectType === identity.objectType,
  );
  if (candidates.length !== 1)
    throw new ApiError(400, 'OBJECT_TYPE_UNBOUND', 'Ontology object type is not bound by this SDK');
  return candidates[0].object(identity.uid);
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

const text: TypeSchema = { type: 'string', nullable: false };
export async function typedRequest<T>(
  transport: Transport,
  path: string,
  method: 'GET' | 'POST',
  input: unknown,
  inputType: TypeSchema | undefined,
  outputType: TypeSchema,
  definitions: Record<string, TypeSchema>,
  settings?: RequestOptions,
  bind?: (identity: ObjectIdentity) => unknown,
): Promise<T> {
  const result = await transport.request(
    path,
    method,
    inputType ? encodeValue(input, inputType, definitions) : input,
    settings,
  );
  return (
    bind
      ? await bindOntologyObjects(result, outputType, bind, definitions)
      : decodeValue(result, outputType, definitions)
  ) as T;
}
export function objectClient<T, I extends string = string>(
  transport: Transport,
  resource: string,
  properties: Record<string, ValueType>,
  objectType?: I,
) {
  for (const type of Object.values(properties)) normalizeType(type, {}, 'column');
  const fields = Object.keys(properties) as Key<T>[];
  const row = (selected: readonly Key<T>[]): TypeSchema => ({
    type: 'object',
    properties: Object.fromEntries(
      selected.map((key) => {
        if (!Object.hasOwn(properties, key))
          throw new ApiError(400, 'QUERY_INVALID', 'Unknown projected property');
        return [key, properties[key]];
      }),
    ),
  });
  const route = '/api/v1/objects/' + encodeURIComponent(objectType ?? resource);
  // Explicit projection: the server rejects inaccessible fields instead of silently
  // omitting them while the TypeScript result promises they exist.
  async function query<K extends Key<T> = Key<T>>(
    input: Query<T, K> = {},
    settings?: RequestOptions,
  ) {
    return typedRequest<Page<Pick<T, K>>>(
      transport,
      route + '/query',
      'POST',
      { ...input, projection: input.projection ?? fields },
      undefined,
      {
        type: 'object',
        properties: {
          revision: text,
          objects: { type: 'list', items: row(input.projection ?? fields) },
          nextCursor: text,
        },
        optional: ['nextCursor'],
      },
      {},
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
  const get = <K extends Key<T> = Key<T>>(
    uid: string,
    properties?: readonly K[],
    settings?: RequestOptions,
  ) => {
    validateValue(uid, { type: 'uid', nullable: false });
    return typedRequest<ObjectResult<Pick<T, K>>>(
      transport,
      route +
        '/' +
        encodeURIComponent(uid) +
        '?properties=' +
        encodeURIComponent((properties ?? fields).join(',')),
      'GET',
      undefined,
      undefined,
      { type: 'object', properties: { revision: text, object: row(properties ?? fields) } },
      {},
      settings,
    );
  };
  return {
    objectType: objectType ?? '',
    object(uid: string): OntologyObject<I, T> {
      validateValue(objectType, { type: 'uid', nullable: false });
      validateValue(uid, { type: 'uid', nullable: false });
      const identity = Object.freeze({ objectType: objectType!, uid });
      return Object.freeze({
        ...identity,
        [ONTOLOGY_OBJECT]: identity,
        get: <K extends Key<T> = Key<T>>(properties?: readonly K[], settings?: RequestOptions) =>
          get(uid, properties, settings),
      });
    },
    query,
    get,
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
export function actionClient<I>(
  transport: Transport,
  resource: string,
  inputs: Record<string, ValueType>,
) {
  return (input: I, settings: ActionOptions): Promise<ActionResult> => {
    if (!settings?.idempotencyKey)
      throw new Error(
        'An explicit idempotencyKey is required. Reuse it when retrying the same operation.',
      );
    return typedRequest<ActionResult>(
      transport,
      '/api/v1/actions/' + encodeURIComponent(resource) + '/invoke',
      'POST',
      { input: validateRow(input, inputs) },
      undefined,
      {
        type: 'object',
        properties: {
          revision: text,
          proposed: { type: 'boolean', nullable: false },
          created: {
            type: 'list',
            items: {
              type: 'object',
              properties: {
                objectType: text,
                uid: { type: 'uid', nullable: false },
                version: text,
              },
            },
          },
          changed: {
            type: 'list',
            items: {
              type: 'object',
              properties: {
                objectType: text,
                uid: { type: 'uid', nullable: false },
                version: text,
                deleted: { type: 'boolean', nullable: false },
              },
            },
          },
        },
        optional: ['proposed', 'created', 'changed'],
      },
      {},
      settings,
    );
  };
}
