/** Offline evidence only: declarations are supplied by the caller, never authenticated. */
export const PAGINATION_LIMITS = Object.freeze({
  pages: 1_000,
  pageSize: 1_000,
  expectedIds: 50_000,
  receivedIds: 100_000,
  identifierLength: 512,
  tokenLength: 4_096,
  stringUnits: 4_000_000,
  diagnostics: 100,
});

export interface PaginationBinding {
  query: string;
  principal: string;
  application: string;
  delegation: string;
  environment: string;
  contract: string;
  securityEpoch: string;
  snapshot: string;
}

export interface PaginationPage {
  request: { binding: PaginationBinding; continuation: string | null };
  response:
    | {
        status: 'ok';
        binding: PaginationBinding;
        authorization: 'unchanged' | 'changed' | 'unknown';
        ids: string[];
        continuation: string | null;
      }
    | { status: 'error'; binding: PaginationBinding; code: string }
    | null;
}

export interface PaginationTrace {
  version: 1;
  mode: 'fixed-snapshot';
  capture: 'complete' | 'truncated';
  binding: PaginationBinding;
  pageSize: number;
  expectedAuthorizedIds: string[];
  pages: PaginationPage[];
}

export interface PaginationDiagnostic {
  code: string;
  path: string;
  message: string;
}

export interface PaginationResult {
  status: 'malformed' | 'violation' | 'pass' | 'incomplete';
  diagnostics: PaginationDiagnostic[];
  diagnosticsTruncated: boolean;
  productionClaimsAllowed: false;
  evidence: 'caller-declared synthetic fixed-snapshot trace only';
}

const bindingKeys = [
  'query',
  'principal',
  'application',
  'delegation',
  'environment',
  'contract',
  'securityEpoch',
  'snapshot',
] as const;

class Malformed extends Error {
  constructor(readonly diagnostic: PaginationDiagnostic) {
    super(diagnostic.message);
  }
}

function parseTrace(input: unknown): PaginationTrace {
  let stringUnits = 0;
  let receivedIds = 0;
  function reject(path: string, message: string, code = 'INVALID_INPUT'): never {
    throw new Malformed({ code, path, message });
  }
  function object(value: unknown, path: string, keys: readonly string[]) {
    if (
      value === null ||
      typeof value !== 'object' ||
      Array.isArray(value) ||
      ![Object.prototype, null].includes(Object.getPrototypeOf(value))
    ) {
      reject(path, 'Expected a plain record');
    }
    const record = value as Record<string, unknown>;
    const ownKeys = Reflect.ownKeys(record);
    if (ownKeys.length !== keys.length || ownKeys.some((key) => !keys.includes(String(key)))) {
      reject(path, `Expected exactly these fields: ${keys.join(', ')}`);
    }
    for (const key of keys) {
      const descriptor = Object.getOwnPropertyDescriptor(record, key);
      if (!descriptor || !('value' in descriptor) || !descriptor.enumerable) {
        reject(`${path}.${key}`, 'Expected an enumerable own data field');
      }
    }
    return record;
  }
  function string(value: unknown, path: string, max: number = PAGINATION_LIMITS.identifierLength) {
    if (typeof value !== 'string' || value.length === 0 || value.length > max) {
      reject(path, `Expected a nonempty string of at most ${max} UTF-16 code units`);
    }
    stringUnits += value.length;
    if (stringUnits > PAGINATION_LIMITS.stringUnits) {
      reject(path, 'Aggregate string budget exceeded', 'LIMIT_EXCEEDED');
    }
    return value;
  }
  function choice<T extends string | number>(
    value: unknown,
    path: string,
    choices: readonly T[],
  ): T {
    if (!choices.includes(value as T)) {
      reject(path, `Expected one of: ${choices.join(', ')}`, 'UNSUPPORTED_VALUE');
    }
    return value as T;
  }
  function array(value: unknown, path: string, max: number): unknown[] {
    if (!Array.isArray(value) || value.length > max) {
      reject(path, `Expected an array with at most ${max} entries`, 'LIMIT_EXCEEDED');
    }
    if (Object.getPrototypeOf(value) !== Array.prototype) {
      reject(path, 'Expected an array with the ordinary Array prototype');
    }
    // Reject holes, extra fields and accessors without invoking array element getters.
    if (Reflect.ownKeys(value).length !== value.length + 1) {
      reject(path, 'Expected a dense array without extra fields');
    }
    for (let i = 0; i < value.length; i++) {
      const descriptor = Object.getOwnPropertyDescriptor(value, String(i));
      if (!descriptor || !('value' in descriptor)) {
        reject(`${path}[${i}]`, 'Expected an own data element');
      }
    }
    return value;
  }
  function binding(value: unknown, path: string): PaginationBinding {
    const record = object(value, path, bindingKeys);
    return Object.fromEntries(
      bindingKeys.map((key) => [key, string(record[key], `${path}.${key}`)]),
    ) as unknown as PaginationBinding;
  }
  function token(value: unknown, path: string) {
    return value === null ? null : string(value, path, PAGINATION_LIMITS.tokenLength);
  }
  const root = object(input, '$', [
    'version',
    'mode',
    'capture',
    'binding',
    'pageSize',
    'expectedAuthorizedIds',
    'pages',
  ]);
  const version = choice(root.version, '$.version', [1] as const);
  const mode = choice(root.mode, '$.mode', ['fixed-snapshot'] as const);
  const capture = choice(root.capture, '$.capture', ['complete', 'truncated'] as const);
  const declaredBinding = binding(root.binding, '$.binding');
  if (
    typeof root.pageSize !== 'number' ||
    !Number.isSafeInteger(root.pageSize) ||
    root.pageSize < 1 ||
    root.pageSize > PAGINATION_LIMITS.pageSize
  ) {
    reject('$.pageSize', 'Expected a safe integer from 1 through 1000');
  }
  const expected = array(
    root.expectedAuthorizedIds,
    '$.expectedAuthorizedIds',
    PAGINATION_LIMITS.expectedIds,
  );
  const seen = new Set<string>();
  const expectedAuthorizedIds = expected.map((id, i) => {
    const path = `$.expectedAuthorizedIds[${i}]`;
    const parsed = string(id, path);
    if (seen.has(parsed)) {
      reject(path, 'Expected IDs must be unique', 'DUPLICATE_EXPECTED_ID');
    }
    seen.add(parsed);
    return parsed;
  });
  const pages = array(root.pages, '$.pages', PAGINATION_LIMITS.pages).map(
    (value, i): PaginationPage => {
      const path = `$.pages[${i}]`;
      const page = object(value, path, ['request', 'response']);
      const request = object(page.request, `${path}.request`, ['binding', 'continuation']);
      const parsedRequest = {
        binding: binding(request.binding, `${path}.request.binding`),
        continuation: token(request.continuation, `${path}.request.continuation`),
      };
      if (page.response === null) {
        return { request: parsedRequest, response: null };
      }
      if (typeof page.response !== 'object' || Array.isArray(page.response)) {
        reject(`${path}.response`, 'Expected a response record or null');
      }
      // Read the discriminant only after checking that it is an own data property.
      const status = Object.getOwnPropertyDescriptor(page.response, 'status')?.value;
      choice(status, `${path}.response.status`, ['ok', 'error']);
      const response = object(
        page.response,
        `${path}.response`,
        status === 'ok'
          ? ['status', 'binding', 'authorization', 'ids', 'continuation']
          : ['status', 'binding', 'code'],
      );
      const responseBinding = binding(response.binding, `${path}.response.binding`);
      if (status === 'error') {
        return {
          request: parsedRequest,
          response: {
            status: 'error',
            binding: responseBinding,
            code: string(response.code, `${path}.response.code`),
          },
        };
      }
      const ids = array(response.ids, `${path}.response.ids`, PAGINATION_LIMITS.pageSize);
      receivedIds += ids.length;
      if (receivedIds > PAGINATION_LIMITS.receivedIds) {
        reject(`${path}.response.ids`, 'Total received ID limit exceeded', 'LIMIT_EXCEEDED');
      }
      return {
        request: parsedRequest,
        response: {
          status: 'ok',
          binding: responseBinding,
          authorization: choice(response.authorization, `${path}.response.authorization`, [
            'unchanged',
            'changed',
            'unknown',
          ] as const),
          ids: ids.map((id, j) => string(id, `${path}.response.ids[${j}]`)),
          continuation: token(response.continuation, `${path}.response.continuation`),
        },
      };
    },
  );
  return {
    version,
    mode,
    capture,
    binding: declaredBinding,
    pageSize: root.pageSize,
    expectedAuthorizedIds,
    pages,
  };
}

export function validatePaginationTrace(input: unknown): PaginationResult {
  const result: PaginationResult = {
    status: 'incomplete',
    diagnostics: [],
    diagnosticsTruncated: false,
    productionClaimsAllowed: false,
    evidence: 'caller-declared synthetic fixed-snapshot trace only',
  };
  let trace: PaginationTrace;
  try {
    trace = parseTrace(input);
  } catch (error) {
    if (!(error instanceof Malformed)) {
      throw error;
    }
    return { ...result, status: 'malformed', diagnostics: [error.diagnostic] };
  }
  let violated = false;
  let incomplete = false;
  function report(kind: 'violation' | 'incomplete', code: string, path: string, message: string) {
    violated ||= kind === 'violation';
    incomplete ||= kind === 'incomplete';
    if (result.diagnostics.length < PAGINATION_LIMITS.diagnostics) {
      result.diagnostics.push({ code, path, message });
    } else {
      result.diagnosticsTruncated = true;
    }
  }
  function checkBinding(binding: PaginationBinding, path: string) {
    for (const key of bindingKeys) {
      if (binding[key] !== trace.binding[key]) {
        report(
          'violation',
          'BINDING_CHANGED',
          `${path}.${key}`,
          'Must equal the declared traversal binding',
        );
      }
    }
  }
  const expected = new Map(trace.expectedAuthorizedIds.map((id, index) => [id, index]));
  const seen = new Set<string>();
  const tokens = new Set<string>();
  let previous: string | null = null;
  let terminal = false;
  let interrupted = false;
  let position = 0;
  let lastExpectedIndex = -1;
  if (trace.capture === 'truncated') {
    report(
      'incomplete',
      'TRUNCATED_CAPTURE',
      '$.capture',
      'Capture does not assert complete evidence',
    );
  }
  if (!trace.pages.length) {
    report(
      'incomplete',
      'NO_PAGES',
      '$.pages',
      'Even an empty result requires a terminal successful response',
    );
  }
  trace.pages.forEach((page, i) => {
    const path = `$.pages[${i}]`;
    if (terminal || interrupted) {
      report(
        'violation',
        'PAGE_AFTER_END',
        path,
        'No requests may follow completion, failure or an unobserved response',
      );
    }
    checkBinding(page.request.binding, `${path}.request.binding`);
    if (page.request.continuation !== previous) {
      report(
        'violation',
        'CONTINUATION_MISMATCH',
        `${path}.request.continuation`,
        i === 0
          ? 'First request must have null continuation'
          : 'Request must use the previous response continuation',
      );
    }
    const response = page.response;
    if (response === null) {
      interrupted = true;
      report(
        'incomplete',
        'MISSING_RESPONSE',
        `${path}.response`,
        'Request has no captured response',
      );
      return;
    }
    checkBinding(response.binding, `${path}.response.binding`);
    if (response.status === 'error') {
      interrupted = true;
      report(
        'incomplete',
        'ERROR_RESPONSE',
        `${path}.response.code`,
        'An error is not a successful traversal, including cursor expiry or invalidation',
      );
      return;
    }
    if (response.authorization !== 'unchanged') {
      report(
        'incomplete',
        'AUTHORIZATION_UNVERIFIABLE',
        `${path}.response.authorization`,
        'Exact membership requires unchanged current authorization, including current row eligibility',
      );
    }
    if (response.ids.length > trace.pageSize) {
      report(
        'violation',
        'PAGE_SIZE_EXCEEDED',
        `${path}.response.ids`,
        'Response exceeds declared page size',
      );
    }
    response.ids.forEach((id, j) => {
      const idPath = `${path}.response.ids[${j}]`;
      const index = expected.get(id);
      if (index === undefined) {
        report(
          'violation',
          'UNAUTHORIZED_ID',
          idPath,
          'ID is absent from the declared authorized snapshot',
        );
      } else {
        if (index < lastExpectedIndex) {
          report(
            'violation',
            'REORDERED_ID',
            idPath,
            'ID precedes an earlier received ID in the expected order',
          );
        }
        lastExpectedIndex = index;
        if (trace.expectedAuthorizedIds[position] !== id) {
          report(
            'violation',
            'ORDERED_MEMBERSHIP_MISMATCH',
            idPath,
            `Expected the ID at declared position ${position}`,
          );
        }
      }
      if (seen.has(id)) {
        report('violation', 'DUPLICATE_ID', idPath, 'ID was already received');
      }
      seen.add(id);
      position++;
    });
    if (response.continuation !== null) {
      if (
        tokens.has(response.continuation) ||
        response.continuation === page.request.continuation
      ) {
        report(
          'violation',
          'CONTINUATION_REUSED',
          `${path}.response.continuation`,
          'Response continuation must be fresh within this traversal',
        );
      }
      tokens.add(response.continuation);
    } else {
      terminal = true;
    }
    previous = response.continuation;
  });
  if (!terminal) {
    report(
      'incomplete',
      'NO_TERMINAL_RESPONSE',
      '$.pages',
      'No successful response with null continuation was observed',
    );
  }
  // A prefix is not evidence of omissions until a successful terminal response claims completion.
  if (terminal) {
    trace.expectedAuthorizedIds.forEach((id, index) => {
      if (!seen.has(id)) {
        report(
          'violation',
          'MISSING_ID',
          `$.expectedAuthorizedIds[${index}]`,
          'Declared authorized ID was not received before completion',
        );
      }
    });
  }
  result.status = violated ? 'violation' : incomplete ? 'incomplete' : 'pass';
  return result;
}
