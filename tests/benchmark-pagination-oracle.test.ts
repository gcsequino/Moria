import assert from 'node:assert/strict';
import test from 'node:test';
import {
  PAGINATION_LIMITS as limits,
  validatePaginationTrace as validate,
  type PaginationTrace,
  type PaginationPage,
} from '../benchmarks/pagination-oracle.js';

function fixture(): PaginationTrace {
  const binding = {
    query: 'query/filter/sort/projection',
    principal: 'human-1',
    application: 'app-1',
    delegation: 'none',
    environment: 'synthetic',
    contract: 'C05',
    securityEpoch: '7',
    snapshot: 'generation-map/revision-12',
  };
  return {
    version: 1,
    mode: 'fixed-snapshot',
    capture: 'complete',
    binding,
    pageSize: 2,
    expectedAuthorizedIds: ['a', 'b', 'c'],
    pages: [
      {
        request: { binding: { ...binding }, continuation: null },
        response: {
          status: 'ok',
          binding: { ...binding },
          authorization: 'unchanged',
          ids: ['a', 'b'],
          continuation: 'next-1',
        },
      },
      {
        request: { binding: { ...binding }, continuation: 'next-1' },
        response: {
          status: 'ok',
          binding: { ...binding },
          authorization: 'unchanged',
          ids: ['c'],
          continuation: null,
        },
      },
    ],
  };
}
function ok(trace: PaginationTrace, i: number) {
  const response = trace.pages[i].response;
  assert.ok(response?.status === 'ok');
  return response;
}
function expectCode(input: unknown, status: string, code: string, path?: string) {
  const before = structuredClone(input);
  const result = validate(input);
  assert.equal(result.status, status);
  assert.ok(
    result.diagnostics.some((d) => d.code === code && (!path || d.path === path)),
    JSON.stringify(result),
  );
  assert.equal(result.productionClaimsAllowed, false);
  assert.deepEqual(input, before);
  assert.deepEqual(validate(input), result);
}

test('valid multi-page snapshot is deterministic and leaves frozen input intact', () => {
  const input = fixture();
  function freeze(value: unknown) {
    if (value && typeof value === 'object') {
      Object.values(value).forEach(freeze);
      Object.freeze(value);
    }
  }
  freeze(input);
  assert.deepEqual(validate(input), {
    status: 'pass',
    diagnostics: [],
    diagnosticsTruncated: false,
    productionClaimsAllowed: false,
    evidence: 'caller-declared synthetic fixed-snapshot trace only',
  });
});

test('empty results require an explicit successful terminal response', () => {
  const input = fixture();
  input.expectedAuthorizedIds = [];
  input.pages = [input.pages[0]];
  ok(input, 0).ids = [];
  ok(input, 0).continuation = null;
  assert.equal(validate(input).status, 'pass');
  input.pages = [];
  expectCode(input, 'incomplete', 'NO_PAGES');
});

test('empty nonterminal pages are allowed only with progressing continuation', () => {
  const input = fixture();
  const empty = structuredClone(input.pages[0]);
  assert.ok(empty.response?.status === 'ok');
  empty.response.ids = [];
  empty.response.continuation = 'empty-next';
  input.pages[0].request.continuation = 'empty-next';
  input.pages.unshift(empty);
  assert.equal(validate(input).status, 'pass');
  input.pages.pop();
  expectCode(input, 'incomplete', 'NO_TERMINAL_RESPONSE');
});

const violations: [string, (t: PaginationTrace) => void, string, string?][] = [
  [
    'missing final item',
    (t) => {
      ok(t, 1).ids = [];
    },
    'MISSING_ID',
    '$.expectedAuthorizedIds[2]',
  ],
  [
    'skipped middle item',
    (t) => {
      ok(t, 0).ids = ['a'];
    },
    'MISSING_ID',
  ],
  [
    'duplicate across pages',
    (t) => {
      ok(t, 1).ids = ['b', 'c'];
    },
    'DUPLICATE_ID',
  ],
  [
    'duplicate within page',
    (t) => {
      ok(t, 0).ids = ['a', 'a'];
    },
    'DUPLICATE_ID',
  ],
  [
    'reordered within page',
    (t) => {
      ok(t, 0).ids = ['b', 'a'];
    },
    'REORDERED_ID',
  ],
  [
    'reordered across pages',
    (t) => {
      ok(t, 0).ids = ['c', 'b'];
      ok(t, 1).ids = ['a'];
    },
    'REORDERED_ID',
  ],
  [
    'unauthorized sentinel',
    (t) => {
      ok(t, 1).ids = ['secret-sentinel'];
    },
    'UNAUTHORIZED_ID',
    '$.pages[1].response.ids[0]',
  ],
  [
    'first continuation',
    (t) => {
      t.pages[0].request.continuation = 'unexpected';
    },
    'CONTINUATION_MISMATCH',
  ],
  [
    'wrong next token',
    (t) => {
      t.pages[1].request.continuation = 'wrong';
    },
    'CONTINUATION_MISMATCH',
  ],
  [
    'silent restart',
    (t) => {
      t.pages[1].request.continuation = null;
    },
    'CONTINUATION_MISMATCH',
  ],
  [
    'token loop',
    (t) => {
      ok(t, 1).continuation = 'next-1';
    },
    'CONTINUATION_REUSED',
  ],
  [
    'page after terminal',
    (t) => {
      t.pages.push(structuredClone(t.pages[1]));
    },
    'PAGE_AFTER_END',
  ],
  [
    'oversized declared page',
    (t) => {
      t.pageSize = 1;
    },
    'PAGE_SIZE_EXCEEDED',
  ],
];
for (const [name, mutate, code, path] of violations) {
  test(`detects ${name}`, () => {
    const input = fixture();
    mutate(input);
    expectCode(input, 'violation', code, path);
  });
}
for (const key of Object.keys(fixture().binding) as (keyof PaginationTrace['binding'])[]) {
  for (const side of ['request', 'response'] as const) {
    test(`detects changed ${side} ${key}`, () => {
      const input = fixture();
      const target = input.pages[1][side];
      assert.ok(target);
      target.binding[key] += '-changed';
      expectCode(input, 'violation', 'BINDING_CHANGED', `$.pages[1].${side}.binding.${key}`);
    });
  }
}

test('detects a longer token cycle', () => {
  const input = fixture();
  ok(input, 1).continuation = 'next-2';
  const page = structuredClone(input.pages[1]);
  page.request.continuation = 'next-2';
  assert.ok(page.response?.status === 'ok');
  page.response.ids = [];
  page.response.continuation = 'next-1';
  input.pages.push(page);
  expectCode(input, 'violation', 'CONTINUATION_REUSED');
});

test('truncation, failed requests and missing responses cannot pass', () => {
  const input = fixture();
  input.pages.pop();
  expectCode(input, 'incomplete', 'NO_TERMINAL_RESPONSE');
  assert.ok(!validate(input).diagnostics.some((d) => d.code === 'MISSING_ID'));
  for (const code of ['CURSOR_EXPIRED', 'CURSOR_INVALIDATED', 'INTERNAL_ERROR']) {
    const failed = fixture();
    failed.pages[1].response = { status: 'error', binding: { ...failed.binding }, code };
    expectCode(failed, 'incomplete', 'ERROR_RESPONSE');
    failed.pages.push(structuredClone(fixture().pages[1]));
    expectCode(failed, 'violation', 'PAGE_AFTER_END');
  }
  const missing = fixture();
  missing.pages[1].response = null;
  expectCode(missing, 'incomplete', 'MISSING_RESPONSE');
  missing.pages.push(structuredClone(fixture().pages[1]));
  expectCode(missing, 'violation', 'PAGE_AFTER_END');
  const truncated = fixture();
  truncated.capture = 'truncated';
  expectCode(truncated, 'incomplete', 'TRUNCATED_CAPTURE');
});
for (const authorization of ['unknown', 'changed'] as const) {
  test(`matching IDs do not override ${authorization} current authorization`, () => {
    const input = fixture();
    ok(input, 1).authorization = authorization;
    expectCode(input, 'incomplete', 'AUTHORIZATION_UNVERIFIABLE');
  });
}

test('IDs and tokens remain opaque, case-sensitive and unnormalized', () => {
  const input = fixture();
  input.expectedAuthorizedIds = ['01', 'é', 'e\u0301', ' A ', 'a'];
  input.pageSize = 5;
  input.pages = [input.pages[0]];
  ok(input, 0).ids = [...input.expectedAuthorizedIds];
  ok(input, 0).continuation = null;
  assert.equal(validate(input).status, 'pass');
  ok(input, 0).ids[0] = '1';
  expectCode(input, 'violation', 'UNAUTHORIZED_ID');
  const tokens = fixture();
  tokens.pages[1].request.continuation = ' next-1';
  expectCode(tokens, 'violation', 'CONTINUATION_MISMATCH');
});

const malformed: [string, (t: PaginationTrace) => void][] = [
  [
    'version',
    (t) => {
      Reflect.set(t, 'version', 2);
    },
  ],
  [
    'mode',
    (t) => {
      Reflect.set(t, 'mode', 'current-authorization');
    },
  ],
  [
    'unknown field',
    (t) => {
      Reflect.set(t, 'extra', true);
    },
  ],
  [
    'missing capture',
    (t) => {
      Reflect.deleteProperty(t, 'capture');
    },
  ],
  [
    'duplicate expected IDs',
    (t) => {
      t.expectedAuthorizedIds.push('a');
    },
  ],
  [
    'numeric ID',
    (t) => {
      Reflect.set(ok(t, 0).ids, '0', 1);
    },
  ],
  [
    'numeric expected ID',
    (t) => {
      Reflect.set(t.expectedAuthorizedIds, '0', 1);
    },
  ],
  [
    'numeric token',
    (t) => {
      Reflect.set(ok(t, 0), 'continuation', 123);
    },
  ],
  [
    'oversized ID',
    (t) => {
      t.expectedAuthorizedIds[0] = 'x'.repeat(limits.identifierLength + 1);
    },
  ],
  [
    'oversized token',
    (t) => {
      ok(t, 0).continuation = 'x'.repeat(limits.tokenLength + 1);
    },
  ],
  [
    'empty token',
    (t) => {
      ok(t, 0).continuation = '';
    },
  ],
  [
    'empty binding',
    (t) => {
      t.binding.query = '';
    },
  ],
  [
    'array hole',
    (t) => {
      delete t.expectedAuthorizedIds[0];
    },
  ],
  [
    'oversized page',
    (t) => {
      ok(t, 0).ids = Array(limits.pageSize + 1).fill('a');
    },
  ],
  [
    'too many expected',
    (t) => {
      t.expectedAuthorizedIds = Array(limits.expectedIds + 1).fill('a');
    },
  ],
  [
    'too many pages',
    (t) => {
      t.pages = Array(limits.pages + 1).fill(t.pages[0]);
    },
  ],
  [
    'ambiguous error',
    (t) => {
      Reflect.set(ok(t, 0), 'status', 'error');
    },
  ],
];
for (const [name, mutate] of malformed) {
  test(`rejects malformed ${name}`, () => {
    const input = fixture();
    mutate(input);
    assert.equal(validate(input).status, 'malformed');
  });
}
for (const value of [0, -1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, '2', 1001, null]) {
  test(`rejects invalid page size ${String(value)}`, () => {
    const input = fixture();
    Reflect.set(input, 'pageSize', value);
    expectCode(input, 'malformed', 'INVALID_INPUT', '$.pageSize');
  });
}
for (const value of [undefined, null, false, 1, 'ok', [], {}]) {
  test(`rejects malformed root and response ${String(value)}`, () => {
    assert.equal(validate(value).status, 'malformed');
    if (value !== null) {
      const input = fixture();
      Reflect.set(input.pages[0], 'response', value);
      assert.equal(validate(input).status, 'malformed');
    }
  });
}

test('rejects accessors without calling them', () => {
  const input = fixture();
  Object.defineProperty(input, 'version', {
    get() {
      assert.fail('must not execute');
    },
  });
  assert.equal(validate(input).status, 'malformed');
});

test('identifier and token length boundaries are inclusive', () => {
  const input = fixture();
  const id = 'x'.repeat(limits.identifierLength);
  input.expectedAuthorizedIds[0] = id;
  ok(input, 0).ids[0] = id;
  const token = 't'.repeat(limits.tokenLength);
  ok(input, 0).continuation = token;
  input.pages[1].request.continuation = token;
  assert.equal(validate(input).status, 'pass');
});

function largeTrace(count: number, pageSize: number): PaginationTrace {
  const input = fixture();
  input.pageSize = pageSize;
  input.expectedAuthorizedIds = Array.from({ length: count }, (_, i) => `id-${i}`);
  input.pages = [];
  for (let start = 0; start < count; start += pageSize) {
    const end = Math.min(count, start + pageSize);
    input.pages.push({
      request: {
        binding: { ...input.binding },
        continuation: start === 0 ? null : `token-${start}`,
      },
      response: {
        status: 'ok',
        binding: { ...input.binding },
        authorization: 'unchanged',
        ids: input.expectedAuthorizedIds.slice(start, end),
        continuation: end === count ? null : `token-${end}`,
      },
    });
  }
  return input;
}

test('accepts maximum expected membership, page size and page count', () => {
  assert.equal(validate(largeTrace(limits.expectedIds, limits.pageSize)).status, 'pass');
  assert.equal(validate(largeTrace(limits.pages, 1)).status, 'pass');
});

test('received-item budget and diagnostic output are bounded', () => {
  const input = largeTrace(1000, 1000);
  const page = input.pages[0];
  input.pages = Array.from({ length: 100 }, () => structuredClone(page));
  const result = validate(input);
  assert.equal(result.status, 'violation');
  assert.equal(result.diagnostics.length, limits.diagnostics);
  assert.equal(result.diagnosticsTruncated, true);
  input.pages.push(structuredClone(page));
  expectCode(input, 'malformed', 'LIMIT_EXCEEDED');
});

test('aggregate string budget rejects individually valid fields', () => {
  const input = fixture();
  const page: PaginationPage = structuredClone(input.pages[0]);
  assert.ok(page.response?.status === 'ok');
  page.response.continuation = 'x'.repeat(limits.tokenLength);
  input.pages = Array.from({ length: limits.pages }, () => structuredClone(page));
  expectCode(input, 'malformed', 'LIMIT_EXCEEDED');
});

const arrayTargets: [string, (trace: PaginationTrace) => unknown[]][] = [
  ['$.expectedAuthorizedIds', (trace) => trace.expectedAuthorizedIds],
  ['$.pages', (trace) => trace.pages],
  ['$.pages[0].response.ids', (trace) => ok(trace, 0).ids],
];
for (const [path, select] of arrayTargets) {
  for (const variant of [
    'inherited map',
    'inherited map accessor',
    'inherited element accessor',
    'own element accessor',
    'null prototype',
  ]) {
    test(`rejects ${variant} at ${path} without executing caller code`, () => {
      const input = fixture();
      const target = select(input);
      let calls = 0;
      const unexpectedCall = () => {
        calls++;
        throw new Error('Caller code must not execute');
      };
      if (variant === 'null prototype') {
        Object.setPrototypeOf(target, null);
      } else if (variant === 'own element accessor') {
        Object.defineProperty(target, '0', { get: unexpectedCall });
      } else {
        const prototype = Object.create(Array.prototype);
        if (variant === 'inherited map') {
          Object.defineProperty(prototype, 'map', { value: unexpectedCall });
        } else if (variant === 'inherited map accessor') {
          Object.defineProperty(prototype, 'map', { get: unexpectedCall });
        } else {
          delete target[0];
          Object.defineProperty(prototype, '0', { get: unexpectedCall });
        }
        Object.setPrototypeOf(target, prototype);
      }
      const prototypeBefore = Object.getPrototypeOf(target);
      const descriptorsBefore = Object.getOwnPropertyDescriptors(target);
      const result = validate(input);
      assert.equal(result.status, 'malformed');
      assert.equal(result.productionClaimsAllowed, false);
      assert.equal(result.diagnostics[0].code, 'INVALID_INPUT');
      assert.equal(
        result.diagnostics[0].path,
        variant === 'own element accessor' ? `${path}[0]` : path,
      );
      assert.deepEqual(validate(input), result);
      assert.equal(calls, 0);
      assert.equal(Object.getPrototypeOf(target), prototypeBefore);
      assert.deepEqual(Object.getOwnPropertyDescriptors(target), descriptorsBefore);
    });
  }
}

test('inherited map cannot replace the authorized expectation with an unauthorized sentinel', () => {
  const input = fixture();
  input.expectedAuthorizedIds = ['allowed'];
  input.pages = [input.pages[0]];
  ok(input, 0).ids = ['secret-sentinel'];
  ok(input, 0).continuation = null;
  assert.equal(validate(input).status, 'violation');
  let calls = 0;
  const prototype = Object.create(Array.prototype);
  Object.defineProperty(prototype, 'map', {
    value() {
      calls++;
      return ['secret-sentinel'];
    },
  });
  Object.setPrototypeOf(input.expectedAuthorizedIds, prototype);
  const result = validate(input);
  assert.equal(result.status, 'malformed');
  assert.equal(result.diagnostics[0].path, '$.expectedAuthorizedIds');
  assert.equal(calls, 0);
});
