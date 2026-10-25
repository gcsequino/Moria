# Offline pagination correctness oracle

Issue #31 / PERF-01d, a bounded slice of parent #20. Depends on the completed
#12 source baseline. Parent #20 remains open. This slice does not depend on
arrival accounting (#25 / PR #26) or runtime entry-point extraction.

## Contract and scope

The pure TypeScript API in `benchmarks/pagination-oracle.ts` compares caller-declared
expected authorized IDs, in their exact expected order, against synthetic captured
request/response pages. It does not discover expected membership from responses,
execute queries, authenticate declarations, decode tokens, or evaluate policy.
It has no I/O, clocks, services, credentials or model calls.

The accepted sources are [C05](../spec/core-contracts.md#c05--serving-revisions-and-pagination),
[S01/S04/S06](../spec/security-contracts.md), [D05/D06](../spec/review-decisions.md),
[WORKLOADS.md](WORKLOADS.md) and [ACCEPTANCE.md](ACCEPTANCE.md). C05 pins a revision
and generation map; stable sorting appends UID ascending. It binds cursors to
query, application, principal, delegation, contract and security epoch. The caller
must supply the entire expected sorted result, including tie-breaking. The oracle
never sorts or normalizes IDs itself.

C05 and S06 apply current authorization at every operation, including eligibility
of both pinned and current rows. Unchanged security epoch alone does not establish
unchanged row eligibility. Consequently version 1 supports only `fixed-snapshot`
evidence with an explicit per-success `authorization: 'unchanged'` declaration.
This asserts unchanged current authorization and current row eligibility throughout
the traversal. `changed` or `unknown` never permits a pass, even if all IDs match.
The oracle does not reinterpret omissions under changed authorization as success.
A violation still takes precedence if observed IDs contradict the declared snapshot.
Dynamic authorization experiments need separate evidence and are not certified here.

A pass is **only internal consistency of caller-supplied synthetic evidence**. It
is not real-server correctness, runtime integration, authorization/isolation proof,
workload coverage, capacity or production certification. `productionClaimsAllowed`
is always `false`, for every outcome. Existing database and isolation tests remain
necessary; this API does not replace them. WORKLOADS' provisional traffic/page mix
is not an approved target and is not executed by this oracle.

## API and example

```ts
import {
  validatePaginationTrace,
  type PaginationTrace,
} from '../../benchmarks/pagination-oracle.js';

const binding = {
  query: 'synthetic-query-filter-sort-projection-digest',
  principal: 'principal-A',
  application: 'application-A',
  delegation: 'none',
  environment: 'synthetic-environment',
  contract: 'C05-query-contract',
  securityEpoch: 'epoch-7',
  snapshot: 'release-generation-map-and-revision-12',
};
const trace: PaginationTrace = {
  version: 1,
  mode: 'fixed-snapshot',
  capture: 'complete',
  binding,
  pageSize: 1,
  expectedAuthorizedIds: ['object-A', 'object-B'],
  pages: [
    {
      request: { binding, continuation: null },
      response: {
        status: 'ok', binding, authorization: 'unchanged',
        ids: ['object-A'], continuation: 'opaque-next',
      },
    },
    {
      request: { binding, continuation: 'opaque-next' },
      response: {
        status: 'ok', binding, authorization: 'unchanged',
        ids: ['object-B'], continuation: null,
      },
    },
  ],
};
const result = validatePaginationTrace(trace); // status === 'pass'
// result.productionClaimsAllowed === false
```

The function accepts `unknown` and returns `PaginationResult`; exported interfaces
also support typed fixture construction. All listed fields are required. Records
reject unknown fields; missing, ambiguous and unsupported modes/versions fail
explicitly. Inputs are plain data records and dense arrays, with no accessors or
extra array fields. Arrays must have this realm's ordinary `Array.prototype`;
custom, subclass, cross-realm and null prototypes are rejected before any array
method or inherited accessor is read. Executable proxies are outside the data-envelope API contract.
Caller-owned input is not mutated or retained in the result.

`binding` at the root declares the fixed traversal context. Every request and
observed response repeats that full binding and must equal it field by field.
`query` identifies all query parameters, ordering and projection; `snapshot` identifies
the entire pinned revision/generation map and release compatibility context. These
are opaque synthetic identifiers, not actual serialized runtime metadata. Delegation
absence must be explicit (for example `none`). Epoch and revision identity use
strings, so large revisions need no numeric conversion.

A page has a request and one of these responses:

- `{ status: 'ok', binding, authorization, ids, continuation }`: captured successful
  response. `authorization` is `unchanged`, `changed` or `unknown`.
- `{ status: 'error', binding, code }`: failed operation, including `CURSOR_EXPIRED`
  or `CURSOR_INVALIDATED`. It cannot yield a successful complete traversal. Error
  codes are opaque bounded strings, not a classification of server correctness.
- `null`: a request without a captured response. Partial response bodies must be
  represented this way, never promoted to a successful response.

`capture: 'truncated'` marks incomplete capture even if a terminal response was
included. `complete` asserts completeness of the capture, not traversal success.
A successful terminal response has `continuation: null`. The first request must
have null continuation, and subsequent requests must exactly match the prior
response token. Response tokens must be fresh within the traversal. No requests
may follow terminal success, an error or an unobserved response. Retries and silent
restarts are not a supported traversal mode; start a separate trace for a new query.
Tokens are opaque strings: no signatures, private keys, token content or runtime
cursor format are required or verified.

Empty and short pages are allowed. C05 specifies a maximum, not a minimum number
of returned items, and current-policy filtering can reduce pages. An empty page
with a fresh continuation is not completion; it requires another captured request.
A complete empty expected result needs at least one successful terminal response,
no returned IDs and unchanged declared authorization. Zero pages never pass.

## Outcomes and diagnostics

Validation first checks the entire envelope. A malformed suffix cannot be hidden
by an earlier success or violation. It returns the first malformed-input diagnostic
without running invariant checks. Otherwise checks run in page order, then missing
IDs in expected order. Results are deterministic and never echo opaque IDs/tokens.
Each diagnostic has a `code`, JSON-style `path` and explanation. Status precedence:

| Status | Meaning |
|---|---|
| `malformed` | Unsupported/malformed input, duplicate expected IDs or exceeded envelope limits. |
| `violation` | At least one observed invariant contradicts the declaration, even if other evidence is incomplete. |
| `incomplete` | No observed violation, but capture, responses, authorization or terminal evidence cannot prove success. |
| `pass` | Complete successful fixed-snapshot traversal exactly matches declared authorized order and membership. |

Malformed codes are `INVALID_INPUT`, `UNSUPPORTED_VALUE`, `LIMIT_EXCEEDED` and
`DUPLICATE_EXPECTED_ID`. Violations include `BINDING_CHANGED`,
`CONTINUATION_MISMATCH`, `CONTINUATION_REUSED`, `PAGE_AFTER_END`,
`PAGE_SIZE_EXCEEDED`, `UNAUTHORIZED_ID`, `DUPLICATE_ID`, `REORDERED_ID`,
`ORDERED_MEMBERSHIP_MISMATCH` and `MISSING_ID`. An ID absent from the authorized
expectation, including a sentinel, is unauthorized relative to that declaration;
this is not an independent authorization decision. Multiple diagnostics can
identify different consequences of one injected fault.

Incomplete codes are `TRUNCATED_CAPTURE`, `NO_PAGES`, `MISSING_RESPONSE`,
`ERROR_RESPONSE`, `AUTHORIZATION_UNVERIFIABLE` and `NO_TERMINAL_RESPONSE`.
An exact prefix with no terminal response is incomplete, not proof of omitted IDs.
A terminal response missing expected IDs is a violation. Prefix order mismatches,
duplicates and unauthorized objects are already observable violations even before
completion. Diagnostic truncation does not suppress the outcome classification.

## Finite limits and evidence limits

`PAGINATION_LIMITS` exports these fixed limits; callers cannot disable them:

| Dimension | Inclusive bound |
|---|---:|
| Captured pages | 1,000 |
| Declared page size | Safe integer 1–1,000 (C05 maximum) |
| Received IDs per response | 1,000 |
| Expected IDs | 50,000 |
| Received IDs across all pages | 100,000 |
| Each ID, binding value, error code | 1–512 UTF-16 code units |
| Each non-null continuation | 1–4,096 UTF-16 code units |
| Sum of ID, binding, error-code and token string lengths, counting every occurrence | 4,000,000 UTF-16 code units |
| Returned diagnostics | 100; `diagnosticsTruncated` reports additional findings |

All object IDs are strings. Numbers (including safe integers), booleans, objects,
null and empty strings are invalid IDs. Numeric and string identities are never
coerced. Whitespace, leading zeros, case and Unicode normalization differences
are preserved. `pageSize` rejects fractions, unsafe integers, NaN and infinity.
An observed response exceeding the declared page size is a violation within the
1,000-entry envelope bound; an array larger than that hard bound is malformed.
Limits bound validation work, not incoming transport allocation; the API receives
an already materialized object. Membership/duplicate checks use Map/Set rather
than repeated linear searches: expected O(expected IDs + received IDs + pages +
validated string units) work and linear bounded auxiliary storage.

Not checked: cursor authentication, 15-minute TTL/original expiry, real query
semantics or UID tie-breaking derivation, current-policy truth, property/link
security, transport fidelity, payload byte size, server history retention,
transaction isolation, workloads, concurrency, latency, capacity, database or
runtime integration. A producer that lies consistently can construct a passing
trace. `evidence` on every result explicitly states the caller-declared synthetic
fixed-snapshot limit.

## Progress and validation evidence

Work is confined to the new oracle, its new offline test file and this document;
shared progress files, accepted contracts, runtime code, arrival accounting,
dependencies, CI and delivery policy are unchanged. The dedicated progress record
is used as required by this slice's exclusive scope.

Validation date: October 25, 2026. Base revision:
`5cc57ce480799e124e2b7b11bf4d428c9f3fe338`, plus these three uncommitted files.

Environment: non-root Linux (`uid=10001`), Node 24.21.0, pnpm 11.25.0,
workspace `.venv` Python 3.11.17. Synthetic fixtures only; no live installation,
credential files, external people or model calls are needed.

- `pnpm format`: passed; only the new TypeScript files needed formatting.
- `pnpm exec tsx --test tests/benchmark-pagination-oracle.test.ts`: 75 passed,
  zero failed/skipped. Fault injection covers membership, ordering, sentinels,
  continuation loops/restarts, every binding component on both sides, errors,
  missing/truncated evidence, changed/unknown authorization, malformed inputs,
  opacity, immutable input, finite limits and diagnostic truncation.
- Initial `pnpm typecheck`: found a string-length helper parameter inferred as
  the literal 512; corrected to `number` before the complete gate.
- Complete `pnpm check`: passed (exit 0). Formatting, SDK drift and TypeScript
  checks passed; 255 TypeScript tests passed with zero failures/skips, including
  the 75 oracle tests and existing real PostgreSQL/Iceberg transaction, cursor,
  authorization and isolation checks. Python formatting, specification/workload
  validation and 14 Python workload tests passed. Both bundled application builds
  and Station Explorer type/build/hosting smoke checks passed. No database tests
  were disabled or skipped. Local full-gate output is in
  `/tmp/issue-31-check.log` (ephemeral workspace evidence, not a published artifact).
- Final scope inspection: only the three permitted new files are present; no
  tracked existing file changes. `git diff --check` passed. The final edit after
  the gate only records these results in this Markdown document.

Independent human review and Linux quality CI on the final published PR head remain
unverified controller obligations. No PR head is published by this checkout task;
changes remain uncommitted. Only this slice is eligible for closure after those
gates. Parent #20 and broader production obligations remain open.

### Array validation review repair

The P2 review finding identified that inherited `map` implementations could replace
validated expected IDs, pages or response IDs, and null-prototype arrays could
throw. Array parsing now rejects any prototype other than the ordinary
`Array.prototype` before invoking array methods. Existing own-element descriptor
checks still reject accessors without executing them.

Regression coverage includes all three array fields with inherited methods,
inherited method/element accessors, own element accessors and null prototypes.
It asserts malformed diagnostics and locations, deterministic results, no caller
code execution and unchanged prototypes/property descriptors. A separate sentinel
fixture reproduces the authorization-expectation substitution attempt.

Repair validation on October 25, 2026, in the same non-root Linux environment:

- `pnpm format`: passed; no files outside the exclusive scope changed.
- `pnpm exec tsx --test tests/benchmark-pagination-oracle.test.ts`: 91 passed,
  zero failed/skipped, including 16 new regression cases.
- Complete `pnpm check`: passed (exit 0), including all 271 TypeScript tests with
  zero failures/skips, existing database/isolation checks, 14 Python workload
  tests, formatting, SDK drift, typechecking, specification validation, both
  application builds and starter checks. No checks were disabled.
- Repair gate output: `/tmp/issue-31-repair-check.log`; focused test output:
  `/tmp/issue-31-repair-oracle.log` (ephemeral local evidence).
- `git diff --check`: passed. Changes remain limited to the three permitted files
  and uncommitted. The post-gate edit only records evidence in this document.

Independent review of the repair and Linux quality CI on the final PR head remain
unverified controller obligations. Synthetic trace consistency still does not prove
real-server correctness or production readiness; parent #20 remains open.
