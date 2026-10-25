# Scoped streaming storage authority

Issue #42 (M2-01c), the bounded #17 slice after #40. Parent #17 remains open.
Implementation baseline: `1094996a284d9b8453e82ec623116fba6dca067d`.

This work follows C04/C08, S01–S03/S06–S08, R01–R02 and production
acceptance P01/P02/P06. Iceberg owns the dataset table format. Restricted
views remain terminal: Transform inputs must resolve to separately authorized
Dataset resources. This wrapper neither propagates weaker grants nor makes a
restricted-view grant sufficient for raw dataset access.

## Scope and service obligations

`createAuthorizedLocalStorage(binding, scope, authority)` in
`services/storage/src/authority.ts` returns a frozen streaming handle for one
exact warehouse/table/object tuple and an explicit subset of `read` and `create`.
Ranges require `read`; capability discovery only describes implementation
support and grants no access. Empty operation sets deny everything. There are
no wildcards, prefixes, inherited object grants, tokens, credentials or new
policy rules. Physical roots and logical locations never confer authority.

Only trusted service composition may supply the binding, scope and
`TrustedStorageAuthority`. Its `checkCurrent` method is the integration seam
for the existing current-policy service, with authenticated execution,
principal/application/delegation and resource identity bound by that service.
The request contains only an immutable exact logical location and operation.
The implementation must verify current permission, markings, environment,
delegation, revocation, expiry and any applicable execution fence on every call.
A captured scheduling decision or a caller-supplied boolean is insufficient.
The service must map logical objects to authorized Dataset resources and pinned
snapshots, supply checker deadlines and transfer abort signals, protect its
configuration, and implement required protected audit records. No current-policy
runtime integration is claimed by this slice.

Only literal `true` permits access. False, malformed decisions and checker
exceptions become `AUTHORITY_DENIED`, with no checker message, cause, physical
path or credential material. Unknown/malformed locations and options retain
adapter validation; cross-warehouse/table calls fail `BINDING_MISMATCH` and
cross-object/operation calls fail `AUTHORITY_DENIED`, before transfer filesystem
access. Factory initialization still creates/checks the trusted binding's
storage directories; it is service setup, not a user storage operation.

Scope and operation lists are copied at construction. Each transfer copies its
location and options before awaiting authority. The returned handle exposes
only the streaming interface, with no raw adapter or authority property. The
local factory's optional verification hook is an internal service mechanism,
not a transfer option. Raw local adapters and their factory remain available
only inside the trusted service boundary for existing internal users; possession
of those APIs by hostile same-process code is outside this protection.

## Check boundaries and residual window

The local adapter invokes the trusted check:

- After synchronous input validation, before opening a read object or acquiring
  a write source reader/creating staging.
- At every read demand, including EOF, before bounded filesystem I/O; again
  after a chunk read and before enqueuing its bytes.
- Before each write source `read()` and after its result, including empty chunks
  and EOF, before consuming those bytes into staging.
- After file sync/close and directory validation, immediately before the
  no-clobber final link operation.

There is no speculative read queue (`highWaterMark: 0`), whole-object buffering,
background revocation polling or policy cache. Writes request the next chunk
only after writing the preceding bounded chunk. Existing byte/chunk/range
limits, empty-chunk yielding, private staging permissions, fsync durability,
no-clobber behavior and uncertain-finalization errors are preserved.

Revocation/expiry is observed when the next check runs, not by a push
notification. An idle read retains its handle until demand, cancellation or
abort; consumers must drain or cancel. A stalled producer is checked after its
pending read resolves; a deadline/abort can cancel it earlier. An unresolved
checker cannot authorize progress; caller abort races the check and releases
transfer resources without waiting for the checker. Check implementations must
honor signals and enforce their own deadlines to avoid retaining their own
resources. Source cancellation callbacks are not awaited, as in the underlying
adapter, and are responsible for releasing their own resources.

A revocation racing after a successful check can overlap one bounded read or
source demand/write chunk. A post-I/O/post-source check suppresses bytes when it
observes denial, but there is still a check-to-enqueue/write window. Kernel I/O
is not forcibly interrupted. Already delivered bytes cannot be retracted.
Finalization can race revocation after the last check; this is not an atomic
provider fence. A finalized object remains only a candidate. The existing
catalog authority must recheck current policy and execution fencing before C04
atomic publication of heads, metadata pointers, checkpoints and lineage.

Denial during transfer errors the read and closes its file, or cancels/unlocks
the write reader and closes/removes private staging. Denial before source
acquisition leaves it unlocked and unconsumed. Previously finalized objects are
never overwritten or deleted. The underlying adapter's host namespace trust,
Linux filesystem and crash/durability limits still apply; see
[storage locations](STORAGE-LOCATIONS.md) and
[storage durability](STORAGE-DURABILITY.md).

## Evidence and remaining gates

Changes are confined to `services/storage/src/`,
`tests/storage-authority.test.ts` and this document. This issue's exclusive file
scope excludes `docs/IMPLEMENTATION.md`, so progress, commands and deviations
are recorded here. No dependencies, protected paths, delivery policy, runtime,
database schemas, production instances or credential files are changed or used.

Synthetic tests use actual temporary local files, four-byte maximum chunks,
64-byte maximum objects and deterministic service authority state changes.
They verify exact scope and operation denial before backend access, default
deny, expiry/revocation/checker failure before and during transfers, read demand
without prefetch, post-read denial, write pre-finalization denial, bounded source
demand, private staging mode/cleanup, reader cancellation/unlock, stalled-check
abort, no-clobber preservation and immutable scope/request snapshots. They are
local filesystem verification of this integration seam, not end-to-end policy,
user-code isolation or production security certification.

Environment: October 25, 2026; non-root Linux UID 10001; Node 24.21.0,
pnpm 11.25.0, repository `.venv` Python 3.11.17. Synthetic IDs are
`warehouse`, `table`, `object`; no production workload or capacity target.

Initial focused verification: `pnpm exec tsx --test
tests/storage-authority.test.ts tests/storage-streaming.test.ts
tests/storage-locations.test.ts tests/storage-durability.test.ts` passed 51 tests,
zero failures/skips. `pnpm typecheck` passed. Additional boundary and mutation
regressions were then added for the full gate. Temporary command evidence is in
`/tmp/issue-42-focused.log`, `/tmp/issue-42-format.log` and
`/tmp/issue-42-check.log`; these logs are not repository artifacts.

Independent review, human review of authorization/publication invariants, and
exact-head Linux CI on current-main integration remain controller-owned,
unverified delivery gates. No commits, publication, merge or production
operations are performed here. S3/catalog and trusted-engine runtime wiring,
cache/spill/result-release enforcement, distributed revocation behavior and
arbitrary-code isolation remain future #17 work. No local model inference is
needed for this storage slice; no paid model APIs are used.

The first full gate stopped at TypeScript checking because the new mutation
regression inferred a literal object ID. The fixture now explicitly widens that
mutable test value to string. No checks or acceptance criteria were changed.

Final local verification on October 25, 2026:

- `pnpm format`: passed; no out-of-scope tracked files changed.
- `pnpm check`: passed, exit 0. All 326 runtime tests passed, zero failures and
  zero skips; runtime test duration 114.135 seconds. Real PostgreSQL/Iceberg,
  publication, crash recovery, current-authority and storage durability tests
  ran. Formatting, SDK drift, TypeScript, Python formatting/schema/workload
  checks, both application builds and Station Explorer verification passed.
- `git diff --check`: passed. Only the five intended files in the exclusive
  scope are changed/added. Changes remain uncommitted.
- No required infrastructure for the local gate was unavailable. Independent
  review and exact-head Linux CI on current-main integration remain unverified;
  the local result does not replace those gates.
