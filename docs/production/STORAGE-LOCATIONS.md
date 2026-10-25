# Logical storage locations and local streaming

Issue #32, M2-01a; bounded foundation for #17, not completion of #17 or M2.
Runtime integration remains untouched. The checkout baseline is `d4b1ffc`, which
contains the manual merge of #31 via #33; #13 is also in its ancestry.

## Contracts and scope

This slice follows [C04 and C08](../spec/core-contracts.md),
[S06–S08](../spec/security-contracts.md), [R01–R02](../spec/runtime-contracts.md),
the accepted [storage decision](../spec/storage-and-row-security.md), and the
[production acceptance obligations](ACCEPTANCE.md), especially P01, P02 and P06.
Iceberg continues to own dataset metadata and table format. Restricted views
remain terminal and cannot become Transform inputs.

Only `services/storage/src/`, `tests/storage-locations.test.ts`,
`tests/storage-streaming.test.ts`, and this document are added. Progress and
results are recorded here under the issue-specific scope exception; the shared
implementation log, delivery policy, dependencies, existing tests, and runtime
integration are unchanged. No credentials, production directories, live
installations or external services are used by this slice.

## API and binding

`locations.ts` defines `StorageLocation`, `StreamingStorage`, options and
`StorageError`. The version 1 location is a strict record:

```ts
{
  version: 1,
  warehouseId: 'warehouse',
  tableId: 'table',
  objectId: 'object'
}
```

All three IDs are opaque, case-sensitive ASCII tokens: 1–128 characters,
starting with an alphanumeric and continuing with alphanumerics, `_` or `-`.
They are not Iceberg paths or resource names. No decoding or normalization is
performed. Slashes, backslashes, dots, percent encoding (including double
encoding), Unicode aliases, colons, whitespace, NUL, absolute paths and unknown
fields/versions are rejected. A physical root or credential-reference field
cannot convey authority because neither is accepted in a location.

`createLocalStorage(binding)` is a trusted internal configuration entry point.
The binding supplies one preexisting absolute isolated root, one warehouse ID,
one table ID, `maxObjectBytes` (nonnegative safe integer), and `chunkBytes`
(1–1,048,576). Configuration is copied; later caller mutation cannot broaden the
binding. Calls for any other warehouse or table fail with `BINDING_MISMATCH`.
The service must own configuration, protect the root and its ancestors, and
choose distinct roots/identities for isolation domains. This API is not a public
endpoint, authorization service, credential loader, or mechanism for accepting
user-selected roots. Possessing a location does not grant access. Only a trusted,
already-authorized service may receive the adapter.

The local representation is `<root>/<warehouseId>/<tableId>/objects/<objectId>`;
private temporary objects live in the adjacent `staging` directory. This layout
is an implementation detail, not part of the provider-independent identity.
Missing binding subdirectories are created with mode 0700; existing directory
ownership and access permissions must already be correctly configured by the
trusted host. Temporary files are created exclusively with mode 0600.

`read(location, { range?, signal? })` returns a Web `ReadableStream<Uint8Array>`.
A range is an offset plus length, representing a half-open interval; both must
be nonnegative safe integers within the configured object bound and actual
file size. Reading beyond EOF fails rather than truncating a requested range.
Zero length at EOF and empty objects are supported. Only an omitted or undefined
range selects the whole object; null, primitives, arrays and incomplete ranges
fail with `INVALID_INPUT` before filesystem access. Range options are copied
before asynchronous I/O. Oversized files and nonregular files are rejected.

`write(location, source, { size, signal? })` requires an unlocked Web readable
stream and an exact declared byte count within `maxObjectBytes`. Each chunk must
be a `Uint8Array` no larger than `chunkBytes`. Excess bytes, oversized chunks,
invalid chunks and early EOF fail. Writes consume one chunk at a time, finish
its disk writes before requesting another, and handle partial filesystem writes.
Reads allocate at most one `chunkBytes` buffer per pull, with a zero high-water
mark; there is no object-wide buffering or speculative read queue. Source
queues and buffers retained by consumers are caller-owned: the adapter cannot
undo an oversized allocation or unbounded source prefetch performed outside it.
Use zero high-water-mark producers for strict demand-driven production. Bounds
are per transfer, not a global concurrency or memory budget.

Read and write options must be non-null, non-array objects. A supplied signal
must be a native `AbortSignal` with its boolean aborted state and callable event
listener methods; signal-shaped records, null and malformed values fail with
`INVALID_INPUT` before a reader lock or file handle is acquired. Omitted read
options remain supported; write options and their exact size are required.
Reader acquisition and abort-listener registration are inside write cleanup,
including failures during listener registration. Cleanup uses native listener
removal so a caller override cannot prevent resource release.

Abort signals stop transfer and clean up file handles/staging. Cancelling a read
stream also closes its handle. Consumers must drain or cancel returned streams;
otherwise an idle stream retains an open handle. A write abort cancels the source
reader, including a pending read; source cancellation callbacks are not awaited
and must release their own resources. Source exceptions fail the write and do
not restart it. Callers should supply deadline-driven abort signals for stalled
producers or indefinitely repeated empty chunks. Each empty chunk yields to the
event loop so deadline timers can run even for continuously ready producers.
Kernel I/O is not forcibly
interruptible; cancellation is checked around bounded I/O and before finalization.

`requireCapability` supports `read`, `range`, and `create` only. Requests for
multipart, overwrite, delete, catalog publication or other capabilities throw
`UNSUPPORTED_CAPABILITY`. Stable errors also include `INVALID_INPUT`,
`UNSAFE_PATH`, `NOT_FOUND`, `ALREADY_EXISTS`, `LIMIT_EXCEEDED`, `CANCELLED` and
`IO_ERROR`; filesystem/source messages and physical paths are not included.

## Finalization, isolation and recovery limits

A write stages privately, checks the complete size, fsyncs and closes the file,
then uses a same-filesystem hard link to create the final name atomically without
replacement. An existing object, symlink or competing winner produces
`ALREADY_EXISTS`; its bytes are never overwritten. Handled failures before
finalization unlink staging and never create a partial final object. The staging
link is removed after finalization, on both success and failure paths.

Final-name creation is the linearization point. A cancellation racing after
that point may lose to a completed write. An exceptional staging-unlink failure
can report an error after a complete object became visible; it is not evidence
that finalization rolled back, and a retry must not overwrite the object. A host
crash may leave staging or an unacknowledged complete final object. No automated
orphan deletion is provided. Directory entries are not fsynced, so this slice
makes no crash-durability or backup/recovery guarantee. File fsync alone is not
production durability conformance.

Directory chains are checked with lstat and realpath at initialization and I/O;
symlink components are rejected. Final read opens use `O_NOFOLLOW` and
`O_NONBLOCK`, then require a regular single-link file. Existing final symlinks
cannot be followed by finalization. Hardlinked files are rejected on read as an
additional defense. There is a short window between final link creation and
staging unlink when such a read can fail `UNSAFE_PATH`; callers should read only
after successful write acknowledgment.

Linux on a local filesystem supporting hard links and these open flags is the
supported platform. This implementation does not use openat2 or a directory-fd
sandbox. Directory checks are not atomic with later path operations: hostile
concurrent renames, mounts, hardlinks, or symlink swaps by a host user with
namespace-write access are outside the guarantee. Root and ancestors must stay
under trusted control throughout each operation. An administrator or same-host
process able to mutate those paths or file contents is trusted; this module
does not isolate arbitrary code sharing the service identity. It provides no
network-filesystem, Windows, mutually untrusted tenant, current-policy stream
revocation, or administrator-resistance claim.

Local finalization does not move catalog heads, publish an Iceberg snapshot,
commit checkpoints/lineage, grant end-user access, or coordinate transactions.
Future integration must retain C04's single PostgreSQL authority and current
policy/fencing checks. A finalized object is merely a candidate for that path.

Existing absolute locations must be resolved through an explicit operator-owned
binding and reviewed inventory/mapping when integration is designed. A caller's
absolute path must never be treated as a grant or auto-converted into a trusted
binding. Existing Iceberg references, retained snapshots and backup pins must
remain valid. This module does not adopt, move, delete or migrate an existing
warehouse. Such work requires a separate bounded migration/integration design
and review of publication, retention, authorization and recovery invariants.

## Verification and progress

Synthetic tests use temporary directories, deterministic stream errors and
filesystem mutations, bounded test timeouts, and recursive cleanup. Existing
`tests/*.test.ts` discovery includes both files without script changes.

Coverage includes strict location/binding validation; real symlink and hardlink
escapes; multi-chunk and empty transfers; exact ranges and malformed limits;
failed/short/oversized/malformed sources; cancellation before and during I/O;
private staging permissions and cleanup; concurrent no-clobber finalization;
read cancellation, injected truncation, and per-chunk producer demand checked
against real file sizes. These are local filesystem conformance tests, not
mock-only isolation, transactional publication, production storage conformance,
or capacity evidence.

Verification environment: October 25, 2026, Linux non-root UID 10001, Node
24.21.0, pnpm 11.25.0, repository `.venv` Python 3.11.17. Synthetic identities
are `warehouse`, `table`, and `object`; there is no workload calibration claim.

Initial implementation verification (before review repairs):

- `pnpm format`: passed; existing tracked files remained unchanged.
- `pnpm typecheck`: passed, including the final full gate.
- `pnpm exec tsx --test tests/storage-locations.test.ts tests/storage-streaming.test.ts`:
  passed on the final implementation: 8 tests, 8 passed, no failures or skips.
- `pnpm check`: passed on the final implementation (exit 0): 279 tests, 279
  passed, zero failures, zero skips; test duration 66.681 seconds. Real embedded
  PostgreSQL, Iceberg publication/history, competing writers, process-crash
  recovery and current-authority tests ran successfully. Formatting, SDK drift,
  TypeScript, Python formatting/schema/workload checks, both application builds,
  and Station Explorer type/build/hosting verification also passed. No required
  infrastructure was unavailable. Local command logs are
  `/tmp/issue-32-format.log` and `/tmp/issue-32-check-final.log`; these temporary
  logs are not repository artifacts. Independent Linux CI on the eventual
  published revision and human review remain controller-owned delivery gates.

Review repair on October 25, 2026 stays within the same exclusive file scope.
Both findings were reproduced with new regressions against the original adapter:
the continuously ready empty-chunk source required the parent process's
five-second SIGKILL timeout, and a null range incorrectly returned a stream.
The adapter now yields on each empty chunk and validates supplied range values
before filesystem access. The empty-chunk regression runs in a child process
with a parent-enforced timeout, checks a 10 ms abort timer, source cancellation
and reader unlock, then verifies empty staging and final-object directories.
Malformed-range cases cover null, booleans, numbers, strings, arrays and
incomplete records, including validation with the object directory removed.

- `pnpm exec tsx --test tests/storage-locations.test.ts tests/storage-streaming.test.ts`:
  passed after repairs: 10 tests, zero failures or skips.
- `pnpm format` and `git diff --check`: passed; no files outside the exclusive
  scope changed.
- `pnpm check`: passed after repairs (exit 0): 281 tests, 281 passed, zero
  failures or skips; test duration 68.878 seconds. Real PostgreSQL and Iceberg
  tests ran, including competing publication, crash recovery and current
  authority checks. Formatting, SDK drift, TypeScript, Python checks, both app
  builds and starter verification passed. No required infrastructure was
  unavailable. Logs: `/tmp/issue-32-repair-format.log` and
  `/tmp/issue-32-repair-check.log` (temporary local evidence, not repository
  artifacts). Changes remain uncommitted for controller-owned publication and
  independent review.

S3-compatible streaming/multipart, durability and credential-revocation
conformance; engine/runtime integration; live authority checks; catalog
publication; coordinated retention/recovery; and independent review remain next
steps under #17. No production storage or capacity claim follows from this slice.

### Transfer-option review repair

The reported write path acquired a reader and registered its listener before
entering cleanup. A malformed signal could therefore leave the source locked.
The new malformed-options regression also reproduced a raw TypeError for null
write options before the fix. Shared read/write validation now rejects malformed
options and signals with `INVALID_INPUT` before transfer resources are acquired.
Write reader acquisition and listener registration are covered by cleanup.
Regressions cover null, primitive and array options, missing write options,
null/signature-only/prototype-only signals, source unlock, empty staging and
object directories, and deterministic failure after listener registration.

The focused storage suite passed all 12 tests with no failures or skips. The
first full gate caught a missing rest-parameter type annotation in the new fault
injection test; it was corrected without changing checks.

Final verification on October 25, 2026:

- `pnpm format` and `git diff --check`: passed; changes remain within the
  exclusive scope.
- `pnpm check`: passed (exit 0), 283 tests passed, zero failures or skips,
  runtime test duration 73.865 seconds. Real PostgreSQL and Iceberg tests ran;
  formatting, SDK drift, TypeScript, Python checks, both application builds
  and starter verification passed. No required infrastructure was unavailable.
- Temporary local logs: `/tmp/issue-32-options-format.log` and
  `/tmp/issue-32-options-check.log`. Changes remain uncommitted; independent
  CI and human review remain controller-owned delivery gates.

This repair changes no runtime integration or production storage guarantees.
