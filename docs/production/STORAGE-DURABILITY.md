# Local storage durability and restart semantics

Issue #40 (M2-01b), dependent on #32/#38, is a bounded local adapter slice of
#17. It follows C04/C08, S06–S08 and R01–R02 in the accepted
[core](../spec/core-contracts.md), [security](../spec/security-contracts.md) and
[runtime](../spec/runtime-contracts.md) contracts, and P01/P02/P06 in
[production acceptance](ACCEPTANCE.md). Iceberg owns the dataset table format;
restricted views remain terminal. This does not publish catalog heads or make
file operations a transaction coordinator.

The identity, binding, bounded streaming, range, isolation and capability
contracts in [storage locations](STORAGE-LOCATIONS.md) remain in force. This
new document supersedes that slice's statement that directories are not fsynced.
Only storage source, new durability tests/fixtures and this document change.
Progress and deviations are recorded here because the exclusive issue scope
excludes the shared `docs/IMPLEMENTATION.md`. No protected paths, dependencies,
scripts, schemas, runtime integration or delivery policy change.

## Supported assumptions and acknowledgment

The supported target is Linux with a local filesystem providing same-filesystem
atomic no-replacement hard links, `O_DIRECTORY`, `O_NOFOLLOW`, and effective
file and directory `fsync`. The filesystem, mount configuration and underlying
storage must honor these operations and flush requests. Network filesystems,
Windows and providers with weaker or unsupported semantics are outside this
contract. A successful syscall on an arbitrary filesystem is not certification
of its crash or power-loss behavior.

The host must supply a preexisting, durably provisioned absolute root, including
its ancestor entries, protected from hostile namespace mutation. Root and
ancestors, ownership, permissions and device configuration remain trusted host
responsibilities. No openat2 sandbox or isolation from a host principal able to
mutate these paths is claimed. Nested mounts or moving/replacing binding
directories while the adapter is in use are unsupported.

Initialization validates the directory chain and creates missing binding
directories with the existing permissions. It synchronizes `objects`, `staging`,
the table directory, the warehouse directory and the root, in that order, closing
each handle. Existing entries are synchronized too: existence does not prove
that a prior initializer reached its durability barriers. This persists newly
created binding entries up to the supplied root. Any directory open, sync or
close failure rejects initialization through the stable storage errors;
unsupported directory synchronization, such as EINVAL, yields `IO_ERROR`.
There is no silent fallback to file-only synchronization.

Successful `write()` acknowledgment is resolution of its returned promise,
after all of the following have completed:

1. Bounded streaming validates the exact declared size in a private staging file.
2. The complete file is fsynced and its handle is closed.
3. A same-filesystem hard link creates the final name without replacing any entry.
4. The objects directory is fsynced and its handle is closed.
5. This operation's staging link is unlinked.
6. The staging directory is fsynced and its handle is closed; transfer cleanup
   releases the source reader and abort listener.

Final-name creation remains the linearization point, preceding acknowledgment.
The final name can never expose a partially streamed file. Both persistence of
the final name and persistence of removal of its staging alias are necessary:
the reader deliberately requires a regular single-link file. No operation
returns success when a required barrier fails. File and directory handles are
closed on success and failure; a close failure is reported, and a remaining
write-file handle receives a cleanup close attempt. Kernel I/O is not forcibly
interruptible and a failing filesystem cannot be made reliable by this adapter.

## Failure, cancellation and reconciliation

Stable error codes and path-free messages are preserved. `StorageError` has an
optional `writeOutcome: 'uncertain'` marker for handled failures after this
operation successfully created the final name. The code still describes the
failure, for example `IO_ERROR`; the marker distinguishes it from failure before
finalization. It does not assert catalog publication or durable completion.

| Boundary | Observable result and obligation |
| --- | --- |
| Before final-name creation | Validation, stream, file-sync, cancellation or link failures reject. Cleanup attempts to remove only this operation's staging file. No final object is created by that failed operation. A competing writer may independently own that name. |
| Existing final name | `ALREADY_EXISTS`; no replacement, even for identical bytes or a previous uncertain attempt. This is not an idempotent success receipt. |
| After final-name creation, before acknowledgment | A handled sync/cleanup error has `writeOutcome: 'uncertain'`. Complete bytes may be visible, but durability/readability is not acknowledged. The final name is never rolled back. |
| Cancellation | Cancellation checked before linking prevents finalization. Once linking is issued, cancellation may lose to completion. The adapter finishes durability barriers and cleanup; a later barrier failure remains uncertain, not a claim that cancellation rolled back the object. |
| Process termination or lost response | No marker or receipt may reach the caller. Even a missing acknowledgment can follow complete durable creation; callers must reconcile rather than infer failure. |
| After acknowledgment | Under the supported assumptions, content, final name and single-link reader state have passed the required persistence barriers. No general media-failure, backup or provider guarantee follows. |

For uncertainty, preserve the logical location and expected size/content identity.
Use the same trusted binding and authorized read path to compare the complete
object with the expected bytes or independently retained digest. A readable
matching object establishes content identity, not proof that a failed durability
barrier later succeeded, nor catalog publication. The API offers no durability
repair/receipt operation. An absent name, a mismatch, or `UNSAFE_PATH` requires
caller/operator reconciliation with ownership and publication records outside
this slice. Do not interpret `ALREADY_EXISTS` as proof of the desired contents,
blindly retry, overwrite, or delete the final object. Uncertain candidates must
not be published merely because a read succeeded.

Cleanup is bounded to the unique staging path created by the current operation.
A successful-path unlink failure gets one additional cleanup attempt; there is
no retry loop or directory sweep. Cleanup failure does not mask an earlier
operation error, and any post-finalization error retains the uncertainty marker.
Pre-finalization cleanup is best effort and is not separately fsynced; crash
recovery can therefore retain private staging debris. It cannot turn debris into
a partial final object.

## Killed writers and the fail-closed limitation

A writer killed before linking can leave a private staging file. A writer killed
between final-name creation and staging unlink can leave two hard links to the
complete bytes. Initialization neither scans nor deletes these files. Reads of
the two-link final object continue to fail with `UNSAFE_PATH` across restarts;
a retry at the same logical identity continues to fail with `ALREADY_EXISTS`.
This is an explicit availability limitation, not automatic recovery. The same
rule rejects an unknown hardlink introduced outside the adapter. There is no
safe general inference here that another link is abandoned rather than live.

A kill after unlink but before acknowledgment can leave a complete readable
object after process restart. A real machine crash before the staging-directory
barrier could still resurrect the staging alias; no acknowledgment was issued.
An acknowledged object has passed both directory barriers, subject to the
filesystem and hardware assumptions above.

Operators must preserve the affected entries and use a separately reviewed
reconciliation procedure that establishes writer quiescence, binding identity,
content identity and catalog/retention ownership before any repair. This slice
provides no destructive operator procedure, automatic orphan garbage collection,
retention engine, catalog-aware cleanup or repair endpoint. Unknown staging
objects and abandoned files are deliberately preserved.

## Verification and progress

Synthetic fixtures instrument filesystem calls only inside isolated subprocesses;
there are no production fault hooks or environment flags. Successful operations
use real temporary files, links and fsync calls. Faults inject EIO and unsupported
EINVAL directory sync errors, file-sync errors, link/unlink errors and close
errors after real handle closure. Tests track every acquired handle and source
lock, assert barrier ordering, and verify bounded cleanup and uncertain errors.
Cancellation is injected immediately before finalization and during the
post-link barriers. Existing streaming, identity, concurrent-writer and path
isolation tests are preserved.

The parent kills writer subprocesses with SIGKILL at deterministic IPC barriers:
after the first chunk, before linking, after linking, after objects-directory
fsync, after staging unlink, after the final directory close but before promise
resolution, and after the caller receives acknowledgment. A separate surviving
process constructs a fresh adapter and checks absent or complete final bytes,
no-clobber retries, unchanged logical binding, bounded range reads, retained
unknown/stale staging files and fail-closed two-link reads. Timeouts bound each
child; fixtures use synthetic `warehouse/table/object` identities only.

These are process-crash and filesystem API conformance tests. SIGKILL leaves the
kernel and page cache running. They do not simulate sudden power removal,
filesystem journal replay, drive cache loss, or cloud/provider faults, and they
do not certify ext4, XFS, container backing storage or a particular mount setup.
No production data, live service, paid model API or capacity claim is involved.

Verification commands and final results are recorded below after the full gate.
Independent review of the persistence/recovery invariants, exact-head Linux CI
and current-main integration are controller-owned prerequisites before manual
merge. Local checks do not substitute for those gates. No commit, push, PR,
merge or deployment is performed in this task.

Remaining #17 work includes S3-compatible durability and multipart behavior,
runtime/catalog integration with atomic publication, credential revocation,
location transitions, coordinated retention/recovery and provider certification.
#17 remains open.

### Executed local results

Verification on October 25, 2026 used Linux, non-root UID 10001, Node 24.21.0,
pnpm 11.25.0 and repository `.venv` Python 3.11.17. The checkout baseline is
`3b8e72fc398b93ec7a24f1ed377156b90432ff58`; evidence covers the uncommitted
issue-40 changes atop that revision. Synthetic temporary directories were on
container overlayfs, not a power-loss-certified filesystem configuration.

- `pnpm exec tsx --test tests/storage-durability.test.ts tests/storage-locations.test.ts tests/storage-streaming.test.ts`:
  passed, 41 tests, zero failures/skips; 4.303 seconds.
- `pnpm typecheck`: passed.
- `pnpm format` and `git diff --check`: passed; only the exclusive file scope
  changed.
- `pnpm check`: passed, exit 0; 314 tests, 314 passed, zero failures/skips;
  runtime test duration 112.208 seconds. Real PostgreSQL and Iceberg tests ran,
  including concurrent publication, crash recovery and restart behavior.
  Formatting, SDK drift, TypeScript, Python formatting/schema/workload checks,
  both application builds and Station Explorer checks passed.
- No required local infrastructure was unavailable. Temporary local logs are
  `/tmp/issue-40-storage.log`, `/tmp/issue-40-typecheck.log`,
  `/tmp/issue-40-format.log` and `/tmp/issue-40-check.log`; they are not repository
  artifacts. Independent review, exact-head Linux CI and current-main integration
  have not been performed here and remain pending before manual merge.

All changes remain uncommitted for controller-owned publication. No parent issue
closure or production durability certification is claimed.
