# Native storage and private closure contract v1

The native implementation is `services/runtime/src/storage.rs`. It is used by
the existing indexing FileIO bounded-read path and native V2 snapshot indexing.
POSIX/S3 transfer and private closure verification are libraries for trusted native
components; this slice does not expose an arbitrary file/object HTTP API.

## Identity and authority

A `LogicalObject` is exactly `{version: 1, warehouseId, tableId, objectId}`.
Identifiers contain 1–128 ASCII letters, digits, underscores or hyphens, begin
with a letter/digit, and contain no path separators. Unknown JSON fields and
unknown versions are rejected. Logical references carry no endpoint, physical
path, credentials or implied permission.

Only trusted operator composition provisions a `PosixStore` or `S3ReadStore`.
Each store fixes one warehouse/table namespace and finite object/chunk limits.
Caller-supplied logical references cannot change that binding. The POSIX layout
matches existing storage adapters:
`root/warehouseId/tableId/{objects,staging}`. The root must already exist.

`ReadAuthority.check_read(object)` and
`WriteAuthority.check_write(object)` are mandatory injected asynchronous guards.
There is no permissive default. Every guard call has a 30-second timeout and
observes the transfer cancellation token before and after it completes.

The existing indexing `io::Scope` grants only its internal sentinel read
reference. It proves the current build/claim, source read grant and target index
grant; it does **not** implement a storage writer.

Native namespace grants additionally bind the real claim to an enabled row in
`native_storage_namespaces`, with exact warehouse, table and captured revision:

| Grant                         | Required namespace purpose/resource                                      | Permitted use                                                       |
| ----------------------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------- |
| `IndexNamespaceReadAuthority` | `dataset-source` / build source                                          | Trusted whole-Dataset source reads                                  |
| `IndexNamespaceReadAuthority` | `index-artifact` / build target                                          | Internal validation of private index output                         |
| `IndexArtifactWriteAuthority` | `index-artifact` / build target                                          | Private index artifact creation                                     |
| `catalog::DatasetIoGrant`     | Declared Dataset input/output, verified session and live native producer | Whole-Dataset reads; output writes require explicit read plus write |

These guards use the existing PostgreSQL `guard_claim` and current permission,
session, epoch and incarnation checks. A source read grant never permits source
data mutation. Dataset publication uses the distinct [catalog contract](catalog.md).
An application or
untrusted function cannot manufacture these grants from request JSON. Operators
must provision the physical binding for the same logical namespace and purpose;
the namespace table is not a credential store.

Namespace policy revisions have immutable history. Policy content changes and
re-enabling a disabled namespace require a fresh revision. A previous revision
cannot be reused after replacement, disabling or deleting the current binding.
Disabling a current namespace may keep its revision; reads still check enabled
state. Namespace markings must be retained by an indexing definition's marking
set and the current principal.
Private indexing artifact writes additionally require the build's complete
marking set to be retained by the destination namespace; this initial artifact
guard therefore requires exact classification rather than permitting a downgrade.

## Transfer operations

| Native operation                                   | Result and boundary                                                                                                                                                    |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PosixStore.read_range` / `S3ReadStore.read_range` | Exact half-open byte range; at most 16 MiB; current read guard before/after bounded transfer                                                                           |
| `read`                                             | Backpressured stream of at most the provisioned chunk size; reauthorization on each next batch and EOF                                                                 |
| `PosixStore.begin_write`                           | Private, exclusively created staging file with expected byte count and SHA-256                                                                                         |
| `PrivateWrite.append`                              | At most one provisioned chunk, with current write guard before/after I/O                                                                                               |
| `PrivateWrite.seal`                                | Exact size/hash, file durability, immutable no-replace finalization and directory durability before successful acknowledgment                                          |
| `S3ReadStore.begin_write` / `PrivateS3Write`       | Hidden owned multipart attempt, bounded append, explicit abort and conditional immutable completion                                                                    |
| `ObjectWriter.recover_object`                      | Provider-independent recovery of a sealed candidate after a lost reply; exact expected size/hash, pinned versions and both read/write guards on every bounded transfer |

Recovery returns `None` only for an authorized absent object. POSIX confinement
failures, changed bytes/versions, missing S3 buckets, provider failures and revoked
authority remain errors. S3 absence also checks that its trusted bucket exists;
credentials unable to establish that fact fail closed. Recovery never retries
multipart completion, overwrites a sealed object or deletes a provider prefix.
The caller still performs its own fenced metadata CAS before making the recovered
reference visible.

Chunks are at most 1 MiB. Object limits are explicit, finite and at most the
existing JSON-safe integer bound; no whole-object buffer is required. Zero-byte
objects remain supported. An invalid range, excess chunk, changed provider
version, short read, integrity failure or revoked grant fails the operation.

Linux `openat2` directory descriptors reject escapes, symlinks and magic links.
Reads also reject nonregular or multiply linked objects. Private files are
created exclusively, verified and synchronized; finalization uses Linux
`RENAME_NOREPLACE`. Ordinary operations cannot overwrite a final object. Partial,
cancelled and integrity-failed writes never produce a final object. Staging-file
cleanup uses only service-created names under the captured directory descriptor.

Cancellation interrupts authority waits and asynchronous transfer waits. A host
filesystem call already running in the blocking pool may finish privately; its
result cannot pass the subsequent cancellation/authority check. Provision finite
process, blocking-I/O, memory and filesystem quotas separately.

Once finalization has happened, a lost durability/authorization acknowledgment
can return `WRITE_OUTCOME_UNCERTAIN`. The immutable object can remain as an
orphan, but no dataset/object-generation pointer is published by this library.
Do not delete an uncertain final object or infer catalog success from its mere
existence. Catalog publication must validate the authorized sealed descriptor
and retain its own transactional idempotency/receipt contract.

## Provisioned S3 transfer

`S3Binding` fixes endpoint, bucket, prefix, region and explicit environment
secret-reference names. It never accepts credentials from a work record.
The AWS SigV4 library signs requests using only those provisioned credentials.
Ambient AWS profiles, metadata credentials, endpoint variables and HTTP proxy
variables are not used. Redirects are disabled. HTTPS uses certificate/hostname
validation; HTTP is accepted only for loopback fixtures.

Physical keys are derived exclusively as
`prefix/warehouseId/tableId/objects/objectId`. The adapter checks HEAD size/ETag,
sends exact Range plus If-Match, requires HTTP 206 and matching Content-Range,
ETag and body length, and denies malformed/oversized responses. A stream pins its
initial ETag and rejects a provider version change between batches.

`PrivateS3Write` signs each payload hash, buffers fixed 8 MiB parts and admits at
most 10,000 parts. Caller chunks remain at most 1 MiB, including chunks crossing
part boundaries. Before Complete, exact full byte count and SHA-256 must match.
Complete uses `If-None-Match: *`; an existing final object is never overwritten.
Zero-byte objects use a conditional PUT. Complete response XML is bounded to
64 KiB, forbids DTDs and requires a success root/ETag: an embedded error in HTTP
200 is not success. Any failed part poisons its attempt. Explicit abort cleans
only the owned upload ID; drop cleanup is best effort. SIGKILL can leave an
unreachable multipart attempt, requiring provisioned incomplete-upload lifecycle
cleanup. No prefix deletion is permitted.

An uncertain Complete is never blindly retried. `recover_write` requires fresh
read and write authority and reads the entire exact immutable object to compare
actual bytes and SHA-256. ETag or provider metadata alone is not content proof.
Recovery yields only a sealed descriptor; Dataset visibility still requires the
catalog's guarded atomic publication.

Native V2 indexing resolves only the trusted `MORIA_NATIVE_STORAGE_BINDINGS`
operator array. Each entry has `namespace` (warehouseId/tableId/revision) and a
`provider`: `{kind:"posix",root,limits}` or `{kind:"s3",binding:S3Binding}`.
Duplicate/ambiguous namespaces and stale revisions fail. Separate required
operator configuration provisions `MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT` and finite
`MORIA_NATIVE_CLOSURE_LIMITS`. Head/caller JSON contains no provider locations.

## Durable paged closure integrity

`ClosureSpool.create` fixes a private logical root, finite entry/reference-byte
limits and a main SQLite-file disk limit. Each `append_page` admits at most 256
entries and 256 KiB. An entry contains a canonical namespace-scoped logical name
and a sealed-object descriptor (logical reference, size, SHA-256). SQLite's unique
name index rejects duplicates across pages atomically. A rejected page cannot
advance stored counts.

SQLite is **private spill and recovery state, never publication authority**.
It uses a service-owned private directory, FULL synchronous DELETE journaling,
a 2 MiB page cache and file-backed temporary state. Provision additional journal
headroom up to the main-file limit. The database and enclosing directory are
durable before acknowledgment. The immutable grant/attempt key is persisted;
reopen requires the same current claim. A replacement owner/fence creates a new
spool and cannot share an old attempt's mutable SQLite state.

`seal` walks the name index in bounded 256-entry pages, reads each referenced
object through the injected current read authority, verifies its complete size
and SHA-256, and computes a domain-separated, canonical closure digest. It checks
the current write authority and atomically persists counts plus the sealed root
receipt. Sealed roots reject append, replay the same receipt and expose bounded
pages only after a current read check. A crash after a committed page can reopen
that exact state; successful sealing cannot infer missing objects from file
discovery.

`snapshot::verify_snapshot` additionally verifies exact whole Iceberg v2 closure
membership, immutable table/snapshot/schema identities and all retained metadata,
Avro manifests and physical Parquet data, including retired/deleted data files.
It supports the explicit unpartitioned copy-on-write flat C03 reader profile;
unsupported partition/sort/delete/encryption/schema features fail closed. Native
Avro, metadata, row, row-group/range and decoded-batch budgets remain enforced.
A generic integrity-only sealed closure is not semantic attestation. Existing
legacy scoped catalog caps remain enforced; native V2 uses its separate paged
format and complete native reader contract.

## Durable native reader attestations

`attestation::table` keys a durable fenced verifier by exact immutable root,
table/snapshot, trusted provider binding fingerprint, incarnation and native
reader implementation. Full closure verification runs once per key outside the
environment barrier. Actual size/hash and immutable physical versions are
recorded in bounded private SQLite state, then exported in bounded pages to
indexed private authority tables. A short shared environment/filesystem barrier
and native claim check publish the ready attestation. Stale verifier fences
cannot export or publish; ready attestations are immutable.

Native file tasks lazily resolve only used logical names through indexed
approved-entry lookups. They do not re-read every inventory/data object per task.
Every bounded transfer still rechecks the current source authority. POSIX ranges
use the same held descriptor and check device/inode/size/mtime/ctime before and
after reads; S3 ranges use the recorded exact ETag/size and conditional requests.
Changed physical objects fail even after attestation is ready. Permission,
session, namespace, security epoch and producer fence are never cached.

## Verification and remaining scope

`tests/native-storage.integration.test.ts` is mandatory in `pnpm test`. It
provisions real PostgreSQL namespace/claim state and the pinned MinIO server,
then executes `/usr/local/bin/moria-storage-tests` (or
`MORIA_STORAGE_TEST_BINARY`) with:

```sh
--ignored --exact provider_and_authority --nocapture --test-threads=1
```

The dedicated integration harness requires its fixture and is deliberately
separate from ordinary library tests; the Node wrapper fails on a missing binary,
failed native execution or anything other than one executed passing case. The
test Dockerfile compiles it from `tests/fixtures/native-storage/harness.rs`.
Credentials are fixture-scoped environment references, not logged or stored.

Cases include a streamed 70 MiB POSIX object, a closure beyond the old 128-entry
and 256 KiB inventory bounds, exact real S3 ranges, immutable write rejection,
size/hash failures, cancellation, path/hardlink denial, killed-process spool
recovery, duplicate-page rollback, provider mutation, namespace revision/disable,
lease takeover, actual multipart writes/abort/uncertain recovery, full retained
Iceberg semantics and in-progress read/write epoch revocation. Separate mandatory
native catalog and V2 indexing wrappers cover direct S3 Dataset publication,
read-only native scans, concurrent tasks sharing one verification, namespace
revision nonreuse and corruption/revocation after ready attestation. They are capability
evidence, not throughput, multi-host, retention/restore or petabyte certification.

Garbage collection/retention/restore coordination, verifier attempt cleanup
qualification, an externally exposed storage RPC, multi-host capacity
and the 1m/10m mixed-load profiles remain open. Functional engine/authority tests
do not certify those scale and recovery obligations.

Recorded local proof, executable/schema digests, exact commands and retained
failed trials are in the [data-plane evidence ledger](../evidence/2026-10-30/native-data-plane/README.md).

## Physical removal gate

Immutable object creation authority does not authorize deletion. A collector must obtain a distinct exact-object removal capability bound to its durable claim, object version, fence, current incarnation, retention exclusion, and tombstoned producer state. Dataset producer and filesystem upload grants must never acquire removal through their existing write interface.

The pinned MinIO qualification failed conditional `DELETE`: an actual wrong `If-Match` header returned 204 and removed the object. Evidence is retained in `../evidence/2026-10-30/native-storage/delete-qualification-001`. ETag-only S3 deletion remains disabled. Safe physical collection requires qualified immutable provider version IDs or a provider that passes real conditional-delete tests. POSIX removal also remains gated until a held-version/private-quarantine protocol is implemented and tested; stat followed by unlink is not an atomic version comparison. Prefix deletion is excluded.

The native S3 provider now records bounded opaque `s3v1/` version tokens containing ETag and provider version ID, pins version-qualified range requests, and checks the returned identity. `ObjectWriter::remove_object` requires a separate `RemoveAuthority`; no producer write guard implements it. S3 removal accepts only non-null version IDs, verifies enabled bucket versioning, reads/deletes the exact version, and verifies its absence. A newer version is retained. Unqualified or null versions return `CONDITIONAL_DELETE_UNSUPPORTED`. Actual versioned MinIO and all existing storage regressions passed 29 tests with zero skips; evidence is in `../evidence/2026-10-30/native-storage/versioned-provider-green001`. The durable GC claim, retention exclusions, uncertain late completion reconciliation, and POSIX removal remain separate gates.

## Qualified Parquet allocation profile

New native verifier receipts identify `iceberg-v2-cow-flat-bounded-v2`; the earlier flat-v1 receipt is not silently promoted. Candidate preparation and durable reader attestations require the current profile, so a pending old-profile candidate must be reverified. Full rolling-upgrade/history migration remains an operational gate.

Before the Parquet/Arrow decoder runs, the verifier checks raw compact footer container counts and schema depth, embedded Arrow IPC schema shape, column metadata, and every compressed page header. Footer bytes are capped at 8 MiB; schema nodes at 1024 with supported flat/list depth; row groups at 4096; columns at 256; row-group rows and leaf values per column at one million. Each compressed column and each decoded page is at most 16 MiB. Expanded buffers, including offset/value overhead, must fit a conservative 64 MiB per-row-group estimate. Actual decoded batches retain their independent existing checks.

Variable-width dictionary expansion is bounded using the conservative product of whole-column uncompressed bytes and leaf value count, summed with other leaf-buffer estimates. This may reject a safe externally produced file rather than infer safety from compressed size. Variable-width delta-byte-array encoding is outside the qualified profile. The native Dataset writer emits non-dictionary Parquet, preserving bounded wide-row outputs without dictionary amplification. Plain and small dictionary external files remain accepted when their explicit estimates fit.

These bounds apply to the qualified native snapshot verifier; they are not a claim to accept arbitrary Parquet encodings, nested schemas, or reader implementations. The raw preflight performs no allocation from encoded page or collection claims. Full native writer/indexing/history regression and population throughput evidence must accompany this profile before broad rollout.
