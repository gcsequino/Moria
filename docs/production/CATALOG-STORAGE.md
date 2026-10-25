# Published catalog metadata reads and publication

Issue #46 (M2-01f), dependent on #44, is a bounded trusted-service integration
under #17. Contracts C01/C04/C08 and S01–S03/S06–S08 apply. This does not
complete #17 or introduce full Iceberg file IO.

`CatalogStorage.read(authenticatedSession, dataset, signal?)` accepts a session
from the existing authentication boundary and an exact Dataset resource identity.
It returns environment, Dataset, pinned revision, snapshot ID, producing execution
and a bounded metadata byte stream. It accepts no physical path, URL, bucket,
object ID, snapshot selection, credentials or caller-provided policy. There is no
public route or generated SDK change. SessionContext remains a trusted internal
type; constructing that object from request JSON would violate its existing
contract. A session hash is mandatory on this path.

Trusted composition supplies exact environment/Dataset bindings, kind `Dataset`,
complete effective namespace/resource/inherited markings, warehouse/table identity,
and a factory composing `createAuthorizedLocalStorage` or
`createAuthorizedS3Storage` with the supplied scope and authority. The factory
returns only `StreamingStorage`; provider settings and SDK types stay in service
composition. No wildcard/fallback resolver or ambient credential lookup exists.
Unsupported identities and RestrictedViews fail before factory invocation.
Bindings are copied and frozen, including their marking arrays. As with existing
ObjectService bindings, trusted deployment must resolve resource kind and complete
markings correctly; this slice does not add a resource registry or a second policy
engine. Changes to resource bindings require replacement of the service binding
and the existing security-epoch invalidation discipline. An old binding must not
be retained as a way around a security tightening.

The existing `Authority.current`, `authorize` and `Authority.assertCurrent` check
principal, application scopes, grants, marking clearance, environment, identity
freshness and session expiry/revocation. A shared environment transaction performs
session/epoch validation and resolves the published head joined to matching
snapshot_history by environment, Dataset, revision, snapshot ID and metadata
location. Both records come from one MVCC statement in a repeatable-read snapshot.
Unpublished candidates, revision-zero heads and inconsistent history fail closed.
No provider initialization or object IO runs in that SQL transaction. Publication,
lease fencing and atomicity are unchanged.

The resulting storage authority scope is read-only and contains exactly the
catalog-resolved logical object. Existing adapters perform authority checks before
access and on stream demand, including after IO before byte release. Each check
uses a fresh PostgreSQL transaction and the original authorization epoch: any
policy epoch change invalidates the stream, even if unrelated to this Dataset.
Session expiration is checked against database time, independently of epoch
changes. Publication alone does not change the security epoch or retarget a stream.
No SQL transaction remains open while a consumer waits for bytes. Cancellation
and denial use existing adapter cleanup; previously released bytes cannot be
recalled. The release check is the authorization linearization point, not an
atomic fence spanning PostgreSQL and the provider.

## Versioned metadata_location encoding

The additive representation is the literal prefix `moria-storage:v1:` followed by
canonical JSON emitted by `encodeMetadataLocation`:

```text
moria-storage:v1:{"version":1,"warehouseId":"warehouse","tableId":"table","objectId":"metadata"}
```

Field order is exactly as shown. Identifiers follow StorageLocation's existing
1–128 character ASCII token grammar. The decoder accepts at most 512 characters
and requires byte-for-byte equality with re-encoding after strict location
validation. Unknown versions, extra/duplicate fields, whitespace variations,
alternate escaping, malformed JSON and invalid tokens fail with `INVALID_INPUT`.
Wrong warehouse/table bindings fail with `BINDING_MISMATCH`. Neither case invokes
the storage factory. Missing published objects fail with `NOT_FOUND`; there is no
fallback to an older snapshot or to the physical-path reader.

Existing physical-path metadata heads are explicitly unsupported by this new API.
They remain unchanged and supported by their existing consumers. Publishing
logical references is limited to the demonstrated metadata-reader integration:
existing Iceberg workers/indexers cannot yet consume these references. No current
production/runtime writer has been switched to this encoding. The stream contains
standard Iceberg metadata, whose manifest/data-file references still require the
existing physical IO machinery; logical metadata reads do not relocate or rewrite
a warehouse. The reader pins catalog provenance and transports bytes; it does not
parse/validate Iceberg metadata or implement manifest/data-file resolution.

## Verification and remaining gates

`tests/catalog-storage.integration.test.ts` creates isolated PostgreSQL databases,
real local files and the checksum-pinned loopback MinIO fixture from #44. Synthetic
workload sessions are minted/authenticated through the real WorkloadSessions and
Authority APIs. The existing worker generates standard Iceberg v2 metadata in a
temporary warehouse using repository `.venv` Python. No replacement dataset
format, paid service, credential file or live installation is used.

Tests cover unpublished/failed-publication candidates, correct provenance, a
publication between stream chunks, concurrent publication/read history coherence,
exact scope, environment/application/Dataset/view/table/object denial, missing
session, marking checks, revocation/expiry before and during reads, grant/application
changes, inconsistent history, unsupported locations, missing objects, bounded
chunks, abort and reader cancellation. Race fixtures also use synthetic snapshot
IDs to distinguish catalog history records; this is a catalog coherence test, not
an Iceberg engine validation claim. Existing SQL and storage tests are unchanged.

Run `pnpm format`, the focused test file, and full `pnpm check`. Executed results
are recorded in `docs/IMPLEMENTATION.md`. Independent no-findings review,
exact-head Linux CI and current-main integration remain controller-owned gates;
local tests do not establish those results. Full engine IO, runtime publication
adoption, credential lifecycle, retention/recovery transitions, dynamic resource
binding management and production capacity remain follow-up #17 work.

## Scoped metadata publication (issue #49)

`CatalogStorage.publish(session, declaration, signal?)` is a trusted-engine-only
seam. It adds no public upload API, object browser or second publication authority.
The declaration supplies execution/worker/fence, expected output revisions, exact
byte sizes, snapshot IDs, streams, lineage and optional checkpoint. Environment
comes from the authenticated session. Output bindings must additionally specify
`icebergTableUuid`, resolved by trusted composition for that exact Dataset/table.
No physical path, endpoint, credential or object identity is accepted in the
request. RestrictedViews cannot resolve as output or lineage input bindings.

Principal grants and application scopes must both include each Dataset's `write`
operation, with clearance for its complete effective markings. Declared lineage
inputs additionally require Dataset `read` and their markings must be included in
every output binding. Trusted composition still owns completeness of lineage and
effective inherited markings; this API does not infer provenance from arbitrary
code. Binding changes require the existing epoch invalidation discipline.

The seam accepts at most 16 outputs and at most 1 MiB per metadata object, with
exact-length intake and bounded aggregate storage (at most 16 MiB of retained
metadata bytes, plus parser/stream overhead). These are API bounds, not capacity
claims. It copies engine bytes before deriving object identity. It uses the
repository-local `.venv/bin/python` and existing pinned PyIceberg metadata model
to validate Iceberg v2 structure, bound table UUID and exact current snapshot ID
without rounding int64 through JavaScript. Validation does not open metadata's
referenced paths or validate manifest/data-file availability, row constraints or
full Iceberg IO. Arbitrary bytes do not qualify as validated metadata. Python
validation has a 30-second deadline and cancellation; errors never echo metadata.
No new Python dependency or installation step is introduced.

Object IDs are SHA-256 over the JSON tuple `(environment, execution, Dataset)`
followed by exact metadata bytes. They live only under the binding's exact logical
warehouse/table. Whitespace changes count as different content. The same bytes and
execution deterministically reconstruct the same logical publication request.
Sequential create-only writes use chunks of at most 256 bytes; composing adapters
must permit that chunk size and the promised object sizes. Current epoch/session
checks run during engine intake, around each write and at provider transfer
boundaries. All provider initialization, validation and IO occur outside SQL
transactions. After every promised write acknowledges completion, one existing
`Catalog.publish` commits heads, history/lineage, checkpoint and receipt. Its
DB-only callback rechecks current session and epoch inside the final transaction.
Existing parent-revision, fencing, lease-expiry and atomic-output rules apply.
Cancellation is checked at the authorization boundary; as with other commits,
an acknowledgment lost after that boundary must be resolved through the receipt.

### Retry and orphan policy

A lost catalog acknowledgment is retried with the same execution, declarations and
exact bytes. A receipt existence probe only selects the retry path: the existing
`Catalog.publish` still validates its request hash under current authorization and
returns the original receipt, even after lease expiry. Changed content, lineage,
checkpoint or output provenance conflicts with the stored publication. No receipt
is returned from a new storage-owned registry or cache.

An `ALREADY_EXISTS` object is **not** proof of matching content or acknowledged
durability. An uncertain upload is propagated and no new publication is attempted.
If a retry encounters a create-only collision, only an already committed catalog
receipt can resolve it; otherwise it fails closed with `ALREADY_EXISTS`. After an
uncommitted partial/uncertain attempt, trusted scheduling must fence the old worker
and use a fresh execution and lease with freshly checked parent revisions. This
seam neither repairs nor deletes old objects. It cannot resume an uncommitted
partially staged execution in place. Failed, cancelled and conflicting attempts
can leave immutable unreachable metadata. Retention/reconciliation must account
for all retained roots and pins under C04/C08 before future cleanup; there is no
live garbage collection or orphan deletion in this change.

Only `CatalogStorage.read` demonstrates consumption of these logical references.
Existing physical-path writers and readers remain unchanged. Full manifest/data
IO, runtime adoption, recoverable location transitions and #17 remain open.

`tests/catalog-storage-publication.integration.test.ts` exercises real PostgreSQL,
filesystem and isolated checksum-pinned S3 storage using real Iceberg-generated
metadata. Orchestration wrappers insert deterministic failure barriers around
actual provider calls; they do not replace transactional or provider behavior.
Coverage includes multi-output read-back, invisible staging, failed/uncertain
completion, cancellation, session/epoch changes, stale/replaced leases, head and
checkpoint conflicts, competing publication, acknowledgment retry and conflicting
retry. Controller-owned independent review, exact-head Linux CI and current-main
integration remain required before acceptance.

## Additive scoped engine read (#56, dependent on #49)

The metadata-only limitation above still applies to `read`. The separate trusted
`readRows` seam now composes the same pin/Authority resolution with PyIceberg
0.12.0 and PyArrow 26.0.0 through read-only scoped FileIO. It returns pinned
`environment`, `dataset`, `revision`, `snapshotId`, `execution` and an async rows
iterator. It does not repin on later publication. No current runtime/indexer or
physical-path worker is switched to this entrypoint.

Service-owned `readObjects` binds every permitted engine name to an exact logical
warehouse/table/object and immutable size. Only canonical `moria://` names under
the bound table are admitted; the inventory is copied, validated and frozen.
Metadata publication does not create this inventory or upload data objects.
Trusted composition must supply the real immutable objects and mappings; mapping
retention/discovery and production writer adoption remain later work. Missing,
foreign, physical-path and provider URL references fail closed. No second catalog
or policy authority is introduced: PyIceberg's NoopCatalog cannot publish, and
PostgreSQL remains the provenance authority.

Parent-only scoped adapters perform metadata and range IO, with fresh authority
transactions on access/release. Rows receive an additional current-authority check
before release. Scan authority waits are abortable with the engine deadline signal
as well as caller cancellation; a late authorization result cannot release rows.
The underlying catalog transaction retains ownership of SQL cleanup and can
remain pending until contention clears or its existing lock timeout expires.
No provider initialization/network IO occurs in a catalog SQL
transaction. The worker receives no storage or DB credentials. See the
[worker documentation](../../workers/iceberg/README.md#scoped-pinned-snapshot-reader-56)
for the exact protocol, process lifecycle, limits and supported feature envelope.
This is Dataset-level access, not query row-policy enforcement, unrestricted
Iceberg/Spark support, arbitrary-code isolation, production capacity or runtime
rollout. Database timeout configuration remains an operational obligation.

`tests/iceberg-scoped-storage.integration.test.ts` uses disposable PostgreSQL,
real PyIceberg-generated v2 manifests and Parquet, real local storage and the
existing pinned loopback MinIO fixture. The fixture writer preserves logical
names inside standard Iceberg files; the reader cannot access its physical files.
Coverage includes exact scalar rows across multiple data files, range offsets,
pinned provenance across a competing publication, table/reference denial,
revocation/expiry, cancellation, return, deadline and child failure. Real PostgreSQL
environment-lock contention tests check cancellation/deadline settlement and child
and listener cleanup before unlocking, then connection/lock cleanup after unlocking.
Oversized
real Parquet rows exercise the frame rejection path. No mock-only storage or
transaction proof replaces the existing suites. Independent-environment checks,
clean independent review, current-main integration and Linux CI on the exact
revision remain mandatory before acceptance; #17 remains open.
