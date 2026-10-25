# Published catalog metadata reads

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
local tests do not establish those results. Full engine IO, write/publication
integration, credential lifecycle, retention/recovery transitions, dynamic resource
binding management and production capacity remain follow-up #17 work.
