# Trusted local Iceberg adapter

The Python worker's existing `stage` and `read` operations are small-fixture interfaces that materialize their input/result. They are not million-row streaming writers or production worker isolation.

`icebergRows` in `stream.ts` adds a pull-based async row iterator for a pinned metadata location and snapshot ID. The worker rejects a mismatch before emitting rows. Arrow scan batches are sliced into at most 1,000 Python row dictionaries per JSON-lines frame; each frame is limited to 4 MiB. The Node consumer pulls stdout with pipe backpressure, validates the start/end protocol and row count, and kills/reaps the worker on cancellation or timeout. Missing completion, worker errors or incomplete frames fail iteration, so ObjectStore staging cannot publish a partial stream.

Integers and decimals become strings, timestamps require a timezone and normalize to UTC microseconds, and dates/UUIDs become strings. Unsupported nested/binary values and nonfinite JSON numbers fail. ObjectStore still validates rows against the declared ontology schema. The default timeout is 30 minutes; callers can lower it. Callers must consume to completion or close the iterator.

This bounds transport buffering and Python dictionary batches, **not every allocation**: Arrow owns its scan-batch memory, a single wide row can be large, and Iceberg manifest/file planning may grow with file count. Million-scale Arrow memory and throughput measurements remain pending. Initial integration tests stream a real 1,001-row snapshot into ObjectStore, preserve old metadata reads, cancel early, and reject snapshot mismatches without publication.

Metadata locations are trusted internal input. This adapter does not authorize storage paths, source grants or markings and does not establish worker leases/fences. A production indexing coordinator must resolve authorized immutable catalog inputs, bind table identity/schema, enforce current security and atomically validate its fence at publication. Do not expose this function directly as a user API or treat a successful stream as publication authorization.

ObjectStore.stage now accepts a trusted `{execution, worker, fence}` lease binding as its final argument. The binding is persisted with the stage, checked before loading and checked with a lease row lock inside publication. A caller of publishStage cannot substitute a newer token. Catalog.renew extends only an unexpired lease with the same owner/token. Successful publication replay returns its receipt even if the lease has since expired, while still checking the current security epoch. The coordinator must supply this binding and arrange renewal/cancellation; legacy and direct fixture stages may still omit it. Expired workers can continue consuming resources until their supervisor stops them, but cannot publish their bound stage.

The internal `IcebergIndexer` in `services/ontology/src/indexer.ts` now composes this reader with catalog-head resolution, source/target authorization, mandatory fence binding, persisted provenance and an in-transaction current-authorization check. It supports ordinary Dataset sources only; restricted-view object sources await policy integration. It is not exposed over HTTP. Trusted release configuration and serving-label consistency, durable scheduling, renewal/cancellation and storage validation remain required; callers must not expose its configuration or the underlying raw adapters to clients.

For process-local renewal/cancellation, use `IcebergIndexer.runSupervised(session, lease, {signal, seconds})`; the default lease duration is 60 seconds and renewal runs every third of that duration. It never replaces an expired token. The stream supports AbortSignal and kills/reaps its local Python child, escalating after ten seconds. Cancellation is also checked inside publication; a committed result wins a later cancel race. Durable scheduling/recovery and database outage timeout configuration remain outside this helper.

`runSupervised` also accepts `timeoutMs` (default and maximum 30 minutes), covering the whole attempt from initial renewal. Expiry aborts the reader and prevents publication once observed; database waits remain cooperative and require appropriate database/pool timeouts. The reader checks its own timeout between buffered rows as well as at process completion, so killing the child cannot leave buffered rows flowing after its deadline.

Indexed source markings are persisted with the ontology generation. ObjectService checks serving label inclusion for reads, queries, actions and action replay; refreshes cannot remove an active required marking. Legacy generations lack this metadata and require an authoritative migration before production. This protection does not replace release binding resolution or restricted-view policy integration.

## Scoped pinned snapshot reader (#56)

`CatalogStorage.readRows(session, exactDataset, {signal?, timeoutMs?})` is an
additive trusted-service entrypoint. It returns the existing pinned provenance
plus an async `rows` iterator. Existing physical-path readers and consumers are
unchanged. The service binding must supply `icebergTableUuid` and `readObjects`:
an immutable inventory of exact engine names, logical StorageLocations and byte
sizes for that Dataset/table. Engine names have the form
`moria://<warehouseId>/<tableId>/<explicit-object-name>`; slash-separated name
components are ASCII tokens, with no empty components, traversal, escapes or
query strings. These are capability names, not provider URLs or filesystem paths.
The inventory must come from trusted service composition, never request JSON.
Objects and mappings must remain immutable for the scan's lifetime.

The pinned metadata stream and every manifest/data range use parent-owned
StreamingStorage. `scoped_io.py` implements only InputFile and seek/read; output
and deletion fail. PyIceberg receives this FileIO explicitly, so table properties
cannot select a different IO implementation. There is no default filesystem,
PyArrow/S3 fallback or ambient provider credential chain. Unknown references fail
before storage access. Each parent access creates an exact read-only object scope
using the original Authority epoch/session. Authority is checked again before
each released row (including buffered rows), outside storage/network transactions.

The fixed repository `.venv/bin/python -I` entrypoint gets only metadata bytes,
pinned table/snapshot IDs, logical names and sizes. Its environment is an explicit
allowlist (locale, nonexistent HOME, fixed thread limits); it receives no provider
or database credentials, physical warehouse root, executable choice or source.
This is trusted platform code, not an arbitrary-code sandbox or network isolation
claim. Parent/child pipes serialize one request or row acknowledgment at a time;
a process-wide Python lock also serializes Arrow's background IO. Each batch waits
for parent acknowledgment, so a paused consumer applies backpressure. Cancellation,
deadline, early return and child failure terminate/reap the child; a failed child
cannot continue releasing a buffered batch. Consumers must exhaust or close the
iterator. The engine deadline starts on first pull. Scan authority waits race
that deadline and caller cancellation through the final post-exit authority check
and iterator cleanup, so a blocked database check cannot delay cleanup or release
buffered rows. The catalog still owns the underlying
SQL transaction: it drains after contention clears (or its lock timeout expires),
and its late result cannot resume the scan. Initial catalog pinning before first
pull and database connection/statement timeouts remain separate obligations.

The deliberately small supported envelope is Iceberg v2, unpartitioned, unsorted
Parquet data snapshots without delete files. Scalars follow `worker.wire`: strings,
booleans, finite numbers, string-encoded integers/decimals, dates, UTC microsecond
timestamps and UUIDs; one-dimensional arrays require non-null scalar elements.
Nested structs/maps, binary, timezone-free timestamps, other data formats,
partition/sort semantics and delete semantics are rejected. Schema evolution and
arbitrary external Iceberg/Spark compatibility are not certified.

Hard limits are 1 MiB metadata; 128 mapped objects/scan tasks; 16 MiB per object;
64 MiB aggregate mapped sizes; 256 KiB per range; 8,192 range requests and 128 MiB
transferred ranges; 4 MiB JSON frames; 100 rows per batch; 100,000 rows and 32 MiB
encoded row frames per scan; 256 columns. The default and maximum engine deadline
is 60 seconds. Linux RLIMIT_AS caps the child address space at 2 GiB, RLIMIT_CPU
caps CPU time at 60 seconds, and core dumps are disabled. Planning and Arrow can
materialize native batches/files before row slicing: input/transport limits do
not predict RSS or compressed-data expansion. Exceeding the process envelope can
kill/fail the scan. These are defensive fixture-scale limits, not production
capacity targets. A partial iterator is not a successful complete read.
