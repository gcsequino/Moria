# Rust dataset-to-object indexing

Rust coordinator/executor processes claim durable PostgreSQL tasks, scan pinned
Iceberg files, reduce stable UID partitions and activate complete object
generations. Node APIs neither poll nor execute these tasks. Both runtimes use
the same PostgreSQL generation-publication function. Dataset publication captures
indexing intent in its existing transaction, so recovery needs no new upload.

This is the first slice of [the indexing protocol](../../docs/production/INDEXING-PROTOCOL.md),
not search implementation or production-capacity certification. Control/claims
currently use PostgreSQL directly. Generated Protobuf scan plans pin protocol and
reader ABI versions; no gRPC listener is implemented in this slice.

## Supported inputs

Linux, Rust 1.99, Iceberg Rust 0.10.1 and Arrow 58 are pinned by the image/lockfile.
The reader supports copy-on-write Iceberg v2 in an immutable shared POSIX
warehouse. Each host needs the same absolute read-only warehouse mount. It maps
same-name columns into a declared object schema with registered permanent UIDs
and exact value-model-v1 scalars/arrays. Computed mappings belong upstream.

Delete files, nonempty Iceberg partition specifications, name mappings,
restricted-view sources and direct S3 input are explicitly unsupported by this
reader release. Existing scoped local/S3 storage adapters and their total
verification limits are unchanged. This service does not bypass those limits.
The management CSV import remains a 1,000-row demonstrator; bulk dataset producers
publish through the catalog authority, not row JSON through the management API.

## Build and operate

Requires **schema version 4 with its exact baseline checksum**. Use the existing
migration owner to initialize a fresh isolated database. There is no automatic
version-3 upgrade, unversioned adoption, live migration or data deletion.

```sh
cargo build --release --locked --manifest-path services/indexing/Cargo.toml
cargo test --locked --manifest-path services/indexing/Cargo.toml --lib
MORIA_DATABASE_MODE=external MORIA_INDEXING_MODE=distributed pnpm exec tsx apps/local/src/api.ts
```

Provide the API's existing external database configuration. Production defaults to
distributed indexing; development retains the legacy fixture path unless selected
explicitly. The API never launches a worker or falls back when workers are absent.
Workspace status and the console distinguish saved and served source revisions,
pending work and failures.

Workers use an externally provisioned runtime role with existing scoped runtime
table/sequence grants: no schema CREATE, ownership, privileged membership or
administrative attributes. Supply its URL through an external environment secret:

```sh
export MORIA_INDEXING_DATABASE_SECRET_REF=INDEXING_DATABASE_URL
export MORIA_INDEXING_DATABASE_TLS=verify
services/indexing/target/release/moria-indexing worker
```

TLS verifies certificates/hostnames by default. `MORIA_INDEXING_DATABASE_CA` can
name an operator-supplied CA file; TLS `disable` is for trusted local fixtures.
URLs with query options are rejected. Credentials never enter declarations,
durable tasks or command arguments.

`MORIA_DATABASE_POOL_MAX` defaults to 4 (range 4–128), replicas to 1 and connection
budget to 32. Require `pool maximum * replicas + 1 <= budget`. Count every API,
scheduler, function worker and Rust process, including replacement overlap.
Provision finite host/container CPU, memory and storage limits separately.

Operator commands use those same credentials:

```sh
moria-indexing status
moria-indexing cancel BUILD_UUID
moria-indexing retry BUILD_UUID
```

Retry retains pinned inputs and their recorded authority. It cannot revive
cancelled work, overwrite newer source data or substitute a fresh identity/session.
Changed authority needs explicit trusted `IcebergIndexer.registerDistributed`
registration, which rechecks permissions/schema compatibility. Registration is
server composition, not an arbitrary request-JSON endpoint.

## Safety and bounds

Processes execute one task at a time; multiple processes claim different tasks
without the old global supervisor lock. Thirty-second leases renew every five
seconds. Bindings have finite deadlines (30 minutes default); SQL statement and
lock deadlines are finite. Current environment/filesystem epochs, grants, session
constraints and cell incarnation fence reads and activation. No old owner can
fail a replacement's build. Durable attempt history survives takeover/retry.

Arrow batches have at most 1,000 rows and a checked 64 MiB decoded budget. Objects
are capped at 4 MiB, metadata reads at 8 MiB and range reads at 16 MiB. Total input
is streamed. Native decoding still needs enforced process memory limits.

Read-only storage reauthorizes before/after bounded byte transfer. Linux openat2
rejects escapes and symlinks; nonregular/multiply linked files are denied. No
arbitrary URLs, ambient provider credentials, writes or deletion are supported.
Warehouse inputs remain immutable under existing publication/retention rules.

Scans write private task/fence rows. Reducers validate global UID uniqueness,
permanent homes and exact schemas, then load an unpublished serving stage. All
partitions seal before activation. Bulk loads, overlay scans and full counts stay
outside the environment commit lock. Failure preserves previous heads and edits.
That commit boundary remains a capacity limit until P03 is proven.

Snapshots, build inputs, plans, attempt history, published generations and receipts
remain retained. Retry removes only failed unpublished scratch. There is no new
automatic garbage collector, history expiration or production restore claim.

## Verification

The test Dockerfile builds release runtime/test executables from the pinned Rust
image. `pnpm check:indexing` requires matching source/schema fingerprints and
nonzero executed Rust cases, including all 100 shared value-conformance vectors.
Native development can set `MORIA_INDEXING_BINARY` and
`MORIA_INDEXING_TEST_BINARY` to matching build outputs.

`tests/indexing-rust.integration.test.ts` uses actual Rust children, PostgreSQL and
PyIceberg files. It covers concurrent scans, paused-owner takeover, real COMMIT
acknowledgment loss, replay, cancellation/retry, duplicate UIDs, exact values,
preserved edits, revocation and generic-import recovery. Existing tests remain in
the full gate. Independent processes are tested; separate-host failover, engine
expansion and the complete mixed-workload/cost ladder still need IDX-01–IDX-12
evidence. Passing functional cases is not a throughput certification.
