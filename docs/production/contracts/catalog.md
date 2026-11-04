# Native Dataset snapshot publication

Status: native POSIX/S3 source publication and native V2 snapshot indexing have
real engine/authority integration proof. This contract does not claim N25 population-wide
serving projections or production throughput.

`catalog::DatasetIoGrant::acquire(Database, VerifiedSession, Lease, IndexNamespace)`
is the only Dataset write capability constructor. The session comes from native
authentication. The lease is a live native execution task or preparing coordinator,
including owner, fence and instance incarnation. Every bounded transfer checks the
real current authority before and after I/O. No indexing source grant can write a
Dataset. The whole-Dataset candidate profile requires explicit output `read` and
`write` permission; write-only uploads need a separate candidate ownership protocol.

The namespace is provisioned in `native_storage_namespaces`: environment, resource,
opaque warehouse/table IDs, revision, purpose `dataset-source`, nonnull Iceberg UUID,
markings and enabled state. Policy revisions have immutable history; content
changes and re-enabling require a fresh, never previously used revision, including
after deleting the active namespace. Callers cannot supply filesystem paths, provider
endpoints or credentials. Published filesystem resources must resolve through the
active source release with matching binding digest and reference and have kind
`Dataset`. A RestrictedView cannot be registered through the generic reader path.
Legacy namespaces require an explicitly trusted Dataset head registration.

Execution `pinnedInputs` names immutable Dataset revisions and snapshots;
`expectedHeads` declares every output and expected revision. Read grants and
current filesystem access are checked for every input. Namespace labels and the
complete inherited filesystem marking union are retained in candidate state;
each output's labels must cover the complete input union. Candidate state also
captures exact namespace revisions and current security epochs.

`prepare_snapshot(store, grant, spool, SnapshotPin, cancellation)` performs native
Iceberg metadata/history, Avro manifest and physical Parquet validation outside
the publication transaction. It accepts an already sealed, bounded, durable
private inventory spool; it does not accept a caller-supplied `SnapshotProof` as
attestation. It writes immutable closure pages (at most 256 entries / 256 KiB
inventory plus a bounded header), then a root at most 16 KiB, through the guarded
object store. Pages form a backwards hash-checked chain with exact ordinals. The
private SQLite path is never a catalog reference. Interrupted preparation leaves
unreachable immutable objects and no visible Dataset head.

The head metadata reference is:

```text
moria-snapshot-v2:<canonical JSON {"version":2,"root":SealedObject}>
```

The JSON uses the shared canonical JSON encoder; alternative encodings are
rejected. `SealedObject` contains only a version-1 logical object reference, byte
length and SHA-256. Root/page bodies use the same canonical encoder. The root
contains version 2, native verifier proof, page count and final-page descriptor.
Proof snapshot IDs are canonical signed-int64 decimal strings. Counts and byte
sizes must be exact JSON integers no larger than 2^53−1; private spool accounting
still obeys operator budgets. Version-1 closure inventory digests retain their
documented `moria-closure-v1` length-delimited entry format. Legacy Node metadata
readers explicitly reject native v2 references rather than bypassing their old
closure bounds.

`publish_snapshots(Database, VerifiedSession, coordinatorLease, candidateIds,
checkpoint?)` calls the shared SQL publication authority. It rechecks the current
session, producer binding, all grants, inherited markings, exact namespaces,
incarnation and coordinator fence. A single transaction compares every declared
output head, writes all heads, immutable snapshot history/lineage, optional
checkpoint, indexing intents and publication receipt, and invokes native execution
completion. Any conflict rolls back the whole group. Checkpoints additionally
require a trusted `runtime_snapshot_checkpoint_bindings` entry for the sync
execution binding's exact connection, stream and configuration revision. Receipt
replay revalidates current authority and returns the original result after a lost
acknowledgement without publishing again.

`snapshot_root` and `snapshot_page` expose bounded digest-checked native reads.
Consumers must validate the complete chain, exact inventory membership, configured
total budgets and native semantic proof before use. A read-only indexer must use a
separate service-private read cache rather than receive Dataset write permission
to recreate inventory scratch state. Native indexing now uses a durable exact-root
fenced verification attestation and lazy approved-entry FileIO; fresh current
authority and immutable physical version checks remain mandatory on each transfer.
It does not repeat whole-closure verification for every file task.

The mandatory real integration wrapper is
`tests/native-catalog.integration.test.ts`. It invokes the compiled
`/usr/local/bin/moria-catalog-tests` harness (or explicit trusted operator override
`MORIA_CATALOG_TEST_BINARY`) and requires exactly one executed test per successful
invocation. Missing artifacts fail. The fixture uses PostgreSQL under a nonowner
role, real PyIceberg/Avro/Parquet objects, protected POSIX transfers, process kills,
and a real PostgreSQL transport proxy for acknowledgement loss.

`dataset_writer::StreamingDatasetWriter` accepts flat scalar and scalar-array Arrow batches under a
real task `DatasetIoGrant`. It validates schema and C03 values, splits at 8 MiB of
logical row data, limits encoded Parquet parts to 16 MiB, and persists immutable
pages of at most 128 part descriptors. A failed append poisons the writer; neither
failure nor `finish` changes a Dataset head. The sealed task result is exactly
`{datasetOutputs:{[datasetResourceUuid]:SealedObjectPartsetRoot}}`.

`dataset_writer::prepare_replacement` requires the real coordinator grant and
matches each part-set root against the same execution's sealed task result,
owner, fence, incarnation, resource and schema hash. It constructs bounded native
Iceberg manifests and a manifest list, then calls the same complete semantic
`prepare_snapshot` verifier. Existing native table metadata, snapshots, branches,
metadata history and closure entries are retained; sequence numbers advance from
the exact previous head. It rejects legacy metadata, schema evolution and provider
migration that this adapter cannot prove. Iceberg metadata retains exact signed
64-bit numbers; native part-set and closure descriptors use canonical C03 JSON.
UUID columns carry canonical Arrow UUID metadata and Parquet UUID annotations;
scalar arrays retain accepted element IDs and nonnull item types. Actual POSIX and
MinIO tests publish two generations, read every current and retained historical
row, and compare permanent UIDs and nullable arrays exactly. Append, upsert,
delete, nested arrays, structs and maps remain unsupported by this replace writer.

FS-owned Dataset IO requires an enabled `native_dataset_bindings` record with the
exact logical namespace/revision, accepted Iceberg schema and provisioned provider
physical identity. The grant captures this record alongside current authority,
and writer open, snapshot preparation and pinned reads check it. Missing bindings
are accepted only for explicitly registered FS-less compatibility Datasets.

The current-authority Dataset I/O SQL guard resolves the real session afresh on
every invocation and reuses that verified state only inside that statement.
Private state kernels are denied to the runtime role. Before/after transfer
checks, execution and task fences, current epochs, inherited filesystem ACLs and
namespace revisions remain mandatory.

Typed serving projections now build off-lock and replay action overlays before
atomic source/projection activation. Remaining dependencies include coordinated
Iceberg expiration, collection of unreachable preparations and verifier attempts
under retained-head/lease rules, restore qualification and production scale
qualification. Functional component tests do not certify those targets.
