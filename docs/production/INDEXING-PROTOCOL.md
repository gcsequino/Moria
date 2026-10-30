# Distributed indexing protocol v1

October 30, 2026. Design decisions for the Rust implementation. The first
[dataset-to-object slice](../../services/indexing/README.md) is implemented;
complete protocol and capacity gates remain unproven. C01–C08, S01–S08 and existing action/cursor contracts remain
binding. This specification settles component interactions without introducing
another Node execution runtime. Proof gates below remain open until executed.

## Ownership and implementation boundary

| Owner | Responsibility | Implementation direction |
|---|---|---|
| Control API | Validate declarations, request builds, inspect/cancel work | Retain existing API; no bulk row relay or worker execution |
| Publication authority | Commit heads, environment revisions, receipts, change records and progress | Existing PostgreSQL transaction boundary, one authority per environment |
| Index coordinator | Pin inputs, plan tasks, assign partitions, track completion | Rust service; multiple replicas with fenced database claims |
| Index executor | Scan/map/validate datasets, prepare object and search partitions | Rust processes on independently provisioned hosts |
| ObjectService | Authorize operational reads/actions and preserve pinned views | Existing contracts; runtime rewrite requires separate evidence |
| SearchService | Authorize search, validate freshness, serve active generations | New protected service; search engine behind a capability-tested adapter |
| Function host supervisor | Admit and isolate user functions | Separate fleet protocol; not an indexing task or implicit dependency |

Use existing engines and format libraries behind these boundaries. An embedded
engine is not a distributed scheduler. Spark interoperability remains required
by M2; this protocol does not select its replacement. Existing Node/Python code
supplies compatibility fixtures while Rust replaces the bulk path. No provisional
Node coordinator, search worker or distributed-queue implementation is required.

## P1: authority, identity and transaction scope

One environment belongs to one authoritative cell initially. PostgreSQL remains
the transaction authority for its objects, security, publication, receipts and
change records. Cross-environment actions stay rejected. Processing partitions
can span hosts without splitting an action across independent databases.

Keep the existing environment revision lock for short authority commits. No
scan, embedding, bulk upload, model call or shuffle runs while holding that lock.
Parallel task preparation removes the global execution lock; it does not remove
commit serialization. A replacement for revision ordering needs the P03 proof in
[ACCEPTANCE.md](ACCEPTANCE.md), not a language port or a claim of extra replicas.

Installation-wide `(identityDomain, canonicalKey) -> UID` reservations remain
durable and unique. Batch resolution is allowed only through that owner. Worker
partitioning cannot allocate alternative identities or infer entity matches.
Dataset UIDs, mappings, overlays, tombstones and action receipts survive rebuilds.
Search is derived; effective object state cannot be rebuilt from Iceberg alone.

Every internal reference includes installation, cell incarnation, environment
and immutable resource identity. The serving directory admits one cell owner.
Failover uses the tested database HA mechanism; no new consensus implementation
is introduced. An isolated restore starts with dispatch disabled and cannot
reuse old worker grants. It must establish a new admitted incarnation and fence
the old writer before accepting traffic. If that fencing cannot be proved,
restore activation is blocked; a restored database counter alone is insufficient.

## P2: versioned bindings and durable work

A binding pins its revision, source/target resource IDs, schema/value-model
version, mapping digest, identity domains, markings, authorization identity,
partition layout, and declared limits. Search bindings additionally pin selected
properties, chunker and embedding model/dimensions. No bearer tokens, provider
secrets, caller-selected file paths or executable handlers enter durable work.

| Record | Required meaning |
|---|---|
| Projection | Target identity, desired input token, active generation and applied token |
| Build | Immutable pinned input snapshot set, binding/layout revisions, expected target head, authorization context, state and receipt |
| Task | Build, input split, destination partition, current attempt/fence, expiry, progress and sealed output descriptor |
| Generation | Complete immutable partition-root descriptor, input provenance and activation receipt |
| Change transaction | Environment commit revision, ordered event ordinals, transaction-end marker and replayable payload references |
| Consumer progress | Binding/layout, partition, durable applied position and retention pin |

Reuse execution attempt/audit facilities where their semantics fit. Desired
state and immutable build history are distinct: a new desired version never
rewrites an accepted build's inputs. Notifications may coalesce; accepted intent
and required replay history may not disappear.

Build states are `pending -> running -> prepared -> active`, with terminal
`failed`, `cancelled`, `conflicted` or `superseded`. A task claim has a renewable
lease and a monotonically increasing attempt fence. An expired claim creates
a new attempt; it never authorizes the previous attempt to acknowledge work.
Permission or binding failures block progress with a safe visible reason; they
do not fall back to a stronger identity or trigger an unlimited retry loop.

## P3: atomic change capture and replay

Dataset publication writes downstream build intent in the SAME transaction as
all promised heads, lineage, checkpoint and publication receipt. Object source
activation, actions, tombstones/restores and relevant binding changes likewise
write their downstream change records in their originating transaction. An
action's edits, audit, outbox and receipt remain atomic under C06.

A change position is `(cellIncarnation, environment, commitRevision, ordinal)`.
Revisions use the existing transactional commit order, not timestamps or a
nontransactional sequence. A transaction-end marker identifies its complete
event set. A checkpoint names an actual committed position; consumers never
infer missing work from numeric sequence gaps. Progress is a vector across
processing partitions, with a complete-transaction visibility barrier for search.

The durable PostgreSQL outbox is the initial replay source. A broker may carry
the same records later, but it cannot become a second publication authority.
Workers poll; notifications are only a latency optimization. Relays deliver at
least once, preserve partition order and tolerate duplicate delivery. There is
no general exactly-once transport or arbitrary-side-effect claim.

Object events identify the UID and exact committed version, not a request to
read whatever is latest. Source-generation activation carries a pinned,
partitioned change manifest with complete membership changes or explicitly
requests a snapshot rebuild. Large changes reference immutable paged data;
millions of rows are not inserted into an activation transaction's payload.
Missing rows imply deletion only for a completed full-coverage source.

Consumer output and its checkpoint commit atomically when they share a database.
For an external search destination: write idempotent immutable versions first,
obtain durable acknowledgement, then checkpoint. A crash between these steps
replays the same versions. Writes are keyed by binding, generation, partition,
UID/chunk and source position; equal keys with different content fail closed.

## P4: partition execution and generation activation

Dataset scan tasks pin standard Iceberg snapshots and reader capabilities,
including required delete semantics. File discovery is not an incremental-change
algorithm. Unsupported schema, delete or mapping features fail explicitly.

Scan tasks may follow file/split boundaries; target object partitions use a
stable UID layout. For layout v1, hash the bytes of
`moria/index-shard/v1\0`, environment UUID, target UUID and UID with SHA-256;
UUIDs are their 16 network-order bytes. Interpret the first eight digest bytes
as unsigned big-endian and reduce modulo the layout's persisted power-of-two
virtual-partition count. Literal `\0` denotes one zero byte. Host reassignment
does not change this layout. A partition-count change requires a new layout and
staged generation, not reinterpretation of an existing checkpoint.

All rows for a UID meet at the same target partition. Executors reject duplicate
canonical UIDs across all input tasks, even equal duplicates; use bounded
sorting/spill or engine facilities, not an unbounded in-memory set. Validate
registered identity/home, exact values, source coverage and schema before sealing.

Workers write private immutable partition output with schema, count, digest,
input and attempt provenance. Partial/rejected attempts remain invisible.
At activation the authority validates the sealed task set, current grants and
markings, expected heads, current coordinator/partition fences and layout. It
atomically changes the complete generation root, records its receipt and emits
search intent. No partially prepared partition becomes a public object head.

Expected source heads and desired automatic-build tokens are checked together.
Old automatic work becomes superseded when a newer desired version wins. An
explicit pinned build reports a head conflict rather than silently repinning.
Same-schema activation may preserve concurrent overlays only under existing
head/schema revalidation rules. It cannot overwrite edits or silently migrate
identity or schema. Multi-output publication remains all-or-nothing under C04.

Cancellation orders against activation in this transaction: if cancellation
wins, activation fails; a committed activation returns success on receipt replay.
Loss of acknowledgement does not cause an additional visible activation.

## P5: ownership transfer and destination fencing

Claims scope ownership to `(cell incarnation, binding, layout, partition)`.
Takeover increments the authority fence. Old workers cannot seal authoritative
completion, advance progress or activate a generation. Check the lease and fence
inside the committing transaction, not only at task startup.

Stale writers must also be harmless at an external destination. Search adapters
must support either atomic rejection of stale versions/fences or immutable
attempt/version namespaces selected only by an authoritative active root. The
v1 protocol uses immutable version namespaces. An old worker can leave private
orphan data, but cannot overwrite active content or make its namespace visible.
An adapter that cannot enforce this capability is unsupported.

Moving a partition transfers its durable checkpoint, layout, input pins and
pending work; takeover resumes at or before the last durable acknowledgement.
Duplicate replay is safe. Placement accounts for hot partitions and host budgets;
it cannot split one UID's ordered updates between simultaneous owners. Split/merge
of a virtual partition is a layout rebuild. Unavailable authority means no new
activation or progress acknowledgement, even if local compute can continue.

## P6: rebuild handoff and search visibility

At rebuild admission, atomically pin an object generation map at environment
revision R and establish a replay-retention pin for changes strictly after R.
Workers read effective objects at R, including overrides/tombstones, and preserve
source/property provenance and policy inputs. They do not index one builder's
filtered visible subset as the shared collection.

Build a private search generation, then replay changes after R in source order.
Choose W only after every partition has durably applied all relevant events
through the same complete environment transaction. Activate the generation at W
with one authority pointer update. Writes after W remain queued. A crash during
the handoff resumes from pins/progress; it never starts from an uncoordinated
latest snapshot or silently skips the intervening changes.

Ongoing search updates also retain versioned entries and advance the public
visibility barrier only through complete transactions. Entries above the barrier
are invisible; for each UID/chunk select the latest applicable version at or
below it, including deletion markers. An in-place overwrite that destroys the
still-visible version is not an acceptable adapter. Existing live readers retain
their generation and required history. A binding/model/layout change builds a
separate generation; embedding models/dimensions are never mixed for ranking.

Object operations preserve existing C05/C06 guarantees. Search is a separately
declared asynchronous projection: responses report binding/generation and served
object revision. A requested minimum object commit token is satisfied only when
the active barrier covers that complete transaction; wait within the declared
deadline or return `CONSISTENCY_TIMEOUT`. Dataset-to-object readiness is reported
separately; dataset revision numbers are not object visibility tokens. No implicit
search pagination capability inherits C05 by reusing an opaque cursor string.

## P7: authorization and invalidation

Workers run under an explicitly provisioned binding identity and bounded
delegation. Ad hoc builds also retain their initiating identity/application and
required session constraints. Recovery reauthorizes that recorded context; it
cannot manufacture a new administrator identity. Existing generic imports lacking
a durable authorized binding remain blocked until authorized retry or explicit
binding configuration. No credential or privilege is inferred from an upload.

Check current authority at bounded reads, embedding dispatch and activation.
Trusted direct storage I/O requires the existing S06 mechanism proof; until that
passes, use scoped mediated streaming outside the management API. Untrusted user
code never receives engine credentials or a raw warehouse/control-database path.
Restricted-view indexing requires the narrow trusted backing-data binding and
retained canonical predicate inputs; view access alone is not a dataset grant.
An adapter without that implementation reports unsupported, not partial success.

Every indexed fragment retains its complete contributing-property provenance,
markings and content version. Derived embeddings retain the union of those
restrictions; a projected text field cannot erase an embedding's dependencies.
Embedding goes through the authorized model destination, with no paid-provider
default. Provider failure cannot acknowledge uncomputed search work. Persist
computed embedding bytes as protected build artifacts before destination
writes; replay reuses those bytes and never assumes model determinism.

Object mutation/source activation invalidates affected search content in the
originating transaction. A UID version gate handles ordinary edits/deletes;
a type/generation gate handles replacements without enumerating every UID under
the commit lock. Older entries can remain physically stored but are ineligible
immediately. This may reduce recall during catch-up; it must be disclosed.

Search applies current authorization/property restrictions and content-validity
gates BEFORE ranking, top-K, facets or counts. Post-filtering an unauthorized
top-K is insufficient. Recheck authority immediately before response release.
If an engine cannot enforce these gates, reject the operation. Exact authorized
retrieval remains the baseline; ANN/hybrid and new query forms require explicit
contract changes and filtered-quality tests, not merely an engine configuration.
Authority outage denies release. Existing security epochs invalidate caches and
pinned cursors; rebuild progress cannot postpone revocation or deletion denial.

## P8: transport, bounds and retention

First-slice disposition: Rust processes use the existing PostgreSQL authority
directly for control and durable claims. Scan plans use generated Protobuf
messages with a pinned reader ABI. There is no gRPC listener or network-control
certification in this slice. The following network transport is the target when
that boundary is introduced, not a claim about current endpoints.

Versioned control messages use Protobuf/gRPC with authenticated service identity;
credentials are provisioned outside declarations and work records. The required
operations are `RequestBuild`, `InspectBuild`, `CancelBuild`, `ClaimTask`,
`RenewTask`, `SealTask` and `ActivateBuild`. Each mutation carries a stable
idempotency key and expected version/fence. Unsupported protocol versions fail
before work starts. Internal RPCs do not create a public universal run API.

Bulk data uses Arrow IPC record batches or standard immutable engine artifacts,
not per-row JSON RPC. Exact Arrow mappings must pass the shared value-model-v1
conformance vectors, including decimals, int64, nulls, arrays and timestamps.
Schema fingerprints and canonical hashes keep their existing meaning. JSON
inspection renders 64-bit counters as decimal strings; transport counters are
explicit integer fields, never floating-point identifiers. Generated wire schemas
and golden encodings belong to the first Rust slice, not a temporary Node runtime.

Bindings/deployments declare finite batch/frame, memory, spill, task deadline,
concurrency and connection limits. Admission reports saturation explicitly.
Snapshot size is not a process-memory budget: traverse metadata/reference
inventories with bounded buffers and durable progress. Preserve complete closure,
identity and content verification; do not bypass checks by raising today's
64 MiB recovery ceiling. Larger-snapshot support is gated on the replacement
reader/verifier, not claimed by this document.

Protect the union of active generations, builds, consumer progress, live cursors,
promised recovery points and backup pins. Rebuild pins are acquired before the
snapshot/log handoff and released only after durable completion/cancellation.
Retention pressure must stop admission before a promised replay boundary is
lost. Missing history returns an explicit rebuild-required failure; it does not
silently skip to latest. Orphan deletion remains gated on proven nonreachability,
existing grace rules and recovery windows. No new automatic GC runs in this slice.

## Required proof before claiming support

| Gate | Experiment and required result |
|---|---|
| IDX-01 | Real PostgreSQL/Iceberg: kill after source commit before dispatch; restart recovers the pinned intent without another upload |
| IDX-02 | Two Rust executors on distinct hosts/processes prepare independent partitions concurrently; exact union/count/UID invariants hold |
| IDX-03 | Kill during scan, sealing and after activation before acknowledgement; receipt replay exposes one complete generation |
| IDX-04 | Pause an old owner, transfer its partition, resume it; destination writes cannot alter visible state or acknowledged progress |
| IDX-05 | Publish newer desired input during an old build; old work cannot overwrite it; edits/identities/tombstones remain correct |
| IDX-06 | Mutate objects before/during/after rebuild cutover; snapshot plus replay equals the reference effective state with no gaps |
| IDX-07 | Multi-object action spans search partitions; readers never see a partially applied transaction at the declared barrier |
| IDX-08 | Revoke identity/property/marking access, delete content and interrupt authority during work/query; no forbidden values/scores/counts escape |
| IDX-09 | Crash between external index acknowledgement and progress commit; replay is idempotent and stale versions cannot resurrect deletion |
| IDX-10 | Missing/corrupt inventory, unsupported Iceberg delete, duplicate UID, exhausted spill or embedding failure prevents activation/progress |
| IDX-11 | Rebalance, restart, cursor/backup pin and retention-pressure tests preserve required histories; isolated restore fences old writers |
| IDX-12 | Execute the workload ladder with wide/history-heavy objects, concurrent actions and ingestion, skew and injected host loss; report offered outcomes, tails, backlog/drain, resource use and cost |

Protocol traces, golden value encodings and model checks can expose design errors
before deployment. They do not replace real transaction, authority, engine or
isolation experiments. Retain existing tests and add Rust compatibility tests;
no mock-only substitute or reduced acceptance gate is permitted.

Implement in vertical slices: durable intent plus a Rust dataset-to-object path;
parallel partition execution and activation; durable object changes and search
rebuild/replay; then engine-specific query capabilities and fleet capacity proof.
Each slice must keep the same protocol identities/receipts and pass its affected
failure gates. A single-host Rust development run exercises the production
protocol; it is not a separate disposable implementation or scale certification.

The [workload contract](WORKLOADS.md) governs numerical calibration and required
run manifests. This document does not invent a throughput guarantee or close
M1–M9, P01–P06, or the historical validator investigation. Storage/engine feature
matrices, operational sizing and the exact wire schema are implementation proof
artifacts; inability to satisfy a capability is an explicit blocker. Single-large-
environment transaction capacity remains bounded by the existing authority until
P03 is independently proven.
