# Native sync and transform execution

Implementation contract. The native sync/transform service is not yet complete; the required evidence below remains outstanding.

## Existing implementation and boundary

The `Transform`, `ConnectorImplementation`, `Connection` and `Automation` declarations and source compiler existed before the native operational plane. They did not include a DuckDB transform adapter or a general connector execution adapter. `workers/iceberg/worker.py` provides trusted Iceberg staging and streaming value codecs; it is not a user-code scheduler. The previous platform jobs run the specific snow and weather ingestion handlers through Node. These paths do not establish native sync/transform completion.

Rust owns authenticated acceptance, immutable release resolution, exact Dataset pins, current authority, registered worker claims, class admission, user-code supervision, protected IO and grouped publication. Python owns the isolated user transform/connector program and supported DuckDB computation. User code receives no database, catalog, warehouse, provider or host credentials. Python does not supervise the platform or publish pointers.

## Closed initial engine profile

The engine profile supports scalar Dataset columns and scalar arrays with non-null items, and replacement of declared output Datasets. Nested arrays, structs and maps remain outside this profile. A transform receives only its declared whole-Dataset inputs; RestrictedViews and row-filtered object sources cannot become implicit full-Dataset inputs. A connection is bound to a released `connector/v1` implementation, validated immutable configuration, one output, coverage, stream and a registered destination binding. Unsupported append/upsert/delete, continuous read, writeback or schema-discovery capabilities fail before user code until their own checkpoint and publication proofs exist.

The Python adapter and dependencies are locked in a digest-pinned, preloaded OCI image. DuckDB uses a private database, bounded threads, memory and spill, disabled external access and disabled extension installation/loading. The guest has no network, read-only root, unprivileged UID, dropped capabilities and CPU/memory/PID/wall limits. Arbitrary Python code is single-attempt. A replacement supervisor may clean or publish sealed output; it cannot transparently rerun started code.

Each engine task reserves an actual dedicated hard-bounded filesystem in the same transaction as its registered lease. Trusted native metadata supplies only its capacity and the accepted memory budget to the adapter. DuckDB receives half of the accepted memory as its own working-memory limit and the reserved filesystem capacity as its temporary-data limit. The cgroup and filesystem enforce the outer bounds. The guest sees only `/spill`; it never sees the host mount, backing file, loop device or provider credentials. This profile executes one arbitrary program on one admitted host. Parallel execution follows explicit independent jobs and DAG nodes; it does not automatically partition arbitrary Python joins.

## Streaming protocol

An engine protocol profile uses bounded control frames and Arrow IPC batches with backpressure. Input and output byte limits are explicit trusted binding limits; they are not a fixed small population limit. A worker retains at most bounded batches, part buffers and durable metadata spools. It never serializes a complete input or result population into a JSON request, result field or PostgreSQL row.

Each input read is checked against its immutable execution pin and actual live Dataset IO grant. Each output part is written through a grant for a declared output and increasing owner fence. Guest messages use logical input/output names and immutable part identifiers; they do not carry host paths, warehouse endpoints or storage credentials. A monotonically checked frame sequence prevents replay without retaining an in-memory set proportional to the number of batches. Logs and structural events have separate small limits and never contain dataset bodies.

One current-authority guard batches its declared resource checks. It preserves each resource/origin operational check and each current provenance, classification and clearance check, while evaluating an identical filesystem origin/operation pair once within that invocation. The private batch accepts at most 256 closed resource/operation entries and retains at most 8 MiB of filesystem results. It has no cache across calls or transactions and cannot be called by the runtime database role with a supplied identity.

The adapter preserves exact int64s and numeric types without implicit numeric casts. DuckDB exports UUIDs as canonical text; only an accepted UID field converts strictly parsed canonical UUID text back to 16-byte Arrow UUID storage. List field metadata is rebound without changing numeric values, and nullable items are rejected. Native IPC and Parquet validation still enforce the accepted physical schema and per-cell complexity before publication.

The guest serializes UUID storage with the exact accepted canonical extension metadata. PyArrow's logical UUID re-encoding adds an empty extension-metadata key that the native canonical UUID contract excludes; the transport adapter preserves the accepted name-only annotation, including UID list elements. Native schema equality remains exact. Unknown field metadata is retained and numeric arrays are never cast.

The native writer seals bounded Parquet parts and produces Iceberg manifests and metadata through a durable spool. It does not append a new full-table manifest on every incoming batch. Preparing an output validates the complete immutable closure once under its grant. A result-complete handshake, successful guest exit, resource accounting, current authority and confirmed cleanup precede private task sealing.

## Connector transport

A connector can request only approved methods and paths within its immutable operator destination binding. Rust resolves endpoint and secret references, validates current destination policy, applies bounded time/body budgets, refuses redirects outside the binding and attaches credentials. Guest responses omit credentials and sensitive transport headers. Source classifications are recorded before any response body or observable failure is returned to the guest. A transform has no connector HTTP capability.

## Acceptance, checkpoint and publication

The logical key is scoped to environment, resource and verified actor/application. Admission pins the released code, input schemas and snapshots, expected output heads, resource class, limits, configuration revision and complete input labels. Request JSON cannot choose actor, release, provider endpoint, secret, worker or class. Accepted long-running jobs use the separate lease-bound private credential profile; ordinary caller expiry does not extend a public bearer credential.

Prepared output heads, grouped publication receipt, source lineage, checkpoint compare-and-set and execution completion commit in one PostgreSQL transaction. A checkpoint belongs to the exact connection, stream and configuration revision. Lost commit acknowledgement reconciles the exact durable receipt before any further action. A receipt-only completed-attempt guard cannot reopen input or output IO. Cancellation or authority, namespace, parent, owner, fence or incarnation changes prevent publication. Already committed connector side effects are not declared exactly-once.

Public SDK and CLI lifecycle routes use the persistent native API: `pipelines invoke|get|events|cancel|wait`. `pipelines worker` replaces the Linux CLI process with the configured native executable before creating a Node workspace or pool. Worker startup requires an operator registration, host identity, actual cleanup privileges and a verified scratch pool; there is no Node execution fallback.

Cancel returns only `{id,cancelRequested:true}` under the common [control contract](execution.md#public-cancellation-control). It does not inspect input/output populations, checkpoints, guest observations or result status to acknowledge the command. Business receipt inspection remains independently protected.

## Required evidence

Completion requires an actual cold image build, real registered native workers and PostgreSQL, three pinned source Datasets, a bounded streaming join/aggregate, grouped output publication, checkpoint conflict, response loss, worker death, stale-owner denial, cancellation and revocation during IO. The proof must record physical rows, peak worker/guest memory, byte/part counts and temporary storage. The production workload ladder is the implementation target; a small functional fixture does not certify capacity.
