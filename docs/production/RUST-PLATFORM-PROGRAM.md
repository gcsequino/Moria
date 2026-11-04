# Native runtime implementation program

October 30, 2026. Active implementation record. This inventory covers the accepted
Moria platform scope, not only the existing demonstrator. A row is complete only
when its implementation, callers, recovery behavior and real integration tests
are complete. A Rust type, protocol declaration, mock, isolated kernel or passing
small fixture does not complete a distributed component or certify capacity.

## Scope and acceptance target

Preserve C01–C08, S01–S08 and R01–R05. The source contracts remain
[the specification](../spec/README.md), [architecture](ARCHITECTURE.md),
[workload profiles](WORKLOADS.md) and [indexing protocol](INDEXING-PROTOCOL.md).
Restricted views remain terminal and cannot authorize transform input. User
repositories are independently deployable applications/functions, not arbitrary
API hosts. The retired Snow/weather example remains a test fixture.

The v0.2 production workload ladder was selected as the implementation target on
October 30, 2026: 1m/10m/50m objects, repeat at 100m; 20/500/2,000 steady external
requests per second and 100/2,000/10,000 peak. Detail/page/filter p95 is 500 ms,
action p95 is 1 s; each operation retains its specified p99 and error denominator.
These numbers are not a claim that the current system meets them. Data-processing
tiers are independent; no petabyte tier, HA SLA, cost ceiling or topology is
inferred from transactional success. Missing inputs and unsupported operation
classes prevent certification. Never substitute a short local run for the full
repetitions, history/width, contention, recovery and soak contract.

The implementation uses Linux and a shared Rust crate in `services/runtime`.
Executables/roles scale independently; sharing code does not mean sharing a
single worker, event loop, process, admission queue or failure domain. The existing
`moria-indexing` executable remains compatible. TypeScript control/UI code is
retained where it does not carry the bulk or operational execution path.

## Complete component inventory

| ID  | Component and baseline implementation                                                                 | Native implementation / structural change                                                                                                        | Dependencies and completion discriminator                                                                                                                                                                                |
| --- | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| N01 | Typed values and policy kernel; `packages/contracts`, `packages/authorization`, `services/security`   | Shared Rust exact codecs, typed AST validation/evaluation/SQL, delegated grants, markings, sessions and current authority                        | Existing value vectors plus cross-runtime policy and real SQL parity; final-release and commit revocation races                                                                                                          |
| N02 | Identity allocation; `Catalog.resolveIdentity`, object homes                                          | Batched canonical-key allocation with durable domain ownership and permanent reservations                                                        | P04; concurrent same-key allocation, transfer/restore fences, no UID reuse                                                                                                                                               |
| N03 | Environment/cell routing; no fleet ownership implementation                                           | Authenticated directory and fenced environment/domain placement                                                                                  | P04/P05; one authoritative owner, bounded revocation validity and partition denial; no implicit cross-cell transaction                                                                                                   |
| N04 | Operational reads/cursors; `ObjectService`, `queryRows`, `CursorStore`                                | Native serving replicas, bounded indexed planning, durable pinned cursors and complete supported query classes                                   | N01/N03; pinned/current predicates before LIMIT/order/group, current deletions, immutable 15-minute original expiry, replica continuation                                                                                |
| N05 | Actions/overlays/publication; `ObjectStore`, `Catalog.action`                                         | Native transactional action owner with pre/post checks, dedupe, audit and outbox; measured commit/admission redesign only if required            | N01/N02/N04; preserve C06 and P03 ordering before replacing environment serialization; lost-ack/same-key/multi-object races                                                                                              |
| N06 | Object change capture; only source activation marker exists                                           | Transactional ordered change frames, complete-transaction watermark, UID/type-generation invalidation and replay pins                            | N05/N08; edits, deletes and generation switches commit intent atomically; replay has no gap or partial transaction visibility                                                                                            |
| N07 | Object/file transfer; Node local/S3 adapters                                                          | Native scoped range/chunk transfer, hashing, private uploads, sealed immutable references and explicit provider bindings                         | N01; real POSIX/MinIO faults, per-transfer authority/fence checks, no ambient credentials or caller paths                                                                                                                |
| N08 | Snapshot/catalog authority; Node buffers closure up to 64 MiB/128 objects and relays base64 to Python | Paged immutable closure, durable scratch, native streaming verification and atomic grouped publication                                           | N07/N14; complete supported Iceberg closure, heads/checkpoint/lineage/receipt/index intent/execution completion in one authority transaction                                                                             |
| N09 | Generic source ingestion; only bounded demonstration/domain fixtures exist                            | Declared connector fleet, replay/checkpoint rules, immutable staged datasets and bounded partitions                                              | N07/N08/N14; interrupted full scan cannot replace good data, source-specific dedupe and durable checkpoint oracle                                                                                                        |
| N10 | Distributed transforms/analytics; no production engine adapter                                        | Native admission/coordinator and scoped Arrow/engine adapters around existing SQL/Spark engines                                                  | N01/N07/N08/N14; actual supported engine/version interoperability and P01/P02; no replacement distributed SQL engine                                                                                                     |
| N11 | Dataset-to-object indexing; first Rust POSIX slice implemented                                        | Extend native coordinator/executors to supported scoped storage and Iceberg profiles, bounded bulk UID paths and retention                       | N02/N07/N08/N14; IDX-01–IDX-12, distinct-process/host recovery, exact union/count/UID and mixed load                                                                                                                     |
| N12 | Search projection/rebuild; absent                                                                     | Native transaction-aware projector, durable embeddings, partition/generation barriers and snapshot/log handoff                                   | N06/N11/N23; stale destinations fenced, no replay gap, persisted embedding bytes reused, complete commit visibility                                                                                                      |
| N13 | Protected exact retrieval; absent                                                                     | Native search service using actual pgvector exact cosine, current property/row restrictions before ranking                                       | N01/N04/N12; R05 topK<=50, zero-vector rejection, fixed chunker/model/dimension generation, current deletion/revocation oracle                                                                                           |
| N14 | Jobs/schedules; `Jobs.runNext` holds global supervisor lock and active uniqueness is per kind         | Native per-task claims, renewable fences, immutable execution specifications, resource-class budgets and durable automation firing               | N01; two independent owners, DB-clock deadlines, coalescing/overlap audit, completion atomically composed with domain publication                                                                                        |
| N15 | Sandbox process lifecycle; Node Docker worker                                                         | Native supervisor with real OCI isolation/accounting and separately capability-tested production backend                                         | N01/N14/N16; actual CPU/memory/PID/network/credential denial, cancellation, bounded frames and exit/accounting proof; never rerun lost user-code attempts                                                                |
| N16 | Function platform broker/nesting; Node invocation and RPC callbacks                                   | Native protected object/filesystem/action/function broker, read evidence, effect restrictions, nested scope and cancellation                     | N01/N04/N05/N15/N21; readWrite calls retain committed effects; editProposal batch commits atomically after output/dependency checks                                                                                      |
| N17 | External outbox/delivery; absent                                                                      | Native destination-scoped dispatcher with stable delivery keys, attempt history and reconciliation                                               | N05/N14/N23; at-least-once delivery, no invented external exactly-once promise; lost acknowledgment and restore with dispatch disabled                                                                                   |
| N18 | Retention, compaction and pins; historical state retained without production collector                | Native pin-aware maintenance controller and tested engine-specific maintenance                                                                   | N06/N08/N14/N19; job/cursor/search/backup pins protect exact references; no destructive GC before recovery-set proof                                                                                                     |
| N19 | Backup/restore and HA lifecycle; no complete production controller                                    | Consistent metadata/key/reference recovery sets, incarnation/ownership rotation, dispatch-disable and effect reconciliation                      | P06/N03/N17/N18; isolated clean-host restore and actual declared failure domains; no HA claim from same-host containers                                                                                                  |
| N20 | Lifecycle/admission/telemetry; independent Node roles partially implemented                           | Native role readiness/drain, aggregate connection/CPU/memory/spill budgets, metrics and safe failure taxonomy                                    | All runtime roles; bounded queues, resource exhaustion, rolling compatibility, independent failure/replacement overlap                                                                                                   |
| N21 | Filesystem metadata/access; Node SQL authority and recursive inherited policy                         | Native shared identity/group/role/marking/placement authority and transactional mutation owner                                                   | N01/N02/N03/N07; existing directory journal, hidden-name, inherited-policy, move/copy/tombstone/replay semantics                                                                                                         |
| N22 | Repository/artifact byte processing; Node filesystem/source packaging                                 | Native bounded archive/hash/content transfer where bulk bytes currently traverse Node                                                            | N07/N21; compilation remains TypeScript; preserve immutable artifact integrity and scoped source publication                                                                                                             |
| N23 | Model gateway; bounded nonstreaming text chat only                                                    | Native protocol adapter for chat/embeddings/tools/structured/streaming with cancellation and backpressure; inference stays in an existing engine | N01/N07/N17; R04/R05 actual capability probes, current destination/data markings and protected traces; local inference for smoke tests                                                                                   |
| N24 | Performance and integration harness; validators/narrow historical kernels only                        | Independent open-loop generator, manifest/oracles/histograms/resource/cost evidence and whole-platform local topology                            | All components; all offered arrivals accounted, declared missing capabilities fail certification, real faults and retained failed trials                                                                                 |
| N25 | Versioned serving indexes; bounded UID-scope SQL operators only                                       | Typed physical projections of effective source plus editable overlays, prepared off-lock and maintained transactionally                          | N04/N05/N06/N11; revision-barrier replay, atomic source/index activation, indexed whole-population sort/filter/group plans and mixed-load proof at every supported profile                                               |
| N26 | Agent/evaluation orchestration; no complete production host                                           | Native admission/supervision and protected model/tool broker around initial LangGraph.js/evaluator artifact adapters                             | N14/N15/N16/N23; pinned releases, never-retry lost attempts, stable logical tool keys, cancellation, real adapter runs, independently recorded evaluation pass/fail/error                                                |
| N27 | Scoped MCP facade; no complete native endpoint                                                        | Native versioned capability/resource/query/action wrappers over protected APIs                                                                   | N01/N04/N05; authorization-filtered discovery, preserved actor/application/delegation and idempotency, no raw database/shell tool                                                                                        |
| N28 | Native release/configuration and instance composition; Node application bootstrap/worker defaults     | Bounded trusted release publisher and native role launch/routing, public caller/SDK compatibility and isolated local topology                    | All runtime components; published declarations provision native bindings, worker commands fail closed without native configuration, operational requests execute through native services, full fresh-instance acceptance |

Keep TypeScript for the declaration compiler, release planning, CLI, generated
SDKs, React applications and bounded administration/OIDC configuration until
measurement identifies a separate hot path. Existing PostgreSQL, Iceberg,
Parquet/Arrow, object stores, inference runtimes and distributed SQL engines are
dependencies to integrate and verify, not engines to rewrite in Rust.

## Integration checkpoint, October 31

The inventory above records the starting implementation and required end state.
Current progress is below. A component with working handlers can still have open
integration or production acceptance work; none of these rows certifies the
production workload ladder.

The latest genuine Source Chat/Embed HTTP preflight passes both cases with every
declared capability under the unchanged deadlines. Actual Source Search custody,
projection, replay, replacement and revocation pass seven cases. The v25 build
passes 77 native units and all six integration harnesses compile; its held Agent
call regression denies public status while an observation is pending and then
completes with exactly one provider call. These are separate exact-source proofs,
not one all-green platform run. See the current [implementation checkpoint](../IMPLEMENTATION.md).

The all-domain Source instance, public pending-observation audit, sparse MOR and
incremental publication are still in progress. The next million-object comparison
has fixed inputs and is running; no result or full production certification is
inferred before its oracle and load windows finish.

| Components | Implemented and exercised                                                                                                                                                                                             | Remaining completion work                                                                                                                                                                                                                 |
| ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| N01–N03    | Native typed policy/SQL authority, permanent owned UID batches, current placement and transfer fences; concurrent owners, database cuts, mutual TLS and real restore/reassignment                                     | Latest whole-instance proof; independent host/failure domains and target-load qualification                                                                                                                                               |
| N04–N06    | Protected replicas, typed projections, multi-object Actions, permanent creation, complete mapped non-primary Link cardinality, short revalidated ordinary-Action publication and numeric ordered change/replay proofs | Combined protected-query/Action production workload; sparse incremental Source publication and preserved old-reader semantics                                                                                                             |
| N07–N08    | Scoped POSIX/MinIO streaming, qualified exact-version deletion, immutable closure, typed Iceberg/Parquet publication and buffered-read revocation                                                                     | Latest combined recovery/collection proof and complete accepted external-reader allocation bounds                                                                                                                                         |
| N09–N11    | Registered native pipeline/indexing authority, three-input real OCI DuckDB transform within its original deadline, registered Sync Dataset producers with replacement writes and an exact million-object oracle       | Actual Source Connection checkpoint/HTTP/fault qualification; append/upsert/delete and incremental/appendOnly ingestion, approved Secret bindings and connector capability coverage; updated million-object and higher-tier qualification |
| N12–N13    | Transaction-aware projector/rebuild, persisted embeddings and pgvector exact retrieval with current policy before ranking                                                                                             | Latest Source readiness and restore integration; target-scale freshness/query qualification                                                                                                                                               |
| N14–N17    | Per-task leases, real OCI supervision, Function/delivery reconciliation and ordinary Agent-to-Function observation/cancellation proofs                                                                                | Source Automation and service Evaluation/Agent child factories; actual maximum-width Pipeline admission and remaining connector fault composition                                                                                         |
| N18–N19    | Clean-host metadata/content/key restore, combined expiry/exact-version cleanup/backup/two-round typed Dataset restore, permanent target custody and bounded history/projection collection                             | Sparse MOR sidecar closure/requalification and actual independent failure-domain recovery; latest whole-platform acceptance                                                                                                               |
| N20–N22    | Readiness/drain/metrics, native filesystem/Source publication, frontend integrity, real shipped React bundle and browser-origin/provider revocation proofs                                                            | Actual interactive browser and whole-instance lifecycle/fault acceptance; final quality gates                                                                                                                                             |
| N23–N27    | Model/chat/embed/stream, local inference, MCP/official SDK, exact retrieval/projector and durable Agent observation proofs                                                                                            | Public pending-observation gates across execution domains, service Evaluation/Automation composition, sustained performance/soak and resource accounting                                                                                  |
| N28        | Native default operational launcher with independent roles and real Source/Dataset/index/frontend/SDK composition                                                                                                     | Complete composed Model/Search/Agent/Evaluation/Pipeline/Automation local instance; browser verification, full gate and latest independent CI                                                                                             |

All failed timing, provider and fixture trials remain evidence. In particular,
MinIO conditional DELETE did not enforce If-Match; physical reclamation cannot
rely on that header. The three-input engine trial must meet its existing deadline;
correctly bounded frames and faster authority checks must supply the improvement.

## Shared API contract

Detailed domain contracts live under `contracts/`; generated messages are in
`services/runtime/protocol`. A declared operation is not advertised as supported
until a real handler and integration proof exist. Capability discovery names
protocol/value/schema/engine versions and supported operations explicitly.

Every internal request binds protocol version, installation/cell incarnation,
environment/resource identity, correlation/request ID and absolute deadline.
Authenticated transport establishes the service and verified session reference.
An actor ID, policy AST, SQL fragment, physical path, endpoint, credential or
`authorized=true` from request JSON is never authority. Trusted immutable binding
revisions resolve those details on the receiver. A grant is a scoped reference,
not permission to skip current checks. Public HTTP/SDK behavior remains versioned
and compatible; native services own the whole operation rather than spawning a
subprocess for each request.

Internal network control uses generated Protobuf/gRPC with mutual service
authentication. Operator-issued certificates and secret references remain outside
declarations and work records. Existing single-cell PostgreSQL authority calls
may remain direct inside trusted native processes where atomicity requires them.
Large data uses bounded Arrow IPC/chunk streams or immutable engine artifacts;
64-bit JSON inspection counters are strings. Each receiver enforces its frame,
batch, deadline, queue and resource budget before admission.

| Interface          | Required operations / atomic boundary                                                                                                                                          |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Authority/bindings | AuthenticateSession, ResolveBinding, CheckCurrent, AuthorizeResource; current policy is rechecked inside the receiving commit/read-release boundary                            |
| Identity/directory | ResolveIdentityBatch(domain, owner fence, canonical keys), InspectPlacement, TransferOwnership; stable same-key result and permanent reservations                              |
| Object serving     | ReadObject, QueryObjects, AggregateObjects, TraverseLinks; immutable binding IDs, typed values, optional minimum commit token and opaque cursor                                |
| Action authority   | InvokeAction(binding, request key, typed input, expected versions); conditions, edits/tombstones, revision, audit, receipt, change/outbox intent commit together               |
| Storage            | ReadRange(scope, object ref, offset, length); BeginWrite -> WriteChunk -> SealWrite(size, SHA256); only sealed immutable refs may be published                                 |
| Catalog            | PrepareSnapshot -> AppendClosurePage -> SealSnapshot; CommitPublication(request key, expected heads, roots, checkpoint, lease); one SQL transaction owns every grouped effect  |
| Execution          | RequestExecution, InspectExecution, CancelExecution, ClaimTask, RenewTask, RecordEvent; completion is a shared SQL/domain publication operation, not a detached acknowledgment |
| Schedules          | Create/Update/DisableAutomation, ClaimDueFirings; `(automation, scheduled UTC instant)` dedupe and next-time advancement are atomic                                            |
| Indexing           | Request/Inspect/CancelBuild, Claim/Renew/SealTask, ActivateBuild; immutable source pin, target/layout generation, owner/fence and complete partition seals                     |
| Change log         | Subscribe(after position, bounded batch), Acquire/Renew/ReleasePin; ordered `(incarnation, environment, commit revision, ordinal)` and transaction-end marker                  |
| Search             | RequestRebuild, InspectVisibility, SearchExact(query, generation/model binding, topK, minimum token); complete transaction watermark and authorization before ranking          |
| Models             | InspectCapabilities, Chat, Embed, Cancel; model/destination bindings, exact dimension/version, bounded streaming and protected dependency/trace metadata                       |
| Delivery           | ClaimDelivery, RenewDelivery, RecordAttempt, ReconcileDelivery; stable destination key, explicit uncertain outcome and no blind duplicate external effect                      |
| Lifecycle          | Readiness, Drain, InspectBudgets, AcquireRecoverySet, RestoreDisabled, ReconcileAndEnable; no dispatch during unreconciled restore                                             |

Read-only RPCs cannot replace transactional final authority checks. In particular,
an `Authorize` response followed by a later unrelated commit would introduce a
revocation race. Domain commit helpers run with the current environment/filesystem
admission locks and lease/epoch predicates inside the authority transaction.
Similarly, a worker acknowledgment cannot publish data or advance a checkpoint.

## Work ownership and sequence

The first parallel implementation wave owns the native policy kernel, native
execution/scheduling authority and native storage/closure adapter. Shared crate,
transport, schema wiring, component registry, serving integration and end-to-end
topology remain integration-owned. Each lane owns separate files; shared schema
and build files have one writer. Subsequent waves consume those tested contracts
for serving/actions, function supervision, catalog/ingestion, change/search,
model/delivery and lifecycle/retention. No lane may silently change a shared
contract or claim the dependent component complete.

The environment lock remains until P03 demonstrates a compatible replacement.
Do not remove it to make a benchmark pass. More replicas or Rust threads cannot
repair serialized commits, an unindexed history query or a provider bottleneck.
Measure them separately and record any physical redesign and recovery protocol.

## Completion ledger and evidence

Current completed predecessor: the dataset-to-object POSIX slice in PR #80,
source `56c307615ffd2271d77c32aa58478c429b20783b`, passed 890 TypeScript/integration,
five native and 16 Python cases on independent Linux CI. This is a prerequisite,
not whole-platform completion. Wave-one implementation and all remaining rows
above are open until their evidence is recorded.

The current implementation has passing real component proofs: native policy parity
(47 valid and 18 invalid cases), identity allocation/transfer and full-width keys
(eight cases), execution/functions/delivery/OCI supervision (44 combined cases),
scoped storage (25 cases), grouped catalog publication (13 cases), V2 snapshot
indexing (eight cases), mutually authenticated RPC (one case), serving across two
replicas (15 cases), filesystem metadata (seven cases), projection construction
(eight cases), and the model gateway (15 cases including combined-stream probes), protected exact retrieval (nine query cases against a trusted fixture generation), and the initial Agent host (eight cases with a real LangGraph guest). Actual local Qwen3 GPU inference passed all six chat capability probes, and Nomic embedding probes and a two-text batch passed against the pinned model digests. Exact source/schema/binary identifiers and retained raw trials
belong to each component's evidence record; these counts do not imply one final
program revision passed all checks together. The failed first execution trial is
retained alongside its successful second trial.

A later complete native search pipeline passes four cases in 24.87 seconds:
actual registered Sync Dataset publication and object indexing, a registered
projector with no ordinary service session, full projection across public row
policies, batched embedding, complete Unicode action-frame catch-up, fresh
background replay, persisted-cache replacement and real upstream socket closure
on exact projector revocation. The evidence is
`native-projection/search-projector-20261030T180833Z`; earlier failed trials remain.
This functional proof does not complete process-loss, full source/model lifecycle,
whole-instance integration or the production workload ladder.

Native function admission and callback composition, scoped storage and snapshot
attestation, typed projections, and the model gateway are implemented in Rust.
The default function worker command now executes the native supervisor. Remaining
N21 filesystem byte/source/governance operations and whole native application
composition are still open. No complete quality gate, whole-platform end-to-end,
full target capacity or HA certification has passed for this program.

Serving aggregate and inverse link operations currently require explicit bounded UID scopes and work/response/deadline budgets. Custom scalar sort also has a complete engine-sort fallback with bounded statement time, work memory, spill and output; the fallback is not indexed-performance evidence. Those guards
bound the supported operator result and authorized candidate relation; a row
budget does not bound every physical PostgreSQL page visit. N25 must provide
versioned physical indexes before claiming whole-population performance. Its
base projection is built off-lock at revision R, catches up every committed
post-R action, and switches with the source generation under the environment
barrier. Actions must maintain active effective projections in their transaction.
A stale projection cannot silently omit objects from a complete object query.

For each row retain source/contract revision, engine versions, supported
capabilities, exact commands, topology, seeds, resource budgets, raw logs and
failure point. Tests must exercise real PostgreSQL, storage, processes and engine
boundaries. Cross-runtime kernel parity supplements rather than replaces full
protected-API tests. Run the complete gate after integration changes; use the
latest-revision independent Linux job before review/merge. Repository human review
of security/publication/recovery invariants remains required.

Final local acceptance starts the declared topology from a fresh isolated state,
then tests administration/authentication, source upload and dataset publication,
native indexing, reads/pages/sorts/aggregates/links, actions and retained edits,
functions/nested effects, search and model/tool flows, revocation/deletion,
scheduled execution/delivery, cancellation, process loss/restart, pinned cursors,
retention safety and restore with dispatch disabled. An unsupported class fails
whole-platform acceptance; it is not reported as a successful empty result.

Local functional acceptance and capacity/HA certification are separate ledgers.
Actual machine capacity, independent failure domains, complete approved run
manifests and cost ceilings constrain the latter. Missing infrastructure or
decisions must stay explicit; permission to use available resources does not
establish that the requested physical capacity or failure domains exist.

## Production-target checkpoint — October 31, 2026

The corrected qualified-reader million-object trial passed the complete streaming
source/effective/UID oracle, but failed the protected HTTP implementation targets.
At 20/s, 600/600 scheduled arrivals succeeded with p95 1,238 ms. At 50/s, 798/1,500
succeeded with 702 admission rejections; at 100/s, 850/3,000 succeeded with 2,150
rejections. Its initial verifier took about 2,082 seconds, and index/projection
completed in 2,394.69 seconds. This is a failed exploratory capacity baseline,
not a production pass. Raw proof and phase/resource observations are in
`evidence/2026-10-30/native-projection/` under the qualified-million driver record.
A measured typed-primary-UID filter caused a full population scan instead of a
physical UID seek; the alias/invariant correction and verification-IO diagnosis
require fresh functional and measured evidence before the ten-million rung.
