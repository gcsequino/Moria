# Implementation checkpoint

## November 4 private-origin repeated validation

Current private credentials now traverse accepted delegation once on successful
resolution, returning the identical immutable proof only after complete execution
validation. Domain failures preserve the previous service/parent revocation error
precedence. Worker, lease, fence, incarnation, purpose and current capability
intersection remain checked; no authorization cache or deadline change is added.
A real PostgreSQL counter regression fails against the old two-traversal resolver
and passes against one traversal with exact proof equality. All 12 authority cases,
complete static checks, nine artifact fingerprints and 77 native units pass on the
frozen component source. Automation still fails child dispatch and remains under
investigation. See `production/evidence/2026-11-04/job-origin-single-traversal`.

## November 4 filesystem authority and current integration results

The filesystem authority resolver now removes eight redundant inner edge
deduplications while retaining recursive UNION cycle/duplicate elimination.
Actual graph sets, labels and required proofs agree across three dependency
geometries; the measured leaf plan falls from 20.356 ms to 2.782 ms. The complete
filesystem/Source suite passes 19/19 including duplicate paths, cycles, live deny
and marking changes. Static checks, exact nine artifacts and 77 native units pass.

The repaired four-file runtime run executes all 52 results with zero skips or
cancellations: 46 pass and six fail including parent results. Full Functions pass
18/18, including lost-COMMIT cases; Agent passes 20/21 subcases. Evaluation's
first ten-case run passes, but repeat/version comparison and service Automation
composition remain open. Pipeline setup expires during its first 16-head producer
at 14 completed heads, before Pipeline subcases. Its widths and original producer
deadline remain unchanged. The earlier exact first-group diagnostic passed; it
is insufficient qualification of the full file.

The Pipeline fixture gives its two producers separate output Datasets while
retaining 64-input/64-output admission. Agent setup now closes its database when
image acquisition fails; a real controlled acquisition failure exits promptly.
An interrupted test run and local Docker bridge recovery remain recorded. See
`production/evidence/2026-11-04/acl-query-edges` for raw results and exact identities.

## November 4 complete cold baseline and build budget

The complete cold baseline on frozen `54d839e` builds all nine release artifacts,
passes static checks and 77 native units, then fails its full functional suite:
1,196 pass, 67 fail including parent results, zero skips/cancellations. The runner
is stopped. Independent CI on `4e2b661` passes static/build/bootstrap but reaches
the outer 45-minute job watchdog before the complete suite finishes. The measured
preparation plus suite requires about an hour, so aggregate CI/hook budgets now
allow completion without changing individual test or operation deadlines.
Native artifact preparation batches the library and six harnesses in one Cargo
invocation; actual JSON selection preserves all nine executable hashes. A new
cold release timing remains unmeasured. See
`production/evidence/2026-11-04/cold-baseline` for exact source/results/failures.

## November 4 current private-session lock correction

A measured Evaluation failure came from repeated private credential issuance
requesting an exclusive execution lock behind valid child readers, then blocking
its own lease renewal/failure path. Exact immutable credential reuse now invokes
the full current origin/worker/lease/delegation checks without that exclusive
upgrade. First issuance remains fenced. The old-schema real shared-lock regression
fails; the new full job-authority suite passes 11/11 with zero skips, including
task and publication phases, expired leases, old fences and worker disablement.
Rust formatting, complete static checks, nine-artifact provenance and 77 native
units pass on the frozen rebuilt source. Broader composed proofs are in progress.
Evidence is `production/evidence/2026-11-04/job-session-reuse`.

## November 4 development feedback fixes

The Agent integration file now has three fresh fixture roots instead of one
aggregate 300-second root. All 21 original subcases retain their waits and
assertions. Its first complete run is 22/24 including roots, zero skips or
cancellations: the Automation service's Function-child admission barrier and its
parent root fail, while admission/model, child isolation/lifecycle and the other
five current-authority/service subcases pass. This is an exposed product failure,
not a reason to extend deadlines. See `production/evidence/2026-11-04/agent-fixture-split`.

The canonical Dockerfile now has a `static-check` target with no native compile
dependency. Its actual cold build and offline `check:static` pass. Independent CI
runs that job before building Rust and the full quality environment. The focused
local gate passes 478/478 cases with zero skips in 22.46 seconds.

Real focused execution exposed a Docker error-message case mismatch in cleanup.
Cleanup now requires a successful exact-name container listing to prove absence;
Docker failures still preserve owned scratch. Each run prints its controller and
scratch identifiers before launch. Real exit-zero and exit-seven lifecycle
smokes preserve the exact exit code and leave no owned pools or temporary paths.

The authoritative checkout is now persistent Linux storage. Development uses
`check:static`, `test:local` and explicit-file `test:integration` gates before the
complete `check`; final checks still include every case with original deadlines.
The same fresh-pool Docker runner accepts focused commands and retains its
ownership/cleanup checks. The starter is now included in normal SDK generation
and SDK drift checking, and its generated output is current.

Full static validation, all 77 native units and two test-selection regression
cases pass in the exact offline image. The first selector check caught and
corrected an implicit TypeScript parameter type; both runs are retained under
`production/evidence/2026-11-04/development-gates`. Full integration remains open.

Workload v0.2 approval validation now accepts approved implementation targets
only with consistent status, implementation scope and a real calendar date.
Historical proposed v0.2 remains readable; v0.1 remains historical. Capability
results propagate target approval without enabling certification or production
claims. Black, all 17 workload/value Python cases and the six-mutation CLI
self-test pass in the offline test environment.

## November 4 remote checkpoint

This checkpoint preserves the latest recoverable shared implementation and its
recorded test evidence. The temporary Linux checkout no longer exists. Source
was recovered from the locally retained `moria-test:gate-v27` image
(`sha256:aa5426fb38bab92ecbbc15cc0dcb81557cacc3b37b9f2ff2bc87ca9feb36955f`),
which contains the newer October 31 source and includes every file in the Windows
review mirror. The mirror's twelve intentional deletions of the superseded
`services/indexing` Rust crate were preserved; that runtime now lives in
`services/runtime`. Recovery uses a persistent Linux checkout.

This is a work-in-progress implementation checkpoint, not a release or production
capacity certification. The previously recorded 77 native units and 55-case
functional slice passed on their frozen sources. Full Agent/Evaluation/Automation
composition, Connector credentials and checkpoints, sparse Dataset MOR and
incremental Source/index publication, a complete inspectable local instance and
end-to-end qualification remain unfinished. Private experiments outside the
preserved shared source are not included. No terminal result from the interrupted
whole-program gate is available; its success must not be inferred. The full
production workload ladder and latest independent CI remain required.

Checkpoint verification reran offline in the preserved image: formatting,
architecture, SDK drift, TypeScript, exact native artifacts and all 77 native
unit tests pass; both React builds pass. The Python workload validator rejects
the approved implementation-target metadata because its schema still requires
the old proposed/unapproved form. The starter check also detects a stale example
SDK. The complete integration suite was not rerun for this preservation push.
See `production/evidence/2026-11-04/checkpoint-validation` for commands, failures
and recovery provenance.

## October 31 latest native qualification

The v26b exact-source build passes full TypeScript, architecture/SDK checks, Rust formatting, 77 native units, all six harness builds and the eight-file 55/55 functional slice with zero skips. It includes strict pending-observation public gates, typed live-parent child progress, Function-only immutable foreign keys and Search candidate fairness. A separate real held-child proof passes three selected tests: no early log/Action/Model/public release, and the parent/child publication barrier completes without deadlock with one receipt each. Full Agent and Evaluation files remain in progress; the latter has an actual RESOURCE_BUSY/controller failure under investigation.

The forty-file non-integration TypeScript run passes 474/476 with zero skips. An external read-only dependency mount prevented Vite temporary writes; the corrected cache mount passes the app-host cases. The remaining obsolete compatibility fixture initialized BrowserSessions with an empty origin; it now binds the actual listener before origin reservation and credential issuance, and its three-case rerun passes. These are separate explicit runs, not one combined all-green gate.

The v10 fixed million-object comparison passes its exact count/sum/xor oracle. Its unchanged short windows deliver 600/600 at 20/s (successful p95 56.806ms), 1500/1500 at 50/s (63.524ms) and 2962/3000 at 100/s (464.074ms, p99 955.705ms). The last window still fails: 38 RESOURCE_LIMIT/429 responses (1.267%). Every failure occurs in its final 0.51 seconds with 64 prior client requests outstanding; request admission saturation is an inference, without exact internal rejection-site telemetry. Native preparation takes 1874.83 seconds under the unchanged four-CPU/12GiB envelope. See `production/evidence/2026-10-31/native-projection/qualified-seek-million-v10-20261031T124948Z`; this is not the full production ladder.

The latest v26b all-domain HTTP provisioning attempt times out its aggregate Chat probe while each completed provider call remains within the original per-call deadline. The failed trial is retained. A fresh retry uses the existing ordinary-authenticated CLI full-probe provisioning path, with all actual API/worker operations still native HTTP; backend and browser readiness are not yet qualified.

The immutable v20 Source Secret/Models/Dataset/Search/MCP/recovery slice passes 76 native units and 53/53 integration tests. The v22 and v23 slices each pass 77 native units and 54/54 integration tests with zero skips. Their exact sources, derived/compiled schemas, commands and hashes are recorded under `production/evidence/2026-10-31/native-integration/20261031T122345Z`, `20261031T132112Z` and `20261031T134056Z`. The earlier failures below remain historical evidence, not current qualification.

Actual Source-aware Search credential integration additionally passes 7/7 with zero skips in 150.68s: real published Model/Provider/Policy/Secret declarations, explicit operator custody, independent service file/use grants, immutable accepted approval reference, disable/reenable fencing before provider I/O, fresh projection, Action replay, embedding reuse and Model Source replacement. See `production/evidence/2026-10-31/native-search-secrets/20261031T141335Z`. The fixture uses explicitly qualified Source markings; the capacity fixture's default markings, rates and budgets remain unchanged.

Actual Agent Model/tool and credential-generation cases now pass independently on v23, including protected Object reads, committed tool effects, private material classification, missed approval pulses and fresh acceptance. An independently polled Model guard fixes a reproduced asynchronous execution-lock self-wait; task fences now precede private-session readers in completion paths. Full Agent/Evaluation/Automation qualification remains open. The latest pending-call diagnostic caught a public Agent status response while observation state was still pending; its 409 assertion remains strict. Evaluation also requires publication-conflict retry and service-lifecycle proofs. Failed trials are retained in the execution ledger.

The new source-declaration batch preserves current/pinned file, descriptor, classification and scalar filesystem permission checks. Actual PostgreSQL tests pass 18/18; a two-resource diagnostic reduces two authority/ACL passes to one, without an authority cache. The v24 snapshot includes this batch, exact task-fence ordering, bounded active HTTP handler idle handling, class-before-origin Model admission and only definite 40001/40P01 Function publication retries. Its combined integration run passes 53/55 with zero skips: an exact-purpose revocation subtest and its parent fail on guard-error precedence. A separately archived rerun passes 7/7 while retaining actual socket-abort and unchanged-visibility assertions. The genuine Source Chat/Embed HTTP composition passes 2/2 with every declared capability under unchanged deadlines. The v25 strict pending Agent read passes its actual held-call regression, and 77 native units plus all six harness builds pass. Full composed qualification remains open.

The v9 million-object run passes the exact full-row oracle. Its unchanged short offered load yields 600/600 successes at 20/s (p95 all 127.39ms), 1492/1500 at 50/s and 1583/3000 at 100/s. Higher rates still fail. A subsequent protected native plan proves a real UID range seek after removing redundant final-WHERE BooleanTest wrappers; its 43-case current serving/Source proof passes. The next million-object comparison has an immutable release build and 1k preflight, but has not run. These remain exploratory windows rather than the full production ladder.

Sparse Dataset MOR/qualified key visibility and incremental Source/index publication remain private development and are not activated or qualified. Connector checkpoints, secret callbacks/multi-slot mappings and all accepted capabilities still need actual composed proofs. The final full local gate, inspectable whole instance and latest independent CI remain required before publication/merge.

The next shared work includes a worker-local ordered Search candidate scan so an unauthorized or unready collection cannot terminate scheduling for independent ready collections. Its real regression places an ungranted earlier collection before a ready replay, requires the direct public request to remain forbidden, and checks no execution is created for the blocked collection. This change is not qualified by the earlier seven-case executable.

The complete local/CI gate now stages separate disposable ext4 pools for Pipeline and whole-instance databases, preserving incarnation markers, and limits test-file concurrency to two without changing in-test concurrency assertions or deadlines. Agent and engine fixture dependencies move to locked test-image preparation; fixture builds disable networking and reject stale package/requirements locks. The refreshed image, complete gate and owned-resource cleanup remain unexecuted while the isolated million-object run owns the proof resources.

## October 31 integration and scale corrections in progress

The combined002 execution snapshot passes 72 library tests. The real eight-file Model,
Search/projector, MCP, history and clean-host recovery regression completed
51/53 tests in 154.98 seconds; its only failing subcase (and enclosing test) was
an old MCP expectation of 403 for a disabled principal. Revocation now destroys
the credential and returns 401. The corrected MCP test, including an off/on pulse
that cannot revive that credential, passes 8/8 tests with zero skips in 8.535
seconds on the same exact executable/schema. These are two explicit runs, not
one all-green combined result. Evidence is
`production/evidence/2026-10-31/native-protocols/20261031T102405Z` and
`production/evidence/2026-10-31/native-mcp/20261031T103243Z`.
Successful Model receipts clear uncertainty; canceled calls reject new provider
attempts. Actual held provider work also terminates on worker-scope revocation,
with failed call bookkeeping; the broader execution suite still has open
Automation/Evaluation composition failures under active correction.

The combined Dataset expiry/cleanup/backup/clean-host restore case now passes
with zero skips in 61.913 seconds
(`production/evidence/2026-10-31/native-dataset-retention/20261031T102121Z-external.json`).
It checks exact old-version removal with a newer external version preserved,
current typed rows, backup after source cleanup, old/current typed snapshot
readback after actual cell reassignment/fresh identity, a new finite policy with
complete current-incarnation/physical-fingerprint root/member qualification, and a different restore
reusing the restored S3 prefix denied with dispatch disabled. It uses the frozen
custody/pruning v7 runtime plus explicitly archived external test sources; the
latest whole-program gate remains pending.

The second complete million-object comparison retained the exact source/effective/
UID oracle. At 20/s, 600/600 arrivals succeeded with p95 367.782 ms and p99
492.262 ms; at 50/s, 963/1,500 succeeded with 537 admission rejections; at 100/s,
985/3,000 succeeded with 2,015 rejections. Its 2,694.555-second total included
747.035 seconds of publication and 1,662.263 seconds of indexing/projection.
These are short exploratory windows, not the full accepted production profile.
The busy/peak failures remain failures. See
`production/evidence/2026-10-31/native-projection/window-uid-million-v7-20261031T073753Z`.
The next immutable comparison tests bounded Parquet-header coalescing. A real
24-header microcase reduced provider range requests from 24 to 2 and SQL storage/
verifier checks from 144 each to 14 each, while preserving current checks before
and after the header phase.

Operational history now has a three-case expanded PostgreSQL proof with zero
skips: retired-generation DROP waits for the actual recovery admission lock,
current/last-two and retained cursor/read roots remain protected, Source and Link
history prune without floor rewind, and ordinary runtime-role policy mutation is
denied (`production/evidence/2026-10-31/native-history/branches-external-20261031T074802Z`).
Real Dataset multipart cleanup passes all three provider-fault branches in one
47-second case: lost part acknowledgement, lost initiation acknowledgement, and
a late initiation after the first absent-object collection. Exact sibling upload
IDs survive; current typed rows remain readable. A separate 26-second case holds
an actual old-reader task across metadata pruning, keeps its old root readable,
and verifies a repeat creates no extra head. Combined exact-version cleanup and
clean-host Dataset recovery remain pending the latest-source run.

Default native API/Source/frontend integration passes nine cases with zero skips
in 76.75 seconds, including the shipped React bundle, immutable browser-origin
reservation, private bootstrap/CSP, inert uploaded HTML/SVG headers and revocation.
Provider/browser-session revocation passes three additional cases in 28.7 seconds,
including a disable/re-enable pulse and restart. Actual interactive browser
acceptance and final combined Evaluation/Automation/service composition are still
open. An omitted trusted scratch/closure configuration correctly blocked the
first interactive fixture before it exposed browser recovery state; its startup
configuration is being made explicit.

The production workload profile is the implementation target. The million-object
run retained its exact 1,000,000-row/UID oracle but failed latency and overload
acceptance; no larger-run capacity or production certification follows from it.
The physical primary UID now has protected HTTP range/equality/order oracles and
an actual indexed plan. A measured 128-byte-read microcase reduced provider reads
from 128 to 1 and SQL I/O checks from 768 to 6, with 5,266 ms versus 50 ms. Current
source revocation still denies buffered reads. The complete repeated offered-load
workload remains pending.

Real Source-installed versioned-S3 Dataset writers and typed array readback pass
(`native-dataset-retention/20261031T071409Z-external.json`, one case, zero skips).
The expanded expiry/cleanup fixture caught ordinary runtime-role retention
self-approval. Operator entry points and SQL mutation controls now distinguish the
actual operator session from ordinary runtime credentials. The new rejection,
closed-history expiration, first-observed 24-hour grace, exact-version deletion
and recovery-after-source-cleanup tests require their combined current proof.
Historical-age imports are explicitly synthetic closed metadata; no active lease
is aged or manufactured.

Model receipts now retain their DB-derived private execution origin and recheck
current source evidence, input ancestry, markings and destination restrictions on
replay/inspection/completion reconciliation. The new shared proof exposed a native
HTTP future stack overflow in a real Source upload; the source-proof boundary is
now heap allocated. Compilation alone did not establish this integration fix;
fresh actual Source/Agent/Search regression testing is required. Earlier failing
snapshots and raw output remain retained.

## October 30 native platform program in progress

The current native MCP endpoint passes eight real integration cases with zero
skips in 7.19 seconds, including official Python SDK 2.3.0 negotiation, two-replica
discovery/cursors, protected query/action results, idempotent action replay,
transport tampering and live revocation. Evidence is
`native-models/20261030T211807Z`. Native directory/HTTP admission additionally
passed the 24-case combined authority/model fixture and actual mutual-TLS
directory RPC transfer proof (`20261030T194142Z` and
`native-projection/directory-rpc-20261030T194311Z`). These are functional proofs,
not production throughput or independent-failure-domain certification.

The permanent indexing service now passes a 14-case real managed Source/Dataset
suite. It verifies mapped raw/effective rows, unrelated environment and instance
epoch refresh through fresh immutable metadata, tightening and broadening policy
behavior, classified-source publication with blocked stale indexing authority,
and accepted maximum-width rows. The two wide rows require 8,388,678 bytes each
for raw plus mapped payload; the normal 8 MiB batch and existing single-row
16 MiB allowance remain unchanged. Exact UID/typed projection and protected
pagination pass. Native Evaluation separately passed seven actual cases,
including score oracles, cancellation and root COMMIT acknowledgment loss
without rerunning user code. Retained failed trials remain part of the evidence.

Native Arrow IPC and the engine I/O broker are implemented with metadata/buffer
preflight before decoding, continuous chunked typed streams and private grouped
output roots. Hard spill reservations verify dedicated ext4 filesystems and
persistent owner markers; uncertain creation/cleanup keeps a slot quarantined.
Actual whole OCI engine integration, complete create/link semantics, recovery
content closure, retention, fresh-instance acceptance and production workload
qualification remain active work. Recovery currently has fenced maintenance
leases and a native exported-snapshot PostgreSQL dump path; no recovery-set or
restore completion is claimed from those pieces alone.

Current follow-up checks found two concrete issues. Arrow IPC compaction was
charging shared encoded backing repeatedly across columns; it now charges the
backing once and retains the original schema. Real unit cases include a narrow
slice of a 2 MiB allocation and a four-column 2 KiB row. The unchanged 120-second
three-input engine fixture now transfers all 18,000 input rows and writes its
6,000 output rows, but still misses completion at output sealing; repeated current
authority checks are being measured and factored within each call.

Source creation, managed-Link frames/cardinality rollback, complete reverse
fanout denial, explicit source attachment and subsequent native import have a
real two-replica integration proof. The native create survives an actual empty
Dataset replacement and indexing/projection refresh. Later raw data loss blocks
invalid clearOverride/refresh while preserving the prior valid generation.
Global mapped non-primary-key one-to-one constraints subsequently passed a real
Source redeploy and same-head native indexing proof (19 cases, zero skips). It
verifies pending readiness, complete key population, duplicate creation rollback,
deletion freeing a key and conflicting restoration denial. Later child-observation
and application-host composition changes still need their combined proof.

Recovery's combined fixture now passes 20 tests with zero skips in 35.72 seconds
(`native-recovery/20261030T235343Z`, source digest
`8e51edeeeb9696b9da7bc98c521f32edeb4097a41e8b50230b28b1b8304df73e`).
POSIX and MinIO both capture a consistent PostgreSQL archive larger than 24 MiB
in bounded parts plus a real 2 MiB filesystem object. Clean-host restore verifies
metadata/content/keys before one dispatch approval transaction, rotates the
incarnation, fences old credentials and workers, requires new CLI-key approval,
reassigns paused cells/domains and reconciles exact retries. Tampered manifests
and missing keys stay closed. Key custody checks the actual configured session
key and its physical versions. Normal runtime database roles stay non-owning;
only the explicit recovery connection uses operator schema admission to preserve
owners and ACLs. Earlier failed trials are retained. Managed typed Dataset and
historical Iceberg readback are the next recovery integration gate, not implied
by filesystem recovery.

The optimized million-object trial exposed a projection-page regression after
adding Action-created object union semantics. Its actual custom/generic plans
walked 975,000 remaining rows and performed as many overlay lookups to select
1,000 rows, taking about 14 seconds per page. The original incomplete trial and
database are retained; it has no successful oracle or capacity claim. A bounded
key-only per-branch TOPK plan measured 13–17 ms on the same population and is
being tested on a fresh pipeline. Publication itself completed in 645 seconds;
full indexing/projection/HTTP workload evidence still remains open.

Qualified versioned MinIO storage passes 29 actual cases with zero skips,
including exact old-version reads/deletion after overwrite and survival of the
newer version. This qualifies that provider primitive. It does not enable
unversioned/POSIX removal or complete retention/garbage collection. The native
retention controller starts with explicit issued-object custody, retain-forever
by default and separately approved policies; general Dataset/history expiration
requires complete publication/pin admission and remains gated.

The production-scale v0.2 profiles were selected as implementation targets on
October 30: 1m/10m/50m objects with a 100m repeat and their specified request
rates, latency, freshness and recovery requirements. This is target selection;
capacity, cost and failure-domain certification still require actual evidence.

The [component inventory and API boundaries](production/RUST-PLATFORM-PROGRAM.md)
catalog N01–N28, including versioned physical serving indexes as a separate
requirement from bounded query correctness. The shared native crate now lives in
`services/runtime`; `moria-indexing` remains compatible and `moria-runtime` hosts
independent roles. Fresh schema version 5 is required. This branch has no live
migration or deployment, and the whole-platform acceptance gate remains open.

Real component integration proofs currently cover policy/SQL parity, concurrent
identity ownership, execution/delivery, protected two-replica serving, immutable
POSIX/S3 transfer, complete retained Iceberg closure verification, grouped
snapshot publication, native snapshot indexing and OCI supervision. S3 cases
include multipart invisibility, server-committed lost acknowledgments and guarded
recovery. Concurrent indexing now shares a durable exact verification attestation
and lazily resolves approved file descriptors instead of rescanning the entire
closure per task. Per-transfer authority and physical-version checks remain.

Native model protocol tests cover chat, ordered streaming, embeddings, schema and
tool validation, stable call replay and in-flight revocation. Configuration alone
does not establish capabilities: persisted, binding/endpoint-specific probes gate
new calls. Real local inference runs separately from deterministic HTTP fixtures.
The test image includes actual pgvector 0.8.7 for PostgreSQL 18; a prerequisite
test executes its exact cosine operator. That engine test does not complete
protected retrieval or certify its performance.

Source-generation changes and overlay versions now capture complete transactional
replay frames, exact version references and end markers. Rollback/process-loss
and monotonic replay-pin proofs pass. Binding-change composition, search,
retention and the snapshot/log handoff still require integration.

Native public function admission, immutable descriptor replay, nested/proposal
brokers and the default function worker cutover pass 44 combined execution,
delivery, function and real OCI cases. The latest trial includes filesystem callback
composition; each raw trial retains its own exact compiled metadata. Native
filesystem metadata passes seven cases and two-replica serving passes 15. Typed
projection construction passes eight core cases; its real protected HTTP and
capacity paths are being verified separately.

Permanent identity allocation now retains full 64 KiB keys outside PostgreSQL
B-tree tuples, indexes a generated SHA-256 key, and compares exact original keys
on conflicts. Eight real cases cover legacy/native width, concurrent allocation,
ownership transfer and lost acknowledgments. Immutable reservations reject
update/deletion. No accepted key-width bound was reduced.

The model gateway passes 15 real integration cases, including independent
stream-tools/structured probes, protected extensions, one-copy embedding batches,
shared class budgets, cross-process cancellation and uncertainty retention. An
actual local GPU run on pinned Qwen3/Nomic digests passes six chat capability
probes plus embeddings in 23.06 seconds; this is functional evidence, not an SLO.
Raw reports are retained under `docs/benchmarks` and
`docs/production/evidence/2026-10-30/native-models`.

Protected exact-query retrieval passes ten real pgvector/native-gateway cases
with classified immutable embedding cache entries, policy checks before LIMIT,
current source replacement/deletion, strict model/visibility pins and revocation.
The registered-service projector additionally passes six real pipeline cases:
64-object Dataset publication/indexing, batched embeddings, Unicode action
catch-up, later background frame replay, cache-preserving replacement, purpose
revocation that closes the actual provider socket, a real 30-second worker lease
expiry and higher-fence takeover, and a model change that builds a separate
complete generation. The six-case trial took 67.30 seconds; raw evidence and its
immutable source are in `native-projection/search-projector-20261030T183516Z`.
The preceding shared-source indexing/model/search run passed 43 cases with zero
skips in 26.47 seconds (`native-search-models-indexing-joint-20261030T181459Z`).
Complete Dataset refresh, recovery, whole-instance and production R05 gates remain
open. The Unicode/paragraph chunker has native coverage.

The native Agent host and real locked LangGraph guest pass thirteen actual cases,
including protected model/tool flow, held-provider lease revocation and inspection
of a durably accepted model intent before its provider receipt exists. Native
Evaluation and the ingestion/analytics adapters remain under integration testing.
Native source publication, CLI publication, qualified POSIX/S3 bytes and filesystem
lease recovery have independent functional proofs; browser publication and final
instance composition remain in progress.

The release-mode million-object fixture passed its complete one-million by 4 KiB
Dataset publication, distributed index, typed projection and streaming identity,
source and effective-value oracle in 27 minutes 51.484 seconds. Its immutable
snapshot predates the within-query authorization factoring. Publication took
847.492 seconds and indexing/projection took 642.776 seconds; the reducer plan
exposed excessive task-first reads, so the build/partition/UID access path is
being corrected before larger runs. Raw logs, plans, complete oracles and resource
samples are retained in `native-projection/one-million-release-trial002`.
This is functional correctness evidence at one million objects. It does not
establish mixed HTTP rates/latencies, retained history, production topology, HA,
cost or soak requirements. The approved workload figures are implementation
targets; production capacity is not certified.

The first actual HTTP admission trial passed 21 integration cases with zero skips
in 19.46 seconds, including real pending request bodies at the shared 64,
model 32 and compiler 4 limits, plus all seventeen model protocol cases. Native
unit coverage verifies the 256-connection listener and permit ownership through
response body chunks, EOF and cancellation. The raw trial is
`native-models/20261030T190247Z`; browser/local CLI authentication is being added
and rechecked. Dedicated hard-bounded ext4 spill filesystems are provisioned for
real engine integration; terminal container cleanup and uncertain create outcomes
must be proved before their slots may be reused.

Retention/recovery and complete native instance composition remain in progress.
The full `pnpm check` gate and independent latest-source Linux CI are required
before human invariant review and merge.

The later model protocol proof passes 17/17 cases with no skips, including a
permanently held COMMIT acknowledgment under the configured five-second call
deadline, receipt reconciliation and lock release. An immutable-source local GPU
run (`native-model-smoke-gpu-20261030T164649Z`) also passes the native Ollama
no-truncation adapter and explicit oversized-context rejection. These are
functional protocol results; no inference throughput or quality target is claimed.

The real wide-row indexing regression passes 80 one-megabyte source objects,
complete source/effective checksums and physical typed-index checks in 39.97
seconds. PostgreSQL bounds each reducer response before sending JSON; the actual
single partition uses 12 batches with a largest payload of 7,340,137 bytes.
Raw source, commands, schemas, checksums and query plans are retained under
`native-projection/wide-reducer-20261030T164824Z`. This width regression does not
certify large-population throughput. The earlier million-object trial failed
before activation at the caller credential's ordinary expiry; it remains failed
evidence pending the private accepted-job/release-build rerun.

## October 30 Rust dataset-to-object indexing

Implemented the first execution slice of the indexing protocol in
`services/indexing`. Independent Rust processes claim PostgreSQL scan/reduce
tasks with renewable leases and fences. Dataset publication captures immutable
build intent in the same transaction. Native Iceberg/Arrow reads, exact value
normalization, global UID checks and unpublished generation loading run outside
the environment publication lock. Both runtimes now share the SQL activation
function; activation retains edits, verifies current authority and atomically
records the generation, receipt and change marker.

Production API composition defaults to distributed indexing and reports saved
versus served source revisions. Node executes no distributed indexing tasks.
Trusted binding registration rechecks permissions and schema compatibility;
explicit retry reuses pinned inputs, and cancellation cannot revoke a committed
generation. Attempt history survives executor replacement and retry. Schema
version 4 and its exact checksum are required on a fresh isolated database;
there is no automatic version-3 upgrade or live migration.

The reader supports immutable shared POSIX, unpartitioned copy-on-write Iceberg
v2 and same-name declared columns. Unsupported delete files, partition specs,
name mappings and direct S3 reads fail closed. Protobuf scan plans pin the reader
ABI; control and claims use PostgreSQL directly, with no gRPC listener yet.
Search projection, separate-host recovery, retention/restore operations and the
complete IDX-01–IDX-12 workload/cost ladder remain open. This functional slice
does not certify million-object throughput or remove the environment commit
capacity boundary. See `services/indexing/README.md` for operator prerequisites
and explicit bounds.

Verification uses actual Rust children, PostgreSQL and PyIceberg files, including
concurrent scans, paused-owner takeover, lost COMMIT acknowledgment, duplicate
UID rejection, retry after file-access failure, cancellation, corrupt protocol
data, schema-lease loss, authority revocation and generic-import recovery. Native
tests include all 100 shared value vectors and kernel-enforced path isolation.
`pnpm check:indexing` rejects stale compiled artifacts and zero native cases.
The pinned test image builds the native runtime/test executables; the full Linux
quality gate and revision-specific hosted CI results are recorded in the PR.
An initial complete run exposed a schema-lease fixture race: pausing after a claim
did not establish that a connection was checked out. The test now waits at a real
PostgreSQL relation-lock barrier before terminating the lifetime lease. That run
also exhausted two existing filesystem/function fixture deadlines under host-wide
parallelism. Failed evidence is retained; the repeated local gate uses two CPU
cores, matching the bounded CI runner, with every assertion and deadline intact.
The first hosted run hit its 30-minute job envelope after a 9m41s cold image build
and a still-progressing full gate; its native indexing cases passed and no test
failure was reported before cancellation. The workflow envelope is now 45 minutes
to include native compilation. Test and production deadlines remain unchanged.
No live installation was migrated or deployed. Human review of authorization,
publication and recovery invariants is required before merge.

## October 30 distributed indexing protocol

Recorded [the Rust indexing protocol](production/INDEXING-PROTOCOL.md) and
linked it from production architecture and acceptance. Settled durable intent
capture, immutable build inputs, partition leases/fences, atomic generation
activation, snapshot/replay handoff, search visibility, current permission
checks and retention. Existing environment transaction ordering is retained;
parallel processing is not claimed to remove that capacity boundary.

This is documentation only. No temporary Node runtime, Rust service, schema
migration, dependency, queue, deployment or capacity result is introduced.
IDX-01–IDX-12 are required future experiments, not passed checks. Design
verification checks local links, protocol structure and whitespace; existing
runtime tests are not rerun for this documentation-only working change.

## October 29 local and published source reconciliation

Reconciled the filesystem, TypeScript function and value-model source with main
`a24ea469676aef2daafb04c5a57dd9119fd3cf75` and the protected-read, HTTP and Python
lifecycle stack through `5c112abb7f4f717853049464281503655070e32e`. Local feature
contracts take precedence; the published management-only default, independent
service roles, scoped snapshot publication and durable recovery are retained.
The original local source is preserved separately before reconciliation.

Shared read admission now supports function-owned transactions without exposing
object stores or authority, and it performs the final session check for both
point reads and queries. Restricted composition preserves the function runtime.
Tests that previously mutated a live object binding now install a new trusted
binding; existing denial, marking and result-evidence assertions are retained.

All HTTP listeners, including the private filesystem broker, use shared transport
admission. Only the exact bounded binary file-upload routes admit bodies larger
than the ordinary 64 KiB JSON limit. Function retry/error metadata is preserved. Browser-enabled management hosts also
admit explicit non-browser bearer calls to filesystem/function routes; valid
workload sessions are authenticated by the handler. Cookie or browser requests
and unrelated routes retain same-origin enforcement. Real authenticated HTTP and
transport-profile regressions fail before this correction and pass afterward.
Generated management SDKs accept the registered workload kinds returned by the
generic execution API. Default startup still creates no Snow/weather workload.

Iceberg staging, streaming, scoped reads and complete-snapshot validation share
the value-model codecs while retaining bounded Python lifecycle ownership and
redacted process outcomes. Publication checks current filesystem read/write
authority and rejects outputs missing inherited input classifications. Real
PostgreSQL/local/S3 tests cover the latter before any object write, and recovery
provenance explicitly retains its classifications.

Source ownership checks register the existing CLI Git adapter, Docker supervisor,
filesystem services and isolated function-image entrypoint with their contract
tests. Unregistered loaders, persistence access, listeners and process primitives
remain rejected. The function image is the only computed code-import entrypoint;
it has no host network, daemon socket or host credentials.

Verification before publication: 137 targeted PostgreSQL/local/S3 publication,
process-lifecycle, static-host and value-system regressions passed, with zero
failures or skips. The bearer regressions first failed on the uncorrected source,
then all 17 authenticated filesystem/transport tests passed. The actual Dockerfile
build and the workflow's repeated bootstrap passed. Commands include
`pnpm exec tsx --test` against the affected suites, `pnpm typecheck`, and the exact
shell steps in `.github/workflows/quality.yml`. The complete `pnpm check` and
latest-revision hosted Linux job remain required acceptance gates before merge;
the PR records their final revision-specific results. Earlier failed runs and
the bearer red/green proof are retained as diagnostic evidence. A subsequent
875-test workflow run exposed a test setup race: its five-second deadline expired
while consuming setup rows before the intended post-exit authority barrier. The
test now holds only its configured read timers until actual SQL contention is
observed, then advances them and requires cleanup within one real second while
the lock remains held. A subsequent nominally successful run reported only 852
cases: the durable-reference recovery module emitted an opaque file-level pass
instead of its 24 cases. That run is incomplete evidence. The normal test command
now rejects this outcome through a small standard spec reporter adapter; a real
zero-exit fixture proves the guard fails and a real executed fixture proves normal
reporting still works. No database test is skipped or replaced. Production limits are unchanged; the separate real
one-millisecond expiry check and all real process/SQL assertions remain. Synthetic
test fixtures only; no live installation, deployment, schema migration or main
branch update is part of this work. The release requires a fresh schema version 3
database; older databases continue to fail without automatic mutation. A passing
combined gate does not establish the cause of historical issue #67 or production
capacity, and human invariant review remains required before merge.

### Hosted Docker compatibility correction

Head `84b442e` passed all 877 runtime/16 Python tests locally but failed the hosted
function-container suite (864/877 passed; 13 failed, including the enclosing
suite). The CPU stats request pinned API 1.51 even though the Docker CLI negotiates
the host's API. A real-daemon regression rejects newer API requests and serves
actual stats through an older API; it reproduced FUNCTION_WORKER_UNAVAILABLE before
the correction. The supervisor now uses the daemon's default API endpoint, with
the same bounded response, status checks, timeout and fail-closed CPU enforcement.
The regression requires a real successful invocation and an actual CPU-limit
rejection through that older stats API. CI records client/daemon versions for
environment evidence. No resource limit or isolation assertion is relaxed.
Exact-head complete local/hosted results are recorded in the PR.

## Remaining component work

- Production function infrastructure: implement the planned Firecracker adapter
  and validate fleet ownership, cleanup routing and capacity separately. Docker
  remains the backend for the current function release. Additional function
  languages are separate from this release.
- Independent latest-revision Linux CI and human review remain required before
  merge. The approved test-daemon wiring is now configured below.

Functions and universal value model v1 are implemented and locally accepted.
Current implementation and verification are recorded below. Historical
entries retain the evidence and limitations that applied when they were written.

## October 29 ontology object values in functions

Status: implementation and local acceptance complete.

Implemented the shared `ontologyObject` envelope type, keyed by immutable type
UUID with UID-only identity transport. TypeScript and Python codecs, shared
conformance cases and compatibility checks cover nullable/nested/list values.
Generated ontology names (`Customer`, `Order`, etc.) denote handles; fetched
records use `CustomerProperties`/`OrderProperties`. There is no public `Ref` suffix.
SDK object factories construct handles without reading data, `get` explicitly
loads projected properties, and transport strips the handle to its identity.
Named endpoint results bind through the matching generated SDK. Built-in
ontology IDs are reserved constants shared by server and generated contracts;
custom bindings use their registered ObjectType UUIDs.

Function acceptance checks reference type, existence and current row access,
then retains markings, read grants and identity evidence in the acceptance
transaction. Output references receive equivalent checks before success or a
proposal commit. The runner hydrates typed handles through the exported OSDK
factory, awaits the result and encodes identities before JSON. UUID object routes
resolve only the matching trusted type binding. Named serving routes remain
available; their authority and property-policy checks are unchanged. Empty
projections permit identity checks without accidentally reading hidden fields.

Focused tests cover revoked access, hidden rows/properties, malformed and
cross-environment identities, unknown/mismatched types, duplicate lists, output
authorization, type rebinding and deletion. The real Docker suite hydrates
generated ontology types, reads their authorized properties and awaits promised
object/list results, retaining only identities in invocation input/output. The
complete Linux `pnpm check` passed: 522 runtime tests and all 16 Python tests,
zero failures/cancellations/skips, formatting, SDK drift, TypeScript including
nominal object-type negative cases, specification checks, both app builds and
starter verification. Full output is retained in
`.review-tools/function-object-values-check.log`. No test sandbox or registry
remains; the pre-existing baseline containers are unchanged. Hosted latest-revision
CI, publication and production-scale operation were not performed.

A regression also exposed transient inspection contention during nested action
proposals; bounded synchronous waits now retry that read-side contention within
their existing deadline, without repeating function code or mutations. The
contention regression and actual nested action commits/rollback passed in the
complete gate. No dependency, migration, deployment or external publication was
added. Regenerate custom SDKs to supply type UUIDs and use the generated property
record names where application code needs fetched snapshots.

## October 29 approved CI Docker wiring

The Linux quality job preloads the exact Node and registry digests used by the
function integration fixture, then mounts the ephemeral runner's Docker socket
only into the non-root offline quality container. A supplemental group matches
the socket's actual host GID. Bootstrap retains no daemon access; user function
sandboxes retain their existing isolation and receive no host socket. Checkout
credentials remain unpersisted and workflow permissions remain read-only.

Local verification built a fresh image tag using the actual
`deploy/test/Dockerfile` and final workflow. Parsed workflow commands passed Bash syntax checks, and
the image pins matched the integration fixture. The actual workflow commands
then passed repeatable bootstrap twice, prerequisite image pulls, and the
complete offline quality hook: 511 runtime tests and 16 Python tests, zero
failures/cancellations/skips, formatting, SDK drift, TypeScript, schema validation,
both app builds and starter verification. Runtime inspection confirmed user
`node`, network `none`, no privileged mode, the socket's numeric supplemental
group and exactly one bind mount for the test socket. Real function isolation,
resource, cancellation and crash tests passed through that supervisor.

Build and gate logs are retained in `.review-tools/ci-docker-wiring-build.log`
and `.review-tools/ci-docker-wiring-check.log`. The temporary CI image and all
test containers were removed; only the pre-existing baseline containers remain.
A hosted GitHub Actions run is not claimed; changes remain local and uncommitted.

## October 29 function audit and corrections

Audited the declared single-host, four-active-invocation release: API and runner
inputs, identity/marking flow, action and filesystem commits, durable call keys,
leases/deadlines, container accounting and cleanup, CLI and SDK consumers.
Four concrete findings were corrected before starting the value-system milestone:

1. Missing-object outcomes were not part of read evidence. Caught failures now
   retain their value hash, revision, resource and markings. If the object later
   appears, the earlier result is withheld. Both HTTP and legacy read transports
   retain that evidence before returning the error.
2. Progress polling repeated the function's queries. When data and security
   revisions are unchanged, polling checks current authority and stored evidence
   in one transaction without rerunning user queries. Changed authority still
   requires strict content checks. Verification checks its abort/deadline between
   reads, and API-only inspection expires deadlines even before a lease expires.
3. Validation errors left undispatched calls marked with an unknown outcome.
   Deterministic failures now retain a failed response and replay it. Calls that
   reached a mutating service retain the conservative unknown-outcome rule when
   their commit receipt is uncertain.
4. Jobs finishing before the CPU timer could avoid accounting. The runner now
   waits for a completion acknowledgement; the supervisor measures CPU while the
   container is live before acknowledging success. A real one-millisecond-budget
   regression rejects a short job. Completed runners also exit promptly on stop.

The full Linux gate passed after these fixes, with real Docker execution,
PostgreSQL, Python, SDK, application and starter checks and no skipped tests.
All 498 runtime tests passed.
Evidence is retained in `.review-tools/functions-audit-check.log`. Firecracker,
fleet routing, production capacity and unrelated components were not audited.

## October 29 universal value system completion

Status: implementation and local acceptance complete for value model v1,
TypeScript/Python codecs and the existing execution engines.

Implemented one versioned descriptor/wire contract, strict normalization and
schema validation, TypeScript and Python codecs, shared conformance vectors,
explicit compatibility rules, and compiler/storage/API/SDK/function integration.
The scalar family remains string, boolean, int64, float64, decimal, date,
timestamp and UID. Stored columns/action parameters retain non-null scalar-array
elements. API/function envelopes additionally support nested records/lists,
nullable containers/elements and optional nested fields. Scoped named aliases
resolve through app-contract registries; Function declarations use inline types.
See [C03](spec/core-contracts.md#c03--values-rows-and-schema-evolution) for exact
wire/native bindings, normalization, limits and compatibility.

`packages/contracts/value-types.ts` supplies shared discriminated TypeScript
descriptors, `values.ts` supplies browser-safe normalization/canonical codecs,
and `workers/value_codec.py` supplies exact Python native bindings. Shared vectors
prove both languages emit equal normalized values, canonical bytes and SHA-256
fingerprints, including binary64 extremes, precision-38 decimals, microseconds,
Unicode and invalid descriptors. Typed fingerprints include the normalized
resolved schema and value-model version. Generic JSON transport rejects values
that serialization would otherwise silently change, and SDK response parsing
rejects malformed UTF-8. Generated clients carry the same codecs and decode
projected rows, action responses and named endpoint envelopes. Stricter decoding
also corrected existing Snow status and import-result contract omissions.

The compiler resolves both value profiles through one validator. Stored schema
activation permits nullable additions and rejects destructive/type/primary-key
changes with `MIGRATION_REQUIRED`. One shared physical mapper creates initial
Iceberg schemas; UUIDs are physical UUIDs, integers/decimals retain exact values,
and Python handles all scalar arrays, including UUID extension arrays and empty
tables. Engine boundary conversion explicitly rejects unsafe JSON integers and
lossy float32 narrowing. Read and stream frames carry value version 1.

Functions pin value version 1 and original input/output schemas. Request replay
hashes normalized typed input against the original declaration across releases.
Nested records, optional fields and empty signatures execute through real pipes.
The prepared runner packages the shared codec and validates before JSON
serialization; the supervisor independently validates before publication. Tests
reject native NaN/undefined results even when the declared field is nullable.
The documented multi-stage example image also built offline and returned 42
under an unprivileged, network-disabled, read-only sandbox; its temporary image
and container were removed.

Final Linux `pnpm check` passed: 511 runtime tests, zero failures/cancellations/
skips, plus formatting, generated SDK drift, TypeScript (including negative
descriptor cases), Python formatting, three declaration schemas, 36 example
resources, 13 structural and 12 semantic rejection cases, all 16 Python unit
tests, both bundled app builds and the standalone starter's type/build/HTTP
verification. This includes real PostgreSQL, Iceberg, local/S3 storage, function
containers, authorization, transactional publication, cancellation and crash
regressions. Full output is retained in `.review-tools/universal-types-check.log`.
Only the pre-existing baseline containers remain; no test function sandbox or
registry is left behind. No required local test infrastructure was unavailable.

Enum/union expansion, stored nested structures and additional execution engines
remain separate from this release. Firecracker, multi-host topology, production
capacity/security certification and independent CI were not verified. The CI
test-daemon privilege decision remains pending. No migration, live instance
change, deployment, commit or external publication occurred. The fresh schema-3
baseline changed; recreate disposable development databases.

## October 29 function completion verification

Status: implementation and local acceptance complete for the agreed
TypeScript/Docker release. Independent CI integration/review remains pending.

The TypeScript/Docker release now includes generated ontology clients through
`context.osdk`, separate supported platform routes through `context.platform.fetch`,
read-only, staged edit-proposal and immediate read-write effects, durable logical
call receipts, and bounded nested invocations. No filesystem or function client
was added to the generated OSDK. Existing value types remain the release contract;
the shared multilingual type-system milestone above is separate.

The trusted worker binds the same object/actions as the normal workspace API.
Its platform bridge supports filesystem metadata/source/content operations and
function invocation/inspection/events/cancellation. Other platform services need
their own trusted integration. No image receives host credentials, daemon access
or an external network. Read permissions and classifications survive caught
data-dependent API failures. Mutation destination checks include all consumed
markings before commit. Own writes advance read anchors; filesystem versions are
captured within the mutation transaction so concurrent changes cannot replace
those anchors. Results and logs recheck current grants, newly applied binding
markings and input evidence. Child acceptance checks its resolved effect inside
the acceptance transaction; brokered cancellation checks the parent fence,
effect and destination markings inside the cancellation transaction.

Staged actions commit as one transaction after typed output validation, with
complete rollback if any later condition fails. Declared function-backed actions
compute typed target property maps outside the action transaction, then recheck
point reads, current permissions, markings, conditions and post-state. Proposals
reject unconstrained predicate dependencies. Immediate writes commit per call;
later failure or cancellation does not imply rollback. Call traces retain
unresolved outcomes without automatically repeating user code.

The approved local Docker test socket enabled real digest-pinned OCI build/push
and execution. The container suite passed actual generated SDK/platform calls,
nested containers, CPU/memory/PID exhaustion, network/root/socket/credential
denials, protocol faults, cancellation, deadlines and SIGKILL of a supervisor.
A replacement fenced the lost lease and removed the surviving sandbox without
rerunning code. Authenticated CLI tests invoke, inspect, read events and cancel
real containers. A disposable browser fixture rendered the actual
`FunctionInvocations` React component against the real service and Docker worker:
input 41 returned 42, retry retained the invocation ID, events stayed ordered,
and a live invocation cancelled with subsequent sandbox removal. The fixture,
registry, image, database and browser tab were cleaned up.

The final Linux `pnpm check` gate passed all 497 runtime tests with no failures,
cancellations or skips, plus formatting, SDK drift, TypeScript, schema checks,
all 14 Python tests, both application builds and standalone starter verification.
This includes the final current-binding marking rechecks for captured error-read
grants, retained failed filesystem-read evidence, and transactional child-effect/
cancellation checks. All 31 focused function tests also passed. Full-run evidence
is retained locally in `.review-tools/functions-complete-check.log`. CI daemon
access remains a separate approval decision; no CI privilege change has been applied.

This is the existing single-host Docker topology. Firecracker, multi-host cleanup
routing, other languages, delegation, record garbage collection and production
capacity/security certification remain separate. The fresh schema version 3
baseline changed; recreate disposable development databases. No live migration,
deployment, commit or external publication occurred.

## October 28 function client boundary

Function invocation, inspection, events and cancellation belong to the platform
API. The client helper moved from `packages/osdk` to `packages/platform`; the CLI,
management console and authenticated service tests use it there. Generated OSDKs
no longer expose `client.functions` or ship function client/contract artifacts.
Regeneration removes those obsolete files, and SDK boundary tests cover their
absence and cleanup. Existing endpoint authorization, effects, idempotency and
runtime behavior are preserved. Linux `pnpm check` passed formatting, SDK drift,
TypeScript and 463 of 464 runtime tests, with no cancellations/skips. All function
service/runner and SDK boundary tests passed. The sole failure was the known
unavailable Docker socket prerequisite; the previously intermittent SQL lease
case passed in this run. Both application builds, standalone starter checks,
Python formatting/schema checks and all 14 Python unit tests passed separately.
Full-run evidence is retained in `.review-tools/functions-client-boundary-check.log`.
Real Docker isolation and the complete gate remain unverified pending the earlier
test-supervisor access decision. General platform API access from function
sandboxes remains proposed, not implemented.

## October 28 filesystem client boundary

Filesystem operations belong to the platform API, independently of the ontology
SDK. The HTTP client helper moved from `packages/osdk` to `packages/platform`,
sharing the existing browser-safe transport with ontology clients. The CLI and
Files console use that platform helper directly. Generated OSDKs no longer expose
`client.filesystem` or include filesystem/storage client artifacts. Regeneration
removes the formerly generated artifacts and drift checks reject their presence.
Function response marking types no longer pull in the filesystem contract bundle.
Existing routes, permissions and runtime behavior are preserved. SDK boundary
assertions and real filesystem HTTP/client coverage passed. The Linux full gate
passed formatting, SDK drift and TypeScript, then reported 464 runtime tests:
462 passed, two failed, no cancellations/skips. Failures were the existing missing
Docker socket prerequisite and SQL lease supervision case previously observed in
the full run; neither was suppressed or weakened. Evidence is retained locally in
`.review-tools/filesystem-client-boundary-check.log`. Both bundled builds, the
standalone starter's typecheck/build/publication/HTTP checks, Python formatting,
schema validation and all 14 Python unit tests passed separately. Full-gate and
container-isolation verification remain pending. General platform API access
inside function sandboxes is still proposed, not implemented.

## October 28 function backend boundary

Docker remains the implemented function backend. Firecracker is the planned
production backend; the staged artifact, guest transport, isolation and acceptance
work is recorded in [FUNCTIONS.md](FUNCTIONS.md#production-backend-plan).
The shared `FunctionWorker` interface and limit validation now live in `worker.ts`.
The service has no Docker import or implicit backend default. The explicit worker
launcher injects Docker; API processes still have no backend. Tests and fixtures
use the shared interface, including a missing-backend startup assertion.
No backend selector, Firecracker stub, dependency, schema or privilege change was
added.

The Linux `pnpm check` attempt passed formatting, SDK drift and TypeScript.
All 13 function service/runner tests passed, including injected execution,
competing workers and missing-backend startup. The complete runtime run reported
464 tests: 462 passed, two failed, no cancellations/skips. One failure is the
known missing Docker socket prerequisite; the other was the existing SQL lease
supervision test reporting `LEASE_LOST` under the full run. A separate rerun of
the entire SQL integration file passed all 48 tests, including that case. No lease
code or test timing was changed; the intermittent full-suite failure remains
unresolved. Full-run evidence is in `.review-tools/functions-backend-check.log`.
The gate stopped before Python/build/starter checks. Firecracker execution and
real Docker isolation remain unverified; the previous test-supervisor access
decision is still pending.

## October 28 function execution implementation

First-release scope is TypeScript OCI invocation: activated repository exports,
durable private invocation records, immutable code/input binding, typed values,
current authorization, supervised worker limits, cancellation, result/event
inspection, API/SDK/CLI and Files-console controls. Function-backed action commits,
delegation, other languages and production capacity remain separate milestones.

Container control is confined to the explicit `function-worker` process. API
instances have no container backend; they persist acceptance/cancellation and read
authorized records. Worker leases/fences prevent stale result publication, and
maintenance handles expired attempts and terminal container cleanup without
automatically rerunning user code. See [FUNCTIONS.md](FUNCTIONS.md).

The focused Node 24/Linux verification passed 13 new service/runner tests with no
failures or skips, including real PostgreSQL, authenticated HTTP/SDK/CLI, concurrent
worker claims, API-only process boundaries, current output classifications, input
revocation and stale/cancelled attempt publication. Service tests use a controlled
computation adapter; the runner test executes known TypeScript in a child process.
Explicit hosted-app function bindings preserve the verified app identity and do
not expose function endpoints on unbound apps. Five existing execution-process,
scheduler-process and SDK tests also passed.
TypeScript, formatting, SDK drift and both bundled application builds passed.
Real OCI/container cases are implemented but could not run. The final Linux
`pnpm check` attempt passed formatting, SDK drift and TypeScript, then reported
464 runtime tests: 463 passed, one failed, zero cancellations/skips. The sole
failure is the container suite's unavailable `/var/run/docker.sock`; the harness
has the pinned Docker CLI but no daemon access. The dedicated test-supervisor
access decision remains pending before container verification and the full gate
can pass. Output is retained locally in `.review-tools/functions-check-blocked.log`.

A deterministic real-PostgreSQL regression reproduced a teardown race: Pool.end
resolved before idle client sockets closed, allowing owned-cluster shutdown to
terminate them. The shared fixture now drains those sockets before stopping its
database. The regression and the final full run passed this check without hiding
errors or relaxing assertions.

No isolation, full-gate or completion claim is made yet. The fresh schema version 3
baseline changed; recreate disposable development databases. No live deployment,
migration, Git commit or external publication occurred.

## October 28 filesystem complete

Status: complete for the agreed first release, accepted on October 28. Namespace,
human and agent homes, group projects, inherited authorization and markings,
versioned local/S3 content, ontology and repository roots, checkout/CI publication,
resource placements, activation, HTTP, SDK, CLI and console workflows are delivered.
Adversarial fixes and the passing 449-test Linux gate are recorded below.

Production capacity validation remains separate from this completed milestone.
The release uses the existing execution runtimes and a fresh version 3 database;
the operating bounds and excluded features remain documented in
[FILESYSTEM.md](FILESYSTEM.md).

## October 28 filesystem adversarial exercises

Exercised real PostgreSQL, local storage, MinIO, HTTP, generated clients and CLI
against permission boundaries, publication, replay, upload races, disconnects and
retained history. Three deterministic seeds drive 420 mixed metadata/content
operations against an independent journal, then check all retained bytes after
orphan collection. No live instances or production credentials were used.

Reproduced and fixed movable agent configuration inheriting memory write grants,
activation bypassing placement publish/delete denies, and replacement/omission
losing future restrictions on old source. Verified upload chunks are copied before
an authorization await so a producer cannot mutate bytes after hashing. Concurrent
different uploads commit one head; identical retries share one receipt. Interrupted
HTTP uploads leave no visible file, parent change or private staging residue.

Resource bindings and dependency edges now qualify names by project. Independent
projects can activate identical namespaces; named lookups require projectId (CLI
--project), while UUID lookup remains unambiguous. Read endpoints conceal inaccessible
identities. Cross-project pinned imports preserve owning-project restrictions and
ambiguous imported names cannot activate. Transform outputs require active writable
placements. Retirement checks transitive pins and ignores retired consumers.

The fresh version 3 baseline changed; recreate disposable development databases.
No migration, deployment, new execution engine or production capacity test is in scope.
Final verification: the complete Linux `pnpm check` passed with 449 runtime tests,
zero failures/cancellations/skips, all 14 Python unit tests, formatting, SDK drift,
TypeScript, schema checks, both application builds and starter checks. The final
run includes matching 404 status, code and message for hidden versus unknown
resources, roles, policies, source checkouts and releases. Node 24.19.0, pnpm 11.25.0
and Python 3.11 ran in a non-root disposable Docker container without external
network access. The earlier complete run also passed all 449 runtime tests.

## October 28 platform filesystem integration

The filesystem exposes protected HTTP, generated SDK, CLI and console workflows,
with instance identities, private human and managed agent homes, group projects,
ontology and code repository directories, inherited grants and markings, stable
resource placements, copy provenance, immutable byte and metadata history,
coherent checkout, CI export publication and explicit plan/apply activation.
Local and S3 backends share verified streaming and reference-aware collection.
Persisted backend bindings prevent accidental physical-store relocation. Schema
version 3 requires a fresh development database; no migration or deployment occurred.

Bound object reads/actions, indexing and dataset metadata access recheck filesystem
policy for resource references and UUIDs. Raw persistence remains trusted internal
composition. Source activation publishes definitions with runtimeReady=false; it
does not provision a generic executor, ingest data or deploy application assets.
Ordinary policy edits cannot remove markings. Declassification, FUSE, cross-project
transfer, hosted Git and history purging are outside this release. See
[FILESYSTEM.md](FILESYSTEM.md) for contracts, commands, CI, bounds and operations.

Final verification: the complete Linux `pnpm check` gate passed on October 28 with
430 runtime tests, zero failures/cancellations/skips, Node 24.19.0, pnpm 11.25.0 and
Python 3.11. The non-root container ran with networking disabled. Formatting, SDK
drift, TypeScript, real PostgreSQL/Iceberg/MinIO, crash/concurrency/authorization,
Python checks, both application builds and the starter check passed. Package-policy
metadata was refreshed separately before the offline run, without changing the lockfile
or disabling verification. Console QA used an isolated synthetic instance: browsing,
text editing to revision 2, reopening the file and retained revisions 1 and 2 passed.
An earlier run reported one PostgreSQL teardown termination after its assertions
passed; it did not recur in the final full gate. No fixture or criterion was relaxed.
No production deployment, new engine, paid model call or capacity benchmark was run.

## October 28 filesystem metadata foundation

The filesystem starts with a fresh version 2 schema; version 1 development databases
must be recreated. No deployed instances or migration/backfill path are in scope.
Added internal filesystem contracts and PostgreSQL storage for stable directory,
file and resource entries, immutable entry versions and actor-scoped replay receipts.
Ordered mutation batches provide expected-revision checks, atomic rollback,
identity-preserving moves, cycle/path bounds, tombstones and project/environment
scope preservation. Catalog transactions now accept a separate admission key so
the filesystem can serialize metadata writes without inventing an environment.

The store is deliberately unexposed. Policy/marking inheritance, principal homes,
ontology/repository registration, content transfer and checkout/deployment remain
separate implementation slices. Content descriptors are metadata, not proof that
referenced bytes exist or are authorized. See [the filesystem guide](FILESYSTEM.md).

Verification: the complete Linux `pnpm check` gate passed on October 28 with Node
24.19.0, pnpm 11.25.0 and Python 3.11 in an isolated non-root container with external
networking disabled during checks. All 382 runtime tests passed with no failures or
skips, including the real PostgreSQL filesystem scenarios, existing Iceberg/S3 and
process/authorization coverage. Formatting, SDK drift, TypeScript, Python checks,
both bundled application builds and the starter hosting check passed. The cached
test image's missing declared Black dependency was installed in the isolated test
workspace before the successful run; no acceptance gate was disabled.

For the current review summary, start with [the October 15 morning report](MORNING-REPORT-2026-10-15.md). The sections below preserve chronological checkpoints; later entries supersede earlier status and remaining-work notes.

Current work is an internal prototype, not the completed first slice. Private remote: gcsequino/Moria. User authorized autonomous local development, WSL/Docker and local inference. No shared deployment or paid model use.

Scale requirement: both datasets and Ontology must support millions of rows/objects. OPS-03 uses progressive 1m/10m populations; 100k is only a smoke workload. The initial materialized 1m benchmark took 27.85 s under the activation lock and peaked near 1.06 GiB Node RSS. The new streaming ObjectStore run staged 10m in 195.13 s outside that lock, published in 3.11 ms and peaked at 192.8 MiB Node RSS. These are different synthetic harnesses, not a controlled throughput comparison. Dataset transforms, sustained mixed load and full multi-row serving at this scale remain unverified.

## Verified

- Rust comparison completed October 15: 24 trials at 1m/10m and 20/100 concurrent requests, 498,001 successes with zero failures. At 10m/100, Rust achieved 3,690 req/s vs fresh Node 2,924, queue p95 26–27 vs 32–35 ms, app peak 12 vs 317 MiB. See the Rust follow-up in ONTOLOGY-TECHNOLOGY.md. Retain the security-tested TypeScript/PostgreSQL API for implementation now; Rust remains a measured serving optimization candidate. No need to repeat this comparison on each overnight wakeup.

Source staging accepts synchronous/async iterables, validates and writes batches of 1,000 outside the environment lock, and publishes by atomically attaching a stage pointer to a commit revision. Legacy generations remain readable. Tests cover paused-stream concurrent actions, partial invisibility, duplicates across batches, failed streams, source-head/schema/epoch conflicts, retained overlays and idempotent publication. UID homes reserved by successful staging batches remain permanent even if a later batch fails. Failed/abandoned stages are retained; garbage collection and worker supervision still need integration.

Earlier fencing checkpoint: 54 tests pass on Windows and non-root Linux without external networking. Stages can persist an execution/worker/fence binding; publication checks and locks the matching live lease in its transaction. Expired/replaced workers cannot publish or swap in a newer token. Receipt replay still succeeds after lease expiry, subject to current epoch validation. Catalog renewal preserves the token and never shortens or resurrects a lease. The authorized indexing coordinator must mandate this binding and handle renewal/cancellation; trusted fixture/legacy stages remain optionally unfenced. No production worker-isolation claim is made.

Same-schema refreshes now publish across concurrent edits when the source head is unchanged and its persisted schema exactly matches the staged schema. New actions validate against that active schema, so later overrides remain compatible without rescanning them under the publication lock. Competing source publications, schema changes and legacy generations without schema metadata retain strict conflict behavior. Changed-schema reads/actions through old bindings fail with SCHEMA_CHANGED. Schema-changing refreshes still require a quiet revision window; old binaries lacking these schema checks cannot safely coexist as writers. Full release/schema migration coordination remains pending.

Policy-aware UID-ordered pagination now runs through ObjectService and the workload HTTP API. SQL applies pinned/current canonical and effective-object restrictions before LIMIT, with reference verification before release. Cursor identity, query, contract, epoch and nonextending TTL are enforced. Explicit hidden projections/predicates/order keys fail before scanning. Custom sorts, aggregations and links remain unsupported. Protected reads/cursor transactions use repeatable-read plus shared environment advisory admission before BEGIN and an environment row share lock; mutations retain serializable transactions and exclusive admission. This avoids measured serialization cycles between concurrent cursor inserts and stale reader snapshots during writes, while preventing security/data publication during each page.

Next overnight work: extend indexing supervision and release-owned source bindings, then address broader query/sort contracts. The concurrent-refresh HTTP measurements are complete (see below). Existing language benchmarks use simplified tables and do not verify these product paths. Keep snapshot/version retention intact during refactoring. scripts/benchmark-staging.ts measures the real ObjectStore loading/publication path, optional authorized pagination and mixed HTTP workload separately from those kernels.

- Added a reproducible ontology technology comparison: Node/TypeScript, Go/pgx and async Python/psycopg execute common PostgreSQL query/action kernels at 1m/10m objects and 20/100 outstanding HTTP requests. Raw results, plans, method and stack recommendation are in docs/ONTOLOGY-TECHNOLOGY.md and docs/benchmarks/technology. This is a serving-design experiment, not production multi-row Moria integration or an Iceberg throughput result. Benchmark TypeScript is included in the normal typecheck.

Earlier mixed-HTTP checkpoint: TypeScript checking and 51 passing tests on Windows and non-root Linux with external networking disabled, no skips. Tests include staging failures, legacy migration, pinned/current pagination, 200 mixed HTTP operations and shared-lock cleanup after rollback. Four corrected 30-second mixed HTTP phases at 1m/10m and 20/100 callers completed 83,009/83,009 requests, including 8,299 actions. At 10m, query/action p95 was 37.02/31.05 ms at 20 callers and 174.13/172.96 ms at 100. Generator and server share a process; rows are narrow, pages walk early queues, and there is no concurrent ingestion. This is not an OPS-03 capacity certification. See PERFORMANCE.md for raw results, failed experiments, commands and limits.

Mixed traffic exposed serialization failures not seen by read-only tests; shared advisory admission before the snapshot fixes those for cooperating readers/writers. A separate million-object run without fresh planner statistics took seconds per page. ObjectStore staging now refreshes statistics before sealing, outside the publication lock; the 10m run validates this built-in path. The prior 4,000-request direct-query benchmark remains historical evidence, not a measurement of the new shared admission under mixed load.

Earlier refresh checkpoint: 53 tests pass on native Windows and non-root Linux without external networking. Same-schema publication during edits passed at 1m with 55,754 successful mixed HTTP operations, then at 10m with 538,269 successes, zero errors and no request above one second. At 10m/20 and 100 callers, query p95 was 46.64/178.16 ms, action p95 40.09/173.66 ms and publication including wait 36.46/111.19 ms. Full refresh staging slowed from 254 to 659 seconds under the larger caller load; the pool and event loop are shared. Diagnostic evidence caught post-refresh scan/sort plans causing nearly 30-second tails; dependent current-UID lookup now preserves indexed pagination across generations. See PERFORMANCE.md for retained failed experiments, plans and remaining workload limits. No benchmark process is left running at this checkpoint.

The trusted Iceberg adapter now offers a pinned JSON-lines row iterator feeding ObjectStore directly. Arrow scan batches replace whole-result materialization on this read path; transport frames and Python dictionary batches are bounded, and Node pipe backpressure follows ingestion consumption. Tests use a real 1,001-row snapshot, early cancellation, old metadata and snapshot-mismatch rejection without publication. Integer/decimal/timestamp wire conversion is explicit. Arrow/file-planning memory and million-scale Iceberg throughput remain unmeasured; staging writes still use the original materialized fixture interface. See workers/iceberg/README.md. The initial coordinator below now integrates this adapter for ordinary Dataset inputs.

- CLI validate/compile; strict declarations, reference/mapping/type/graph checks and canonical fingerprints.
- RestrictedView transform prohibition; external dependencies fail explicitly until implemented.
- Typed expression evaluation and SQL, with real PostgreSQL differential tests for nulls, Unicode, numeric precision, timestamps, arrays and membership.
- Wire-value validation and UTC microsecond normalization.
- Serializable catalog publication: fenced leases, epochs, all output pointers, lineage and checkpoint in one transaction, conflict rollback and lost-ack retry.
- Permanent UID registry with uniqueness and collision retry.
- Real PyIceberg staging and exact-metadata reads. Its private in-memory SQLite catalog is discarded; PostgreSQL is the publication authority. Tests prove conflicting outputs remain hidden and old snapshots remain readable.
- Stable action idempotency across revisions, expired-key nonreuse and conditional concurrency.
- Versioned canonical source generations and overlays: source disappearance/reanimation, null versus clearOverride, persistent deletion tombstones and duplicate UID rejection.
- Database-backed current principal attributes, marking clearances, distinct application scopes/grants, security epochs and audit. Stale human identity synchronization fails closed after 60 seconds; unchanged sync refresh does not invalidate cursors.
- Integrated internal point-read and single-object action service enforces current authority, row/property policy checks, before/after write policies and action conditions. Explicit denied projections fail even for nonexistent rows. Replay checks current permission before returning an old receipt.
- Opaque cursor storage binds query, identity/delegation, contract and security epoch with a nonextending 15-minute expiry. Integrated UID-ordered multi-page reads check pinned and current canonical/effective restrictions, deletions, disappearance and current authorization. Replay preserves the data pin and deadline; changed contracts, queries or authority invalidate it.
- Model gateway checks destination/markings before sending and current authorization before releasing output. Loopback local-chat smoke returned Moria in 286 ms. No paid API used. Tools/streaming remain unsupported.
- Worker process kill tests verify rollback before COMMIT and receipt recovery after COMMIT using real Iceberg metadata.
- Benchmarks found 171/200 failed contending actions before a fix. Writers now queue before taking their serializable snapshot; all 200 succeed. Batched activation improves 10k rows from 1.59 s to 0.22 s. See PERFORMANCE.md and raw records for limits and methodology.
- CLI optionally validates bound policy fields, action parameters and principal attribute types through --attributes. Without it, policyTypesChecked is false. Production activation must require an authority-owned type registry, not client-provided attributes.
- Narrow workload HTTP API for point reads and single-object actions. Opaque 15-minute bearer sessions are hashed at rest and minted only by trusted operator code. Identity is bound server-side; request-body identity spoofing is rejected. Session expiry/revocation is rechecked inside operations, including idempotent replay. JSON body size is bounded. No public login/admin endpoints or browser CORS support.

## Environment

Ubuntu 24.04 runs under WSL2. Docker Engine 29.1.3 and Compose 2.40.3 installed; hello-world container succeeded. Ubuntu packaged Node 18 is not supported by Moria; Linux execution needs Node 24 or a pinned container. Windows Node 24 and pnpm 11.25.0 work. Python worker uses the repository virtual environment.

Existing LocalLLM restarted through Windows system PowerShell. Its launcher cannot use the bundled PowerShell PSHOME executable path. No launcher changes made. Credentials remain outside this repository. Real chat smoke passed; tool/streaming smoke remains pending.

## Remaining gates

A narrow workload API exists as a server factory tested on loopback, not a deployed service. OIDC human login, IdP/group synchronization, release/deployment ownership, RLS, full audit coverage, TLS/ingress hardening and delegated service sessions remain pending. Authority administration and session minting are trusted internal interfaces and must never be exposed to application callers. Point and UID-ordered query operations integrate current checks; sorting, aggregation and link traversal still need equivalent enforcement.

Iceberg supports internal replace/append staging only. Production coordination must verify durability, files, schema, keys, table identity, parent snapshot and storage paths. Native and Linux tests and commit-boundary process crashes pass; storage-failure injection, production writer isolation, pins and GC remain pending. Phase 0 is not complete.

An internal coordinator now ties ObjectStore staging to catalog-published Iceberg snapshots, current access and mandatory worker fencing. Production indexing still needs release-owned bindings, restricted-view policy integration, durable scheduling/recovery, lease supervision and storage validation. All generations/history and failed/abandoned stages are retained. The new additive stage migration preserves legacy data reads; general release/schema migration tooling remains pending. Links, action-created objects, multi-object edits and outbox remain pending. Cursor retention/GC must respect active pins; currently history is retained indefinitely.

Generated OSDK/React, isolated production workers, agents/retrieval/evaluations/MCP, install/backup/restore, license inventory and full performance gates remain pending. Python dependency versions are constrained from the Linux test image; dependency hashes and apt snapshot locking remain release work. Test-container isolation does not prove the eventual worker sandbox.

Continue current authorization and pinned-read/cursor enforcement, compiler policy contexts and worker isolation. Keep local commits and measured evidence. Do not expose internal store methods directly to clients or equate library tests with production readiness.

## Catalog-resolved indexing checkpoint

The internal IcebergIndexer now resolves an immutable Dataset head through committed snapshot history, checks source read and target index grants/scopes/markings, requires a persisted worker fence and streams outside the publication lock. It records source revision, metadata location, snapshot ID, initiating principal/application and binding fingerprint in index_provenance before publication. Current epoch, identity freshness and session expiry/revocation are rechecked inside publication; a failed check leaves the old ontology head intact. The trusted configuration is cloned and cannot drop source markings from the target label set. No caller-supplied metadata location is accepted by this coordinator.

This is a single-attempt internal interface, not a public indexing endpoint or durable scheduler. Release-owned binding resolution and enforcement that serving uses these same markings remain required. RestrictedView remains a legal object source in the spec, but this initial adapter rejects it until canonical row/property restrictions can be bound correctly. Transform inputs remain Dataset-only. Crashes may leave orphan stages; retries currently rebuild, and callers must renew/cancel long jobs. Input snapshot pinning does not require the dataset to remain the latest head. Table identity, storage sandboxing/durability checks, streaming writers and large-scale Iceberg memory measurements remain gates. Existing ontology benchmarks have not been relabeled as Iceberg throughput evidence.

Verification: pnpm check passes 55 tests on native Windows. The added integration test uses real Iceberg and PostgreSQL, verifies provenance and row publication, rejects invalid sessions and marking downgrade, and revokes source access between staging and publication to verify rollback/current authorization. Linux verification is recorded after completion below.

Linux verification also passed all 55 tests, no skips, in the non-root test image with `docker run --rm --network none moria-test:local` (image 61f98e5efab5). Reproduce with `pnpm check` on Windows or build using `docker build -f deploy/test/Dockerfile -t moria-test:local .` before the Linux run. No benchmark rerun was needed: this checkpoint adds coordination, not serving query plans or benchmark kernels.

## Local indexing supervision checkpoint

`IcebergIndexer.runSupervised` now renews the supplied live fence before work and every third of its lease duration (60 seconds by default). Renewals are serial, never overlapping. A failed renewal cancels the stream and preserves the lease-loss error; stale leases never start work. Cleanup removes timers/listeners and drains an outstanding renewal. `run` remains available for externally supervised jobs. Neither method claims a replacement token automatically.

The Iceberg iterator accepts AbortSignal, rejects already-cancelled reads before spawning, checks cancellation between buffered rows, and terminates/reaps its Python child. Termination escalates to SIGKILL after ten seconds if needed. Indexing checks cancellation inside its publication transaction; a completed publication wins a subsequent cancellation race. Tests cover active-stream cancellation, cancellation before publication with the prior generation unchanged, real PostgreSQL renewal across multiple one-second lease windows, stopping renewals after completion, and replacement-worker fencing. Native Windows `pnpm check`: 56 passing tests, no skips.

This is process-local supervision, not durable scheduling, crash recovery, a public cancel endpoint or a worker sandbox. Database operations are not interrupted by AbortSignal; pool/query timeout configuration still governs cleanup during a database outage. A worker can retain resources until cancellation is observed; the persisted fence remains the authoritative publication guard. Full indexing execution deadlines, durable attempt records and restricted-view release bindings remain work. No serving technology or benchmark claim changed.

Linux verification: all 56 tests also pass as non-root with external networking disabled (image 8dbd62efeeb5). Reproduce: `pnpm check`; or `docker build -f deploy/test/Dockerfile -t moria-test:local .` followed by `docker run --rm --network none moria-test:local`. No skips, test/benchmark processes or deployments remain running.

## Supervised execution deadline checkpoint

Supervised indexing now has an execution-wide deadline: 30 minutes by default, with a caller option `timeoutMs` that may lower but not raise that cap. It starts before the initial renewal, stops further renewals on expiry, cancels the stream and reports WORKER_TIMEOUT when cancellation is observed. A completed commit still wins a later cancellation. The unsupervised `run` method remains for external supervisors. This is cooperative enforcement: pending database waits are not interrupted, so it does not promise a hard wall-clock return deadline during an outage.

Review also found that the reader's timeout killed Python but could still yield rows already buffered in Node. Timeout is now checked between rows, preventing further buffered output after expiry. Regression tests use a real Iceberg reader paused after its first row and resume it after the deadline; PostgreSQL supervision tests check deadline cancellation, invalid limits and preservation of an already committed result. Streaming and query benchmark results are unchanged; no new performance claim is made.

Verification: all 57 tests pass on Windows (`pnpm check`) and non-root Linux with networking disabled (`docker build -f deploy/test/Dockerfile -t moria-test:local .`, then `docker run --rm --network none moria-test:local`; image 15fd7813051a). No skipped tests. Tests include the buffered-row timeout regression. Remaining next work: release-owned source/serving policy bindings, durable indexing attempt/recovery records, and broader query contracts; no reason to repeat unchanged language kernels.

## Persisted source marking checkpoint

Ontology stages and published generations now persist required markings. The Iceberg coordinator supplies its target label set, which must include every configured source label. Point reads, paginated queries and actions check that their serving binding includes all markings required by the active generation, inside the protected transaction. Action replay performs this check before returning the old receipt. Later source publication cannot drop an active required label, even through the trusted raw stage API. Adding labels is permitted; old serving bindings fail closed until updated. There is no declassification path in this prototype.

This closes the gap where independently configured indexing and serving bindings could disagree about source markings. It does not establish release resolution or infer labels from storage. Legacy/direct-fixture generations with null marking metadata remain compatible; production migration must backfill authoritative labels before serving them. Raw ObjectStore remains trusted internal persistence, not an authorization boundary. Restricted-view row/property bindings and automatic dataset lineage marking resolution still remain work.

Windows pnpm check passes all 58 tests. New real PostgreSQL coverage checks label-array cloning, correct reads/queries, omitted labels, new actions and receipt replay, rejected refresh rollback, and stricter-label publication invalidating an older serving binding. The check extends existing read metadata lookup; actions now also check metadata before idempotent replay, adding a query. Prior million-object performance results predate this change; a follow-up serving benchmark is required before treating those latencies as current.

Linux verification also passes all 58 tests, no skips, with `docker run --rm --network none moria-test:local` after building deploy/test/Dockerfile (image 3426ec2bde50). Next performance follow-up should persist nonempty required markings in the staging/HTTP harness and repeat representative mixed loads at 1m/10m and 20/100 callers. No test or benchmark is left running.

## Marking-aware serving remeasurement

Updated initial and refresh benchmark generations to persist sensitive markings. Native pnpm check passes all 58 tests. Sequential real mixed-HTTP runs at 1m/10m and 20/100 callers completed 79,626 successful requests, zero errors and no request over one second, including 7,962 actions. At 10m, query/action p95 was 38.34/32.53 ms (20 callers) and 167.94/163.57 ms (100). 10m initial staging: 197.77 seconds; publication: 3.40 ms; peak Node RSS including generator: 292.2 MiB. See PERFORMANCE.md, two http-markings result files and their source-hash manifest for commands and limits. No concurrent refresh was included in those scale runs; earlier refresh evidence is retained separately. Keep TS/Node + PostgreSQL provisionally. These short warm narrow-row results do not complete OPS-03.

A separate 1,000-object concurrent-refresh smoke (`http-markings-refresh-smoke.json`, `--rows 1000 --http --refresh --seconds 1`) completed both marked refreshes with 1,348 HTTP successes and zero failures. This validates the updated refresh harness, not million-scale refresh performance. No benchmark processes remain running.

## API contention/error checkpoint

Catalog transaction retry exhaustion now throws a sanitized CONCURRENT_MODIFICATION after three failed serialization/deadlock attempts instead of leaking raw SQLSTATE to the generic HTTP error path. HTTP maps this to 503, PostgreSQL lock-wait timeout to RESOURCE_BUSY/503, and statement cancellation to QUERY_TIMEOUT/504. Serving marking mismatch is a 409 configuration conflict. Unknown database errors retain INTERNAL/500 with a generic message; SQL diagnostics are not returned.

Expanded the real HTTP/PostgreSQL integration test: PostgreSQL-raised serialization failures verify three attempts and complete rollback; an actual blocked advisory lock verifies lock timeout; pg_sleep with statement_timeout verifies query timeout; unknown database error verifies redaction. A normal query succeeds afterward, exercising transaction/connection cleanup. The serialization case injects SQLSTATE rather than claiming to reproduce a natural deadlock. No automatic client retry, new Retry-After promise, or broader retry of side effects was introduced; callers retry actions only with the same idempotency key and input.

Native pnpm check passes all 58 tests. This changes failure responses only, so the successful serving benchmark remains applicable without another scale run. Durable job records/recovery, release binding resolution and broader query capabilities remain next implementation work.

Linux verification also passes all 58 tests, no skips, as non-root with external networking disabled (image 6da533686354). Commands: `docker build -f deploy/test/Dockerfile -t moria-test:local .` then `docker run --rm --network none moria-test:local`. No test/benchmark processes remain running.

## HTTP input and error-envelope checkpoint

HTTP JSON decoding now rejects malformed UTF-8 instead of silently substituting replacement characters in submitted values. Encoded bodies other than identity are explicitly unsupported; the existing 64 KiB bound remains. Invalid path percent-encoding and repeated properties query parameters produce INPUT_INVALID/400. Integration coverage submits malformed bytes to an action and then successfully invokes with the same idempotency key, proving rejection did not reserve or apply that action.

Error envelopes now include correlationId, retryable and empty details per the core contract, retaining requestId as a compatibility alias. Only contention, lock-wait and query-timeout errors are marked retryable; this does not perform retries or waive action idempotency requirements. Windows pnpm check passes 58 tests, with new cases added to the real HTTP integration test.

The prototype still uses /v1 routes; the spec's /api/v1 and /management/v1 prefixes and generated OpenAPI/SDK remain an explicit compatibility gap to address before a public client contract. This checkpoint does not claim production ingress hardening or complete API conformance.

Linux check also passes all 58 tests without skips, non-root and network-disabled (image 8710afcee2ba). Reproduce with `docker build -f deploy/test/Dockerfile -t moria-test:local .` and `docker run --rm --network none moria-test:local`. No test/benchmark processes remain running.

## Canonical serving prefix checkpoint

Serving routes now accept the spec's /api/v1 prefix. The existing /v1 prefix remains a compatibility alias into the same handler, session checks, cursor scope and action idempotency scope; switching prefixes cannot repeat an action. /management/v1 remains reserved and returns not found. The real HTTP integration suite now runs through the canonical prefix, with explicit alias read/replay checks. All 58 tests pass on Windows.

Added apps/api/README.md with supported routes, session boundary, input limits, cursor/idempotency/error semantics and remaining gaps. No public listener/deployment, management endpoint, OpenAPI generation or SDK is claimed. Previous performance harnesses continue using the equivalent alias; source hashes identify those measured versions.

Linux verification: all 58 tests pass without skips, non-root and network-disabled, image 8b68fd5069d6. Reproduce via `pnpm check` or build deploy/test/Dockerfile and run `docker run --rm --network none moria-test:local`. No test/benchmark processes remain running.

## Readability cleanup requested during review

Applied conventional formatting to TypeScript application/service/library code, tests and scripts; Python workers/validators/benchmarks; and Go/Rust benchmark sources. JSON/YAML source fixtures and schemas are formatted consistently. Raw benchmark evidence is unchanged. Added pinned Prettier, .prettierrc.json, .prettierignore, .editorconfig, Python Black configuration and a development-only formatter requirement. `pnpm format` applies formatting; `pnpm check` now includes `format:check`. README and AGENTS.md document the conventions.

Verified all 34 changed TypeScript files exactly match formatter-only transformations of their previous contents; Python ASTs are unchanged. Windows formatting/type checks and all 58 tests pass. This is a readability/tooling change, with no intended runtime behavior changes or new performance claims. The overnight heartbeat remains paused.

Linux formatting/type checks and all 58 tests also pass as non-root with networking disabled (image a5bebf8ee4f0). Black check passes for all four Python files; Go/Rust sources were formatted with their native tools. No benchmark result files were changed.

## Technical diagrams

Added docs/TECHNICAL-DIAGRAMS.md with five implementation-grounded Mermaid diagrams and rendered SVG copies under docs/diagrams: component architecture, protected pagination, action/idempotency, Iceberg indexing/publication and source/overlay state. The document links to source modules and distinguishes in-process libraries from deployment units and planned integrations. All five Mermaid blocks rendered successfully; architecture and query renders were visually inspected. Documentation/assets only, so runtime tests were not rerun. The heartbeat remains paused.

## Runnable local asset workspace — October 20, 2026

Added `pnpm start`: loopback UI and API with an embedded, persistent PostgreSQL cluster and local Iceberg warehouse. The browser imports a bounded CSV/JSON asset dataset, queries authorized objects, saves independent title/status/notes overrides through the existing action service, and can clear overrides. Operator, West editor and East reader are explicitly demo workload identities; their row policies and action grants are enforced server-side. The local operator key authorizes selecting those demo profiles, not an OIDC identity. Fixed local bootstrap bindings and a dependency-free browser client keep this first slice small; generated SDK/React and general release resolution remain unfinished.

Imports validate all rows before allocation, resolve permanent UIDs by source ID, use the real Python Iceberg writer, atomically publish dataset metadata, then run the existing fenced indexer. Refreshes preserve overlays; omitted objects retain identity/edits for reappearance. Imports are process-serialized and bounded to 1,000 rows/64 KiB requests. Dataset and serving publication remain separate; a post-publication index failure reports that the prior objects remain visible and asks for reimport. No durable job recovery or safe garbage collection is claimed.

The browser shell is public on loopback; data endpoints require a generated local key. Exact Host and Origin validation, restrictive CSP, no CORS, and server-created short-lived workload sessions protect the local boundary. The key and database password live only in ignored `.local/workspace/credentials.json`. Data files persist there alongside the warehouse. Restart detects an initialized PostgreSQL cluster rather than rerunning initdb.

Moved the existing real Iceberg subprocess adapter from the test helper into workers/iceberg/stage.ts, retaining the test re-export. Browser QA found Windows stdin decoding corrupted Unicode source text; the writer now explicitly enables Python UTF-8 mode. A real round-trip regression includes accented punctuation and Japanese text.

Verification: Windows `pnpm check` passes all 59 tests with no skips. The new real HTTP/PostgreSQL/Iceberg test checks missing credentials, hostile Origin/Host, denied imports, restricted queries and direct reads, denied actions, action replay, duplicate rejection, source-versus-overlay separation, refresh, database stop/start, disappearance/reappearance and reset-to-source. Browser verification imported four sample objects, saved a pump status override, refreshed source titles, restarted the local app, and confirmed the saved status remained. Windows restart was exercised manually as well as database restart in the test. This change has not been rerun on Linux. The local app is left running for review; no production deployment or Cloudflare changes were made.

Walkthrough and limitations: apps/local/README.md. Prior technical diagrams describe the October 15 backend; this checkpoint adds the local UI, trusted bootstrap and startup process above that foundation.

## Reusable app host and bundled React console — October 20, 2026

Replaced the local vanilla browser implementation with `apps/management`, a typed React/Vite static application. Preserved import, authorization-aware browsing, individual overrides, reset-to-source and protected pagination. Added an Applications page with verified release inventory and CLI recovery guidance. The local server now composes a generic static host, rather than reading files from a special local UI directory. Existing data/credentials and versioned overlays remain intact.

`services/app-host` stores content-addressed static artifacts, validates file hashes, promotes an atomic active pointer, retains previous releases, supports rollback, rejects unsafe paths/files and fails closed on corruption. Stable routes redirect into version-specific paths; HTML base paths keep assets tied to that version after later promotion. CLI commands bootstrap/recover the bundled console and publish/promote/rollback/verify/serve custom artifacts. Recovery works without the database, web server or console, quarantines damaged releases, and never modifies dataset storage. Mutation locks serialize each app's pointer/artifact changes; `apps unlock` checks that the owner PID is dead. Publication/recovery scratch files are isolated per build.

Moved console operations to `/management/v1/workspace`, `/management/v1/datasets/assets/import` and `/management/v1/apps`, retaining local aliases. Application inventory requires the current instance:manage grant/scope; dataset writes retain their own permission. Hosting an artifact grants no backend access. Custom `apps serve` uses a separate local origin and is static-only; scoped browser/API bindings remain required for a functioning data-driven custom app. No OIDC, generalized release-owned data bindings, production deployment, generated OSDK or arbitrary backend hosting is claimed. Static shells remain readable on loopback, with protected data endpoints; authenticated asset delivery is still a spec gap.

Windows `pnpm check` passes 62 tests without skips. Three added tests cover artifact lifecycle/HTTP integrity and routing, actual CLI bootstrap/recovery without infrastructure, and CSV parsing. The real PostgreSQL/Iceberg test now uses canonical management ingestion and checks operator-only inventory access. Browser QA confirms React edits survive source refresh and retained prior edits are visible; the management Applications page displays healthy active/previous releases. Linux was not revalidated. The running local server is left available for review.

The user-identified SNOTEL objective is recorded in docs/SNOTEL-NEXT.md with official NRCS/AWDB references and an end-to-end acceptance workflow. No SNOTEL connector or live observations have been implemented yet. Hosting/CLI operations and limitations are documented in docs/APP-HOST.md.

## Scoped SNOTEL application — October 20, 2026

Added the bundled `apps/snotel` React Snow Observatory and reusable `createScopedAppServer`. The separate loopback origin binds one app-specific key to trusted object services and a server-held workload session. It exposes only snow stations, observations and read-only sync status; browser-supplied profiles/bearers cannot expand authority. The management CLI/API initiates ingestion. Existing snow permissions survive bootstrap, including revocations. Both bundled applications can be rebuilt/recovered independently without the database or UI.

The official AWDB connector validates explicit station/date selections, daily SNWD/WTEQ units and dates, preserves missing data/quality flags, and records source request URLs, semantic JSON captures, hashes and retrieval timestamps. HTTPS retries rotate official DNS addresses with fresh sockets, preserve TLS hostname verification, and bound connection/response size/time. Three Iceberg dataset heads (raw, stations, observations) publish together; two serving object types and the successful-sync marker then publish atomically. Stable UIDs survive source corrections. Failed downloads leave prior serving data available. Dataset and serving publication remain separate; there is no automatic refresh schedule, durable job resumption or per-user OIDC.

React supports station search/state filtering, date selection, separate depth/SWE charts with missing-data gaps, latest reading dates, coverage, and a daily QC/QA table. Browser reload queries persisted Moria objects. Tested station switching, 30-day filtering, missing gaps and reload in an isolated test instance with clearly named TEST stations; these fixtures were never installed in the live instance.

Verification: Windows `pnpm check` passes 66 tests without skips, including real PostgreSQL/Iceberg correction/failure persistence, raw source retention, scoped HTTP denial, revocation surviving bootstrap, and rollback of multiple serving stages when completion fails. Both management and SNOTEL CLI recovery/verification were exercised. On the actual running origins, the snow key is rejected by management (401), snow management routes are absent (404), and a management-origin request to snow is rejected (403). Linux was not rerun.

Live acceptance is currently BLOCKED by NRCS upstream availability. Individual live Niwot readings were retrieved during investigation (April 1–3, 2025 depth 37/37/39 in and SWE 12.6/12.8/12.8 in), but no full sync completed: five-station seasonal and April windows, and a one-station three-day window returned timeouts/HTTP 502/504 after bounded retries. The official metadata endpoint also returned 502 in the browser. No manually reconstructed response or synthetic data was published to the live `snotel` environment. The running port-3211 app shows the failed sync and no completed dataset. Run `pnpm moria snotel sync` when AWDB recovers; live-source ingestion and populated-app acceptance remain outstanding. See docs/SNOTEL.md for operation and limitations.

## Live SNOTEL ingestion through Report Generator CSV — October 20, 2026

Resolved the preceding live-ingestion blocker using the official NRCS Report Generator CSV export. The no-argument CLI sync now selects `report-csv`; explicit configuration accepts `source: report-csv` or `source: awdb`. Existing explicit configs without a source retain AWDB semantics. CSV carries station identity, coordinates and elevation alongside daily SNWD/WTEQ values, eliminating the separate REST metadata dependency. Selection is explicit, not a silent fallback.

The connector validates daily/end-of-day report declarations, exact unit-bearing headers, stable station metadata, station identity, dates, duplicates and finite numbers. It handles quoted CSV fields, preserves reported zero versus missing days/blank values, and retains the downloaded CSV text with URL/hash/retrieval timestamp in Iceberg. Unknown quality flags, county and timezone offset stay unknown. The station timezone field is nullable, and UI attribution/notes distinguish CSV from AWDB rather than claiming REST suspect filtering or inventing metadata. Both paths reuse stable UIDs, scoped reads, transactional serving publication and outage preservation.

Live acceptance now passes: five Colorado stations (Niwot, Berthoud Summit, Loveland Basin, Vail Mountain, Fremont Pass), October 1, 2025–October 20, 2026, 1,925 station-days, 1,918 with both measurements. The source downloaded successfully in a probe and again through the operator CLI. Every depth/SWE value returned by the protected, paginated Moria API matched the independent download. After stopping/restarting the local server and PostgreSQL, all 1,925 persisted records still matched without fetching NRCS again. Browser QA verified real station switching, 30-day filtering, the full-period charts and source limitations. The live app is left running on 3211; the prior test-only screenshot is superseded by the real-data screenshot from this checkpoint.

Windows `pnpm check` passes 68 tests with no skips, including CSV parsing rejection cases and real PostgreSQL/Iceberg ingestion across the REST-to-CSV source transition, stable observation identity and exact raw CSV retention. Existing scope/revocation tests still pass. Linux was not rerun. Report Generator worked consistently during this verification, but future upstream availability is not guaranteed; the app reads persisted data independently of NRCS. Operational details are in docs/SNOTEL.md.

## Readable application addresses — October 20, 2026

The snow app now opens at `http://127.0.0.1:3211/snow/`. Startup and `snotel open` print that route; the origin root redirects to it. The reusable host serves the active HTML directly at stable application routes, with a no-store response and a release-specific HTML base so asset requests remain pinned to one immutable version. Existing `/releases/<hash>/` links remain supported. Promotion/rollback changes which release a stable page loads on its next navigation. Exact Host/Origin checks and scoped data authentication are unchanged.

Host lifecycle coverage checks the readable route, deep routes, noncached HTML, pinned asset base, missing assets, and rollback resolving to the prior release.

## Shared browser auth and Google provider foundation - October 20, 2026

Added maintained-library OIDC authorization code + PKCE, signed ID token validation, browser-bound single-use login transactions, issuer/subject identities, explicit invitations, PostgreSQL app-bound opaque sessions, encrypted server-held provider access tokens, logout and current authorization checks. The snow host uses cookies and the shared React session hook; legacy browser keys no longer authorize data. Provider configuration and session state are separate from application bundles. Local CLI setup, configuration, temporary recovery links and offline provider-disable recovery are available. Existing app recovery and ingestion commands remain independent of Google.

Local tests exercise a real HTTP OIDC issuer with JWKS/signatures and real PostgreSQL: wrong issuer/audience/nonce/signature/expiry, PKCE/state binding, callback replay, invitation rejection, email subject binding, session persistence across service recreation, fresh UserInfo checks and provider failure, grant/principal/session revocation, provider disable/re-enable, app binding, CSRF and cookie behavior. The real Iceberg/SNOTEL serving integration now exercises cookie recovery and rejects legacy keys on data endpoints. See docs/AUTH.md for setup and limitations.

Google live credentials/consent remain pending. UserInfo validation is not Workspace directory/group synchronization and does not claim the specification's full bounded external revocation guarantee. Management remains on local operator authentication. Production TLS/proxy support, untrusted-app hostname isolation, rate limiting, directory synchronization and periodic auth-row cleanup remain pending.

Validation: pnpm check passes all 70 tests on Windows. Browser QA verified signed-out/setup state, CLI recovery in an existing tab, station/chart loading, refresh, sign-out, and cookie-session persistence across a full local runtime restart. The existing five stations and 1,925 observations remain available. Live Google login awaits user credentials; Linux was not rerun.

Live Google verification completed: the invited account signed in through Google and the browser session endpoint reported recovery=false. A browser refresh retained login and loaded stations/charts. Read-only inspection of current PostgreSQL grants and application scopes found exactly snow-stations:read and snow-observations:read. Protected object queries using the live session context succeeded; authority checks rejected write/index/source-write/unrelated-read/action-invoke permissions. Browser requests for unrelated assets on the snow host returned NOT_FOUND; the management API rejected the same browser session as UNAUTHENTICATED. No grants or source data were changed during verification.

## Management Google authentication and explicit administrators — October 20, 2026

Management now shares the OIDC session host and React sign-in/sign-out flow, using a separate administrator invitation list and app-bound cookies. Explicit initial admin grants and workspace policy attributes enable asset ingestion, reads, edits/reset and app inventory. Caller-selected browser profiles and legacy keys no longer authorize management operations. Returning sign-ins and bootstrap preserve revocations. Object operations bind the administrative requirement to their authorization snapshot; imports and SNOTEL synchronization recheck initiating authority before publication.

CLI grant-admin configures invitations; auth recover --app management issues short-lived recovery access without Google. Provider disable and bundle recovery remain available. The snow application retains exactly its two read scopes. Updated management assets were published locally and recovery browser QA loaded the original four assets with persisted edits. No live source data was modified during this checkpoint.

Validation: pnpm format and pnpm check passed, with 71 tests and no skips on Windows. Real PostgreSQL/Iceberg integration covers Google-like OIDC admin sign-in, import/edit persistence, CSRF, cross-app session isolation, grant revocation, no regrant on login/bootstrap, mid-import session revocation and provider-independent CLI recovery. Live management Google consent remains pending the user adding the port-3210 callback to the existing Google Web client; the snow Google login was already verified. Linux was not rerun. Production hardening and directory synchronization remain pending as documented in AUTH.md.

## Public HTTPS ingress — October 20, 2026

Added opt-in HTTPS public listeners with explicit trusted origins, Secure cookie sessions, exact Host/Origin checks, HTTPS proxy enforcement and HSTS. Local CLI credentials and both recovery endpoints are rejected on public listeners; the existing localhost listeners and recovery remain available. Public management and snow reuse their existing provider bindings and distinct grants/scopes.

Published moria.gsequino.dev and snow.gsequino.dev through a dedicated healthy Cloudflare Tunnel to this Windows computer. Both public shells return 200; unauthenticated management data returns 401; recovery routes return 403; the snow host exposes no management route. Local CLI recovery ticket issuance still succeeds. Windows pnpm check passes all 72 tests with no skips, including a real-issuer public OAuth callback and PostgreSQL authority checks. Public Google consent remains pending registration of the new callbacks. This deployment depends on the running computer and two processes; no reboot service was installed. See PUBLIC-HOSTING.md for launch/recovery and remaining operational limitations.

## Generated OSDK and bundled-app migration — October 21, 2026

Added deterministic standalone TypeScript SDK generation from versioned app contracts, a browser-safe same-origin cookie transport, typed object reads/filters/projections and cursor pagination, explicit idempotency-key actions, operational endpoints and session helpers. Shared object schemas and action inputs feed both the server bindings and bundled contracts. Generated directories include a contract fingerprint and are checked for drift by pnpm check. Custom apps can run `pnpm moria sdk generate <contract.json> --out <directory> [--check]`; no runtime server dependency or secret is embedded.

Management and Snow now use their generated clients for data and auth. Snow's generated surface contains only station/observation reads and status; management exposes its declared asset actions and operational endpoints. Server-side authorization remains authoritative and unchanged. Snow requests use cancellation; management retains exact action input and idempotency key for uncertain retries. CLI provider recovery and bundle recovery remain available.

Validation: pnpm format and pnpm check pass on Windows, with 75 tests and no skips. Compile-time negative cases reject undeclared Snow capabilities, invalid fields/filter values/action inputs and missing mutation keys. Real HTTP/PostgreSQL/Iceberg tests exercise generated reads, pagination, imports, persistent edits, idempotent replay/conflict and session/grant revocation. Custom CLI generation and --check both pass. Published management release 823e7db9d0a38345346e585ca28b6538f276bbdecfc73505ca848abf993a0941 and Snow release eef2f3d1b90e3b9cb167fd6e1a1ac43d0fde65a711d70b6a56cb4903b3ac3b4a through existing CLI bundle recovery. Public browser Google sign-in succeeded for both updated apps: management displayed administrator status and all four assets with retained edits; Snow displayed five real stations and Niwot depth/SWE charts. No live data or grants were changed. Linux was not rerun.

Scope: this is generation from explicit served-app contracts, not automatic full-manifest/OpenAPI discovery or npm publication. Generated types do not perform response-schema decoding or grant authority. Agent credential transport, schema-version negotiation and broader ontology surfaces remain future work. See OSDK.md for contract format, usage and limitations.

## Durable execution, Front Range weather, console and lineage — October 21, 2026

Added a PostgreSQL-backed queue for fixed trusted SNOTEL and NWS workloads: persisted idempotent requests, coalescing, bounded retries, attempt history, worker fences/heartbeats and persistent schedules. Snow retries reuse staged Iceberg snapshots and publication receipts. Weather retains raw source receipts and atomically publishes forecasts with job completion. Serving data stays available during refreshes. CLI enqueue/status/retry/schedule commands and local recovery remain available.

Management now exposes Overview, Assets, Applications, Executions and Lineage. It includes Sync now, schedule controls, job results, operational health and job/lineage navigation. Data lineage reads recorded snapshots/publications/indexing/object generations; workflow lineage reads immutable app SDK declarations, explicitly distinct from usage or authority. All instance job/lineage APIs remain admin-only. A general resource explorer for non-administrators and folder permissions/marking inheritance are not implemented. Foundry research and the proposed phased design are documented separately.

Snow now includes five representative Front Range NWS forecasts, area comparison, nearest-loaded-station matching, date-labeled depth/SWE and exact one/seven-day changes. Missing reports are never zero-filled; changes are not new snowfall. Source age, period expiration, NRCS quality limitations, distances and actual report dates are visible. Cameron's nearest currently loaded station is about 38 miles away; more local station coverage is future work. No avalanche forecast or route safety inference is produced.

Initial durable release passed 82 tests and was deployed with an offline backup. Live scheduled NRCS ingestion succeeded with five stations/1,925 observations; scheduled NWS ingestion and a Google-admin Sync now both succeeded with five areas. Six-hour SNOTEL and hourly NWS schedules are enabled. Subsequent comparison/health/security work passed 87 tests with no skips, including real PostgreSQL, real Iceberg, subprocess crashes, OIDC fixture sessions, direct route probes, independent app/user grant revocation, cross-app/invalid cookies, CSRF and lineage payload exclusion. Live forced interruption was not exercised; recovery evidence is limited to the isolated tests and ordinary deployment restart. Ordinary deployment restart retained both Google browser sessions; no claim of a successful live forced-crash test is made.

Read-only live audit confirmed Snow scope and all Snow grants remain exactly snow-stations:read and snow-observations:read. Four assets retain the exact prior effective-content SHA-256 (bb301deedd0b7625635a14055a7c84bb515ddee8730b8dd50b77ba48771bf3fd). Both local CLI recovery-ticket paths work. Public browser QA confirmed management administrator access, health-to-job navigation, Snow read-only access and workflow declaration navigation. Browser QA caught and corrected an observation-date label that incorrectly showed the requested coverage date instead of the selected report date.

An isolated clone of the pre-durable backup verified all 1,187 files and twelve app releases, recovered four assets with matching content hash, two overlay versions, five stations and 1,925 observations, and exercised both local recovery sessions. The backup predates jobs/weather. Iceberg files were byte-verified, not read from relocated paths; absolute warehouse paths still constrain restoration. See BACKUP-RESTORE.md and scripts/verify-backup.ts.

The examples/station-explorer starter includes its own generated station-only SDK, React session/data flow, immutable workflow declaration, build/hosting/rollback walkthrough and negative type examples. Separate typecheck/build/SDK drift/isolated AppHost/HTTP smoke verification passes. Static publishing still does not create a custom API or identity; the required explicit server/auth binding is documented. Production apps were not replaced by the starter.

Limitations: local Windows host must remain running, no OS reboot service, no arbitrary-code execution sandbox, no off-device backup/rotation, no warehouse relocation, no full OpenSnow parity, and no non-admin permission-filtered lineage explorer. Static application contract metadata is public alongside JavaScript and must not contain secrets. Linux was not revalidated. See EXECUTION.md, ADMIN-CONSOLE.md, LINEAGE.md and examples/station-explorer/README.md.

Final additional-scope verification: pnpm format and pnpm check pass again after the date-label fix (87/87, zero skips; .local/overnight-check-final.log). Management active release is 56e8f85975c8061041e2586b61c4b5e9efe21d80db5dfe3fa1a48b846bdabcbd; Snow active release is 4f2fa80b4b98e7849b671de35feba8b0a4ac69e7e5cb670121fe57c49ab11eee. These UI-only promotions required no runtime restart or permission changes. The hourly follow-up remains active until the first morning run at/after 07:30 America/Denver, then is instructed to deliver the final overnight evidence and pause itself.

## Production architecture M0 draft — October 24, 2026

Added docs/production version 0.1: workload cards and calibration JSON, benchmark specification, service/state/authority boundaries, existing-contract disposition, failure matrix, cost model, decision register, and prioritized M1-M2 backlog. GitHub goal #1 and M0 #2 track delivery. Numerical targets are synthetic proposals, not measured ServiceNow capacity or accepted production SLAs. No runtime behavior or production deployment changed; no new capacity/failure/recovery benchmark was run.

Validation: JSON parsing, request mix totals, monotonic load ladders, arithmetic, local document links, and content checks passed. Existing source history and uncommitted implementation were preserved. GitHub source publication remains a separate reviewed prerequisite; M0 remains open for decision review and contract reconciliation.

## Production architecture M0 contract review — October 24, 2026

Version 0.2 makes shared and tier-specific workload inputs explicit, accounts for unsuccessful offered arrivals, separates retained-storage recovery from full warehouse reconstruction, and defines a non-overlapping cost ledger. Added acceptance traceability and mechanism proof gates; reconciled historical deployment defaults with production direction while preserving behavioral contracts. The v0.1 calibration remains unchanged. Source publication is complete at baseline 702daeb; M0 remains open for reviewed merge and explicit product-decision dispositions.

Validation: scripts/validate-production-workloads.py --self-test passes the current profile and rejects six invalid mutations; Black check passes. Local document links and Git whitespace checks pass. This validator checks contract invariants, not runtime throughput or capacity. No runtime behavior changed and no runtime suite, load, failover, or restore experiment was rerun for this review.

## Development quality gates — October 24, 2026

Added Linux PR verification with read-only workflow permissions and pinned Actions. The complete `pnpm check` gate now includes Python formatting, read-only specification validation, workload invariants, bundled app builds, and the Station Explorer type/build/hosting smoke test. The Linux image installs all gate dependencies; setup is exercised twice and the completion hook runs offline as a non-root user. Added OpenHands bootstrap/completion configuration, a PR template, bounded follow-up tasks, and human review rules. These controls do not grant production access or enforce GitHub branch protection.

Local validation: the complete pnpm check gate passed in a fresh short-path Windows copy with Node 24.19.0, pnpm 11.25.0, and Python 3.12.14: 87 runtime tests, zero failures/skips, formatting, SDK/type checks, Python validators, both app builds, and the starter smoke check. The deeply nested workspace failed native Windows path-length limits. Native Windows development support has been dropped; Linux is the supported development and hosting target. The earlier Windows run is historical evidence only. CI pins Python 3.11. Linux verification passed at revision 1f18c1a in [Quality run 40127887482](https://github.com/gcsequino/Moria/actions/runs/40127887482): image build, repeated bootstrap, and the complete offline non-root gate with 87 passing tests. Actual OpenHands-session integration remains unverified; CI exercises the setup and hook scripts directly. No production changes or capacity claims are included.

## PERF-01a calibration validation — October 24, 2026

Issue #23 adds reusable read-only v0.2 calibration parsing and explicit external-operation
capability checks in `benchmarks/workloads.py`. The existing document CLI delegates to the
same implementation. Structural errors identify fields before cross-field indexing;
provisional approval and accounting flags remain enforced. Full-profile operation coverage
requires all six API classes. Capability-scoped selection reports omitted classes, original
weight, and effective mix, without claiming certification. Usage is in
`docs/production/WORKLOADS.md`. Neither historical v0.1 nor provisional v0.2 calibration
values were changed. This is calibration input validation, not a complete run manifest.

Validation environment: non-root Debian Linux, Node 24.21.0, pnpm 11.25.0, Python 3.11.17
in `.venv`. The first setup invocation used the host's Python 3.13; Python 3.11 was then
provisioned locally, `.venv` recreated, and `PATH="$PWD/.venv/bin:$PATH" bash .openhands/setup.sh`
passed. The installed Docker CLI had no daemon, so validation used the non-root host.
`pnpm format`, Black on the changed Python files, and
`.venv/bin/python -m unittest benchmarks.test_workloads` passed (14 deterministic tests).
The final complete `pnpm check` gate passed with 87 runtime tests, zero failures/skips,
all 14 calibration tests, Python validation, both app builds, and starter verification.
The independent PR Linux quality result is recorded in the PR evidence.

Scope remains bounded to #23; parent #20 remains open. No load generator, traffic experiment,
new model invocation, migration, production access, deployment, or authorization/publication/
retention behavior change. Capability declarations do not prove runtime support or capacity;
background/data workloads, resolved run inputs, and approved gates remain future obligations.

## M1-01 external database lifecycle — October 25, 2026

Issue #13 depends on SRC-01 (#12) and is bounded to configuration, schema ownership
and existing management/Snow/auth behavior under accepted C07/C08 and S01/S08
contracts. External APIs use a scoped role, verify the version/checksum and close
only their pool on shutdown. Only the development launcher owns an embedded
cluster. Environment secret references, redacted errors, IPv6 URL normalization,
verified TLS defaults, connection timeouts and replica pool budgets remain intact.
See [database lifecycle](DATABASE.md) for final operator instructions and limits.

The updated scope supports fresh initialization and explicit versioned migrations
only. The runner rejects any existing unversioned `moria` schema before DDL,
including empty schemas. The reference-schema validator and compatibility
heuristics have been deleted, along with tests specific to that removed behavior.
The version-1 baseline SQL and checksum remain unchanged. Version 1 is currently
the only supported version; repeated application verifies it without changing
rows. There is no supported earlier versioned upgrade step in this release;
future steps require explicit reviewed migrations. Unknown/newer versions and
checksum mismatches fail closed.

The migration transaction retains its exclusive advisory lock. Runtime services
hold both a lifetime shared lease and independent shared locks on every checkout;
lease loss stops admission while active work drains under migration exclusion.
The real HTTP lease-loss regression remains. The four-connection minimum still
covers actual Snow and weather handlers (lease, supervisor, ingestion lock or
publication transaction, and nested query); tests exercise synthetic downloads,
real Iceberg publication, first-attempt completion and migration exclusion.
Configuration tests retain IPv6/TLS/credential protections and replica budgeting.

Replacement real PostgreSQL coverage checks empty, partial and complete unversioned
schemas. Two rejected attempts preserve catalog definitions and all table data,
leave no version marker, and cannot pass runtime verification. Existing populated
management/Snow/auth fixtures retain their rows through repeated versioned
migration. All unrelated authorization, transactional, restart and ownership
regressions remain. Only isolated synthetic fixtures are used; no credential file
or live installation is accessed. No protected paths or delivery policy changed.

Validation environment: non-root Linux, Node 24.21.0, pnpm 11.25.0 and repository
`.venv` Python 3.11.17, PostgreSQL 18.4; AMD Ryzen 7 9800X3D with 16 logical
CPUs. Synthetic correctness fixtures run on the shared host; no load/capacity
claim follows. Working tree is based on
`7633bc20e67abd9fdaa7855d3976e364980e1fd4`.

Validation completed:

- `pnpm format` passed.
- `pnpm exec tsx --test --test-name-pattern='unversioned|migration ownership' tests/sql.integration.test.ts`
  passed all four selected real PostgreSQL tests.
- Full `pnpm check` passed: 97 runtime tests, zero failures/cancellations/skips;
  formatting, SDK drift, TypeScript, Python formatting, specification validation,
  14 workload tests, both bundled builds and Station Explorer verification.
  Snow/weather minimum-pool handlers, external restart/edit preservation, OIDC,
  management authentication and lease-loss fencing all passed.
- `git diff --check` and the staged equivalent passed. Protected-path comparison
  against HEAD is empty. Baseline SQL is byte-identical to the provided checkout,
  preserving the version-1 checksum.

Full gate output is retained locally at `/tmp/issue-13-simplify-check.log`, with
focused output at `/tmp/issue-13-simplify-focused.log`; these are workspace logs,
not durable benchmark evidence. No required gate infrastructure was unavailable.
Remote TLS, actual IPv6 network connectivity, installation cutover/restore,
mixed-release operation and production capacity remain unverified. Human review
of migration ownership/role invariants and latest-revision Linux CI remain required
before manual merge. Changes remain uncommitted for the controller.

## Issue #44 — S3 streaming adapter

Bounded M2-01e work on `delivery/issue-44`, based on
`863a50896fddf4a34935ee404950f4bdcb077e42`, following dependency #42 and accepted
C04/C08 and storage authority contracts. The real MinIO conditional multipart
completion proof passed before adapter implementation. Added the pinned official
AWS SDK, checksum-verified isolated server provisioning, S3 implementation of the
existing streaming seam, shared authority/transfer validation, and real-provider
contract/fault tests. Detailed prerequisite provenance, guarantees, commands and
limitations are in [STORAGE-S3.md](production/STORAGE-S3.md).

Protected-path use is limited to the explicitly authorized package.json,
pnpm-lock.yaml and deploy/test/Dockerfile exception. Other changed paths are
services/storage/src, the two permitted new test suites and fixtures, and the
requested production/progress documentation. Existing authorization assertions,
required checks and delivery policy are preserved. No runtime/catalog/lease changes,
credential-file access, live installation, paid model call or publication action.

Verification uses non-root Linux, Node 24.21.0, pnpm 11.25.0 and repository `.venv`
Python 3.11.17. The official MinIO package was downloaded, checksum-verified and
extracted to `/tmp/issue-44-minio-dist`; local commands set
`MORIA_TEST_S3_BINARY=/tmp/issue-44-minio-dist/usr/local/bin/minio`.
The ordinary archive returned HTTP 410, so provisioning uses the official GitHub
release asset. Server readiness and fixture credential-copy bugs found during
focused testing were corrected. The focused shared/S3/authority run passed 30
tests with no skips; the final shared/S3 run passed 18 tests. `pnpm format` and
`pnpm typecheck` passed. Full gate results are recorded below.

The Docker image build is unverified: `docker version` cannot connect to
`/var/run/docker.sock`. Installing into `/usr/local/bin` is unavailable under the
container's no-new-privileges setting; the documented environment override uses
the exact verified executable without requiring root. Real local server tests
remain mandatory and ran successfully. Runtime integration, byte/Iceberg metadata
relocation, remote TLS/provider qualification and production lifecycle/recovery
remain #17 follow-ups. Independent exact-revision CI and human review remain
controller-owned prerequisites to manual merge.

Initial full `pnpm check` passed (344 tests, zero failures/skips, 104.086 seconds
runtime-test duration), including real PostgreSQL/Iceberg, SDK drift, TypeScript,
Python formatting/schema/workload checks, both application builds and starter
verification. Final review explicitly disabled all ambient SDK profile defaults
and enabled hard request timeouts. The added subprocess regression intercepts
profile/credential-file reads before access and observed zero attempts during real
signed read/write. The focused S3 suite passed five tests. A final full gate covers
these repairs; logs are `/tmp/issue-44-check-final.log`. Formatting-only braces and
Dockerfile command layout were also expanded for readability.

Final verification: `MORIA_TEST_S3_BINARY=/tmp/issue-44-minio-dist/usr/local/bin/minio pnpm check`
passed with exit 0: 345 tests, 345 passed, zero failures/cancellations/skips,
104.438 seconds runtime-test duration. Formatting, SDK drift, TypeScript, real
PostgreSQL and Iceberg tests, Python checks, both builds and starter verification
all passed. `pnpm format:check` and `git diff --check` also passed after final
readability edits. The Docker provisioning shell and embedded JavaScript pass
`bash -n` and `node --check`; an actual image build remains unverified for the
Docker-daemon limitation above. Logs remain local under `/tmp/issue-44-*`.
No required runtime test infrastructure was unavailable. Changes remain
uncommitted; independent latest-revision CI and human review are still required.

## Issue #47 — default Snow composition retirement

Before behavior changes, traced runtime, CLI, management, auth and execution
composition into the coverage-transfer matrix in
[SNOW-RETIREMENT.md](production/SNOW-RETIREMENT.md). Scope excludes catalog,
leases, migrations, generated files and protected build/delivery paths. Existing
standalone domain fixtures remain. Validation and any limitations follow below.

### Implemented scope and evidence — October 25, 2026

- Base checkout: `9f818d230cf95ed1c069d1a0aefaa02f94c10bc4`, branch
  `delivery/issue-47`. No local main or remote-tracking refs are available to
  establish current-main integration; no fetch, publication or merge was performed.
- Default combined/API/worker/scheduler composition no longer initializes Snow or
  weather. `createPlatformJobs` admits explicit trusted handlers, with an empty
  default registry. Jobs implementation, scheduling/fencing policy, catalog,
  storage and database history are unchanged.
- Management retains asset workflows, administrator authorization, lineage,
  application inventory and job history. Retry is offered only for registered
  handlers. Auth defaults to management; required-provider selection preserves
  shared declaration validation while ignoring unavailable unused credentials.
  Retired startup settings and POC CLI operations fail explicitly.
- Synthetic process fixtures retain real PostgreSQL effects, atomic completion,
  duplicate dispatch, independent restart, stale fences and drain assertions.
  New composition tests exercise real startup and two neutral hosted applications,
  cross-app token/origin denial, management isolation, retired routes and invalid
  retry input. Existing standalone Snow tests remain unchanged.
- Independent review initially identified missing setup invitation sequencing and
  retry ID validation before SQL. Both were fixed with regressions. Final review:
  zero findings. Existing promoted management assets require explicit
  `pnpm moria apps recover management`; startup does not overwrite them.

Validation on non-root Linux: Node 24.21.0, pnpm 11.25.0, workspace Python 3.11.17.
Temporary synthetic PostgreSQL clusters and fixture artifacts only; no live data,
production credentials, external model service or deployment was used.

| Command                                            | Result                                                                                                                                      |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `pnpm format`                                      | Passed; protected files unchanged.                                                                                                          |
| Focused auth/composition/execution/scheduler tests | 6 passed; no skips. Later auth/composition regression run: 4 passed.                                                                        |
| `pnpm check` after fixes                           | Passed: formatting, SDK drift, TypeScript, 348 runtime tests, Python checks, both builds and starter checks; zero failures/skips.           |
| `pnpm check:python`                                | Passed, including Black, specification/workload validation and 14 Python tests. Run separately because full gate stops at runtime failures. |
| `pnpm build`                                       | Passed for management and retained Snow fixture. This is required check coverage, not default startup composition.                          |
| `pnpm check:starter`                               | Passed type/build/hosting/static isolation checks.                                                                                          |
| `git diff --check`                                 | Passed.                                                                                                                                     |

Passing full output for this workspace is in `/tmp/issue47-check-latest.log`;
supplemental checks are in `/tmp/issue47-python.log`, `/tmp/issue47-build.log` and
`/tmp/issue47-starter.log`. Earlier full runs failed six S3 tests because
`/usr/local/bin/minio` was absent; it subsequently became available and the real
S3 tests passed. No infrastructure provisioning was performed by this task.
A final authorization recheck before retry-availability diagnostics has also
received zero review findings; its full rerun passed with 348 tests, zero failures
and zero skips, plus all subsequent gates (`/tmp/issue47-check-complete.log`).

Limitations: no storage tests were disabled or changed. Exact-head Linux CI and
integration with current verified main remain controller
gates; uncommitted local checks cannot establish either. No lease repair or PR39
changes were reused. The diff contains 32 files, below the 40-file bound, and no
protected-path edits. Historical domain/UI/generated/build and recovery fixtures
remain for a separate cleanup. This slice does not complete M1, M3 or parent #14.

### Issue #46 — published catalog metadata to scoped storage

Bounded M2-01f integration after #44: added `services/catalog/src/storage.ts`,
real integration coverage and an isolated database fixture, plus
`docs/production/CATALOG-STORAGE.md`. Existing Catalog, Authority, session and
scoped adapter APIs are reused without modifying existing runtime consumers,
publication/lease code, schemas, dependencies, protected paths or existing tests.
The service pins a coherent head/history pair, validates strict versioned logical
locations, and rechecks current session/epoch authority through bounded storage
reads. Effective markings and exact Dataset identity come from trusted service
bindings. No writer migration or automatic warehouse relocation is introduced.

Verification uses non-root Linux, Node 24.21.0, pnpm 11.25.0, repository `.venv`,
real embedded PostgreSQL and the preprovisioned checksum-pinned MinIO server.
Initial `pnpm typecheck` passed. The first focused run found a test setup error:
the local adapter requires its root to exist. Creating that synthetic temporary
root repaired the fixture; the next run passed 23 tests, zero skips. Additional
scope and failed-publication regressions were then added; the focused run passed
29 tests with no failures or skips. The final implementation also verifies a
head switch to different Iceberg-generated metadata bytes and validates
cancellation before provider initialization.

Independent no-findings review, exact-head Linux CI and integration with current
main remain unverified controller-owned gates. No commit, push, PR, merge, deploy
or external communication was performed. This slice does not complete #17:
manifest/data-file IO, logical-reference support in existing Iceberg consumers,
credential lifecycle and recoverable transitions remain follow-ups. No production
capacity claim is made.

Final local verification on October 25, 2026, against checkout baseline
`9f818d230cf95ed1c069d1a0aefaa02f94c10bc4` with uncommitted issue changes:

- `pnpm format`: passed; no existing source or protected files changed.
- `pnpm exec tsx --test tests/catalog-storage.integration.test.ts`: 29 passed,
  zero failures/cancellations/skips (before the final strengthening above).
- `pnpm check`: exit 0 on the final implementation; 374 tests passed, zero
  failures/cancellations/skips, runtime test duration 104.591 seconds. Real
  PostgreSQL, Iceberg, local filesystem and pinned MinIO tests all ran. Formatting,
  SDK drift, TypeScript, Python formatting/specification/workload checks, both
  application builds and starter type/build/hosting checks passed.
- `git diff --check`: passed. Only the issue's permitted files are changed.

Python is `.venv` 3.11.17. No required runtime infrastructure was unavailable.
Temporary evidence logs: `/tmp/issue-46-format.log`,
`/tmp/issue-46-focused.log`, `/tmp/issue-46-check.log`. These local logs are not
production or capacity evidence. Changes remain uncommitted for the controller;
the independent review, CI and current-main gates above remain outstanding.

### Issue #49 — durable scoped metadata publication

Bounded M2-01g seam after #46: `CatalogStorage.publish` stages acknowledged
create-only metadata through the existing authorized adapters and calls existing
`Catalog.publish` once for atomic publication. Only the allowed catalog storage
files, new integration tests and production/progress documentation change.
Catalog transaction/lease code, schemas, adapter internals, existing tests,
dependencies, protected paths and runtime consumers are unchanged.

Accepted C04/S01/S02/S03/S06/S07/S08 contracts inform current-session and epoch
checks, Dataset write authority, complete effective markings, terminal restricted
views, immutable content-bound logical identities and receipt-based retries.
Pinned PyIceberg validates bounded v2 metadata structure/table/current snapshot;
manifest/data-file verification and runtime adoption remain follow-up work.
Uncommitted partial or uncertain staging cannot be resumed over create-only
collisions: fence the old attempt and schedule a new execution/lease. No deletion,
GC, second receipt registry or alternate publication authority is introduced.

Tests use synthetic temporary warehouses, real embedded PostgreSQL, filesystem
and checksum-pinned isolated MinIO with real Iceberg-generated metadata. The
initial focused run exposed an upload chunk-size mismatch; the seam now emits
256-byte chunks compatible with the configured adapters. The next run exposed an
incorrect new-test expectation: session revocation increments the security epoch
and correctly returns AUTHORIZATION_CHANGED. That expectation was corrected;
existing assertions were not edited. Additional barriers check session expiry
inside the final transaction and cancellation of stalled engine intake.

Verification environment: non-root Linux, Node 24.21.0, pnpm 11.25.0,
repository `.venv` Python 3.11.17. Commands: `pnpm format`, `pnpm typecheck`,
`pnpm exec tsx --test tests/catalog-storage-publication.integration.test.ts`,
`pnpm check`, `git diff --check`. Final results are recorded below after execution.
Independent no-findings review, exact-head Linux quality CI and current-main
integration remain unverified controller-owned gates. Changes remain uncommitted;
no publication, deployment, live installation access or external communication
was performed. #17 remains open. This is not a capacity or full Iceberg IO claim.

Final local results on October 25, 2026, against baseline
`42491b01e691802becbc4fbf57eb706f84e487b9` with uncommitted issue changes:

- `pnpm format`: passed; no existing tests or protected files changed.
- Focused publication integration suite: 32 tests passed, zero failures,
  cancellations or skips; 40.839 seconds. Both real providers and PostgreSQL ran.
- Initial full `pnpm check`: exit 0, 406 tests passed. Final strengthening checks
  both failed output heads and unchanged checkpoint state, in addition to absent
  history and receipt, and expands test control flow for readability.
- Final `pnpm check`: exit 0, 406 passed, zero failures/cancellations/skips;
  runtime-test duration 161.732 seconds. Formatting, SDK drift, TypeScript,
  PostgreSQL/Iceberg/filesystem/pinned-S3 regressions, Python formatting and
  specification/workload checks, both builds and starter verification passed.
- `git diff --check`: passed. The five changed files are within issue scope.

No required local runtime infrastructure was unavailable. Logs are local at
`/tmp/issue-49-format.log`, `/tmp/issue-49-focused-final.log` and
`/tmp/issue-49-check-final.log`; they are not production or capacity evidence.
Independent review, exact-head Linux CI and current-main integration remain
unverified as noted above; local success does not waive those acceptance gates.

### Issue #49 verification repair — lineage authorization

The review finding was confirmed against C04 and S01/S02: the publication success
fixtures used empty lineage and did not exercise input read permission or the
inherited-marking union. Added bounded integration coverage in the existing
publication suite for both real filesystem and isolated pinned S3 adapters with
real PostgreSQL and PyIceberg-generated metadata. No production implementation,
existing assertions, dependencies, protected files or delivery policy changed.

New cases declare a nonempty input snapshot map and assert exact persisted
lineage, producing execution, output snapshot IDs and revisions for both outputs,
read back the metadata through CatalogStorage, and retry without changing the
committed catalog state. Separate denials remove only the input read grant, only
the input application scope, or the inherited marking from only the second
output while the principal retains every marking. Each denial asserts zero
writes, identical heads/history/checkpoint/receipt state and invisible outputs.
These fixtures exercise trusted declared provenance; they do not claim full
input scheduling or manifest/data-file reader integration.

Validation commands: `pnpm format`,
`pnpm exec tsx --test tests/catalog-storage-publication.integration.test.ts`,
`pnpm check`, and `git diff --check`. Final results are recorded below.
Independent review of the repaired revision, exact-head Linux quality CI and
current-main integration remain controller-owned and unverified. #17 remains
open; all changes remain uncommitted.

Repair validation results (October 25, 2026):

- `pnpm format`: passed.
- Focused publication suite: exit 0; 40 tests passed, zero failures,
  cancellations or skips (63.870 seconds).
- Final `pnpm check`: exit 0; 414 runtime tests passed, zero failures or skips
  (179.665 seconds). Formatting, SDK drift, TypeScript, real PostgreSQL and both
  storage providers, workspace `.venv` Python checks, both builds and starter
  verification passed. No required local infrastructure was unavailable.
- `git diff --check`: passed. Repair edits are limited to the publication
  integration test and this progress log; pre-existing issue changes remain.

Local logs: `/tmp/issue-49-repair-format.log`,
`/tmp/issue-49-repair-focused.log`, `/tmp/issue-49-repair-check.log`.

### Issue #47 — explicit null workload input repair

Review repair on October 25, 2026, against checkout baseline
`03df0c9f71e8fee1ad3778b1964ae15eeef99fc9`. The existing coverage-transfer matrix
in `docs/production/SNOW-RETIREMENT.md` remains applicable. Scope is limited to
the management enqueue route, its existing synthetic execution process test,
and this implementation record; no Jobs, lease, storage or protected-path changes.

The real PostgreSQL regression reproduced explicit `input: null` being stored as
`{}` before the fix. The route now defaults only omitted input. Assertions verify
persisted null, same-key null replay, 409 `IDEMPOTENCY_CONFLICT` for explicit empty
object and omitted input against that key, and unchanged persisted input after
conflicts. Omitted input still persists as `{}` and replays with explicit `{}`.
Existing independent process restart, scheduling, fencing, recovery and drain
assertions remain intact.

Validation uses non-root Linux, Node 24.21.0, pnpm 11.25.0 and workspace `.venv`
Python 3.11.17 with synthetic PostgreSQL fixtures. `pnpm format` passed. The
focused `pnpm exec tsx --test tests/execution-process.integration.test.ts` failed
on the expected stored-payload assertion before the fix, then passed with zero
skips after the fix. Evidence: `/tmp/issue47-payload-before.log`,
`/tmp/issue47-payload-after.log` and `/tmp/issue47-payload-format.log`.

Full `pnpm check` passed (exit 0): 377 runtime tests, zero failures or skips,
formatting, SDK drift, TypeScript, Python checks (14 tests), both application
builds and starter checks. Evidence: `/tmp/issue47-payload-check.log`.
`git diff --check` passed. No required runtime infrastructure was unavailable.
Independent review, exact-head Linux CI and current-main integration remain
unverified controller-owned gates. Changes
remain uncommitted; no publication, merge, deployment or live data operation was
performed. Historical Snow fixtures and the remaining retirement scope are
unchanged.

## Issue #53 — deterministic local lease supervision regression

Scope is limited to `local lease supervision renews a live fence and aborts on
replacement` in `tests/sql.integration.test.ts` and this record. Baseline:
`42491b01e691802becbc4fbf57eb706f84e487b9`, branch `delivery/issue-53`.
Read `withLease`, `Catalog.claim`/`renew`, the indexing supervision caller,
accepted C04/R01 contracts and production acceptance obligations before changing
the test. No production semantics, schemas, deadlines, dependencies, delivery
policy, protected paths or other tests changed. The #37 repair loop remains
stopped; #47/#50 remain blocked pending this repair's merge and integration.

Diagnosis on October 25, 2026 used the original test with temporary observation
of real `Catalog.renew` and a synthetic 1100 ms event-loop stall at work entry.
Command: `pnpm exec tsx --test --test-name-pattern='^local lease supervision renews a live fence and aborts on replacement$' tests/sql.integration.test.ts`.
Recorded execution order in `/tmp/issue-53-race.log`:

- Startup renewal began at 0.155 ms and succeeded at 1.325 ms.
- Work entered; the event loop resumed at 1101.619 ms after the injected stall.
- The scheduled renewal began at 1101.922 ms and failed at 1105.303 ms with
  `MoriaError: Worker lease expired or was replaced`, code `LEASE_LOST`, at
  `services/catalog/src/catalog.ts:170`.
- The test failed before replacement was attempted (one failure, zero skips).

This reproduces the expiry mechanism under controlled scheduling starvation,
not a captured historical full-suite failure. A one-second PostgreSQL lease
expires against `clock_timestamp()` while a 333 ms Node timer can be delayed
past that expiry under contention. Renewal correctly rejects that expired
lease. The original 2200 ms and 450 ms sleeps neither synchronize SQL completion
nor reserve the event loop. A `LEASE_LOST` after actual replacement or expiry is
required behavior, not automatically a flaky error. Diagnostic instrumentation
and the synthetic stall were removed.

The regression now controls only Node `setTimeout`, leaving PostgreSQL time and
all Catalog operations real. It uses the existing 60-second default lease and
unchanged 15-second test timeout. A call-through Node spy observes each renewal
promise; after SQL completion an unmocked `setImmediate` turn lets supervision
schedule its successor or propagate cancellation before the next controlled
tick. Four 20-second ticks each require exactly one renewal with the original
execution/worker/fence and default duration. Before each tick, SQL sets the
synthetic row's expiry to 30 seconds ahead (longer than the entire test timeout)
so every renewal must observably extend it. Assertions check SQL liveness,
strict expiry extension and exact worker/fence, rather than relying on timestamp
rounding or wall-clock sleeps. Work completion must remove the timer and preserve
the exact SQL expiry through another 40 seconds of controlled scheduling.

Replacement is ordered explicitly: expire the original row, claim through
Catalog, assert the live replacement and fence increment of exactly one, then
advance the old worker's renewal. It must abort with `LEASE_LOST` without changing
the replacement row. Stale startup is rejected. A separate expired-start probe
uses the current replacement worker/fence and verifies `LEASE_LOST`, no entry to
work and no resurrection or mutation of the expired row. This distinguishes
expiry from fencing replacement with real SQL effects.

Validation uses non-root Linux, Node 24.21.0, pnpm 11.25.0, workspace `.venv`
Python 3.11.17 and disposable embedded PostgreSQL with synthetic data. No model
inference is needed. Initial focused validation passed. Initial `pnpm typecheck`
reported TS2769 because the spy's return type permits undefined; an explicit
assertion now requires the actual Catalog promise before awaiting it. Twelve
focused runs before that additional assertion passed, four concurrent fresh
processes/databases at a time, with no retries, failures or skips. Final repeated
validation and the complete gate are recorded below.

Limitations: this checkout has no local `main` or remote-tracking refs, so current
main freshness is unverified. Independent-environment full validation and
independent assertion review are not established by same-host repeated tests.
Those gates, Linux quality CI on the exact published head, current-main
integration and merged-main validation remain mandatory controller-owned work.
No commit, push, PR, merge, deployment, live-data operation or external contact
was performed. Changes remain uncommitted; this record does not unblock #47/#50.

Executed validation:

- `pnpm format`: passed with changes confined to the scoped test. Initial
  `pnpm typecheck` failed as described above; the complete gate subsequently
  passed TypeScript checking.
- The focused command above ran another twelve times after the promise assertion,
  using Python `ThreadPoolExecutor(max_workers=4)` to launch twelve independent
  `subprocess.run` calls (no retry). All twelve exited 0 with one test passed,
  zero failures/cancellations/skips each. These runs overlapped `pnpm check`.
  Evidence: `/tmp/issue-53-final-repeat-{1..12}.log`; the preceding batch is
  `/tmp/issue-53-repeat-{1..12}.log`.
- First full `pnpm check`: exit 0, 374 runtime tests passed, zero failures,
  cancellations or skips; runtime test duration 115.906 seconds. Formatting,
  SDK drift, TypeScript, Python formatting/specification/workload checks,
  application builds and starter checks all passed. Evidence:
  `/tmp/issue-53-check.log`. All required local runtime infrastructure was
  available. After expanding the observer's if/else into readable blocks, the
  complete gate was run again on the final source; result follows below.
- `git diff --check`: passed. No protected paths changed.

Final-source `pnpm format` and `pnpm check` both exited 0. The final gate passed
374 runtime tests with zero failures/cancellations/skips in 99.111 seconds,
including the revised real-PostgreSQL regression; all other gate stages passed.
Evidence: `/tmp/issue-53-final-format.log` and `/tmp/issue-53-final-check.log`.
The two complete gates ran in this implementation environment; neither is
claimed as independent-environment validation or an independent review.

### Issue #53 verification repair — October 25, 2026

Verified the review finding against `withLease` and its actual Catalog and
indexer callers: after abort, the callback catch replaces any callback error
with `signal.reason`. An assertion inside that callback can therefore be
misclassified as the expected `LEASE_LOST`. The existing expiry diagnostic log
was also inspected; its recorded SQL failure precedes replacement, as described
above. No production defect or change to lease semantics is required.

The replacement phase now uses explicit work-entry and work-completion promises.
All replacement, renewal-barrier and post-abort assertions execute outside the
callback. The captured replacement snapshot is compared after supervision rejects
with `LEASE_LOST`, including its exact expiry, worker and fence. Cleanup releases
and drains work even when an assertion fails, preserving the original assertion
error. The live-renewal, stopped-timer, stale-start and expired-start checks and
15-second timeout remain intact. Changes remain limited to the scoped test and
this record.

Validation commands and evidence:

- `pnpm format`: exit 0 (`/tmp/issue-53-review-format.log`).
- `pnpm exec tsx --test --test-name-pattern='^local lease supervision renews a live fence and aborts on replacement$' tests/sql.integration.test.ts`:
  exit 0, one pass, zero failures/cancellations/skips
  (`/tmp/issue-53-review-focused.log`).
- The same focused command ran twelve times via workspace `.venv/bin/python`,
  `ThreadPoolExecutor(max_workers=4)` and independent `subprocess.run` calls,
  overlapping the complete gate. All twelve exited 0, each with one pass and
  zero failures/cancellations/skips. No retries. Logs:
  `/tmp/issue-53-review-repeat-{1..12}.log`.
- Temporary test copies at `tests/.issue-53-negative.ts` used the same focused
  command with that path. The staged pre-repair test passed despite replacing
  the expected replacement snapshot with an intentionally wrong object (exit 0).
  The repaired test rejected that mutation (exit 1, `ERR_ASSERTION`). Separate
  mutations forcing unconditional assertion failures at the post-abort check
  and inside the lost-renewal barrier also exited 1 with `ERR_ASSERTION`, rather
  than being accepted as `LEASE_LOST`. Logs:
  `/tmp/issue-53-review-negative-{baseline-row,repaired-row,repaired-abort,repaired-barrier}.log`.
  Temporary copies were removed; the actual test was not mutated.

These checks use real disposable PostgreSQL and synthetic data on the same
non-root Linux host and workspace `.venv`; no model smoke test is needed. They
are not independent-environment validation or independent review. This checkout
still has only `delivery/issue-53` at baseline
`42491b01e691802becbc4fbf57eb706f84e487b9`; current-main freshness cannot be verified
from local refs. Independent validation/review, exact-head Linux quality CI,
current-main integration and merged-main validation remain outstanding and
controller-owned. No publication or merge was performed; #47/#50 remain blocked.

Full `pnpm check` on the repaired source exited 0: 374 runtime tests passed,
zero failures/cancellations/skips, runtime duration 108.864 seconds. Formatting,
SDK drift, TypeScript, Python formatting/specification/workload validation,
both application builds and starter checks passed. Evidence:
`/tmp/issue-53-review-check.log`. Required local runtime infrastructure was
available; independent-environment runtime validation remains unverified.
Final `git diff --check` passed. Only the authorized test and this record differ
from HEAD; no protected paths changed. Changes are left uncommitted for the
controller, preserving the pre-existing staged changes.

### Issue #47 empty-history guidance repair — October 25, 2026

Confirmed that Overview and Executions both mount `Platform` and share its loaded
empty-history message. That message still directed users to removed sync and
schedule controls. Replaced it with guidance that executions come from explicitly
configured workloads and no bundled workloads run by default. The loading state,
filters, historical job visibility and execution behavior are unchanged.

Reviewed the retirement coverage matrix and accepted runtime contracts R01/R06.
Source inspection confirms both views use the corrected block, and searching
management source finds neither obsolete instruction. This bounded copy repair
changes only `apps/management/src/platform.tsx` and this record; no tests or
protected paths are changed. Browser interaction was not independently exercised.

Validation: `pnpm format` passed (log: `/tmp/issue-47-guidance-format.log`). Full
`pnpm check` results are recorded below. Independent review, exact-head Linux CI
and current-main integration remain controller-owned and unverified in this repair.
Changes are left uncommitted; no publication, merge or live data operations occurred.

Full `pnpm check` exited 0: 377 runtime tests passed with zero failures,
cancellations or skips. Formatting, SDK drift, TypeScript, Python checks, both
application builds and starter checks passed. Required local runtime infrastructure
was available. Evidence: `/tmp/issue-47-guidance-check.log`.
Final `git diff --check` passed; only the two scoped files above differ from HEAD.

## Issue #56 — scoped pinned Iceberg snapshot reads (October 25, 2026)

Bounded scope: additive CatalogStorage trusted reader composition, dedicated
TypeScript/Python scoped readers, disposable Iceberg fixtures/integration tests,
and storage/worker documentation. Dependencies are the existing #49 catalog pin
and publication integration, Authority and authorized StreamingStorage adapters,
PyIceberg 0.12.0 and PyArrow 26.0.0. No dependencies, schemas, adapter internals,
protected delivery paths or existing runtime consumers changed.

The implementation pins existing catalog provenance, uses exact immutable
service-owned reference mappings, mediates every manifest/Parquet range in the
parent, and checks current authority before releasing rows. The fixed Python
entrypoint has an allowlisted environment, serialized bounded IPC, process memory
and CPU limits, bounded row frames and deterministic child cleanup. The initial
fixture run exposed physical names in fixture output handles; those handles now
preserve logical names in genuine Iceberg manifests. A malformed-reference test
was corrected to target the current snapshot's manifest list as well as retained
history. Neither finding was resolved by relaxing reader denial.

Verification on the uncommitted working tree based on
`c531f72e19b8d557e12160eb4a4e5ed16591c77d`, non-root Linux, Node 24.21.0,
pnpm 11.25.0 and workspace Python 3.11.17:

- `pnpm typecheck` passed.
- `pnpm exec tsx --test tests/iceberg-scoped-storage.integration.test.ts` passed
  all 27 cases, zero skips, using disposable PostgreSQL and both real adapters.
  Synthetic inputs include two 150-row files with exact int64/decimal/date/time/
  array values, a competing retained snapshot and a row exceeding the frame limit.
- `pnpm format` and `.venv/bin/python -m black workers benchmarks docs/spec/schemas
tests/fixtures/iceberg-scoped-storage` completed; fixture Black check passed.
- Full `pnpm check` passed: 444 tests, zero failures/cancellations/skips, plus
  formatting, SDK drift, TypeScript, Python/spec validation, both application
  builds and starter build/hosting smoke. Local command output is retained at
  `/tmp/issue-56-check.log` for this workspace session.
- `git diff --check` passed. Only the issue's allowed source/test/documentation
  paths are changed; existing physical-path and storage regressions remain intact.

Limits: fixture-scale unpartitioned, unsorted v2 Parquet snapshots with no deletes;
no query row policy, arbitrary-code isolation, production capacity or runtime
rollout claim. Mapping provisioning and immutable retention are trusted composition
obligations. Database waits remain cooperative. Independent-environment execution,
clean independent review, current-main integration and Linux CI on the exact head
are unverified here and remain controller/human gates. #17 remains open. Changes
are left uncommitted; no publication, deployment or live-installation work occurs.

## Issue #55 — independent static-host lifecycle

Scope: standalone `apps/local/src/static-host.ts`, a shared static listener used
by the existing custom-app CLI, process integration fixtures and delivery docs.
The accepted static-host authority boundary and existing AppHost/serveApp
contracts remain unchanged. Required explicit app/directory/port configuration
serves only verified promoted artifacts on loopback. Startup performs no build,
bootstrap, artifact mutation, database initialization or backend composition.
The shared listener retains exact Host/Origin checks, CSP, safe asset paths and
release pinning. Signal shutdown closes incomplete connections as well as the
listener. CLI selection and port restrictions remain intact.

The process tests use temporary synthetic artifacts, credential-free static
children, and a separate real API child with scoped database credentials supplied
only in its environment. The API regression uses disposable PostgreSQL and a
synthetic asset imported/read through authenticated APIs. It checks static
non-dispatch, restart independence and continued database availability. Generic
hosting tests are unchanged. No protected paths, API/runtime composition, grants,
schema, SDK, dependencies or package scripts were changed.

Validation in this checkout (Node 24.21.0, pnpm 11.25.0, non-root Linux, repository
`.venv`): `pnpm format` completed. Targeted process tests and full `pnpm check`
results are recorded below after completion. Initial fixture failures were fixed
by using raw HTTP for a wrong Host header (fetch rewrites it) and a scoped runtime
PostgreSQL role; no production checks were relaxed.

Limitations: this is local static lifecycle evidence, not production ingress or
cross-origin management support. Same-origin `/api` needs separate trusted proxy
routing to the API. No deployment, proxy configuration, external provider/model
call, live state, capacity claim or publication is included. #14/M1 remains open;
environment/stream ownership and remaining service separation are later work.
Independent-environment validation, independent review with no findings,
current-main integration and exact-revision Linux quality CI are unverified here
and remain mandatory controller gates. Changes are left uncommitted.

Completed validation on 2026-10-25: targeted
`pnpm exec tsx --test tests/static-host-process.integration.test.ts` passed all
3 tests. Full `pnpm check` exited 0: formatting, SDK drift, TypeScript, all 380
runtime tests (0 failures, 0 skips), Python formatting/specification/workload
checks, both application builds and Station Explorer type/build/hosting checks
passed. `git diff --check` passed. Existing generic hosting coverage ran intact.
No required local infrastructure was unavailable. Independent-environment and
controller gates above remain unverified; this is not a claim of issue closure.

### Issue #55 malformed-request repair — October 25, 2026

Confirmed that URL parsing in `serveApp` precedes its internal error boundary.
Added a real child-process raw-socket regression sending `GET http://[ HTTP/1.1`
with the correct loopback Host. Before the repair, the regression failed because
the connection ended without an HTTP response. The shared static listener now
catches request-handler failures, returns an uncached 400 for invalid URLs or a
generic 500 for other uncaught failures, and closes the affected connection.
If headers were already sent, it destroys that response instead of writing a
second status. Error details are not returned. Host/Origin checks and shared
artifact serving remain unchanged.

The regression asserts the controlled 400, then a successful valid request with
the expected promoted version from the same still-running process. Targeted
`pnpm exec tsx --test tests/static-host-process.integration.test.ts` passed all
3 tests, including the real API/PostgreSQL lifecycle regression, with no skips
(log: `/tmp/issue-55-repair-targeted.log`). `pnpm format` and `git diff --check`
passed. Full `pnpm check` exited 0: all 380 runtime tests passed with no failures
or skips, including unchanged generic hosting tests. Formatting, SDK drift,
TypeScript, Python checks using the workspace `.venv`, both application builds
and starter checks passed (log: `/tmp/issue-55-repair-check.log`). Required local
runtime infrastructure was available.

This repair changes only the shared listener, its scoped process test and this
record. Existing staged issue work is preserved. No protected paths were changed
and no credential files were accessed. Independent-environment validation, independent review,
current-main integration and exact-revision Linux quality CI remain unverified
controller gates. #14/M1 remains open; changes are uncommitted.

### Issue #56 authority-wait repair — October 25, 2026

Confirmed the scoped reader awaited current authority without an abort race and
passed only the caller signal through CatalogStorage, omitting the engine's
internal deadline. Scoped authority checks now race the combined scan signal,
remove their abort listener on every outcome, observe late promise rejection and
recheck cancellation before allowing output. CatalogStorage forwards that signal
and denies cancelled checks before/after its current-authority transaction and
before session validation when a blocked transaction resumes. No authorization
failure becomes permission to read. Existing catalog transaction ownership,
physical-path readers, adapters, dependencies and protected paths are unchanged.

Added real PostgreSQL environment advisory-lock contention cases for both local
and isolated S3 storage, before worker startup and with buffered rows. Each test
observes an actually blocked authority backend, then verifies deadline or caller
cancellation rejects pending iteration and completes iterator return while the
lock remains held. It checks no further storage opens, no remaining child or
caller abort listener, and then releases the lock and verifies pool and advisory
lock cleanup. The underlying catalog transaction is not forcibly cancelled: its
connection drains after lock release or the existing lock timeout. Initial
catalog pinning precedes the engine deadline; deployment database/pool timeouts
remain required. This repair does not claim general database cancellation.

Validation: `pnpm format`, `pnpm typecheck` and `git diff --check` passed.
`pnpm exec tsx --test tests/iceberg-scoped-storage.integration.test.ts` passed
all 35 tests with no skips, including eight real contention cases
(log: `/tmp/issue-56-repair-targeted.log`). Full `pnpm check` exited 0:
all 455 runtime tests passed with zero failures/skips, plus formatting, SDK drift,
TypeScript, Python formatting/specification/workload checks using the workspace
`.venv`, both application builds and Station Explorer type/build/hosting checks
(log: `/tmp/issue-56-repair-check.log`). Required local infrastructure was available.
Final `git diff --check` passed.
Independent-environment checks, clean independent review, current-main integration
and exact-revision Linux CI remain mandatory unverified controller gates. #17
remains open. Changes remain uncommitted; no credential files were accessed.

### Issue #56 post-exit authority-wait repair — October 25, 2026

Confirmed that successful child close removed the engine deadline timer and caller
abort listener before the final authority check. Their lifetime now extends through
that check to the existing iterator cleanup. No authority or storage semantics,
dependencies, protected paths or existing consumers change.

Extended the real PostgreSQL contention regressions to the post-exit phase for
local and isolated pinned S3 storage. A test-only transaction wrapper acquires an
exclusive environment lock once the actual reader child has exited, then delegates
to the real catalog transaction. Tests observe the blocked PostgreSQL backend and
require deadline/cancellation rejection, iterator completion, no further storage
opens and child/listener cleanup before unlocking. Pool and advisory-lock cleanup
are checked after unlocking. The underlying SQL transaction still drains after
contention clears; this is not general database cancellation.

Before the repair, all four new post-exit cases failed with missing expected
rejection (log: `/tmp/issue-56-post-exit-before.log`). After the repair,
`pnpm exec tsx --test tests/iceberg-scoped-storage.integration.test.ts` passed all
39 tests with zero failures/skips (log: `/tmp/issue-56-post-exit-targeted.log`).
`pnpm format`, `pnpm typecheck` and `git diff --check` passed. Full `pnpm check`
exited 0: all 459 runtime tests passed with zero failures/skips, plus formatting,
SDK drift, TypeScript, Python formatting/specification/workload checks using the
workspace `.venv`, both application builds and Station Explorer type/build/hosting
checks (log: `/tmp/issue-56-post-exit-check.log`). Required local infrastructure
was available. Final `git diff --check` passed.

Independent-environment validation, clean independent review, current-main
integration and exact-revision Linux CI remain unverified controller gates; #17
remains open. Changes are left uncommitted.

## Issue #58 — API-only management artifact lifecycle separation (2026-10-25)

Scope: the existing runtime skips management bootstrap for API-only, and the
shared server accepts an optional static-serving composition flag with its
existing behavior as the default. Both local and public API-only listeners
return controlled uncached JSON 404s for UI/static/release routes. Auth and API
routing retain the existing Host/Origin, trusted HTTPS proxy, administrator,
session, CSRF, recovery and protected operation checks. Combined development
still bootstraps/serves management; execution-only roles remain independent.
AppHost inventory and lineage are read-only at API startup/request time.

Dependency: the #55 standalone static lifecycle already present in this checkout.
No schema, dependency, Catalog/storage/engine, Jobs/lease, generated-client,
protected-path or delivery-policy changes. No existing tests changed. Acceptance
is bounded to lifecycle separation, not management/object privilege separation,
multi-environment scheduling or arbitrary-code execution.

New real child-process/PostgreSQL coverage uses a disposable synthetic cluster,
scoped runtime grants and the repository-local Python 3.11 Iceberg worker.
Runtime and existing API entrypoint start without UI source/dist or promoted
artifacts; a fixture rejects importing the UI builder. Coverage includes real
recovery, inventory/lineage/object reads, Iceberg import, action mutation,
unauthenticated/wrong-origin/non-administrator session denial, current-grant
revocation, public binding/recovery checks, non-serving after later promotion,
and static/API/external-database lifecycle independence. No live installation,
credential-file inspection, provider account or model API is used.

Local validation on non-root Linux, Node 24.21.0, pnpm 11.25.0 and workspace
Python 3.11.17, based on checkout revision
`2b8d405144bafad1c14d9724b35767e70db7ed0c` plus these uncommitted changes:

- `pnpm format`: passed; no out-of-scope changes.
- `pnpm exec tsx --test tests/api-only-process.integration.test.ts`: passed,
  one real process/PostgreSQL test, no skips (4.35 seconds).
- Final `pnpm check`: passed, exit 0. All 421 runtime tests passed with zero
  failures or skips (148.22 seconds). Formatting, SDK drift, TypeScript,
  Python validation/formatting and all 14 Python tests, both application builds
  and starter verification passed. Existing combined-mode, scheduler/execution
  and static-process regressions passed unchanged.
- `git diff --check`: passed. Required local infrastructure was available.

Initial fixture runs corrected flat object-field assertions, allowed the existing
inventory operation to create an empty AppHost root, and replaced an incorrect
origin-bound-session assumption with denial through a real non-administrator
session. The first full gate caught test header type inference; it was corrected
before the passing gate. No production authentication behavior or existing test
was changed. No checks were disabled. Local logs are
`/tmp/issue-58-format.log`, `/tmp/issue-58-targeted.log` and
`/tmp/issue-58-check.log`; these are ephemeral evidence, not CI.

Independent-environment full checks, clean independent review, current-main
integration and exact-head Linux CI are unverified controller gates, not local
claims. Publication belongs to the controller and merges remain manual. #14/M1
remains open for environment/stream ownership and remaining role separation.
Changes remain uncommitted.

## Issue #61 — platform composition PostgreSQL teardown repair (2026-10-25)

Scope: tests/platform-composition.test.ts, the directly owned development
cluster in apps/local/src/development-database.ts, and this record. Base revision
is `55cbdb125b8a5fcf4b7f126ae447749bf2bcd94a`. Runtime composition, external
PostgreSQL semantics, signal handling, leases, schemas, dependencies, storage
engines and delivery policy are unchanged. All original test bodies/assertions
are byte-for-byte unchanged. No protected paths changed.

The initial CI-log access blocker was resolved by the controller supplying
`/tmp/issue61-ci-failed.log`. Read it before implementing the repair. The offline,
non-root Linux gate passed formatting, SDK drift and TypeScript, then failed
with 420/421 runtime tests passing, zero skips. Its sole failure was the default
composition test at line 40, with `terminating connection due to administrator
command` and the pg-protocol parser stack. Startup had reached readiness.
Python/build/starter stages were not reached in that failed CI run. Log contents
were used as diagnostic evidence only; no credential files were inspected.

Read the accepted core/security/runtime contracts, production acceptance,
development setup and database lifecycle documentation. Inspected runtime entry
points and all runProcess/startDevelopmentDatabase callers, composition cleanup,
Jobs loops/stop, schema release, embedded-postgres stop and exit-hook ownership,
and pg pool/client teardown. Runtime already waits for startup, listeners, jobs,
schema release and pool.end() before stopping its development cluster. External
mode has no cluster owner. The defect is in assuming pool.end() means every
socket is disconnected: pg-pool removes idle clients from its tracked list
before their asynchronous end callbacks finish.

Local evidence matched the CI signature:

- Baseline focused test passed both tests, but temporary instrumentation recorded
  pool.end requested; three client.end requests; pool.end resolved; cluster.stop
  requested; then three client end events (`/tmp/issue-61-ordering.log`).
- Temporarily delaying client disconnect by 100 ms reproduced the same test
  failure and PostgreSQL error/parser stack (`/tmp/issue-61-reproduction.log`).
  This was diagnostic fault injection only. No timeout or retry was added to
  the final tests. All temporary logging/delay instrumentation was removed.
- The initial unchanged `pnpm check` passed 421/421 runtime tests and 14 Python
  tests (`/tmp/issue-61-check.log`), which did not rule out this teardown race.

The development database owner now tracks each connected client until its real
`end` event, removes completed promises, and waits for remaining disconnects
before invoking embedded cluster stop. The existing caller still drains and
ends its pool first. Tracking begins at connection time so clients already
removed from the pool are covered. There is no error suppression or alteration
to persistent paths, initialization or external PostgreSQL behavior.

The added regression uses a real disposable PostgreSQL cluster and two real
clients, holding their end calls with explicit barriers. One client is removed
before pool.end(), the other is idle. It proves pool.totalCount can reach zero
while both disconnects remain pending, checks cluster stop has not been requested,
queries real persisted data while waiting, releases the idle client first and
proves the removed client still prevents shutdown. Both end events must precede
the stop request. A transparent stop wrapper waits for cleanup even on the old
ordering, without suppressing errors or allowing the ordering assertion to pass.
A real restart verifies the marker persists. No mock substitutes for SQL or
cluster start/stop, and no live installation is used.

Validation:

- The final regression against the original development-database.ts exits 1 at
  `cluster stop must wait for actual client disconnections` (actual 1, expected
  0); the two original tests pass (`/tmp/issue-61-regression-red-final.log`).
- With the repair, all three focused tests pass, zero skips
  (`/tmp/issue-61-regression-green.log`). Six further runs in two batches of
  three concurrent processes all exit 0, with three tests passing in each and
  no skips (`/tmp/issue-61-repeat-1.log` through `-6.log`). Fixed run counts;
  no retry-until-pass behavior.
- `pnpm format` and `git diff --check` passed. The complete repaired `pnpm
check` exited 1: formatting, SDK drift and TypeScript passed; 421/422 runtime
  tests passed, zero skips (156.84 seconds). Both the new regression and the
  original composition test passed. The sole failure is
  `tests/api-only-process.integration.test.ts:78`: `TypeError: fetch failed`,
  caused by `read ECONNRESET` (`/tmp/issue-61-repair-check.log`). The fixed-count
  focused batches overlapped the beginning of this gate; the log does not
  establish the cause of the reset. It must not be dismissed as transient.
- The runtime failure prevented the gate's downstream stages from executing.
  Ran `pnpm check:python && pnpm build && pnpm check:starter` separately: exit 0,
  including Python formatting/validation and all 14 Python tests, both builds,
  and starter type/build/hosting verification. These passes do not override the
  failed full gate.

Remaining blocker: diagnose the API-only integration connection reset in an
explicitly authorized follow-up. That test is outside issue #61's allowed files;
no changes or retries were made to obtain a green gate. The scoped lifecycle
repair and deterministic regression are complete locally, but issue acceptance
and roadmap resumption remain blocked by the failed complete quality gate and
independent validation/review requirements.

Environment: non-root Linux, Node 24.21.0, pnpm 11.25.0, repository-local Python
3.11.17. Independent-environment validation, clean independent review, current
main integration, exact-head Linux CI and merged-main validation remain
unverified controller gates. No publication, commits, live operations, external
contacts or paid model calls were performed. Changes remain uncommitted.

## Issue #56 — final focused initial-pinning cancellation pass (2026-10-25)

Scope is the trusted readRows lifecycle in services/catalog/src/storage.ts,
real PostgreSQL regressions in tests/iceberg-scoped-storage.integration.test.ts,
and scoped-reader documentation. Accepted C04 exact-snapshot and S06 current
release authority contracts remain unchanged. No protected paths, dependencies,
schemas, storage adapters, physical-path readers or runtime adoption changed.

The read deadline and caller cancellation now cover initial catalog pinning,
starting at readRows invocation and continuing through iterator completion.
Initial openRead races cancellation while retaining an observer of its eventual
result. Catalog.transaction retains ownership of rollback, advisory unlock and
client release; cancellation checkpoints prevent late pinning from initializing
storage. The iterator owns timer/listener cleanup on completion, error, return
and throw, including return/throw before its first pull. Existing startup,
buffered-row and post-child-exit cancellation/deadline guards remain in place.

Added real PostgreSQL advisory-lock regressions for caller cancellation and
deadline on both local and isolated pinned S3 fixtures. They require CANCELLED
before unlocking, confirm the backend is still blocked, verify no storage/child
startup and caller-listener cleanup, then verify pool/lock drainage and no late
storage startup after unlocking. Additional iterator regressions cover return
and throw before first pull and normal exhaustion. The existing one-millisecond
deadline assertion now covers rejection during either initial pinning or first
pull and requires the CANCELLED code.

Validation: pnpm format passed. Full pnpm check exited 0 in the provided
non-root Linux checkout with Node 24.21.0, pnpm 11.25.0 and workspace Python
3.11.17. Formatting, SDK drift, TypeScript, all 471 runtime tests (zero failures
or skips; 248.79 seconds), Python formatting/schema validation and all 14 Python
tests, both builds and starter verification passed. Both local and isolated S3
initial-pinning regressions rejected before unlock and passed drainage assertions;
all existing startup/buffered-row/post-exit cancellation tests passed. Raw gate
output is /tmp/issue-56-final-check.log; formatting output is
/tmp/issue-56-final-format.log. git diff --check passed.
Independent-environment verification, clean independent review, current-main
integration and exact-head Linux CI remain controller gates and are unverified.
No commits, publication, live infrastructure or external contacts; #17 remains
open and changes remain uncommitted.

## Issue #63 — bounded complete scoped snapshot publication (2026-10-25)

Scope: additive trusted `CatalogStorage.publishSnapshot`, one dedicated publication
helper, fixed Python closure validation, genuine Iceberg fixture support, dedicated
integration tests and storage/worker documentation. Dependencies #49/#56 and accepted
C04/S01/S02/S06/S08 contracts are preserved. No dependencies, schemas, protected paths,
storage adapter internals, physical-path consumers, runtime rollout or delivery
policy changed. Work is on `delivery/issue-63`; changes remain uncommitted.

The new seam binds every exact-size immutable object's content and canonical logical
name into the existing catalog request hash, validates retained metadata/manifests/
Parquet through real pinned parsers, stages through existing scoped create-only
adapters and publishes only after acknowledged completion. It returns the existing
receipt and service-owned immutable references for trusted reader binding replacement.
The metadata-only API and object identity remain unchanged. Full limits and retry,
orphan and uncertain-acknowledgment semantics are in CATALOG-STORAGE.md.

Initial validation: workspace Python 3.11 real fixture/validator smoke passed.
Initial TypeScript checking found a missing explicit fixture array type. Initial
focused tests exercised both local and isolated S3 adapters; the only runtime
failures were new expiry assertions expecting AUTHORIZATION_CHANGED rather than
Authority's existing UNAUTHENTICATED contract. First repair pass corrected these
test declarations/expectations without changing authority behavior or weakening
assertions, strengthened stalled-intake cancellation and unsafe-reference cases,
and completed validation bounds. Subsequent check results are recorded below.

No live installation, credential file, paid model, external person, commit, push,
PR, merge or deployment was involved. Fresh independent no-findings review,
current-main integration and successful Linux quality CI on the exact published
head remain unverified controller gates. #17 stays open and #18/#19 remain
dependency-gated; production durability/capacity, recovery, Spark interoperability,
arbitrary-code isolation and runtime adoption are outside this slice.

The repaired focused integration command
`pnpm exec tsx --test tests/catalog-snapshot-publication.integration.test.ts`
passed 62 tests with no failures/skips in 93.28 seconds (raw output:
`/tmp/issue-63-focused-repair1.log`). `pnpm typecheck`, `pnpm format`, workspace
Black formatting and `git diff --check` passed. A subsequent parser probe showed
that PyIceberg accepts an unresolved current-snapshot ID at model-parse time.
The second repair pass added explicit current/reference snapshot resolution and
cross-history immutable snapshot-definition checks, plus a dangling-snapshot
regression. The full gate below includes that final regression. The two-repair
budget is exhausted; further failures must be reported rather than repaired here.

Final verification: `pnpm check` exited 0 on the uncommitted issue changes above
base `aff3d976075db3d2c965cfe0bab1cb383275ee53`. All 535 runtime tests passed
(zero failures, cancellations or skips; 338.94 seconds), including 64 complete
snapshot tests across real PostgreSQL/local storage and isolated checksum-pinned
MinIO, and all existing metadata publication/scoped reader tests. Formatting,
SDK drift, TypeScript, Python formatting/specification/workload validation, all
14 Python tests, both application builds and Station Explorer type/build/hosting
verification passed. Raw full-gate output: `/tmp/issue-63-check.log`; formatting:
`/tmp/issue-63-format.log`; Black: `/tmp/issue-63-black.log`. Final
`git diff --check` passed. Environment: non-root Linux, Node 24.21.0,
pnpm 11.25.0, workspace Python 3.11.17, PyIceberg 0.12.0 and PyArrow 26.0.0.
Only synthetic disposable databases, warehouses and S3 fixtures were used.
No required local infrastructure gate was skipped. Controller-owned independent
review, current-main integration and exact-head Linux CI remain unverified;
local success does not satisfy or replace those acceptance gates.

### Issue #63 — schema validation review repair (2026-10-25)

This follow-up addresses the explicitly requested schema-validation findings only.
The earlier two-pass repair history above is preserved; this repair does not change
delivery policy or authorize further unrelated repairs. Regressions use genuine
Iceberg metadata and Parquet containing nulls, retaining integer snapshot IDs
without JavaScript number coercion. They retain the original metadata while changing
root nullability, exercise a different resolved current schema for snapshots with
omitted schema IDs, and supply unknown or duplicate schema IDs in root/retained
metadata. Repeated snapshot definitions stay consistent so unrelated snapshot
checks cannot mask the schema-resolution gap. Every rejection asserts zero storage
writes and unchanged heads, history/lineage, checkpoints and publication receipts.

Verification commands and final results for this repair are recorded below.

The pre-fix reproduction (`pnpm exec tsx --test
 tests/catalog-snapshot-publication.integration.test.ts`, output
`/tmp/issue-63-schema-reproduction.log`) reproduced all six schema rejection gaps
on both adapters: each candidate incorrectly succeeded. Twelve failing subtests
plus their two parent tests were reported. The retained-schema regression was
subsequently tightened to make the unknown ID valid only in the root, isolating
resolution inside retained metadata.

The validator now rejects duplicate schema IDs within a metadata file, conflicting
schema definitions across history, and unresolved current/explicit snapshot schema
IDs. Its scan cache includes PyIceberg's resolved projection, so snapshots without
schema IDs cannot reuse a historical scan under a different current schema. These
scan contexts share the existing 128-scan and aggregate row/byte bounds.

Final focused command: `pnpm exec tsx --test
 tests/catalog-snapshot-publication.integration.test.ts` passed all 76 tests,
zero failures/cancellations/skips, in 116.29 seconds. Output:
`/tmp/issue-63-schema-focused.log`. This includes real disposable PostgreSQL,
local storage and isolated S3, original successful publication/read/retry cases,
and all new pre-staging schema regressions. `pnpm format`, workspace Black and
`git diff --check` passed; no protected paths or dependencies changed.

Full `pnpm check` exited 0: all 547 runtime tests passed, zero failures,
cancellations or skips (366.40 seconds); all 14 Python tests, formatting, SDK drift,
TypeScript, specification/workload validation, application builds and Station
Explorer verification passed. Output: `/tmp/issue-63-schema-check.log`. The final
retained-metadata test refinement also passed the focused suite above and a fresh
`pnpm format:check` (`/tmp/issue-63-schema-format-check.log`). Validator source was
unchanged throughout the full gate. Final `git diff --check` passed.

Changes remain uncommitted. No required local infrastructure was unavailable or
skipped. Fresh independent no-findings review, current-main integration and Linux
quality CI on the exact published head remain unverified controller-owned gates.
The scope and production/runtime limitations recorded above remain unchanged;
#17 remains open and #18/#19 remain dependency-gated.

### Issue #63 reader compatibility repair

Verified the two additional review findings against the pinned PyIceberg parser
and unchanged scoped reader. New real PostgreSQL/local/S3 regressions reproduced
successful publication of missing sort orders, empty sort orders, and a current
schema renaming `value` to `renamed` while the selected snapshot retains schema 0.
The pre-fix run is recorded in `/tmp/issue-63-reader-regressions-before.log`.

The fixed validator requires nonempty sort orders and compares each metadata
file's current snapshot resolved projection fields with its advertised current
table schema before scan-cache reuse. Ordered field definitions must match;
schema IDs alone may differ. Validation still precedes all scoped writes and
catalog publication. Regressions on both adapters require `INVALID_INPUT`, zero
writes and unchanged heads, history/lineage, checkpoint and receipt state.
No reader, dependency, schema, protected path or delivery policy changed.

Verification: `pnpm format`, `.venv/bin/python -m black workers benchmarks
docs/spec/schemas` and `git diff --check` passed. `pnpm exec tsx --test
tests/catalog-snapshot-publication.integration.test.ts` passed all 82 tests, zero
failures/cancellations/skips, in 124.76 seconds with real disposable PostgreSQL,
local storage and isolated S3 (`/tmp/issue-63-reader-focused.log`). Full
`pnpm check` exited 0: all 553 runtime tests passed with zero failures,
cancellations or skips (363.91 seconds), and all 14 Python tests, formatting,
SDK drift, TypeScript, specification/workload validation, application builds and
Station Explorer verification passed (`/tmp/issue-63-reader-check.log`). Final
`git diff --check` passed. No required local infrastructure was unavailable.
Fresh independent review, current-main integration and exact-head Linux quality
CI remain unverified controller-owned gates. Changes remain uncommitted; the
existing production/runtime limitations and dependency gates remain in force.

## Issue #65 — durable scoped reference recovery (2026-10-25)

Scope: existing catalog publication/read seams, one dedicated inventory helper,
dedicated recovery integration tests and a fresh-process fixture under
`tests/fixtures/iceberg-scoped-storage`, plus catalog/worker documentation.
Dependency #63's domain-separated complete-snapshot commitment already binds the
canonical reference inventory; no schema, dependency, adapter, runtime, protected
path or delivery-policy change is needed. Accepted C01/C04/C08 and S01–S03/S06
remain the governing contracts. Changes are uncommitted on `delivery/issue-65`, based on
`7fce280b0d49c22be35b7641db7c13c50186f43f`.

Complete publication persists the exact committed reference array through the
existing authorized create-only StreamingStorage path before Catalog.publish.
Its service-derived location hashes the canonical pinned metadata StorageLocation
with `moria-reference-inventory-v1\0`. Fresh bindings recover only from catalog
provenance, reject noncanonical/duplicate/extra fields and names, verify metadata
and inventory commitment, validate UUID/snapshot, then verify every object's size
and scoped content hash before any row release. The iterator retains verified
bytes and provenance; current Authority checks continue through consumption.
Explicit trusted mappings and metadata-only APIs are preserved. Missing older
inventories fail closed. No registry, provider listing, metadata URL fetching,
physical fallback, cleanup, adoption or migration is introduced.

Limits: inventory 256 KiB; intake at most 65,536 frames of at most 256 KiB per
object; root metadata 1 MiB; 128 referenced objects each at most 16 MiB; total
verified metadata/objects 64 MiB per scan, plus inventory and bounded transient
copies. Publication retains 64 MiB input across at most 16 outputs, plus at most
256 KiB inventory per output. Existing child request/transport/row limits are
unchanged. One maximum 60-second reader deadline covers pinning, recovery and the
iterator lifetime, with safely observed pending SQL cleanup. Inventory failures
preserve heads/history/lineage/checkpoint/receipt and existing orphan semantics.
Only the committed exact request recovers its receipt; collisions cannot establish
durability. All promised snapshots' objects and inventories remain retained.

Environment: non-root Linux uid 10001; Node 24.21.0; pnpm 11.25.0; repository-local
Python 3.11.17; PyIceberg 0.12.0; PyArrow 26.0.0; embedded-postgres package
18.4.0-beta.17 (PostgreSQL 18.4); MinIO RELEASE.2025-09-07T16-13-09Z
(Go 1.24.6 linux/amd64), isolated fixture SHA-256
`7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f`.
Only disposable synthetic PostgreSQL/provider fixtures and generated ephemeral
sessions/credentials are used; no credential file or live installation is read.

Initial verification: `pnpm typecheck` passed. The first
`pnpm exec tsx --test tests/catalog-reference-recovery.integration.test.ts` passed
20 tests, zero skips, across local storage and isolated S3 (72.275 seconds;
`/tmp/issue-65-focused.log`). This includes fresh-process rows/scalars/provenance,
receipt recovery after lease expiry, competing publication, malformed/swapped/
missing/oversized durable data, failed/uncertain/cancelled inventory writes,
revocation/expiry and cancellation/deadline during inventory intake and SQL pinning.
Additional metadata-only and partial-inventory cases and exact-scope assertions
are undergoing verification. `pnpm format` and full `pnpm check` were started;
final results are recorded below when available.

Scope conflict: the existing complete-publication regression counts exactly one
metadata write plus the referenced objects per output. The required durable
inventory necessarily adds one write per output. The file containing that
assertion is outside the issue's listed test scope. Permission for that one expected
write-count update is pending; the assertion has not been weakened or changed.

Independent-environment full check, independent no-findings review, current-main
integration and exact-head Linux quality CI remain unverified/controller-owned.
No publication, merge or deployment is performed. #17 remains open and #18/#19
remain dependency-gated. This is not production recovery/capacity certification,
runtime rollout or a new retention/expiration policy.

Final focused verification:

- `pnpm exec tsx --test tests/catalog-reference-recovery.integration.test.ts`:
  24 passed, zero failed/skipped (90.943 seconds;
  `/tmp/issue-65-recovery-verified.log`). Includes partial inventory intake,
  metadata-only compatibility, exact read/create scopes, same-environment/table
  publication inventory swaps and unchanged committed state after fresh-process
  receipt recovery. Both local and isolated S3 variants passed.
- `pnpm format`, `pnpm format:check`, `pnpm typecheck` and `git diff --check`:
  passed (`/tmp/issue-65-format.log`, `/tmp/issue-65-format-final.log`).
- `pnpm check:python && pnpm build && pnpm check:starter`: passed, run separately
  to exercise the checks after the full gate's expected runtime-test failure
  (`/tmp/issue-65-remaining-checks.log`). This does not turn a failed full gate
  into a pass. No database tests were skipped or checks disabled.

Bounded review repair 1: recovery initially reused the metadata-only Python
validator. To preserve the complete publisher's child isolation/resource envelope,
recovery now reuses `validateSnapshot` after content verification and before row
release. This also revalidates the entire closure, with the existing 2 GiB
address-space/60-second CPU/zero-core limits and isolated minimal child environment.
No worker or adapter changes were needed. `pnpm format` and full `pnpm check`
were rerun against this final implementation (`/tmp/issue-65-check-final.log`).
The focused 24-test result above precedes this repair; final-gate recovery results
below supersede it for the final source.

The first full `pnpm check` completed with exit 1: 577 tests, 573 passed, four
reported failures (the two provider-specific write-count assertions and their two
enclosing tests), zero cancelled/skipped, 456.590 seconds. Both assertions at
`tests/catalog-snapshot-publication.integration.test.ts:212` expected 18 writes
and observed 20: the additional durable inventory for each of two outputs.
No other failures occurred. Formatting, SDK drift and TypeScript stages passed.
The assertion remains unchanged because its file is outside the authorized scope;
the required minimal follow-up is changing only `fixture.objects.length + 1` to
`fixture.objects.length + 2`, then rerunning the full gate. No missing infrastructure
caused this failure. The host kernel is Linux 6.18.33.2-microsoft-standard-WSL2;
the checkout and test data are on the Linux filesystem, using the non-root user.

Final-source full gate: `pnpm check` exited 1 after 427.817 seconds
(`/tmp/issue-65-check-final.log`): 577 tests, 573 passed, four reported failures,
zero cancelled/skipped. All 24 dedicated recovery tests passed with the reused
complete-snapshot validator. The only failures remain the two expected-write-count
assertions (20 actual versus 18 expected) and their enclosing local/S3 tests.
Formatting, SDK drift and TypeScript stages passed on this final source. The later
Python/build/starter stages were already verified separately; the full gate itself
is failed, not accepted. One bounded review repair was used; no checks or existing
tests were weakened. `git diff --check` passes and all changes remain uncommitted.
Acceptance is blocked on the narrowly identified test-scope decision and then a
passing full gate, in addition to the independent review/environment, current-main
integration and exact-head Linux CI gates listed above.

Bounded review repair 2 (#65 verification feedback): the requested repair resolves
the test-scope decision above. Updated only the existing complete-publication
regression's exact write count to include one durable inventory per output, in
addition to root metadata and referenced objects (20 writes for the two-output
fixture). The assertion still requires an exact count; its per-write unchanged
catalog-state checks and all row/provenance/receipt checks remain intact. No
production code, protected path, dependency or delivery policy changes are needed
for this repair. The original failed-gate results above remain historical evidence.

Repair verification uses the same non-root Linux environment and repository-local
Python 3.11.17, Node 24.21.0 and pnpm 11.25.0. `pnpm format` passed
(`/tmp/issue-65-repair-format.log`). Full `pnpm check` passed (exit 0;
`/tmp/issue-65-repair-check.log`): all 577 tests passed, zero failed/cancelled/skipped,
runtime-test duration 427.231 seconds. Both local and isolated S3 publication and
recovery suites passed, using real disposable PostgreSQL and PyIceberg fixtures.
Formatting, SDK drift, TypeScript, Python formatting/specification/workload checks,
both application builds and starter verification all passed in the same full gate.
`git diff --check` also passed. This supersedes the local full-gate blocker above.
Independent
environment/review, current-main integration and exact-head Linux quality CI
remain unverified and controller-owned. #17 remains open; #18/#19 remain
dependency-gated. Changes remain uncommitted.

## Protected point-read release check — October 27, 2026

ObjectService.read now rechecks current authority after object evaluation, inside
the same protected transaction, before returning the result. This aligns point
reads with queries when a session expires during a blocked database read (#69).

A disposable real-PostgreSQL regression establishes a source-table lock barrier
after initial authorization, waits for the session deadline pinned before the
read snapshot, then releases the lock. The original point read returns data and
fails the regression; the query control denies release. The repaired point read
also denies release, and subsequent valid and missing-object reads verify cleanup.
No session-row mutation after snapshot creation is used to simulate wall-clock
expiry. Full quality results are recorded in the pull request. This narrow repair
does not implement the shared lifecycle/enforcement work in #72 or resolve #67.

## Protected-operation owner — October 27, 2026

The object point/query release lifecycle now has one private owner. Store and
authority handles no longer escape ObjectService; binding declarations are copied,
and requiring adds gates without weakening previous requirements. Action commit
ownership remains in Catalog.action. The management server delegates retired-job
retry validation into Jobs instead of issuing raw catalog queries itself.

A TypeScript semantic architecture gate and negative fixtures reject direct and
indirect raw access, unregistered callers/operations, runtime database imports,
computed imports and missing registered tests. Shared release tests include actual
HTTP point/query requests as well as real database barriers. See
docs/production/PROTECTED-OPERATIONS.md for owners, proof boundaries and unmet
external enforcement. #72 remains open until independently enforced review and
all required evidence are satisfied. Full check results belong in the PR.

## Shared HTTP transport owner — October 28, 2026

All four native listener factories and their nested workload dispatchers now use
one transport owner. Trusted profiles preserve workload, browser/CLI, static and
public-proxy contracts; browser headers cannot select local CLI/recovery authority.
Body parsing, transport admission, baseline headers, timeouts and safe failure
handling are shared. Resource authorization remains in the protected-operation
owners. Architecture fixtures reject raw listeners, unregistered profiles/callers
and request-event bypasses; real HTTP barriers cover every profile and actual
factory. Existing SQL auth and independent process suites remain in the full gate.
See docs/production/HTTP-BOUNDARY.md for the inventory, proxy trust assumption,
intentional response differences and unmet independent enforcement/review gates.
Full exact-revision verification is recorded in the PR; #73 remains open.

## Shared Python lifecycle owner — October 28, 2026

The five direct Python adapters now share fixed task startup, bounded input/output,
backpressure, deadlines, cancellation, graceful-to-force termination and reaping.
Streaming protocol checks, materialized compatibility, storage authority and
publication fencing remain in adapters. Safe outcomes retain actual exit code and
signal; only explicit validator exit65 means invalid content. Unknown native failures
remain unknown. No raw stderr or request data enters diagnostics.

Production source guards reject raw process primitives and unregistered callers.
Every adapter runs the same fixed-child lifecycle contract, with real PID cleanup
checks; real SQL/PyIceberg/local/S3 suites remain. The dependency-owned development
database is separately registered. Catalog storage fixtures reuse its actual-client
drain behavior after a deterministic red regression reproduced early cluster stop.
This addresses the observed cleanup path without diagnosing #67's validator failure.
See docs/production/PROCESS-LIFECYCLE.md for limits, outcome mappings, OS/dependency
assumptions and unmet independent review/enforcement gates. Full exact-revision
results belong in the PR; #74 remains open.

## Native Dataset publication and indexing — October 30, 2026

Native scoped POSIX/MinIO transfer, immutable sealing, bounded durable closure
verification and transactional grouped Dataset publication have real integration
evidence. The native replace writer now emits UUID and scalar-array Parquet with
accepted Iceberg metadata, retains previous snapshots and verifies complete
current/historical readback on both providers. Managed Dataset IO additionally
binds the published Dataset identity, exact accepted schema, namespace revision
and physical provider identity. The catalog contract documents these boundaries.

The release pipeline completed one million 4-KiB objects with permanent UID
allocation, registered Sync publication, native indexing, projection activation
and a complete streaming identity/source/effective checksum oracle. This remains
functional evidence; the measured 27m51s trial predates the reducer build-prefix
and within-call SQL guard optimizations. Mixed protected HTTP SLOs, ten-million
objects, wider-than-RAM and soak/recovery targets are not certified by it.

Permanent indexing now captures full Dataset columns, mapped Object properties,
serving policy/revision and projection revision under a registered indexing
purpose. Fresh builds may reauthorize that same exact purpose at current epochs
without rewriting old proofs. Actual small mapped-source tests have demonstrated
epoch refresh and captured-policy behavior; the final combined proof and the
maximum-width mapped profile remain open. Failed and exploratory trials are
retained separately in the native catalog evidence ledger.

General external Parquet pre-allocation bounds, approved history expiration,
conditional physical garbage collection, final integrated runtime checks and
target-scale certification remain required. Existing limits and authorization
checks have not been raised or bypassed to claim completion.

The native DuckDB engine now streams three pinned 6,000-row wide inputs through
an isolated OCI guest and commits 6,000 detail plus ten aggregate rows in one
grouped publication. Trial 021 passed that path in 82.77 seconds under the
unchanged 120-second job deadline, including SDK/CLI receipt replay and numeric
event pagination. A measured current-authority bottleneck was reduced from
452 ms to 100 ms by checking an identical filesystem origin/operation once per
guard invocation; all twenty operational checks remained. There is no cache
across calls. Trial 024 passed the authority negatives and repeated the grouped
publication in 74.35 seconds under an exclusive host resource lock, but the
overall suite failed on UUID IPC metadata parity in the verifier. The exact
metadata bridge passes a real PyArrow/native golden-schema test and codec
roundtrip. Trial 026 then passed all eight tests: grouped publication in 70.98s,
native UUID/array Parquet verification in 47.98s and a final 32.81s guest that
exhausted a real dedicated ext4 filesystem at 105,906,176 bytes. Cleanup removed
mode000 files and reused the same slot for all three jobs; each database deadline
remained 120 seconds. The observed worker RSS was 80,703,488 bytes. This is a
replace-only functional proof. Sync checkpoint/fault cases, the durable callback
observation journal and complete engine acceptance remain open. Raw failed
trials and source/artifact fingerprints remain in the native pipeline ledger.

### Exact-issued recovery reclamation proof (2026-10-30)

The explicit recovery retention controller passes **7/7 actual PostgreSQL/MinIO tests, zero skips (35.242s)**. The fresh producer uses actual recovery authority and backup writes; expiration/deletion controls use separately labeled historical closed DATA and actual qualified versions. Coverage includes retention and namespace reservation, terminal producer fencing, retained foreign-reference denial, exact-version removal, policy revoke after held HEAD, killed collector/real lease expiry/higher fence takeover, and null-version no-delete. Evidence: `production/evidence/2026-10-30/native-retention/20261031T045252Z/`. General C04 Dataset/history expiration and broad object reclamation remain gated on the reference/pin/publication inventory; this evidence does not close those paths or establish 24-hour aging or capacity certification.

### Combined native recovery, indexing and lifecycle (2026-10-30)

The v10 immutable source passed all 61 Rust library tests and a combined 37/37
actual integration cases, zero skips, in 294.931 seconds. The initial trial had
an absent catalog harness executable path; the successful external driver set
that path explicitly and used the same unchanged runtime and catalog binaries.
Evidence: `production/evidence/2026-10-30/native-recovery/20261031T051616Z-external.json`
and its raw test log; the original failed run remains retained.

The combined run covers bounded HTTP admission, current readiness and protected
metrics, disabled CLI approval across restart, unfinished-body drain timeout,
numeric change pagination, restore fencing and lost acknowledgments, clean-host
POSIX/MinIO metadata/content/key restoration, and permanent mapped indexing at
small and maximum row widths. A managed Dataset was restored into new physical
Dataset/filesystem roots, read through actual native scoped IO, replaced by a
real native publication, captured/restored a second time, and read at both the
current and prior snapshot. Logical namespace/schema/Source identity remained
exact while physical binding and binding revision changed under the restore proof.

A separate v10 protocol trial passed expanded MCP discovery across replicas with
102 visible Object resources, the official Python SDK, typed metadata/actions,
transport tampering and revocation, and all 17 Model gateway cases. The combined
protocol trial failed search: a manually seeded generation used the old declared
binding revision, and rebuild used a nonexistent provenance column. The fixture
now derives the actual authority-generation fingerprint; unchanged-binary
retrieval then passed all nine cases. The production join correction requires a
fresh compiled proof. These results do not close the complete quality gate,
production workload ladder, general history collection or HA certification.

### Fresh native protocol closure (2026-10-30)

The fresh v12 snapshot passed 61 Rust library tests and all 42 actual protocol
integration cases, zero skips. This combines expanded MCP/official SDK and CLI
checks, all 17 model-gateway cases, exact protected pgvector retrieval and the
registered Dataset-to-Object search projector with full action replay, durable
embedding reuse, complete source/model replacement generations, and exact-purpose
revocation that closes a held upstream socket without changing heads or visibility.
The production provenance join now uses the actual immutable `contract` column;
the first existing grant check identifies exact projector revocation before its
more general Dataset epoch check, while retaining all lease/current-authority
checks. Evidence: `production/evidence/2026-10-30/native-protocols/20261031T053004Z.json`
and its raw test/build logs. Earlier failed trials remain. This closes this
functional protocol checkpoint, not general retention, whole-instance acceptance,
the complete quality gate or production capacity certification.

## Native operational history retention — October 31, 2026

Operational PostgreSQL history now has explicit operator policy and a monotonic
revision floor, normalized retained read pins and bounded transactional collection.
The default retains all history. An approved plan observes a full-day grace period
and rechecks live cursor, execution, build, search and replay pins before each step.
Collection retains two source generations, the floor generation and permanent
identity/audit headers; expired reads fail explicitly. Filesystem and physical
Dataset/object reclamation remain separate controllers.

The v13 immutable source passed 61 Rust units and an eight-case actual history and
change integration checkpoint after a separately archived expected-phase-list test
correction. The runtime and catalog binaries were unchanged. Actual native Dataset
publication/index/projection and real protected Actions produced closed projection
intervals; cleanup removed obsolete intervals while all 64 current rows, source
payloads and protected HTTP reads remained exact. The ordinary source-history
fixture retained two complete generations and permanent UID/header state. Positive
aging uses labeled historical DATA timestamps, not fabricated live leases or an
elapsed-day claim. Evidence: `production/evidence/2026-10-31/native-history/20261031T061533Z-external.json`.
The subsequent recovery-admission DDL guard, retired-table collection, all races,
full latest-revision gate and target-scale maintenance still require fresh evidence.

2026-10-31 Dataset retention checkpoint: explicit managed-S3 policy/expiration/reconciliation controller now compiles with the exact issued-object journal, qualified root versions, execution/indexing pins, immutable candidate tombstones, 24-hour first-observed orphan grace, Iceberg metadata rewrite and final head CAS. Frozen source `20261031T063554295001Z-c04-expiration-private-v1` passed schema generation, TypeScript, metadata-pruning unit (1/1) and actual native Source/writer/managed recovery regression (15/15, zero skips, 290.36s). Raw evidence: `docs/production/evidence/2026-10-31/native-retention/expiration-component-20261031T063941Z`. Dedicated positive expiration, qualified provider deletion, pin/CAS, recovery and held-provider fault tests remain required. No external or POSIX snapshots become eligible by this checkpoint.

The qualified one-million-object trial completed its exact source/effective/UID oracle, but did not meet protected HTTP targets. Twenty scheduled requests/second completed 600/600 with p95 1238ms; 50/s completed 798/1500 (702 HTTP 429), and 100/s completed 850/3000 (2150 HTTP 429). Raw immutable evidence remains `docs/production/evidence/2026-10-30/native-projection/qualified-million-driver-v6-20261031T052859Z`. Reader I/O amplification and whole-population typed UID access are active measured bottlenecks; this is not a capacity certification and does not justify proceeding to ten million yet.

2026-10-31 native composition checkpoint: immutable source
`20261031T065101356371Z-native-launcher-development-observation` passed 7/7 actual
integration cases with zero skips (86.14 seconds). It includes the default external
and development launchers, qualified embedded PostgreSQL/vector loading, native
children-before-database shutdown, split OIDC and accepted-human verification,
complete failed-secondary-action observation replay, and the composed Source,
protected frontend/SDK, native writer/index and creation-refresh path. Evidence:
`production/evidence/2026-10-30/native-filesystem/native-launcher-development-20261031T065101Z`.
This does not certify production capacity or every management UI workflow.

The shared native source validator now accepts the same scalar-array column
profile for Dataset columns, Object properties and Action inputs; four focused
native validation cases passed, including the shared compiler vectors. The
whole-library suite passed 67/67 with no ignored cases after adding permanent-UID
predicate aliasing and the operator filesystem-retirement mechanism. Actual
projection CHECK/HTTP/plan regression and a fresh offered-load run remain required
before attributing a performance improvement. Filesystem orphan reclamation is
implemented but its qualified-provider integration proof is still pending.

2026-10-31 filesystem orphan/multipart retirement checkpoint: immutable source
`20261031T073038927146Z-native-filesystem-multipart-gc` passed the actual native
HTTP/MinIO fault test (1/1, zero skips, 12.81 seconds). It proves grace enforcement,
permanent upload tombstones, historical-reference protection and interval reset,
qualified exact-version removal that preserves a newer version, separate operator
and SQL privileges, durable pre-Initiate ownership, known-ID Part acknowledgement
loss and 17 lost Initiate acknowledgements reconciled in bounded exact-key pages.
A neighboring upload survives. Earlier failures and the narrowly qualified MinIO
pagination variant are preserved. Evidence:
`production/evidence/2026-10-30/native-filesystem/filesystem-multipart-gc-20261031T073039Z`.
POSIX physical deletion remains explicitly unsupported; no unconditional removal
fallback or production-capacity claim is added.

2026-10-31 operational-history branch checkpoint: the actual collector/projection
fixtures passed 3/3 with zero skips (18.72 seconds), using the exact native artifact
from `20261031T073038927146Z` plus separately archived test/helper changes. Five
real native projection generations and six native Link actions exercise actual
retired-table DROP, complete link-history compaction, shared backup admission,
current/latest-two/floor retention, structural active-Search pinning and positive
collection after pin release. New cursor and normalized read pins after planning
block publication; below-floor pins fail without rewinding it. A true non-owning
connection cannot self-approve or edit collection/floor state. Evidence:
`production/evidence/2026-10-31/native-history/branches-external-20261031T074802Z`.
No history SQL change or fabricated live execution lease was needed.

### Data custody and capacity checkpoint, 2026-10-31

The qualified one-million-object release trial preserved the exact source/effective/UID oracle. On four CPUs and 12 GiB RAM, publication took 747.0 seconds, native indexing/projection 1,662.3 seconds and the total through HTTP 44m55s. All 600 arrivals at 20/s succeeded (all-arrival p95 367.8 ms); 50/s and 100/s rejected 537/1,500 and 2,015/3,000 arrivals with RESOURCE_LIMIT/429. These short exploratory windows do not certify production targets. Raw evidence: `production/evidence/2026-10-31/native-projection/window-uid-million-v7-20261031T073753Z`.

Subsequent actual accepted-verifier micro coverage measured the remaining per-file Parquet header path: eight columns, three row groups and 24 headers changed from 24 ranges/24 versions and 144 checks in each current-authority category to two ranges/three versions and 14 checks. The window read 2 MiB instead of 459 KiB; allocation and final authority checks remain bounded. A fresh million-object trial is needed to measure end-to-end effects.

Qualified managed S3 Dataset cleanup now journals real producer-issued outputs and acknowledged multipart IDs, uses permanent tombstones plus distinct exact-version deletion/exact-upload abort authority, and retains a late-effect watcher. The actual three-point Dataset multipart fault proof passed with a neighboring upload preserved and current typed rows unchanged. Ordinary producer/issued journal forgeries are denied. Scoped ordinary BackupAuthority IO remains supported through validated lease helpers; only the actual operator controller initializes budgets. Restore target custody and immutable Dataset requalification are implemented, with full fresh S3 roundtrip coverage pending. POSIX/unversioned deletion and external legacy Dataset reclamation remain unsupported. Current metadata pruning independent of pinned old physical roots is being completed separately.

### Execution and Connector admission checkpoint, 2026-10-31

Combined execution trial004 retains its failed result: 51/53 tests passed, with Agent and Evaluation setup rejected by the new Source Model policy agreement check. The fixtures now declare qualified private Markings and matching Source/operator destination policies; this correction has not yet passed a fresh native run. Function control-only cancellation and completed-result preservation, durable observations, execution authority and sandbox tests passed within that failed trial. Earlier failed trials remain separate.

Agent/Evaluation metadata now captures complete Model/Secret classifications before code, includes exact required Secret use operations, and pins immutable approval references for current and retained checks. A real Evaluation→Agent→Function service-purpose fixture is staged. Connector checkpoint identity is separated from lease/revocation generations and expected output heads; it binds interpretation, physical output schema/key/namespace, endpoint and approved material semantics. These changes are pending the next coherent immutable build and real regression. The active capacity trial holds native build/test resources; no runtime pass is inferred from static integration.

### Protected query and composed browser checkpoint, 2026-10-31

The ordinary scalar/multi-object Action path now prepares under shared admission and
revalidates sealed source, overlay, authority, policy and binding anchors inside its
short publication transaction. Create, Link, attachment and Function-prepared paths
retain their existing admission. Actual barrier/race/rollback and published Source
tests passed 26/26 without skips. Narrow typed projection maps preserve both captured
and current policy predicates while avoiding unused wide source/effective JSON.

The exact v20 native artifact passed another 13/13 actual PostgreSQL/native HTTP
tests, including canonical float cursor keys, complete old-cursor pagination across
projection replacement, effective current-policy restrictions and typed query/point
parity. Protected seven-field plans at 300 rows use 26/48 shared buffer hits; these
small diagnostic plans do not certify production capacity. Evidence:
`production/evidence/2026-10-31/native-serving/native-ratio-source-v20-20261031T122733Z`.

The same-offered million-object v9 run preserved its exact oracle. At 20/s all 600
arrivals succeeded; 50/s rejected 8/1,500 and 100/s rejected 1,417/3,000 with bounded
RESOURCE_LIMIT responses. Higher-rate production targets remain unmet. Actual browser
management Action/retry, Function 41-to-42 with terminal event, independent published
frontend creation/readback and cross-origin denial were observed. That disposable
outer browser trial remains failed because its final fixture compared float JSON as
text; the corrected typed numeric oracle is now checked before future browser waits.
The broader single-instance Model/Search/Agent/Evaluation/Pipeline/Automation browser
composition and a retained inspectable local instance are still outstanding.

2026-10-31 protected UID seek checkpoint: the actual typed PostgreSQL A/B showed
that wrapping a UID comparison in `IS TRUE` prevented a range index condition.
Only redundant final-WHERE wrappers were removed; observable Boolean expressions,
policy evaluation and all current/captured predicates are preserved. The fresh
immutable native build passed 38/38 Source, Action, projection and protected HTTP
checks without skips (96.09 seconds). Its actual HTTP UID-range page at ordinal
211 seeks the canonical UID index with zero preceding filtered rows. Evidence:
`production/evidence/2026-10-31/native-serving/native-protected-plan-20261031T124421Z`.
No million-row capacity result is inferred from this functional plan.

A separate actual PostgreSQL/retained-publisher Source regression passed 17/17.
Current operations require live files from the exact current resource release;
historical release restrictions still contribute classifications. Explicit pinned
old executable/declaration reads continue to require their exact live mapped files.
Replacing a deleted declaration through a genuine new publication permits current
resource access, preserves its protected historical label, and still denies the
old pinned file proof. An exact resource grant works without global filesystem
browse permission, while a direct filesystem read remains forbidden. This
authorization invariant change requires independent human review.

### Data history and delta protocol checkpoint

Young metadata compaction and scheduling-time retained-snapshot resolution passed
the real held-reader proof in
`production/evidence/2026-10-31/native-retention/dataset-history-selection-v2-20261031T111934Z`.
Physical retention ages and real pins are unchanged. Managed versioned-S3 cleanup,
independent backup recovery and permanent restore-target custody have combined
proof in the C04 evidence already recorded above.

Replacement persists actual Source primary keys/allowed modes and verifies key
uniqueness with bounded spill before publication. Task partsets bind accepted
mode/coverage, schema/key hashes and expected head. Dataset key declarations remain
non-null, matching the accepted Python validator; nullable non-key columns remain
valid. Compiler Connection coverage/key checks match the native contract. The
additional real composite scalar fixture is being verified separately.

Append/upsert/delete activation is gated on the complete protocol in
`production/contracts/dataset-writes.md` and `production/contracts/dataset-deltas.md`:
receiver-qualified persistent closure/run directories, standard Iceberg equality
deletes with bounded merge-on-read/compaction, and incremental Source/index
publication. Private directory, authenticated-chunk and packed-key-run primitives
have focused tests; graph SQL has isolated syntax coverage. Custody/refcount/recovery,
standard-delete oracle and transactional publication integration are still being
implemented. These primitives do not imply an enabled production mode or capacity
certification.

Dataset key verification checkpoint (2026-10-31): frozen composite-key v6 passed 3/3 native integration tests with zero skips (125.88s), including exact boundary scalar values, canonical composite duplicates, null-key rejection, preserved prior typed rows/head, and live-reader retained-history compaction. Evidence: `docs/production/evidence/2026-10-31/native-retention/dataset-composite-pk-v6-20261031T133255Z/`. Incremental MOR/qualified graph and Source delta publication remain under implementation and are not enabled by this checkpoint.

Million-object v10 checkpoint (2026-10-31): exact count/sum/xor oracle passed on the frozen qualified seek release build. Unchanged short open-loop windows completed 600/600 at 20/s (successful p95 56.806ms), 1,500/1,500 at 50/s (63.524ms), and 2,962/3,000 at 100/s (464.074ms; 38 RESOURCE_LIMIT/HTTP429). The 100/s window still fails and this is not full production certification. Raw all-arrival/deadline/success-only metrics, operation-level failed arrivals and resource observations are retained under `docs/production/evidence/2026-10-31/native-projection/qualified-seek-million-v10-20261031T124948Z/`. Native preparation took 1,874.83s; all trial artifacts and limits are recorded.

### Current Source and revocation fixture reconciliation (2026-11-04)

Catalog and filesystem classification fixtures now register, publish and apply
actual Source definitions before exercising domain access. Their inherited-label,
atomic publication and live-revocation assertions retain the production active
resource binding checks. Fixtures that disable a principal assert that its old
credential remains revoked after reenable and mint a new credential for later
independent operations. Native PostgreSQL vector setup tolerates the extension
that the quality image already installs, while retaining its exact engine and
cosine assertions. Focused results and the initial failed expectation are retained
in `production/evidence/2026-11-04/source-fixture-contracts`. These functional
checks do not certify production capacity; the complete independent quality gate
remains required for the integrated revision.

## November 4 Connector custody implementation checkpoint

Connector acceptance now binds up to eight exact operator credential slots to
current caller/service use grants, immutable approval generations and semantic
cursor revisions. Fresh guards cover claim, callback, in-flight work, publication
and result release; material remains in the native HTTP host. Known reflected
material is withheld. The operator can inspect exact Connector approval requests
without reading credentials. Full replacement remains the supported write profile;
other modes and capabilities still require implementation and qualification.

The frozen source compiles all nine native artifacts, passes complete static
checks, provenance and all 79 native units. The new real Sync/Dataset/checkpoint
fixture is written and typechecks but has not yet run. Runtime proof, latest full
quality gates and production-scale evidence remain open; this checkpoint does
not complete N09.

The first composed Connector trial passes custody and generation fencing but
fails engine setup: the shared instance fixture supplied an administrative DB
connection to scratch registration, which requires a non-owning runtime role.
Both fixture callers now use that required role. The next diagnostic exposes
Secret use incorrectly entering a data-operation-only batch helper. Secret use
now calls the existing exact Secret-aware filesystem authority helper, retaining
its kind, operation, markings and current file checks. Expected claim denials are
classified so one denied job cannot stop the worker. A queued Secret file denial
and subsequent successful window are added to the real fixture; fresh combined
native build and runtime qualification are pending. No runtime restriction or
individual deadline is relaxed.

### Connector custody and checkpoint qualification

The registered native replace/full Sync profile now passes eight focused checks
with eight exact operator-approved credentials, a real isolated Python/Arrow
guest, typed Dataset readback, durable semantic cursor reuse and live socket
revocation. The caller needs exact logical operations without filesystem browsing.
[Proof and remaining scope](production/evidence/2026-11-04/connector-secret-custody/README.md)
retain the failed readback trial and exact artifact/fixture identities. Optional
capabilities, other write modes, full HTTP/worker fault coverage and scale testing
remain open; this is not complete N09 qualification.

Function cancellation fixture checkpoint (2026-11-04): the real CLI/API test now
checks the accepted cancellation control receipt and separately verifies terminal
state through authenticated CLI wait, retaining actual Docker cleanup checks.
The full file passed 17/17 with zero skips; all nine artifact checks, 79 native
units and static checks passed. Evidence and the original failed expectation are
under `production/evidence/2026-11-04/function-cancellation-control`.

SQL provenance checkpoint (2026-11-04): external Snow imports preserve durable
request/configuration/raw-pin audit without presenting audit field names as
Dataset lineage. Fixed internal source-to-object pairs use current verified
provenance admission. The legacy indexer checks current permissions/session
before classification SQL and retains final publication checks. Reconciled
rollback, lineage and independent browser-origin fixtures passed the full SQL
file, 48/48 with zero skips, plus all nine artifacts, 79 native units and static
checks. Original and interrupted failures are retained under
`production/evidence/2026-11-04/sql-provenance-contracts`.

### Connector bounded HTTP failures and capability membership

The actual registered Sync/OCI guest fixture now passes 13/13 checks, adding
redirect, truncated-response, HTTP503, oversized-body and original HTTP-timeout
failures with exactly one request/attempt and unchanged prior head/checkpoint.
Batch admission accepts an unordered implementation capability set containing
`batchRead`; other operation dispatch remains unavailable. The exact all-nine/81
unit build, static checks and runtime evidence are retained in
`production/evidence/2026-11-04/connector-http-faults`. This remains functional
replace/full qualification, with the remaining N09 scope recorded there.
Native MOR primitives checkpoint (2026-11-04): the library now includes
receiver-qualified graph traversal, authenticated bounded range reads,
path-copy directories, packed key/locator runs, streamed merge, exact row
selection and bounded verified-v2 bootstrap. `cargo check --locked --lib -j2`
and all 91 library tests pass, with no ignored or filtered tests. An isolated
PostgreSQL check applies the graph schema and rejects unissued read/reuse claims.
The graph and Source delta schemas remain excluded from the installed baseline;
v2 dispatch is unchanged. C04 publication, sidecar retention/clean-host recovery,
sparse writer activation and Source delta publication remain unverified.
See `production/NATIVE-MOR-PRIMITIVES.md` and
`production/evidence/2026-11-04/native-mor-primitives/` for scope and results.

Native graph custody checkpoint (2026-11-04): the fresh baseline now installs
receiver-qualified graph custody. Real coordinator bootstrap preserves the
published v2 head; backup includes every reachable graph content member at its
exact provider version. Fresh isolated restore requalifies actual copied bytes
under new physical versions and incarnation, reconstructs retained roots and
bounded references, and preserves standard Iceberg typed readback. Graph GC
protects pending propagation, respects unchanged grace, and drains committed
retirements across a disabled-policy successor without changing their original
physical/incarnation authority.

Frozen trial008 passes static checks, all nine exact native artifacts, 91 native
units and the full custody integration (1/1, zero skips/cancellations, 164.04s controller wall time),
including real policy succession, busy propagation, conflicting version/reused
target and missing backup-child failures with disabled dispatch. The predecessor
SQL's policy-successor regression is reproduced red. Earlier trial005 separately
passes typed/array key readback and warmed-cache authority revocation (2/2);
these are not combined same-source results. Its 64-row positive has little
margin under the unchanged 120-second root deadline. Failed trials are retained.
C04 v3 publication, sparse writers, distributed projected-key bootstrap, Source
delta publication, full independent gates and production capacity remain open.
See `production/NATIVE-MOR-PRIMITIVES.md` and
`production/evidence/2026-11-04/native-graph-custody/` for exact scope/results.

Indexing admission checkpoint (2026-11-04): revoked preclaim work now becomes a
durable failed build instead of terminating the shared drain. Known authority
rejections use savepoint isolation under the selected row lock; unknown/schema
errors still propagate, and no lease is fabricated. The schema-lease test watches
its actual PostgreSQL blocker while retaining migration fencing assertions.
All original indexing cases plus actual plan/task/advance failure-isolation
regressions passed 16/16, with all nine artifacts, 79 native units and static
checks. Evidence: `production/evidence/2026-11-04/indexing-preclaim-isolation`.
Native key-index correction (2026-11-04): bounded and streaming partial compaction
now retain tombstones so untouched older levels cannot resurrect deleted keys.
The actual v3 root validator permits paired tombstone directories with zero live
rows while preserving positive-row, pairing and count checks. All three new
regressions fail on prior behavior (91 existing units pass) and pass after the
fix; frozen trial009 passes all 94 native units, nine-artifact provenance and
full static checks. This is library correctness evidence, not an activated
all-deleted Dataset mutation proof. `production/NATIVE-MOR-INDEX-PROTOCOL.md`
settles explicit deferred digests, version-three run tags with version-two reads,
exact touched-row verification and bounded distributed projected-key bootstrap.
Those remaining APIs, sparse publication and their integration proofs remain
under implementation. Evidence: `production/evidence/2026-11-04/native-index-tombstones/`.

### Decoded Connector reflection and instance-fenced test scratch

The host now rejects known credential material after JSON decoding as well as in
raw bytes, covering escaped values and keys before any guest response. The real
15/15 Connector gate verifies absent material in logs/staged checkpoints alongside
the existing custody, typed Dataset, HTTP and revocation cases; all99 native units
and artifact fingerprints plus full static checks pass. Separate test instances
now receive separate freshly provisioned scratch pools, preserving incarnation
fencing. Evidence and retained failures are in
`production/evidence/2026-11-04/connector-decoded-reflection`. Full Pipeline, final
integrated quality/CI, remaining capability handlers and scale targets stay open.

### Bounded model capability probes

Probes now buffer their bounded provider stream under the existing independent
live guard and protected completion fence, avoiding duplicate full authorization
per token. Full Models19/19 and Secret custody11/11 pass, including actual held
probe revocation; all99 native units/artifact fingerprints and static checks pass.
The real local Qwen3/Nomic trial now completes all chat/stream/tool/structured and
embedding probes before a later logic-configuration API failure. Retained failed
composed trials and exact component proof are in
`production/evidence/2026-11-04/model-buffered-probes`; full platform and capacity
qualification remain open.
Deferred digest format checkpoint (2026-11-04): new native runs use explicit
version-three tags for deferred hash/locator entries. Locators represent a
missing digest explicitly; a verified all-zero digest is still a known digest.
Readers preserve version-two known-record compatibility and reject new tags
under that older version, unknown versions and mismatched headers. Frozen
trial010 passes all 96 native units and exact provenance for all nine rebuilt
executables. Current bootstrap still emits verified payload hashes; projected
scanning, touched-row derivation, distributed task closure and sparse publication
remain unactivated. Evidence: `production/evidence/2026-11-04/native-deferred-digests/`.

Projected key derivation checkpoint (2026-11-04): serial keyed bootstrap now
projects actual typed key columns and records explicit deferred digests. Exact
receiver-qualified retained v2 member/version receipts can use the existing
verified-spool/retained-subset path; unsupported legacy versions retain full
validation. The new touched-key API resolves the actual qualified locator,
checks file/physical row/sequence/selection, applies existing Parquet allocation
guards, validates the complete typed row and full encoded key, verifies any
known digest and rechecks current authority. It accepts no supplied locator.

Frozen trial013 passes static checks, all nine native artifacts, 96 units and
64-row array-key/nullable-payload readback with every bootstrap digest deferred
and a full typed point-row oracle (1/1, zero skips/cancellations; 101.67s root under
120s, 108.75s controller). A failed helper test exposed generic schema validation
accepting extra columns; exact key/full-row column guards now reject them.
Initial chunk certification still performs one-time full byte authentication
when prior receipts are absent. Distributed tasks, new-loader negatives,
latest-format authority/C08 reruns, actual standard position-delete oracle,
sparse C04 publication and production workloads remain open. Both Dataset
contracts now explicitly use standard position deletes to preserve accepted
array keys, with actual installed-reader proof required before activation.
Evidence: `production/evidence/2026-11-04/native-projected-keys/`.

Qualified native loader checkpoint (2026-11-04): retained-v2 reuse now requires
the stored sealed reader profile, environment, Dataset and snapshot to agree
with the actual current head. Root wrappers and pages are read at their exact
qualified provider version with unchanged hash, canonical and structure checks.
A real old-version deletion trace reproduced newer-current substitution before
the fix. The corrected reader explicitly requests the old version, accepts it
while present and rejects its actual absence without head or graph publication.

Frozen trial015 passes full static checks, nine-artifact provenance, all 99
native units and 12 integration cases across four separate sequential files
(4 version traces, 6 qualification negatives, 1 warmed authority revocation,
1 full C08 custody), with no skipped or cancelled cases. C08 passed in 133.71s
root / 146.46s controller and retains exact-version collection, policy successor
propagation, independent clean-host typed recovery, fresh qualifications and
missing-child rejection with dispatch disabled. All controllers were removed.
Wrong point locators, sequence, full key, digest and schema have negative units;
a real installed Parquet two-row-group unit verifies relative selection.
Evidence: `production/evidence/2026-11-04/native-qualified-loader/`.

The earlier trial013 array-key positive remains separate evidence. No sparse
Dataset mode or distributed bootstrap is enabled by this checkpoint. Their
publication/journal integration, independent standard position-delete readback,
large multi-row-group Dataset and legacy fallback fixtures, whole-platform CI
and approved capacity ladder remain required.

Dataset commit journal checkpoint (2026-11-04): actual managed replacements and
certified unchanged metadata pruning append immutable contiguous edges in their
existing publication transaction. Records bind exact qualified old/new roots,
normalized Iceberg schema and existing partset hash, ordered keys/current key
hash, immutable binding revision, actual candidate or maintenance-run UUID and
sealed completion/publication receipt. Existing private Job versus authenticated
session authority paths remain distinct. Metadata pruning records mode=null and
checks exact current-snapshot/non-history-metadata equivalence; native decoding
validates the actual persisted semantics before commit. A non-advancing result
creates no fabricated Dataset revision.

Frozen trial021 passes full static checks, all nine exact native artifacts, all
102 units and the real two-case journal gate (0 skipped/cancelled; 31.26s and
32.13s roots under unchanged 120s deadlines, 64.92s controller). Native publication
acknowledgement replay creates no duplicate edge. Ordinary journal insertion and
updates are denied. An exact injected journal failure rolls back head,
publication, execution completion and journal, preserving prior typed rows.
Earlier failures and a safe synthetic schema diagnostic are retained in
`production/evidence/2026-11-04/native-dataset-commit-journal/`.

Sparse mutation/no-op/row-compaction producers, qualified changed-row/delete-key
roots, chain-derived actual indexing pins/consumer IO, incremental Source/Search
activation and distributed bootstrap remain open. A fresh full graph recovery
rerun after this schema addition, combined whole-platform checks, independent
current CI and production workload evidence are still required. This fresh
baseline checkpoint does not migrate a live installation.

Replacement journal recovery checkpoint (2026-11-04): TS-only overlay022 on the
exact trial021 native artifacts passes static/provenance, all 102 units and the
full C08 gate (1/1, zero skips/cancellations; 152.87s root under unchanged 300s,
154.19s controller). Two actual backed-up replacement journal records retain
their immutable identities, normalized schema/key/binding facts and exact
producer/publication/completion receipts. Every basis/result root resolves
through the actual new-physical/new-incarnation restore qualification. Real
HEAD/GET/SHA checks use copied wrapper versions at the fresh prefix; original
audit versions return 404 there. Actual newly authorized typed reads recover
both heads, while old-incarnation journal producer reentry fails exactly with
CANDIDATE_STALE and changes no record. Existing graph collection/succession and
missing-backup disabled-dispatch coverage remains green. Evidence:
`production/evidence/2026-11-04/native-dataset-commit-journal/recovery/`.
This backup precedes metadata pruning; its later unchanged edge is not claimed
restored. Sparse v3 publication and actual incremental window custody/replay
remain under implementation.

V3 Catalog foundation work in progress (2026-11-04): strict version-three
references, a metadata-independent versioned index summary, actual stored-root
profile dispatch and a common bounded native batch-stream interface are staged
on the development branch. Catalog/EngineIO/Evaluation wiring is partial. No v3
head producer or sparse mode is activated; Worker/attestation, staged validation,
root registration, recovery/pruning and actual end-to-end qualification remain
pending. These edits have formatting/schema generation only; native/static and
integration checks have not yet run. This checkpoint preserves unfinished source
before integrating the current Source store/IO changes.

Unactivated v3 foundation checkpoint (2026-11-05): frozen trial025 passes full
static checks, exact provenance for all nine rebuilt native artifacts and all
112 library units with no ignored/filtered tests. It compiles strict v2/v3
reference dispatch, the metadata-independent index-summary binding, actual
stored-root profile selection and common Catalog/EngineIO/Evaluation bounded
scan adapters against the integrated Source/store/IO baseline. Legacy unscoped
readers reject both native formats. The existing FlatV2 retained-snapshot test
keeps its standard-table/history/count assertions; common scans use the
format-aware count witness. Failed compiler-memory and adapter trials are
preserved. Evidence: `production/evidence/2026-11-05/native-v3-catalog-foundation/`.

No v3 head or sparse mode is activated by these checks. Actual stage-bound
preparation, root qualification/publication, Worker/attestation, recovery/pruning
and runtime integration remain pending before activation. This is a bounded
foundation checkpoint rather than completion of N09 or N18.

V3 custody checkpoint (2026-11-05): trial039 qualifies two actual v3 published
heads through backup, non-advancing expiry, exact source root-version collection
and fresh-prefix restore. Capture, full byte/hash copy and post-copy validation
use persisted receiver-qualified versions; an identical newer current version
cannot replace them. A missing selected version yields the exact enumeration
failure with no sealed capture or head change. Fresh restore qualifications,
typed current/retained graph reads, journal root remapping, original successor
policy/propagation behavior and dispatch-disabled failure cases are exercised.
The original v2 sidecar custody regression uses the same exact image. Static
checks and all 113 native units pass. Evidence:
[qualification](production/evidence/2026-11-05/native-v3-custody/summary.json).

Production output selection remains unchanged. V3 expiry retains current
standard metadata/history files until graph metadata pruning is implemented;
it fabricates neither a revision nor a delta edge. A separately identified
closed unpublished attached-candidate expiry/retirement origin mismatch remains
pending a genuine prepare/cancel/GC regression. V3 reader integration, sparse
position-delete writes, distributed bootstrap, incremental Source chains,
production scale qualification and full independent CI remain required.
