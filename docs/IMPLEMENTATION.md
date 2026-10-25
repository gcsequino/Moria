# Implementation checkpoint

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

| Command | Result |
|---|---|
| `pnpm format` | Passed; protected files unchanged. |
| Focused auth/composition/execution/scheduler tests | 6 passed; no skips. Later auth/composition regression run: 4 passed. |
| `pnpm check` after fixes | Passed: formatting, SDK drift, TypeScript, 348 runtime tests, Python checks, both builds and starter checks; zero failures/skips. |
| `pnpm check:python` | Passed, including Black, specification/workload validation and 14 Python tests. Run separately because full gate stops at runtime failures. |
| `pnpm build` | Passed for management and retained Snow fixture. This is required check coverage, not default startup composition. |
| `pnpm check:starter` | Passed type/build/hosting/static isolation checks. |
| `git diff --check` | Passed. |

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
