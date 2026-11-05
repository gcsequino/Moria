# Implementation phases and acceptance plan

This plan records the original first-slice acceptance phases. D03 finalizes the restricted-view boundary. See the [implementation checkpoint](../IMPLEMENTATION.md#native-integration-and-production-goals--november-5-2026) for execution results. Each phase ends in runnable evidence; earlier phases are internal milestones, not a launch with missing security. The full first slice requires all phases below.

## Phase 0 — prove mechanisms and pin dependencies

Create the implementation monorepo after review: `apps/api`, `apps/cli`, `packages/contracts`, `packages/compiler`, `packages/osdk`, `packages/authorization`, `services/supervisor`, `runtimes/python`, `runtimes/typescript`, `adapters/agents`, `deploy/compose`, `tests/acceptance`, `examples/maintenance`.

Pin compatible supported runtime/package versions and image digests, inventory licenses, and add Moria's accepted Apache-2.0 license and notices before public release. Prove serializable action retry, pinned-revision pagination, policy invalidation, fenced Iceberg publication and isolated worker egress. Prove a multi-table catalog-pointer update plus ingestion checkpoint/lineage commit under crashes; validate readers/writers and coordinated snapshot expiration. Benchmark a small version-history model. Deliver records with exact commands and outcomes. If the stack cannot meet the contracts, revise the decision before building on it.

## Phase 1 — schema/compiler/catalog/CLI and security authority

Implement strict declarations, semantic compiler, immutable revisions, deployment-unit ownership, environment mapping, plan/deploy/rollback, artifact storage and OIDC login. Add current grants, marking clearances, security epochs and audit before serving user data. Generate management API schema and TypeScript client from the IR. Exit: separate admin/domain/app manifests deploy, stale plans fail, scope self-escalation fails, secrets stay out of artifacts and JSON output is scriptable.

## Phase 2 — dataset commits, identity and transform execution

Implement Iceberg tables/metadata through an existing library and Moria catalog adapter, mode semantics, atomic multi-output heads, lineage, identity allocator, fenced jobs, Python/DuckDB worker protocol and test connector. Include transform preview without publication. Exit: two-source consolidation produces one stable canonical UID, failed full scans do not delete data, retries and crashes cannot advance checkpoints without data, and older builds cannot overwrite newer heads.

## Phase 3 — Ontology, complete first-slice security and actions

Implement source generations, versioned overlays/links, typed query compiler, cursor history, markings/restricted views/object/property policies, sets and serializable actions/outbox. Exit: query and action concurrency tests plus the full security matrix pass; source deletion/reanimation and explicit tombstones behave differently; old idempotency keys cannot replay after deployment. This is the main correctness gate.

## Phase 4 — shared OSDK and React flow

Generate typed object/action bindings with optional restricted fields, browser/workload auth and structured errors. Serve static React queue/detail/action frontend. CLI exercises the same operational API. Exit: frontend and service identity see their authorized subsets, a browser action is visible through the returned token, incompatible contracts produce upgrade-required errors, and no custom server deployment path exists.

## Phase 5 — gateway, agents, retrieval, evaluation and MCP

Implement one model protocol adapter, LangGraph.js adapter, pgvector exact retrieval, fixture ingestion, trace controls, evaluation runner, scheduler and scoped MCP wrappers. Use a mock model for deterministic CI and a separately recorded real configured-model smoke test. No test claims to have validated the user's PC endpoint until explicitly connected/tested. Exit: agent uses generated OSDK, forbidden model destinations fail, retrieval never returns denied content, evaluation cases and errors are inspectable through CLI, and no provider secrets reach agent code.

## Phase 6 — install, restore, upgrade and performance

Deliver pinned Compose packaging, documented provisioning/TLS/secrets, quotas and diagnostics. Back up quiesced state to a separate destination, restore on a clean Linux host and repeat the query/action/agent flow. Verify outbox dispatch stays paused until reconciliation. Run declared benchmark workload and publish measured limits. Release only after security/restore/license gates pass; no HA claim.

## Canonical acceptance matrix

The following replaces the conflicting earlier first-slice lists. Cases are acceptance specifications, not executed tests. Each implementation test must preserve fixtures and observed results under these IDs.

| ID | Setup / stimulus | Required outcome | Phase |
|---|---|---|---|
| COMP-01 | Unknown declaration field, bad reference kind, cyclic transforms | Strict validation/compiler rejection with paths; link cycles allowed | 1 |
| DEP-01 | Two plans from same environment revision; activate first then second | Second gets stale-plan conflict; no partial activation | 1 |
| DEP-02 | App requests additional scope without an authorized Grant | No new access; deployment cannot mint grants | 1 |
| DEP-03 | Domain change removes property used by registered app | Reject until consumers migrate or explicit coordinated maintenance | 1 |
| SEC-01 | Reader with ordinary read but missing one of two markings | No data; obtaining marking alone still lacks ordinary grant | 3 |
| SEC-02 | Add marking after descendant snapshots already exist | Existing descendants restricted before next authorized read; rollback does not remove restriction | 3 |
| SEC-03 | East-region user reads restricted view with East/West rows, then declares it as a transform input | Only East rows; direct backing read denied; compiler rejects the transform before execution. Ontology backing remains allowed and rechecks current policy. | 1/3 |
| SEC-04 | Denied property queried via projection/filter/sort/group/aggregate | No values or predicate leakage; explicit PROPERTY_FORBIDDEN independent of data | 3 |
| SEC-05 | Hidden target linked from visible object; stored set contains hidden UID | No hidden target/count/key exposure; sets reauthorize | 3 |
| SEC-06 | Revoke group/scope after first page and during agent execution | Cursor invalidated; next tool/model/output operation denied; code rollback cannot restore permission | 3/5 |
| SEC-07 | Current row moves out of region after cursor pinned old data | Old snapshot does not reveal now-denied row | 3 |
| SEC-08 | Worker attempts raw DB, blob path, provider egress or another worker's publication | Denied; audit safe; no host secret exposure | 0/5 |
| DATA-01 | Crash before files durable, before catalog commit, after commit-before-ack | No partial snapshot; committed retry returns same snapshot/checkpoint | 2 |
| DATA-02 | Multi-output Iceberg build with one head changed concurrently | All Iceberg catalog pointers, Moria heads and checkpoints unchanged by losing build; HEAD_CONFLICT | 2 |
| DATA-04 | Expire/compact snapshots while job, cursor or backup pins old data | Pinned metadata/data retained; exact-snapshot read remains correct; unsupported reader features fail explicitly | 2/6 |
| DATA-03 | Failed full scan omits previous UID; incremental batch omits it | Neither implies deletion; only full committed replacement or explicit delete does | 2 |
| ID-01 | Two sources describe same work order; transform coalesces fields | One row/UID with explicit precedence; subsequent rebuild preserves UID | 2 |
| ID-02 | Two unrelated source records use key 123; later one recycles it | Different canonical keys/incarnations -> different UIDs; ambiguous incarnation fails | 2 |
| ID-03 | Same UID appears twice in canonical output | Reject generation, no implicit merge | 3 |
| OBJ-01 | Assign Bob, remove canonical source UID, then restore it with Carol | Hidden while absent, Bob overlay restored on same UID | 3 |
| OBJ-02 | Set null then clearOverride; action delete then source refresh | Null distinct from clear; action tombstone persists until restore action | 3 |
| ACT-01 | Two actions both try to claim an unassigned work order | At most one condition passes/commits; serializable retry re-evaluates | 3 |
| ACT-02 | Lose action response, deploy new revision, retry same key/input | Original outcome returned; no second mutation | 3 |
| ACT-03 | Same key/different input; retry after stored result expires | Conflict / RESULT_EXPIRED; neither re-executes | 3 |
| ACT-04 | Fail last edit in multi-object action | No edits/audit-success/outbox success committed; failure audit safe | 3 |
| ACT-05 | Writeback delivery fails after action commits | Object change remains; delivery reports independent retry/failure | 3 |
| READ-01 | Null values, Unicode strings, arrays, stable tied sort and pagination | Documented type/null/order behavior matches reference evaluator | 3 |
| READ-02 | Action alters sort field between pages | Pinned data revision prevents duplicate/moving rows; security reductions still apply | 3 |
| READ-03 | Query with new commit token; expired cursor; unsupported cross-index token | Visible committed edits / explicit expiry / explicit consistency error | 3 |
| APP-01 | Same generated SDK used by React and agent workload identity | Typed queries/actions, different authorized subsets, no alternate agent data path | 4/5 |
| AI-01 | Marked retrieval passage sent to forbidden model destination | Gateway refuses before provider request; traces stay protected | 5 |
| AI-02 | Delete/revoke document after vector generation | No stale passage/provenance/count exposure | 5 |
| AI-03 | Stream interrupted; tool retry; evaluator case error | Incomplete result explicit; one action effect; case error separate from fail | 5 |
| AI-04 | MCP caller invokes forbidden action | Same denial and audit as OSDK; no shell/database bypass | 5 |
| OPS-01 | Restore backup into clean host with edits/policies/UIDs | Same authorized query/action behavior, no UID reuse; external dispatch paused | 6 |
| OPS-02 | Disk full, database unavailable, worker killed, lost notification | Bounded errors, no partial commits; durable jobs rediscovered | 6 |
| OPS-03 | Million-scale datasets and Ontology; progressive 1m/10m populations, 20 clients on declared hardware, realistic widths/history/policy selectivity, concurrent ingestion/actions | Separately measure data pipelines and authorized serving; report sustained throughput, p50/p95/errors, memory/storage and lock waits. Target p95 indexed first-page query <500ms/action <1s; point-read or 100k smoke results are insufficient. Document hardware requirements and bottlenecks honestly. | 6 |

## Fixture design

Maintenance fixture: two source systems with overlapping work orders, explicit source incarnation fields, equipment links, region East/West, protected internalNote property and a sensitive marking. Personas: reader-East, operator-East, operator-West, auditor, security-admin and autonomous/delegated agent identities. Use at least one object visible but not editable, one marked object without clearance, and one invisible linked object. Mock providers record whether a request was received so destination denials can be verified, not inferred from client errors.

## Review and change discipline

User-approved changes update the decision row, prose, schema/examples and affected acceptance cases together. New defaults do not become accepted scope merely because an example validates. Runtime tests, benchmark results and reference comparisons must be reported separately. Implementation may start after review corrections are incorporated; nothing in this package deploys or publishes the platform.
