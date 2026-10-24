# Moria core contracts v0.2

Normative words describe Moria's reviewed design, with the Iceberg integration mechanism pending Phase 0 proof. They do not assert Foundry equivalence. D03 is finalized: restricted views serve reads and Ontology backing, but cannot be transform inputs.

## C01 — resource identity, repositories and environments

A logical resource has an installation-assigned UUID, immutable project ID and kind, namespace, mutable display name, and immutable revisions. Names are unique within `(project, namespace, kind)`. Source references are `namespace/Kind/name`; no project-wide shorthand ambiguity remains in v0.2. Namespaces themselves have metadata.namespace `_project`; all other resources refer to a declared namespace. Object type names are metadata, not object identity.

`moria init/register` registers a deployment-unit UUID and project UUID. A checked-in resource lockfile binds namespace/kind/name to logical resource IDs. The server allocates IDs during initial registration/plan; offline validation uses source references and cannot invent target IDs. A rename requires an explicit old-ID/new-name operation; omission does not delete. The manifest's resources are owned by its deployment unit. Ownership transfer requires both transfer authority and current revision checks.

Logical resource IDs are shared by environments in one project. Runtime keys include environment ID; development rows, edits, grants, checkpoints and secrets never become production data through code promotion. Contract packages contain logical resource IDs and schema fingerprints. Moving to a separate installation requires an explicit import/binding step and regeneration of target contract packages; identical compiled packages are not assumed portable across installations.

An application pins a contract package digest. The environment resolves its IDs to compatible active revisions. Additive properties are compatible if existing queries/results retain their meanings; removing/renaming properties, changing types/nullability to reject formerly valid values, changing action required inputs, or changing UID mapping is breaking. New required action inputs are breaking; optional inputs with defaults can be additive. Security tightening is immediate even if it changes visible results. An old client must never retain access just to preserve compatibility.

## C02 — object UIDs and canonicalization

UIDs are opaque UUID strings, unique in an installation-wide registry, permanently bound to one logical entity. UUID generation is backed by a uniqueness constraint with collision retry; randomness alone is not the uniqueness contract. An object UID has one home object type in v0.1. Different representations requiring multiple types use explicit linked objects; polymorphic identity is later work.

Transform authors match/coalesce source records and supply an identity domain plus canonical key to a narrow `identity.resolve` worker operation. The durable mapping `(identityDomain, canonicalKey) -> UID` is idempotent and survives failed builds, source deletion and restore. The registry does not inspect source records, guess matches, or merge entities. Reserved but unused IDs are harmless. Workers can resolve only their declared output identity domains.

A canonical key must identify an entity for its lifetime. A raw key that can be recycled must include a source incarnation identifier or be mapped through a durable, versioned identity dataset maintained by the transform. If upstream provides no way to distinguish incarnations, the transform must fail/quarantine ambiguous rows for operator resolution. Namespacing a raw key prevents cross-system collisions but does not solve temporal reuse. Changing matching logic may attach an additional canonical alias only through an explicit audited identity migration; it must not allocate new UIDs for existing entities silently. Merging/splitting existing UIDs is deferred, with a migration-required error.

Canonical output contains exactly one row per UID. Duplicate UIDs fail indexing, including equal duplicate rows. Imported UIDs require pre-registration/binding in the registry. Environment datasets may contain the same logical UID, but all state access remains environment-scoped. Action-created objects use the same allocator with a key bound to action request identity. Attaching imported source data to such an object requires explicit mapping, never key collision heuristics.

## C03 — values, rows and schema evolution

Declared values: string (UTF-8, case-sensitive), boolean, int64, float64, decimal, date, timestamp, UID and one-dimensional arrays of non-null scalar elements. Decimal has precision 1–38 and scale 0–precision. Wire int64 and decimal values are strings to avoid JavaScript precision loss. Floats must be finite. Dates use `YYYY-MM-DD`; timestamps are UTC RFC3339 normalized to microsecond precision. UID text uses lowercase UUID canonical form. The compiler enforces ranges, precision, scale and timezone normalization.

Dataset rows contain every declared column; missing nullable input is normalized to explicit null during ingestion, missing required input fails. Unknown columns fail unless a new schema revision is deployed. Object responses distinguish omitted unauthorized properties from explicit null. Required properties must exist after source mapping and overlays. Arrays retain order and duplicates; equality is element-wise. Array ordering/aggregation is unsupported initially; contains-element is supported. Enum/union/nested structures are future extensions.

New nullable columns may be added with null defaults. Type changes, primary-key changes and dropping referenced columns require a migration plan. No silent numeric narrowing or case normalization. Retained overlays incompatible with a new property type block activation until transformed or explicitly cleared with audit. All schema fingerprints are computed from canonical JSON, not YAML formatting.

## C04 — dataset commits and source completeness

Each dataset is an Iceberg table. A Moria snapshot record references the exact Iceberg snapshot ID, metadata location, schema/partition IDs, expected parent heads, input snapshots, producing build/sync, lineage and effective markings. Iceberg owns the table manifest format. Data files and candidate metadata are durably written and verified before publication. A Moria-controlled catalog adapter must publish Iceberg metadata pointers, all promised output heads, lineage and ingestion checkpoints in one PostgreSQL transaction. Stock per-table commits followed by a separate Moria commit are insufficient. This mechanism requires Phase 0 crash/concurrency proof; files are never transaction coordinators.

Moria reads pin explicit Iceberg snapshots; multi-input selection resolves the head map in one catalog read transaction. An external client making independent table loads can observe different commit times unless it uses the pinned Moria snapshot set; do not claim cross-table read-snapshot consistency for unrelated external reads. Other writers must use the same authorized commit path. Dataset UID uniqueness remains a Moria validation invariant, not an assumed Iceberg primary-key constraint.

Publication requires expected output-head revisions and a current fenced worker lease. Any head conflict rejects the entire output group with `HEAD_CONFLICT`; old builds cannot overwrite newer results. A retry of the same logical publication returns its committed snapshot IDs. Input snapshots are pinned at scheduling; scheduling does not imply latest-at-commit input data.

Modes:

| Mode | Required semantics |
|---|---|
| replace | Complete logical output replaces prior contents. Missing prior rows imply deletion only when the completed source contract declares full coverage. |
| append | Add rows; does not imply deletion. Declared key uniqueness is checked against the result, not only this batch. |
| upsert | A keyed batch replaces entire rows at supplied keys; unspecified keys remain. Partial-column patches are not dataset writes. |
| delete | Explicit key list removes those rows; requires a declared primary key. |

Transforms initially emit replace snapshots. Connector capability/configuration can permit other modes; unsupported modes fail before executing. Sources must declare `full`, `incremental`, or `appendOnly` coverage. A full scan is staged until successful completion; failed pages do not publish a partial replacement. Incremental sources require explicit deletes. A transform combining sources decides canonical existence from completed input snapshots; disappearance from one contributing system need not remove the canonical object.

Checkpoints are scoped to `(environment, connection, stream, configurationRevision)` and advance only with their data commit. One leased writer owns each stream. Reconfiguration requires a checkpoint compatibility declaration or an explicit reset/backfill. Retry after commit-before-ack reads the committed checkpoint; source replay may still require deduplication using declared event keys. No generic exactly-once source delivery claim.

Iceberg snapshot expiration, compaction and orphan-file cleanup are coordinated with Moria retention. Garbage collection deletes only files unreferenced by every retained Iceberg snapshot/metadata root and Moria pin, after a 24-hour grace period. Snapshot/release/cursor/job/backup references protect data. A backup pin is established before collection can run. Retention cleanup must not remove UID reservations, active overlays, action key tombstones or audit records accidentally.

## C05 — serving revisions and pagination

Source data is staged as an immutable serving generation and activated atomically. Source activation and actions each allocate an environment commit revision under the same short database revision lock, ensuring commit-order tokens rather than using a nontransactional sequence as a visibility guarantee. The first implementation trades write concurrency for an unambiguous watermark. Record this bottleneck in benchmarks.

Versioned source generations and edit/link histories support reads at revision R. A query pins a release-compatible environment generation map plus R. No database transaction remains open between HTTP pages. Cursors contain an authenticated opaque reference to R, query digest, application/principal/delegation, contract, security epoch, tie-breaker and expiry. TTL is 15 minutes from the first page, never extended. Retain required versions while cursor references are live. Expired cursor: `CURSOR_EXPIRED`; changed security epoch: `CURSOR_INVALIDATED`; neither silently restarts.

Security checks use current policies/attributes. A current row-security predicate uses both the pinned row and its current effective row: both must permit access. A deleted current object is not returned through an older cursor. This prevents old data revisions bypassing access changes caused by a region/owner edit. Policy semantics may reduce an old page's result set; newly authorized data requires a new query. No promise to recall bytes already returned.

Queries: eq/ne, ordered scalar comparisons, in, isNull, and/or/not, and array contains. Equality against null is invalid; use isNull. Ordinary comparisons with null are unknown; filters include only true. Boolean expressions use three-valued logic. Strings compare by a specified Unicode code-point order, not deployment locale. Null sorts last in both directions. Stable sorting appends UID ascending. Page size defaults to 100, maximum 1,000; query timeout 30 seconds.

Aggregations include visible-row count and numeric sum/min/max/average; null numeric values are ignored, empty sum/min/max/average return null. count returns int64 string. Unauthorized properties cannot be filtered, ordered, grouped, aggregated or traversed as link keys. Hidden/inaccessible point reads return the same `NOT_FOUND` as absent objects. Links expose only edges whose endpoints and link access are authorized. Link cardinality violations fail generation activation or action commit, not query time. Initial links are keyed many-to-one mappings or action-managed relations; mapping schema declares the mode.

Static object sets store UIDs and reauthorize every read; missing objects are omitted and returning UIDs reappear. Dynamic sets store a typed query, evaluate against the current selected revision and reauthorize. Both are versioned resource definitions; set sharing grants no object access. No total count of hidden keys is returned.

## C06 — edits, deletion and actions

Effective value = active overlay when present, otherwise mapped source value. `set(property, null)` is an override and requires nullable property. `clearOverride(property)` removes it and reveals the current source value; on action-created objects clearing a required value without a source is invalid. UIDs are immutable.

Confirmed source deletion hides a source-backed object but retains overlays/history. Reappearance of its UID restores overlays. Explicit action deletion creates a separate tombstone that source refresh cannot clear; an authorized restore action removes it. The object is still invisible if no source row exists, unless action-created. Content purge is an administrative maintenance operation with dependency/retention checks; it retains a minimal non-reusable identity reservation and documents loss of reanimation content. Purge API implementation is later, not an implicit delete behavior.

Initial actions declare typed inputs, a condition expression and an ordered edit list. In one serializable transaction, resolve targets, check current identity/scopes/markings, authorize reads/writes, validate conditions and optional expected versions, apply the edit list, check post-state policies and link constraints, and commit edits/audit/outbox/idempotency outcome together. All referenced reads participate in the transaction. Cross-environment edits are rejected. Hard defaults: 100 objects/action, 5-second database transaction timeout, three internal serialization retries. Exhaustion returns retryable `CONCURRENT_MODIFICATION`; validation/permission denials are nonretryable.

Functions execute outside this transaction and may return a typed edit proposal plus the object versions read. Commit rechecks versions and declarative conditions. Initial function proposals cannot depend on unconstrained aggregate/predicate reads; such dependencies require a declarative condition re-evaluated in the transaction. No provider call, arbitrary user code or external write occurs inside the action transaction.

Clients supply idempotency keys for every action. Scope: environment + application + principal/delegation + stable action ID + key. First accepted request pins action revision, input hash and request ID. Same key/different payload or explicitly different revision returns `IDEMPOTENCY_CONFLICT`. Same key with omitted revision returns the original outcome even after deployment, subject to current read authorization. Concurrency on the same key returns `IN_PROGRESS` or the completed result. Unauthorized requests cannot reserve keys. Store completed result bodies 30 days, then retain key/hash/status/commit-reference tombstones indefinitely. A later retry returns `RESULT_EXPIRED` and never executes again. Result retention is configurable but cannot enable silent key reuse.

Actions return changed object versions and a commit token. Initial PostgreSQL queries/point reads can choose a revision at least that token; existing pagination cursors cannot be advanced, so restart a query for newer state. Future asynchronous indexes must wait up to a requested timeout or return `CONSISTENCY_TIMEOUT`. External writeback is an outbox delivery with its own pending/delivered/failed state and idempotency key; delivery failure never pretends the committed action rolled back.

## C07 — deployment, rollback and APIs

Plans bind desired graph/artifact digests to environment release-map revision and current security epoch. Compile validates resource ownership, references, dependencies, contracts and authority. Stage schema/index changes without changing active traffic. Activate through compare-and-swap, rechecking all consumers and permissions. Failed staging leaves old releases active. Omitted resources remain; removals require an explicit deletion list and dependency checks.

Security configuration has a separate active epoch from code releases. An administration deployment can change security, but code rollback never restores old grants, markings or declassification authority. A combined release requiring a policy update must name that prerequisite; no cross-unit atomicity is implied. Promote the same artifact digests across environments with explicit secret/identity-provider bindings. Environment data stays separate.

Browser applications authenticate with OIDC authorization code + PKCE; CLI uses the same browser-assisted flow with loopback callback, with device flow only when the provider supports it. Headless CI uses a dedicated workload identity. Setup/admin actions remain CLI-accessible; browser login is allowed. Frontend assets are immutable, served separately from APIs and authenticated; no SSR or custom HTTP services.

API prefixes remain `/management/v1` and `/api/v1`. Query/action objects use JSON with strict validation and stable error codes. HTTP: 400 invalid input, 401 unauthenticated, 403 forbidden resource operation, 404 hidden/absent object, 409 conflict, 410 expired cursor/result, 422 unsupported capability, 429 quota, 503 unavailable, 504 timeout. Error envelope: code/message/correlationId/retryable/details, with no hidden values in details. CLI exit codes retain the foundation mapping. OpenAPI route schemas and generated SDK compile tests are a Phase 1 deliverable; this package defines declaration schemas, not a completed HTTP API implementation.

## C08 — recovery and upgrades

First backup procedure is quiesced: stop new mutations, drain/fence workers and outbox dispatch, pin referenced artifacts, then capture PostgreSQL including Iceberg catalog pointers, all retained Iceberg metadata/manifests/data files, identity-provider state and encrypted secrets/configuration with checksums. Include UID reservations, current policies, action dedupe tombstones and delivery records. A host-local volume is not a backup; export to an operator-chosen separate destination. Restore to a new host with dispatch disabled, verify manifests and identities, rotate runtime credentials, then explicitly reconcile outbox deliveries before enabling them. Replaying an old backup must not automatically resend already delivered external effects.

Cross-project and cross-environment data import are explicit audited operations. A restored installation replacing the old host preserves its identity authority; running both as independent writers after restore is unsupported. Upgrade uses expand/migrate/contract where coexistence is possible. Non-coexistent migrations require maintenance and a verified backup. No zero-downtime or automatic disaster-recovery SLA is promised in the first slice.
