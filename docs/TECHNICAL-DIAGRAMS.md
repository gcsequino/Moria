# Moria technical diagrams — implemented state

Based on implementation commit `618b2eb`, October 15, 2026. These diagrams describe the current prototype, not the complete target platform. Boxes represent code components unless explicitly labeled as storage or a process. They do not imply independent deployments.

Rendered SVG copies are available without a Mermaid extension:

| Diagram | SVG |
|---|---|
| Component architecture | [Open](diagrams/01-architecture.svg) |
| Protected query sequence | [Open](diagrams/02-query.svg) |
| Action and idempotency sequence | [Open](diagrams/03-action.svg) |
| Iceberg staging and publication | [Open](diagrams/04-indexing.svg) |
| Object state and reanimation | [Open](diagrams/05-object-state.svg) |

The Mermaid blocks below are the editable sources. SVGs were rendered with Mermaid CLI 12.0.0 using the neutral theme; rendering tools are development-only and are not application dependencies.

## 1. Component architecture

Solid components are implemented. Dashed connections to gray components are planned integrations. Trusted setup supplies bindings and workload credentials; there is no public administration endpoint.

```mermaid
flowchart TB
    Client["Workload client / integration tests"]
    UI["Planned: React UI + generated OSDK"]
    Setup["Trusted local setup"]

    subgraph Node["TypeScript / Node — libraries and local server factory"]
        API["HTTP API<br/>/api/v1 — /v1 compatibility alias"]
        Sessions["WorkloadSessions<br/>opaque bearer authentication"]
        Service["ObjectService<br/>query / point read / single-object action"]
        Policy["Authorization + expression compiler<br/>grants, scopes, markings, row/property rules"]
        Authority["Authority<br/>current identity, attributes, security epoch"]
        Store["ObjectStore + CursorStore<br/>generations, overlays, pinned pagination"]
        Indexer["IcebergIndexer + lease supervisor"]
        Catalog["Catalog<br/>transactions, leases, dataset publication"]
        CLI["CLI + declaration compiler<br/>validation and compiled contracts"]
        Gateway["ModelGateway<br/>destination checks + current access checks"]
    end

    PG[("PostgreSQL<br/>catalog, security, ontology, receipts, cursors")]
    Worker["Python child process<br/>PyIceberg / Arrow reader"]
    Iceberg[("Iceberg metadata + Parquet files<br/>local test warehouse")]
    LLM["Configured model endpoint<br/>local inference smoke-tested"]
    Release["Planned: release-owned binding resolver"]
    Jobs["Planned: durable scheduler / recovery"]

    Client --> API --> Sessions --> Service
    UI -.-> API
    Setup --> Sessions
    Setup --> Authority
    Setup --> Service
    Setup --> Indexer
    Setup --> Gateway
    Service --> Policy
    Service --> Authority
    Service --> Store --> Catalog --> PG
    Sessions --> Authority --> PG
    Indexer --> Authority
    Indexer --> Store
    Indexer --> Catalog
    Indexer --> Worker --> Iceberg
    Gateway --> Policy
    Gateway --> LLM
    Release -.-> Service
    Release -.-> Indexer
    Jobs -.-> Indexer
    CLI -.-> Release

    classDef planned fill:#f1f5f9,stroke:#64748b,stroke-dasharray:5 5,color:#334155;
    class UI,Release,Jobs planned;
```

The model gateway is a separate internal library, not an HTTP route or an agent runtime. Its caller supplies a trusted current-authorization resolver. The Python reader is a child process; production worker sandboxing is not implemented. Rust exists in the comparison benchmark, not in the production serving path.

Sources: [API](../apps/api/src/server.ts), [service](../services/ontology/src/service.ts), [indexer](../services/ontology/src/indexer.ts), [security](../services/security/src/authority.ts), [gateway](../services/model-gateway/src/gateway.ts), [compiler](../packages/compiler/src/index.ts).

## 2. Policy-aware paginated query

A cursor pins the data revision, **not authorization**. Every page must remain allowed under current identity, current policies and current object visibility.

```mermaid
sequenceDiagram
    autonumber
    participant C as Workload client
    participant A as HTTP API / sessions
    participant S as ObjectService
    participant P as Authority / policy compiler
    participant D as PostgreSQL

    C->>A: POST /api/v1/objects/type/query + bearer
    A->>D: Authenticate hashed session token
    D-->>A: Bound environment, principal, application
    A->>S: query(session, validated request)
    S->>P: Current identity, scopes, grants, markings
    P-->>S: Policy projection + security epoch
    Note over S,P: Reject forbidden projection/filter fields before scanning
    S->>D: Acquire shared environment advisory lock BEFORE BEGIN
    S->>D: BEGIN REPEATABLE READ, lock environment row FOR SHARE
    S->>D: Recheck epoch, identity/session, validate schema + markings
    alt First page
        S->>D: Pin current environment revision and 15-minute deadline
    else Cursor page
        S->>D: Validate token binding, epoch and original deadline
        D-->>S: Original data revision + last UID
    end
    S->>D: Query pinned and current source/overlay states
    Note over S,D: Apply canonical-row and effective-object restrictions BEFORE LIMIT
    D-->>S: Page candidates + one lookahead row
    S->>P: Reference-check candidates, apply permitted projection
    S->>D: Recheck current authority, save next cursor if needed
    Note over S,D: Cursor binds caller, app, query, contract, epoch and original expiry
    S->>D: COMMIT, release shared advisory lock
    S-->>C: Revision + permitted objects + optional nextCursor
```

No transaction stays open between pages. Query ordering is UID ascending. Arbitrary sort, aggregation and link traversal remain unsupported. A policy/SQL disagreement fails closed instead of returning the row.

Sources: [ObjectService.query](../services/ontology/src/service.ts), [SQL query builder](../services/ontology/src/query.ts), [cursor persistence](../services/ontology/src/cursors.ts), [transaction admission](../services/catalog/src/catalog.ts).

## 3. Single-object action and idempotent replay

```mermaid
sequenceDiagram
    autonumber
    participant C as Workload client
    participant A as HTTP API / sessions
    participant S as ObjectService
    participant T as ObjectStore / Catalog
    participant D as PostgreSQL

    C->>A: Invoke action + Idempotency-Key + typed input
    A->>S: Authenticated session + action binding
    S->>S: Current grants, scopes, labels, validate edits and policies
    S->>T: edit(request, before/after guard, current-access callback)
    T->>D: Acquire exclusive environment advisory lock BEFORE BEGIN
    T->>D: BEGIN SERIALIZABLE, lock environment row
    T->>D: Recheck epoch, session, active schema and source markings
    T->>D: Look up caller/application/action/key receipt
    alt Matching unexpired receipt exists
        D-->>T: Previously committed result
        Note over T,D: Changed input or expired receipt is an error
    else New logical action
        T->>D: Read current canonical row and latest overlay
        D-->>T: Current source + overrides + tombstone
        T->>T: Compute proposed state, check condition and before/after policies
        T->>D: Advance environment revision, append overlay version
        T->>D: Save idempotency receipt in SAME transaction
    end
    T->>D: COMMIT, release exclusive lock
    T-->>C: Committed revision / original receipt
```

Serialization/deadlock failures retry the database-only transaction up to three attempts. No user code or external network side effect runs inside this transaction. Action writes are currently single-object; multi-object actions and an external-effects outbox are pending. Changing between `/v1` and `/api/v1` does not change the idempotency scope.

Sources: [action service](../services/ontology/src/service.ts), [overlay edits](../services/ontology/src/store.ts), [receipt transaction](../services/catalog/src/catalog.ts).

## 4. Catalog-pinned Iceberg indexing and atomic visibility

```mermaid
flowchart TB
    Start["Trusted caller<br/>session + binding + live worker fence"]
    Auth["Check source read + target index permissions<br/>preserve source markings"]
    Pin["Resolve committed dataset head and snapshot history<br/>pin metadata location + snapshot ID"]

    subgraph Loading["Outside the environment publication lock"]
        Python["Python / PyIceberg / Arrow<br/>read exactly the pinned snapshot"]
        Pipe["JSON-lines stream<br/>bounded frames + pipe backpressure"]
        Stage["ObjectStore.stage<br/>validate and write batches of 1,000"]
        Validate["Validate retained overrides; ANALYZE<br/>seal stage as ready"]
        Provenance["Persist index provenance<br/>caller, binding hash and source pin"]
        Python --> Pipe --> Stage --> Validate --> Provenance
    end

    subgraph Publish["Short exclusive environment transaction"]
        Guard["Check epoch / current access / cancellation<br/>lock and validate persisted worker lease"]
        CAS["Check source-head and schema compatibility<br/>reject marking downgrade"]
        Attach["Advance revision<br/>attach generation pointer + schema + markings"]
        Commit["COMMIT — generation becomes visible"]
        Guard --> CAS --> Attach --> Commit
    end

    Lease["Process-local supervisor<br/>renew lease, cancel on loss<br/>30-minute default deadline"]
    Old["Previously published generation<br/>remains available during staging"]
    Fail["Failure / conflict / cancellation<br/>no partial generation publication"]

    Start --> Auth --> Pin --> Python
    Start --> Lease
    Lease -->|abort signal| Pipe
    Provenance --> Guard
    Stage -->|error| Fail
    Guard -->|denied or stale| Fail
    CAS -->|conflict| Fail
    Old -->|serves until commit| Readers["Authorized reads"]
    Commit -->|new reads see new generation| Readers
```

Only pointer/metadata publication is performed under the exclusive environment lock; the rows are already staged. Compatible same-schema refreshes may cross intervening edits if the source head is unchanged. Failed stages and all generation history are retained; safe garbage collection is pending. Database waits are not immediately interrupted by cancellation.

**Two distinct publications:** this diagram starts with an already committed dataset snapshot. Dataset publication itself uses `Catalog.publish` to commit Iceberg metadata pointers, snapshot history, a receipt and an optional connector checkpoint atomically. It does not copy Parquet files into PostgreSQL. Ontology publication then separately attaches the staged serving rows. There is no single transaction spanning both publications.

The current coordinator supports ordinary Dataset inputs. Restricted views remain valid ontology sources in the spec, but their indexing policy-binding integration is unfinished. They cannot be transform inputs.

Sources: [indexer](../services/ontology/src/indexer.ts), [lease supervision](../services/ontology/src/lease.ts), [stream adapter](../workers/iceberg/stream.ts), [Python reader](../workers/iceberg/worker.py), [stage publication](../services/ontology/src/store.ts), [dataset publication](../services/catalog/src/catalog.ts).

## 5. Object state, revisions and reanimation

This is a logical storage map, not a complete foreign-key ER diagram. Both current and pinned reads compose a source generation with the latest overlay at the relevant revision.

```mermaid
flowchart LR
    Identity["identities<br/>domain + canonical key -> permanent UID"]
    Home["object_homes<br/>UID -> permanent object type"]
    Env["environments<br/>revision + security epoch"]
    Gen["source_generations<br/>environment + type + revision<br/>stage pointer, schema, markings"]
    Stage["source_stages<br/>status, expected revision, fence"]
    Rows["staged_source_rows<br/>stage + UID -> canonical values"]
    Overlay["overlay_versions<br/>environment + type + UID + revision<br/>overrides + tombstone"]
    Effective["Effective object at revision R<br/>canonical values overlaid with latest edits <= R"]
    Hidden["Not returned<br/>source absent OR tombstoned OR policy denied"]
    Visible["Authorized projection<br/>permitted properties only"]

    Identity --> Home
    Env --> Gen --> Stage --> Rows
    Home --> Rows
    Home --> Overlay
    Rows --> Effective
    Overlay --> Effective
    Effective --> Hidden
    Effective --> Visible
```

- If a UID disappears from the source, its overlays and permanent identity remain.
- If that same UID returns, retained edits apply again; a retained tombstone still hides it.
- Setting a property to null is an explicit override. Clearing an override reveals the current source value.
- UID reuse for a different entity is not allowed. Multiple source systems are coalesced by transforms upstream; the store does not perform entity resolution.
- Legacy generations use `source_rows` instead of a stage pointer and remain readable. Legacy schema/marking metadata needs authoritative migration before production use.

Sources: [identity allocation](../services/catalog/src/catalog.ts), [source and overlay storage](../services/ontology/src/store.ts), [policy projection](../packages/authorization/src/objects.ts).

## What these diagrams do not claim

No React/OSDK demonstrator, human OIDC, deployed management API, durable job recovery, complete release resolver, production worker isolation, or complete audit/backup/restore exists yet. The diagrams show implementation boundaries, not production readiness. See [morning report](MORNING-REPORT-2026-10-15.md) for test/benchmark evidence and [implementation checkpoint](IMPLEMENTATION.md) for subsequent changes.
