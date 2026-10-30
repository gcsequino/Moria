# Production architecture and authority boundaries

Version 0.2, October 24, 2026. Target direction is accepted; detailed mechanisms below are proposed and require proof. This document extends first-slice deployment decisions without superseding C01-C08, S01-S08, or R01-R06. [Acceptance traceability](ACCEPTANCE.md) identifies proof owners and delivery gates; documentation reconciliation is not runtime certification.

## Components and state ownership

| Component | Owns | May access | Must not do |
|---|---|---|---|
| Management/control API | declarations, release plans, orchestration requests | authorized control schemas and scoped artifact operations | proxy bulk dataset bytes or execute user code |
| Object-serving API | object queries/actions and current authorization | serving/security schemas with constrained roles | trust caller policy/identity or use stale authority |
| Scheduler/coordinator | firing keys, executions, leases, attempt state | execution/catalog APIs and constrained schemas | perform bulk transforms in its event loop |
| Publication authority | dataset heads, receipts, provenance/checkpoints | atomic catalog transaction boundary | allow independent second-catalog publication |
| Trusted engine adapters | engine-specific planning/staging | scoped snapshots/staging and execution identity | publish without authority or pass broad credentials to user code |
| Isolated user runtime | bounded code execution and private scratch | declared mediated inputs/outputs/destinations | access raw control DB, warehouse root, or host sockets |
| Static app host | immutable validated release artifacts | artifact store | acquire backend permission by hosting an app |
| Storage/database operators | durable state, keys, recovery lifecycle | installation-specific infrastructure | couple data lifetime to API process termination |

One repository may supply several binaries/entry points. Initial logical schemas may share one PostgreSQL cluster; roles and connection budgets are distinct. Physical database separation must not break atomic commits that currently span security, objects, audit, outbox, and receipts.

Do not stop or initialize an external PostgreSQL server from an API lifecycle hook. The development launcher may coordinate an explicitly development-only database.

## Distributed indexing implementation boundary

[INDEXING-PROTOCOL.md](INDEXING-PROTOCOL.md) defines durable intent, partition
claims and destination fencing, atomic generation activation, snapshot/replay
handoff, search visibility, current authorization and recovery gates for Rust
indexing services. It introduces no runtime implementation or capacity claim.
The management API remains a control plane; no temporary Node bulk-processing
or distributed-worker implementation is required. Processing parallelism does
not supersede the environment transaction boundary or P03 proof obligation.

## Transaction and publication boundaries

Actions preserve C06: serializable authorization/condition checks plus all edits, audit, outbox, and receipt in one transaction. No provider call or arbitrary code runs inside it. Cross-environment edits remain rejected.

Dataset publication preserves C04: durable staged files precede a single authority commit of promised heads, provenance, and checkpoint. Multi-input consumers receive a pinned snapshot set. Independent external table loads do not imply an atomic cross-table read.

A transactional outbox bridges external effects. External delivery is independently retried/reconciled, not falsely rolled back with an already committed action.

Keep the existing environment revision lock until a replacement ordering and visibility protocol is proven. Independent API replicas do not remove contention. M7 compares database alternatives only after workload evidence.

## Cells and identities

A cell is a fault/capacity boundary, not an implicit global transaction boundary. Initially place a whole environment in one cell. Routing and migration are authenticated and fenced; a move must not produce concurrent authoritative writers.

Installation-wide UID uniqueness, canonical-key idempotence, and permanent reservations survive cells (C02/D01). Provisionally keep one durable logical identity registry with partitioned ownership by identity domain. Route resolutions to the authoritative domain owner; random UUID generation alone cannot satisfy the contract. Reservation batching may optimize allocation only if concurrent resolution of the same canonical key returns the same UID and uniqueness is enforced. Domain transfer fences the previous owner and preserves mappings, aliases, and unused reservations. Ordinary reads need no global allocation lookup. Alternative mechanisms require the same proofs. Restored or moved environments preserve identity lineage and cannot reuse reservations.

An environment remains wholly within one cell initially. Local policy authority is valid only while ownership/fencing and current security state can be established. Global membership, attribute, session, and marking changes must reach every affected environment under S01/S06; partitioned cells cannot continue from indefinitely cached authority. The routing/epoch protocol must specify bounded validity and fail-closed behavior before M7 can claim isolated availability. A global directory need not be consulted on every healthy request, but eliminating that lookup cannot weaken revocation. Cross-cell transactions and cross-installation identity federation remain unsupported. Moving an environment inside an installation preserves resource IDs and object identities; import into a different installation still requires explicit binding and contract-package regeneration.

## Storage and worker security

The existing S07 prohibition on raw user-worker DB/blob access remains. Direct data-plane access is limited to trusted platform engine components authorized for whole inputs. Short-lived credentials alone are not proof of immediate revocation or row-level security.

Untrusted code receives mediated input/output streams or isolated, materialized inputs and staging outputs inside a supervised boundary. Dataset authorization, conservative output markings, destination restrictions, and publication reauthorization remain required. Spark UDFs sharing a process or credential environment with a privileged engine cannot be labeled sandboxed. The first Spark adapter runs trusted declared transforms; arbitrary UDF support is gated on a validated isolation design.

Restricted views remain terminal and cannot be transform inputs. Analytics and object reads enforce current policies before releasing results; authority unavailability fails closed. Long-lived output streams recheck authority per emitted batch. Any adapter unable to preserve the contract is unsupported rather than silently using cached credentials.

The catalog rejects unknown locations, unauthorized tables, path escapes, mismatched identities/schema/parent revisions, and stale fences. Storage adapters must validate endpoint, multipart/range I/O, read-after-write requirements, encryption, and failure behavior; S3 compatibility is tested, not assumed.

## Compatibility and migration

Maintain a versioned matrix for protocol/schema revision, API/SDK, runtime, PostgreSQL, Iceberg format/features, readers/writers, storage, Kubernetes, and identity provider. Pin exact versions/digests only when interoperability is tested.

Use expand/migrate/contract for compatible schema changes. Block incompatible writers and unsupported reader features. Code rollback does not restore older security epochs or remove markings. Deploying an app never grants access.

For the current installation: capture a verified backup; rehearse import/restore with dispatch disabled; compare IDs/grants/effective-object hashes; fence old writers; switch routing; verify; keep a fenced rollback target. Do not run old and restored copies as independent writers.

## Existing contract disposition

| Existing document/decision | Disposition |
|---|---|
| core-contracts.md C01-C08; security-contracts.md S01-S08 | Preserve unless separately reviewed; new runtime must pass the existing acceptance matrix |
| architecture.md A01 / review-decisions.md D11 | TypeScript remains usable; process extraction replaces shared lifecycle, not a mandatory language rewrite |
| A02 / D02 | PostgreSQL remains initial authority; no universal single-cluster scale guarantee |
| A03 | Iceberg retained; object storage/catalog authority replaces local-path deployment assumption |
| A06 / D12 | Compose remains development/small option; Kubernetes/HA becomes production target; original trusted-organization security claim is not broadened automatically |
| A07 / OPS-03 | Expand 1m/10m gates with WORKLOADS.md; old measurements remain historical evidence |
| runtime-contracts.md worker protocol | Versioned execution adapters extend it; arbitrary code remains isolated and cannot publish independently |
| implementation-plan.md phases | Historical first-slice sequence; GitHub M0-M9 is the long-term delivery sequence |

First-slice deployment decisions are reconciled in architecture.md and review-decisions.md. Later semantic changes must update affected contracts and acceptance cases together; provisional mechanism choices do not authorize such changes.

## Authorization timing and engine admission

Preserve S01/S06: native permission changes take effect at security-epoch commit; external identity-provider changes take effect at synchronization commit, with the initial at-most-60-second polling/staleness bound and fail-closed behavior after that bound. Emergency platform disable applies at the next protected operation. These distinct events are not universal instantaneous revocation.

Bounded responses recheck the current epoch after evaluation and immediately before release; a race requires retry or denial. An operation that linearized before revocation may finish delivery. Actions serialize security checks with commit. Workers reauthorize data reads, output commits, tool calls, and external dispatch; each emitted stream batch checks current authority. Cancellation is best effort; release/publication denial is authoritative.

Trusted engine direct reads require whole-Dataset authorization; RestrictedView access never supplies that grant. Direct I/O must preserve the bounded-read authorization boundary and protect cached results, spill files, and downloads. Credentials scope access but their expiry cannot substitute for revocation checks. Until M2 proves this protocol against S06, mediated reads remain the supported boundary. Trusting an engine includes controlling plugins, executable plans, UDFs, credentials, and network destinations. User code sharing privileged engine credentials is outside the supported isolation boundary.

## Pinned reads and analytical projections

C05 cursors pin an environment revision and compatible generation map, with query/contract/identity/security context. Their 15-minute lifetime starts at page one and never extends. Replicas and cell migration must retain and validate that state without a transaction held between pages. Expiry returns `CURSOR_EXPIRED`; changed security epochs return `CURSOR_INVALIDATED`. Current and pinned canonical row policies both apply; currently deleted objects remain hidden. Security tightening may reduce old-page results. Never substitute the latest revision or silently restart pagination.

An action commit token cannot advance an existing cursor. New operational reads honor that token or, for a future asynchronous index, return `CONSISTENCY_TIMEOUT` after the requested wait. Snapshot-pinned Iceberg analytics are a separate capability with explicit freshness; they do not automatically provide operational pagination or action read-your-writes guarantees. Engines must declare their supported consistency capabilities.
