# Production acceptance traceability

Version 0.2, October 24, 2026. Planning obligations, not executed results.

The [existing acceptance matrix](../spec/implementation-plan.md#canonical-acceptance-matrix) defines first-slice cases. This crosswalk retains those cases and adds production stimuli. Its milestone columns specify when evidence is required, not when work was completed. Contract reconciliation can finish in M0 while runtime proof remains in the implementing milestone. An earlier passing fixture is not proof of a new topology or engine.

## Contract-to-production crosswalk

| Obligation | Existing contract / cases | Production extension and required evidence | Gate / accountable role |
|---|---|---|---|
| Resource identity and release compatibility | C01/C07; COMP-01, DEP-01–03 | Concurrent replica plans reject stale activation; grants do not come from deployment; import versus same-installation cell migration preserves the correct identity boundary | M1/M7; platform lead |
| Canonical identity, reservations, overlays | C02/C06; ID-01–03, OBJ-01–02 | Concurrent cells resolving one key obtain one UID; domain transfer and restore retain all reservations; stale owners cannot allocate; source replay preserves overlays/tombstones | M1 migration, M7 allocation; architecture lead |
| Atomic catalog publication | C04; DATA-01–02; R01/R02 | Real engine multi-output commit with one competing head rejects the whole group; kill before durability, before commit, after commit-before-ack; verify every pointer, checkpoint, lineage row, receipt and stale lease | M2/M3; data-platform lead |
| Source completeness and maintenance | C04; DATA-03–04 | Failed full scans never replace good data; replay/deduplication verified per connector; compaction/GC under live job, cursor, and backup pins preserves exact-snapshot reads | M2/M6; data-platform lead |
| Current authority and inherited markings | S01/S02/S06; SEC-01–02, SEC-06 | Revocation races across replicas, workers, streams, caches, and cell partitions; bounded identity-sync delay disclosed; authority outage denies output; lineage changes cannot publish unmarked descendants | M1/M2/M5/M7; security lead |
| Restricted views and property/link policy | S03–S05; SEC-03–05, SEC-07 | Same visible set across operational and analytical access; denied predicates/counts/links leak no hidden values; views remain forbidden transform inputs | M2/M5; security lead |
| User-code isolation / trusted engine I/O | S07/S08; SEC-08; R02 | Real denied DB/blob/host/egress attempts; no engine credential access from arbitrary code; stale grants cannot continue bounded reads or release results; test spill/download paths | M2 trusted adapter, M3 user runtime; security lead |
| Pinned operational reads | C03/C05; READ-01–03, SEC-06–07 | Replica changes, action writes, history cleanup, and cell migration preserve revision/order/15-minute original expiry; no silent restart; both pinned/current policy apply; commit tokens honored or explicit timeout | M1/M5/M7; serving lead |
| Transactional actions and effects | C06; ACT-01–05 | Concurrent replicas, hot-object contention, response loss, timeout and expired receipt tests preserve one effect, serializable pre/post checks, atomic audit/outbox, and independent delivery reconciliation | M1/M5/M7; serving lead |
| Recovery and lifecycle | C08; OPS-01–02 | Clean-host restore includes identity, policies, keys, all recoverable Iceberg references and dedupe records; dispatch remains disabled until reconciliation; old writers fenced | M1 rehearsal, M4 HA; operations lead |
| Workload economics and capacity | OPS-03 | Production workload manifest, mixed loads, open-loop offered rate, tails/errors/backlog, wide/cold data, contention, recovery headroom, and complete cost ledger; retain smaller historical benchmark scope | M0 harness specification, M5/M6/M9 evidence; performance lead |
| Application and model integrations | R03–R05; APP-01, AI-01–04 | Shared authorized API, destination checks, protected traces, deletion/revocation and tool idempotency survive the new topology; defer unsupported capability explicitly | M8; application/security leads |

Roles are responsibilities to assign to implementers, not claims that staffing has been allocated. M0 defines the evidence contract; it does not require every future runtime gate to pass before independent M1 work begins.

## Unresolved mechanism proofs

| ID | Provisional implementation direction | Required discriminator / decision record | Owner / deadline |
|---|---|---|---|
| P01 | One PostgreSQL publication authority behind engine adapters | Demonstrate atomic grouped pointer/head/checkpoint commit using the actual pinned Spark/catalog versions. If an adapter offers only per-table commits, reject that capability rather than weakening C04. | data-platform lead; M2-02 before M2-03 |
| P02 | Trusted declared transforms with mediated reads until direct I/O proof | Identify every storage credential holder and executable-code boundary; prove current authority at bounded reads and result release, including engine caches/spill. No arbitrary UDF admission by naming it trusted. | security lead; M2-01/M2-02, before M3 user-code support |
| P03 | Keep the environment revision lock and SQL history | Measure contention and history retention. Any replacement must prove commit ordering, pinned pages, current-policy reduction, and action-token visibility under crashes/concurrency. | serving lead; M5 measurement, before M7 replacement |
| P04 | Partition identity ownership by domain in one logical registry | Prove canonical-key idempotence, unique UID reservations, owner transfer and restore fencing. UUID randomness or independent cell registries are insufficient. | architecture lead; before M7 migration |
| P05 | Environment-local policy authority with fenced ownership | Define authorization validity during control-plane partitions; prove global security changes obey S01/S06. Explicitly record which failure modes trade availability for fail-closed behavior. | security/architecture leads; before M7 certification |
| P06 | Expand/migrate/contract and coordinated recovery pins | Pin tested API/schema/engine/storage combinations; establish backup retention before GC; recover with keys and dispatch disabled, then reconcile effects before resuming. | operations/data-platform leads; M2/M4 |

These are implementation proof gates. Owners may choose mechanisms within existing contracts without asking for approval of implementation trivia. An inability to meet a contract is a blocker or an explicitly reviewed semantic change, never an implicit relaxation.

## Distributed indexing protocol gates

[INDEXING-PROTOCOL.md](INDEXING-PROTOCOL.md) defines IDX-01 through IDX-12
for the Rust implementation: durable handoff, parallel partitions, crash and
receipt recovery, destination fencing, rebuild/replay continuity, complete-
transaction search visibility, current authorization, idempotent delivery,
bounded verification, retention/restore and mixed-workload capacity. These
are unexecuted acceptance obligations. Language-neutral traces and model
checks cannot close real-engine, transaction or isolation gates. P01–P06 and
the existing numeric-approval/evidence requirements remain in force.

## Evidence and closure discipline

Each executed gate must record source revision, contract/profile revision, exact component versions, topology, dataset/seed, commands, raw output, fault point, observed invariant, limitations, and pass/fail status. Include the verification identity and date. Never record a proposed test as passed, and never use mock-only evidence for transaction, authorization, or runtime isolation claims.

M0 can close only when workload assumptions have an explicit disposition, this reconciliation is reviewed, the benchmark/failure/cost contracts and compatibility policy are specified, and implementing issues contain dependencies and acceptance gates. Future P01–P06 proofs stay open on their implementation milestones. Unapproved numeric targets or budgets cannot be treated as certified business commitments.
