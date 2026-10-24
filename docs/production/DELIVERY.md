# Delivery, verification, costs, and decisions

Version 0.1; planning specification, not executed results.

## Failure matrix

| Stimulus | Required invariant / observation | First milestone |
|---|---|---|
| API death/lost reply after action commit | replay returns receipt under current authorization; no duplicate mutation | M1 |
| Coordinator death / duplicate schedule delivery | unique logical firing; durable work rediscovered | M1/M3 |
| Worker lease expiry or replacement | stale worker cannot publish; cancellation bounded and observable | M2/M3 |
| Partial upload / storage timeout / disk full | incomplete data remains invisible; no advanced checkpoint | M2 |
| Competing catalog heads / metadata mismatch | all-or-none output group, explicit conflict | M2 |
| Compaction/expiration during job/cursor/backup | pinned content survives; no dangling recoverable references | M2/M4 |
| Authority outage / revocation mid-query | protected output fails closed; current epoch enforced | M1/M5 |
| DB primary/failure-domain loss | declared durability/RTO measured; no split brain | M4 |
| Full restore / outbox replay | identity/grants preserved; dispatch disabled until reconciliation | M4 |
| Backfill plus ingestion burst | serving reservation protected; admission/backlog evidence retained | M3/M6 |
| Hostile runtime attempts DB/blob/host/egress access | enforced denial, safe audit; no claim based on configuration alone | M3 |
| Cell partition/migration | unrelated cells remain available; one authoritative owner | M7 |

Production interruption tests are not part of this planning work. Run deterministic fault injection against isolated environments and publish exact evidence.

## Cost contract

For a measurement window, total cost = allocated compute + physical storage + storage operations + network/egress + licenses/support + backup/DR + allocated operator labor. Separate measured usage from rate-card assumptions and document region/currency/date. Use an auditable ledger, not a guessed provider quote.

Report cost/TB accepted, cost/TB transformed (with pass count), cost/TB-month physically retained, cost/million successful reads/actions, baseline idle cost, startup latency, and recovery cost. Denominators distinguish logical input, compressed data, derived copies, replicas, and backups. Report both actual blended costs and marginal scaling costs.

On premises include amortized hardware, power, spare capacity, networking, and labor. Cloud autoscaling does not remove storage/HA baseline costs; on-premises scale-down does not undo hardware purchase. Model burst buffer and compute/storage locality.

Dollar ceilings remain a product-owner decision. Until they are set, runs report economics but cannot certify economic viability. Compare feasible configurations at the same workload/SLO before choosing the cheapest qualifying one.

## Prioritized M1-M2 backlog

Effort bands are relative engineering complexity, not calendar promises: S = bounded local change; M = several integration points; L = protocol/security/migration work; XL = uncertain cross-engine proof. Estimate again after spikes.

| ID | Priority | Work | Depends on | Size |
|---|---|---|---|---|
| SRC-01 | P0 | Audit and publish existing source baseline without runtime data/secrets, preserving history and human identity | verified Git identity, scoped source review | L |
| M1-01 | P0 | External database/configuration and migration ownership; development-only embedded launcher | SRC-01 | M |
| M1-02 | P0 | Separate API, scheduler, coordinator/worker and static-host entry points | M1-01 | L |
| M1-03 | P0 | Multi-instance lifecycle, bounded pools, readiness/draining, portable images | M1-02 | M |
| M1-04 | P0 | Isolated current-data migration/restore rehearsal and rollback | M1-01; final verification after M1-03 | L |
| M2-01 | P0 | Logical storage locations, streaming adapter, scoped credentials | M1-01, authority design | L |
| M2-02 | P0 | Catalog/Spark publication interoperability spike before full integration | M2-01, C04 invariants | XL |
| M2-03 | P1 | Production catalog conformance, pins, compaction/GC, recovery retention | M2-02, M1-04 | L |
| PERF-01 | P0 | Open-loop benchmark manifest/oracle/result harness | M0 cards; incremental operation coverage | L |

M1 code anchors: apps/local/src/main.ts owns embedded DB and coupled shutdown; services/execution/src/jobs.ts owns a fixed-handler queue with global per-kind active-job uniqueness; services/execution/src/snow-jobs.ts wires fixed workloads. Do not claim general multi-environment scheduling until uniqueness/ownership semantics are redesigned and tested. M2 anchors: workers/iceberg/stage.ts and stream.ts, services/catalog, and the existing indexing/publication path.

## Decision register

| ID | Decision / provisional position | Owner role | Needed by |
|---|---|---|---|
| Q1 | Accept or recalibrate T-S/T-B/T-E offered loads and mixes; no measured ServiceNow equivalent available | product owner with performance lead | M0 review |
| Q2 | Select D-M/D-L ingestion tier, burst duration, retention and freshness | product owner | M0 review |
| Q3 | Confirm HA availability/RPO/RTO and failure model | product owner with operations lead | M0/M4 design |
| Q4 | Set dollar cost ceilings, deployment region/hardware and staffing | product owner | economic certification |
| Q5 | Verify source author identity and publication baseline | repository owner | SRC-01 |
| Q6 | Validate trusted-engine storage authority and untrusted code boundary | security/architecture owner | M2/M3 |
| Q7 | Choose UID allocation/routing protocol across cells | architecture owner | M7 implementation |
| Q8 | Pin catalog/engine/version compatibility from proof | data-platform owner | M2 exit |

Roles indicate responsibility, not assigned staff. Unresolved product decisions remain open; elapsed time does not approve proposed targets.

## M0 completion disposition

Draft workload cards, numerical calibration inputs, harness specification, failure matrix, component/state ownership, cost formula, and prioritized backlog are now available. The generator is not implemented; no new capacity evidence exists. Contract reconciliation and numeric/budget approval are pending. The package is included in the source baseline and also recorded in GitHub M0.

Keep M0 open. With the source baseline published, the next work is PERF-01 and M1-01, subject to the outstanding workload-contract decisions.

## GitHub implementation backlog

- [SRC-01: Review and publish the existing source baseline](https://github.com/gcsequino/Moria/issues/12)
- [M1-01: Externalize PostgreSQL and configuration lifecycle](https://github.com/gcsequino/Moria/issues/13)
- [M1-02: Extract independent API, scheduler, worker, and static-host entry points](https://github.com/gcsequino/Moria/issues/14)
- [M1-03: Package portable replicas with bounded lifecycle and resources](https://github.com/gcsequino/Moria/issues/15)
- [M1-04: Rehearse current-installation migration and rollback in isolation](https://github.com/gcsequino/Moria/issues/16)
- [M2-01: Introduce logical storage locations and a scoped streaming adapter](https://github.com/gcsequino/Moria/issues/17)
- [M2-02: Prove Spark interoperability with atomic governed publication](https://github.com/gcsequino/Moria/issues/18)
- [M2-03: Implement catalog conformance and retention-safe maintenance](https://github.com/gcsequino/Moria/issues/19)
- [PERF-01: Implement the open-loop workload manifest and correctness harness](https://github.com/gcsequino/Moria/issues/20)
