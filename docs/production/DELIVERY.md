# Delivery, verification, costs, and decisions

Version 0.2; planning specification, not executed results.

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

## Recovery and availability measurement contract

The numerical candidates in [WORKLOADS.md](WORKLOADS.md#provisional-service-targets) are engineering calibration inputs. They are not approved service commitments. Record a failure scenario, protected state, topology, clock origin, and success oracle for every recovery claim.

| Scenario | Required scope and evidence |
|---|---|
| Process/worker loss | Durable accepted work is rediscovered; expired attempts are fenced. Measure completion delay separately from API availability. |
| One HA failure domain lost | Candidate acknowledged-action RPO 0 applies only where the surviving synchronous database copies and referenced storage satisfy the declared durability boundary. Losing quorum must stop acknowledgments; availability cannot override durability. Do not extend this claim to all replicas lost, operator deletion, or regional disaster. |
| Database failover | Candidate RTO <=5 minutes starts at the injected failure, including detection and promotion; it ends when routing, current authorization, and correct read/action probes meet the declared service target. A healthy database process alone is insufficient. |
| Recover using surviving durable storage | Candidate DR RPO <=15 minutes and RTO <=4 hours require a retained, mutually compatible database recovery point, referenced files, keys, identities, and artifacts. Confirm capacity and availability of the surviving store; do not presume it survived every scenario. |
| Full storage copy/rebuild after storage-domain loss | Separate recovery tier with measured byte volume, network capacity, request limits, indexing time, and cost. It does not inherit the four-hour candidate. Define degraded-service scope and a separate time to full processing capacity. |
| Logical corruption or compromised credentials | Select a known-good point, restore identity/security safely, rotate compromised authority as appropriate, reconcile external effects, and retain an auditable loss window. Backup retention alone does not establish a recoverable point. |

RPO is the maximum interval of acknowledged protected updates missing from the consistent recovered state, measured against the fault time; also report missing action IDs/counts and source offsets. For slow-detected corruption, report detection delay and actual lost history separately. RTO begins at failure (not when an operator begins restoration) and includes detection, provisioning, restore, verification, fencing, and traffic enablement. Report time to minimum operational service and time to full analytical/processing capacity separately. Do not claim restored processing capacity when only APIs have recovered.

A full copy of 1 decimal PB in four hours needs at least 69.44 GB/s of payload throughput; 30 PB needs 2.083 TB/s, before protocol, retries, validation, and index reconstruction. Consequently the four-hour candidate must be evaluated against a declared retained-store recovery design or a separately provisioned and measured replica, not extrapolated from a small restore. These are arithmetic lower bounds, not capacity measurements.

Proposed HA availability measurement: fixed-cadence, authenticated, authorized end-to-end read/action probes from declared client vantage points. A probe interval is good only if its required operations finish correctly within their stated deadlines; unexpected failures, dependency outages, admission rejection at supported load, and planned maintenance count as bad. Publish probe coverage, operation-specific results, end-user request success/latency, and outage intervals; neither empty traffic nor successful health endpoints demonstrate availability. The proposed monthly 99.95% permits 21.6 minutes of bad time in a 30-day month. Approval must set probe cadence, vantage points, month boundaries, and treatment of missing evidence before certification. Keep the load-test unexpected-error ratio separate from this time-based SLI.

### Consistent recovery set and retention

Every recovery manifest must identify the database backup/recovery-log range, catalog heads and immutable referenced files, app/runtime artifacts, schema/protocol versions, identity configuration, key references, and restore instructions. Include source offsets and external-delivery receipts where they define the resumed work. Never embed secrets or key material in the manifest.

Garbage collection must protect the union of references reachable from every supported database recovery point, plus live job/cursor pins and unexpired staging leases. A backup of today's heads alone does not protect a historical point-in-time restore. Retention cannot be shortened until the supported recovery window and outstanding pins have expired; if reference protection cannot be established, stop destructive maintenance and alert. Storage versioning or replication alone is not proof of recoverability, particularly if deletes or corruption propagate.

Restore to an isolated target with dispatch disabled. Verify object identities, grants, overlays, receipts, catalog-to-file references, and representative content checksums. Reconcile already-completed external effects before dispatch; fence the old deployment before enabling authoritative writes. Retain evidence of the chosen point, losses, every timed phase, reconciliation decisions, and successful post-restore workload probes. Rehearse loss of a storage domain as well as database loss before claiming that scenario is supported.

## Cost contract

Use the [cost ledger and synthetic example](COST-MODEL.md). Count each billed or amortized resource once, then allocate its cost across workload owners. Backup/DR is a purpose tag, not an extra subtotal added on top of already counted backup storage, standby compute, and transfer. Record currency, rate provenance/date, measurement window, usage units, and allocation basis.

Report workload-attributed cost/TB accepted, cost/TB transformed, cost/TB-month physically retained, and cost/million successful reads/actions alongside total platform cost, idle baseline, burst startup time, and recovery cost. Never add these unit-cost ratios together. Logical input, compressed bytes, copies, replicas, and backups have separate denominators. Report actual blended and incremental costs; neither substitutes for the other.

Dollar ceilings remain a product-owner decision. Until they are set, runs can compare costs but cannot certify economic viability. Compare configurations at equal workload, correctness, durability, and service objectives; include on-premises amortization, power, spare capacity, networking, and operations labor. Scale-down does not undo purchased hardware or HA/storage commitments.

## Prioritized M1-M2 backlog

Effort bands are relative engineering complexity, not calendar promises: S = bounded local change; M = several integration points; L = protocol/security/migration work; XL = uncertain cross-engine proof. Estimate again after spikes.

| ID | Priority | Work | Depends on | Size |
|---|---|---|---|---|
| SRC-01 | Complete | Published reviewed source baseline; prior local history retained separately | [closed #12](https://github.com/gcsequino/Moria/issues/12) | Done |
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
| Q5 | Resolved: source baseline published with configured human identity; prior local history retained separately | repository owner | SRC-01 complete (#12) |
| Q6 | Validate trusted-engine storage authority and untrusted code boundary | security/architecture owner | M2/M3 |
| Q7 | Choose UID allocation/routing protocol across cells | architecture owner | M7 implementation |
| Q8 | Pin catalog/engine/version compatibility from proof | data-platform owner | M2 exit |

Roles indicate responsibility, not assigned staff. Unresolved product decisions remain open; elapsed time does not approve proposed targets.

## M0 completion disposition

Draft workload cards, numerical calibration inputs, harness specification, failure matrix, component/state ownership, non-overlapping cost ledger, and prioritized backlog are available. The generator is not implemented; no new capacity evidence exists. Numeric/budget approval remains open; see the package review disposition for contract reconciliation. The v0.1 package was published in the source baseline. This revision strengthens the design and verification contracts without claiming that later milestone runtime proofs have been executed.

Keep M0 open until assumptions have explicit dispositions and linked implementation issues have reviewable acceptance criteria. No unstated target becomes approved through implementation or elapsed time. M0 design completion requires disposition of these decisions and ready implementation gates; it does not require executing the M2/M3/M4 proofs assigned to later milestones.

### Work that can proceed and decision gates

| Work | Ready under explicit engineering assumptions | Actual gate |
|---|---|---|
| PERF-01 harness | Implement parameterized arrivals, accounting, correctness oracles, fault manifests, and cost ledger; use provisional cards as synthetic fixtures | Q1/Q2 approval required to certify a product tier, not to build the harness |
| M1-01 through M1-04 | Externalize database/configuration, separate lifecycles, package replicas, and rehearse isolated migration against preserved contracts | Deployment evidence and safety checks before migration/cutover; Q3 before an HA promise |
| M2-01 storage | Prototype logical locations and bounded I/O against isolated fixtures | Q6 authority design before production scoped access; adapter conformance before compatibility claims |
| M2-02/M2-03 catalog | Prototype trusted transforms and publication conflict/failure handling | Q6 security proof and Q8 compatibility matrix before supported engine release; tested retention before destructive maintenance |
| Economic comparison | Populate measured resource usage, explicitly assumed rates, and sensitivity cases | Q4 cost ceilings before economic certification |
| M7 routing | Document candidate protocols and required invariants | Q7 protocol selection and workload evidence before environment sharding |

Prioritize PERF-01 and M1-01 concurrently; start isolated M1-04 baseline recovery capture early, with final migration proof after M1-03. Follow M1 extraction with the M2 publication spike before committing to complete engine integration. Relative sizes estimate complexity only; no staffing, schedule, or spending commitment is implied. Unknown product thresholds block certification claims, not reversible foundation work.

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
