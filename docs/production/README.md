# Production architecture program

Version: 0.2, October 24, 2026. Status: reviewed engineering draft with unresolved product targets; no production capacity or SLA certification.

System of record: [goal #1](https://github.com/gcsequino/Moria/issues/1), [M0 #2](https://github.com/gcsequino/Moria/issues/2). Issue acceptance gates track progress. These source documents define the proposed contract; evidence must identify its exact revision.

## Read together

- [Workloads and benchmark contract](WORKLOADS.md)
- [Architecture, authority, and compatibility](ARCHITECTURE.md)
- [Delivery, failures, cost, and decision register](DELIVERY.md)
- [Current machine-readable calibration profiles](workloads.v0.2.json)
- [Historical v0.1 calibration profiles](workloads.v0.1.json)
- [Acceptance traceability](ACCEPTANCE.md)
- [Review findings and decision disposition](REVIEW.md)

## Accepted direction versus assumptions

Accepted direction: portable cloud/on-premises deployments, tens of millions of operational objects, a large-bank ServiceNow-instance class of transactional workload, separately scalable potentially petabyte-scale ingestion/analytics, and economical/bursty/sustained profiles.

The request rates, mixes, availability, freshness, and recovery numbers below are provisional synthetic engineering targets, not measured ServiceNow capacity, customer commitments, or an assertion that the current implementation can meet them. Product-owner review and representative evidence are needed before certification.

The current local source reports an October 21 checkpoint of 87 tests. Earlier 10-million-object benchmarks do not certify this contract. No new performance, restore, failover, or engine interoperability run was performed for this documentation change.

## Relationship to existing contracts

The October 14 first-slice specification remains the source of existing semantic requirements. This package extends deployment/scale direction; it does not silently change identity, authorization, publication, action, or pinned-read guarantees. See [architecture mapping](ARCHITECTURE.md#existing-contract-disposition).

The old first-slice Phase 0 and this roadmap's M0 are different milestones. Historical statements such as "no implementation yet" in the old review package describe that review date, not today's implementation.

## Source publication status

The October 24 source baseline contains the current implementation and this contract package. Prior local history remains retained separately; historical commit IDs in evidence may refer to that earlier local history. Runtime data, credentials, dependency installations, and build outputs are excluded. See [source baseline](../SOURCE-BASELINE.md) for publication scope and verification.

M0 stays open until the reviewed contract is merged, required assumptions have an explicit disposition, and backlog readiness gates are complete. Validation of this document package is distinct from implementing the workload generator in PERF-01 or certifying any production capacity. M1 configuration/lifecycle work can proceed independently of final numeric sizing; see REVIEW.md for the decision boundaries.
