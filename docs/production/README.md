# Production architecture program

Version 0.2. The production workload ladder is the implementation target. Current results and open gates are in the [implementation checkpoint](../IMPLEMENTATION.md#native-integration-and-production-goals--november-5-2026).

System of record: [goal #1](https://github.com/gcsequino/Moria/issues/1), [M0 #2](https://github.com/gcsequino/Moria/issues/2). Issue acceptance gates track progress. These source documents define the proposed contract; evidence must identify its exact revision.

## Read together

- [Workloads and benchmark contract](WORKLOADS.md)
- [Architecture, authority, and compatibility](ARCHITECTURE.md)
- [Distributed indexing protocol for the Rust implementation](INDEXING-PROTOCOL.md)
- [Native runtime component inventory and API contracts](RUST-PLATFORM-PROGRAM.md)
- [Delivery, failures, cost, and decision register](DELIVERY.md)
- [Current machine-readable calibration profiles](workloads.v0.2.json)
- [Historical v0.1 calibration profiles](workloads.v0.1.json)
- [Acceptance traceability](ACCEPTANCE.md)
- [Review findings and decision disposition](REVIEW.md)

## Accepted direction versus assumptions

Accepted direction: portable cloud/on-premises deployments, tens of millions of operational objects, a large-bank ServiceNow-instance class of transactional workload, separately scalable potentially petabyte-scale ingestion/analytics, and economical/bursty/sustained profiles.

The approved request rates and sizes are defined in the [workload contract](WORKLOADS.md). Availability, recovery, processing tiers and cost require their own evidence and decisions.

## Relationship to existing contracts

The October 14 first-slice specification remains the source of existing semantic requirements. This package extends deployment/scale direction; it does not silently change identity, authorization, publication, action, or pinned-read guarantees. See [architecture mapping](ARCHITECTURE.md#existing-contract-disposition).

The first-slice Phase 0 and this roadmap's M0 are separate milestones.

M0 stays open until the reviewed contract is merged, required assumptions have an explicit disposition, and backlog readiness gates are complete. Validation of this document package is distinct from implementing the workload generator in PERF-01 or certifying any production capacity. M1 configuration/lifecycle work can proceed independently of final numeric sizing; see REVIEW.md for the decision boundaries.
