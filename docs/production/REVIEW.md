# M0 contract review

Revision 0.2, October 24, 2026. Related: [M0](https://github.com/gcsequino/Moria/issues/2).

## Scope and evidence boundary

This review makes the production contract implementable and traceable. It is not a capacity, availability, security-isolation, or recovery certification. The published baseline's 87 passing tests remain historical runtime evidence; this review does not extend their coverage to the production design.

Version 0.1 is retained as historical workload input. Current workload details are in [WORKLOADS.md](WORKLOADS.md) and workloads.v0.2.json. Architecture/security obligations are in [ARCHITECTURE.md](ARCHITECTURE.md) and [ACCEPTANCE.md](ACCEPTANCE.md). Recovery, economics, and decisions are in [DELIVERY.md](DELIVERY.md).

## Findings addressed

| Finding in v0.1 | Required resolution |
|---|---|
| Global machine-readable defaults did not identify enterprise-only background traffic | Explicit profile scope, workload mix and per-tier background definitions |
| Error rate alone could hide requests rejected or never sent | Separate offered, admitted, completed, failed, rejected and missed-arrival accounting |
| Open-loop arrival and user think-time simulation could be conflated | Define which measurements belong to independent traffic scenarios |
| Four-hour DR could be read as a universal petabyte-copy promise | Name the recovery mode, dependency set and measured storage/network requirements |
| Cost categories could double-count replicas, backups or transfer | Non-overlapping ledger entries and explicit units/allocation assumptions |
| First-slice deployment decisions could conflict with long-term direction | Reconcile historical defaults without silently changing behavioral contracts |
| Trusted engine access could be mistaken for untrusted worker permission | Explicit trust boundary and proof gates before arbitrary-code execution |
| Existing acceptance IDs lacked clear owners in the new milestone plan | Traceable contract-to-proof matrix with no implied implementation completion |

These are contract corrections, not evidence that the corresponding production mechanisms already exist.

## What may proceed

- [M1-01](https://github.com/gcsequino/Moria/issues/13): external configuration/database lifecycle and migration ownership can proceed independently of the final throughput tier.
- [M1-02](https://github.com/gcsequino/Moria/issues/14): process/ownership design can proceed; implementation depends on M1-01.
- [PERF-01](https://github.com/gcsequino/Moria/issues/20): implement a manifest-driven generator and correctness oracles. Document validation is not completion of this issue.
- M2 storage/catalog prototypes can be designed against preserved C04/S07 contracts. Engine compatibility, isolation, and publication still require their milestone proofs.

Do not provision large benchmark capacity, set paid-resource budgets, certify an SLA, or choose a distributed database based solely on provisional numbers.

## Product decisions still open

| Decision | Current disposition | Effect while unresolved |
|---|---|---|
| Transaction rate/mix | 2,000 steady / 4,000 busy-hour / 10,000 15-minute peak API requests/s remains a synthetic proposal | Usable as a calibration scenario; not an approved production commitment |
| Ingestion priority | 1 PB per 30 days and 1 PB/day remain distinct scenarios | No single capacity/retention bill is represented as the chosen target |
| Availability and recovery | Proposed objectives need failure-model and recovery-mode agreement | No production availability/DR claim |
| Economics | No currency/region/hardware/staffing or dollar ceilings selected | Report usage and sensitivity; economic viability cannot be certified |
| Source publication | Complete in baseline 702daeb / issue #12 | No longer blocks M1 or harness implementation |

The broader architecture direction and large-bank ServiceNow workload class are accepted. Numeric profiles do not assert measured ServiceNow equivalence. Silence or elapsed time does not approve a proposed target.

## Completion rule

M0 can close when its reviewed, versioned contract is merged; required workload/service decisions have an explicit disposition; benchmark specification and cost/failure accounting are usable; and M1-M2 work is prioritized with dependencies and effort bands.

A decision may explicitly adopt a provisional engineering assumption while deferring a production SLA, but that disposition must be recorded. Runtime proof gates belonging to M1-M9 stay with those milestones and must not be claimed by closing M0.

## Verification record

Record the exact validator command, positive/negative checks, document-link checks, and reviewed commit in the pull request. Retain the v0.1 workload file unchanged. No runtime benchmark or live interruption is required or implied by this documentation review.
