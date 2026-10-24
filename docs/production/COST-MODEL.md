# Cost ledger and comparison contract

Version 0.2. This template is a specification and includes a synthetic arithmetic example, not a provider quote, measured run, or approved budget.

## Units and accounting boundary

Use decimal B/GB/TB/PB for data volume (1 TB = 10^12 B). Use GiB for memory when the resource meter does. Rates must use the same units as usage or record the conversion. For planning only, a normalized month is 30 days = 720 hours; actual invoices and calendar-month SLIs use their actual periods. Keep currencies separate unless a dated exchange rate is declared.

Identify window start/end, run/workload/profile revision, deployment topology, region or facility, currency, pricing date, rates source, reserved-capacity treatment, tax treatment, and operations staffing assumptions. Preserve both measured quantities and assumed rates. Missing rates or labor estimates are unknown, not zero.

A ledger row has: unique resource/charge ID; category; purpose tags; workload allocation key; usage; unit; rate per unit; amount; rate provenance; measured/estimated status; and notes. Charge categories are compute, storage capacity, storage operations, network, software/support, facility/power, and labor. Purpose tags include serving, ingest, transform, analytics, backup, DR, and shared control. Each amount appears in one category once, even when it has multiple purpose tags.

Total window cost is the sum of unique ledger amounts. Workload allocations for each row must sum to 100%; leave an explicit shared/unallocated bucket when attribution is uncertain. Backup/DR cost is a filtered view of these rows, not an additional charge. If a managed-service price bundles storage, operations, or support, exclude those costs from separate rows unless independently billed.

For on-premises resources, allocate hardware purchase less residual value over its declared useful life, plus maintenance, power/cooling, facility/network costs, and labor. Do not add the full purchase price to the same window's amortization. For cloud commitments, report the allocated commitment cost and incremental usage; do not add a hypothetical on-demand price as an incurred cost. Record unused reserved capacity as idle cost.

## Metrics

| Metric | Numerator | Denominator / interpretation |
|---|---|---|
| Ingestion unit cost | Costs allocated to ingestion in the window | Decimal TB of unique source payload durably accepted; retransmission is usage overhead, not another accepted byte |
| Transformation unit cost | Costs allocated to transformation | Decimal TB read into transform stages; report unique source bytes and pass count alongside it, including retries separately |
| Retention unit cost | Storage capacity charges only | Integrated physical byte-hours / (10^12 x 720), yielding normalized TB-months; report protection and copy layout |
| Read/action unit cost | Serving costs allocated separately to reads and actions | Millions of successful logical reads or actions; retries increase cost but not success count |
| Idle baseline | Fixed commitments plus idle-window usage | Cost/hour and normalized cost/30 days; measure workload-free behavior without suppressing required maintenance |
| Marginal burst cost | Additional cost relative to a specified equal-duration baseline | Extra accepted work; include buffers, startup, retries, and recovery/drain period |
| Recovery cost | Ledger rows incurred during the recovery window, allocated to recovery | Total event cost plus recovered volume/time; distinguish already-budgeted standby from incremental spend |

Publish total platform cost with these ratios. A fully loaded ingestion ratio that includes all platform costs may be useful, but label it separately from ingestion-attributed cost. Do not sum costs derived by multiplying several fully loaded ratios; that would count shared resources repeatedly.

Report raw/logical bytes, compressed unique bytes, derived materializations, and billed physical bytes independently. Managed object storage may bill logical object size while including internal replication in its price; do not multiply that bill by an assumed provider replication factor. Self-managed replicas and erasure-coding overhead belong in actual physical usage. Document compression ratio direction and per-dataset retention.

## Synthetic 24-hour ledger

The following rates are invented arithmetic inputs in a fictional currency, CU. They say nothing about provider prices or economic viability. Assume constant occupancy for the illustrated window.

| Unique charge | Usage | Assumed CU/unit | Amount CU |
|---|---:|---:|---:|
| Operational compute | 48 node-hours | 0.50 | 24.00 |
| Transform compute | 12 node-hours | 0.75 | 9.00 |
| Primary storage | 2 TB x 24/720 = 0.066667 TB-month | 20 | 1.333333 |
| Backup storage | 1 TB x 24/720 = 0.033333 TB-month | 20 | 0.666667 |
| Storage operations | 2 million requests | 0.40/million | 0.80 |
| Network transfer | 0.10 TB | 5/TB | 0.50 |
| Operations labor | 0.50 hours | 40/hour | 20.00 |
| **Total** | | | **56.30** |

Round only displayed results. Backup storage is already included in the 56.30 CU total. Primary plus backup retention is 0.10 TB-month and costs 2 CU, giving 20 CU/TB-month of capacity. This ratio excludes operations, transfer, and labor.

For an illustrative allocation, assign operational compute to serving, transform compute to transformation, storage to retention, and the remaining 21.30 CU to shared/unallocated. If 3 TB passes through transform stages, the attributed transform cost is 9/3 = 3 CU/TB. If those stages represent 1 TB of unique source input over three passes, report both counts. A separate fully loaded total/unique-input figure is 56.30 CU/TB; it must not be added to the transform ratio.

## Scenario comparison and evidence

Compare small, bursty, and sustained profiles at identical retained data, offered workload, durability, and freshness targets. Vary compression, retention, materialization fraction, shuffle/read amplification, storage operations per TB, utilization, cold starts, committed versus elastic compute, transfer locality, and operator hours. Identify bottlenecks and rejected work; a cheaper run that drops work or misses its declared target is not a qualifying alternative.

Required output: ledger rows, allocation mapping, rates and provenance, byte counters, integrated storage occupancy, throughput/latency/correctness results, and sensitivity table. A script should verify unique charge IDs, compatible units, row arithmetic, allocation totals, and denominator availability. The synthetic example is a fixture for PERF-01, not an implemented cost collector.

Dollar ceilings and approved rates remain unresolved product decisions (Q4 in [DELIVERY.md](DELIVERY.md#decision-register)). Parameterized instrumentation and comparisons can proceed while they are open; economic certification cannot.
