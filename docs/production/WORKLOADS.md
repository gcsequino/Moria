# Workloads and benchmark contract

Version 0.2. All numerical targets are provisional calibration points, not measured ServiceNow equivalents, approved requirements, or accepted production SLAs. [workloads.v0.2.json](workloads.v0.2.json) is the current machine-readable companion; v0.1 is retained unchanged as history. Neither file is an executable load generator or a complete run manifest.

## Units and scope

Report separately: external API request, logical business action, database transaction attempt/commit, mutated object, background execution, source event, source byte, transformed byte, and physical stored byte. One action can mutate several objects and trigger background work. Retries count as attempts, not additional logical successes. Input volume uses decimal bytes; memory and logical payload sizes use binary bytes.

The transaction, data-processing, and deployment profiles are independent axes. A run names its combination explicitly; T-E does not imply D-L or HA. An environment is initially the transactional isolation and placement unit. Multi-environment distributions must be declared separately from a single large-environment run.

## Transactional calibration ladder

| Card | Object population | Steady offered API requests/s | Busy-hour requests/s | Peak requests/s for 15 min |
|---|---:|---:|---:|---:|
| T-S: economical | 1m | 20 | 50 | 100 |
| T-B: bursty enterprise calibration | 10m | 500 | 1,000 | 2,000 |
| T-E: large-enterprise calibration | 50m, repeat at 100m | 2,000 | 4,000 | 10,000 |

These are synthetic load-generator settings, not evidence of bank traffic. Ramp through intermediate rates to find the capacity boundary. Success at a smaller tier does not certify a larger tier.

The following fixture and external-request mix apply to **all three cards** unless a separately named sensitivity run overrides them. Use 4 KiB mean logical object payload; repeat sensitivity trials at 1 KiB and 16 KiB. Fifty million 4-KiB payloads total 204.8 GB before indexes, history, overhead, replicas, and backups. Seed 20% of objects with overlays, 2 retained source generations, and 10 historical edits per edited object. Exercise relation fan-out 0, 5, and 50 as distinct trials. A high-fan-out stress run must supply its cap before execution.

External request mix: 35% detail reads; 30% pages (50 rows, projected response cap 256 KiB); 15% filtered/sorted lists; 10% single-object actions; 5% multi-object actions (mean 5, maximum 100 objects); 5% bounded aggregates/relationship reads. Each logical business action maps to one action request. Steady offered action rates are therefore 3/s, 75/s, and 300/s for T-S, T-B, and T-E; retries/internal work are additional. Pagination follow-up requests count as offered API requests, not free work outside the mix. A run must specify query selectivity, aggregate bounds, and the aggregate/relationship split.

Use two identity-mix trials (80% human/20% integration, then 20%/80%). These labels describe principals and policy, not load-generator pacing. Include three region attributes, at least two markings, property restrictions, and current permission changes. Apply production authentication/session checks. Vary visible fractions across 1%, 10%, and 100% in separate trials; define the principal population and policy distribution in the run manifest.

Distribute 80% of normal traffic across the full population and 20% across a 1% hot set. Add a separate contention trial in which 1% of action arrivals target the same 100 objects. Report condition conflicts and serialization retries separately from infrastructure errors.

### Background execution

Background work is additional to the external rate. The provisional steady execution rates are T-S 0.5/s, T-B 12.5/s, and T-E 50/s. These synthetic values scale with steady external load; they are not inferred ServiceNow behavior. Each execution proposes two object updates and one simulated outbox delivery. Busy-hour doubles the execution rate; peak multiplies it by that card's peak/steady external ratio. Warmup, recovery, and soak return to the steady rate.

The hourly bulk update sizes are respectively 1,000, 25,000, and 100,000 objects. Schedule the first update at minute 15 of each measured trial, then every 60 minutes, so short trials include the event. Record actual mutations, internal requests, completion delay, and backlog rather than counting only accepted jobs. Use deterministic local writeback fixtures; never contact production destinations.

## Offered-load accounting

Use an open-loop rate schedule with recorded scheduled timestamps, deterministic seed, explicit arrival distribution, bounded outstanding work, and timeouts. Human principal labels do not add think time to this schedule. A closed-loop session/think-time test is a separate named experiment and cannot substitute for offered-rate capacity evidence.

For every measurement window, reconcile scheduled logical arrivals into terminal outcomes or outstanding work. Missed generator arrivals remain in the offered denominator. After a declared drain deadline, unresolved requests are timeout/incomplete outcomes. Retries belong to the original logical arrival and are also counted in attempt metrics. Do not inject retries as replacements for new scheduled arrivals.

Report both all-arrival and valid authorized arrival denominators. Expected negative probes (denied/invalid requests and intentionally failing conditions) have a separately declared schedule and expected outcomes; they are additional to the 100% primary request mix. Their rate and expected-result checks must be fixed before a run. A successful negative probe is not a successful business operation. Unplanned conflicts in ordinary valid traffic are unsuccessful business outcomes even if the API correctly rejects them.

The provisional valid-arrival unsuccessful fraction is <=0.1% at a claimed supported rate. Its numerator includes admission rejection (including deliberate 429), unexpected errors, timeouts, unresolved work, and business operations that did not complete successfully by their deadline. Report these categories individually; background workflow completion is measured separately. Unexpected server/transport errors also have a separate <=0.1% attempt-rate target, including retry attempts. These two denominators must never be substituted for each other.

Latency runs from scheduled arrival to complete response and includes generator delay and server queueing. Also report generator dispatch delay, database commit latency, application time, and asynchronous workflow completion delay. Publish successful-operation percentiles alongside outcome fractions and timeout counts; percentiles alone cannot establish success. Enqueue acknowledgment is not business-workflow completion.

## Provisional service targets

| Dimension | Proposed engineering target |
|---|---|
| Indexed detail/page/filtered-list queries | p95 <=500 ms; p99 <=2 s, per operation |
| Single/multi-object actions within supported limits | p95 <=1 s; p99 <=3 s, per operation |
| Valid authorized arrivals not completed successfully | <=0.1% of logical arrivals |
| Unexpected server/transport errors | <=0.1% of attempts, including retries |
| Safety | zero observed invariant violations; any violation fails the run |
| HA operational API availability | candidate 99.95% monthly; denominator/exclusions pending review |
| Small deployment | no HA promise; declared maintenance/cold-start behavior |
| HA acknowledged action loss after one failure-domain loss | candidate RPO 0 within the declared synchronous durability boundary |
| HA database failover | candidate RTO <=5 min |
| Disaster recovery | candidate RPO <=15 min, RTO <=4 h using surviving compatible durable storage; includes metadata and keys |
| Burst recovery | oldest operational backlog returns within its declared baseline bound <=30 min after peak |
| Scheduled/bulk freshness | completion deadline per pipeline, pending workload selection |
| Continuous freshness | candidate p95 <=60 s, p99 <=5 min source-to-published result |

Aggregate/relationship deadlines, negative-probe rate, baseline backlog bound, and cost ceilings remain unset. A trial may measure them, but cannot claim to pass an undefined gate. Cold-start trials publish startup latency and do not inherit warm targets. No global sub-second streaming SLA is assumed. RPO 0 excludes simultaneous loss of all synchronous copies. The 4-hour DR target does not cover copying or rebuilding a petabyte warehouse; that scenario requires a separate measured recovery tier. RTO starts at fault time and ends after correct service probes pass, as specified in DELIVERY.md. Monthly availability requires separate operational evidence and cannot be certified by a short benchmark.

## Independent data-processing ladder

D-S: 100 GB/day input. D-M: 1 PB per 30 days. D-L: 1 PB/day. Each assumes 30-day input retention. D-L is a planning scenario; the user's ingestion rate is not yet selected. Separate raw-input bytes from compressed bytes and derived materializations.

At D-L average payload bandwidth is about 11.57 GB/s (92.59 Gbit/s), before overhead, replication, reads, shuffle, and retries. For a 4x 15-minute burst, declare buffer capacity, source replay behavior, and drain capacity. Equal-to-average processing capacity cannot clear backlog. The same burst experiment applies independently to each data card.

Exercise append, keyed merge, schema evolution, full refresh, incremental indexing, wide/skewed joins, grouped aggregation, compaction, snapshot planning, and expiration. Compare 16 MiB, 256 MiB, and 1 GiB file layouts at equivalent logical volume. Record file counts and planning cost; these experimental sizes are not universal optima.

Analytical trials start at 5, 20, and 50 concurrent queries. Separate partition-pruned aggregates, wide scans, and joins. Declare queries, deadlines, scanned-byte limits, dataset distributions, and transformation expansion/shuffle factors before running. Combined certification runs transformations, queries, compaction, and serving concurrently. Dataset records are not ontology objects: every pipeline declares materialization fraction and incremental indexing rate.

## Reproducible harness and acceptance

1. Use isolated infrastructure. Pin source revision, images/dependencies, schema/policy, engine/storage versions, seed, topology/failure domains, quotas, and failure script. Record generation commands and exit status. Fill every required run-specific input in the JSON companion; null means unresolved, not unlimited or zero.
2. Seed population/history separately from measurement. Verify counts, identity uniqueness, source/overlay checksums, and authorization fixtures.
3. Record generator saturation, missed arrivals, outstanding work, and coordinated-omission exposure. A generator-limited run is invalid for capacity certification, even if achieved latency appears good.
4. Run a 10-minute warmup, 30-minute steady window, 15-minute peak, and at least 30-minute recovery. Repeat independently three times. Run separate 60-minute busy-hour, 24-hour soak, cold-cache, and wider-than-memory trials. Certifying petabyte tiers requires sufficient actual scale/duration beyond this minimum.
5. Publish per-operation p50/p95/p99, logical outcomes, attempts/retries, achieved/offered rates, concurrency, queue age, resource saturation, network/storage bytes, and costs. Preserve raw histograms and time series; do not average percentiles.
6. Continuously check idempotency, source/overlay preservation, unauthorized-read sentinels, pagination, publication/checkpoint pairing, and outbox state. Exercise DELIVERY.md faults and isolated restore with dispatch disabled.
7. Preserve run-manifest.json, summary.json, histograms, time series, correctness results, cost ledger, and commands. Invalid or failed runs remain recorded.
8. Label evidence as exploratory, capability-scoped, or full-profile. A capability-scoped run may exercise a declared subset but must publish missing operation classes and its modified mix; it cannot certify the original card. Unsupported operations never count as successes.
9. Full-profile certification requires all operation classes, all selected concurrent background/data workloads, approved numeric gates, no unresolved required inputs, and every repetition passing safety, service, recovery, and cost gates. Report the capacity boundary and uncertainty rather than selecting only the best run.

Harness implementation is tracked separately. Existing benchmark kernels do not establish coverage of this contract. M0 can produce reviewable provisional contracts without certifying production capacity; approving assumptions and producing capacity evidence are distinct decisions.

## Validate calibration and operation capabilities

From the repository root after development setup:

```sh
# Validate the checked-in calibration without changing any files.
.venv/bin/python scripts/validate-production-workloads.py
# A different v0.2 calibration file can be supplied as the positional argument.
.venv/bin/python scripts/validate-production-workloads.py docs/production/workloads.v0.2.json
# Require every external API operation class in the caller's capability declaration.
.venv/bin/python scripts/validate-production-workloads.py --scope full-profile \
  --operations detail page filteredSorted singleObjectAction multiObjectAction aggregateOrRelation
# Explicitly select a subset and report the resulting mix and missing classes.
.venv/bin/python scripts/validate-production-workloads.py --scope capability-scoped \
  --operations detail page
```

The reusable Python entry points in `benchmarks.workloads` are `parse_calibration(json_text)`,
`load_calibration(path)`, `validate_calibration(decoded_object)`, and
`check_capabilities(profile, operations, scope=...)`. Validation returns a detached JSON
object; failure raises `CalibrationError` with an ordered `errors` list. For example,
`$.profiles[0].steadyRps: ...` identifies an invalid rate, and a missing-field error names
both the containing object and the required field. The CLI prints these errors to stderr
and exits nonzero. It never rewrites the input. Unknown fields and unsupported versions
are rejected; v0.1 remains historical and is not converted.

The existing document validator now delegates to this module, preserving its `validate`
list-of-errors interface and six-mutation `--self-test`. The reusable validator first checks
required fields, types, finite numbers, units, and fixed semantic flags, then checks the
existing cross-field rules (card coverage, mix total, rate ordering/scaling, populations,
percentiles, and bulk timing). The normal `pnpm check` gate also runs deterministic unit
fixtures via `.venv/bin/python -m unittest benchmarks.test_workloads`.

Operation selection covers the six external API classes in `mixPercent`. Both the API and
CLI require an explicit scope when checking capabilities. A `full-profile` selection rejects
every missing class, including a class whose calibration weight is zero. Empty, unknown,
duplicate, malformed, or zero-total-weight selections fail. A `capability-scoped` selection
reports `selectedOperations`, `missingOperations`, `originalMixPercent`,
`selectedOriginalPercent`, and `effectiveMixPercent`. The effective percentages redistribute
the selected original weight proportionally to total 100; they are not a claim that the
omitted load ran. For detail/page, the selected original weight is 65%, with an effective
mix of approximately 53.846%/46.154%. The input mix is unchanged. Output uses contract class
order regardless of selection order; floating percentages can have normal rounding error.

`fullProfileOperationCoverage` only describes the caller's declared coverage. Even with all
classes selected, `fullProfileCertified`, `numericTargetsApproved`, and
`productionClaimsAllowed` remain false. Capability-scoped output keeps that scope even if
all classes are selected. No implementation support or successful outcomes are inferred.
Background/data experiments remain separate obligations: this API does not select or execute
them. The calibration is not a complete benchmark run manifest; required run-input names
and unresolved decisions are declarations, not resolved values. Passing these checks supplies
neither runtime/capacity evidence nor approval of provisional targets, and does not certify
any card or deployment.
