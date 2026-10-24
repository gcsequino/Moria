# Workloads and benchmark contract

Version 0.1. All numerical targets are proposed calibration points, not measured ServiceNow equivalents or accepted production SLAs.

## Units

Report these separately: external API request, logical business action, database transaction attempt/commit, mutated object, background execution, source event, source byte, transformed byte, and physical stored byte. A single action can mutate several objects and trigger background work. Retries count as attempts, not additional logical successes.

Latency is scheduled-arrival-to-complete-response and includes queueing; time spent waiting in the load generator is reported separately. Also report commit latency, application/database time, and asynchronous completion delay. Acknowledgment of enqueue is not completion of a business workflow. Input volume uses decimal bytes; memory uses binary units.

## Transactional calibration ladder

| Card | Object population | Steady offered API requests/s | Busy-hour requests/s | Peak requests/s for 15 min |
|---|---:|---:|---:|---:|
| T-S: economical | 1m | 20 | 50 | 100 |
| T-B: bursty enterprise calibration | 10m | 500 | 1,000 | 2,000 |
| T-E: large-enterprise calibration | 50m, repeat at 100m | 2,000 | 4,000 | 10,000 |

These are load-generator settings selected for exploration, not evidence that a bank uses these rates. Ramp through intermediate rates to find the capacity boundary; do not treat success at T-S as success at T-E.

T-E assumes 4 KiB mean logical object payload, with separate 1 KiB and 16 KiB sensitivity runs. Fifty million 4-KiB payloads total about 204.8 GB before indexes, history, database overhead, replicas, and backups. Seed 20% of objects with overlays, 2 retained source generations, and 10 historical edits per edited object. Exercise relation fan-out 0, 5, and 50, with an explicitly capped high-fan-out stress case.

External request mix: 35% detail reads; 30% pages (50 rows, projected response cap 256 KiB); 15% filtered/sorted lists; 10% single-object actions; 5% multi-object actions (mean 5, maximum 100 objects); 5% bounded aggregates/relationship reads. These classes sum to 100%. A business action maps to one action request in this generator. T-E steady therefore offers 300 logical actions/s; retries and internal database work are additional. Unsupported operations are reported as coverage gaps, never relabeled successes.

Use two identity mixes (80% human/20% integration, then 20%/80%). Include three region attributes, at least two markings, property restrictions, denied requests, and current permission changes. Apply production authentication/session checks. Vary visible fractions across 1%, 10%, and 100%.

Distribute 80% of normal traffic across the full population and 20% across a 1% hot set. Add a contention trial that targets 1% of actions at the same 100 objects. Report serialization retries and rejected conditions separately from infrastructure errors.

Background load is not included in external request rates: offer 50 executions/s at T-E steady (each proposes two object updates and one simulated outbox delivery), doubling during busy-hour, plus a 100,000-object hourly bulk update. Record resulting internal requests/mutations and backlog. Use deterministic local writeback fixtures; do not contact production destinations.

## Provisional service targets

| Dimension | Proposed engineering target |
|---|---|
| Indexed detail/list queries | p95 <=500 ms; p99 <=2 s |
| Single/multi-object actions within supported limits | p95 <=1 s; p99 <=3 s |
| Unexpected errors at the declared supported rate | <=0.1%; all attempts included |
| Safety | zero observed invariant violations; any violation fails the run |
| HA operational API availability | candidate 99.95% monthly, definition/exclusions pending review |
| Small deployment | no HA availability promise; declared maintenance/cold-start behavior |
| HA single-failure-domain acknowledged action loss | candidate RPO 0 under the declared synchronous durability boundary |
| HA database failover | candidate RTO <=5 min |
| Disaster recovery | candidate RPO <=15 min, RTO <=4 h; end-to-end recovery must include storage and keys |
| Burst recovery | oldest operational backlog back within baseline limit <=30 min after peak |
| Scheduled/bulk processing freshness | completion deadline per pipeline; do not substitute API latency |
| Continuous processing freshness | candidate p95 <=60 s, p99 <=5 min source-to-published result |

Condition failures, expected authorization denials, and invalid inputs have separate categories; they cannot conceal infrastructure errors. A deliberate 429 is an admission rejection and counts against offered-load acceptance at a claimed supported rate. A cold-start profile must publish its startup latency and cannot inherit the warm target.

No global sub-second streaming SLA is assumed. Exact budgets and availability/error denominators are unresolved product decisions. RPO 0 does not cover arbitrary simultaneous loss of all synchronous copies.

## Independent data-processing ladder

D-S: 100 GB/day input, 30-day input retention. D-M: 1 PB per 30 days, 30-day input retention. D-L: 1 PB/day, 30-day input retention. D-L is a planning scenario; the user's ingestion rate is not yet selected. Keep raw-input bytes separate from compressed lakehouse bytes and derived materializations.

At D-L the average incoming payload is about 11.57 GB/s (92.59 Gbit/s), excluding protocol overhead, replication, reads, shuffle, and retries. For a 4x 15-minute burst, model explicit buffer capacity and drain rate; equal-to-average processing capacity cannot clear backlog.

Exercise append, keyed merge, schema evolution, full refresh, incremental indexing, wide join, skewed join, group aggregation, compaction, snapshot planning, and expiration. Compare 16 MiB, 256 MiB, and 1 GiB file layouts at equivalent logical volume; record file counts and planning cost. Values are experimental layouts, not universal optimal sizes.

Analytical load: start at 5, 20, and 50 concurrent queries; separate partition-pruned aggregates, wide scans, and joins. Query deadlines and maximum bytes scanned are workload-specific. Run Spark transformations, Trino queries, compaction, and serving concurrently when certifying the combined profile.

Do not equate dataset records with ontology objects. Specify materialization fraction and incremental update rate for each pipeline; benchmark indexing separately.

## Reproducible harness specification

1. Isolated environment only. Pin source revision, container/dependency digests, schema, policy, engine/storage versions, seed, topology, quotas, and failure script. Record the generation command and its exit status.
2. Seed the full population/history independently from measurement. Verify expected counts, identity uniqueness, source/overlay checksums, and authorization fixtures.
3. Use an open-loop offered-rate generator with bounded outstanding work and recorded scheduled timestamps. Account for missed arrivals, generator saturation, and coordinated omission; never infer throughput from a closed-loop concurrency number alone.
4. Establish a 10-minute warmup, a 30-minute steady measurement, 15-minute peak, and at least 30-minute recovery window. Perform three independent repetitions. Run a separate 60-minute busy-hour trial, 24-hour soak, and cold-cache/wider-than-memory trial. Certifying petabyte tiers requires sufficient actual scale/duration, not extrapolation from this minimum.
5. Report per-operation p50/p95/p99, successes/failures/timeouts/rejections, achieved/offered rates, retry amplification, in-flight concurrency, queue age, resource saturation, network/storage bytes, and costs. Publish raw histograms and time series, not averaged percentiles.
6. Run correctness oracles continuously: idempotency receipts, source/overlay preservation, unauthorized-read sentinels, pagination consistency, publication/checkpoint pairing, and outbox state.
7. Perform deterministic fault trials from DELIVERY.md. Restore in a separate target with dispatch disabled. Preserve logs of expected versus observed outcomes.
8. Produce run-manifest.json, summary.json, per-operation histograms, queue/resource series, correctness results, cost ledger, and commands. Invalid/generator-limited runs remain recorded but cannot certify capacity.
9. Require every repetition at the claimed tier to meet safety and defined service/cost gates. Report the capacity boundary and uncertainty; do not select only the best run.

The v0.1 JSON declares calibration inputs. It is not an executable load generator. Building the harness is tracked separately; existing benchmark kernels cannot claim full coverage of this specification.
