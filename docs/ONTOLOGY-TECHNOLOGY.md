# Ontology technology decision: initial million-scale evidence

**Update October 15:** the Rust follow-up below demonstrates better throughput and lower memory than a fresh Node baseline. The initial three-language results remain valid for those implementations; they must not be used to claim Rust has no advantage.

**Implementation follow-up:** off-lock streaming staging and integrated UID-ordered, policy-aware pagination are now implemented. The real service completed short 1m/10m runs at 20/100 callers without failures after fixing query planning and cursor transaction contention. See [PERFORMANCE.md](PERFORMANCE.md) for evidence and limits. The activation/pagination work described as future work in the original study below records the decision at that time; arbitrary sorts, aggregations and sustained mixed-load capacity remain unverified. These newer runs do not compare Rust with the integrated TypeScript implementation.

Measured October 14, 2026, America/Denver. Keep TypeScript/Node for the ontology API and PostgreSQL for its serving store at this stage. These measurements do not justify a language rewrite. Prioritize indexed query paths, bounded responses, authorization-aware aggregates and short publication transactions. Keep Iceberg for datasets; this experiment does not compare dataset execution engines.

This is a provisional implementation decision, not a production capacity certification. The tested servers are deliberately small benchmark kernels, not the full Moria API. The existing generation/overlay model, complete policies and React rendering still need integrated measurement.

## Experiment

Ran 36 measured trials: 1m/10m objects, 20/100 outstanding HTTP requests, Node/Go/Python, three repetitions per combination. Each trial had one second of warmup and six seconds of measurement. All **510,047 measured requests succeeded**, including response-content checks; none timed out. A 10k smoke run passed first. The repository typecheck and all 46 tests passed after adding the harness.

The workload was 70% queue pages of 100 objects, 20% object details and 10% idempotent actions. Requests read stored authorization; queries exclude marked rows and a restricted field. Actions serialize through an advisory lock before their transaction and write a revision, history and receipt. Each implementation uses the same SQL and 20 database connections. Every response is decoded and validated by a separate load generator.

One Node or Python event loop was compared with Go using GOMAXPROCS=1. This controls the per-worker comparison; it does **not** establish a tuned multicore winner. Node uses pg 8.23.1, Go uses pgx 5.7.6, Python uses psycopg 3.3.6 with aiohttp 3.14.5. Runtime versions: Node 24.22.0, Go 1.26.9, Python 3.11.2, PostgreSQL 18.4. Different drivers, pool scheduling and serializers remain part of the implementations being compared. Python version/serializer alternatives were not evaluated.

Host: Ryzen 7 9800X3D, 32 GiB RAM, Windows with Ubuntu 24.04 in WSL2. The non-root Docker container was limited to eight CPU equivalents and 12 GiB for the database, application and generator together. PostgreSQL used 512 MiB shared buffers, 16 MiB work_mem, fsync on and no parallel query workers. Data lived on Docker storage. Existing local inference remained available outside the container; host background activity was not isolated. No paid model calls were made.

## HTTP results

Requests/sec is the median of three trials. Latency columns show the **range of each trial's p95**, not a pooled percentile or confidence interval. Memory is the maximum application high-water RSS across the three runs, excluding PostgreSQL and the generator.

| Objects | Concurrent requests | Implementation | Successful requests/sec | Queue p95 ms | Detail p95 ms | Action p95 ms | App peak MiB |
|---|---:|---|---:|---:|---:|---:|---:|
| 1m | 20 | Node/TS | 3,098 | 4.1–4.5 | 3.1–3.6 | 45.3–46.6 | 294 |
| 1m | 20 | Go | 2,307 | 4.3–4.8 | 3.1–3.5 | 58.5–64.9 | 17 |
| 1m | 20 | Python | 1,893 | 4.1–11.1 | 3.5–10.8 | 78.1–79.1 | 56 |
| 1m | 100 | Node/TS | 3,086 | 30.2–30.8 | 29.5–30.0 | 80.3–83.5 | 288 |
| 1m | 100 | Go | 2,209 | 105.5–119.0 | 102.9–123.0 | 162.0–191.6 | 22 |
| 1m | 100 | Python | 1,755 | 50.0–59.6 | 49.4–58.5 | 63.6–134.4 | 61 |
| 10m | 20 | Node/TS | 3,005 | 4.6–5.9 | 3.0–4.3 | 42.8–44.4 | 294 |
| 10m | 20 | Go | 2,157 | 5.9–5.9 | 4.1–4.1 | 63.1–64.2 | 17 |
| 10m | 20 | Python | 1,919 | 4.5–4.7 | 3.7–3.8 | 75.6–77.6 | 55 |
| 10m | 100 | Node/TS | 2,885 | 33.5–34.7 | 32.3–33.6 | 84.6–87.5 | 311 |
| 10m | 100 | Go | 2,120 | 109.2–115.2 | 110.9–115.0 | 169.5–174.5 | 22 |
| 10m | 100 | Python | 1,695 | 59.0–65.6 | 57.8–64.6 | 62.3–70.0 | 61 |

At 10m/100, Node's queue p99 ranged from 42.8–48.0 ms and action p99 from 89.0–96.4 ms. All per-operation p50/p95/p99/max values and error maps are retained in the raw results. These short trials are not enough to characterize rare stalls.

Node achieved about 36% more throughput than Go and 70% more than Python at 10m/100. Go used dramatically less application memory. Python's action p95 was lower than Node's in that combination, although it delivered fewer actions per second. There is no universal latency winner: closed-loop trials run each implementation at a different achieved arrival rate, and transaction/pool scheduling matters. Fixed-arrival-rate tests are needed for equal-load latency comparisons.

The application processes consumed roughly 0.85–0.94 CPU cores on average; generator CPU remained approximately 0.48–0.73 cores in the 100-request cases. This suggests application-worker pressure rather than a fully CPU-saturated load generator, but does not rule out generator overhead. The eight-core container quota was not a promise that each application used eight cores.

## Database query costs

These are separate, single-query EXPLAIN ANALYZE execution times, before network/application overhead. Each entry is the range of three executions. Indexed queries use keyset pagination and return the same 100-object projection.

| Query shape | 1m execution ms | 10m execution ms |
|---|---:|---:|
| Typed columns, matching composite index | 0.41–0.43 | 0.44–1.05 |
| JSON fields, matching expression index | 0.41–0.41 | 0.41–0.46 |
| Typed columns, forced scan/sort | 279–286 | 3,534–3,542 |
| JSON fields, forced scan/sort | 396–397 | 4,176–4,319 |
| OFFSET 100,000, indexed ID-only page | 9.6–12.2 | 8.9–25.1 |
| Exact authorized-region counts by status | 24.8–29.0 | 262–452 |

Both indexed layouts used the intended indexes. The 1m plans each touched 112 shared buffers for 100 returned objects. Forcing scans disables index/bitmap paths deliberately; these are not accidental planner choices. The deep-offset query returns only IDs, so its timing is not an equal-payload comparison against the 100-object queue; nevertheless it shows the avoidable work of walking preceding entries.

At 10m, the object table was 4.77 GiB and its three indexes together were 1.89 GiB. The synthetic objects contain about 500 bytes of stored data each; wider properties, more indexes, generations, overlays and link fan-out can change the result substantially. The dataset could fit within the memory budget, and no cache flush was performed. This is not evidence of disk-bound or cold-cache capacity. The 1m seed took 11.6 seconds; adding the next 9m objects plus vacuum/analyze took 103.7 seconds. Server-side synthetic generation is not Moria ingestion performance.

## Implementation decisions

1. **Keep Node/TypeScript for the initial integrated ontology API.** It was the strongest throughput/read-latency implementation in the initial three-language comparison. The Rust follow-up below changes the performance ranking, while retaining the existing integrated API pending representative policy/history tests. Java and alternative serializers remain untested. Benchmark normal multicore configurations before extrapolating per-worker results.
2. **Use PostgreSQL as the initial ontology serving engine, with explicit indexed query contracts.** Typed serving columns are a reasonable schema/validation choice for known filter and sort fields, but these results do not prove typed indexes beat equivalent JSON expression indexes. Flexible properties can remain JSON where appropriate. Composite index ordering must match actual predicates and orderings; this is consistent with PostgreSQL's [multicolumn-index guidance](https://www.postgresql.org/docs/current/indexes-multicolumn.html).
3. **Use bounded projections and keyset pagination for workflow lists.** Do not fetch millions of objects into the application to filter/sort them. Add integrated policy-aware query compilation and pinned cursors before treating the benchmark path as a usable product endpoint.
4. **Treat dashboard aggregation as separate performance work.** Exact counts over millions of rows already cost hundreds of milliseconds alone. Evaluate incremental/materialized aggregates or a separate analytics path, preserving current authorization and invalidation semantics. This study does not select an alternative analytics database.
5. **Keep Python for Iceberg/data-worker orchestration for now.** Its HTTP result is not a reason to rewrite Arrow/Iceberg processing. Benchmark bounded-memory batch execution, bulk loading and real transforms separately before selecting their implementation technology.
6. **Shorten publication critical sections.** The real 1m activation still holds the environment writer lock for roughly 28 seconds and materializes all rows. Stage/validate serving generations outside that lock and publish with a short transaction. Kubernetes or a faster HTTP language does not remove this serialized dependency.

## Required next evidence

Integrate the winning query design with real Moria generations, overlays, current row/property/marking checks, revocation and pinned pagination. Measure links and representative React workflow payloads. Then run equal-arrival-rate tests, sustained mixed reads/actions/ingestion, cold/wider-than-memory populations, retained history, multicore workers and a real deployment host. Establish explicit workflow latency/error budgets before declaring OPS-03 satisfied. Ten million is a benchmark milestone, not a product limit.

No runtime language or production storage implementation was changed by this study. Source and reproduction instructions: [benchmark harness](../benchmarks/ontology/README.md). Evidence: [full results and plans](benchmarks/technology/technology.json), [environment/source manifest](benchmarks/technology/manifest.json), [smoke run](benchmarks/technology/smoke.json). The measured image is recorded in the manifest; the Dockerfile now pins the same resolved Python dependencies and Go checksums for rebuilding.

## Rust follow-up — October 15

Added release-compiled Rust 1.99.0, Axum 0.8.9, Tokio 1.53.3 (current-thread), tokio-postgres 0.7.18 and deadpool-postgres 0.14.2. Cargo.lock records all resolved dependencies. The same SQL, connection count, JSON decoding/encoding, authorization fixture, idempotency checks, workload, hardware limits and per-worker methodology apply. Node was rerun in the same experiment. Twenty-four full-size trials completed with **498,001 successful requests and zero failures**, after a passing 10k smoke test.

| Objects | Concurrent requests | Implementation | Median requests/sec | Queue p95 range ms | Action p95 range ms | Max app MiB |
|---|---:|---|---:|---:|---:|---:|
| 1m | 20 | Node | 3,216 | 4.3–4.5 | 43.6–44.7 | 296 |
| 1m | 20 | Rust | 3,998 | 2.7–3.5 | 36.2–37.4 | 8 |
| 1m | 100 | Node | 3,039 | 29.7–31.3 | 80.5–82.8 | 290 |
| 1m | 100 | Rust | 3,959 | 22.9–23.6 | 58.1–60.6 | 12 |
| 10m | 20 | Node | 3,007 | 4.3–4.7 | 44.8–45.9 | 310 |
| 10m | 20 | Rust | 3,728 | 3.7–4.1 | 36.1–37.2 | 7 |
| 10m | 100 | Node | 2,924 | 31.5–34.6 | 81.0–87.2 | 317 |
| 10m | 100 | Rust | 3,690 | 26.1–26.9 | 61.0–63.5 | 12 |

At 10m/100, Rust delivered **26% higher throughput**, with application memory about 12.3 MiB versus 316.9 MiB. Its median sampled application CPU was 0.77 cores versus Node's 0.94. The generator used about 0.80 cores with Rust, so this is an observed throughput advantage, not a measured upper capacity limit. Exact query counts, detail latencies, tail distributions and plans are in [raw results](benchmarks/technology/rust-comparison.json); [manifest](benchmarks/technology/rust-manifest.json) records the measured image and binary hashes. [Smoke results](benchmarks/technology/rust-smoke.json) are separate.

**Stack selected for continued implementation:** TypeScript/Node for the existing integrated API, policy compiler, CLI and SDK; PostgreSQL for ontology serving and transactions; Python with native data engines and Iceberg for datasets; React/TypeScript for custom UIs. Rust is now an evidence-backed candidate for a dedicated serving component, rather than an untested suggestion. This is an engineering tradeoff: retaining the existing security-tested path lets the next work address the approximately 28-second activation lock and implement correct policy-aware pagination, while the measured Node read latency remains in the tens of milliseconds. It is not a claim that Node is faster or more memory-efficient than Rust.

A production Rust migration requires equivalent policy/value semantics, generation/overlay behavior, pinned/current authorization checks, revocation and failure tests, followed by equal-arrival-rate and multicore benchmarking. The benchmark handler is intentionally not promoted into the product. All earlier limits on duration, cache state, dataset width and missing full Moria integration remain applicable.
