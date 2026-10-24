# Initial performance evidence

The comparative 1m/10m PostgreSQL and Node/Go/Python study is documented in [ONTOLOGY-TECHNOLOGY.md](ONTOLOGY-TECHNOLOGY.md), with a reproducible harness under `benchmarks/ontology`. It measures mixed HTTP benchmark kernels separately from the integrated Moria paths below; it does not remove their outstanding scale gates.

Measured October 14, 2026 (America/Denver). Raw records use UTC timestamps. No language change is indicated by these measurements. These short microbenchmarks are not production capacity estimates or the complete OPS-03 acceptance test.

## Required scale

User clarified that both datasets and the Ontology must support millions of rows/objects. The 100k measurements below are developmental evidence only. Architecture and OPS-03 now require progressive 1m/10m validation; 10m is a benchmark milestone, not a product limit. Dataset ingestion/transforms and authorized Ontology queries must be measured separately, with sustained concurrent work, realistic row sizes and retained history. The present in-memory activation harness is deliberately capped at 1m until bounded-memory loading is implemented; increasing that guard alone is not a scalability fix. New point-operation samples use a deterministic stride across the population instead of touching only the first 1,000 generated objects, so they are not directly comparable to earlier warm working-set results.

## Workload and hardware

AMD Ryzen 7 9800X3D, 16 logical CPUs, approximately 32 GiB host RAM, Windows 11, Node 24.19.0, native PostgreSQL 18.4. Isolated fresh database per run. Synthetic objects have UID, region and nullable assignee. Each run activates one full generation, warms 50 point reads, then performs 1,000 reads and 200 writes with 20 closed-loop clients. The integrated service run additionally checks current authority, markings, row/property policies and action conditions. Identity fixture is a workload principal; OIDC is not included.

Percentiles describe successful requests; failures are separately counted. Connection startup contributes to early maximum latencies. Later phases benefit from warmed connections and caches: do not interpret faster authorized actions as authorization having negative cost. Node RSS excludes PostgreSQL and Python. A local model server was running; these are not controlled dedicated-host results.

## Findings and changes

| Measurement | Initial 10k objects | Queued writes, 10k | Batched activation, 10k | Integrated run, 100k |
|---|---:|---:|---:|---:|
| Source activation | 1,587 ms | 1,596 ms | 223 ms | 2,359 ms |
| Internal action successes / attempts | 29 / 200 | 200 / 200 | 200 / 200 | 200 / 200 |
| Internal action p95 | 3.8 ms* | 66.7 ms | 66.7 ms | 68.4 ms |
| Internal point-read p95 | 1.17 ms | 1.12 ms | 1.57 ms | 1.70 ms |
| Authorized point-read p95 | — | — | — | 5.28 ms |
| Authorized action p95 | — | — | — | 30.92 ms |

*The original action latency is misleading alone: 171/200 requests failed with serialization errors. It is not a desirable baseline.

Writers previously waited for an environment row lock after taking a SERIALIZABLE snapshot. At 20 clients, three retries could not recover reliably. Cooperating writers now acquire a PostgreSQL session advisory lock before beginning the serializable transaction. This keeps commit ordering and coordinates across processes; process death releases the lock. The database row lock and CAS checks remain. Lock acquisition has a 30-second timeout, the prior connection setting is restored, and cleanup failures discard the connection. Twenty-writer regression and process-kill tests pass. Uncoordinated external writers remain unsupported.

Source activation previously sent approximately three database requests per row. Batching registration checks, home bindings and inserts in groups of 1,000 reduced round trips. At the same 10k size, activation was approximately 7.1 times faster. The 100k integrated run activated approximately 42k rows/second. It peaked at approximately 310 MiB Node RSS; this still materializes all input rows and is not a streaming-memory result.

Raw evidence is in `docs/benchmarks/`. The earlier `batched-100000.json` overlapped briefly with the test suite; use the later `authorized-100000.json` for the current 100k baseline.

The Linux/WSL2 non-root container run also completed every request at 100k objects and 20 clients. Activation took 2.32 seconds; authorized point-read p95 was 17.05 ms and action p95 was 90.61 ms. Peak Node RSS was approximately 356 MiB. This run used Node 24.22.0, PostgreSQL 18.4, a WSL VM reporting approximately 15.4 GiB RAM and Docker writable-layer storage. Differences from native Windows are confounded by runtime versions, virtualization and storage; they do not establish an OS ranking. Raw data: `linux-100000.json`.

The first native Windows 1m-object run completed all operations at 20 clients: 1,000 authorized point reads and 200 authorized actions, p95 5.74/33.26 ms respectively. Activation took 27.85 seconds (approximately 35.9k rows/second); sampled peak Node RSS was 1,140,391,936 bytes (approximately 1.06 GiB), excluding PostgreSQL. Raw data: `authorized-1000000.json`. Sampling now spans the population. This is still a narrow-row, single-generation, short point-operation test, not evidence of million-scale dataset transformations, multi-row queries or sustained mixed-load capacity. Activation currently holds the environment writer lock during loading: its approximately 28-second duration approaches the 30-second writer lock timeout, making concurrent ingestion/action testing and off-lock staging mandatory before claiming this scale is supported.

## Reproduction

```sh
pnpm exec tsx scripts/benchmark.ts --rows 100000 --clients 20 --output .local/benchmark.json
docker build -f deploy/test/Dockerfile -t moria-test:local .
docker run --rm --network none moria-test:local
```

The image is a non-root test harness with a digest-pinned Node base, not production packaging. Python dependency versions are constrained; apt repository contents and package hashes are not release-locked yet.

## Remaining performance work

- Sustained mixed reads/writes with latency of failed requests, multiple repetitions, warm/cold storage and percentile confidence.
- Broader multi-row authorized sorting, aggregation and pagination workloads; the new UID-ordered query checkpoint below covers a narrow warm queue.
- HTTP/OIDC, connection pooling across services, PostgreSQL memory/CPU/I/O and full pipeline resource accounting.
- Integrate the new staged generation/head-swap path with fenced Iceberg publication and sustained-write conflict handling.
- Columnar/streaming worker input instead of whole-dataset JSON, and bulk UID allocation.
- Overlay-history and source-generation retention/compaction with active pins respected.
- Benchmark the same workload on the intended Linux deployment and under constrained CPU/RAM.

The environment writer lock remains a deliberate throughput ceiling. Kubernetes will not remove it. Profile these paths before considering another implementation language.

## Streaming staging checkpoint — October 15

ObjectStore now accepts iterable/async-iterable rows, validates and writes batches of 1,000 into unpublished stages, and activates a completed stage through a small pointer transaction. Retained-overlay compatibility is checked in bounded batches against the expected revision before publication. A final epoch/revision check prevents a concurrently edited environment from accepting stale validation. Legacy source generations and pinned reads remain supported by an additive schema migration; older binaries using positional generation INSERTs require updating before writing to this schema.

The first native Windows 1m run staged narrow rows in 20.38 seconds, published in **3.07 ms**, and peaked at **216.8 MiB Node RSS**. This uses the real ObjectStore and PostgreSQL, not the language-comparison tables. Publication timing includes its lookup, lock acquisition and commit; it is not a direct lock-hold measurement. Registry seeding uses server-side synthetic identities outside the timing, and the benchmark has no retained overlays. The previous 27.85-second/1.06-GiB benchmark used a materialized random-UID fixture and exercised additional operations, so these are not controlled total-throughput comparisons. The structural improvement is that row loading no longer holds the environment write lock.

Tests demonstrate that a paused source stream does not block an action, that partial and failed sources never become visible, and that a subsequent stale publication is rejected. **Strict global revision CAS still means continuous action traffic can prevent a staged candidate from publishing.** Safe source-head/schema-aware revalidation is a separate next step; do not claim sustained mixed-ingestion capacity from this checkpoint. Failed/loading stages remain retained and require future lifecycle/GC coordination. Binding stages to authorized Iceberg snapshots and fenced workers remains unfinished.

Reproduce with `pnpm exec tsx scripts/benchmark-staging.ts --rows 1000000 --output .local/staging.json`. The harness supports 1,000 through 10m rows and buffers only one 1,000-row batch; callers passing a prebuilt array still own that array's memory. Very wide rows increase per-batch memory. Raw initial evidence: `docs/benchmarks/staging-1000000.json`.

The matching **10m-object run** completed staging in **195.13 seconds**, publication in **3.11 ms**, and peaked at **192.8 MiB Node RSS** (`docs/benchmarks/staging-10000000.json`). Both runs verified exact row counts, invisible pre-publication revisions, representative point reads across the population and publication replay. Lower sampled RSS in the larger run is runtime/GC variation, not evidence that memory decreases with volume. These two single-run observations support bounded input buffering for narrow rows; they do not measure PostgreSQL memory, full pipeline capacity, concurrent workload latency or overlay-heavy staging.

## Integrated pagination checkpoint — October 15

The real ObjectService now executes policy-aware UID-ordered pages against staged or legacy source generations, overlays and tombstones. Both pinned and current canonical row restrictions and effective-object policies apply before LIMIT. Projection/predicate permissions are checked before data access, and each returned row is independently checked by the reference evaluator. Opaque cursors bind identity, query, contract and epoch, preserving the original 15-minute deadline. Integration tests cover changes between pages, hidden properties, numeric/array/null predicates, expiry, revocation and concurrent cursor replay.

An initial UNION/CTE generation lookup took 1.32–1.39 seconds per 100-object page at 1m. Resolving immutable generation pointers in the same transaction and using constant stage keys allowed ordered index access: the next run took 3.4–9.7 ms. Evidence is retained in `query-initial-1000000.json` and `query-indexed-1000000.json`. This is query-shape evidence, not a language comparison.

Concurrent testing then found serialization cycles between transactions reading and inserting cursor metadata: 113/1,000 failures at 20 callers and 1/1,000 at 100 (`query-concurrent-initial-1000000.json`). Protected query/cursor transactions now use repeatable-read snapshots and an environment share lock. Data/security mutations remain serializable; their environment row changes wait for the active page. Cursor tokens are append-only. All 4,000 requests in the corrected two-population runs succeeded.

| Objects | Concurrent callers | Successes / attempts | Queries/sec | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|
| 1m | 20 | 1,000 / 1,000 | 876 | 25.15 | 86.42 |
| 1m | 100 | 1,000 / 1,000 | 1,042 | 99.95 | 109.40 |
| 10m | 20 | 1,000 / 1,000 | 887 | 25.31 | 89.27 |
| 10m | 100 | 1,000 / 1,000 | 1,057 | 100.86 | 106.49 |

Native Windows, Node 24.19.0, PostgreSQL 18.4, 20 database connections. Each page returns 100 narrow objects filtered by the current principal's region. Each caller walks from the beginning: this is a warm early-queue working set, not uniform population sampling. Each load lasts roughly one second, without warmup beyond three sequential pages; tail estimates and throughput are preliminary. Latencies include failures (none in the corrected runs). Peak Node RSS across staging and querying was 243.5/248.1 MiB. The 10m run staged in 197.43 seconds and published in 3.89 ms. Raw corrected results: `query-concurrent-1000000.json` and `query-concurrent-10000000.json`.

These measurements exclude HTTP, concurrent writes, retained overlay history, complex filters, custom sorts and Iceberg execution. HTTP routing and session revocation are tested separately at fixture scale. The environment share lock can delay writes during a page; SQL statements have a 30-second timeout, which is not a whole-request deadline. Sustained mixed-load measurement remains mandatory. Custom sorts and aggregations are explicitly unsupported rather than silently approximated.

```sh
pnpm exec tsx scripts/benchmark-staging.ts --rows 1000000 --query --output docs/benchmarks/query-concurrent-1000000.json
pnpm exec tsx scripts/benchmark-staging.ts --rows 10000000 --query --output docs/benchmarks/query-concurrent-10000000.json
```

## Mixed HTTP follow-up — October 15

The loopback HTTP harness now exercises workload bearer authentication, current marking clearance, canonical region restriction, hidden-property projection, pinned page chains and action overlays together. Requests are 70% pages of 100, 20% point reads distributed across the population and 10% idempotent actions. It validates response contents, ascending page continuity and stable pinned revisions. The generator does not retry failed requests; latency samples include failures. This uses the real API factory and ObjectService, not the language kernels.

The first 10k smoke run exposed query failures during writes (`http-mixed-smoke.json`). A diagnostic rerun captured PostgreSQL `40001` after internal retries: 31 queries failed (`http-mixed-diagnostic.json`). Repeatable-read alone did not fix a reader taking its snapshot before waiting for an environment row changed by a writer. Protected readers now acquire a shared environment advisory lock **before BEGIN**, matching the existing exclusive writer lock. Readers can run together; queued writes and readers take their snapshots after acquiring their respective locks. The row lock and current checks remain. Point reads and cursor operations use the same shared admission. Direct uncoordinated database writers remain unsupported.

The corrected five-second smoke phases completed without failures (`http-mixed-fixed-smoke.json`). The HTTP integration test now includes 200 interleaved requests from 20 callers. This change preserves serializable mutation transactions and pinned/current policy enforcement; it does not eliminate the environment-level write-concurrency limit.

The first million-object HTTP run omitted the explicit ANALYZE used by the earlier direct-query harness. It had no request failures but query p95 was **3.87 seconds at 20 callers and 15.75 seconds at 100** (`http-mixed-unanalyzed-1000000.json`), well outside the workflow goal. A fresh run with statistics refreshed before HTTP load reduced those to **35.71/158.69 ms**, with 42,240/42,240 requests successful (`http-mixed-1000000.json`). This experiment changes statistics preparation, not language or hardware; exact query-plan captures were not collected. Staging now runs ANALYZE on the shared staging table before marking a generation ready, outside the publication lock. At larger retained-history sizes, statistics maintenance and plan stability need further measurement. The 1m successful run used the equivalent explicit harness ANALYZE; subsequent 10m testing uses staging's built-in refresh.

| Objects | Callers | Query p95 ms | Detail p95 ms | Action p95 ms | Total successful requests |
|---|---:|---:|---:|---:|---:|
| 1m | 20 | 35.71 | 25.84 | 29.63 | 21,792 |
| 1m | 100 | 158.69 | 153.78 | 154.50 | 20,448 |
| 10m | 20 | 37.02 | 26.89 | 31.05 | 21,000 |
| 10m | 100 | 174.13 | 165.75 | 172.96 | 19,769 |

All **83,009 requests succeeded** across the four corrected phases, including 8,299 actions. The 10m run used staging's automatic statistics refresh, staged in 197.84 seconds and published in 3.72 ms. Peak combined Node server/generator RSS was 315.2 MiB at 1m and 312.8 MiB at 10m, excluding PostgreSQL. Raw 10m evidence: `http-mixed-10000000.json`. Throughput at 10m was approximately 699 total requests/sec at 20 callers and 656 at 100: more concurrency did not increase throughput in this configuration. Results meet the numerical query/action latency goals for this narrow workload, but do not satisfy OPS-03's broader width/history/ingestion and sustained-load gates.

Each phase schedules requests for 30 seconds then drains outstanding work. Narrow fixtures and early-queue page traversal remain favorable; point reads and actions cover dispersed UIDs. Overlay versions accumulate from real actions, but this is not a long-history stress test.

The load generator and HTTP server share one Node process, and the pool has 20 connections. There is no separate warmup; the 100-caller phase inherits cache and overlays from the 20-caller phase. These are closed-loop observations, not fixed-arrival-rate capacity measurements. Ingestion, OIDC, TLS, wider rows, cold cache, long history and independent client/server hosts remain unmeasured. The original read-only numbers above predate shared admission and should not be substituted for mixed-workflow performance.

```sh
pnpm exec tsx scripts/benchmark-staging.ts --rows 1000000 --http --seconds 30 --output docs/benchmarks/http-mixed-1000000.json
pnpm exec tsx scripts/benchmark-staging.ts --rows 10000000 --http --seconds 30 --output docs/benchmarks/http-mixed-10000000.json
```

## Refresh publication during edits — October 15

Source generations and stages now persist their object schema; stages also record the source head they replace. Publication may cross later environment revisions only when that head remains active and the staged schema exactly matches its persisted schema. Actions validate against the active schema before editing. Consequently all later overrides are compatible without rescanning them in the publication transaction. Competing refreshes still conflict; schema migrations and legacy generations without metadata retain strict environment-revision CAS. Security-epoch checks remain mandatory. Old API bindings fail closed after a schema change. This is a same-schema refresh path, not general schema-rebase or release-migration support.

The refresh harness (`--http --refresh`) streams a full replacement during each load phase, retains prior generations and edits, and keeps HTTP traffic active until both the minimum duration and publication complete. It checks the new canonical source and samples retained action overlays afterward. The first 1m run published both refreshes with 55,467 HTTP successes and no errors (`http-refresh-1000000.json`). However, long drain times suggest rare stalls hidden by p99; diagnostic runs add maxima, counts above one second and a representative pinned/current query plan. Do not treat its favorable p95 as a complete latency result.

The diagnostic confirmed **29.86-second maximum query latency** despite a sub-200-ms p99 (`http-refresh-diagnostic-1000000.json`). Its captured post-refresh plan scanned the current generation and sorted before LIMIT. Preserving the dependent current-UID lateral lookup with OFFSET 0 prevented that flattening; the new plans use the pinned UID index plus individual current-row lookups. This is a targeted UID-pagination plan constraint, not a guarantee for arbitrary predicate selectivity.

The corrected 1m run (`http-refresh-indexed-1000000.json`) completed **55,754 requests, zero failures, zero requests over one second**. At 20/100 callers, query p95 was **46.20/175.78 ms**, action p95 **39.66/171.63 ms**, and maximum across all operations **180.75/198.20 ms**. Refresh staging took **26.04/66.88 seconds**, publication including queue wait **28.60/118.15 ms**. These phases requested a five-second minimum and actually ran 26.09/67.14 seconds until refresh completed. Both phases use one shared 20-connection pool; higher caller count slows ingestion. Peak Node RSS was 322.9 MiB. The larger 10m concurrent-refresh run remains pending at this checkpoint; prior 10m mixed results did not include ingestion.

```sh
pnpm exec tsx scripts/benchmark-staging.ts --rows 1000000 --http --refresh --seconds 30 --output docs/benchmarks/http-refresh-1000000.json
pnpm exec tsx scripts/benchmark-staging.ts --rows 1000000 --http --refresh --seconds 5 --output docs/benchmarks/http-refresh-indexed-1000000.json
```

All runs retain narrow synthetic rows, same-process HTTP generation and shared database resources. This serving ingestion consumes an async row generator, not an Iceberg worker. Dataset snapshot authorization/fencing, wider schemas, long history, real deployment packaging and sustained multi-source ingestion remain gates.

### Completed 10m concurrent-refresh run

`http-refresh-10000000.json` completes the same corrected implementation at 10m objects. Both refreshes published while HTTP traffic continued, with **538,269 successes, zero failures, and zero requests over one second**, including 53,826 actions. The two phases ran for 254.07 and 658.78 seconds, ending after refresh and outstanding HTTP work drained. Each phase starts with a normal newly minted 15-minute workload session; session TTL was not weakened.

| Callers | Query p95 ms | Detail p95 ms | Action p95 ms | Maximum across operations ms | Refresh staging seconds | Publication including wait ms |
|---:|---:|---:|---:|---:|---:|---:|
| 20 | 46.64 | 34.64 | 40.09 | 369.05 | 254.01 | 36.46 |
| 100 | 178.16 | 170.60 | 173.66 | 216.50 | 658.53 | 111.19 |

Initial staging before traffic took 197.71 seconds and publication 2.98 ms. Peak Node RSS including the server, generator, latency samples and row producer was 342.9 MiB, excluding PostgreSQL. Three full source generations are retained by the end, plus action history. Captured post-refresh plans preserve the UID index path. The server-side scope remains narrow and warm; this is stronger evidence for concurrent refresh than the earlier short runs, not a complete OPS-03 certification. Sharing 20 connections and one event loop slows ingestion at 100 callers: isolate and measure worker/pool budgets before extrapolating deployment capacity.

```sh
pnpm exec tsx scripts/benchmark-staging.ts --rows 10000000 --http --refresh --seconds 30 --output docs/benchmarks/http-refresh-10000000.json
```

## Persisted-marking follow-up (October 15)

After generation markings and action-replay checks were added, the staging harness now persists the sensitive marking in initial and refreshed generations. Repeated the real mixed HTTP workload at 1m/10m objects, 20/100 callers, 30 seconds per phase: **79,626 successes, zero failures, no request over one second**, including 7,962 actions. Raw evidence: `http-markings-1000000.json`, `http-markings-10000000.json`; source hashes and base commit are in `http-markings-manifest.json`.

| Objects | Callers | Query p95 ms | Detail p95 ms | Action p95 ms | Total successful req/s |
|---|---:|---:|---:|---:|---:|
| 1m | 20 | 37.29 | 26.67 | 31.01 | 690.92 |
| 1m | 100 | 165.18 | 159.44 | 160.29 | 650.16 |
| 10m | 20 | 38.34 | 27.52 | 32.53 | 671.23 |
| 10m | 100 | 167.94 | 161.24 | 163.57 | 635.09 |

Initial 10m staging took 197.77 seconds, publication including acquisition/commit 3.40 ms. Peak Node RSS across loading and HTTP was 292.2 MiB (1m: 313.0 MiB), including the in-process generator and latency samples, excluding PostgreSQL. Hardware: Ryzen 7 9800X3D, 16 logical CPUs, approximately 31.6 GiB host RAM, Windows, Node 24.19, PostgreSQL 18.4, 20 DB connections. The mixed workload remains 70% 100-row pages, 20% detail, 10% action, enforcing canonical region and hidden-property restrictions as well as markings.

These are single short runs with narrow rows, warm early-page traversal, accumulated overlays and generator/server sharing one process. No concurrent ingestion in these two runs. Prior concurrent-refresh evidence remains separately labeled and predates the latest marking checks. This is a regression measurement, not a paired isolation of check overhead or a complete OPS-03 certification. The observed latencies support retaining TS/PostgreSQL provisionally; wider/history-heavy workloads, separate load generation and arrival-rate tests remain gates.

```sh
pnpm exec tsx scripts/benchmark-staging.ts --rows 1000000 --http --seconds 30 --output docs/benchmarks/http-markings-1000000.json
pnpm exec tsx scripts/benchmark-staging.ts --rows 10000000 --http --seconds 30 --output docs/benchmarks/http-markings-10000000.json
```

Updated refresh harness smoke: `pnpm exec tsx scripts/benchmark-staging.ts --rows 1000 --http --refresh --seconds 1 --output docs/benchmarks/http-markings-refresh-smoke.json` published both marked generations with 1,348 successes and no failures. This small smoke does not update the 1m/10m concurrent-refresh performance claims.
