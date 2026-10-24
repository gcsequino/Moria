# Ontology technology comparison

Exploratory benchmark kernels, not production servers. Never expose these HTTP handlers: they deliberately implement only a small common subset of Moria semantics.

Build and run from the repository root on Linux:

```sh
docker build -f deploy/test/Dockerfile -t moria-test:local .
docker build -f benchmarks/ontology/Dockerfile -t moria-ontology-bench:local .
mkdir -p docs/benchmarks/technology
docker run --rm --network none --cpus 8 --memory 12g \
  -v "$PWD/docs/benchmarks/technology:/results" moria-ontology-bench:local
```

The non-root container must be able to write the results directory. Defaults: 1m then 10m objects, 20/100 outstanding requests, three repetitions of six seconds, one-second warmup before each. Override `BENCH_SIZES`, `BENCH_CLIENTS`, `BENCH_REPS`, `BENCH_SECONDS`, `BENCH_OUTPUT` for diagnostics. The database lives on container storage and is discarded with the container; only results are mounted. No credentials are persisted in results. Do not use a real application database.

Every language executes the same SQL, with the same 20-connection limit and statement timeout. Each request reads a stored session restriction. Queue/detail responses exclude marked objects and a restricted property. Actions acquire a shared advisory lock before SERIALIZABLE BEGIN, check authorization, update an eligible object, increment a revision, write history and store an idempotent receipt. Preflight checks denied reads, equal typed/JSON queue results and duplicate receipt replay. Every measured response is parsed and checked for status, expected page shape and restricted-data leakage. These checks do not prove full production policy equivalence.

The workload is 70% 100-object queue pages, 20% details, 10% actions, using a deterministic stride over eligible IDs throughout the population. Each concurrency slot starts its next request after completion: this is closed-loop saturation, not a specified arrival rate or a number of human users. Five-second client timeouts count as failures and their latency is included. Short repetitions do not establish sustained capacity or reliable rare-event percentiles.

Node and Python use one event loop; Go is restricted to GOMAXPROCS=1 for the per-worker comparison. This measures these particular HTTP/driver/JSON implementations, not intrinsic language speed or tuned multicore deployments. SQL performs response projection/JSON aggregation; every application decodes and re-encodes JSON. Automatic prepared-statement caching is disabled in Python/Go to match the unnamed Node queries. Order rotates across repetitions to reduce order bias, but all runs share an evolving database, cache and write history. Indexes exist during seeding; the second seed time is incremental growth from 1m to 10m.

EXPLAIN ANALYZE compares typed and expression-indexed JSON keyset queries, both queries with index scans disabled, a deep OFFSET page, and exact live counts. These queries run separately from HTTP load, three times each. Scan comparisons deliberately disable index/bitmap paths; do not interpret them as a natural planner failure. Full plans and buffer counts are retained. There is no cache flush, so these are not controlled cold-cache measurements.

Report successful requests/sec, failures, per-operation latency distributions, application high-water RSS, application CPU and sampled PostgreSQL process CPU. The external load generator reports its own CPU: near-one-core usage may limit observed throughput. CPU sampling is approximate and does not capture every short-lived/background process. RSS excludes the database and generator. Database table/index sizes exclude WAL and history tables. Container resources are shared by database, app and generator. Host activity outside the container is not isolated.

Not measured: Iceberg transform throughput; actual Moria source generation/overlay storage; full policy compiler, revocation races and pinned cursors; links; OIDC; TLS; browser rendering or React interaction latency; multi-process scaling; HA; concurrent bulk ingestion; long-running soak behavior. Keep the existing integrated Moria benchmark as a separate baseline.

## Rust follow-up

Build `Dockerfile.rust` after the base benchmark image, then run `moria-rust-bench:local` with the same resource limits and results mount. It defaults to `BENCH_IMPLEMENTATIONS=node,rust` and writes `rust-comparison.json`, preserving the earlier results. The Rust server uses release compilation, Axum, a current-thread Tokio runtime, a 20-connection deadpool-postgres pool, and tokio-postgres `query_typed` without prepared statement caching. It decodes/re-encodes the same JSON result as the other implementations. The [driver documentation](https://docs.rs/tokio-postgres/0.7.18/tokio_postgres/struct.Client.html#method.query_typed) explains how explicit parameter types avoid extra preparation round trips. Cargo.lock and the compiler-image digest pin this experiment's dependencies. `rust-version.txt` is generated inside the build stage, not supplied by the host.

```sh
docker build -f benchmarks/ontology/Dockerfile.rust -t moria-rust-bench:local .
docker run --rm --network none --cpus 8 --memory 12g \
  -v "$PWD/docs/benchmarks/technology:/results" moria-rust-bench:local
python benchmarks/ontology/summarize.py docs/benchmarks/technology/rust-comparison.json
```
