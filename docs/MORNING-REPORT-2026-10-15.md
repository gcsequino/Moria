# Moria morning report — October 15, 2026

Moria has a tested internal backend prototype, not a completed first usable slice. Source is in this repository. Implementation head before this report: `24e1717`. Overnight changes are local commits; no deployment, paid inference or public publication was performed.

## What works

- PostgreSQL-backed canonical source generations and persistent overlays preserve permanent UIDs, reanimation edits, explicit nulls and deletion tombstones.
- ObjectService and the workload HTTP API enforce current authorization, markings, canonical row restrictions and property permissions for point reads, UID-ordered paginated queries and single-object actions. Cursors pin data while rechecking current visibility and have nonextending expiry. Action receipts are idempotent and rechecked under current access.
- Ingestion writes validated batches outside the publication lock, then publishes a generation pointer atomically. Same-schema refreshes can cross concurrent edits; competing source generations and incompatible schemas conflict.
- The internal Iceberg indexer resolves committed catalog snapshots, streams rows with backpressure, records provenance, binds a persisted worker fence and rechecks access before publication. Supervision renews leases, cancels lost workers and applies a cooperative 30-minute deadline. Markings persist into serving and cannot be removed by a later refresh.
- `/api/v1` is the canonical serving prefix; `/v1` remains an equivalent alias. Error responses distinguish contention and timeouts, redact database diagnostics, and include correlation/retry metadata. Malformed UTF-8 and ambiguous projection parameters are rejected.

## Stack decision and measurements

Retain TypeScript/Node for the integrated API/compiler/CLI and planned SDK/React interfaces, PostgreSQL for ontology serving, and Python/native data engines with Iceberg for datasets. Rust is a measured optimization candidate, not a production migration.

The comparable Rust/Node HTTP kernel experiment completed 498,001 requests with zero failures. At 10m objects/100 callers, Rust achieved 3,690 versus Node 2,924 req/s, with approximately 12 versus 317 MiB app memory. These simplified handlers do not implement the complete Moria security/versioning contract. See [technology decision](ONTOLOGY-TECHNOLOGY.md).

The latest integrated HTTP run includes persisted markings, region restrictions, hidden properties and concurrent actions: 79,626 successful requests, zero failures, no request over one second. Each phase lasted 30 seconds; mix was 70% pages of up to 100 objects, 20% point reads and 10% actions.

| Population | Callers | Query p95 | Action p95 |
|---|---:|---:|---:|
| 1m | 20 | 37.29 ms | 31.01 ms |
| 1m | 100 | 165.18 ms | 160.29 ms |
| 10m | 20 | 38.34 ms | 32.53 ms |
| 10m | 100 | 167.94 ms | 163.57 ms |

10m initial staging took 197.77 seconds, publication including acquisition/commit 3.40 ms, peak Node memory 292.2 MiB including the load generator. Hardware: Ryzen 7 9800X3D, 16 logical CPUs, about 31.6 GiB RAM, Windows/Node 24.19/PostgreSQL 18.4, 20 DB connections.

Earlier integrated concurrent-refresh testing completed 538,269 requests at 10m with zero failures: query p95 46.64/178.16 ms and action p95 40.09/173.66 ms at 20/100 callers. Refresh staging took 254/659 seconds and publication including wait 36/111 ms. This predates the later persisted-marking checks. The updated marking-aware refresh harness passed a separate 1,000-object smoke; that does not replace the earlier scale measurement.

Limits: narrow synthetic rows, warm early-page traversal, short mixed runs, generator and server share a process, PostgreSQL memory excluded. Latest scale runs have no concurrent ingestion. Wide rows, longer history, independent load generation, arrival-rate saturation, storage failure and million-scale Iceberg throughput remain unverified. OPS-03 is not complete. See [performance evidence](PERFORMANCE.md) and raw JSON/source-hash manifests under `docs/benchmarks`.

## Verification and reproduction

Latest code passed all 58 tests on Windows and non-root Linux with external networking disabled, no skips. The suite uses real PostgreSQL and Iceberg and covers publication rollback/crash boundaries, revocation, stale fences, stream cancellation, cursor consistency, marking downgrade, action replay, timeout/error handling and cross-prefix idempotency. Latest Linux test image: `8b68fd5069d6`.

From the repository root:

```sh
pnpm check
docker build -f deploy/test/Dockerfile -t moria-test:local .
docker run --rm --network none moria-test:local
pnpm exec tsx scripts/benchmark-staging.ts --rows 1000000 --http --seconds 30 --output .local/review-1m.json
pnpm exec tsx scripts/benchmark-staging.ts --rows 10000000 --http --seconds 30 --output .local/review-10m.json
```

On this Windows host, Docker commands run through `wsl -d Ubuntu-24.04 -u root -- docker ...`. Real Python tests require the existing repository virtual environment. Benchmarks retain local database files; reserve disk space and do not run them concurrently with other load tests.

## Commit map

| Commits | Work |
|---|---|
| `5d26eb9`, `567494b` | Language comparisons, including Rust |
| `5aba194`, `239f04a` | Streaming staging and policy-aware pinned pagination |
| `e4ade80`, `ef0a592` | Reader/writer admission, planner statistics, indexed refresh reads |
| `a12df91` | Pinned Iceberg streaming and 10m concurrent-refresh evidence |
| `107433a`, `d59b144` | Persisted fences and authorized catalog indexing |
| `70f162b`, `c5d2548` | Lease supervision, cancellation, deadlines |
| `4ef9c60`, `0243899` | Persisted markings and serving remeasurement |
| `25e3b83`, `09c7888`, `24e1717` | Error handling, strict HTTP inputs, canonical routes |

Earlier foundations and accepted specs remain in the preceding commits. Full checkpoint history is in [IMPLEMENTATION.md](IMPLEMENTATION.md).

## Next work and review gates

1. Release-owned indexing/serving bindings, including restricted-view canonical row/property policy integration. Restricted views remain legal object sources and terminal transform inputs; the current indexer supports ordinary datasets only. Legacy generations need authoritative marking backfill.
2. Durable job/attempt records, crash recovery and input-pin retention. Current supervision is process-local; database operations do not abort immediately with an AbortSignal. Storage sandboxing, table identity/durability checks, streaming writers and safe GC remain incomplete.
3. Generated OpenAPI/SDK and React demonstrator, human OIDC/group synchronization, deployment ownership and audit coverage. No management API or production listener exists.
4. Broader ontology capabilities: sorting, aggregation, links, action-created objects, multi-object actions and outbox.
5. Wider/longer performance testing and backup/restore/failure injection. Do not promote benchmark handlers or test-container isolation into production claims.

Review should focus first on the remaining scope and the desired first end-to-end user workflow. The current backend is a foundation for that workflow, not a finished product.

Report finalized at the requested 08:00 America/Denver stopping point on October 15. Latest code checks remain 58/58 on both platforms; this report-only change did not require another test run. The overnight heartbeat is being paused for review. No Moria benchmark/test process or Docker container remains running.
