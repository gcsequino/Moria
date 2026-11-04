# One-million-object native data and protected HTTP baseline

Exploratory local release trial, not production capacity or SLO certification. All exact data-oracle checks passed. Higher offered HTTP rates exceeded admission capacity.

Source: 1,000,000 rows × 4,096 logical bytes, 249 Parquet objects, 4,019,642,204 physical data bytes. The complete source/effective-object/UID oracle matched source sums and XORs. Typed projection size: 11,666,595,840 bytes.

Source publication: 747.035 seconds. Native indexing and projection: 1,662.263 seconds. Streaming oracle: 90.802 seconds. Total through HTTP and plan samples: 2,694.556 seconds (44m55s).

| Offered rate | Scheduled | Successful | Admission rejections | All-arrival p95 |
| --- | ---: | ---: | ---: | ---: |
| 20/s | 600 | 600 | 0 | 367.8 ms |
| 50/s | 1500 | 963 | 537 | 2670.2 ms |
| 100/s | 3000 | 985 | 2015 | 2410.2 ms |

Latency percentiles in the raw driver include rejected arrivals; they are not successful-request-only percentiles. Every rejected request returned RESOURCE_LIMIT/429. Arrival files preserve the full deterministic offered workload. The complete population remains eligible for page and filter/sort queries; aggregate queries use an explicit one-UID scope. The mixed load, response limits, arrival deadlines, authorization, selectivity and generator limitations are recorded in population-1000000/http-baseline.json.

Successful-only percentiles were subsequently derived from each unchanged
`http-{rate}-arrivals.json` sample list, filtering `successful=true` and sorting
`completedMs-scheduledMs` with nearest-rank `ceil(n*p)`. Rejections remain in the
scheduled denominator above; this calculation changes no original samples.

| Offered rate | Successful p50 | Successful p95 | Successful p99 |
| --- | ---: | ---: | ---: |
| 20/s | 58.691 ms | 367.782 ms | 492.262 ms |
| 50/s | 1934.971 ms | 3136.814 ms | 3532.511 ms |
| 100/s | 2004.671 ms | 2706.541 ms | 2879.884 ms |

The foreground trial held the host's exclusive native-proof resource lock. Container limits: four CPUs, 12 GiB RAM, 24 GiB RAM plus swap. Sampled cgroup peak: 12885565440 bytes; maximum sampled current memory: 12884897792 bytes. This includes file cache and kernel memory. Maximum individual sampled process RSS: 214478848 bytes; these are not summed. Final cgroup memory events recorded no OOM or OOM kill. Raw memory, CPU ticks, process IO and PostgreSQL counters are retained. PostgreSQL track_functions was disabled, so no per-function attribution is claimed.

Immutable source, external driver, executable digests, schema/source fingerprints and commands are recorded in manifest.json. Diagnostic filenames are labels; timestamps inside diagnostic JSON are authoritative (the first diagnostics-20261031T0812.json was actually captured at 08:25:53 UTC). This executable predates the subsequent per-file Parquet header-window fix and restore requalification work.

The trial has one source generation, full visibility, no preloaded overlay history and short windows. It does not establish production scale, restricted visibility, relation fanout, steady-state recovery, repetitions, soak or ten-million-object capacity. Prior failed and completed trials remain preserved separately.
