# Million-object qualified seek comparison v10

The exact frozen 12:44:21 source/schema was built in the **release** profile. Native artifact preflight, the 1,000-row preflight, and the one-million-row exact count/sum/xor oracle passed. The complete foreground million trial exited 0 and released its exclusive resource lock. This is an exploratory short comparison; it is not full production capacity or SLO certification.

The million trial started at 2026-10-31 14:48:46 UTC and the last resource sample was at 2026-10-31T15:22:17.897Z. The HTTP windows ran from 15:20:37 through approximately 15:22:14 UTC. The measured driver imported the current frozen benchmark helper directly; its source and hashes are under `executed-driver/`. The older v9 local helper difference is preserved separately. The offered rates, 30 second windows, seed, operation mix, arrival deadlines, resource limits, and query distribution were unchanged.

| Offered requests/sec | Successful / scheduled | HTTP429 | Successful p95(ms) | Successful p99(ms) | All-arrival p95(ms) |
|---:|---:|---:|---:|---:|---:|
| 20 | 600/600 | 0 | 56.806 | 85.523 | 56.806 |
| 50 | 1500/1500 | 0 | 63.524 | 87.162 | 63.524 |
| 100 | 2962/3000 | 38 | 464.074 | 955.705 | 462.635 |

All requests completed within the original 10-second arrival deadline. There were no late HTTP200 responses. The 100/sec window still failed: 38/3,000 arrivals returned `RESOURCE_LIMIT`/HTTP429 (1.267%). Failed arrivals occurred at 29.360–29.870 seconds, and every failed arrival had exactly 64 prior client requests outstanding at dispatch. Frozen request admission was 64 and TCP connection admission was 256. The server log contains no rejection-site telemetry, so request-admission saturation is an inference rather than a proved internal rejection site. `http-outcome-analysis.json` preserves every failed operation, arrival, outcome, client concurrency count and the independently recomputed metrics.

Native preparation took **1,874.83 seconds (31m14.8s)** before the external HTTP callback. Its phases were identity 78.27s, generation 22.31s, publication 564.28s, indexing/projection 1,029.01s, exact oracle 88.01s, and operator plan samples 92.80s. The generic raw capacity result still lists the protected HTTP callback among remaining work; the actual completed callback and raw samples are separately recorded in `population-1000000/http-baseline.json` and `http-*-arrivals.json`. The original result is preserved.

Resource limits remained 4 CPUs, 12GiB memory and 24GiB memory+swap. There were 127 samples, cgroup peak 12885667840 bytes, maximum individual process RSS 215314432 bytes, and zero OOM/OOM-kill events. The memory-limit event maximum was 135414; that is recorded without assigning an unmeasured performance cause. All raw process/IO/cgroup/disk observations are retained.

The separate bounded read-only publication observer is archived with its script/hash and result. It changed no runtime settings or measured source. No EXPLAIN logging was enabled during the rate windows.

The profile remains one source generation without preloaded overlay history, one workload identity with full visibility, no browser/relation-fanout/limited-visibility mix, and no production warm-up, repetitions, failure-domain proof, recovery, soak or wider-than-memory certification. Page and filter/sort retain whole-population distributions; the aggregate remains the explicitly scoped one-UID exploratory query. The next implementation work is the coherent native MOR/bootstrap/reader/retention/recovery path, not a new million run.
