# One-million-object protected-query trial v9

This is an exploratory single-host implementation comparison, not production capacity or SLO certification. Frozen source: `20261031T112705281601Z-protected-narrow-capacity-v9`, manifest `c89ee5ec0b2c85d8f133bc588591f0ac25ba58bb0721ab2d0cc32b985cdee356`. Source schema generation happened inside the frozen copy before compilation; the generated schema and SHA are archived. The release build passed, followed by the actual PartRoot-v2/declared-key writer proof and the 1,000-object exact/40-request protected HTTP preflight.

The million run held the Moria proof resource lock exclusively. It used the same four-CPU, 12-GiB-memory/24-GiB-memory-plus-swap container, population, 4,096-byte rows, generator/workload seeds, query scopes, offered rates, mix, 30-second windows and ten-second arrival deadlines as v8. The archived local v8 HTTP driver was reused with artifact/output path changes; it did **not** import the later live metrics helper. It did import the newly frozen projection setup and native P03/narrow-query implementation. There was no EXPLAIN capture in the rate windows or binary/limit/deadline change during the trial.

The independent oracle passed for every one of the 1,000,000 rows, including exact count, source sum and XOR. The typed projection occupied 11666579456 bytes. Identity took79.45s, generation22.45s, publication630.91s, indexing/projection1,111.68s and the oracle90.18s; the recorded preparation total was2,029.58s (33m49.6s). Publication was slower than v8's387.84s; that variance is preserved without attributing a cause.

| Offered requests/s | Successful /scheduled | RESOURCE_LIMIT429 | Successful-only p95 |
| --- | --- | --- | --- |
|20|600/600|0|127.39ms|
|50|1492/1500|8|1275.16ms|
|100|1583/3000|1417|1667.70ms|

V8 success counts under the same offered profile were600/600,995/1500 and1001/3000; its successful-only p95 values were317.97,2771.03 and2522.94ms. The higher offered rates still fail, and successful latencies remain above the production target. The original v9 all-arrival p95 values are127.39,1274.56 and1539.82ms; errors are included in those values, so they must not be presented as successful latency.

`http-successful-metrics.json` separately derives nearest-rank successful/all-arrival percentiles, dispatch delay, status counts and original-deadline classification from unchanged per-arrival records. It records every input SHA and the archived reconciliation script. The same reconciliation was applied to v8. No late-success reclassification changes these success counts. Original driver summaries and per-arrival files remain unchanged.

There are137 resource samples. Cgroup memory peak was12,885,676,032 bytes; maximum sampled current memory was12,884,848,640 bytes. Both include shared file cache and kernel memory. Maximum individual RSS was230,723,584 bytes; RSS values must not be summed. Final cgroup events:

```text
low 0
high 0
max 63184
oom 0
oom_kill 0
oom_group_kill 0
```

A bounded read-only phase diagnostic at11:56:53 captured the real verifier and cumulative database counters, without changing server settings. A later diagnostic missed workers that had already completed and exited with`CAPACITY_PROCESS_NOT_FOUND`; its failure is preserved separately and did not fail or modify the exit-zero trial. No runtime work or resource lock remains active.
