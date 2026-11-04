# One million objects: release functional trial

The actual pipeline completed on one million objects with 4,096 logical bytes per
row: native permanent UID allocation, bounded PyArrow/PyIceberg source production,
native registered Sync publication, two native index workers, typed projection
activation and a streaming complete UID/source/effective checksum oracle.

This is functional evidence. It does not certify mixed protected HTTP latency,
peak throughput, a ten-million-object profile, wider-than-RAM behavior or a soak.
The trial predates the measured reducer build-prefix correction and the later
within-SQL Dataset guard factoring; subsequent comparisons must identify their
own exact immutable source and binary.

| Phase | Elapsed seconds |
| --- | ---: |
| Permanent UID allocation | 72.566 |
| Source production | 22.743 |
| Native publication | 847.492 |
| Native indexing and projection | 642.776 |
| Complete streaming oracle | 84.881 |
| Whole trial | 1,671.484 |

The producer stored 4,019,642,204 physical Parquet bytes across 249 retained
objects. The activated projection occupied 11,666,636,800 bytes. The source digest
was `093e0de3767ba7ffc0eb0010772a9afc783f2e518104f3cce4cd4645fb2d8cb4`.
The oracle compared every UID/domain/key, the canonical source and effective
values, count, complete checksum, modular checksum sum and checksum XOR.

The first actual reducer page returned 1,000 rows in 847.314 ms with 170,496 shared
buffer hits and 4,332 reads. Its task-first nested scan motivated the subsequent
build-prefixed staging index; these raw plans describe the earlier implementation.
Typical partitions required 63 bounded reducer pages with maximum returned JSON
of 4,111,000 bytes, below the normal eight-MiB limit.

The 311 resource samples distinguish process memory from the container cache:
maximum worker RSS was 85,012,480 bytes, catalog harness RSS 33,120,256 bytes and
one PostgreSQL backend RSS 223,682,560 bytes. The cgroup peak was 12,322,816,000
bytes, with maximum anonymous memory 472,162,304 bytes and file cache
11,887,771,648 bytes. Cgroup memory is not reported as runtime heap.

`run.txt`, `raw/`, `resources.jsonl`, `resource-summary.json`, the immutable source
manifest, compiled metadata and binary hashes retain the actual commands and
results. The fixture closed its disposable database on completion, so a later
index comparison must use a separately retained live trial rather than claim to
reuse this database.
