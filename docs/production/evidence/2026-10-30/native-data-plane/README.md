# Native data-plane functional evidence

These are local real-engine/authority integration results. They do not certify
the 1m/10m profiles, throughput, multi-host failover, retention or restore. Earlier
trials below were captured in the task's tool transcript; separate raw files were
not retained. The new takeover trial has the actual captured TAP file.

## Recorded takeover trial

- Raw output: [n11-takeover.tap.txt](n11-takeover.tap.txt).
- Result: 8 tests passed, 0 failed/cancelled/skipped, 27,929.375155 ms.
- PostgreSQL schema contract: version 5,
  `06b5bcd7209e4dda2ca2b4ff688d158a68e0cabf3e57ab962e335cfe769b4fd6`.
- Exact native harness SHA-256:
  `f2c1669add8a74d29c6ac2b2def829131fbbfacc3f8834f7e9c4f79df0348d46`.
- Harness artifact:
  `/var/lib/docker/volumes/moria-indexing-target/_data/debug/deps/native_catalog-6893d4754de6d6d0`.
- Linux checkout: `/tmp/moria-reconcile-20261029`.
- Toolchain/container: `moria-indexing-toolchain:local`, Linux Rust 1.99;
  Node fixture container `moria-test:rust-indexing`, Node 24.22.0.
- Real PostgreSQL with nonowner runtime role; actual pinned MinIO fixture;
  PyIceberg-produced retained metadata, Avro manifests and physical Parquet;
  two independent native worker processes. Each fixture has its own isolated
  database/storage and generated UUID namespace. No workload credentials are
  retained in evidence.
- Transfer limits: 128 MiB/object, 1 MiB chunks, 16 MiB native ranges;
  closure budget 10,000 entries / 1 GiB referenced bytes / 32 MiB private spool
  (split across inventory and physical-version recorder for the verifier).
  Native rows/batches retain their separate existing decoded-byte limits.

The test stops a real verifier process with SIGSTOP immediately after its logged
claim, expires its planner and verifier leases in PostgreSQL, then lets a second
native process replace the attempt with the next fence. Only one completed full
verification is recorded. Resuming the old process cannot change the immutable
ready attestation or duplicate activation. Other cases prove concurrent tasks
share one verification, and that cached verification does not cache permission or
accept a changed physical file version.

Exact Linux-equivalent commands (the host invoked these through WSL):

```sh
docker run --rm --user 1000:1000 \
  --mount type=bind,src=/tmp/moria-reconcile-20261029,dst=/work -w /work \
  moria-test:rust-indexing \
  /app/node_modules/.bin/tsx scripts/indexing-schema.ts --write

docker run --rm --name moria-native-takeover-build \
  --mount type=bind,src=/tmp/moria-reconcile-20261029,dst=/work \
  --mount type=volume,src=moria-indexing-cargo,dst=/usr/local/cargo \
  --mount type=volume,src=moria-indexing-target,dst=/work/services/runtime/target \
  -w /work/services/runtime moria-indexing-toolchain:local \
  cargo test --test native_catalog --no-run --locked -j 2

docker run --rm --name moria-native-takeover-proof --user 1000:1000 \
  --mount type=bind,src=/tmp/moria-reconcile-20261029,dst=/work \
  --mount type=bind,src=/var/lib/docker/volumes/moria-indexing-target/_data/debug/deps/native_catalog-6893d4754de6d6d0,dst=/usr/local/bin/moria-catalog-tests,readonly \
  -w /work moria-test:rust-indexing \
  /app/node_modules/.bin/tsx --test --test-reporter=tap \
  tests/native-snapshot-indexing.integration.test.ts

sha256sum /var/lib/docker/volumes/moria-indexing-target/_data/debug/deps/native_catalog-6893d4754de6d6d0
```

The host captured the actual final command's stdout using PowerShell `Tee-Object`
to the linked TAP file. Artifact source-digest provenance is separately enforced
by `scripts/check-indexing.ts`; this focused run records the exact executable hash
and schema admission, and does not claim the complete program gate has run.

## Earlier successful trials (transcript evidence)

| Slice                                                | Result |           Duration | Relevant boundary                                                                                                                      |
| ---------------------------------------------------- | -----: | -----------------: | -------------------------------------------------------------------------------------------------------------------------------------- |
| Initial native storage/semantic snapshot integration |  22/22 |           24.850 s | PG/MinIO/POSIX guards; 70 MiB stream; 1,024-entry durable spool; SIGKILL/reopen; real Avro/Parquet negatives                           |
| Native catalog first complete regression             |  10/10 |           22.618 s | Grouped heads/history/lineage/checkpoint/receipt/completion; conflict/revocation; real lost-ack replay                                 |
| Catalog with read-only native root/page consumer     |  11/11 |           23.222 s | Removed Dataset write grants; private scratch and actual native Arrow scan of 300 rows                                                 |
| Storage with physical deleted-history validation     |  24/24 | Combined run below | All 3 retained Parquet files / 301 physical rows and corrupted retired-file rejection                                                  |
| Catalog + storage combined                           |  35/35 |           29.727 s | Separate real DBs; no skipped cases                                                                                                    |
| Native V2 POSIX/MinIO indexing                       |    5/5 |           17.149 s | Actual 300-row source generations; provider binding ambiguity/staleness; changed content denial                                        |
| S3 multipart storage                                 |  25/25 |           33.615 s | 9 MiB multipart; irregular chunks; hidden partials; immutable completion; abort/revocation; actual Complete ack loss and full recovery |
| Direct S3 Dataset publication                        |  12/12 |           32.706 s | Source objects/root/pages written directly to MinIO; guarded atomic PG publication; read-only native Arrow scan                        |
| Durable verifier + lazy version-pinned native FileIO |    7/7 |           24.894 s | Two native workers, one verification per root; corruption and permission checks after ready                                            |
| Namespace revision nonreuse + catalog regression     |  13/13 |           31.981 s | Disable/re-enable, replacement/deletion history and immutable ledger; S3 publication retained                                          |

All reported successful trials had zero failures, cancellations and skips. Exact
older executable hashes/schema checksums were not separately captured here;
runtime schema admission still checked the compiled exact contract against each
real database. They are not represented as the new takeover artifact's digest.

## Failed and corrected trials (transcript evidence retained)

| Failure                             | What failed                                                                                   | Correction and practical implication                                                                                                                                                |
| ----------------------------------- | --------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| SQLite `SQLITE_CANTOPEN_SYMLINK`    | SQLite NOFOLLOW rejected the `/proc/self/fd/.../index.sqlite` open                            | Resolve the already-held private directory capability, compare device/inode around SQLite open, retain no-symlink/single-link checks. No path confinement was relaxed.              |
| Nested Node test scheduling timeout | A child case awaited a new child on the outer test context                                    | Bind the child context correctly. This was a fixture scheduling deadlock, not a native component result.                                                                            |
| `INCOMPATIBLE_SCHEMA`               | A shared SQL definition changed after the binary was compiled                                 | Coordinate a stable schema window, regenerate manifest and rebuild. Exact-version admission remained enforced.                                                                      |
| `EXTRA_SNAPSHOT_OBJECT`             | Catalog fixture included the separately pinned current metadata object inside its own closure | Remove that fixture-only extra entry. Native exact membership correctly rejected it.                                                                                                |
| Prepared-event timeout              | Node required JSON at column zero; libtest prefixes its first output line                     | Parse from the JSON object's actual start, as in the storage wrapper. The native commit had already passed.                                                                         |
| Proxy preparation event timeout     | The fixture routed every small preparation guard query through the commit-loss relay          | Prepare normally, then route only the real mutating publication SELECT through the relay. The actual server commit/lost response/replay is still tested; no deadline was increased. |
| Wrong indexed row-count assertion   | Fixture queried legacy `source_rows`, while native generations point to `staged_source_rows`  | Assert the actual active generation's stage rows. Native POSIX/MinIO indexing itself was already successful.                                                                        |

No failed trial was converted into a skip, relaxed byte cap or permission bypass.
Verifier attempt cleanup/retention and multi-host capacity/restore qualification
remain program obligations beyond these functional tests.
