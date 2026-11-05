# Native MOR primitives

This checkpoint provides authenticated bounded range reads, immutable graph-backed
path-copy directories, packed key/locator runs, bounded streaming merge, exact
physical row-selection bitmaps, and a bounded bootstrap from verified standard
Iceberg v2 files. A v3 reader supports exact-root traversal, key lookup and
single-pass selected-row scans. Standard Iceberg files remain the data truth.

The graph custody and Source delta schemas are excluded from the installed
baseline. Catalog publication, serving and recovery continue to select the
existing v2 paths. These primitives do not enable production sparse writes.

## Verification

The library passes `cargo check --locked --lib -j2` and all 91 native library
unit tests, including 14 tests for the new primitives, with no ignored or
filtered tests. An isolated real PostgreSQL check installs the graph schema and
rejects unissued read and reuse claims. Tests run without external networking;
unit tests run as a non-root user. Raw results and exact source hashes are in
[evidence](evidence/2026-11-04/native-mor-primitives/summary.json).

These checks establish library behavior and schema admission only. They do not
qualify v3 publication, clean-host recovery, or production capacity.

## Activation requirements

- Complete the sparse append/upsert/delete writer, changed-row selections, key
  run maintenance and standard Iceberg position-delete publication. Preserve
  old roots and exact key semantics, including arrays and rejected null keys.
- Wire v3 candidate preparation into C04's existing single transaction across
  all output heads, lineage and Connector checkpoints. Qualify crash recovery,
  head conflicts, fenced retries and commit-before-ack replay.
- Include every graph descriptor, certificate and data object in retention,
  backup and restore custody. Requalify exact physical versions under the new
  installation incarnation before C08 enables reads or dispatch. Prove that old
  readers, jobs and backup pins protect every required object through collection.
- Complete Source delta publication with the full native writer footprint
  acquired before the environment revision lock, including mapped Link
  uniqueness and current projection membership.
- Run the affected native and end-to-end suites, the complete independent
  `pnpm check` gate and the approved production workload acceptance profile
  before claiming activation or achieved capacity.
