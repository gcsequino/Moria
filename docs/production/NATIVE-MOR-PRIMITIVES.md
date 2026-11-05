# Native MOR primitives and sidecar custody

The library provides authenticated bounded range reads, immutable graph-backed
path-copy directories, packed key/locator runs, bounded streaming merge, exact
physical row selections and verified standard Iceberg v2 bootstrap. A v3 reader
supports exact-root traversal, key lookup and selected-row scans. Standard
Iceberg files remain the data truth.

The graph schema is installed in the fresh baseline. Real coordinator bootstrap
attaches a graph sidecar to an existing retained v2 root without changing the
published Dataset head. Catalog publication and serving still select the v2
paths. Source delta remains excluded. Sparse append/upsert/delete and v3
publication are not activated.

## Graph custody

Backup traverses the actual immutable root graph through a durable bounded queue,
includes every descriptor, certificate and raw content member, and pins exact
source provider versions before copying. The fresh isolated restore requalifies
only actual copied objects against their new physical versions and installation
incarnation, then reconstructs retained graph roots and reference propagation.
Original qualifications and producer audit identities remain immutable.

Collection drains closed producer stages and committed expiration work in
bounded steps. Pending graph propagation protects the Dataset's graph objects
from candidate admission. Disabling policy A and approving B can resume A's
already committed retirement and propagation under the same Dataset, physical
fingerprint and incarnation; the original immutable expiration authority is
retained. Current B still governs deletion admission, ownership and grace.

A snapshot-local single-entry run metadata cache avoids reopening the same
immutable descriptor for every key. Cache hits still perform live root/file
checks and version-pinned provider reads. It never caches permissions or results.

## Verification

Frozen trial008 passes full static checks, exact provenance for all nine native
executables and all 91 native library tests, with no ignored or filtered cases.
Its real PostgreSQL/local versioned-S3 custody integration passes with zero
skips or cancellations (149.15-second root; 164.04-second controller wall time). The test proves two historical graph
heads and standard v2 typed rows survive restoration into a new database and
provider prefix after exact source-sidecar collection. Fresh qualifications have
new provider versions. It also proves policy succession, pending-propagation
protection, conflicting qualification denial and rejection of an already owned
restore target. Removing one actual backup graph child's exact provider version
makes its versioned HEAD return 404 and makes restore fail while dispatch remains
disabled and no new-incarnation graph qualifications exist.

The old trial006 SQL fails the same policy-successor test with
`GC_POLICY_CHANGED`. Trial007 reaches the final missing-child restore denial but
fails its expected public-error assertion; trial008 asserts the exact public
`INDEXING_FAILED` result and actual missing-version/disabled-state evidence.
All failed trials are preserved in [evidence](evidence/2026-11-04/native-graph-custody/summary.json).

A separate earlier trial005 passes both the full 64-row typed/array-key readback
and real revocation after warming the metadata cache. These are separate frozen
sources, not a three-test pass on trial008. The 64-row positive completed near
its unchanged 120-second root deadline; broad concurrency and scale remain open.
The initial primitives-only [evidence](evidence/2026-11-04/native-mor-primitives/summary.json)
remains a historical checkpoint before baseline installation.

## Remaining activation requirements

- Complete sparse append/upsert/delete, changed-row selections, tombstone-safe
  run maintenance and standard Iceberg position-delete publication. Preserve
  old roots, array key semantics and nullable non-key columns; reject null keys.
- Wire v3 candidate preparation into C04's existing single transaction across
  every promised output head, lineage and Connector checkpoint. Prove crash
  recovery, head conflicts, fenced retries and commit-before-ack replay.
- Complete bounded distributed projected-key bootstrap. The current bounded
  serial bootstrap reads payloads to derive row hashes and is not qualification
  of the accepted production bootstrap profile. Deferred digests must be explicit;
  no fabricated hash can stand in for verified row content.
- Complete Source delta publication with every native writer footprint acquired
  before the environment revision lock, including mapped Link uniqueness and
  current projection membership.
- Run affected native and end-to-end suites, the complete independent
  `pnpm check` gate and approved production workloads. Local MinIO and a new
  database/prefix prove the protocol slice, not independent hardware failure
  domains, capacity, availability or cost targets.
