# Native MOR index protocol

This defines the remaining key-index and bootstrap work under C04/C08. The
existing graph-sidecar custody proof does not activate this protocol or certify
production throughput. Standard Iceberg files remain Dataset truth.

## Deletion and empty results

Compacting a subset of newest-first runs retains a key tombstone even when no
older value occurs in that subset. An untouched older level may still contain
the key. Tombstones can be discarded only by a separately qualified complete
terminal compaction that proves no older matching value remains; current merge
APIs do not claim that proof and always preserve tombstones. Full encoded key
bytes determine identity after routing; sequence ties and duplicate keys within
a run remain invalid.

Zero live rows do not imply that hash and location directories are absent.
A valid all-deleted result can retain paired tombstone directories. Positive
live rows require both directories; one directory without its counterpart is
invalid for every row count. The live-row count remains distinct from index
entry, file and physical-row counts. An empty result never authorizes omission
of retained history or deletion of its files.

## Deferred row digests

The index must provide the exact ordered full key to qualified file, physical
row ordinal and data sequence. Bootstrap does not need to decode wide payload
columns merely to compute a row digest. Each locator carries either an explicit
verified canonical full-row digest or an explicit deferred state. A hash-run
entry likewise distinguishes a deferred digest, verified digest and key deletion.
No all-zero digest, absent key or tombstone represents a deferred digest.

The next run descriptor is version 3 with the eight-byte `MORIAKR3` header.
Existing record tags remain 0 (deleted), 1 (verified hash) and 2 (locator with
verified hash). New tags are 3 (deferred hash) and 4 (locator without a hash).
The locator keeps its existing file/physical-row/data-sequence fields; tag 4
omits only the digest bytes. Hash tags are valid only in hash runs and locator
tags only in location runs. The reader also accepts version 2 with `MORIAKR2`
and its original tags, rejects unknown versions/tags, and never reinterprets old
bytes. Existing qualified v2 sidecars remain readable. A deferred locator still
binds the exact file/row/sequence/key and every existing graph qualification.
The Rust locator represents its digest as `Option<[u8; 32]>`; `None` is deferred,
while hash runs use an explicit `DeferredHash` value distinct from `Deleted`.

Before a touched row can supply a content digest or determine whether an upsert
is unchanged, the native reader must read that physical row from the exact
qualified file/version, validate the complete target schema and typed values,
recompute its full encoded ordered key and compare it with the requested key.
It then computes the canonical full-row SHA-256 digest and verifies any prior
verified digest. A wrong key, row, sequence, file membership, schema or provider
version fails. Deferred is never a reason to skip an update or claim row equality.
The proof remains bound to the pinned snapshot and current read authority; it
cannot authorize publication or later result release on its own.

## Bounded distributed bootstrap

Bootstrap is an explicit accepted execution against one pinned expected head,
not an implicit fallback inside every sparse mutation. Its immutable plan
partitions actual standard data-file row groups/ranges. Each task binds the
execution/task/fence/incarnation, full schema and ordered-key schema hashes,
expected head, exact file/version and physical row interval. Every required
interval must be covered exactly once. Tasks use projected typed key columns
and bounded spill; stable physical ordinals do not change when deletes filter
visible rows. Standard position/equality delete sequence semantics apply before
visible-key uniqueness and counts are certified.

Tasks seal bounded key/locator runs and actual row-selection receipts under
their real producer authority. The coordinator accepts only the complete set
of sealed reports from the accepted plan. Cross-task uniqueness is checked by
deterministic routing and comparison of full encoded keys in bounded spill;
hash collisions never establish equality. No per-row PostgreSQL or provider
round trip, population-sized in-memory collection or wide-column scan is part
of projected-key bootstrap.

The coordinator rechecks the expected head and current authority before
installing the receiver-qualified root. Failed, missing, overlapping, duplicate,
stale or fenced task reports cannot install a partial index. Graph admission
must be derived from actual sealed task custody and exact target membership;
a task-supplied path, arbitrary graph node or caller-provided pin grants nothing.
The current serial bootstrap has not implemented this distributed task protocol.

## Sparse publication and required proof

A touched-key mutation creates only changed data, exact standard Iceberg
position deletes for superseded physical rows, affected row-selection chunks,
new key runs and copied directory paths. It preserves unchanged qualified roots
and obeys bounded LSM read and compaction budgets. Index lookup must not silently
invent an empty table when an index is unavailable or incompatible.

Candidate validation proves the final standard Iceberg snapshot agrees with its
complete key/location mapping, row selections, counts and declared key semantics.
All promised Dataset heads, provenance, Connector checkpoint CAS and completion
still publish in C04's single PostgreSQL transaction. An unchanged Dataset result
still needs expected-head, lease and checkpoint checks before acknowledgement.

Qualification must include an all-deleted result followed by append/upsert,
partial compaction above an older key, deferred/verified digest round trips,
wide payloads excluded from bootstrap I/O, wrong physical locators, arrays and
nullable non-key columns, cross-task duplicates and missing reports, unchanged
upserts, actual standard-engine position-delete readback, old-reader retention,
crash/conflict/replay and clean-host graph recovery. Production activation waits
for these proofs and the approved workload ladder.
