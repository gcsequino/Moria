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

The new run descriptor is version 3 with the eight-byte `MORIAKR3` header.
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
wide payload columns excluded from key decoding, separately measured migration/authentication I/O, wrong physical locators, arrays and
nullable non-key columns, cross-task duplicates and missing reports, unchanged
upserts, actual standard-engine position-delete readback, old-reader retention,
crash/conflict/replay and clean-host graph recovery. Production activation waits
for these proofs and the approved workload ladder.

## Format checkpoint

The explicit version-three codec and optional Rust locator digest are
implemented. Frozen trial010 builds all nine native executables and passes
all 96 library units with no ignored or filtered tests. Known zero digests,
deferred digests and deletion remain distinct, new tags are rejected under
version two, and original known version-two records remain readable. This is
[codec evidence](evidence/2026-11-04/native-deferred-digests/summary.json), not
qualification of projected bootstrap or sparse publication. At trial010 bootstrap
still derived verified hashes from payloads. The following checkpoint adds
projected derivation and a touched-row reader; distributed task receipts remain
open.

## Projected-key and touched-row checkpoint

The serial keyed derivation now projects only declared key columns and stores
explicit deferred digests in its spill and runs. An exact previously qualified
retained v2 root can reuse its member/version/schema proof through the existing
verified-spool and retained-subset verifier; the current real coordinator,
expected head, physical fingerprint and incarnation must match every receipt.
Changed versions, missing membership and current-authority failures do not
trigger a trust fallback. Unsupported legacy version tokens keep the original
full byte and typed validation path.

`NativeSnapshot::read_key_row(&Arc<Self>, values)` resolves the actual pinned
index and returns an optional complete typed value and locator with a verified
digest. It accepts no caller-supplied locator. It checks exact file membership,
physical ordinal, data sequence and live row selection, reads the chosen Parquet
row group through version-pinned graph IO with the existing footer/page
allocation guards, verifies the complete schema and full encoded key, checks any
prior verified digest, and rechecks current authority before returning.

Frozen trial013 passes static checks, exact provenance for nine native artifacts,
96 units and the actual 64-row array-key/nullable-payload graph integration under
its unchanged 120-second deadline. Every bootstrap locator is explicitly deferred;
the first actual point read matches the full typed oracle and returns a verified
digest. [Evidence](evidence/2026-11-04/native-projected-keys/summary.json) retains
the failed projection-column guard trial and the successful focused gate. That earlier checkpoint did not qualify the new loader negatives or latest-format
authority/recovery. Those proofs are recorded separately below; distributed tasks,
provider amplification and sparse publication remain open.

Initial migration cost is separate: a v2 root without authenticated chunk
receipts still needs explicit one-time byte authentication to create persisted
exact-version certificates. Key-only decoding does not imply that those bytes
were never read. Sparse mutations must reuse those persisted qualifications and
must not rehash or redecode the complete population per mutation.

## Qualified loader version binding checkpoint

Retained-root reuse now also verifies the stored qualification row's sealed
state, reader profile, environment, Dataset and snapshot against the actual
current expected head. Every physical fingerprint and producer incarnation
still matches its original exact member receipt. A synthetic profile mismatch
was accepted before this check and is now rejected with immutable guards enabled.

Root wrappers and metadata pages use explicit receipt-version GETs before full
SHA-256, canonical encoding and structural validation. An actual regression
trace reproduced the prior defect: after the old qualified wrapper version was
deleted, the loader read a newer current copy and accepted it. The new loader
requests the old version explicitly. A newer current copy is valid only while
the old qualified version remains readable; actual missing old root/page
versions fail closed without changing the Dataset head or publishing a graph.
Only unsupported immutable-version formats use the original full-validation
legacy path. Missing qualified bytes or invalid receipts never authorize that
fallback.

Frozen trial015 passes static checks, exact nine-artifact provenance and all 99
native units. Four separate sequential integration files pass 12 cases with no
skips or cancellations: four actual root/page version traces, six qualification
negatives, warmed current-authority revocation and the full graph custody proof.
The latter preserves exact-version GC, original pending-retirement authority
across policy succession, independent clean-host typed recovery and disabled
dispatch after a missing backup child. Every controller is absent at completion.
The point-verifier units reject wrong file, ordinal, sequence, key, digest and
schema; an actual installed Parquet writer/reader unit selects a row from its
second row group. This is not a qualified large multi-row-group Dataset proof.
[Evidence](evidence/2026-11-04/native-qualified-loader/summary.json) preserves
the failing stored-profile and wrapper-version traces as separate trials, plus
the earlier insufficiently specific member-version expectation. The earlier
64-row array-key/nullable-payload positive belongs to trial013 and is not counted
as another same-source trial015 result.

Distributed bootstrap, sparse writes and C04 publication, changed-row DatasetCommitDelta
production, the independent standard position-delete reader oracle, legacy
fallback integration, full independent CI and production workload certification
remain open.

## Dataset publication journal checkpoint

Actual replacement and certified metadata-pruning publications now append
immutable, contiguous Dataset edges in their existing publication transactions.
The record binds exact qualified basis/result roots, accepted normalized schema
and key identity, immutable binding revision, actual candidate or maintenance-run
UUID and sealed receipt. Metadata pruning uses an explicit unchanged reason,
verified logical equivalence and a null write mode; it fabricates no mutation.
Frozen trial021 passes all nine native artifacts, 102 units, static checks and
two real publication/rollback cases.
[Evidence](evidence/2026-11-04/native-dataset-commit-journal/summary.json) records
the exact scope. Sparse changed-row/delete-key roots, standard position-delete
readback, actual distributed bootstrap and indexing-window custody/consumption
remain unactivated.

## Qualified graph scan task contract

A graph source task remains an indexing `scan` task and uses a closed version-one
plan with `readerProfile=iceberg-v2-native-mor-bounded-v3`. Its `file` witness binds
the exact logical name, sealed immutable content, physical row count, selected
row count and data sequence. The witness grants no IO. Each task reopens the
actual immutable indexing-build root, checks its stored catalog/reader profile
and current namespace binding, resolves the file's qualified selection through
that root, and compares every witness field before scanning only that file.
Physical ordinals advance before visibility filtering. Row counts use selected
rows; physical file and row counts remain separate.

An explicit `file: null` task is valid only when that actual qualified root has
zero data files and zero visible rows. Missing wire bytes or an absent file field
never establish an empty source. Graph plans cannot select the flat reader, and
flat plans retain their existing native FileScanTask wire ABI. Both paths feed
the same canonical Source mapping, UID derivation, payload budgets, staged rows
and sealed-task pipeline. Coordinator planning streams bounded per-file witnesses
and validates complete root file/visible-row counts at EOF before dispatch.

This reader bridge does not create a v3 Dataset head or qualify a v3 writer.
Actual v3 root preparation, standard metadata/index agreement, stage validation,
atomic publication, recovery/pruning and sparse position-delete production remain
separate required integration gates.

## Staged v3 replacement protocol

The native v3 replacement task emits version-three partsets. Its actual sealed
task output is the registered graph partset node itself. Bounded file and page
directories bind file certificates and all page descriptors; page references
retain their existing ordinal/previous checks without linear graph-height
growth. Coordinator admission derives the task, owner and fence from the actual
sealed execution report and resolves every selected file/page through that
registered task root. Imported nodes, paths, counts and versions grant nothing.

Whole replacement validates newly written typed rows in bounded batches, checks
complete ordered-key uniqueness in private spill and builds exact physical
locators, row selections and canonical row digests. It shares standard Iceberg
manifest and metadata generation with the existing replacement writer. An
index-summary node excludes metadata/file-tree roots; the same canonical sealed
summary reference is embedded in the current standard snapshot summary and
the outer graph descriptor. This removes a hash cycle.

A ready graph stage supplies the private verification view. Its identity carries
the actual stage and expected-head basis revision with no fabricated published
revision. Registration retains one root member; graph edges and exact receiver
qualifications retain descendants. Final validation enumerates
the actual current standard manifests, rejects deletes in this whole-replacement
profile and checks exact file/row counts, data sequences and full selections.
Candidate insertion and stage attachment share one transaction; existing C04
head/checkpoint/completion/journal publication remains atomic.

The native service entry is staged for qualification. Existing production output
selection is unchanged. Trial030 builds nine executables and passes static plus
113 units; no actual v3 Catalog publication or recovery proof is claimed at this
checkpoint. Runtime gates, standard-engine position deletes, sparse mutations,
distributed bootstrap and incremental Source chains remain required.
