# Dataset commit deltas and incremental Source publication

This is the required v3 implementation contract. The current full replacement/full indexing path remains supported. Delta production and consumption must be receiver-qualified and tested before activation; these records do not constitute completed implementation or capacity evidence.

## Commit record

The actual Dataset head publication transaction appends one immutable `DatasetCommitDelta` per changed output. Its identity is the real candidate UUID or maintenance-run UUID, not a guest event ID. Every actual head revision advance has one contiguous edge, including an explicitly certified unchanged result; an operation that does not advance the head fabricates no edge. Grouped outputs, exact expected-head CAS, checkpoint CAS, receipt and execution completion remain atomic.

```text
DatasetCommitDelta {
  version: 1,
  commit: UUID,
  dataset: UUID,
  basis: { revision, snapshot?, metadataRoot? },
  result: { revision, snapshot, metadataRoot },
  datasetSchemaHash, datasetSchema, bindingRevision, keySchemaHash, orderedPrimaryKey,
  mode: replace | append | upsert | delete | null,
  changeKind: replace | mutation | unchanged,
  unchangedReason?: metadataPrune | compaction | noOp,
  equivalenceReceipt?,
  completeRowsRoot?, deleteKeysRoot?,
  mutationFootprint?: { version: 1, node: NodeReference },
  producer: { kind: execution, execution, candidate }
          | { kind: maintenance, run, policy },
  sealedReceipt
}
```

Roots are opaque descriptors of actual sealed, producer-owned native output. Receivers obtain exact physical qualification through Catalog; supplying an event or checksum never grants IO. Full rows are complete typed target rows. Delete frames contain exactly the declared ordered key columns. The result root, key index, standard Iceberg data/position-delete manifests and counts must agree. Replace is a bootstrap/full-rebuild boundary. A missing event, incompatible profile or absent qualified index rejects the incremental path rather than treating missing work as empty. A replacement edge uses its qualified complete result root as the full-rebuild boundary; it does not claim an incremental changed-row diff.

The key index binds the exact resulting snapshot/file visibility, schema/key profile and current key count. Standard position deletes bind exact receiver-qualified old data-file paths and physical row ordinals, with inclusive data-sequence applicability; replacement rows live in new ordinary data files and do not match those prior locators. This explicitly replaces the earlier equality-delete proposal while preserving accepted typed array primary keys. `datasetSchemaHash` is SHA-256 over the native canonical JSON of the actual Iceberg Schema descriptor stored in `datasetSchema`, matching the native task-partset schema hash. The actual table schema must equal the accepted binding after decoding both through the installed Iceberg Schema codec; default spellings such as an absent versus empty identifier-field list are normalized by that codec. The record also binds the exact current immutable binding revision, which commits to its accepted raw schema and key declaration. It is distinct from the Source store's composite object/layout schema hash. Ordered key descriptors retain the existing normalized key hash and complete key bytes.

Compaction changes physical layout while preserving logical rows; its event explicitly records that verified property, rather than asking consumers to infer it from matching counts. Metadata pruning is a different `unchanged` edge: it preserves the exact current snapshot descriptor and non-history table metadata, while a real Dataset revision advances to its qualified new metadata root. Its receipt binds both actual root keys and a digest of that preserved current snapshot descriptor. Maintenance edges have `mode: null` only with `producer.kind: maintenance`, `changeKind: unchanged`, an actual unchanged reason and equivalence receipt; they never fabricate or inherit a Dataset write mode. Execution edges keep their exact accepted write mode even for a certified no-op. A non-advancing maintenance result produces no Dataset edge. Other no-op or compaction receipts require their own actual producer proof; snapshot IDs, matching row counts or guest assertions alone are insufficient.

## Mutation footprint version one

This protocol is agreed for sparse production; activation still requires the actual writer, receiver and standard-reader proofs. A mutation uses one bounded descriptor under the result graph's fixed `mutation` edge. The outer RootDescriptor stores the identical `mutationFootprint: { version: 1, node: NodeReference }`. The immutable publication journal binds that reference to its real qualified result root. The descriptor reference has nodes=1 and logicalBytes=0, with entries equal to the total affected rows; the three rooted directories carry their own entry, node and byte bounds. A subset of changed rows or keys is never presented as a complete NativeSnapshot. The existing completeRowsRoot remains the complete result root for a replacement boundary; both legacy subset-root columns are null for mutation edges.

The footprint descriptor binds:

- Version one, table UUID, actual Dataset schema hash, normalized key-schema hash and ordered primary key.
- The accepted append, upsert or delete mode; actual basis revision, snapshot, metadata pointer and root key; predetermined result snapshot and expected basis revision plus one. The basis also binds its verified live row count. Only verified initial revision zero omits the prior snapshot, metadata and root. Revision and fence identities are canonical bigint strings.
- The real execution, coordinator owner/fence/incarnation, predetermined stage UUID and bounded directory of actual sealed task-input receipts.
- Exact inserted, replaced and deleted row counts, and optional completeRows/deleteKeys directory references. For declared keys these are also exact key counts; no implicit key is invented for a multiset Dataset.

It excludes the outer result root's own hash/key and the later allocated environment revision. The result root includes the footprint, so embedding that result root in its child would create a hash cycle. Atomic candidate publication instead binds both directions through the actual journal edge and sealed publication receipt. Input receipt summaries name actual sealed task identities and result hashes; they grant no reads of task inputs. The native coordinator verifies their accepted execution/schema/mode and actual task custody before sealing its derived output.

The fixed `completeRows` child is a persistent directory of receiver-qualified full-schema data files. Each entry binds its actual immutable file certificate, row count, data sequence and one operation, `insert` or `replacement`. The version-one producer writes separate bounded insert and replacement streams, so each file has one operation. The replacement count excludes inserted rows. A changed key whose Source mapping changes its Object UID is still a complete replacement row, so the consumer removes the prior mapped UID through its actual key map.

The fixed `deleteKeys` child is a persistent directory of qualified native key runs. Each run uses the existing version-three full-key codec and tombstone values, bound to the same key-schema hash and predetermined result sequence. Runs contain exactly the complete encoded keys actually removed from the current result. Requested missing keys and keys superseded by complete upsert replacement rows are excluded. Standard position-delete files also cover superseded old physical rows, but those physical tombstones do not turn a replacement into a logical key removal.

Absent directories mean zero corresponding rows and must agree with the descriptor's counts and actual producer receipts. Counts, node references, task declarations and guest checksums alone never qualify output. The mutation receiver verifies current full-key equality, qualified immutable old file/version membership, physical row ordinal and data sequence before admitting physical deletion, and proves the final standard snapshot, selection/index roots and footprint counts agree. Byte/row/route bounds apply before IO; large changed sets use bounded task partitions, spill and path copying.

The consumer obtains a real GraphReadContext from each accepted captured journal-window member, opens the actual result root and follows only the fixed typed mutation descendants. It verifies the reference against that result root and immutable edge, including basis/result identity, schema/key profile and producer receipt. A passed NodeReference or Locator never creates IO authority. Every intermediate real result root remains pinned until the consumer commits or releases its window; journal records create no permanent historical pin. Recovery and graph collection follow the existing qualified result-root closure, including footprint directories/files/runs and bounded receipt descriptors.

An empty append or all-missing delete completes under a certified no-effect output receipt without advancing the Dataset revision or creating a journal edge. Grouped output coverage, exact expected-head checks, checkpoint CAS, receipt and execution completion remain atomic. A mixture containing real changes publishes only the actual changed output. A no-key append can preserve Dataset multiset semantics, but a consumer requiring stable Dataset keys must explicitly bootstrap rather than invent row identities.

## Acceptance and custody

Publication qualifies the result against the current enabled binding and logical
namespace revision. Its basis comes from the actual immutable prior Catalog
revision, snapshot and metadata pointer. A retained sealed basis location may
carry its original logical namespace revision when its warehouse/table, exact
physical fingerprint and receiver incarnation still match the current binding.
An expired root or mismatched historical pointer is rejected. A provider change
requires actual receiver qualification; a journal entry does not supply it.
This preserves a valid Dataset publication when classification advances while
an older indexing purpose remains explicitly blocked. It grants no root IO and
does not relax current authority, schema/key or Source rebuild requirements.

The indexer consumes a contiguous chain beginning at its actually active Dataset revision. Every event's basis must equal the preceding result, and every receipt/root must match the actual publication journal. A bounded window may combine several contiguous commits by full Dataset key, using native spill; it may publish only the net result at the selected real target snapshot. Gaps and schema/Source mapping changes require explicit bootstrap. Arrival order, a later timestamp or the current head alone cannot prove a missing interval.

Actual pending indexing intents/builds pin every intermediate event's exact row/key roots in the derived contiguous window until their materialized PostgreSQL result and consumer watermark commit. A pin of only the latest result snapshot is insufficient when older builds have been superseded. Missing or expired chain custody requires explicit rebootstrap; journal membership alone grants no IO. Lifetime follows the current enabled indexing definition and actual pending/active work; no invented lease or caller-created pin is accepted. Current authority, namespace, schema/key profile, Source mapping/release and renewable worker fence are rechecked at bounded IO and final publication. After successful materialization, ordinary Object/Function/Search reads use retained PostgreSQL versions rather than requiring physical Dataset replay. Recovery copies live delta roots and restores their real consumer state; exact replay remains idempotent by commit and consumer identity.

## Initial key map and bounded staging

The initial full native build creates a complete Dataset-key-to-Object-UID map while reading the same verified rows as the Source mapper. Store the full encoded key and compare it after a compact hash lookup; hash equality never substitutes for key equality. The map binds the Dataset key/schema hash, indexing definition digest, actual Dataset revision and active Source generation. Initial count/uniqueness proofs must cover the complete input. No key map may be manufactured from a partial staging page.

For each bounded delta batch, the native mapper resolves prior UIDs through that actual map and canonical PostgreSQL state. It computes the new canonical row from complete incoming rows through the same C03 codecs and Source mapping as a full build. A changed mapped UID emits an explicit removal of the old UID and addition of the new UID. Delete keys resolve their prior UIDs without reading old wide Parquet payloads. New UID uniqueness and permanent identity reservations still apply across the complete active result; overlays and explicit Action tombstones survive Source disappearance/reappearance.

The staged result includes the real basis generation, target Dataset pin, definition digest, per-UID prior-source/edit tokens, canonical replacements/removals and key-map updates. A guest cannot submit this structure directly. Bounded rows/bytes, key width, spill, fanout and replay-window limits are admitted before work. Conflicting current source/edit tokens require re-preparation or a durable conflict; they never silently rebase accepted input.

## Short atomic publication

Provider IO and closure/delta qualification finish before a Source PostgreSQL mutation transaction begins. The actual per-source worker lease reserves a store-local `sourceVersion`; it does not reserve a future environment revision. Canonical rows use Source-version intervals and overlays retain environment-revision semantics. Immutable aliases map each published environment revision to its exact Source version, Dataset snapshot and compatible physical store/projection.

The worker uses one bounded READ COMMITTED transaction to lock exact affected UID homes in sorted order, validate new UID import/attachment and identity rules, and prepare only the changed canonical rows, key-map entries and typed projection intervals. Lease and current-authority checks during preparation use separate short connections; they must not accidentally retain environment control-row locks in the mutation transaction. No provider IO occurs inside it. MVCC keeps old current-index tuples and rows visible until the final commit; prepared Source versions never become visible through a missing or future alias.

Final publication acquires the existing environment/filesystem control admission, rechecks the actual lease/fence, exact indexed basis, current Source/schema/key/serving/layout/constraint identities and edit tokens, then allocates one environment revision. It appends the Source-generation alias, consumer watermark and change journal and commits the already-prepared row/index changes atomically. No full table is copied merely to create a generation. Canonical current/historical lookups select the exact alias Source version before existing policy, creation-precedence and attachment checks. FullSource identities pin the logical source head separately from the stable physical stage/store ID.

Every production Action/create/link/broker writer must acquire the same exact sorted UID-home locks before environment admission. Until that complete lock-order conversion is proved, Source final admission is **try-only** while UID locks are held; contention rolls the entire transaction back and follows the original bounded retry/deadline policy. It cannot wait in the reverse order. A killed or rejected preparation exposes neither partial current indexes nor a generation/watermark. Takeover may resume only the exact immutable qualified window under its new real fence. Retired versions remain subject to actual cursor/job/search/backup pins and retention floors.

The durable Search journal preserves every changed UID, in bounded pages under the same logical commit. A browser change frame may instead carry whole-resource invalidation when its UID cap would be exceeded; it must not truncate an exact UID list or force a full Search rebuild merely because the browser frame is capped. Overlay, permanent identity and explicit Action tombstone semantics remain the existing kernels' responsibility.

Source/index publication is atomic for its materialized consumer state. A Dataset commit may exist before its Object index becomes ready, as in the full path. An invalid mapping leaves the prior Object generation and consumer watermark unchanged, with a durable failed/blocked intent. A later coalesced chain or explicit qualified bootstrap may repair it; the consumer cannot silently skip the failed interval.

## Required proofs

Actual tests must compare incremental append/upsert/delete and mixed windows against an independent complete-row oracle and an actual installed standard Iceberg reader. Record reader/library versions and prove typed array-key updates, deletes, empty results and compaction; do not infer compatibility from the native accelerator. These tests include key-to-UID changes, missing deletes, duplicate/null keys, hash-collision handling, checkpoint/head conflicts, task takeover and acknowledgement loss. Held old cursors/readers, concurrent Actions, Source/model changes and revocation must preserve the previous result or deny release without a partial generation/watermark. Recovery must replay each commit exactly once after independent-copy restore. Separate scale trials measure changed rows, files, SQL/provider calls, read/write amplification, compaction, lag and protected HTTP outcomes with unchanged offered load; no certification follows from a small functional fixture.

## Publication journal checkpoint

The fresh baseline installs an immutable journal for actual managed replacement
publications and certified metadata-pruning edges. C04 records each real
candidate UUID only after the exact historical result, publication and execution
completion receipt agree in its existing transaction. Roots bind current
physical/incarnation qualification; profile capture uses the accepted typed
Iceberg Schema and current immutable binding revision. Replacement records refer
to the qualified complete result root as a full-rebuild boundary.

The actual metadata-pruning transaction records its real maintenance-run UUID,
policy and sealed publication receipt. Native verification compares the exact
current snapshot descriptor and every non-history table metadata field before
producing its equivalence receipt; counts alone cannot authorize the edge. The
stored mode is null. The native caller decodes the persisted semantics before
committing. A deferred constraint rejects managed heads without their matching
journal edge. Ordinary insert and immutable-update denials and a real injected
journal failure are covered by the focused publication gate. A failure rolls
back head, publication, execution completion and journal together.

Frozen trial021 passes static checks, provenance for all nine exact native
artifacts, 102 units and two actual PostgreSQL/MinIO integration cases with no
skips or cancellations (64.92s controller).
[Evidence](../evidence/2026-11-04/native-dataset-commit-journal/summary.json)
preserves the earlier compile, parameter, receipt-path and raw-schema failures
and the exact synthetic descriptor diagnostic. This checkpoint does not enable
mutation deltas, execution no-op or row-file compaction production, chain-derived
indexing pins/consumer IO, incremental Source publication or a live-instance
migration. Independent replacement-journal recovery is recorded below; the combined
independent gates remain required.

## Replacement journal recovery checkpoint

The full graph custody gate now also verifies the actual replacement journal
from its backup snapshot after a fresh database/provider-prefix restore. The captured
record fields, candidate/execution identity, schema/key/binding identity
and exact publication/completion receipt is preserved. Both basis/result root
references resolve through sealed current-physical/new-incarnation root/member
qualifications from the actual restore producer. Independent exact-version
HEAD/GET and SHA-256 checks read the copied wrappers at the new prefix; the old
audit versions return 404 there. New-authority typed reads recover both heads,
while old-incarnation journal producer reentry fails with `CANDIDATE_STALE`.

Frozen TS-only overlay022 reuses the exact trial021 native artifacts, passes
static checks and all 102 units, and passes the complete custody case under its
unchanged 300-second deadline (152.87s root, 154.19s controller, zero skips or
cancellations). [Evidence](../evidence/2026-11-04/native-dataset-commit-journal/recovery/summary.json).
These are restored replacement/full-bootstrap boundaries. This backup precedes
metadata pruning and does not claim recovery of that later edge. Mutation
frames, actual window pins/watermarks and incremental replay remain unactivated.
