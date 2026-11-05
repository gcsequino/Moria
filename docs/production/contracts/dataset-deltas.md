# Dataset commit deltas and incremental Source publication

This is the required v3 implementation contract. The current full replacement/full indexing path remains supported. Delta production and consumption must be receiver-qualified and tested before activation; these records do not constitute completed implementation or capacity evidence.

## Commit record

The actual Dataset head publication transaction appends one immutable `DatasetCommitDelta` per changed output. Its identity is the real candidate/commit UUID, not a guest event ID. Grouped outputs, exact expected-head CAS, checkpoint CAS, receipt and execution completion remain atomic.

```text
DatasetCommitDelta {
  version: 1,
  commit: UUID,
  dataset: UUID,
  basis: { revision, snapshot?, metadataRoot? },
  result: { revision, snapshot, metadataRoot },
  schemaHash, keySchemaHash, orderedPrimaryKey,
  mode: replace | append | upsert | delete,
  completeRowsRoot?, deleteKeysRoot?,
  insertedKeys, replacedKeys, deletedKeys,
  execution, sealedReceipt
}
```

Roots are opaque descriptors of actual sealed, producer-owned native output. Receivers obtain exact physical qualification through Catalog; supplying an event or checksum never grants IO. Full rows are complete typed target rows. Delete frames contain exactly the declared ordered key columns. The result root, key index, standard Iceberg data/position-delete manifests and counts must agree. Replace is a bootstrap/full-rebuild boundary. A missing event, incompatible profile or absent qualified index rejects the incremental path rather than treating missing work as empty.

The key index binds the exact resulting snapshot/file visibility, schema/key profile and current key count. Standard position deletes bind exact receiver-qualified old data-file paths and physical row ordinals, with inclusive data-sequence applicability; replacement rows live in new ordinary data files and do not match those prior locators. This explicitly replaces the earlier equality-delete proposal while preserving accepted typed array primary keys. Compaction changes physical layout while preserving logical rows; its event explicitly records that verified property, rather than asking consumers to infer it from matching counts.

## Acceptance and custody

The indexer consumes a contiguous chain beginning at its actually active Dataset revision. Every event's basis must equal the preceding result, and every receipt/root must match the actual publication journal. A bounded window may combine several contiguous commits by full Dataset key, using native spill; it may publish only the net result at the selected real target snapshot. Gaps and schema/Source mapping changes require explicit bootstrap. Arrival order, a later timestamp or the current head alone cannot prove a missing interval.

Actual pending indexing intents/builds pin the event's exact row/key roots until their materialized PostgreSQL result and consumer watermark commit. Lifetime follows the current enabled indexing definition and actual pending/active work; no invented lease or caller-created pin is accepted. Current authority, namespace, schema/key profile, Source mapping/release and renewable worker fence are rechecked at bounded IO and final publication. After successful materialization, ordinary Object/Function/Search reads use retained PostgreSQL versions rather than requiring physical Dataset replay. Recovery copies live delta roots and restores their real consumer state; exact replay remains idempotent by commit and consumer identity.

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
