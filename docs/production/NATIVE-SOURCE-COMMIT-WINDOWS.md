# Qualified Source commit windows

This implementation slice derives transient Dataset custody and bounded journal pages from a real accepted indexing build and its current materialized Source alias. It does not yet publish an unchanged alias or enable mutation consumption. The existing full-build path still performs publication.

## Custody and ordering

A window binds the actual build ID, immutable definition identity, Source generation/store/version, indexed Dataset basis and accepted target revision. Its contiguous coverage range protects intermediate journal result roots while bounded qualification progresses. Root-to-journal and scoped/range indexes support the reverse GC reference check. The range grants no object IO. Terminal or superseded builds close the range in their existing status transaction, removing it from the live indexes without scanning old members. A replaced or disabled indexing definition, changed Dataset physical binding or different incarnation also makes a range ineligible. A nonterminal takeover resumes exact captured members under its new fence; a terminal retry cannot revive closed custody.

Actual publication appends the journal after enqueue in the same C04 transaction. A deferred hook on the exact inserted build sees the actual journal edge before commit and also handles current-head registration. It never scans other builds. The existing shared Dataset retention admission prevents an expiry race before the range becomes visible. No new worker lease, producer, event, root qualification or accepted delegation is manufactured.

## Bounded qualification API

`runtime_source_delta_page(build, worker, fence, maximum)` accepts only the actual live coordinator claim and a maximum of 1–256 entries. It resumes the immutable journal from the persisted revision/snapshot/metadata cursor. Each returned membership page has at most 1 MiB of descriptors; it references existing immutable receipt/schema records instead of copying their potentially wide contents into another record. Backlog above a page budget remains resumable work.

Each edge must exactly match the preceding basis and actual historical result, publication receipt and execution/candidate or maintenance origin. Dataset binding revision, typed schema hash, key hash and ordered primary keys must agree throughout. Receiver root qualification comes from the real current physical provider/incarnation, using the historical namespace identity for an older committed edge. Normalized native typed schema hashes remain distinct from the Source store composite schema hash; raw Iceberg JSON spelling is not compared.

A real replacement boundary, journal gap, incompatible profile, unsupported equivalence or unavailable receiver root selects explicit bootstrap. It never silently drops a missing interval. Qualified metadata-pruning edges must retain the same snapshot and exact certified basis/result roots. Pending pages persist their cursor and exact journal membership under the existing shared Dataset retention admission. The live range is the root pin; members do not create duplicate physical pin rows on supersession. The actual native planner performs qualification under its renewable coordinator fence before continuing the existing full-build path.

## Remaining activation

The next slice must atomically reuse canonical storage and typed projections for a genuinely unchanged chain while recording an exact new Dataset watermark/Source alias and preserving historical readers and overlays. Mutation footprint reads, coalescing, changed-key publication, complete writer lock order, exact Search change pages and independent replay/recovery remain gated. No incremental alias, target watermark or capacity result follows from custody qualification alone.
