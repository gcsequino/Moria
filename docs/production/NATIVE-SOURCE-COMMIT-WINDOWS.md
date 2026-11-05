# Qualified Source commit windows

An accepted native indexing build derives transient Dataset custody and bounded journal pages from its current materialized Source alias. A certified metadata-only window can advance the logical Source revision while keeping its physical rows and projections. All other supported work still follows the existing full build.

## Custody and ordering

A window binds the actual build ID, immutable definition identity, Source generation/store/version, indexed Dataset basis and accepted target revision. Its contiguous coverage range protects intermediate journal result roots while bounded qualification progresses. Root-to-journal and scoped/range indexes support the reverse GC reference check. The range grants no object IO. Terminal or superseded builds close the range in their existing status transaction, removing it from the live indexes without scanning old members. A replaced or disabled indexing definition, changed Dataset physical binding or different incarnation also makes a range ineligible. A nonterminal takeover resumes exact captured members under its new fence; a terminal retry cannot revive closed custody.

Actual publication appends the journal after enqueue in the same C04 transaction. A deferred hook on the exact inserted build sees the actual journal edge before commit and also handles current-head registration. It never scans other builds. The existing shared Dataset retention admission prevents an expiry race before the range becomes visible. No new worker lease, producer, event, root qualification or accepted delegation is manufactured.

## Bounded qualification API

`runtime_source_delta_page(build, worker, fence, maximum)` accepts only the actual live coordinator claim and a maximum of 1–256 entries. It resumes the immutable journal from the persisted revision/snapshot/metadata cursor. Each returned membership page has at most 1 MiB of descriptors; it references existing immutable receipt/schema records instead of copying their potentially wide contents into another record. Backlog above a page budget remains resumable work.

Each edge must exactly match the preceding basis and actual historical result, publication receipt and execution/candidate or maintenance origin. Dataset binding revision, typed schema hash, key hash and ordered primary keys must agree throughout. Receiver root qualification comes from the real current physical provider/incarnation, using the historical namespace identity for an older committed edge. Normalized native typed schema hashes remain distinct from the Source store composite schema hash; raw Iceberg JSON spelling is not compared.

A real replacement boundary, journal gap, incompatible profile, unsupported equivalence or unavailable receiver root selects explicit bootstrap. It never silently drops a missing interval. Qualified metadata-pruning edges must retain the same snapshot and exact certified basis/result roots. Pending pages persist their cursor and exact journal membership under the existing shared Dataset retention admission. The live range is the root pin; members do not create duplicate physical pin rows on supersession. The native planner performs qualification under its renewable coordinator fence before attempting certified unchanged publication or the existing full-build path.

## Certified unchanged publication

A fully qualified metadata-prune window can publish an unchanged Source generation. The logical environment revision and Dataset watermark advance to the actual accepted build; the physical Source store and content version remain unchanged. The immutable generation alias records both identities. Publication rechecks the latest Source basis, exact current Dataset head, active serving and projection profile, and the live coordinator fence in one transaction. It performs no Dataset reads, scan or reduce tasks, canonical writes, projection DDL, or invalidation frame. Unknown equivalence, a replaced basis, or a changed profile uses the original full build.

The typed projection keeps its original activation interval, including edits already replayed in that interval. Search resolves the new logical Dataset provenance; it may replay into an existing Search generation only when both exact Source aliases share the same physical store and content version. Existing execution and frame pins and current authority checks remain in force. A true change in either physical store or content version is not equivalent.

The focused v2/v3 gate exercised two real pruning commits per format, the existing physical store, key map and projection, a retained typed cursor, session revocation, and Action override/tombstone state. The v3 case also installed the compiled Model and used a registered Search projector with a loopback embedding provider: one existing Search generation reached the latest environment revision without sending more embedding inputs. See [the exact artifacts and results](evidence/2026-11-05/source-unchanged-publication/summary.json).

## Remaining activation

Mutation footprint reads, coalescing, changed-key publication, complete writer lock order, exact affected-UID Search pages and independent replay/recovery remain gated. The bounded fixture does not establish production capacity.
