# Moria — decisions and review follow-up

User review: all unchallenged defaults are accepted. D02 now adopts Iceberg as requested. D14 records Moria and the accepted Apache-2.0 default. D03 is finalized: restricted views are terminal data-consumption resources and cannot be transform inputs; marking-union/declassification rules remain unchanged. The new Iceberg integration details are an implementation design to validate in Phase 0, not a tested guarantee.

| ID | Accepted default / explicit follow-up | Consequence / alternative |
|---|---|---|
| D01 | Installation-wide opaque object UIDs allocated by a durable registry; environment state is separate. Transform authors supply explicit canonical identity keys. | Registry allocates and remembers identities, but does not match entities. Stable source keys can be canonical keys only if never recycled. Multiple independently writing restored installations are not supported as one identity authority. |
| D02 | PostgreSQL for catalog, jobs, security, operational object history and transactions; Apache Iceberg tables with Parquet data files for datasets; isolated DuckDB/Python transforms. | Standard Iceberg snapshots/manifests replace bespoke table manifests. Moria must coordinate catalog pointers, output heads, lineage and checkpoints atomically; Phase 0 verifies this integration. Distributed compute and large operational search remain later work. |
| D03 | **Accepted:** markings union through derivation; all attached markings must be satisfied. Restricted views are terminal data-consumption resources and cannot be used as transform inputs. | They remain queryable and can back Ontology object types. Further transforms must use separately authorized backing datasets. Automatic restricted-view policy propagation through transforms is out of scope. |
| D04 | Property restrictions omit denied fields; filtering/sorting/aggregation on caller-inaccessible properties fails. No configurable masking in v0.1. | Avoids treating redaction as null or leaking fields through predicates. Rich masking can be added later. |
| D05 | Current authorization applies at every protected operation. Security epoch changes invalidate existing pagination cursors. | Restart pagination after security changes. Code rollback never rolls back grants or markings. In-flight work already authorized can finish its bounded operation; bytes already delivered cannot be recalled. |
| D06 | Queries pin a logical environment data revision for 15 minutes. Initial SQL serving is strongly consistent with action commits. | Repeatable pagination needs version history. Exact time-travel and unlimited historical query access are not exposed. |
| D07 | Source disappearance preserves identity/edits; same UID restores them (accepted). Action deletion is a separate tombstone requiring an explicit restore action. | A source refresh cannot undo an intentional application deletion. Purge removes content only through a separate administrative procedure; UID reservations remain. |
| D08 | Dataset output heads use compare-and-swap. A competing build finishing late conflicts rather than silently replacing newer data. | Rebuild against fresh heads; no automatic rebase of data changes. Snapshot replacement is the initial transform mode. |
| D09 | Declarative actions commit under serializable isolation; user functions can propose edits outside the transaction, with mandatory read-version validation at commit. | No arbitrary user code or network calls inside a database transaction. Complex predicates initially require declarative conditions. |
| D10 | Idempotency scope includes stable action ID, not revision. Store result for 30 days, then retain a non-reusable key tombstone. | Retries cannot repeat effects after deployment or retention expiry. Old-result expiry returns an error rather than executing again. Storage cost grows with unique request keys. |
| D11 | TypeScript platform/CLI/OSDK; Python transforms/connectors; LangGraph.js first agent adapter; pgvector retrieval. | Reuse familiar runtimes and OSS engines; framework-neutral adapter contract remains mandatory. Hosted LangSmith is not required. |
| D12 | Linux x86-64, Docker Engine plus Compose installation; one trusted organization. No mandatory hosted provider. | No HA promise. Suggested first benchmark host: 8 CPU cores, 32 GiB RAM, SSD; this is a test target, not measured capacity or a minimum requirement. |
| D13 | Exact filtered vector search first; no approximate retrieval claims. Text fixtures with deterministic chunking first. | Slower at scale, but predictable authorization and recall. ANN indexing is gated on filtered-retrieval benchmarks later. |
| D14 | Project name: **Moria**. Original platform code: **Apache-2.0** (accepted default). | Name rationale: a dwarven civilization of foundries; digging too deeply and greedily risks awakening a Balrog. Dependency and model-weight licenses remain separate. Add the license/notice files when creating the implementation repository. |

## Explicit initial limitations

- Scalar values plus one-dimensional scalar arrays; no geospatial/media/time-series-specialized APIs yet.
- Static key sets and dynamic query sets are included; visual set editors are not.
- Batch connectors and writeback test doubles are implemented in the first slice. Streaming capability interfaces exist, but unsupported start/stop operations fail explicitly until implemented.
- Cross-project resource dependencies, distributed transactions, arbitrary action predicate code inside transactions, unattended agent resume, and cross-installation identity federation are deferred.
- Model references identify configured endpoints/revisions; an unversioned upstream model is recorded as mutable, not reproducible.

These limitations narrow the first implementation, not the long-term Foundry-alternative objective. Changes to permanent UID or security semantics require explicit review.
