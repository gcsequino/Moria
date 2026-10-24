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

## Production continuation accepted October 24, 2026

Revision 0.2 reconciliation. D01-D14 retain their first-slice semantics. D02, D11, and D12 describe initial implementation/deployment choices rather than permanent limits. Accepted production direction includes portable cloud/on-premises operation, independently scalable services and processing, tens of millions of objects, a large-bank ServiceNow-instance class of transactional workload, and separately scaled potentially petabyte-scale ingestion/analytics. No universal ServiceNow transaction rate is assumed.

| Continuation | Disposition and proof gate |
|---|---|
| D15: deployment and engines | Linux containers and Kubernetes/Helm for production; smaller profiles remain possible. PostgreSQL, Iceberg, and bounded DuckDB remain initial foundations; Spark/Trino provide the distributed processing/analytics direction, with Kafka/Flink only for continuous-streaming needs. Exact versions and optional engines require conformance evidence. |
| D16: authority | C04 remains one PostgreSQL authority transaction across all promised catalog pointers, output heads, lineage, and checkpoints. Per-table commits cannot replace it. C06 retains atomic edits/audit/outbox/receipts. |
| D17: storage trust | S07 prohibits raw warehouse/control-DB access for user workers. Controlled trusted engines may use scoped direct storage only after current-authorization/isolation proof. Arbitrary code sharing credentials is not isolated. Restricted views remain terminal. |
| D18: cells | Initially keep a whole environment in one cell. Preserve installation-wide canonical identity resolution and reservations. Fenced migration, security freshness during partitions, and UID ownership require proof before M7 scale-out. Cross-cell transactions are not promised. |
| D19: numerical targets | Production workload rates, freshness, availability, recovery, and budgets remain calibration assumptions until explicitly accepted and evidenced. Population size alone is not throughput. |

The [acceptance mapping](../production/ACCEPTANCE.md) identifies existing obligations and unresolved proof owners. Routine mechanism choices belong to accountable implementation roles. Changes to permanent identity, authorization, or transaction semantics require explicit contract review. This continuation is design direction, not implementation or capacity certification.

## Explicit initial limitations

- Scalar values plus one-dimensional scalar arrays; no geospatial/media/time-series-specialized APIs yet.
- Static key sets and dynamic query sets are included; visual set editors are not.
- Batch connectors and writeback test doubles are implemented in the first slice. Streaming capability interfaces exist, but unsupported start/stop operations fail explicitly until implemented.
- Cross-project resource dependencies, distributed transactions, arbitrary action predicate code inside transactions, unattended agent resume, and cross-installation identity federation are deferred.
- Model references identify configured endpoints/revisions; an unversioned upstream model is recorded as mutable, not reproducible.

These limitations narrow the first implementation, not the long-term Foundry-alternative objective. Changes to permanent UID or security semantics require explicit review.
