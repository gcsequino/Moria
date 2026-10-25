# Data, execution and workflow lineage

Moria's first lineage view connects persisted metadata so an operator can trace recorded dataset and object publications, inspect versions and see the work that produced them. The view does not infer causal edges from similar names or close timestamps.

## Reference model

Foundry Data Lineage is a source-to-consumer data-flow exploration tool: dataset discovery, expandable ancestors/descendants, freshness and build context, schemas and producing logic. Moria's dataset snapshots, publication receipts and index provenance form the initial foundation for that experience. [Official Data Lineage overview](https://www.palantir.com/docs/foundry/data-lineage/overview)

Foundry Workflow Lineage is broader than execution history. It maps Ontology resources and their use in applications, actions, functions, models and related components, supporting dependency inspection and change impact. Its documented capabilities include downstream property usage, resource details and version/refactor operations. Moria must keep declared workflow dependency, observed execution and data derivation distinct. A job-attempt list alone is not equivalent to Workflow Lineage. [Official Workflow Lineage overview](https://www.palantir.com/docs/foundry/workflow-lineage/overview)

## Current data and execution slice

`services/lineage/src/service.ts` reads the catalog in a repeatable-read transaction. The management endpoint supplies the administrator authorization boundary; the reader itself is an internal API, not a per-resource authorization implementation.

- Dataset versions link to committed publication receipts and exact Iceberg snapshot IDs.
- Persisted index provenance links source snapshots to staged or published object generations, with binding fingerprint and application identity.
- Durable jobs link to their recorded attempts, and to publications whose persisted execution ID matches the job ID. A Snow sync links only by its exact persisted ID.
- Weather publications link to their job ID, captured official NWS source receipts and configured area forecasts. Weather publication storage is PostgreSQL; the graph does not portray it as Iceberg.
- Source receipts expose only validated official URLs, SHA-256 fingerprints and retrieval time. Response bodies, forecast prose, arbitrary job results/configuration/errors and physical metadata locations are excluded.
- Generations without a known source relationship remain visible with a stated provenance gap. The successful attempt is not inferred from the latest attempt number: publication records currently identify the execution, not the attempt.

The query is bounded: 50 snapshots, 50 index stages, 50 object generations, 50 jobs, 200 attempts, 50 Snow syncs and 50 weather publications; each weather publication contributes at most 10 source receipts and 10 configured areas. Snapshot/generation selection is by revision, not global wall-clock recency. Source snapshots referenced by selected index records may add nodes beyond the first snapshot sample. Limitations and truncation are returned alongside the graph. This is not an exhaustive instance export.

## Declared workflow dependency slice

The paired workflow implementation uses metadata packaged in each active immutable bundled app artifact (`moria-workflow.json`): version, environment and the app's generated contract. It connects apps to declared object types, actions and endpoints. Object type identities use the same environment/resource keys as the data graph, joining the two views.

These are **declared capabilities and dependencies**. They do not prove actual runtime use, grant access, reveal property-level code usage or establish an action's write target unless that relation is explicitly recorded. An app contract can request an operation while current authority denies it. Old artifacts without workflow metadata must be described as lacking coverage, rather than assigned invented relationships. Implementation and verification status is recorded in `IMPLEMENTATION.md`.

## Next implementation stages

1. Promote the graph payload to typed relationships with evidence kind, source revision and completeness metadata. Preserve separate categories for containment, declared dependency, data derivation and execution.
2. Generate workflow metadata from the compiler IR: stable resource IDs, contract/release digests, actions' explicit target types and property reads/writes. Add downstream impact traversal before attempting automated refactors. Static declarations and observed usage must remain separately labelled.
3. Add server-paginated neighborhood expansion, environment filters, latest/historical version selection and saved views. Scope search, counts and edge endpoints to the caller's metadata permissions before allowing non-administrators into the graph.
4. Add access explanations and marking origins only when authoritative containment/lineage closure exists. The viewer must never become a substitute for the enforcement path.
5. Persist attempt-level publication attribution if precise retry-to-output tracing is needed. Do not reconstruct it from timestamps after a crash.

Acceptance checks include matching exact source revisions, no fabricated edges for missing metadata, no raw-body/path leaks, malformed/unapproved source URLs omitted, authorization revocation on HTTP reads, and bounded graph results. Real PostgreSQL tests cover the data/execution read model; app workflow artifact tests cover declared dependencies separately.
