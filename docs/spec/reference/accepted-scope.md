# Accepted scope and architecture decisions

Updated October 14, 2026. These decisions supersede conflicting proposals in the initial research pack. Unmentioned inventory items remain proposed, not implicitly approved.

## Data platform ownership

Own the data platform: its dataset/catalog contracts, versioning, execution integration, lineage, authorization, and operational lifecycle. Reusing open source storage and compute engines is compatible with ownership; outsourcing the platform's semantics to a required managed service is not the target.

D3: Provide a general-purpose connector abstract class and extension contract. Ship no production system connectors initially. Future implementations can target arbitrary systems. Test fixtures and contract-test doubles are allowed without becoming bundled production connectors.

Proposed connector contract areas, subject to specification: configuration validation, secret references, connection testing, capability discovery, schema discovery, bounded reads, checkpoints, cancellation, retry classification, and lifecycle cleanup. Streaming, change capture, and writeback should be optional capability interfaces rather than mandatory methods on every connector. Checkpoint durability and retry semantics belong in the platform contract, even when a connector supplies opaque checkpoint values.

## CLI and development

Repository topology: initially develop the platform itself in one monorepo. Support user monorepos and separate administration, domain, and application repositories from the start. Repositories are not security boundaries. Applications declare scopes; policy owners grant them separately. Resources have authoritative deployment ownership. Applications pin published versioned domain contracts for SDK generation and compatibility validation.

Projects contain namespaces that group resources for ownership and policy without implicitly granting access. Frontends and agents have explicit application identities and scopes. All declarations are managed through the CLI.

P4: The CLI is the first-class way to interact with a live system, including inspection, administration, deployment, execution, and debugging. Platform management APIs support it; browser authoring must not be required.

Public execution commands and lifecycles are domain-specific: transform builds, function invocations, agent executions, evaluations/results, and ingestion syncs or continuous connections. Do not expose a universal `runs start/inspect/cancel` abstraction. Shared leases, attempts, logs, cancellation primitives, and tracing remain internal infrastructure. Cross-domain `activity list` provides discovery and points to domain inspection commands.

P11: No hosted IDEs or notebooks. Developers use their preferred local editors and push artifacts through the CLI.

P10: Host custom React frontends. Do not offer user-deployed custom API hosting. Frontends access Ontology objects and actions through the generated OSDK (P2). The platform itself still requires APIs for OSDK access and CLI operations. Server-side functions and action execution are distinct from exposing arbitrary user-defined HTTP endpoints.

Proposed frontend delivery: CLI builds or accepts a built static artifact, publishes immutable versions, and promotes a version. Establish build location, routing, authentication integration, and rollback contracts in the detailed specification.

## Shared application access

P2/A2: React applications and agents use the same generated OSDK resource and action contracts. Support different authentication flows for browser users and server-side agents without creating a privileged alternate agent data path. Generated bindings for different languages should derive from shared contracts. Exact initial languages remain undecided.

Identity, application scopes, and execution identity must be explicit for actions. Do not infer that an agent's access should bypass ordinary object or action authorization.

## Models and AI

A1: Provide a gateway for models hosted anywhere, including the user's existing PC-local model. Its protocol, endpoint, and connectivity have not been inspected in this chat. Provide extensible provider integration rather than assuming a specific vendor or protocol for every model.

Proposed gateway contract areas: model references and version metadata, capability discovery, streaming, tool calling, structured output, authentication, timeouts, errors, usage accounting, and tracing. Unsupported capabilities must be explicit. Preserve local-only deployment as an option.

Fine tuning on hosted GPUs is deferred. Reserve separable concepts for model artifacts, long-running training jobs, and compute providers. Do not build training orchestration yet. Training need not be coupled to inference request execution; later training outputs can be registered with an inference provider. Detailed artifact formats, registry responsibilities, and provider contracts remain open.

A3: All agents are code-defined; no agent authoring UI. Start with one agent SDK, not yet chosen, and allow additional open source SDKs through an adapter abstraction. Proposed common lifecycle: load versioned code/configuration, start a run, stream events, cancel, return results/errors, and report trace context and usage. Agent SDK internals should remain adapter-specific. Ontology access goes through OSDK rather than an adapter-specific data client.

A4: Include retrieval using an existing open source implementation. Select it later. Keep document/source identity, authorization, provenance, and deletion behavior visible at the platform boundary so implementation reuse does not break platform guarantees.

A5: Include evaluations. A6: Include AI execution tracing. A7: Include MCP interfaces. Exact initial MCP tools, evaluation interfaces, and event schemas remain to be specified.

A8: Defer traditional ML lifecycle tooling.

## Consequences for the initial demonstrator

Replace any visual pipeline or app authoring proposal with code plus CLI deployment. Exercise the owned dataset platform, Ontology, and actions from a custom React frontend using OSDK. Add a code-defined agent that uses the same OSDK contracts, invokes the model gateway, and produces traces and evaluation results. Demonstrate connectors through a test double until a real connector implementation is separately selected.

## Security and initial deployment

Markings, restricted views, and object/property policies are required in the first working slice. Their detailed semantics and acceptance cases must be specified before selecting storage and query engines.

The initial supported deployment is one Linux host operated by a single trusted organization. Application/user authorization still applies within that organization. Mutually untrusted tenant isolation and multi-host orchestration are outside the initial deployment target. Kubernetes is a possible later production deployment target, not a prerequisite or a settled requirement for scalability.

Accepted object identity and reintroduction: an object UID permanently identifies one logical object and is never reused for a different object, across time or source systems. Raw source keys must be mapped accordingly rather than assumed globally unique or non-recyclable. The same UID reappearing in source data reanimates the same object and restores retained edits. Different logical objects require different UIDs. UID allocation/persistence still requires detailed design; the restore-versus-reset decision for the same UID is settled.

Accepted multi-source consolidation: combine source datasets in a transform, merging/coalescing records from multiple systems into one canonical record per logical object before mapping the output dataset into the Ontology. Matching and field precedence belong in transform code, not an implicit Ontology entity-resolution layer. The canonical UID must remain stable across rebuilds and changes to contributing records.

## Decisions still needed

- Pin/test accepted TypeScript platform/OSDK, Python data runtime and LangGraph.js adapter versions.
- Validate accepted pgvector exact retrieval integration.
- Local model endpoint/protocol and gateway-to-PC connectivity.
- Concrete single-host packaging, sizing, and worker isolation mechanisms.
- Prove Iceberg catalog atomicity and the reviewed contracts; D03 is finalized.

These are future specification inputs, not reasons to delay recording the accepted scope.

## Implementation-readiness review outcome

Project name: Moria, with the dwarven foundry/Balrog naming rationale. Apache-2.0 is the accepted default for original implementation code. The user accepted all unchallenged defaults D01–D14, selected Apache Iceberg tables for datasets (Parquet remains the data-file encoding), and requested clarification of restricted-view transform preservation in D03. D03 is now finalized: restricted views are terminal data-consumption resources, cannot be used as transform inputs, and may still serve queries and back Ontology objects. Automatic row-policy propagation through transforms is out of scope. Detailed accepted defaults are in ../review-decisions.md.
