# Native API contracts

The [component inventory](../RUST-PLATFORM-PROGRAM.md) owns scope, dependencies and
completion status. Domain documents and generated wire messages describe actual
boundaries; they do not imply an implemented handler or a passed scale gate.

- [Authorization](authorization.md): strict policy/value/grant kernel and SQL parity.
- [Identity](identity.md): stable allocation and fenced domain ownership.
- [Execution](execution.md): immutable work, DB-clock leases, scheduling and atomic completion.
- [Storage](storage.md): scoped transfer, immutable sealing and streaming closure verification.
- [Catalog](catalog.md): native closure roots and grouped publication authority.
- [Serving](serving.md): protected replicas, typed queries and transactional actions.
- [Changes](changes.md): complete transactional replay and retention pins.
- [Delivery](delivery.md): destination-scoped external attempts and reconciliation.
- [Sandbox](sandbox.md): OCI supervision, bounded protocol and durable cleanup.
- [Functions](functions.md): native public admission and protected broker composition.
- [Filesystem](filesystem.md): native metadata authority and qualified operations.
- [Models](models.md): protected inference, actual capability probes and bounded streams.

Common rules: authenticated callers, receiver-resolved immutable bindings,
versioned exact values, finite resource/deadline budgets, current authority at
release/commit, stable idempotency scope, monotonic fences, safe errors and honest
capability discovery. Cross-domain publication effects share the PostgreSQL
authority transaction; a chain of successful RPCs is not an atomic commit.

[Protected retrieval and projection](search.md) defines N12/N13 snapshot/log, chunk, model and before-ranking authorization boundaries; handler completion requires its listed evidence.

- [Directory](directory.md): current placement, transfer fences and explicit activation.
- [MCP](mcp.md): scoped discovery and protected query/action tools.
- [Engine I/O and spill](engine-io.md): bounded Arrow streams and hard attempt-owned scratch.

- [Recovery content](recovery-content.md): exact custody, multipart metadata/key verification and isolated restore.

- [Lifecycle](lifecycle.md): role probes, bounded drain and protected process metrics.

- [Operational history retention](history-retention.md): normalized pins, explicit policy, revision floors and bounded PostgreSQL collection.
- [Physical retention](retention.md): qualified exact-version custody and collection.
- [Automations](automations.md): durable firing and native domain-factory dispatch; implementation proof remains pending.
- [Connector capabilities and secrets](connectors-secrets.md): accepted capability coverage, semantic checkpoints and host-only approved secret use.

- [Native Dataset write modes](dataset-writes.md): accepted modes, ordered keys, bounded uniqueness and incremental file/index publication.

- [Dataset commit deltas](dataset-deltas.md) — qualified incremental Source and index publication.
