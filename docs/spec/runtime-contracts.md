# Moria runtime and AI contracts v0.2

## R01 — shared mechanics, separate public lifecycles

All bounded executions record resource revision, release, principal/application/delegation, security context, logical execution ID, attempt ID, input refs, artifact digest and correlation ID. Workers claim time-limited leases; publication verifies the fencing token. Retrying bookkeeping does not make arbitrary side effects exactly once. Internal job records are not a public universal run API.

| Domain | States and result semantics | Retry/cancel behavior |
|---|---|---|
| Transform build | queued -> running -> publishing -> succeeded; failed/cancelled/conflicted are terminal | Retry attempts preserve pinned inputs and expected heads. Commit winning a cancel race is success. Head conflict requires a new build. |
| Bounded sync | queued -> reading -> committing -> succeeded; failed/cancelled terminal | Each committed batch and checkpoint is durable. Cancellation may leave earlier batches committed; result reports progress. Full replacement scan publishes only at completion. |
| Continuous connection | stopped -> starting -> active -> stopping -> stopped; failed | Stop finishes or aborts current batch before checkpoint report. Capability may be declared but initial runtime returns unsupported. |
| Function invocation | accepted -> running -> succeeded/failed/cancelled | Typed result/error; default synchronous wait, invocation ID always available. No automatic retry for undeclared side-effect behavior. |
| Agent execution | queued -> running -> succeeded/failed/cancelled; awaitingInput/resuming only if adapter advertises them | Default no automatic restart after worker loss; report failed with last event/checkpoint. Resume only through adapter capability, current authorization and idempotent tool calls. |
| Evaluation | queued -> running -> completed/failed/cancelled | Completed can contain failed cases; report pass/fail/error separately. A case retry is a new recorded attempt. |
| Outbox delivery | pending -> delivering -> delivered or retryScheduled -> delivering; failed | Independent from action result. Bounded retries and dead-letter inspect/retry through CLI. |

Default execution limits: transform/sync 30 minutes, function 30 seconds, agent 10 minutes, evaluation 60 minutes; declarations can request lower limits, administrators cap increases. Cancellation gets 10 seconds for cleanup before worker termination. Artifact commits remain atomic. Timeout waiting in CLI does not cancel work. Events have execution-local monotonic sequence numbers; follow supports a resume cursor, with explicit expiry if event retention has elapsed. Default event/result retention 30 days, subject to labels and audit rules.

## R02 — connectors and artifact ABI

Python connector base class exposes `describe()`, `validate(config)`, `test(context)` and `close()`. `describe` returns ABI version, configuration JSON Schema and capabilities. Optional capability interfaces: `read_batch(context, checkpoint)`, schema discovery, continuous read, and `write_batch(context, delivery)`. Batch read yields typed data batches and checkpoint candidates; only platform commit acknowledgments make a checkpoint durable. Connector contexts expose scoped secrets, cancellation, source egress, logging and output staging, never catalog database credentials.

Declare replay support, ordering/event keys, checkpoint format/version and completeness mode. Errors classify invalid configuration, unauthorized source, transient source, schema mismatch, bad record and unsupported capability. Invalid records fail the sync initially; quarantine becomes an explicit option later. ABI mismatch rejects artifact loading. No production connectors are shipped; a deterministic test double exercises duplicate events, failed pages, commit-before-ack and writeback retries.

Artifacts are OCI image digests plus a versioned JSON-lines protocol over supervised pipes: start, event, checkpoint-candidate, result and error envelopes. Images have dependency locks and SBOM metadata. The supervisor independently authenticates execution IDs; an arbitrary worker cannot publish another worker's results. Python transform wrappers receive pinned Iceberg snapshot references and write staged Parquet data plus declared schema/identity metadata. The trusted Iceberg adapter assembles and validates Iceberg metadata before the atomic catalog publication; workers cannot bypass it with direct table commits. SQL preview is a bounded local/worker operation using the same transform version, with limited rows and no head publication.

## R03 — functions and agents

TypeScript functions receive typed inputs, scoped OSDK and logging/cancellation contexts. Ordinary functions return values; mutation functions return edit proposals for the action transaction described in C06. They cannot host HTTP endpoints. Generated OSDK bindings are the sole supported user-code object/action interface; internal service endpoints are inaccessible to worker identities.

First adapter: LangGraph.js behind an interface with describe/load/start/events/cancel/result, plus optional checkpoint/resume. LangGraph internal state is not a platform domain model. In the first demonstrator resumability is disabled; later enabling it requires proving durable checkpoint, cancellation and replay behavior. Agent tools use the same generated OSDK as the frontend and never receive broader access by virtue of being tools.

Model/tool calls carry stable call IDs. Action tool idempotency keys derive from execution ID + logical tool-call ID and survive retries. Re-running an entire agent may produce different model decisions, so it is not a transparent retry of the old execution. External model cost or effects cannot be undone by agent cancellation.

## R04 — model gateway

The initial provider adapter supports an OpenAI-compatible HTTP chat/embedding shape as a protocol choice, not a requirement to use OpenAI's service. No assumption is made about the user's actual local server until tested. ModelProvider separates endpoint/secret/destination from Model declarations of upstream name, immutable revision if available and capabilities. Advertised tools/JSON/streaming/embeddings are verified by adapter probes; unsupported requests fail rather than silently degrading.

Normalize request/result IDs, messages, structured output schema, tool calls, finish reason, usage and errors. Preserve provider-specific fields in a namespaced metadata object. Streaming emits ordered text/tool/usage events and a terminal result; a broken stream is failed/incomplete and is not silently restarted after partial output. Retry transient pre-response failures within budget; record each billable attempt. Default request timeout 120 seconds, no prompt-content logging by default. Local models must be reachable from the gateway host through an explicit configured endpoint/private connection. No automated public exposure of the user's PC.

## R05 — retrieval, evaluation and MCP

Retrieval reuses pgvector's vector similarity implementation in PostgreSQL. The initial collection accepts UTF-8 text documents from fixture datasets, chunks by 1,000 Unicode code points with 100 overlap and paragraph-boundary preference, and records chunker revision, source revision, embedding model/dimensions and chunk IDs. Changing embedding model/dimension requires a staged rebuild; mixed embeddings never share one searchable generation. Exact cosine search initially, maximum topK 50, zero vectors rejected; hybrid ranking and ANN are later. Permission checks and deletion filtering are mandatory regardless of engine.

EvaluationSuite pins an agent revision via release resolution, fixture snapshot, evaluator artifact and model configuration. Cases record input hash, expected assertions, result/error, tool/action traces, duration and reported usage. Default evaluator is deterministic code asserting synthetic behavior; LLM judges are optional and must record their own model/configuration. Repeats produce separate scores and variance; no determinism claim for inference. Tests that mutate objects run in a dedicated evaluation environment and cannot use production credentials. Comparison requires compatible case IDs and evaluator versions or reports noncomparability.

MCP exposes scoped resource inspection, object queries and actions as wrappers over platform APIs, preserving principal/application/delegation and action idempotency. No generic database or shell tool. Capability discovery is authorization-filtered. CLI commands cover retrieval query/rebuild, evaluation result comparison, trace inspect, model test and MCP configuration. OpenTelemetry structural spans correlate calls, but platform audit/results remain the durable authoritative records.

## R06 — automations and scheduling

Automation declarations select a domain operation, target resource, input and intervalSeconds; timezone-specific cron is deferred. Each firing is identified by automation ID + scheduled UTC instant, with a service identity and scope. Missed intervals coalesce to one firing by default, and overlapping executions skip new firings with an audited reason. Policy revocation prevents the next dispatch. Scheduler lease/fencing and a unique firing key prevent duplicate scheduling, but domain side effects still require their own idempotency. Domain status remains visible through builds/syncs/agents/evaluations, not generic runs.
