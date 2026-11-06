# Native local instance composition

This catalogs the concrete components required by the complete local acceptance
flow. It complements the [program inventory](RUST-PLATFORM-PROGRAM.md) and
[caller audit](NATIVE-CALLER-AUDIT.md). Configuration, functional evidence and
production-scale qualification are separate states.

## Required components

| Component | Responsibility and admission |
| --- | --- |
| PostgreSQL 18 and pgvector | Current authority, immutable releases, leases, receipts, typed projections, exact vector ranking and durable retention references. The native and retained administration connections use the same trusted cell setting. |
| Native API and lifecycle | One native front door for authentication, filesystem, Source, objects/actions/links, Models, execution domains, Search and MCP; bounded body/connection/request budgets, readiness and graceful drain. |
| Native scheduler | Runs actual reaping and schedule ticks independently of the API. Source Automation creates a fresh purpose-bound child through its native factory. |
| Bounded bootstrap and OIDC listener | Migrations, explicit operator configuration, authentication navigation and actual identity verification. It forwards no operational request to a Node business service. |
| Browser origin ownership | Durable immutable origin-to-environment/application reservation, current App enablement, exact frontend digest and directed Source-file ACL checks. Independently trusted applications use separate origins. |
| Management React and clients | The shipped generic UI, platform clients and ontology SDK use authenticated native routes. Cancellation is a constant control receipt; inspecting results and events requires retained authority. |
| Native filesystem content | Qualified immutable uploads, digest/size/CAS checks, protected downloads, durable multipart ownership and separately authorized reclamation. Raw uploaded HTML/SVG is an attachment under an inert sandbox policy. |
| Source compiler and publisher | Shared bounded TypeScript/browser compilation produces a qualified artifact. The native publisher independently validates real checkout bytes, schemas, imports, identities, file provenance and current publication authority. |
| Dataset namespace bindings | Explicit logical namespaces, physical binding fingerprints, accepted Iceberg schemas, stable keys and classification floors. Configuration does not grant byte I/O. |
| Dataset writer and publication | Real task/coordinator leases, bounded Arrow/Parquet streaming, sealed immutable output, exact expected heads and atomic publication receipts. |
| Object indexing and projection | Registered indexing purpose, actual Dataset head, complete source/mapped policy context, typed layouts, permanent UID homes, overlays, creation population, link constraints and current/pinned readers. |
| Function worker | Actual registered worker/class/host, immutable Source and OCI artifact, native supervisor and broker. Tools use current permissions plus durable read/taint evidence. |
| Agent worker and adapter | Actual locked LangGraph adapter and published guest bytes, declared Application, Model and input/output schema. A tool child has its own native Function execution. |
| Evaluation control and evaluator | Explicit evaluation environment, real fixture Dataset, accepted Agent/evaluator pins, bounded cases, separate native evaluator attempts and case receipts. Service composition requires its separate approved purposes. |
| Model gateway | Actual published Model/Provider/Policy/Secret declarations, exact operator endpoint/material approval, current invoke/use permissions, capability probes, bounded dispatch and classified receipts/streams. |
| Search projector and query | Independently registered projector purpose, current source generation and actual embedding capability; durable chunk/embedding generation and policy-filtered exact pgvector cosine ranking. |
| Pipeline worker and engine | Real registered transform purpose, accepted input pins, explicit output schemas/namespaces, isolated Python/DuckDB artifact and adapter, hard-bounded spill, native publication and current retained-input checks. |
| Engine scratch pool | A fresh dedicated two-slot ext4 pool for this instance, trusted host registration, exact devices and a privileged native janitor. A pool belonging to another database incarnation is never reused or reset. |
| Automation control worker | Independently registered maintenance and target purposes, current Source schedule and actual scheduler firing. Run-only audit withholds child identities, outcomes, errors and result data. |
| Recovery and collectors | Native protected backup/restore, PG-history, Dataset and filesystem collectors with independent operator authority and live-reference admission. Provider deletion remains explicitly qualified; unsupported conditional deletion fails closed. |

The local proof uses actual loopback Qwen chat and Nomic embedding services.
Their endpoint and material are explicit fixture/operator configuration. Published
Secret bindings alone cannot select credentials. Browser tickets, cookies and
tokens belong only in a private mode-0600 state file outside repository evidence.

Source Application scopes are requests. Effective authority is the independently
approved current application scope intersected with current principal grants.
Publication neither creates those grants nor makes an extra requested scope
effective.

## Published acceptance roots

The test builds genuine immutable roots rather than inserting fake resource
bindings:

- The foundation defines the Application/frontend, Dataset, ObjectType, Actions,
  Links and retained classification.
- The model root defines the Model/Provider/Policy/Secret resources and binds
  their exact Source declarations to operator-approved transport/material.
- The logic repository publishes the exact OCI Agent and evaluator program
  bytes, AgentAdapter, Agent, fixture Dataset and EvaluationSuite.
- The retrieval root imports the real ObjectType and embedding Model.
- The pipeline repository publishes a real DuckDB Transform and an explicitly
  classified output Dataset.
- The automation root imports the actual Agent and Application and uses an
  independently approved service profile.

The API/default launcher, scheduler and ordinary workers run as the unprivileged
instance user. The pipeline proof starts a separately registered native worker
with only the required OS cleanup privilege; that process is not a Node request
proxy. Its actual PID is recorded before exec and matched to the native worker
readiness event. Cleanup signals only that owned process, then restores ownership
only on the verified directory created by the fixture.

## Verification state

The exact v24 whole Model HTTP preflight passed 2/2 real cases with no skips.
It exercised genuine Source configuration, explicit Secret approval, all six
chat capabilities and embedding, and denial before independent grants.
Its [metadata](evidence/2026-10-31/native-instance/whole-instance-model-v24-20261031T143321Z/metadata.json)
records the immutable artifacts and command.

The composed November 5 fixture completed the native local-model Agent,
one Evaluation case, Pipeline and exact output pin, Search with real
embeddings, Automation and current-revocation checks. It reached the browser
handoff and timed out waiting for interactive verification. The [checkpoint](../IMPLEMENTATION.md#native-integration-and-production-goals--november-5-2026)
records the result. [Browser and clean-host recovery](https://github.com/gcsequino/Moria/issues/84)
remain open.

Production-scale performance remains unqualified. A correct small functional
instance, exact million-object oracle or a successful individual query plan does
not certify the higher-rate or larger-tier targets.
