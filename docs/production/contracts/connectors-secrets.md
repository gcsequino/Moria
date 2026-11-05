# Connector capability and secret contracts

This is the N09 implementation contract. The initial native pipeline profile implements `batchRead` with full coverage and replacement output. It explicitly rejects other capabilities and write modes; those rejections are not completion of the accepted Source contract. [Pipelines](pipelines.md) defines supervision and grouped publication. [Engine I/O](engine-io.md) defines the bounded Arrow transport.

## Capability admission

The native credential implementation now accepts up to eight declared references
with exact operator mappings. Its static checks, all nine artifact fingerprints
and 79 native units pass on a frozen build; real composed qualification is pending.
Other write modes, coverage and capabilities remain unavailable. This checkpoint
must not be presented as complete N09 qualification.

The operator destination's `credentials` array contains `secret` (resolved UUID),
`materialRef` (host environment reference), `header` and `scheme` (`raw` or
`bearer`). It must map every declared secret exactly once, at most eight slots,
with distinct lower-case `authorization` or `x-` header names. Transport, cookie
and proxy headers are rejected. Empty mappings retain the existing single bearer
slot; multiple declared secrets require explicit mappings. These values are
trusted operator metadata, never guest request fields. `secret inspect-connector`
returns the exact approval requests without reading material; `secret approve`
and `secret disable` retain operator-only custody control.

Accepted windows pin every approval identity, generation, incarnation and semantic
material revision. Ordinary callers and registered services independently need
all exact use operations. Current checks cover claim, callbacks, in-flight polling,
publication and result release. A missed disable/reenable pulse cannot revive a
window. The cursor hashes semantic material revisions and header interpretation,
excluding current credential bytes and approval generations. Reflected known
material in a provider response is rejected before any guest response is released.

The released `Connection` selects one immutable `connector/v1` implementation, a validated configuration, stream, coverage, write mode, output Dataset and declared SecretReferences. The accepted execution pins each of those identities, schemas and revisions. Public input cannot select a different implementation, endpoint, secret, mode, destination or actor. Configuration validation uses the same offline trusted schema registry as Source publication; schema references cannot fetch network or local files.

| Source capability | Required operational behavior                                                                                                                                                                                                                    | Current qualification                                                                                |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------- |
| `batchRead`       | One admitted, bounded, single-attempt guest program streams accepted rows; output, checkpoint CAS and execution completion publish atomically.                                                                                                   | Replace/full engine boundary has functional proof; Sync checkpoint and fault proof remains required. |
| `schemaDiscovery` | Read-only bounded discovery through the same fixed transport and current SecretReference grants. Proposed schemas are typed metadata and cannot silently mutate an accepted output schema or publish data.                                       | Rejected before code; implementation and proof required.                                             |
| `continuousRead`  | Bounded execution windows with durable source cursor/watermark, explicit restart boundary and backpressure. Each window publishes its output and cursor atomically. A worker replacement cannot transparently rerun arbitrary started user code. | Rejected before code; implementation and proof required.                                             |
| `writeback`       | Explicit external side-effect intents, destination policy, idempotency capability and uncertain-outcome reconciliation. A successful Dataset publication does not prove an external effect occurred exactly once.                                | Rejected before code; implementation and proof required.                                             |

Capability sets are normalized sets, not an ordering-sensitive magic array. Each requested operation must be declared by the implementation and supported by the configured adapter profile. Merely accepting a declaration or advertising a capability does not authorize dispatch.

## Output and checkpoint identity

The Dataset IO grant derives the write contract from the accepted Connection and current pinned Dataset definition. The contract contains mode, coverage, full output schema, ordered primary key, expected output head and whether rows are complete rows or keys only. The guest does not supply this contract or physical file references. A versioned sealed part root binds its actual execution/task/owner/fence/incarnation, output Dataset, schema/key hashes, expected head and write contract. The coordinator verifies every root against the exact sealed task result before preparing any output.

Replace, append and upsert accept complete typed rows; delete accepts only the exact ordered key schema. Incoming keys must be unique. Append must also be disjoint from retained keys. Upsert/delete use bounded key-index shards and spill joins, rewriting only affected immutable files/manifests. A table lacking the required index needs an explicit bounded bootstrap; it cannot fall back to holding the table or key population in memory. These modes remain unavailable until their writer and publication proofs pass.

Coverage describes the input enumeration, independently of the output application mode. `full` permits all four modes only after every accepted input partition seals; full upsert preserves keys absent from the enumerated input, and full delete consumes explicit keys only. `incremental` permits append, upsert and delete; replacement is rejected without a future explicit complete-snapshot capability. `appendOnly` permits append only. A declared primary key still requires append disjointness; an explicitly keyless Dataset has multiset semantics. Delete's connector output schema is the exact ordered key schema, while its task root also binds the complete target schema hash.

The checkpoint semantic revision hashes the connector artifact and ABI, normalized validated configuration, stream, mode, coverage, output Dataset UUID, actual physical schema and ordered key, supported write modes, namespace identity/revision, Iceberg table UUID and approved secret material-binding revisions. It excludes unrelated Source release changes, bearer credentials and secret plaintext. Execution metadata separately pins the full current Source/binding revision for authority and audit. A material change affecting cursor interpretation starts a new checkpoint scope; a pure credential rotation with the same semantic material-binding revision does not silently reset the cursor.

The durable cursor key is `(environment, Connection UUID, stream, semantic revision)`. Output publication, cursor expected-version CAS, grouped receipt and execution completion share one PostgreSQL transaction. Lost acknowledgement resolves that exact receipt before accepting a next window. Cancellation, changed owner/fence/incarnation, incompatible definition or failed cursor CAS makes the whole publication transaction invisible.

## SecretReference authority and custody

`SecretReference.spec.binding` is a logical name. Its `consumers` list constrains eligible consumers; neither field grants access to operator credentials. A newly published declaration naming a known binding cannot read or cause use of that binding.

Operator registration must explicitly approve the exact SecretReference UUID, normalized declaration fingerprint, resolved consumer UUID, trusted consumer profile and revision, endpoint/destination fingerprint, operator material reference and material-binding revision. The approval also pins environment/incarnation and allowed markings. Its definition is immutable; disabling or replacing it increments a durable generation and permanently fences previously accepted attempts. A Source declaration cannot register or approve its own custody row. Qualified references resolve through the accepted release's immutable owned/dependency map, with ambiguity rejected.

The actual verified caller must independently hold both the consumer operation and exact `SecretReference UUID:use`, intersected with current application scopes and grants. Current Source, dependency and actual code/descriptor file ACLs and classifications remain required. Ordinary Model calls use the same verified session for `Model.invoke` and `SecretReference.use`; there is no provider impersonation. Registered pipeline, projector and Agent calls use their existing actual accepted service identity and its accepted/current intersection. Their execution bindings must include every declared SecretReference use operation. `use` returns no material and grants no filesystem browsing.

The native host resolves material only from the already approved consumer profile. For Models, the existing operator route `secretRef` remains custody authority; the Source ModelProvider/SecretReference declaration only narrows that authority. For connectors, the immutable trusted destination selects the approved material reference. There is no global environment lookup driven by Source `spec.binding`.

Material stays in the native host request path. The guest receives no plaintext, material reference, authorization header, cookie, provider credential, socket or host path. The host checks current consumer/secret/source authority and destination flow before dispatch, while receiving bounded chunks, and before releasing any response or observable error. Secret/source classifications are recorded before a guest can observe the callback outcome. Redirects are disabled, methods/paths are allowlisted under the fixed destination, and sensitive response headers are removed. Public errors and service logs contain only static codes. Revocation closes an in-flight request and prevents publication; a later reenable cannot revive an earlier accepted capability.

## Required qualification

Real PostgreSQL, native workers and deterministic HTTP peers must prove approved use, missing use scope, wrong consumer, forged logical binding, changed profile/destination, direct material-read denial, exact Source-file ACL denial, private-to-public destination denial, missed disable/reenable pulses, in-flight revocation and no plaintext in guest frames/events/logs. Actual Sync tests must cover fixed endpoint injection, checkpoint conflict, grouped rollback, lost commit acknowledgement, worker death after code start, cancellation, bounded streaming and every enabled mode/capability. The production workload ladder remains the implementation target; functional evidence does not certify capacity.

## Model custody qualification

The `secret inspect-model`, `secret approve` and `secret disable` native operator commands exchange bounded JSON on stdin. Inspection returns only the actual immutable consumer binding tuple; approval adds an explicit request key, semantic material revision and classification set. The database schema owner must authorize custody. Ordinary roles cannot insert, replace or disable approvals even with general runtime-table DML privileges. Current readiness reports missing approval without reading material or invoking a provider.

Approvals are tied to exact actual Model route/profile, resolved destination and normalized Secret declaration. Pinned dependency declarations and their current actual declarations both retain contributing-file and descriptor ACL checks. A Model declared in a different Source release can use its separately owned provider/Secret only through the exact immutable dependency identity map. Global filesystem browsing scope is not needed for approved Model/Secret use, and direct filesystem read remains independently denied without its own scope.

The October 31 `native-integration/20261031T122345Z` immutable proof passes all ten actual Source/HTTP Model custody cases, alongside current Models, Dataset custody, recovery, Search and MCP tests. Agent/service approval-generation capture, Connector callbacks, multi-slot credentials and checkpoints require their own composed proofs; ordinary Model qualification does not imply those paths passed.
