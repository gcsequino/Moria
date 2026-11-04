# Native function admission and publication

`NativeFunctions` resolves active filesystem Function resources, accepts authenticated invocations, exposes private status/events/cancellation, and composes sandbox output with atomic function receipts. The worker uses the native OCI supervisor and a required native `FunctionPolicy`; API processes do not construct a Docker backend. There is no Node service bridge.

## Public protocol and immutable acceptance

The existing platform SDK routes remain:

- `POST /api/v1/functions/{resource UUID}/invoke`, with `Idempotency-Key` and `{input,wait?}`.
- `GET /api/v1/invocations/{invocation UUID}`.
- `GET /api/v1/invocations/{invocation UUID}/events?after={decimal sequence}`.
- `POST /api/v1/invocations/{invocation UUID}/cancel`, with `{}`.

Cancellation returns the uniform [control receipt](execution.md#public-cancellation-control)
`{id,cancelRequested:true}`. It does not return invocation state or artifacts;
successful committed receipts remain available through their normal retained gate.

The transport derives a private `VerifiedSession` from the authenticated bearer token. No request supplies a principal, application, release, artifact, parent execution, preparation flag, worker, resource class or lease. Current Function Source/dependency/file visibility, exact `<Function UUID>:invoke` permission and application scope, and effective markings are checked before acceptance and retained data access. Explicit legacy `functions:invoke` remains compatible. Acceptance pins the selected operation; a later global grant cannot rescue a revoked scoped job. Invocation-specific Source ACL checks do not grant direct filesystem browsing. Results are scoped to the initiating environment/principal/application; an administrator does not gain another caller's results through a job list.

Acceptance resolves the exact active filesystem binding and declaration under environment and filesystem admission. It records the immutable release digest, definition, OCI digest, typed input/schema, original verified session hash, one attempt ID, correlation ID and one single-attempt execution task. Images must already be preloaded by digest. The initial runtime is `typescript-v1`; all three declared effects remain distinct.

One logical key binds the environment, Function UUID, principal, application, normalized input and original input schema. Same-key retries return the original invocation after redeployment; changed input conflicts. Missing nullable top-level inputs normalize to null; nested optional fields use the shared value model. Inputs and canonical normalized inputs are bounded to 32 KiB. Request serialization uses a per-key admission lock before the database snapshot, so unrelated invocations do not share a global acceptance lock.

The deadline includes queue and startup time, is at most thirty seconds, and cannot exceed the parent's deadline. Registered resource classes bound live task/publication leases across replicas and environments. Waiting for a result is bounded to twenty-five seconds and does not cancel an invocation. Accepted/running responses use HTTP 202 and a Location header. Caller cancellation commits terminal authority before container cleanup; a successful receipt wins later cancellation.

## Worker authority and current epochs

`functions worker`, `functions once` and `functions cleanup` require `MORIA_FUNCTION_WORKER_OWNER` and `MORIA_FUNCTION_HOST_ID`. The owner must have a live, enabled registration matching the current incarnation, host and allowed resource classes. The launcher constructs `NativePlatformBroker` explicitly. Worker concurrency defaults to four and is bounded to 1–32; database resource-class budgets remain authoritative across workers.

Generic execution claim and seal paths reject the Function domain. Only the registered sandbox path can claim function work and seal its output. Function publication is internal worker composition, never an independent acknowledgement RPC. Lease loss never authorizes another execution of user code; cleanup and publication of already sealed output are separately recoverable.

Functions preserve pinned code across an unrelated filesystem redeployment. Before a pending first attempt or sealed publication is claimed, the worker fully revalidates the original live session, current Function visibility/scope/clearance, inherited markings and durable read/reference evidence. It records the checked epochs in `runtime_function_epochs`, anchored to the immutable execution/binding/incarnation and exact single task. The claim binds that acknowledgement to its actual owner/fence. Generic executions retain their strict immutable epoch checks.

An in-flight function cannot refresh an epoch arbitrarily. A trusted readWrite filesystem mutation obtains a private `MutationAnchor` only after a same-transaction live-lease check. Its acknowledgement requires both environment and filesystem exclusive admission locks, the exact live task/owner/fence, the old acknowledged epochs, monotonic current epochs, current session/permissions/Function filesystem access and clearance for the complete marking union. The mutation, normal security audit and new acknowledgement commit together. The guest receives no epoch token or acknowledgement API. All ordinary checks continue to compare against the acknowledged epochs and current permissions.

## Required policy composition

`FunctionPolicy: PlatformBroker` has no permissive default implementation. Its required hooks authorize input references before code, validate final output/read evidence, apply staged proposals inside publication, check retained results/logs, and validate/rebase prepared function-backed actions. Production uses `NativePlatformBroker` and native object/filesystem authority. Guest-supplied labels never classify an invocation.

Protected reads persist bounded actual read evidence and markings before returning data to the guest. The context accumulates an environment-qualified union. Logs append protected execution events and use that union; prompt/data text never appears in service logs. Retained results and events require current caller access and revalidation of the actual consumed content, visibility and references. Unrelated revisions may advance only when the consumed content and policy evidence remain valid. Expired thirty-day results/events are withheld while logical keys remain reserved.

`readOnly` permits supported protected reads and read-only descendants. `readWrite` commits supported mutations per logical call using a stable parent/action/key namespace; a later code failure does not undo a committed call receipt. Prior read evidence is checked before mutation and rebased only to that call's own admitted changes. `editProposal` stages the existing guest action invocation route instead of committing it immediately. It permits point-read dependencies; queries with unproved phantom/range dependencies fail closed. Final publication rechecks every dependency and action binding, then commits the entire proposal batch with the function receipt.

Nested invocations retain the verified actor, inherit actual read evidence and markings, prohibit cycles and more than four levels, and receive no extra worker slot. The broker permits inspection of supported descendants and records dependencies before returning their results/logs. Parent failure/cancellation fences live descendants through their lease checks.

## Atomic publication and function-backed actions

Successful guest output must first pass independent type normalization, CPU accounting, completion handshake, successful exit, broker final validation and confirmed container removal. The sandbox seals private prepared output; this is not a successful function result.

The native worker obtains a publication lease, enters the appropriate environment/filesystem admission, locks the exact current execution fence, and calls policy publication. In the same PostgreSQL transaction it applies staged edits, records the immutable typed result/hash/marking receipt, updates retained invocation classification and invokes `runtime_execution_complete`. Any failure rolls all publication effects back. Publication also has a client wall timeout derived from the remaining database lease/deadline. Losing a PostgreSQL response cannot leave its session admission locks held forever: timeout drops the dedicated connection, then reconciles the authoritative state without overwriting an already committed receipt. A lost commit response can be recovered through the durable receipt and original logical key without rerunning code.

Function-backed actions compute an `editProposal` function outside the action transaction with `prepare_only=true`. Preparation cannot stage additional mutations. The outer action then checks the same immutable result/hash, original action/function binding, current authority, strict point-read dependencies, destination markings and typed editable properties. It commits the edits, action receipt, prepared-function mapping and post-write evidence anchors atomically. Replaying the action receipt rechecks current prepared evidence and permissions without executing code again.

The real integration fixture uses actual filesystem source activation, authenticated native HTTP, the production native broker, PostgreSQL and digest-pinned Docker guests. It exercises private admission/replay, redeployment, committed readWrite calls, atomic proposal rollback, cancellation, supervisor death, nesting, prepared actions and retained-data authority. The combined execution/delivery/sandbox/function trial [003 raw TAP](../../benchmarks/native-execution-functions-delivery-sandbox-20261030-trial-003.tap) passed 44/44 checks with no skips. It includes lost public acceptance responses, actual PostgreSQL COMMIT response loss with SIGKILL, deadline-driven release after a permanently lost response, live worker revocation, generic function-seal rejection, out-of-admission epoch acknowledgement rejection and failed protected observations. An actual guest filesystem move commits retained labels and its own epoch acknowledgement/audit before returning a successful protected result. The [trials ledger](../../benchmarks/native-execution-functions-delivery-sandbox-20261030-trials.json) preserves the preceding failed trial and tested artifact identities. Isolation/resource fault coverage is in the [sandbox contract](sandbox.md). Production capacity and independent latest-revision CI remain separate evidence requirements.

### Failed action observations and callback completion

A caught action business failure is a protected observation. Its durable witness pins
normalized input, exact action/binding contract, consumed classifications, static outcome
and any already-prepared child receipt. Before retained output/event release the native
kernel replays that action inside a rollback savepoint. All mutations, deliveries,
receipts and audits are rolled back; no function code is recomputed. A changed outcome,
contract or current authorization withholds retained data. These witnesses are copied
through parent/child dependencies and share the 512-record/8 MiB ledger budget (at most
32 failed action witnesses). Successful immediate action receipts remain durable even
when later function publication fails.

The supervisor begins a durable pending observation before each broker callback. The
broker clears that exact call only in the same transaction that commits its actual
read/trace/classification evidence. Child callback outcomes are captured before child
consumption and propagated only after durable taint/dependency observation. Unknown
errors or uncertain commits leave the journal pending or permanently blocked. In-memory
callback success cannot clear the journal; terminal result/event access and sealing
require durable completion.
