# Native function sandbox supervision

The Rust runtime supervises an existing guest runner over its version 1 JSON-lines ABI. The guest runner and generated SDK remain reusable user-code tooling. PostgreSQL execution authority, container lifecycle, resource accounting, cancellation and broker admission run natively.

This component is the sandbox mechanism. Trusted immutable operator/release bindings select a pinned image and output descriptors. Public function admission, deployed release discovery, edits, nested function policy and function result publication require the separately composed authenticated serving broker and domain receipt transaction. A sealed task is private prepared output, not a successful business publication. No Node function handler or permissive broker is started by this component.

## Binding and authority

`runtime_function_bindings` extends an immutable `runtime_execution_bindings` row with the OCI repository and exact SHA-256 digest, versioned output descriptors, effect, memory limit, CPU budget and wall timeout. Limits are 32–1024 MiB, 1–30000 CPU milliseconds and 1–30 wall seconds. Guest value version is 1. Execution must have domain `function`, retry policy `never`, exactly one task and exactly one claimed attempt.

`SandboxSupervisor::new(database, host_id, broker)` requires an explicit operator-provisioned host UUID and an implementation of `PlatformBroker`. `execute(lease, cancellation_receiver)` accepts a current owner-bound execution lease. It does not accept image names, commands, URLs, actors, tokens or resource limits from the guest. The image must already be present with the exact repository digest; no pull occurs. Image-declared volumes are rejected.

`SandboxContext` has private fields and no deserializer. It is constructed from the live lease and immutable stored execution context. Getters expose the verified environment, principal, application, session hash, effect, lease, attempt and correlation IDs to the trusted broker. `check()` revalidates the incarnation, task ownership/fence, database-clock lease, binding, session and authorization epochs. It runs before and after platform calls and protected logs, before the completion handshake, during execution and before sealing.

The required asynchronous broker methods are:

```rust
http(context: &SandboxContext, call: HttpCall) -> Result<HttpResponse>
log(context: &SandboxContext, message: &str, consumed: &[ConsumedMarking]) -> Result<()>
finish(context: &SandboxContext, result: &serde_json::Value) -> Result<()>
```

`HttpCall` contains a bounded guest call ID, method, relative `/api/` path, allowlisted headers and optional base64 body. It contains no actor or credential. The broker applies API-specific operation, effect, idempotency and reference policies using the trusted context. Authentication headers and arbitrary destinations are rejected before broker dispatch. `HttpResponse` contains a status, allowlisted headers and base64 body. The supervisor bridges both existing HTTP and legacy OSDK guest messages to this interface.

The broker records trusted consumed markings through the context as protected reads succeed. Guest messages cannot provide or remove them. Log callbacks receive the current marking union, and final output authorization runs through `finish`. Guest text and data never go to service logs. The test broker is limited to real native Record reads and protected database logs; it is not a production fallback.

## Isolation and bounded protocol

The native supervisor talks directly to the local Docker Unix socket, using the existing HTTP client and a bounded attach stream. Containers use UID/GID 65532, no network, no capabilities, no-new-privileges, read-only root, private IPC, a 16 MiB noexec/nosuid/nodev tmpfs, 64 PIDs, exact cgroup memory and swap limits, one CPU of throughput and a CPU-time rlimit. No host filesystem, socket, database credential, warehouse credential or ambient worker environment is mounted or passed to the guest. Docker's standard seccomp profile remains enabled.

CPU use is sampled from Docker's actual cgroup statistics every 200 milliseconds and again before acknowledging a result. The native accounting guard terminates an over-budget guest; the kernel CPU rlimit rounds up to whole seconds as a backstop. Polling and daemon scheduling introduce enforcement latency, so this is a resource bound rather than nanosecond-precise billing. Wall time also includes image verification and container startup and cannot exceed the remaining database lease/deadline or trusted binding timeout.

Docker attach frames are bounded to 256 KiB total across stdout/stderr, with JSON lines bounded to 64 KiB, at most 32 unique platform calls, 32 KiB request/response bodies, at most 124 logs of 4 KiB, and a 32 KiB canonical result. Headers, call IDs and methods are validated. Stderr is counted and discarded. Invalid framing, excess output, unsupported versions and invalid typed values fail closed.

A result is normalized independently using the pinned output descriptors. The supervisor verifies current ownership and CPU accounting, sends `complete`, requires a successful container exit, invokes final broker authorization, removes the container and then seals private output under the same lease. Output following the result is rejected. A result without the handshake and successful exit cannot seal.

## Cancellation, crashes and recovery

Each execution has a durable `runtime_function_sandboxes` journal with the immutable owner/fence, host, attempt/correlation IDs and container name. A second supervisor cannot insert another attempt. Cancellation, authority loss, lease loss, timeout, resource exhaustion and protocol failure terminate the attempt; execution authority never reruns its code automatically. An external action performed before a crash retains its independent authoritative receipt.

Before a protected start, HTTP/SDK call, log or finish callback, the supervisor
commits an exact pending observation under the live task owner, fence and attempt.
The required broker clears it only in the transaction that persists the actual
read/call evidence and the monotonic consumed-marking union. An HTTP success by
itself does not clear the journal. The supervisor checks the durable completion
before returning the callback to guest code. A lost response, rollback or process
death therefore leaves an incomplete observation even when memory is gone.

Pending observations prevent terminal metadata, logs and results from being
released; an explicit blocked observation also prevents release. Running
structural status remains pollable before a callback body is delivered. Retained
artifact reads use one repeatable-read snapshot so authorization cannot inspect
an earlier pending state and then release a newly committed failure event.
Durable attempt markings are included in Function, Agent, Evaluation and pipeline
retained authority. Markings and blocked state cannot be rolled back by a later
journal update, and a private seal requires confirmed cleanup with no pending or
blocked observation. These mechanisms still require the real broker's evidence
transaction; there is no default completion broker.

Cleanup has a ten-second bound, verifies the container labels, resolves its immutable Docker ID, stops/kills it and removes it idempotently. Failure to confirm cleanup prevents sealing. `cleanup_abandoned()` removes up to sixteen local orphan sandboxes whose executions are terminal or whose original leases are no longer live. It is scoped to the configured host ID and exact execution label.

The creation journal distinguishes not-issued, uncertain and acknowledged Docker creation. If a create request loses its response, absence is insufficient to declare cleanup complete: the journal remains eligible for repeated cleanup until the exact container can be observed and removed. This avoids treating an ambiguous external response as proof that no process exists. Terminal journal state, observed CPU, consumed markings and cleanup status remain available for protected operator inspection.

The real integration fixture runs the existing pinned guest image against PostgreSQL and native serving. It covers typed results, HTTP/SDK object reads, isolation, CPU/memory/PID exhaustion, malformed or excessive output, owner rejection, timeout, cancellation, session revocation, supervisor SIGKILL, stale-lease fencing and idempotent orphan removal. These mechanism tests do not establish public function admission or full distributed capacity.
