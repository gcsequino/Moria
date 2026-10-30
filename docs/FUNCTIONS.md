# TypeScript functions

Functions are exported resources in a registered code repository. Filesystem
publication validates and activates the declaration; a separate function worker
executes its immutable OCI image. Invocation uses the resource UUID, never a path
or an unqualified namespace. The first runtime is `typescript-v1`.

## Completion checklist

Status: implementation and local acceptance complete for TypeScript on Docker;
CI wiring is configured. Independent latest-revision CI/review remains required.

The current release targets TypeScript on Docker, using the shared version-1
platform value model with TypeScript/Python codecs. Additional function languages,
Firecracker and production capacity are tracked
separately in [IMPLEMENTATION.md](IMPLEMENTATION.md#remaining-component-work).

The release acceptance criteria are durable private invocations; pinned releases,
artifacts and inputs; current authorization; typed values; OSDK and supported
platform API access; three enforced effects; bounded nesting; idempotent calls;
results/events/cancellation; function-backed actions; and real Docker isolation,
resource and supervisor-crash tests. API, CLI and Files-console controls use the
same authenticated platform clients. See the dated verification evidence in
[IMPLEMENTATION.md](IMPLEMENTATION.md).

The complete Linux gate uses a test-only Docker socket. The CI job is configured
to preload digest-pinned Node/registry images before its offline gate.
Only the non-root quality container receives the ephemeral runner's daemon socket,
with its socket group added explicitly; bootstrap receives no socket. Function
sandboxes remain network-disabled and receive no daemon socket or host mounts.
Use a disposable test runner: daemon access gives the trusted test supervisor
control of that runner. Independent latest-revision CI must pass before review
and merge.

## Build and publish

Start with [the example](../examples/functions/README.md). An image contains the
versioned runner, shared value codecs and a module with a default async function export. CI checks code,
locks dependencies, builds the image with SBOM/provenance metadata, pushes it and
records its distribution manifest digest in the Function's artifact declaration:

```json
{
  "apiVersion": "moria/v1alpha2",
  "kind": "Function",
  "metadata": { "namespace": "logic", "name": "increment" },
  "spec": {
    "artifact": {
      "uri": "oci://registry.example/team/increment",
      "digest": "sha256:<64 hexadecimal characters>"
    },
    "runtime": "typescript-v1",
    "inputs": { "value": { "type": "float64", "nullable": false } },
    "outputs": { "value": { "type": "float64", "nullable": false } },
    "effect": "readOnly",
    "limits": { "timeoutSeconds": 30, "memoryMiB": 256, "cpuMillis": 1000 }
  }
}
```

Export an array of these resources through `moria fs publish <repository-root>
--exports exports.json`, then review and apply the filesystem activation plan.
Operators preload the exact `repository@sha256:...` image on each function worker.
Invocation never downloads code, resolves a mutable tag or installs dependencies.
Images with declared volumes are rejected.

Build the matching runtime directory with
`node scripts/build-function-runtime.mjs <output-directory>` on Node 24. The example
Dockerfile performs this in a separate build stage without installing dependencies.
Package the complete directory, including its local module metadata and codecs;
copying only `runner.mjs` is insufficient. Keep this runtime pinned to the Moria
release used by the supervisor.

## Worker boundary

Use the existing external PostgreSQL configuration for separate API and worker
processes. Run `pnpm moria functions worker` on a Linux worker host. It shares the
instance database, filesystem storage configuration and trusted object bindings
with the API. The initial runtime binds the local workspace's `assets` object API,
its edit/reset actions, the filesystem API and function invocation/inspection.
Additional object/action services require the same trusted bindings as the normal
API. This release does not implement new ontology capabilities solely for functions.

Only this explicit worker process constructs the Docker backend, injecting it into
the service. `services/functions/src/worker.ts` defines the backend-neutral
`FunctionWorker` contract and shared resource limits; the service does not import
Docker or choose a backend implicitly. Starting a worker without an injected
backend fails before polling. API processes enqueue, inspect and cancel database
records without a Docker backend or socket.
Neither the default combined development server nor the fixed Snow/weather worker
implicitly starts a function worker. With no available worker, an accepted request
expires visibly; it is never executed later under a new attempt.

The trusted function worker needs Docker CLI and local daemon access. Treat that
worker as privileged infrastructure. User images receive no Docker socket, host
mounts, database credentials, platform token or provider secrets. Containers run
as UID/GID 65532, with a read-only root, all capabilities dropped, no new privileges,
private networking disabled, bounded PIDs/memory/CPU and a bounded scratch tmpfs.
The runtime uses Docker Engine API v1.51 resource statistics for CPU accounting.
The runner waits after its result frame for the supervisor's `complete` message.
The supervisor measures CPU while the container is still running before sending
that acknowledgement; even jobs shorter than the sampling interval are measured.
Images must use the matching release's runner protocol. An accounting failure
prevents successful output publication. See Docker's
[container options](https://docs.docker.com/reference/cli/docker/container/run/)
and [resource statistics](https://docs.docker.com/reference/api/engine/version/v1.51/).

## Production backend plan

Docker remains the implemented backend. Firecracker is the planned production
backend for untrusted functions; it is not implemented or selectable yet. Backend
selection belongs to the trusted worker launcher, never a Function declaration or
invocation parameter. No automatic fallback from Firecracker to Docker is planned.
Node 24 and the `typescript-v1` function API remain independent of this choice.

The service owns acceptance, authorization, idempotency, leases, fencing, typed
validation, platform authorization and result publication. A backend owns digest
verification, sandbox creation, resource accounting, bounded protocol transport,
cancellation and idempotent removal. `execute` receives only the invocation ID,
pinned artifact, limits, start message, authorized RPC/log callbacks and abort
signal. It resolves only after successful execution exit and settled callbacks;
invalid protocol, aborts and failed accounting reject. `remove` must also work
after a worker restart when no in-memory execution handle survives. Changing the
backend must preserve these guarantees and the existing input/output/log bounds.

Implement Firecracker in these bounded steps:

1. Prepare verified guest artifacts before invocation. Keep digest-pinned OCI as
   the published code format, with a trusted preparation step producing a guest
   root filesystem. Record the source digest, derived filesystem digest and pinned
   guest kernel/runner versions; do not assume Firecracker boots OCI directly.
   Verify the prepared bytes before use. No downloads or dependency installation
   during an invocation.
2. Boot one fresh microVM per attempt on Linux/KVM using the production jailer.
   Use an immutable root, isolated bounded scratch storage, an unprivileged function
   process, host resource controls and no guest network interface or host secrets.
   Transport the existing versioned messages through a bounded vsock channel to a
   minimal guest runner; guest requests remain untrusted and pass through the same
   supervisor authorization callbacks. See Firecracker's
   [design](https://github.com/firecracker-microvm/firecracker/blob/main/docs/design.md),
   [jailer](https://github.com/firecracker-microvm/firecracker/blob/main/docs/jailer.md)
   and [vsock](https://github.com/firecracker-microvm/firecracker/blob/main/docs/vsock.md)
   documentation.
3. Enforce deadlines and memory/CPU budgets from the host, including VM overhead.
   Cancellation and worker loss must fence publication and remove the VMM, sockets,
   scratch storage and resource-control state. Persist host/backend ownership and
   route cleanup to that owner before supporting multiple hosts or backend pools;
   local absence alone cannot prove cleanup on another host. Use a single backend
   per instance until that routing exists. Do not add snapshot reuse in this step.
4. Run the same lifecycle/protocol acceptance cases against both adapters, plus
   real VM tests for isolation, cancellation, host/guest crashes, resource limits,
   artifact tampering and restart cleanup. Measure cold start, memory overhead and
   concurrency on representative KVM hosts before setting production targets.

The current interface separates execution infrastructure; it does not supply
artifact preparation, fleet scheduling or production isolation certification.

## Invoke and inspect

The caller needs current `functions:invoke`, matching application scope, filesystem
read access to the active Function and clearance for all effective markings.
Invocation records are private to the initiating principal/application/environment.
Administrators do not acquire another caller's results through a generic job list.

```sh
pnpm moria functions invoke <function-uuid> --config input.json --key calculation-001
pnpm moria functions invoke <function-uuid> --config input.json --key calculation-002 --async
pnpm moria functions get <invocation-uuid>
pnpm moria functions events <invocation-uuid> --cursor 0
pnpm moria functions cancel <invocation-uuid>
pnpm moria functions wait <invocation-uuid>
```

Remote CLI access uses the same HTTPS origin and short-lived workload token as
filesystem access. The platform API client in `packages/platform/src/functions.ts`
exposes `invoke`, `get`, `events` and `cancel`; function invocation is not bundled
into generated ontology SDKs. Invoke requires an idempotency key; synchronous
waiting is the default. `wait: false` returns acceptance immediately. A waiting timeout does
not cancel execution. HTTP returns a Location header and retains the invocation
ID in errors after acceptance. The Files console resolves Function resources and
provides JSON invocation, status, result, event and cancellation controls.

Hosted applications opt in through the trusted `functions` binding on
`createScopedAppServer`. Calls retain the verified app session, current application
scopes and filesystem permissions. Declaring a client method does not enable a
server endpoint. The bundled Snow server has no function binding.

One key binds the caller, environment, Function UUID and original input hash. An
identical retry returns the same invocation even after redeployment; different
input conflicts. Records pin the release, definition, OCI digest, attempt,
correlation ID and verified session context. Functions are never automatically
retried. Use a new key to request a new computation deliberately.

## Function context and result safety

The ontology SDK and platform API are distinct interfaces. Filesystem operations
and function invocation belong to the platform API and are not bundled into
generated OSDK clients. Supported platform routes are exposed through
`context.platform.fetch`; there is no `context.osdk.filesystem` or
`context.osdk.functions`.

The default export receives `(input, context)`. Context exposes `invocationId`, `valueVersion: 1`,
`signal`, `log`, `effect`, `osdk` and `platform.fetch`. Export the generated
ontology client's factory as `createOsdk` alongside the default function; the
runner constructs `context.osdk` with its supervised transport. An artifact
without that export can still use `context.platform.fetch`. The legacy read-only
`context.fetch` ABI remains available for existing object-read artifacts.

`platform.fetch` accepts relative API paths and GET/POST operations. Supported
routes are bound ontology point reads, queries and actions, the filesystem API,
and function invocation/get/events/cancel. It preserves HTTP status, content type
and binary bodies within the 32 KiB limit. Other platform services require a
trusted integration before becoming available here. Absolute/external URLs,
identity/authentication headers and internal control endpoints are rejected.
Only content-type, idempotency-key, x-moria-size and x-moria-digest request headers
are accepted. The supervisor supplies the verified initiating identity; no bearer
token or server credential enters the container.

Every read and mutation uses current caller permissions, application scopes and
markings. Successful authorization checks made during data-dependent failures
also classify the invocation and retain read-permission requirements. Catching a
protected API error cannot turn the resulting information into unmarked output.

Inputs and outputs use the shared [value model](spec/core-contracts.md#c03--values-rows-and-schema-evolution).
Int64 and decimal values remain exact strings in TypeScript; dates, UTC timestamps
and UIDs also remain canonical strings. Nested records/lists, optional nested
fields and empty input/output maps are supported. Missing nullable top-level
parameters/results normalize to null; nested omission requires `optional`.
Unexpected fields or invalid values fail validation. The runner validates native
output before serialization, rejecting NaN, undefined, BigInt and Date objects
where wire values are required; the supervisor validates again before publication.
Each invocation returns `valueVersion: 1`; unsupported stored/protocol versions
fail closed. Typed request keys hash normalized input against the originally
pinned schema, so equivalent decimal/time spellings replay the same invocation.
The supervisor records conservative markings and read evidence before returning
OSDK data to code.
Results and events recheck current access. Changed read evidence withholds old
results/logs. Content-equivalent object reads can advance their revision after
unrelated changes; a concurrent revision change can still conservatively reject
an in-flight mutation. A committed call advances its own read anchors, with
filesystem versions captured inside the mutation transaction so a competing
write cannot be mistaken for that call's changes. Writes into destinations that
omit any consumed marking are rejected. Delegated sessions are unsupported.

| Effect | Behavior |
| --- | --- |
| `readOnly` | Point reads, queries, filesystem reads and read-only child functions. POST queries are reads; mutating routes are rejected. |
| `editProposal` | Point-read dependencies and staged ontology actions. Valid output triggers one transaction for the complete staged batch; any failed condition or policy rolls it all back. Filesystem mutations and arbitrary write calls are rejected. |
| `readWrite` | Authorized supported mutations commit per call. Later code failure or cancellation does not undo earlier commits. |

Each mutating logical call needs a stable idempotency key, scoped to its parent
invocation. Repeating it with different input conflicts. The durable `calls`
trace distinguishes started, succeeded, failed, proposed and committed operations.
A crash between a service commit and receipt persistence can leave an
unknown outcome. Such a call is never blindly repeated, and function code itself
is never automatically retried.

A declared `Action.function` must reference an `editProposal` Function with the
same input schema. Its typed outputs are a property map matching editable target
properties; the UID is immutable. Trusted runtime composition resolves that
Function reference to its resource UUID. The function computes outside the
transaction, then the action rechecks point-read evidence, current authority,
destination markings, condition and post-state before committing edits and the
idempotency outcome together. Preparation cannot stage additional mutations.
Unconstrained predicate/aggregate read dependencies are rejected for proposals.
This supports the existing single-target action runtime.

Nested invocations retain caller identity and inherit consumed input evidence and
markings. Cycles and nesting beyond four invocations fail explicitly. A child's
deadline cannot exceed its parent's; cancellation and parent failure stop live
descendants. Async acceptance is available through `{wait:false}`. No extra
worker slot or broader permission is granted to a nested call.

## Ontology object parameters and results

Generated ontology types such as `Customer` and `Order` denote object identities.
They can appear in input/output fields, nested records and lists. Declare the
corresponding envelope type with `type: "ontologyObject"`, its immutable
`objectType` UUID and `nullable`. Both the API and trusted worker must bind that
UUID to the same ontology service. App contracts include the UUID as each object's
`id`; generated public names have no `Ref` suffix.

```ts
import { createClient, type Customer } from './generated/index.js';
export const createOsdk = createClient;

export default async function echo(input: {
  customer: Customer;
  customers: Customer[];
}): Promise<{ customer: Customer; customers: Customer[]; customerName: string }> {
  const current = await input.customer.get(['name']);
  return { customer: input.customer, customers: input.customers, customerName: current.object.name };
}
```

Export the matching `createOsdk` factory so the runner can bind incoming identities
to those handles. Construct identities with `osdk.objects.customers.object(uid)`.
The wire and retained invocation contain only `{objectType, uid}`. Use
`customer.get(projection, options)` for current, permission-checked properties;
the fetched record has the generated `CustomerProperties` shape. Query/get/all
methods on an object client continue returning property records with explicit
projections. Identity construction grants no access and performs no fetch.

Input references are authorized before code executes. Returned references are
validated before success or an edit proposal can commit. These checks enforce
type identity, existence, current object/row access and markings while leaving
properties unread. Consumed properties retain their normal evidence rules;
revoked access, deleted objects and changed type bindings withhold retained results.
The environment comes from the verified caller, never a supplied reference.

The runner awaits a plain result or `Promise<Result>` before typed serialization.
Rejected promises fail the invocation. Promises inside individual fields are
invalid values. Results remain named-field objects; object lists live under a
declared field, and empty output maps return `{}`. Platform API invocation results
carry wire identities; applications can bind them through their matching OSDK.

## Limits, cancellation and recovery

There are at most four active accepted/running invocations per environment and
four concurrent attempts per worker. Deadlines include queue/startup time and are
at most 30 seconds. Memory is 32–1024 MiB (256 default); CPU budget is 1–30000 ms
(1000 default), with one CPU throughput cap and per-process OS limits as an
additional guard. CPU usage is sampled; budget excess triggers termination.
Cleanup permits ten seconds before forced termination. No multi-tenant isolation
or production-capacity certification is implied.

Each attempt holds a ten-second renewable lease and a private fencing token. Only
the exact current attempt can publish a result before its deadline. Cancellation
commits a terminal state before worker cleanup; a committed success wins a later
cancel. Expired workers fail visibly. Worker maintenance cleans terminal/orphaned
containers without allowing API processes to control Docker. A worker outage or
database failure never authorizes an old attempt to publish or repeat code.

JSON inputs, outputs and API response bodies are at most 32 KiB each. Each invocation
allows at most 32 broker calls, 256 read observations, 128 total events, 4 KiB per
log and 256 KiB of worker protocol output. Event sequence cursors resume without replaying earlier events.
Result/event access expires after 30 days; request keys remain non-reusable. The
first implementation retains underlying database records for later maintenance.

## Verification status

Real PostgreSQL and authenticated HTTP/SDK/CLI tests exercise acceptance, replay,
redeployment, current permissions, effects, atomic proposal rollback, binary file
transfer, nested calls and changed input evidence. The Docker suite builds and
publishes a real digest-pinned OCI artifact containing a generated SDK, executes
it, denies host/network/credential access, exhausts CPU/memory/PIDs and kills a
supervisor to verify fencing and replacement cleanup without rerunning code.
Only the trusted test supervisor receives the approved test Docker socket.
User function containers receive no daemon socket. No deployment or production
capacity certification has been performed.

The completed function audit passed the full Linux `pnpm check`, with no failures
or skips, plus the Python, schema, SDK, build and starter checks. Current type-system
verification is recorded in [IMPLEMENTATION.md](IMPLEMENTATION.md). The actual console
component was also exercised against the real worker for invoke, retry, events
and cancellation, followed by sandbox cleanup.
