# Native execution authority v1

This contract covers durable scheduling, admission, task leases and publication
bookkeeping in `services/runtime/src/execution.rs`. It does not implement domain
handlers, sandbox supervision, connector/transform engines, agents, evaluations,
external delivery or retention/restore. Those components consume this authority;
their complete semantic and isolation tests remain required.

## Trust and ownership

Execution bindings are installed by trusted operator/release composition. A
binding fixes an environment/resource, immutable release and execution spec,
principal/application/session context, required operations/markings, environment
and instance epochs, and installation incarnation. Only `enabled` may change;
updates to identity, context or spec require a new binding ID. This is an internal
control contract, not public function admission or an arbitrary universal run API.
Authenticated public admission must derive actor/session from the transport and
preserve function replay and result-access semantics when that migration lands.
Workers cannot select an executable, shell command, provider URL or actor through
task plans. Database credentials remain outside plans and declarations.

Trusted bindings contain at most 256 required operations. This accommodates the
accepted 64-input/64-output Dataset profile, including every input read, output
read/write and Source/control prerequisite. The registered service authority's
existing 4,096-operation ceiling and every per-operation current intersection
remain separate. Raising an internal binding count does not grant a capability
or change a public profile's input/output limits.

The current database schema and checksum, non-owning runtime-role validation,
migration exclusion and aggregate connection budget apply to every connection.
Reads of status/events take shared environment/filesystem admission and recheck
current binding authority before release. A failed authorization check never
becomes a worker claim or a business completion.

## Immutable execution spec

```json
{
  "version": 1,
  "domain": "transform",
  "release": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "resourceClass": "standard",
  "timeoutSeconds": 120,
  "retryPolicy": "idempotent",
  "inputSchema": { "type": "object", "properties": {} },
  "pinnedInputs": [],
  "expectedHeads": {}
}
```

Domains are `transform`, `sync`, `function`, `agent`, `evaluation`, `delivery`,
`indexing` and `maintenance`. Registration does not claim that a handler exists.
Workers explicitly claim only the domains they implement. Functions and agents
require `retryPolicy: "never"` and exactly one work task. Their code is never
automatically re-executed after loss of an executing owner. Other domains may
request `idempotent` retry, with at most three task attempts. A retry never changes
the pinned inputs, expected heads, artifact or normalized input.

References use canonical UUIDs and nonnegative decimal-string revisions, up to 64
inputs and 64 expected heads. The release is a lowercase SHA-256 digest. Input is
an envelope in value model v1 and is normalized against its pinned descriptor.
Request replay uses the original execution's descriptor, including after a new
binding is installed for the same actor/resource. Input hashing is specifically
`moria/runtime-input/v1\0`, then each of normalized descriptor JSON and normalized
value JSON prefixed by its unsigned 64-bit big-endian byte length, SHA-256 over
the concatenation. This is an execution-input digest, not a replacement for the
compiler's release/value fingerprint contracts.

Request identity is `(environment, resource, principal, application, requestKey)`.
Keys are bounded to 180 ASCII token characters. Repeated matching normalized input
and task plans returns the original ID; changed input or plans is a conflict.
Only this logical key is serialized for deduplication, with no installation-wide
enqueue or execution lock. Every execution stores the original spec and context;
database triggers reject later changes to pinned state and task plans.

## State, admission and fences

An execution moves `queued -> running -> preparing -> succeeded`. Failure and
cancellation are terminal. Task sealing leaves work private and cannot be reported
as a successful business operation. Tasks move `pending -> running -> sealed`;
failure/cancellation fences incomplete tasks. Attempt history remains retained.

Each claim locks a resource-class admission row briefly, chooses an execution and
task using `FOR UPDATE SKIP LOCKED`, and commits an owner UUID, increasing bigint
fence, lease deadline and attempt/event records together. Class limits cover both
global and per-environment running work and publication reservations. Defaults
are 32 global tasks and four per environment; operator configuration is bounded
to 1–1,024. This is a configured budget, not evidence of supported capacity.

Leases last at most 30 seconds and never extend beyond the execution deadline.
Database time controls claims, renewal, expiration and schedule eligibility.
Fence and sequence values cross JSON boundaries as decimal strings. Every native
write/release path must validate incarnation, owner, fence, lease and current
authority. The SQL task-write guard is
`moria.runtime_task_check(task_id, owner_id, fence)`, called in the same transaction
as task-private writes. Cancellation invalidates future renewals/seals/publication;
it cannot undo a committed success.

An expired idempotent task is reclaimed with a higher fence; its old attempt is
marked interrupted. An expired function/agent task fails the execution and cannot
be reclaimed. A sealed task can acknowledge the same result/digest again, but
changed results or old owners are rejected. Expired publication bookkeeping can
be reclaimed without re-executing sealed work. Authority revocation fails dispatch.

## Atomic business completion

After every work task seals, `prepare` claims a separate publication lease.
The domain publication authority holds its normal environment/filesystem admission,
checks expected heads and performs all domain effects, checkpoints, receipts and
completion in **one PostgreSQL transaction**:

```sql
BEGIN;
-- Domain-owned current-authority, expected-head, publication and receipt writes.
SELECT moria.runtime_execution_complete(execution_id, owner_id, fence, result_json);
COMMIT;
```

The helper verifies current authority, the publication lease and all task seals.
Transaction rollback removes both effects and completion. A committed matching
receipt can be replayed without another effect. There is deliberately no detached
completion CLI/RPC. A worker acknowledgment, task seal or queue acceptance is not
business completion. External effects require a separate transactional outbox
contract and cannot be made exactly once by this helper.

## Scheduling and events

Intervals are 60–604,800 seconds. Each automation stores immutable normalized input,
plans and binding. Reconfiguration may toggle it or change the interval; changing
pinned input/binding needs a new automation ID. Concurrent schedulers lock due
automation rows with `SKIP LOCKED`, record the unique `(automation, scheduled UTC
instant)` firing and advance `next_run` atomically. This row ownership cannot
outlive the database transaction. Missed intervals coalesce to one firing. The
last enqueued execution determines overlap; overlaps and revoked authority create
explicit `overlap`/`denied` records without another execution. No process-wide
supervisor lock is held while work runs.

Execution events have increasing execution-local bigint sequence numbers. Follow
reads at most 256 events after an exclusive decimal-string cursor. Progress
payloads are at most 8 KiB, with at most 256 custom events per execution. State
events remain available even when the custom log budget is exhausted. No event
expiry/GC is introduced; the eventual 30-day retention contract requires pin-aware
lifecycle implementation and explicit expiry responses.

## Native control interface

Operator commands use `moria-indexing execution COMMAND` and one JSON document on
stdin (at most 128 KiB). Credentials use the native runtime's external environment
secret reference. The following are internal control examples; IDs are supplied
by trusted composition.

| Command            | Input                                                                                 |
| ------------------ | ------------------------------------------------------------------------------------- |
| `request`          | `{binding, requestKey, input, tasks:[{key, partition}]}`                              |
| `status`, `cancel` | `{id}`                                                                                |
| `claim`, `prepare` | `{owner, domains:["transform"]}`                                                      |
| `renew`            | The returned lease object                                                             |
| `seal`             | `{lease, result, digest}`                                                             |
| `event`            | `{lease, payload}`                                                                    |
| `events`           | `{id, after:"0"}`                                                                     |
| `schedule`         | `{id, binding, interval:60, enabled:true, input, tasks}`                              |
| `tick`             | `{}`; perform one bounded expiry/scheduling pass                                      |
| `scheduler`        | No stdin request; start immediately and repeat native reap/ticks until SIGINT/SIGTERM |

At most 256 tasks are admitted per execution. Plans contain only a bounded key
and optional partition number below 4,096. Sealed task results and business
receipts are at most 64 KiB. Domain deadlines are at most 30 seconds for functions,
10 minutes for agents, 60 minutes for evaluations and 30 minutes otherwise.
Cancellation is authoritative for state/publication; actual sandbox termination
and orphan cleanup follow the separate [sandbox contract](sandbox.md).

## Public cancellation control

Function, Agent, Evaluation and Pipeline `POST .../{id}/cancel` returns exactly
`{id,cancelRequested:true}`. SDK and CLI clients use this control receipt. It does
not reveal terminal state, whether any child was accepted, result, error, usage,
logs or cleanup completion. The same receipt applies if a business receipt already
committed. GET/events/case/receipt inspection retains its full current source and
observation proofs; interrupted pending observations can continue to return 409.

The authenticated root owner must retain the exact root operation, current
Source/code-file ACL and current registered purpose. Pipeline ownership follows
its recorded requester, distinct from the internal service actor. Cancellation
does not read dynamic child artifacts to authorize or acknowledge the command.
Unrelated actors and public JSON cannot choose a control identity or family.

The authorized transaction inserts one immutable cancellation signal with the
actual requester and database clock. Its audit write touches the root authority
row even after success, so a previously opened repeatable-read transaction cannot
retain a stale current-parent proof. Live ancestry checks use indexed signals and
immediately fence claims, callbacks, admission and publication. Already committed
business receipts and statuses remain intact; receipt-only replay distinguishes a
later cancellation from one preceding completion and never restores general IO.

Descendant bookkeeping expands through the indexed accepted parent relation in
batches of at most 64, one execution transaction at a time. It never holds a
parent execution lock while waiting for a child. The scheduler and normal claims
continue expansion; caller acknowledgement is not a population-wide traversal.
Physical cleanup still requires actual terminal container/removal proof, and hard
spill remains reserved until that cleanup completes. This new control contract
requires its fresh PostgreSQL/OCI regression before qualification.

## Verification obligations

`tests/native-execution.integration.test.ts` exercises actual Rust commands and
PostgreSQL, independent task owners, class admission, stale fences, task replay,
atomic publication rollback/replay, actual commit-response loss followed by
SIGKILL, single-attempt failure, deadlines, event bounds, session disable and
parallel scheduler firing/coalescing/overlap/revocation. Passing this foundation
does not certify execution throughput, sandbox isolation, external delivery,
separate-host failover, restore or all Moria components.
