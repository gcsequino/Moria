# Native transactional delivery v1

The Rust delivery dispatcher consumes durable outbox intent independently of the
action or dataset publication that created it. A failed, cancelled or uncertain
delivery never changes a successful action receipt into a rolled-back action.
This is at-least-once delivery when the registered destination supports a stable
idempotency key. It does not promise external exactly-once effects.

## Trusted destination binding

An operator/release installs a row in `runtime_delivery_destinations` referencing
an immutable `runtime_execution_bindings` row. The execution binding has domain
`delivery`, protocol version 1, a stable destination resource UUID and required
operation `<destination-resource-UUID>:write`. It fixes the actor/application,
session if applicable, security epochs, installation incarnation and baseline
markings. The destination row fixes its endpoint, secret reference, idempotency
capability, accepted markings, attempt limit and retry delay. Only its enabled
flag can change. New configuration needs a new binding row; accepted intent pins
the old destination configuration and bytes.

Endpoints are explicit HTTPS URLs with no userinfo, query or fragment. Redirects
are disabled, ambient proxies are ignored and TLS certificates/hostnames are
verified. `MORIA_DELIVERY_CA` optionally supplies a trusted CA PEM. No caller URL
or provider credential appears in a delivery request or task plan. An optional
secret-reference name resolves from the dispatch host's external environment;
its value becomes a Bearer header and is never copied to events/results. Secret
binding is privileged operator composition, not an end-user secret lookup API.

`MORIA_DELIVERY_ALLOW_LOOPBACK_HTTP=1` permits literal loopback HTTP endpoints for
isolated fixtures. It does not admit remote plaintext hosts or `localhost` DNS
indirection. Production destinations use HTTPS.

## Intent and atomic domain commit

The action/publication authority invokes this helper in its existing transaction,
after creating the corresponding authoritative receipt:

```sql
SELECT moria.runtime_delivery_enqueue(
  destination_binding_uuid,
  logical_effect_key,
  payload_jsonb,
  complete_inherited_source_markings,
  origin_jsonb
);
```

Action origins are exactly `{kind:"action",environment,application,principal,action,key}`.
The actor/environment must match the destination's execution binding and the
non-null action receipt must exist in `moria.action_keys`. Publication origins are
exactly `{kind:"publication",environment,execution}` and require that publication
receipt. Every origin field is a string. Public admission must derive these fields
from authenticated execution context; neither actor nor source markings come
from arbitrary caller JSON. The domain authority supplies the complete union of
inherited markings, including policy-derived reads. Destinations must accept that
union and the current principal must retain its clearance and operation grant.

Intent is deduplicated by stable destination resource, complete origin scope and
logical effect key. This preserves action identity's environment/application/
principal/action/key scope even if two actors use the same human-readable key.
Matching payload, origin and markings replay the original delivery ID. Changed
content conflicts. A new destination binding cannot redirect accepted intent.

The SQL authority stores both payload JSON and the exact `jsonb::text` wire bytes
at enqueue, plus SHA-256 of those UTF-8 bytes. All attempts reuse those stored
bytes and verify their digest before dispatch. This digest is not a replacement
for release/value-model fingerprints. Payload bytes and provenance are immutable.
The native control interface has no detached enqueue command; successful queue
acceptance or worker acknowledgment cannot stand in for atomic domain commit.

## Claim, dispatch and result protocol

Workers use per-delivery `SKIP LOCKED` claims with an owner UUID, increasing bigint
fence, installation incarnation and at most 30-second lease. Execution and delivery
claims lock the same resource-class admission row and count all live execution
tasks, publication reservations and delivery leases. Sealed/unleased work is not
double-counted. Global and per-environment class limits remain configured budgets,
not measured capacity claims.

Before external dispatch the worker rechecks current authority under shared
environment/filesystem admission and commits a durable `sent_at` marker. That is
the bounded dispatch admission point. Revocation afterward cannot undo an admitted
external operation. The database transaction is not held while waiting on the
provider. A trusted worker may record the observed provider receipt after that
admitted operation even if authority subsequently changed; public status reads
still require current authority. Old owners cannot overwrite a replacement.

The HTTP operation is one POST with:

| Field                                  | Value                                      |
| -------------------------------------- | ------------------------------------------ |
| `Content-Type`                         | `application/json`                         |
| `Idempotency-Key`, `Moria-Delivery-Id` | Stable delivery UUID                       |
| `Moria-Delivery-Attempt`               | Current decimal-string fence               |
| `Moria-Payload-Sha256`                 | Exact stored wire-byte digest              |
| Body                                   | Exact stored payload bytes, at most 64 KiB |

A successful destination returns a 2xx JSON receipt, at most 16 KiB:

```json
{
  "version": 1,
  "deliveryId": "00000000-0000-0000-0000-000000000001",
  "payloadHash": "64-lowercase-hex-characters-matching-the-request",
  "receiptId": "bounded-provider-receipt-reference"
}
```

Unknown fields, wrong IDs/digests, invalid receipts, redirect responses, oversized
responses and network failures are unconfirmed outcomes. HTTP timeout is at most
10 seconds and never exceeds remaining lease/deadline time. No raw provider error
body is saved to platform logs. Receipt IDs are bounded ASCII tokens.

## Retry, uncertainty and cancellation

States are `pending -> delivering -> delivered`, or `retryScheduled`, `failed`,
`uncertain`, `cancelled`. Default automatic attempts are three, configurable from
one to ten. Backoff doubles from a registered 1–300-second delay, capped at five
minutes; an automatic cycle has a 30-minute deadline. Attempt/fence history is
retained across explicit dead-letter retry.

An idempotent destination may retry an unconfirmed or interrupted dispatched
attempt with the same delivery ID and bytes. The destination must actually dedupe
that key; setting a capability flag does not prove that an arbitrary provider
implements it. A non-idempotent destination enters `uncertain` after any ambiguous
dispatched attempt and never automatically retries. A claim abandoned before its
durable sent marker is safe to reclaim with a higher fence. Missing configuration
before dispatch produces a dead letter without an external call.

Cancellation before dispatch stops future sends. After a possibly dispatched
attempt it reports uncertainty, because cancellation cannot retract an external
effect. Cancelling a delivered record leaves it delivered.

Privileged operator reconciliation requires the current uncertain fence, a
`delivered` or `notDelivered` resolution, and a durable provider-evidence reference.
It records immutable reconciliation history. Delivered evidence records the
observed external outcome without resending. `notDelivered` is valid only when the
provider has proved that the old attempt is terminal and cannot apply later;
an ordinary `not found` lookup is insufficient. Resending also requires current
authority. This is an explicit operator procedure, not an automatic generic
provider-reconciliation claim. Worker peers must not receive this privilege.

## Native operator interface

`moria-indexing delivery COMMAND` accepts one bounded JSON input on stdin.
`worker` runs the Rust dispatcher until SIGINT/SIGTERM. Host/process loss after
the sent marker is recovered according to the destination capability above.

Run `reconcile` separately with `MORIA_INDEXING_DATABASE_SECRET_REF` resolving
to an operator database login with `CREATE` on the `moria` schema. The dispatcher
and ordinary delivery commands still require a non-owning runtime login. PostgreSQL checks the
operator login, exact uncertain fence and evidence, and records the transition
and its history in one transaction. A worker cannot reopen an uncertain delivery
by updating its row or writing reconciliation history directly.

| Command            | Input                                                                 |
| ------------------ | --------------------------------------------------------------------- |
| `once`, `claim`    | `{owner:"UUID"}`; owner comes from authenticated peer identity on RPC |
| `renew`            | Returned delivery lease                                               |
| `status`, `cancel` | `{id:"UUID"}`                                                         |
| `retry`            | `{id,expectedFence:"1"}`; current dead letters only                   |
| `reconcile`        | `{id,expectedFence,resolution,evidence}`; privileged operator only    |
| `register-writeback` | Operator-only Action writeback registration; see [Connector writeback](connectors-secrets.md#action-origin-writeback). |
| `worker`           | `{}`                                                                  |

Status remains independent of the originating action result. It exposes receipt,
attempt count, hash and origin, not raw credentials. There is no raw provider
acknowledgment RPC that can falsely mark work delivered.

## Evidence and remaining boundaries

`tests/native-delivery.integration.test.ts` uses actual Rust processes, PostgreSQL
transactions and a deterministic HTTP destination. It exercises rollback and
invisible intent, shared execution/delivery admission, identical-byte retries,
actual response loss, SIGKILL/reclaim, uncertainty/reconciliation, cancellation
after dispatch, dead letters, source markings and revocation. These local tests
do not certify arbitrary provider behavior, separate-host failover, throughput,
cost or restore safety. Restore must keep dispatch disabled until external effects
are reconciled; no automatic retention/GC or production restore is added here.
