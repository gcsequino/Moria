# Versioned effective serving projections

Status: native construction, replay, activation and action maintenance have real
PostgreSQL/native-runtime integration coverage. Typed operational query integration
and complete workload proof remain in progress; this is not a capacity pass.
Retained functional results and exact artifact identities are in the
[native projection evidence](../evidence/2026-10-30/native-projection/README.md).

The projection is derived from one immutable native source stage and its complete
editable overlay history. It is not an authorization authority. Every serving
request retains verified session, current resource/property access, pinned and
current canonical row policy, pinned and current effective row policy, deletion
checks and the existing cursor's immutable original expiry. All restrictions and
the caller filter precede order, aggregation and LIMIT.

## Trusted binding and physical data

An operator provisions a versioned `runtime_projection_bindings` entry for an
environment/object resource. Its immutable definition declares sparse source and
effective typed fields, sort layouts, reader database roles, row/batch budgets and
bounded DDL admission. The combined typed field count is at most 128; full source
and effective JSON remain present even for unmaterialized properties. This avoids
the heap tuple limit for hundreds of worst-case TOAST pointers while retaining
every object and property.

The bounded TypeScript indexing registrar pins `projectionRevision` into the
immutable indexing definition and fingerprint. A layout-only revision change can
therefore create a fresh native build against the same Dataset head. A native
build cannot silently adopt a different expected layout revision.

Each candidate owns a logged table named only from its server-generated UUID.
The schema-owner provisioner uses fixed search paths, quoted UUID-derived names,
strict C03 type whitelists and trusted role references. Callers never submit SQL,
identifiers or physical table names. Runtime service credentials retain their
existing nonowner/non-CREATE restrictions. Bulk construction and query execution
use the native runtime and PostgreSQL.

Rows retain `uid`, `from_revision`, nullable `to_revision`, tombstone state,
canonical `source` JSON and overlay `effective` JSON. Stored typed source/effective
columns use strict immutable C03 codecs. Source rows are never replaced by edited
values. Null sorts last in either direction; UID ascending is the final tie.
String comparison follows Unicode code-point order under PostgreSQL `C`
collation, including astral characters; canonical JSON's UTF-16 key ordering is
a separate serialization rule.

## Preparation, replay and activation

1. Capture environment revision R and the exact immutable stage/schema/binding
   anchors under a short protected admission. Release the environment barrier.
2. Build source plus latest overlays at or before R in bounded UID pages. Each
   page commits its progress under the current native owner/fence; no population
   is accumulated in process memory. PostgreSQL batch and row byte budgets apply.
3. Build declared physical indexes under bounded DDL admission, outside the
   environment barrier. Validate exact source UID membership and counts.
4. Replay every committed overlay after R in revision/UID order from the durable
   overlay journal. Save a complete replay watermark, including revisions with
   no relevant edits. Failed/expired owners resume only under a fresh native fence.
5. Enter the existing P03 exclusive environment barrier. Catch only the bounded
   tail; a larger tail releases the barrier and returns to off-lock catch-up.
   Assert current stage/schema/layout anchors. Activate the source generation and
   projection in the same authority transaction. A crash or conflict exposes
   neither a partial source generation nor a mismatched serving index.

Every preparation statement has a finite timeout capped by both the declared DDL
budget and the remaining native build deadline. Two transaction-scoped DDL slots
bound concurrent index construction. A busy slot or oversized final replay tail
preserves committed progress, releases publication locks and resumes under a new
native coordinator fence. No authority result is cached between pages.

Every later overlay write maintains the active projection in the action's
transaction, including clear-override, tombstone and restore. Historical versions
remain available for pinned cursors. Source disappearance never deletes retained
overrides; future reappearance applies them. The next source candidate replays
them against its own canonical data. An active projection cannot silently omit a
canonical source object or skip a failed update.

## Native serving interface

`projection::resolve(client, environment, resource, pinnedR, currentR)` returns an
optional `ProjectionPlan` with `pinned` and `current` relations. Each relation
contains a trusted qualified table reference, projection/stage identity,
activation and closed revision bounds, normalized schema, typed source/effective
column mapping and exact declared layout vectors/index names. No caller-authored
SQL fragment is accepted.

At revision R the relation includes versions with `from_revision<=R` and
`to_revision>R OR to_revision IS NULL`, excluding tombstones. The newest active
relation may use its current-row partial indexes under the protected current
revision. A pinned old relation joins the current active relation by UID. Both
canonical/effective restrictions remain mandatory before LIMIT.

The existing typed policy compiler emits virtual quoted field identifiers;
serving substitutes only trusted projection column mappings, preserving parameter
offsets, exact scalar types and three-valued logic. Unmaterialized fields retain
the correct bounded JSON evaluation path. Projection availability never changes
authorization semantics.

## Index width, overflow and bounded fallbacks

Sort layouts declare exact scalar vectors/directions and the UID tie. PostgreSQL
B-tree tuple widths are finite. Per-layout generated key-safety predicates keep
oversized keys out of the normal B-tree without truncating values, rejecting
valid source data or dropping objects.

A matching layout always combines authorized/filter normal rows from the index
with authorized/filter overflow rows using an exact bounded PostgreSQL sort.
Each branch produces its true TOP K; their union is ordered by the full typed
keys and UID before the final LIMIT. Caller routing never tests or returns a
hidden global overflow count. Operator-only evidence distinguishes zero-overflow
indexed plans from overflow work.

General scalar sort without a compatible layout uses a correct bounded engine
sort to preserve C03. The existing explicit UID-scope bounded path remains
available. Neither fallback is an indexed-profile pass. Admission, SQL statement,
work memory, temporary-file, full-operation and response budgets are mandatory;
exhaustion returns RESOURCE_LIMIT without a partial sorted result. A server-side
byte preflight on the final authorized TOP K precedes large JSON wire transfer;
property projection and the public response cap remain enforced afterward.

N18 retains concrete projection tables, generation references and all active
cursor/job/search/backup pins before permitting a drop. No destructive projection
collector is enabled without that recovery-set proof. Functional tests must cover
mid-build actions, replay races, schema/layout changes, takeover, atomic source
activation, pinned cursors, tombstones/restores and exact C03/PostgreSQL ordering.
Indexed 1m/10m mixed-load plans and every width/history class require separate
retained evidence; bounded fallback never relabels an unsupported profile as a
capacity pass.
