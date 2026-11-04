# Native operational serving contract

Version 4. Protected point reads, UID pagination, typed sorting,
aggregation, UID-reference traversal, declarative single/multi-target actions
and durable receipts run in `moria_runtime::serving`. The protected path
calls native authority and policy code directly. Node does not execute these
operations or mediate a subprocess for each request.

This contract describes the implemented native boundary. Aggregates require
explicit budgets and support either whole populations or inclusive UID scopes;
inverse traversal requires an explicit UID scope. General scalar sorting
uses a matching versioned projection layout or a bounded PostgreSQL engine sort.
Workload profile certification, capacity, HA and the P03
ordering replacement remain separate proof obligations.

## Trusted definitions and authority

The migration owner creates `moria.native_serving_bindings` and
`moria.native_serving_actions`. Trusted release composition validates/upserts
definitions under the existing environment admission lock; application request
JSON cannot register or replace them. Object rows contain environment, resource,
immutable object UUID, binding revision, complete definition and enabled state.
Action rows identify the object resource and immutable definition revision.

Object definitions use the existing `ObjectBinding` JSON fields: `objectTypeId`,
`schema`, `policies`, optional `requiredOperations`, `writePolicy` and
`propertyWrite`. The schema's primary key is a non-null permanent UID. A UUID
route resolves that UUID exclusively and never falls back to a name binding.
The definition's policy resource and object UUID must agree with its row.

`Authority::authenticate(token)` accepts a 43-character opaque platform session,
hashes it server-side, and constructs a private `VerifiedSession`. Principal,
application, environment and token hash have read-only getters; callers cannot
deserialize a request body into a verified session. Current workload/browser
session validity, enabled principal/application, grants, scopes, markings, human
synchronization/provider freshness and instance-principal binding are checked
inside each protected transaction and again before commit/release.

`moria.native_serving_resource_access` preserves current filesystem resource
activity, inherited ACLs, principal/group/administrator subjects, explicit deny
precedence, cross-environment denial, retained markings and provenance/release
dependency closure. A legacy resource with no filesystem owner retains its
existing declaration-based authorization; a filesystem-owned resource cannot
drop those additional checks. Private table/schema credentials never enter an
application request or definition.

## Service and HTTP boundary

Native callers use `NativeServing { db }` with an authenticated session:

- `read(session, reference, uid, projection) -> Result<JSON>`
- `query(session, reference, request) -> Result<JSON>`
- `aggregate(session, reference, request) -> Result<JSON>`
- `traverse(session, link_reference, request) -> Result<JSON>`
- `invoke(session, action_reference, idempotency_key, input) -> Result<JSON>`

`serve(db, SocketAddr)` starts an explicit loopback workload listener; it prints
its bound address and drains admitted requests on SIGINT/SIGTERM. External/native
service transport uses the separate authenticated runtime protocol. HTTP routes
retain `/api/v1` and the prototype `/v1` alias:

| Operation               | Request                                                                      | Success body                     |
| ----------------------- | ---------------------------------------------------------------------------- | -------------------------------- |
| Point read              | `GET /api/v1/objects/{reference}/{uid}?properties=uid,name`                  | `{revision,object}`              |
| Pinned query            | `POST /api/v1/objects/{reference}/query`                                     | `{revision,objects,nextCursor?}` |
| Bounded aggregate       | `POST /api/v1/objects/{reference}/aggregate`                                 | `{revision,groups,types}`        |
| UID-reference traversal | `POST /api/v1/links/{reference}/traverse`                                    | `{revision,objects}`             |
| Declarative action      | `POST /api/v1/actions/{reference}/invoke`, `Idempotency-Key`, body `{input}` | `{revision}`                     |

Bearer authentication is required. Host must match the bound listener; Origin
headers and non-loopback ingress are denied. JSON request bodies are at most
65,536 bytes; unsupported content encodings and unknown request members fail.
Read/query deadlines are 30 seconds including final checks. Ordinary overlay
actions have one five-second deadline across preparation, publication and at most
three re-preparation/serialization attempts. Function-backed calculation retains
its separately declared execution limit before the bounded action transaction.
Cancellation or a failed cleanup discards the connection instead of returning an
open transaction/advisory lock to the pool. A timeout during COMMIT remains an
unknown delivery outcome resolved by the same idempotency key.

Errors contain `error.code`, static `message`, `requestId`, `correlationId`,
`retryable` and `details:null`; no hidden row values or SQL diagnostics are
returned. Expired result bodies return RESULT_EXPIRED (410); immutable key
tombstones still prevent re-execution. A complete program-wide C07 matrix
conformance run remains required before claiming public API compatibility.

## Ordinary overlay action preparation

Single-target and multi-target `set`, `clearOverride`, `delete` and `restore`
actions prepare under the existing shared environment/filesystem admission.
Preparation retains the complete source/override evidence for every observed
`(resource, UID)`, including an unmodified primary object used by expressions.
The private plan is limited to 101 observed objects and 64 MiB. It has no request
constructor or deserialization surface and carries data/version evidence only.

Preparation releases all admission locks before publication. A receipt-specific
lock, derived from the full environment/application/principal/action/key tuple,
is acquired before the exclusive environment publication barrier. Publication
reauthorizes the actual current caller, exact Action/Object contracts and current
Source/dependency ACLs/markings. It compares security/instance epochs, typed
principal attributes, every captured source generation and each observed overlay
version, then reevaluates current and generation-captured row/property/write
policies, conditions and value expressions. An anchor change discards the plan
and restarts preparation within the original deadline. No stale success or
partial multi-object result is returned.

Only the resulting atomic revision, overlays/projection updates, receipt,
audit/change frame and delivery effects execute in the publication transaction.
Same-key receipts remain reauthorized on replay. Create, link/unlink, explicit
source attachment, Function-prepared edits and sandbox proposal transactions
retain their established admission path until their additional footprints have
independent publication proofs.

`moria.native_action_resource_access` derives the exact resource/read/write ACL
demands from the enabled trusted Action and Object definitions. It accepts an
actual session hash and Action reference, never caller state or a demand list.
The internal batch memoizes only within that SQL call; current authorization is
repeated for publication. Rust rejects missing, duplicate or misaligned resource
identities in its result. Curated Action invocation does not acquire an invented
Object write capability. Direct filesystem operations retain their own explicit
filesystem permissions.

The immutable native proof `20261031T105001264841Z` passed 26 real cases,
including cross-replica same-key races, changed primary/secondary inputs,
replacement Source generations, policy/grant revocation, complete rollback and
scalar/batch Source ACL parity. This is functional evidence; the production
throughput/latency profile still requires a new measured run.

## Projection page inputs

When pinned and current revisions identify the same current projection relation,
the page plan uses one row alias while retaining both independently compiled
policy contexts. Historical pages keep the current-row join and deletion checks.
The four returned internal maps contain the validated AST dependencies for
selected values, predicates, sort/aggregate inputs and current plus captured
source/effective row policies. Raw Dataset source fields use the canonical source
column mapping; they never substitute a similarly named mapped effective field.

Materialized values use the shared C03 scalar encoders. Arrays preserve ordered
scalar encodings, null items, empty arrays and null arrays; unmaterialized values
use exact JSON extraction. Final authorized TOP-K byte preflight, response limits,
statement/work/temp budgets, cursor pins and retained-read witnesses remain in
force. Property authorization still precedes population filtering/ranking/limit.

The immutable `20261031T111842652683Z` proof passed 38 cases, including independent
point/query codec parity, old cursors across projection/source replacement,
current row/property restrictions, overflow membership, revocation and Action
races. In its 300-row, 4-KiB diagnostic, exact protected UID/rank page SQL used
26/47 shared-buffer hits instead of 743/885 in the previous plan. These are
small functional diagnostics, not throughput or production-scale certification.

## Reads and cursors

Query shape is `{filter?,projection?,pageSize?,cursor?,sort?,scope?,budget?}`. Page size defaults to
100 and is at most 1,000. Projection names are unique; denied projection and
predicate fields fail before data is read, including empty populations. The
filter uses only fields/literals and accepted typed boolean operators, at most
256 nodes/depth 16. Request-supplied principal attributes/parameters are denied.
Default pagination uses UID ascending. Custom sorting uses up to four distinct
`{property,direction:"asc"|"desc"}` scalar keys, NULLS LAST in either direction,
Unicode `C` text ordering, and UID ascending as the final tie-breaker. Array keys
are explicitly unsupported. Scope and budget are optional for sorting and must
occur together when supplied.

Readers acquire environment/filesystem shared admission before a READ COMMITTED
transaction. Domain writers require the exclusive admission, and explicit revision
and generation pins preserve the data view. Fresh statement snapshots let final
authority checks observe a credential revocation committed during computation.
Active source schema and required markings must match the trusted binding. Pinned
and current canonical row/effective-object restrictions and current tombstones
apply before SQL limiting. Native policy evaluation verifies the returned rows;
SQL/reference disagreement is a failed operation, not a silently filtered page.
The final authorized page has a server-side 64 MiB sum limit across pinned/current
source/effective JSON before those values enter the driver. Decoded candidate
states retain the same 64 MiB cap and public response objects remain at 8 MiB.
These are safety limits, not the workload profile's smaller response target.

Cursors are random opaque 32-byte references, rendered as 43-character base64url
tokens. Only their SHA-256 hash is stored. Binding includes environment,
principal/application/delegation, immutable contract revision and the canonical
query. Security epoch is checked separately. Every continuation retains its
original revision and exact 15-minute deadline. Concurrent continuations and
different native replicas can resume the same reference without extending it.
Expired/invalidated cursors fail explicitly and never restart from current data.
Custom cursors persist `{uid,keys}` with normalized typed values from the pinned
effective state. Continuations bind the original scope, budget and sort as well
as the existing identity/contract/query fields; changed sort or budgets cannot
reinterpret a cursor. Current overlays can revoke visibility but cannot change
the pinned ordering keys.

## Actions, replay and external effects

Action definitions retain typed inputs, condition, ordered edits, target
parameter, markings and revision. The target parameter is a UID or non-null UID
array; a call touches 1–100 distinct targets in one environment/object type.
Supported edits are set, clearOverride, delete and restore. UIDs cannot change.
Canonical/effective visibility, object/property write policy, conditions,
filesystem authority and post-state restrictions are checked transactionally.
Current source data plus prior overlays determine the effective value; source
refresh does not clear overrides or explicit tombstones.

Environment revision allocation, every target overlay, action receipt, native
audit and delivery enqueue commit atomically. Failed later targets roll back
earlier targets and do not reserve a successful key. No provider call or user
function runs inside the transaction. Function-backed actions prepare through
the native function runtime before acquiring the mutation transaction, pin the
action/object contract, check the immutable prepared receipt/read set and use
typed output fields as property edits. Prepared marking flow, publication and
the owning receipt mapping commit together. Replays check current prepared
evidence and skip user-code execution.

Receipt scope is environment/application/principal/stable action/key. Native
receipts also pin input schema and original object resource in
`native_serving_receipt_inputs`, so a changed action definition cannot reinterpret
an accepted request or execute it again. Same key/different normalized input
conflicts. Replay rechecks current action and result-resource access. Expired or
removed result bodies leave non-reusable key tombstones. Legacy receipts lacking
input metadata use the current schema only when action revision matches;
otherwise only an exactly matching canonical input hash is provable. No metadata
or normalization is invented for a historical receipt; broader coexistence
requires an explicit migration proof.

Trusted optional effects are `{destination: UUID,key: boundedEffectName}`. Payload
is fixed as `{version:1,action,key,result}` and logical key is
`action/{action}/{actionKey}/{effectKey}`. After inserting the receipt, the same
transaction calls `moria.runtime_delivery_enqueue` with the union of declared
object/action and current filesystem/provenance markings. Caller-supplied
destination payloads are not admitted. Delivery uncertainty remains separate
from the committed action outcome.

## Explicit bounded capability

`scope` is `{fromUid,throughUid}` with inclusive canonical UID bounds. `budget`
is `{scanRows,responseBytes,timeMs,groups?}`: scanRows 1–10,000, responseBytes
1–8,388,608, timeMs 1–30,000, and groups 1–1,000 when provided. Scope and budget
occur together. The whole-operation deadline includes admission, SQL, decoding,
final authority check and commit. The decoded data cap remains 64 MiB.

`scanRows` bounds fully authorized candidates admitted to sorting/grouping after
the caller filter, both pinned/current canonical/effective restrictions and
current deletions. It does not bound physical index/heap visits: those are
subject to the operation deadline and deployment resource limits. Counting raw
rows for this budget would expose hidden population size. The SQL plan uses
generation/UID-range indexed input, a MATERIALIZED authorized relation limited
to scanRows+1 and a mandatory overflow guard. Sorting/grouping runs inside SQL;
the process does not collect a population to sort or aggregate it. An overflow
returns RESOURCE_LIMIT (429), with no partial total, ordering or fanout response.

The versioned projection resolver accepts only complete source generations and
current immutable layout bindings with overlays applied through the admitted
revision. SQL uses typed source/effective columns where declared, both pinned
and current row/object restrictions, current deletions and authorized predicates
before ranking. Historical continuations join their original relation to the
current active relation by permanent UID. Exact matching layouts run two TOP K
branches: finite-width B-tree keys and complete oversized-key engine sorting,
then merge using the full typed keys and UID. Oversized keys are never truncated
or omitted, and hidden global overflow counts never select caller behavior.

Missing projections or layouts retain complete PostgreSQL engine ordering under
the request deadline, a local 16 MiB work-memory limit, finite operator-configured
temporary-file limit of at most 256 MiB, and the page/response limits above.
Unlimited temporary-file configuration fails RESOURCE_LIMIT. Exhaustion never
returns partial ordering. Engine fallback and overflow branches do not establish
indexed performance certification. Arbitrary inverse-link indexing, mixed-load
profile certification and capacity/HA proof remain separate. P03 environment
ordering remains intact.

## Aggregates

Request shape is `{filter?,groupBy?,metrics,scope?,budget}`. It rejects cursor,
projection, sort and pageSize. `groupBy` has at most four distinct scalar fields;
`metrics` has 1–16 named entries with aliases of 1–100 ASCII letters, digits,
underscore or hyphen. Each metric is `{op,property?,outputScale?}`. Count has no
property. Sum/average require int64, decimal or float64; min/max accept scalar
types. Every filter/group/metric property is authorized before querying,
including empty scopes. Group budget is required. `avg` aliases `average`.

The explicit UID-scope profile retains its 10,000-row maximum. Without scope,
scanRows is mandatory and bounded at 1–1,000,000,000; groups at 1–1,000;
responseBytes at 1–8 MiB; and timeMs at 1–30,000. The whole-population profile
requires a trusted finite PostgreSQL temp_file_limit of at most 256 MiB and sets
local work_mem to 16 MiB. A versioned projection uses its trusted typed columns
for policy predicates and selected aggregation fields. Otherwise the engine
uses the canonical pinned/current relation. Only authorized selected fields are
materialized. Complete scan, group-count and result-byte guards run in SQL before
any result row reaches the driver. Budget exhaustion returns RESOURCE_LIMIT and
never a partial total or subset of groups. Statement deadline errors are explicit.
This engine fallback supplies correctness, not an indexed grouping/capacity claim;
physical grouping layouts and workload certification remain separate obligations.

Count is a non-null int64 string. Other metrics ignore null inputs and return
null for empty input. Ungrouped empty input returns one group with `{}` key;
grouped empty input returns no groups. Int64 sum rejects overflow. Decimal sum
has precision 38 and retains input scale; overflow rejects the operation.
Float64 outputs must remain finite. Exact int64/decimal average requires an
explicit outputScale 0–38 and returns precision-38 decimal rounded half away
from zero. Float64 average uses float64 and rejects outputScale. No exact input
is silently converted to floating point. The `types` map gives each output
descriptor. Groups order by typed group keys ascending, NULLS LAST.

For example, `{metrics:{total:{op:"sum",property:"amount"}},scope,budget}`
returns `{revision,groups:[{key:{},values:{total:"12.50"}}],types:{total:{type:
"decimal",precision:38,scale:2,nullable:true}}}` when amount has scale two.

## Trusted UID-reference links

Traversal uses a trusted immutable many-to-one mapping whose target
key is the permanent primary UID, so target uniqueness is enforced by existing
generation/action storage. Other target keys require enforced unique mappings
before activation/commit; discovering duplicates at query time is insufficient.
`native_serving_links` stores environment, stable resource, immutable revision,
source/target resource bindings, enabled state and strict definition
`{mode:"uidReference",sourceProperty,markings}`. The source property must be a
UID scalar. Request JSON cannot provide or replace a definition.

Request is `{uid,direction:"out"|"in",projection?,scope?,budget,fanout}` with
fanout 1–1,000. Link read authorization, filesystem closure, both endpoint read
policies and both key-property permissions apply before any lookup. Missing or
hidden anchor returns NOT_FOUND; a missing/hidden destination yields no edge.
Outbound traversal is an indexed permanent-UID point lookup returning at most
one object. Inverse traversal requires source UID scope and budget and applies
the authorized SQL UID-reference predicate. It fails if fanout or authorized
scan budget is exceeded; it has no partial pagination. Both directions use one
explicit admitted revision and repeat current authority before release.

SDK integration should expose `queryBounded`, `aggregateBounded` and
`traverseUidReference` as named capabilities until the full indexed profile is
proved; these names must not advertise whole-population filteredSorted coverage.

## Native function broker

`platform::NativePlatformBroker::new(db)` implements the required native
`sandbox::PlatformBroker` and `functions::FunctionPolicy` traits. The production
composition provides no permissive default broker. The sandbox context is
constructed from a trusted execution lease. Its active private credential is
checked against the exact lease and current actor; accepted immutable user/service
delegation ceilings intersect current grants, application scopes and clearance.
Guest HTTP headers cannot supply a session or execution actor.

Object point/query/aggregate reads call the native library within the protected
admission. `native_function_reads` records immutable binding revision, pinned
read revision, authorized fields, actual returned/scoped-aggregate-input UIDs and
environment-qualified declared/filesystem/provenance labels before a response
leaves that transaction. Exact C03 fingerprints cover the actually consumed
projection/predicate/order/group/metric fields. Unrelated hidden fields do not
affect retained-result availability. Separate minimal admission policy witnesses
preserve historical canonical/effective restrictions under current attributes.
Query evidence re-runs the authorized typed query and original position to prove
its consumed UID/order/value set; advancing a source generation with unchanged
consumed values does not alone invalidate a result. Whole-population aggregation
stores the immutable typed query and a bounded group-result/type hash instead of
a population of UID witnesses. Retained release repeats current property/resource
authority and the original pinned/current row/effective policies, then recomputes
the exact observed aggregate. An unchanged permitted aggregate remains releasable;
changed consumed output is withheld. Trusted own writes refresh that summary only
after strict prior-evidence checks in the same admitted mutation transaction.
Point-read-only/edit-proposal ancestry cannot issue aggregates.
Input/output ontology-object references recursively use
the same point-read boundary, with at most 256 references. The evidence ledger
has at most 512 entries, each at most 1 MiB and 256 read groups. Retained function
results/events recheck actual old/current canonical/effective visibility, current
property permissions, source schema, binding revision and filesystem closure
using the current verified caller; they do not require an expired original
session to become valid again. Parent evidence is copied into child admission.

The broker obtains labels from those native checks. Guest JSON, returned
markings and requested projections cannot invent or discard consumed labels.
Protected log insertion uses the current accumulated union and the monotonic
execution event stream. Output references are checked before sealing and again
inside publication. Final task/publication fence checks use the same database
transaction and hold the execution/task row locks through commit.

Read-only functions cannot invoke mutations. Read/write functions use action
keys `function/` plus the canonical SHA-256 of parent execution, guest call key
and action. Incoming labels must fit both action and object destinations before
commit; a later function failure does not undo that committed action.

For editProposal functions, the ordinary action invoke route validates inside a
rolled-back SQL savepoint, then durably stages normalized input and the complete
action/object binding fingerprint. It returns `{proposed:true,revision}` without
publishing an overlay, receipt or effect. Proposal functions accept point-read
dependencies only. All source/effective point values, current authority and
bindings are rechecked before applying staged actions in stable order inside the
same transaction as the final function receipt. A later staged-action failure
rolls back the whole batch. Prepare-only calculation denies additional action
calls. Nested invocation/inspection is restricted to direct children, with the
existing bounded ancestor/effect rules; child read evidence and function-resource
authority dependencies are copied into the parent before data returns. Retained
validation uses that durable evidence in the same transaction. Guest filesystem
metadata routes use native current authority, durable evidence and the protected
own-mutation acknowledgment described in `filesystem.md`. Function-backed actions
called from another function still require their native integration proof.
Both endpoint identities/current visibility, link permissions/markings and all
key properties must be authorized before limiting or counting edges.

## Evidence

`tests/native-serving.integration.test.ts` provisions actual PostgreSQL and two
native workload replicas with non-owning runtime credentials. It checks denied
rows/properties/transport shapes, cross-replica cursor replay and expiry,
concurrent idempotent actions, multi-target rollback, retained overlays and
tombstones, response loss plus SIGKILL, session expiry behind an actual SQL
barrier, queued grant revocation and filesystem provenance marking denial.
The redeployment/expired-key case verifies pinned input metadata. These are
functional proofs; they do not certify the mixed-load profile or cell/HA limits.

For operator workload evidence only, `MORIA_NATIVE_EXPLAIN_SAMPLES=1..16` captures
that many exact protected query plans using the same bound parameters and
transaction. The default is off. It runs EXPLAIN ANALYZE during a separate
warm-up, subject to the same deadlines and finite work/temp caps, then emits a
private `serving_plan` JSON event capped at 256 KiB. Expression-bearing fields
and SQL text are removed; actual physical nodes, index names, counters and buffers
remain. Callers cannot request capture or receive these plans. Plan classes
distinguish projection layouts with complete overflow, projection/canonical
engine sorting, UID reads and explicit bounded scopes; a class tag alone is not
a performance certificate.

Accepted source actions can bind explicit typed UID expressions and edits across
at most 100 exact objects and object types, with at most 1,000 edits in one
transaction. Values read the primary target's pre-action state; parameter, field,
principal-attribute and existing Boolean expressions use the shared strict AST.
Every referenced property is authorized before evaluating it. Both current and
generation-captured read, object-write and property-write policies apply. The
complete observed classification flows to every written destination. One
environment revision, immutable receipt, audit and delivery enqueue commit
atomically; a later denied target rolls back all earlier edits. Replay retains
the original input types and target-resource set across redeployment.

Nonidentity source mappings store the complete original Dataset row separately
from the mapped object row. Row policies evaluate the canonical Dataset fields;
object policies, edits and returned properties use the mapped object plus its
overlay. Identity mappings omit the duplicate canonical payload. Each native
generation captures its serving policy contract. Queries, aggregates, links and
protected search candidates intersect the captured generation policy with the
current contract before limits, counts or ranking. Tightening applies immediately;
broadening cannot release an old generation that its captured policy denies.

Creation uses the target's operator-registered identity domain in its current
managed cell. The permanent allocator compares the full canonical tuple of
environment, application, principal, action, idempotency key, target type and
typed identity key; the tuple cannot exceed 64 KiB. A domain owned elsewhere
fails with `OWNERSHIP_LOST`. Created properties are overrides; the synthetic
base contains the UID and nullable defaults. Canonical Dataset fields are
inferred only through the accepted mapping. An unknown required canonical field
fails with `ACTION_CANONICAL_SOURCE_REQUIRED`. Creation, home, overrides, audit,
changes and receipt share the action transaction. Typed receipts include
`created` and `changed` UID/type/version entries. Creation existence survives
Dataset replacement. Clearing a required override without an actual source
value fails atomically.

`POST /api/v1/objects/{type}/{uid}/attach-source` requires an idempotency key and
`{sourceDataset,expectedRevision}`. It requires current instance management,
target write, source read, filesystem permissions, object policies and complete
classification flow. The Dataset must be the target's accepted source. It records
one immutable association and receipt; it grants no Dataset byte access. Import
of a created UID without that association is rejected.

Binding definitions have a monotonic authority generation. Definition, revision
and enablement changes advance it and invalidate current security epochs.
Protected read witnesses and prepared actions bind the declared revision plus
this generation, so disabling and reenabling an identical definition cannot
revive an old witness.

### Browser origin ownership

Browser JavaScript shares ambient authority throughout one origin. A published
frontend digest, per-application cookie name, route prefix or `Referer` is not an
isolation boundary. Each configured browser origin is permanently reserved to
one `{environment, application}` tuple in `native_browser_origins`. Native HTTP
and the bounded OIDC listener reserve the trusted configured origin before
exposing cookies or frontend bytes. A local ephemeral listener reserves its
actual bound origin before announcing readiness. Request `Host` never chooses a
reservation.

`native_browser_origin_admit(origin, environment, application)` canonicalizes
scheme/host/default port, checks an existing application registration, permits
same-tuple replicas and rejects a different owner with
`BROWSER_ORIGIN_RESERVED`. Raw ordinary DML cannot create, update or delete a
reservation; reservations cannot be reassigned by a redeploy, app deletion,
restart or incarnation change. The protected table is part of full recovery.
There is no public HTTP origin-registration endpoint. Independently trusted
applications require different approved origins, including after reinstall or
restore. Operators must never reverse-proxy distinct applications onto one
origin or reuse an origin while previously served JavaScript may remain live.

The bounded OIDC callback origin is reserved to the same tuple as well; it
exposes cookie-bearing session controls and cannot host independent app code.
It may differ by port from the cookie target only under
the existing same-host/scheme explicit configuration. It redirects to the exact
configured native target and does not proxy business requests. Native APIs still
check current application binding, actual session state, exact POST Origin,
fetch-site and CSRF rules on every request. The cached ownership admission is
only an immutable-origin reservation, never cached session or data authority.
This adds an intentional deployment restriction and requires security review.

The native sign-in shell is fixed host code at `/api/auth/bootstrap`, with an
external `/api/auth/bootstrap.js` script and no private frontend bytes. Recovery
links target that shell, which consumes the fragment before same-origin recovery
and then navigates to the app. Unauthenticated HTML navigation to a protected
entry redirects there; private release assets remain unavailable. Cross-site
fetches and iframes remain denied. Safe GET/HEAD top-level document navigation
(`Sec-Fetch-Mode: navigate`, `Sec-Fetch-Dest: document`) can carry the app's Lax
cookie after a real OIDC redirect; this never permits cross-origin API fetch or
POST without exact Origin.

### Permanent credential revocation

Disabling or replacing a provider, disabling an application/principal/instance
identity, changing a principal kind, or replacing an established identity binding
permanently disables its already issued browser/workload credentials. Provider
revocation clears encrypted IdP access tokens. A disabled credential cannot be
reenabled by an ordinary row update, a process restart, an enabled flag pulse or
an old provider configuration. Authentication of that credential returns
`UNAUTHENTICATED`; independent permission and ACL denials retain their normal
forbidden response. Accepted session-origin jobs remain anchored to the revoked
original credential and cannot borrow a later sign-in. New authentication must
produce a new ordinary credential under current policy.

The bounded OIDC initializer preserves an explicitly disabled same-revision
provider. It does not temporarily disable unchanged enabled providers on startup.
A changed trusted provider configuration is a new registration; its old sessions
remain revoked. The shared SQL triggers apply to both native and retained bounded
authentication callers. An indexed per-environment browser-auth generation also
invalidates pending callback/recovery flows after a revocation pulse. Flow
consumption rechecks that generation and holds its row through session creation;
remote IdP verification cannot race a disable and mint from an obsolete flow.
An unrelated security revocation in the same environment may conservatively
require restarting an in-progress sign-in. No ordinary expiry or human freshness
window is extended. This deliberate permanent-revocation restriction requires
security review.
