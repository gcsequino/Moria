# Native filesystem contract

The metadata kernel in `moria_runtime::filesystem` implements current entry
authority and revisioned namespace operations directly in Rust/PostgreSQL.
Native HTTP and the function broker use the same kernel. Qualified content,
copy/policy governance and source lifecycle also run natively. This document does
not certify N21 capacity or replace the filesystem ordering barrier. Orphan byte
collection uses the separate exact-object controller described below. Only qualified
versioned S3 object removal and exact owned S3 multipart abort are enabled;
POSIX physical deletion remains outside its capability.

`NativeFilesystem { db }` accepts a private verified session. Its implemented
library methods are `get(session,id)`, `resolve(session,path)`,
`list(session,parent,cursor)`, `metadata(session,id,operation,argument)` and
`apply(session,key,changes)`. Metadata operation names are version, history,
role and policy. Policy reads require administer permission. Public integration
retains the existing `/api/management/v1/fs` route and SDK shapes.

Reads acquire environment/filesystem shared admission before READ COMMITTED
transactions, with explicit entry/content revisions and generation pins. Final
checks see committed credential revocation. Mutations acquire both exclusive locks before BEGIN and use a
serializable transaction. Each operation has a ten-second deadline including
queueing and final checks. Cancellation discards the connection and rolls back
uncommitted metadata. Metadata responses are capped at 1 MiB.

Entry authority uses the current session, principal/application enablement,
grants/scopes, human freshness and instance identity. Inherited ACLs preserve
explicit deny precedence and principal/group/administrator matching. The
marking closure includes ancestry, provenance, resource dependencies, source
release/dependency contents and retained labels. Cross-environment paths or
labels fail. Get/resolve conceal inaccessible entries as NOT_FOUND. Directory
listing applies this authority inside SQL before its 101-row limit.

Listings return up to 100 entries ordered by Unicode C name and permanent UID.
Opaque cursors bind actor, parent, security epoch and the original fifteen-minute
deadline. Directory revision or instance epoch changes invalidate continuation;
resumption never extends expiry. This cursor protects namespace continuation,
and does not claim a historical snapshot of every file's content pointer.

Apply accepts 1–100 ordered changes and a stable 1–128 character request key.
Directory create, move and empty-directory/file delete preserve the existing
trusted-composition boundary. Arbitrary file content locations, new runtime
resource identities and scope registration are rejected; those require
qualified storage/source composition. Names must be NFC UTF-8, 1–128 bytes,
contain no ASCII controls, separators, dot or dot-dot. Revisions are positive
int64 strings and UUIDs are canonical lowercase.

Each change checks current permission, expected entry/parent revisions and
placement. Moves cannot cross project/environment scope, create cycles, exceed
64 path levels/4096 bytes, relocate managed homes or nest active source roots.
Inherited markings are retained on moved entries and audited through the
instance security epoch. Typed resources retire through their owning domain.
Changes, parent touches, immutable entry versions and the complete idempotency
receipt commit together. A later failure rolls back earlier changes. Same
actor/key with different input conflicts; replay repeats current change and
receipt visibility checks.

Read/write function mutations must additionally validate their exact live lease
and consumed-label destination floor, and use the trusted own-mutation epoch
acknowledgment in the same exclusive transaction. The acknowledgment records
only an authorized mutation by that task/fence; it cannot replace current
permission, marking or provider checks. Edit-proposal functions cannot publish
filesystem mutations as ordinary actions.

The native broker durably stores actual metadata response fingerprints and
environment-qualified labels before returning a body. Retained results/logs
repeat current ACL/provenance checks and exact consumed metadata checks. Directory
page evidence stores its position independently of opaque cursor expiry. Failed
callbacks retain successful resource checks and their negative observation;
mutation failures retain exact entry/parent revision witnesses and recheck write
grants without executing writes under a reader admission. Child functions copy
filesystem evidence through the same bounded ledger. A successful own mutation
refreshes trusted evidence and acknowledges its audited epoch in the mutation
transaction. Stable function-qualified keys and private call traces bind the
filesystem receipt to its parent execution, including when later code fails.

Qualified file IO uses `NativeFilesystem::begin_file(session,key,FileWrite,context,call)`
and `read_file(session,id,revision,context)`. A `ContentDeclaration` specifies exact
SHA-256, size and media type; callers never supply physical locations. Trusted
operator storage bindings choose POSIX or S3, and their physical fingerprint must
match `fs_storage_bindings`. The reserved logical namespace is filesystem/content;
it has no Dataset/index namespace authority. Every provider chunk rechecks current
opaque session, target/parent ACL, destination label floor, physical binding and,
when applicable, exact sandbox lease. Files are bounded at 64 MiB, chunks at 1 MiB,
total transfers at thirty minutes and HTTP body idle time at thirty seconds.

POST `/entries/{parent}/files?name=...&expectedRevision=...` creates a file;
POST `/entries/{id}/content?expectedRevision=...` replaces its content. Both require
Idempotency-Key, Content-Type, x-moria-size and x-moria-digest. GET
`/entries/{id}/content?revision=...` returns the qualified immutable version.
Accepted upload labels only grow across retries. A thirty-second database lease
and increasing fence protect durable upload candidates. Exact pinned bytes/hash
are recovered after an uncertain provider acknowledgment; authorization errors,
version changes and unsafe paths never become an absent-object success. Immutable
metadata, parent versions, mutation receipt and candidate completion commit in one
transaction after byte sealing. A stale uploader cannot publish its candidate.
The broker bounds guest writes at 32 KiB and returned binary content at 256 KiB.

POST `/copy` preserves source export/read authority, destination flow and permanent
content reference/provenance. POST `/entries/{id}/policy` uses expectedRevision and
strict typed grants/labels; labels cannot be removed. Both repeat current authority
on receipt replay and audit security changes. A private sandbox acknowledgment
runs after each own audited mutation, before subsequent current-state checks in
the same transaction; rollback removes the mutation and acknowledgment together.

`NativeSources` implements register, checkout, release, publish, plan, activation
and retirement. POST `/sources/{id}/register` takes `{kind,expectedRevision}`;
POST `/sources/{id}/checkout` takes `{}`. Checkouts contain actual qualified file
versions, directory revisions and the canonical source digest, with at most 2,000
entries and 64 MiB of source bytes. Descriptor/declaration JSON and compiler
artifacts are bounded at 16 MiB per file. Nested source roots are refused.

GET `/compiler-context` returns `{revision,fingerprint,principalAttributes}`.
POST accepts only `{principalAttributes}` and requires current instance:manage.
Contexts are immutable; additive fields create a new fingerprint and audited
environment security epoch. Existing field type changes/removals are rejected.
Compiler attributes are never inferred from one principal's values.

The bounded TypeScript compiler remains a control-plane tool. It writes a
`CompiledSourceArtifact {version:1,rootId,sourceDigest,descriptor,compiled,owned,
compilerContext}` to a qualified file outside the source tree. Native POST
`/sources/{id}/publish` accepts only `{expectedSourceDigest,artifact:{entryId,
revision,digest}}`. The publisher checks actual artifact bytes, pinned source
declarations, repository export/runtime rules, exact import assembly and compiled
fingerprints. Embedded trusted Draft 2020-12 schemas and native semantic checks
independently validate resource references, namespace/type mappings, policy ASTs,
action contracts and producer cycles. The recorded compiler context must exist in
the operator registry and remain compatible with current types. Publication checks
the complete source/artifact/dependency labels and current publish authority under
the final source CAS; it does not grant Dataset/model/job operational authority.

GET `/releases/{digest}` rechecks the bounded immutable release graph and every
current source ACL. POST `/sources/{id}/plan` accepts `{digest,deletions}` and pins
actor, environment/instance epochs, active revision and a fifteen-minute deadline.
POST `/plans/{id}/apply` takes `{}` and atomically materializes permanent resource
identities, placements, retained labels, release history and filesystem dependency
edges with its idempotency receipt. Omitted owned resources persist until explicit
deletion. Source activation returns `runtimeReady:false` until separately admitted
native domain bindings are composed. POST `/sources/{id}/retire` requires the
registration revision and denies retirement with active dependents. Source JSON
cannot select a registered service actor or create data-authority edges.

The CLI and management browser share pure source assembly and semantic validation.
Trusted schema validators are produced by `scripts/compiler-schema.ts`; `--check`
rejects drift. Browser validation uses these static functions under the existing
Content Security Policy, and WebCrypto computes the same canonical SHA-256 hashes
as the synchronous Node compiler. Dynamic Connector configuration schemas are
validated by the native publisher before publication; the browser explicitly
reports this validation as pending. Both clients read and hash every pinned source
file, verify pinned imports and the operator compiler context, and use the guarded
compiler artifact upload. The native publisher independently checks the artifact,
source bytes, current authority and final source compare-and-set. Browser publishing
does not rely on a Node service or relax script policy.

The real filesystem fixture exercises two replicas and both POSIX and MinIO/S3,
including a suspended uploader, real lease expiry, exact-object recovery and stale
sender rejection. Native serving also proves inherited source filesystem ACL
revocation through actual published Dataset/Object resources. The native function
suite exercises an actual protected guest move, one audited epoch acknowledgment,
retained result/log release and receipt trace. The fresh combined management/source
trial passed 58 cases with no skips, including native source publication and the
latest accepted-upload classification guards, plus existing TypeScript source
lifecycle regressions. This is functional evidence, not a capacity certificate.

GET `/home` provisions only the current verified primary principal and its managed
home. Concurrent replicas return the same permanent binding. Principal kind and
agent manager come from current authoritative identity attributes; body-supplied
identity is never accepted. Fixed root read/list grants do not inherit into other
homes. An agent gets read/list on its home, an explicit existing manager with full
home rights, and write access limited to its memory subtree. POST `/projects`
accepts `{parentId,name,expectedRevision}`, checks current register permission and
creates the project/source/environment directories atomically. POST `/groups`
accepts `{name,groupKey,expectedRevision}` and requires instance:manage; group ACLs
match only current authoritative environment-qualified membership. POST
`/principals` accepts `{id,enabled}` under instance:manage, audits the change and
invalidates every bound environment. These management mutations are denied to
guest broker callbacks; home provisioning requires no caller-selected actor.

Native Source activation separates complete configuration from availability.
`MORIA_NATIVE_SOURCE_PROFILES` maps an exact environment, project UUID and
qualified declaration to trusted storage, service, model or resource-class
configuration. It never supplies a caller actor. Missing profiles remain explicit
pending metadata. A complete configuration attempt either commits all bindings
with the filesystem activation or rolls back. Dataset schemas and physical
bindings are pinned; ordinary schema evolution is exact-only. Object mappings
retain the complete canonical Dataset row. All data origins are admitted before
capturing permanent indexing authority. Application and Grant declarations
validate independent current security administration and cannot create grants.
Activation performs no provider calls or container-runtime inspection.

`GET /api/management/v1/fs/sources/{root}/runtime` recomputes current configured
bindings, profile identity, managed cell, model probes and relevant worker
availability. Artifact inspection runs outside the database admission; a final
current admission checks the same source and binding fingerprints before release.
Search readiness includes its actual generation, current object/model contracts
and registered projector purpose. The result is qualified to the caller's
authority; it is not a grant to another application. The immutable activation
receipt retains its original readiness result; this endpoint reports current
state. Neither configuration nor readiness establishes a capacity certificate.

## Resource-scoped filesystem admission

Security boundary change requiring human review before merge: a protected operational
resource call may prove its actual UUID-scoped capability (or its explicitly retained
published-kind global capability) and then evaluate the exact resource/source ACLs
without granting global filesystem access. `native_resource_filesystem_access` derives
current identity from the opaque session and validates the operation; its raw-state
counterpart and `native_filesystem_acl_state` are private (`REVOKE ALL FROM PUBLIC`).
Direct filesystem routes still require the corresponding `filesystem:*` scope and grant.
No synthetic identity scope or caller-provided authorized flag is accepted.

`fs_source_resource_files(digest,reference,entry_id)` is an immutable publisher-derived
proof. Both trusted publishers derive declaration/descriptor file identities from the
verified checkout; repository exports additionally depend on their verified code inputs.
ACL admission follows directed resource/provenance dependencies and these exact files,
including available retained mappings. Sibling declarations are not ACL dependencies
merely because they share a release. Existing conservative retained markings are kept.
Missing current release proof fails closed with `SOURCE_FILE_PROOF_REQUIRED`; this is
not permission to infer a mapping from caller JSON. The maximum required ACL roots is
4096; overflow fails without truncating authorization.

Hosted frontend bytes use a separate app-bound guard: the current session application
and environment must match the trusted host and current published browser Application.
The frontend URI/digest, enabled application, source/dependency ACLs and classifications
are rechecked before release. This guard grants no Object, Dataset, Model or direct
filesystem access and introduces no Application read permission.

## Uncommitted upload retirement

`moria-runtime filesystem-retention approve|disable|plan|once` is an operator-only
controller, independent of filesystem upload/read permissions. Each command reads
one strict JSON object from stdin (maximum 16 KiB). `approve` accepts
`{requestKey,namespace,minimumAgeSeconds,recheckSeconds}`; the namespace must be
`filesystem/content` with its exact configured revision. The minimum age is
86400–31536000 seconds and recheck interval 30–604800 seconds. The policy pins the
physical binding, current instance incarnation and approving database role.
All controller mutations require that role and actual schema configuration
privilege; the ordinary runtime DML role cannot approve its own cleanup.

`plan` accepts `{policy,after:UUID|null,limit:1..64}` and returns a keyset page.
It considers only expired, uncommitted uploads. Under recovery-retention and
filesystem exclusive admission, it checks current entries, every retained entry
version and retained recovery membership. It then permanently tombstones the
producer and starts the first proven-unreferenced interval. An idempotent retry
cannot resume a tombstoned upload (`UPLOAD_RETIRED`, HTTP 410). Observing a
reference clears the interval; after its removal a new full grace interval is
required. Upload creation time or expired credentials alone never satisfy it.

`once` accepts `{policy,owner:UUID,timeSeconds:1..20}`. It claims at most one
eligible upload with a monotonic fence, 30-second lease and exact instance and
admission-backend identity. Each provider authority check repeats the current
policy, filesystem/recovery references and physical binding checks. Removal
requires a separately constructed `RemoveAuthority` and an exact non-null S3
VersionId from the qualified versioned provider. A newer version is preserved.
POSIX and unversioned S3 deletion return explicit `unsupported`; there is no
prefix or unconditional deletion fallback. The durable candidate remains under
bounded periodic observation after absence/removal, while its producer tombstone
remains permanent. This controller never selects committed file content or
expires filesystem history.

Before a filesystem S3 Initiate request, the upload authority commits an immutable
`native_filesystem_multipart_intents` row for the actual upload, owner/fence,
unique canonical UUID object key, physical binding and instance incarnation.
The returned provider upload ID is journaled before any Part or Complete request.
A lost Initiate acknowledgement is reconciled only under that precommitted exact
key, permanent producer tombstone, operator claim and full orphan grace.
`DiscoveryAuthority` can list at most 16 provider records for that exact key;
neighboring keys and upload IDs are filtered before release. Each exact returned
ID is durably recorded and requires a distinct fenced `AbortAuthority` before and
after provider Abort. Aborting a page invalidates provider cursors, so the next
bounded cleanup pass starts at the exact key again. The watcher also catches late
Initiate effects. No prefix deletion or general multipart-list/abort permission is
derived from filesystem write authority.

The qualified MinIO profile was tested with a lost Part acknowledgement and with
17 real native Initiate acknowledgement losses, each under a new producer fence.
Two bounded cleanup steps removed all exact owned attempts while an independently
addressed neighboring attempt remained accessible. The test's retention interval
is explicitly aged fixture metadata; it is not an elapsed-day soak claim.
