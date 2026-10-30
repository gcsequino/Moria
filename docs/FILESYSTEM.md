# Platform filesystem

**Status: complete for the agreed first release, accepted October 28.** Functional
and adversarial verification passed the complete Linux gate with 449 runtime tests.
Production capacity validation is a separate milestone; operating bounds and
release exclusions are documented below.

Moria stores its namespace, policies and immutable metadata history in PostgreSQL,
and verified immutable file bytes in local storage or S3. Entries have stable
UUIDs; paths are names, not identities. Schema version 3 requires a fresh development
database. There is no migration or backfill of older instances.

The filesystem organizes source, configuration and runtime resource placements.
Dataset rows remain in Iceberg; ontology rows and overrides stay in their existing
stores. Secrets belong in external secret stores, never source files or dotfiles.

## Namespace and types

| Entry | Meaning |
|---|---|
| Directory | Named container, inherited policy and optional managed role. |
| File | Immutable bytes behind a versioned head, size, SHA-256 digest and media type. |
| Resource | Environment placement of a permanent logical resource UUID; its definition resolves from an active release. |

The canonical root contains `/system`, `/groups` and `/users`. Verified identities
receive a private home at `/users/<instance-principal-id>`, with configuration,
memory, runs and projects directories. Renaming preserves home identity. Groups
bind to an authoritative group key in an environment; a filesystem grant never
manufactures group membership.

Agents are workload principals with a registered manager, separate from their code
or execution records. Trusted identity synchronization supplies `agent: true` and
`managerId` containing the manager's instance UUID. Agents can read configuration
and write memory; configuration, registration and publication remain manager-controlled.
The supervisor provisions credentials through the short-lived workload-session
service. File APIs cannot mint identities or change authoritative membership.
Managed home subdirectories may be renamed within their home, but cannot be
deleted or moved into another permission boundary.

Projects contain `source` and `environments`. Ontology and repository roles register
on project source directories with arbitrary names and multiple siblings. Source
roots cannot nest or overlap, including after a move. A project owns each namespace
permanently; retirement does not let another source impersonate it.
Independent projects may use the same namespace. Named resource lookup requires
the project UUID (`fs resource <reference> --project <uuid>`); resource UUIDs are
unambiguous without that option. Runtime adapters must bind resource UUIDs when
names occur in multiple projects.

An ontology has `.ontology.json` plus individual `*.object.json`, `*.link.json`
and `*.action.json` declarations. Supporting resources use `*.resource.json`.
A repository has `.repository.json` and ordinary source, dependencies and tests.
It is one checkout, CI and deployment unit; exported artifacts get individual,
permanent resource UUIDs. Descriptors are ordinary versioned files:

```json
{
  "version": 1,
  "name": "Operations logic",
  "namespace": "operations",
  "runtime": "typescript-v1",
  "dependencies": []
}
```

Omit `runtime` for ontologies. Repositories accept `typescript-v1` and
`python-duckdb-v1`. Dependencies pin another root UUID and release digest:
`{"rootId":"<uuid>","digest":"sha256:<64 hex characters>"}`. CI emits a Resource[]
using the existing manifest contracts. Publication validates exports with the
semantic compiler; the control plane never imports submitted code. Each export's
artifact URI/digest identifies its own callable build artifact. This release does
not introduce a generic code executor or new language engine.
Pinned imports may cross projects and retain their owning project's policies.
A release cannot activate imports whose identical references belong to different
projects; give those imports distinct namespaces. Transform output datasets must
have active placements, and activation requires write authority on each output.

## Security and history

Operations require a verified, unexpired session, current principal grants,
application scopes, all effective markings and a matching filesystem grant.
Ownership alone grants nothing. Explicit denies win. Every verified member of a
delegation chain must pass independently. Markings and groups are environment-qualified.
Administrative grants resolve current `instance:manage` authority and disappear
when that authority is revoked.

Directory markings apply to existing descendants and historical versions. Moves
retain old labels. Copies retain provenance; publication snapshots retain source
markings. Later source restrictions also propagate to derived entries. Ordinary
policy edits cannot remove markings. Replacing or omitting a declaration retains
its release association, so future restrictions on old source still propagate.
Declassification is a separate security
feature; moving, copying or republishing cannot perform it.

Resource dependencies carry restrictions into derived placements, including when
an upstream placement is activated later. Release envelopes conservatively include
the declared markings of their compiled resources and pinned imports. Split source
units when builds require independent classification boundaries.

Listings hide inaccessible entries and never return hidden counts. Opaque
continuations bind identity, directory revision and policy epoch, expiring after
15 minutes. Historical reads reauthorize current policy. Filesystem bindings also
gate object reads, actions, indexing and dataset metadata streams. Legacy internal
demo bindings without placements remain trusted internal interfaces; they are not
automatically imported into user projects. Supervisors must use the protected
resource resolver and `Authority.resourceAccess` when composing other bound adapters.

The resource resolver and dataset metadata reader return current classifications
for downstream execution and destination checks. Metadata streams abort if those
classifications change. Index publication rejects a target that cannot preserve
its input's current filesystem markings, including an unbound target.

Entry changes use expected revisions and actor-scoped idempotency keys. Up to 100
ordered changes commit atomically with versions and replay receipts. Replays still
require current authority. Moves cannot cross project or environment boundaries.
Deletion is explicit; directories must be empty. Managed homes and system roots
cannot be deleted generically. Source roots use retirement; typed resources use
explicit dependency-checked activation deletions. History, audit, identities,
receipts, releases and referenced content are retained indefinitely in this release.
Planning, applying and replaying activation require current read and publish
authority on affected placements, plus delete authority for explicit deletions.
An active release protects all transitive pinned roots from retirement; retiring
the consumer releases that protection.

## Local development and CI

Start `pnpm start`. The console's **Files** view supports browsing, uploads,
downloads, text editing, history and activation-plan review. Filesystem operations
use the platform API at `/api/management/v1/fs`, with a client helper in
`packages/platform/src/filesystem.ts`. Filesystem access is separate from the
generated ontology SDK; client availability never grants authority.

The CLI uses the running local instance's credentials by default. `~` resolves the
verified identity's home. Remote CI sets `MORIA_API_TOKEN` to a short-lived workload
session and passes `--url https://instance.example`. Remote requests require HTTPS;
tokens never enter the checkout binding or command output.

```sh
pnpm moria fs home
pnpm moria fs project '~/projects' example
pnpm moria fs init '~/projects/example/source/domain' --kind ontology --namespace example
pnpm moria fs checkout '~/projects/example/source/domain' ./domain
# Edit declarations and run local tests. Git can track the entire directory.
pnpm moria fs push ./domain
pnpm moria fs publish '~/projects/example/source/domain'
# Inspect the returned digest, then create and review an activation plan.
pnpm moria fs plan '~/projects/example/source/domain' sha256:<release-digest>
pnpm moria fs apply <plan-id>
```

For code, initialize with `--kind repository --namespace logic --runtime typescript-v1`
or `python-duckdb-v1`. CI runs its tests/build, pushes source, then calls
`fs publish <root> --exports exports.json`. The export is a resource array, not
executable discovery code. The active environment comes from the verified session.

Checkout creates a new directory and verifies pinned bytes. `.moria.lock.json`
binds the root UUID, revisions and instance origin. Interrupted checkout retains
an incomplete marker. Push rejects a stale remote base or concurrent changes and
never advances an active release. In Git it uses tracked files; plain directories
exclude Git metadata, dependencies, build directories and the binding. Symlinks
and special files are rejected. Never place credentials in a source unit.

Omitting local files never deletes remote source. Use `fs rm <remote-file>`, then
`fs bind <root> <local-directory>` to accept the new base without replacing local
files. An explicit remote `fs mv` plus a local rename works the same way. Resource
identity comes from the exported reference, independently of source filenames.

Omitting definitions also retains active resources. Pass an explicit resource-ref
array using `fs plan ... --config deletions.json` to remove them. Plans bind actor,
release, active revision and policy, expire after 15 minutes and retain receipts.
Activation creates placements and activates definitions. `runtimeReady: false`
distinguishes this from provisioning an executor, ingesting data or deploying an
application. Existing dedicated runtime and app-deployment workflows stay separate.

Other commands: `ls`, `stat`, `mkdir`, `mv`, `cp`, `rm`, `get`, `put`, `history`,
`resource`, `policy`, `group`, `register`, `retire`, `enable`, `disable`, `collect`.
Paths or UUIDs identify entries. `get <uuid> --revision <revision> --out <new-file>`
works for retained revisions after deletion. `history <uuid> --cursor <revision>`
pages backward. Downloads never overwrite local files. `fs policy <entry> policy.json`
replaces explicit `markings` and `grants` arrays and requires `administer`.
Instance enable/disable and collection require current instance administration.

## Storage and operating bounds

The default `MORIA_FILESYSTEM_BACKEND=local` stores bytes beneath
`<instance-directory>/filesystem-warehouse`. Multiple API processes must share the
same physical store. S3 configuration uses explicit external secrets:

```text
MORIA_FILESYSTEM_BACKEND=s3
MORIA_FILESYSTEM_S3_ENDPOINT=https://s3.example.com
MORIA_FILESYSTEM_S3_BUCKET=moria
MORIA_FILESYSTEM_S3_REGION=us-east-1
MORIA_FILESYSTEM_S3_PREFIX=instance-one
MORIA_FILESYSTEM_S3_ACCESS_KEY_ID=<external secret>
MORIA_FILESYSTEM_S3_SECRET_ACCESS_KEY=<external secret>
```

Optional: `MORIA_FILESYSTEM_S3_SESSION_TOKEN`. There is no ambient AWS profile or
credential-file discovery. S3 must support conditional create-only multipart
completion. A persisted binding hash prevents accidental relocation of logical
object references; credential rotation preserves it. Back up PostgreSQL and its
matching byte store together.

Use a dedicated bucket when possible. Maintenance needs bucket-level multipart
listing plus abort/delete authority; it filters the configured table prefix before
aborting an upload. Object reads and writes retain their exact logical-object scopes.

Uploads are bounded, backpressured and SHA-256 checked before publication. Failed
integrity, stale revisions and revoked authority cannot advance heads. Reads pin
a revision and recheck authority during transfer; EOF integrity failure aborts
the HTTP body. Downloads use attachment disposition, sandbox CSP and `nosniff`.

Run `fs collect` periodically from the operator's scheduler. Each invocation handles
up to 50 old candidates and 50 abandoned staging uploads, with a 24-hour cutoff.
Transfers have a 30-minute upper bound; HTTP transfers have a 30-second deadline.
Any retained version reference prevents byte deletion, including copied content and
publication snapshots. Collection never purges published history. Staging scans
are bounded at 1,000 records; a bucket lifecycle rule for incomplete multipart
uploads also helps large S3 stores.

Names are NFC, case-sensitive UTF-8, at most 128 bytes. Paths have at most 64 segments
and 4,096 bytes. Files are at most 64 MiB. Source units have at most 2,000 entries,
64 MiB, 16 immediate dependencies and 64 releases per dependency graph; individual
JSON source files are at most 16 MiB. Listings and history page 100 candidates.
These are first-release bounds, not production throughput claims. FUSE, symlinks,
cross-project transfer, hosted Git, history purging and new engines are outside
this release.

## Implementation and verification

Contracts and assembly: `packages/contracts/filesystem.ts`,
`packages/compiler/src/source.ts`. Persistence, content, management and release
services: `services/filesystem`. Current inherited policy:
`services/security/src/filesystem.ts`. HTTP: `apps/api/src/filesystem.ts`, consumed
by the platform API client, CLI and management console.

`tests/filesystem*.test.ts`, `tests/source.test.ts` and shared storage contract tests
exercise real PostgreSQL, local storage, MinIO, HTTP and the CLI. Run the complete
supported Linux gate with `pnpm check`.
