# Database configuration and migration lifecycle

Issue #13 separates schema ownership from external service startup. `pnpm start`
retains the development cluster by default. `MORIA_DATABASE_MODE=external` selects
an operator-managed PostgreSQL connection; `NODE_ENV=production` requires it.
External mode never initializes, starts or stops PostgreSQL. Shutdown drains HTTP
and jobs, releases the schema lease and closes this process's pool only.

## Configuration

Set `MORIA_DATABASE_SECRET_REF` to the **name** of an environment variable holding
a PostgreSQL URL. Run `pnpm exec tsx apps/local/src/migrate.ts` separately with
`MORIA_MIGRATION_SECRET_REF` referencing the migration owner's URL. Inject values
through the deployment environment/external secret manager, never command-line
arguments, repository files or logs. The runtime does not need the migration
secret. URLs require an explicit username, password, host and database. URL query
options are rejected, including implicit certificate/key file paths. Errors name
the setting or failed operation, without printing URLs or database error details.

`MORIA_DATABASE_TLS=verify` is the default and validates the server certificate
using Node's trust store. `disable` is an explicit option for isolated loopback
fixtures. Other TLS modes are rejected. Remote TLS deployment remains unverified.

`MORIA_DATABASE_POOL_MAX` defaults to 10 (4–100). The minimum of four
connections covers the actual bundled handlers with the lifetime schema lease:

- Snow: schema lease + `Jobs.tick` supervisor + Snow ingestion lock + one
  query/transaction connection. Staging and publication run sequentially; their
  transaction callbacks reuse the supplied client for fencing and completion.
- Weather: schema lease + supervisor + publication transaction + the nested
  `Authority.current` query in `snow.ingestionGuard(client)`. Weather has no
  separate ingestion-lock connection. Its remaining guard checks and completion
  reuse the publication client.

Real PostgreSQL tests measure four simultaneously checked-out connections in
both paths, with injected synthetic downloads and no retries. Smaller pools are
rejected. This minimum supports sequential bundled jobs; concurrent API work
needs additional headroom.
`MORIA_DATABASE_REPLICAS` defaults to 1;
`MORIA_DATABASE_CONNECTION_BUDGET` defaults to 20 (minimum 5). Validation requires
`replicas × pool maximum + 1 <= budget`, reserving one migration connection.
Operators must set the replica count and budget for the whole deployment and
leave additional server connections for administration and other clients. This
is configuration validation, not cluster-wide admission or a capacity claim.
Connection acquisition times out after 5 seconds; idle pool connections after
30 seconds. Migration uses a separate maximum-one-connection pool.

The existing `MORIA_LOCAL_DIR`, warehouse paths, application access keys, OIDC
configuration and recovery commands retain their behavior. This change does not
relocate authentication secrets or convert the local host into a complete
production deployment. Management/Snow listener ports are validated before any
local state is opened. The development database port is not a listener owned by
external mode.

## Roles and schema ownership

Provision a migration owner and a separate runtime login. The migration owner
owns the `moria` schema and tables; runtime must have no ownership (including
membership in the owning role), superuser, BYPASSRLS, CREATEDB, CREATEROLE or CREATE
on the `moria` schema. External startup checks these constraints. Do not grant
runtime membership in privileged roles. Runtime remains a trusted platform role,
not an end-user or arbitrary-code identity; application authorization remains in
the existing transaction and policy code.

After migration, an operator can grant the following to an already-provisioned
`moria_runtime` role (no passwords are included here):

```sql
GRANT USAGE ON SCHEMA moria TO moria_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA moria TO moria_runtime;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA moria TO moria_runtime;
REVOKE UPDATE, DELETE ON moria.security_audit FROM moria_runtime;
REVOKE ALL ON moria.schema_version FROM moria_runtime;
GRANT SELECT ON moria.schema_version TO moria_runtime;
```

Review grants for each future migration. Startup does not provision database roles
or grants. Existing application bootstrap creates only its missing trusted
bindings; it does not reset existing revocations. This is not new RLS or untrusted
SQL isolation evidence.

The migration runner takes a nonblocking exclusive PostgreSQL advisory lock
`731804135219` in the same transaction as all DDL and the version marker. Competing
migrations fail clearly. External services hold the shared form on a dedicated
connection for their lifetime. Every runtime pool checkout also holds its own
shared lock until release, including transactions and one-shot pool queries.
Lease loss stops new checkout admission and initiates service shutdown; already
active database work retains migration exclusion while draining. A checkout
verifies the schema under its own lock before exposing the connection. Failed
lock cleanup destroys the connection instead of returning it to the pool. Version 3 records a checksum of the filesystem-enabled baseline DDL;
missing, unknown or checksum-mismatched schema versions cannot serve. The
migration command initializes a fresh database or verifies an already-versioned
schema. It never bootstraps identities, grants or application data. An existing
`moria` schema without `moria.schema_version`, even an empty schema, is rejected
before any schema DDL or data changes. No automatic reset or schema inference is
performed. Initialization and its version marker commit atomically.

## Disposable development databases and versioned upgrades

Existing unversioned databases are disposable development instances and cannot
be upgraded by this runner. Rejection leaves their schema, data and absence of a
version marker unchanged. Do not add a marker manually.

To recreate a disposable embedded development instance explicitly:

1. Stop its launcher and all workers. Confirm that it is disposable development
   state and that no live installation is involved.
2. Choose a new, empty absolute directory and run
   `MORIA_LOCAL_DIR=/absolute/path/to/new-disposable-workspace pnpm start`.
   This initializes a fresh PostgreSQL database and warehouse with new development
   identities and authentication configuration. The old directory stays intact;
   do not copy its old warehouse or credentials into the new instance.
3. Reconfigure development authentication and reimport synthetic/sample data as
   needed. This is a new development installation, not preservation or restore.

For an external disposable development database, stop all clients and have the
operator provision a new empty database owned by the migration role. Point the
migration secret reference at that database, run
`pnpm exec tsx apps/local/src/migrate.ts`, grant the scoped runtime role access,
and start with the runtime reference and a fresh development workspace directory.
An operator may separately remove the old disposable database after confirming
its target and that its data is no longer needed; the application never does so.

For an already-versioned installation, keep the database and warehouse paths in
place. Before any future versioned upgrade, quiesce/fence applications, workers
and dispatch, follow C08 and the [restore guide](BACKUP-RESTORE.md), and rehearse
with isolated data. Run the matching migration command as the owner, review grants,
then restart the scoped runtime and verify identities, grants, edits and auth
before enabling routing/jobs. Older binaries do not honor the advisory lock and
must remain stopped. No live migration or restore is performed by this issue.

This development release supports schema version 4 with its exact baseline checksum.
Version 4 adds the Rust indexing protocol's durable work and shared publication
authority. It requires a fresh isolated database; no automatic version-3 upgrade
or live installation migration is included.
Version 1 and 2 databases must be recreated using a new disposable directory; no upgrade
or data-preservation path is provided. Unknown versions fail closed; there is no
automatic downgrade, security rollback, rolling-upgrade or zero-downtime promise.
Changing migration ownership requires human invariant review before merge.

## Evidence and limits

The SQL integration suite uses an isolated PostgreSQL cluster owned by the test
harness, outside the API lifecycle, synthetic records, real Iceberg files and a
separate restricted runtime login. Existing management HTTP refresh/edit/restart,
Snow sync, OIDC, revocation and CLI recovery assertions run against that role.
The original embedded database stop/start test remains. External API/pool shutdown
leaves PostgreSQL queryable, and reconnect preserves edits. Migration tests compare
all existing table rows before/after repeated versioned migration, reject concurrent
migrations and incompatible versions, reject owner-role startup and DDL from runtime,
and prove unversioned empty, partial and complete schemas are rejected without
changing schema/data or creating a version marker. No mock substitutes for these
checks. See [implementation evidence](IMPLEMENTATION.md) for executed commands.

A separately hosted PostgreSQL deployment, remote TLS, full installation
backup/restore/cutover and mixed-release operation remain unverified. The shared
schema lock coordinates participating processes, not legacy binaries or a database
administrator. This issue makes no production capacity or security certification.
