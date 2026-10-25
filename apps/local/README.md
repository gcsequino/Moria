# Local asset workspace

Run `pnpm start` from the repository root. Open the loopback URL printed in the terminal; use `pnpm moria auth recover` for a temporary administrator session, or configure Google and explicitly grant an administrator invitation. The app binds only to `127.0.0.1:3210` and starts its own PostgreSQL cluster on `127.0.0.1:55432`. Default startup hosts management only. No Snow/weather workloads are registered. No Docker, external account, or model endpoint is required.

Prerequisites: Linux, Node 24, pnpm dependencies (`pnpm install --frozen-lockfile`), and the Python environment from the root README. `MORIA_PYTHON` can override the Python executable. Run as an ordinary user because PostgreSQL cannot run as root. Windows hosts must use a Linux container or WSL2 Linux filesystem; native Windows development is unsupported.

## Try the complete workflow

1. Sign in with local recovery or an invited administrator account.
2. Open Assets and import the prefilled synthetic sample.
3. Open an asset, edit Status and save the override.
4. Load the refreshed sample and import it. Source values change while saved overrides remain.
5. Restart and sign in again. Stable IDs, data and overrides remain. Use all source values clears overrides.

The browser uses current administrator permissions. Standalone tests retain East/West
workload profiles; browser callers cannot choose those identities.

## Import format

Upload a UTF-8 CSV with the header below, or a JSON array of objects with these five string fields. The textarea supports direct JSON editing. The sample buttons only fill the preview; the import button publishes it.

```csv
id,title,region,status,notes
PUMP-001,North intake pump,West,Inspection due,Inspect the inlet seal.
MOTOR-003,Conveyor drive motor,East,Maintenance,Bearing replacement scheduled.
```

All fields are required; notes may be empty. Region is `West` or `East`. IDs must be unique and stable across refreshes. Each import replaces the entire source dataset, rather than appending. Omitted IDs become invisible, with their identity and overrides retained if they later return. IDs and regions are source-controlled; title, status and notes can be overridden independently. Limits: 1–1,000 rows, 2,000 characters per field, 64 KiB JSON request. Empty imports are rejected.

## Persistence and operation

`.local/workspace/` holds PostgreSQL files, the Iceberg warehouse, and a generated credentials file. It is ignored by Git. Keep the whole directory intact: Iceberg metadata includes absolute paths, so moving it requires migration. Stop the app before making a filesystem backup. This is not a validated production backup/restore procedure.

Optional environment variables: `MORIA_PORT`, `MORIA_DB_PORT`, `MORIA_LOCAL_DIR`, `MORIA_PYTHON`. Run from the repository root. Port overrides must be 1024–65535. Recovery links are temporary and single-use. Run the recovery command again for another browser. Retired `MORIA_SNOTEL_PORT` and `MORIA_SNOW_PUBLIC_*` settings fail explicitly.

## Boundaries

This is one local asset dataset with fixed schema and trusted bootstrap bindings.
Management uses app-bound browser sessions and current administrator authorization.
The local operator credential is reserved for CLI recovery. Exact Host/Origin
checks remain enforced. Optional trusted HTTPS proxy bindings are described in
[public hosting](../../docs/PUBLIC-HOSTING.md).


Imports validate the whole input, allocate stable UIDs, write real Parquet/Iceberg snapshots, publish catalog pointers, then run the fenced Iceberg indexer. Dataset and ontology publication are separate transactions. If indexing fails after dataset publication, the old objects stay visible and the UI asks you to import again. Imports are serialized within this process. Durable job recovery, safe garbage collection, arbitrary dataset schemas, generated SDKs and production deployment remain future work. The bundled React client calls the existing API directly.

Action retries after a lost response keep the same idempotency key while the edit is unchanged and the editor remains open. Queries are paginated with the existing protected cursor contract. The local app is intentionally a small runnable end-to-end slice, not a replacement for the broader platform roadmap.

## React hosting update

The browser UI now lives in `apps/management` and is built with React/Vite. `apps/local` is the instance bootstrap/API composition, not a separate static UI implementation. `pnpm start` bootstraps the bundled app through the shared host. See `docs/APP-HOST.md` for CLI recovery and custom app publication. Ingestion now uses `/management/v1/datasets/assets/import`; the previous `/local/import` remains an alias. The Applications page shows installed releases, gated by the operator's management grant. The earlier statement that React is future work is superseded by this checkpoint; generated SDKs and OIDC remain future work.


## SNOTEL application

`pnpm start` also hosts Snow Observatory at the separately printed port-3211 link. Run `pnpm moria snotel sync` to ingest official NRCS data and `pnpm moria snotel open` to retrieve its scoped access link. See `docs/SNOTEL.md`. Configure its port with `MORIA_SNOTEL_PORT`. Its key and snow warehouse are persisted alongside the asset workspace; its read-only workload cannot administer the instance.

## External PostgreSQL

Set `MORIA_DATABASE_MODE=external` and provide an environment secret reference for
an already-migrated database and scoped runtime role. Run `pnpm exec tsx apps/local/src/migrate.ts`
separately as the migration owner. External startup performs no DDL, and stopping
the APIs closes their pool without stopping PostgreSQL. See
[database lifecycle](../../docs/DATABASE.md) for validation, role grants, pool
budgets, version compatibility and explicit recreation of disposable unversioned
development instances.

## Independent static process

To serve an existing trusted promoted artifact without starting the workspace or
PostgreSQL, run from the repository root:

```sh
pnpm exec tsx apps/local/src/static-host.ts --app management --directory /path/to/disposable-instance --port 3212
```

App, instance directory and port are explicit and required. Missing or damaged
promoted artifacts fail startup without rebuilding them. SIGINT/SIGTERM stop
only static delivery; artifacts and independently running APIs remain intact.
The management shell alone grants no session or API access. Same-origin `/api`
requests require separate trusted proxy routing to the API service; this is not
a cross-origin management client. See [static delivery](../../docs/production/STATIC-DELIVERY.md)
for publication, restrictions, lifecycle and verification details.
