# Local Moria instance

Use Linux as an ordinary user. Install the repository prerequisites and build or provision the matching native runtime executable. Set `MORIA_RUNTIME_BINARY` to its path if it is outside the default installation location. [Development setup](../../README.md#development) · [Native runtime](../../services/runtime/README.md)

Run `pnpm start` from the repository root and open the printed management URL. The combined launcher defaults to management on port 3210, the separate authentication listener on 3211 and embedded PostgreSQL on 55432. Use `pnpm moria auth recover --app management` for a temporary operator session, or configure Google and explicitly invite an administrator. [Authentication guide](../../docs/AUTH.md)

The default local directory is `.local/workspace/`. Use a new absolute `MORIA_LOCAL_DIR` for an isolated instance with a fresh schema. Port settings are `MORIA_PORT`, `MORIA_AUTH_PORT` and `MORIA_DB_PORT`. Keep credentials, database files and provider storage outside Git.

## Management and Source

Open **Files** to work with the native platform filesystem and publish supported Source declarations. Operational capabilities depend on the approved bindings, matching native executables and registered workers for the instance. Default startup hosts management. The old bundled Snow application and its configuration flags are retired. [Filesystem and Source](../../docs/FILESYSTEM.md) · [Native instance requirements](../../docs/production/NATIVE-INSTANCE-COMPOSITION.md)

The fixed-schema asset demo is a compatibility workflow for the explicit `MORIA_LOCAL_RUNTIME=legacy` launcher. It accepts UTF-8 CSV or JSON with `id`, `title`, `region`, `status` and `notes`. Imports replace the source dataset. Stable IDs preserve existing overrides. The import accepts 1–1,000 rows with fields of at most 2,000 characters and a 64 KiB JSON request limit. Use an isolated workspace for this workflow.

## Independent processes and recovery

External PostgreSQL requires an already initialized schema, scoped runtime credentials and `MORIA_DATABASE_MODE=external`. Initialize the schema separately as the migration owner. See the [database guide](../../docs/DATABASE.md) and [runtime process guide](../../docs/production/EXECUTION-PROCESSES.md).

The [static host](../../docs/production/STATIC-DELIVERY.md) can serve a verified promoted artifact separately. It grants no session or API access. Browser API access requires a trusted same-origin routing configuration.

Use the [recovery guide](../../docs/BACKUP-RESTORE.md) for backup scope and isolated restore. See [public hosting](../../docs/PUBLIC-HOSTING.md) for explicit trusted HTTPS listeners.
