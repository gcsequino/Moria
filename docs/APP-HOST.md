# Application host and bundled management console

Moria now ships its local management console as a built React/Vite application. It is published, verified and promoted through the same AppHost store used for custom static apps. `pnpm start` installs the initial console if needed, starts persistent PostgreSQL, and serves the promoted app on the local management origin. The existing dataset and edits are preserved.

## Operator commands

Run from the repository root. The default instance directory is `.local/workspace`; pass `--directory <instance-dir>` or set `MORIA_LOCAL_DIR` for another instance. CLI commands use the local filesystem and do not require the management page, HTTP server, or database to be running.

```sh
pnpm moria apps bootstrap
pnpm moria apps list --json
pnpm moria apps verify management
pnpm moria apps recover
pnpm start
```

`bootstrap` builds and installs the bundled console only if no active installation exists. It does not override a deliberate promotion. `recover` builds the console from this checkout, restores corrupted copies of that same content-addressed release if necessary, and promotes it. It does not erase datasets, identities, grants, or overlays. The local operator executing these commands is trusted.

For a custom app, build its static artifact with a relative base (for Vite: `base: './'`), then:

```sh
pnpm moria apps publish snow-observatory ./path/to/dist --json
pnpm moria apps promote snow-observatory <full-version-hash>
pnpm moria apps serve snow-observatory --port 3211
pnpm moria apps rollback snow-observatory
```

Publish returns the full SHA-256 version. It never promotes implicitly. Rollback atomically exchanges the active and previous versions. `serve` starts a loopback static host on a separate port; it refuses the configured management port and the management app ID. Custom-app API/auth bindings are a separate capability and are not implicitly granted by publishing assets. The first custom host is static-only: no management credentials, CORS bypass, arbitrary user backend, SSR, or shell build execution is supplied.

If a CLI publisher crashes while holding a mutation lock, `pnpm moria apps unlock <id>` removes it only when its recorded process no longer exists. Live owners are never displaced. An incomplete lock (crash before its PID was recorded) requires the operator to verify no publisher is running and remove that one `mutation.lock` file; this is reported explicitly rather than guessed.

## Artifact and routing contract

Each app has `apps/<id>/releases/<sha256>/` and an atomically replaced `active.json`. Manifests include file hashes; the version hashes app ID plus the ordered file manifest. Publication stages complete bytes before making the release directory visible. Promotion validates every file. Serving checks requested file integrity and fails closed on tampering. Builds do not modify a previous release, and old release URLs remain available after promotion. Recovery quarantines damaged copies before replacing them with verified bytes. Garbage collection is not implemented.

Artifacts must contain `index.html`. Symlinks, traversal, dotfiles, unsupported file extensions, oversized files, and excessive file counts are rejected. Limits: 2,000 files, 16 MiB per file, 64 MiB total; no source maps. Treat published artifact contents as code and do not include secrets.

Stable entry URLs redirect to `/releases/<version>/...` with path/query preserved. HTML has a version-specific base URL, so late asset requests from an open older tab cannot accidentally mix releases. HTML is not cached; versioned assets can be cached immutably. Unknown extensionless routes serve the app shell; missing asset paths return 404. The React console uses in-page navigation. Reopen the stable root URL to switch an existing tab to the promoted release.

Each custom app gets a separate origin, rather than sharing the management console's origin/session storage. Static delivery uses a restrictive CSP and does not allow service workers. The snow app now uses shared OIDC/cookie authentication; see [AUTH.md](AUTH.md). Static shells remain publicly readable on loopback. Management also uses Google sessions with separate explicit admin invitations, retaining the independent local operator credential for CLI recovery. Separate ports do not isolate cookies; production deployments of untrusted apps need separate hostnames.

## Management versus object APIs

The console uses a generated app-specific OSDK; see [OSDK.md](OSDK.md). Its endpoint bindings call:

- `GET /api/management/v1/workspace`: readiness/status for an authenticated administrator.
- `GET /api/management/v1/apps`: app inventory, gated by the current `instance:manage` grant and application scope.
- `POST /api/management/v1/datasets/assets/import`: real Iceberg ingestion, gated by administrative and dataset-write permissions.
- `/api/v1/objects/...` and `/api/v1/actions/...`: existing protected query/action service.

`/local/status` and `/local/import` remain compatibility aliases. Being bundled does not confer a server-side grant. The Applications page is read-only; the CLI owns publishing, promotion and recovery. Human login is implemented for snow and management through the shared auth host. Legacy management paths remain available to the authenticated local CLI. Complete management OpenAPI/client generation, deployed ownership, production TLS and general application release bindings remain future platform work.

## Verification

Tests cover two releases, deterministic publication, promotion, rollback, old asset URLs, SPA fallback, missing assets, traversal, tamper rejection, release/pointer recovery, busy locks, invalid imports and independent CLI bootstrap/recovery. The real PostgreSQL/Iceberg HTTP test also exercises management-route permissions, canonical import, authorization, persistent overrides and database restart. Windows browser QA covers the React edit and refresh flow and Applications inventory. Linux has not been revalidated for this checkpoint.

## Scoped SNOTEL binding

The bundled Snow Observatory is composed by `pnpm start` on its own origin (default 3211). `createScopedAppServer` binds trusted server-selected object services to an app-specific key and internal workload session. Hosting/promoting a static artifact cannot request extra scopes or inject a caller-selected identity. Only the two snow read services and read-only sync status are exposed. Generic `apps serve` still serves static assets only. See [SNOTEL.md](SNOTEL.md) for ingestion, source semantics and scope details.

Bundled recovery accepts an explicit app: `pnpm moria apps recover snotel` or `pnpm moria apps recover management`; omission retains the management default.

Stable application routes now serve the active HTML without exposing its release hash in the page URL. HTML remains `no-store` and its injected base pins assets to the selected release. Explicit release URLs remain available for pinned views. The scoped snow app mounts at `/snow/`; its origin root redirects there. This supersedes the earlier stable-route-to-release redirect behavior.
