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
pnpm moria apps publish sample-app ./path/to/dist --json
pnpm moria apps promote sample-app <full-version-hash>
pnpm moria apps serve sample-app --port 3211
pnpm moria apps rollback sample-app
```

Publish returns the full SHA-256 version. It never promotes implicitly. Rollback atomically exchanges the active and previous versions. `serve` starts a loopback static host on a separate port; it refuses the configured management port and the management app ID. Custom-app API/auth bindings are a separate capability and are not implicitly granted by publishing assets. The first custom host is static-only: no management credentials, CORS bypass, arbitrary user backend, SSR, or shell build execution is supplied.

If a CLI publisher crashes while holding a mutation lock, `pnpm moria apps unlock <id>` removes it only when its recorded process no longer exists. Live owners are never displaced. An incomplete lock (crash before its PID was recorded) requires the operator to verify no publisher is running and remove that one `mutation.lock` file; this is reported explicitly rather than guessed.

## Artifact and routing contract

Each app has `apps/<id>/releases/<sha256>/` and an atomically replaced `active.json`. Manifests include file hashes; the version hashes app ID plus the ordered file manifest. Publication stages complete bytes before making the release directory visible. Promotion validates every file. Serving checks requested file integrity and fails closed on tampering. Builds do not modify a previous release, and old release URLs remain available after promotion. Recovery quarantines damaged copies before replacing them with verified bytes. Garbage collection is not implemented.

Artifacts must contain `index.html`. Symlinks, traversal, dotfiles, unsupported file extensions, oversized files, and excessive file counts are rejected. Limits: 2,000 files, 16 MiB per file, 64 MiB total; no source maps. Treat published artifact contents as code and do not include secrets.

Stable entry URLs redirect to `/releases/<version>/...` with path/query preserved. HTML has a version-specific base URL, so late asset requests from an open older tab cannot accidentally mix releases. HTML is not cached; versioned assets can be cached immutably. Unknown extensionless routes serve the app shell; missing asset paths return 404. The React console uses in-page navigation. Reopen the stable root URL to switch an existing tab to the promoted release.

Each custom app gets a separate origin, rather than sharing the management console's origin/session storage. Static delivery uses a restrictive CSP and does not allow service workers. Management uses shared OIDC/cookie authentication; see [AUTH.md](AUTH.md). Static shells remain publicly readable on loopback. Management also uses Google sessions with separate explicit admin invitations, retaining the independent local operator credential for CLI recovery. Separate ports do not isolate cookies; production deployments of untrusted apps need separate hostnames.

## Management and application authority

The native console uses the management runtime status and resource-specific
routes. Each operation checks current app and principal authority. The native
launcher can host an explicitly configured published application. The CLI
static preview serves assets only. [Administration console](ADMIN-CONSOLE.md) ·
[Native composition](production/NATIVE-INSTANCE-COMPOSITION.md)

`pnpm moria apps recover management` rebuilds and promotes the bundled console.
Explicit release URLs continue to serve their pinned assets. [Current verification](IMPLEMENTATION.md#native-integration-and-production-goals--november-5-2026)
