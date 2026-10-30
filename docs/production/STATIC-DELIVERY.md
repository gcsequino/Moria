# Independent static delivery

Issue #55 supplies a bounded M1 static-host lifecycle using the existing trusted
AppHost store. It does not complete #14/M1: environment/stream ownership and the
remaining service split retain their own acceptance work.

From the repository root, with the documented Node 24 and pnpm dependencies:

```sh
pnpm exec tsx apps/local/src/static-host.ts \
  --app sample-app --directory /path/to/disposable-instance --port 3212
```

All three arguments are required. `--directory` is the instance directory used
by `moria apps`, not the `apps` subdirectory or a source/build directory. The host
reads only `apps/<app>/active.json` and that app's immutable releases. Select a
trusted app ID and a decimal port from 1024 through 65535. The listener binds only
`127.0.0.1`; there is no public-address option or environment-variable fallback.
An occupied port fails startup. SIGINT and SIGTERM close the listener and its
connections; restarting preserves the promoted pointer and all release bytes.

Prepare and promote artifacts separately using the existing operator commands:

```sh
pnpm moria apps publish sample-app /path/to/built-artifact --directory /path/to/disposable-instance --json
pnpm moria apps promote sample-app <full-sha256-version> --directory /path/to/disposable-instance
```

Static startup verifies every file in the promoted release before listening.
Missing promotion, missing files and integrity failures fail startup; startup
never builds, bootstraps, repairs, overwrites or cleans up artifacts. Publication
and promotion remain trusted operator operations. Later stable-page requests
resolve the active pointer, while HTML pins assets to `/releases/<version>/`.
Old release assets remain readable after promotion and restart. Requested assets
continue to receive the existing manifest/hash checks and safe-path validation.
HTML is no-store; versioned assets retain immutable caching.

The shared listener preserves the CLI's exact loopback Host/Origin checks, CSP,
no-service-worker policy and `serveApp` routing. Existing custom-app CLI port
restrictions and management-ID rejection remain intact. The new entrypoint may
explicitly select `--app management` to serve an already-promoted management
shell. This grants no API permission, session or recovery credential. No
PostgreSQL connection, workspace, workload queue, auth provider, warehouse,
recovery key or execution handler is initialized by this entrypoint.

## API routing boundary

Static delivery has no management, object, action or authentication handler. It
has no CORS allowance, request forwarding or proxy. API requests must reach the
API service separately. Unknown extensionless routes may receive the existing
SPA shell; receiving HTML does not execute a backend operation.

The accepted production architecture uses trusted proxy routing: same-origin
`/api` traffic must be routed separately to the API service, including auth
routes; legacy `/management/` and `/local/` routes, if used, also belong to the API
service. Static-only delivery on a separate port is not a fully functional
cross-origin management client. Generated SDK same-origin assumptions and API
session/authorization checks remain unchanged. This entrypoint accepts only its
exact loopback Host and Origin; production ingress compatibility/configuration
is separate work, and this change supplies no public-origin binding, DNS or
proxy configuration.

## API-only companion (#58)

`pnpm exec tsx apps/local/src/api.ts` uses the existing external PostgreSQL and
auth/workspace composition without management artifact bootstrap or serving.
It starts with no promoted management release or dist directory. Root, static
assets, release paths and unknown UI routes return uncached JSON 404s on both
local and configured public API listeners after their existing Host/Origin and
HTTPS proxy checks. Later publication/promotion cannot turn that listener into a
static host. Authorized inventory and lineage may still read AppHost metadata.

`pnpm start` retains combined development bootstrap/static behavior. Prepare and
promote artifacts separately for independent static delivery; neither API-only
nor static-only builds them. Stopping either listener leaves the other and
external PostgreSQL running. API permissions, CSRF, recovery and session checks
remain unchanged; same-origin routing and local-development limits above still
apply. This supplies no production ingress configuration or client changes.

## Evidence and remaining gates

`tests/static-host-process.integration.test.ts` uses actual child processes and
temporary AppHost artifacts. Static-only cases run with no database/provider
environment. They exercise promotion, old version URLs, integrity failures,
missing promotion, invalid configuration, Host/Origin rejection, API/auth
non-dispatch, occupied ports, restart and both shutdown signals, including an
incomplete HTTP connection. A separate real API child and disposable PostgreSQL
cluster retain an authenticated synthetic asset across static restarts; the test
checks the asset through the real object API and keeps the database alive after
API shutdown. No live installation or provider account is used.

Run `pnpm check` for the full gate, including unchanged generic hosting tests.
See [implementation evidence](../IMPLEMENTATION.md) for results. Independent
environment checks, independent review with no findings, current-main integration
and Linux quality CI on the exact revision remain mandatory controller gates.
Local evidence alone does not close #14/M1 or certify production delivery.
