# Station Explorer: a small Moria React app

This runnable starter renders a station list using its own generated OSDK, browser sessions and a read-only contract. It includes source, standalone generated files and an immutable workflow declaration. It never requests a management key, copies Snow's browser token or makes cross-origin API calls.

## Build and preview

From the Moria repository root (using the repository's already installed dependencies):

```sh
pnpm moria sdk generate examples/station-explorer/app-contract.json --out examples/station-explorer/src/generated
pnpm exec tsc --project examples/station-explorer/tsconfig.json --noEmit
pnpm exec vite build --config examples/station-explorer/vite.config.ts
pnpm exec vite preview --config examples/station-explorer/vite.config.ts --host 127.0.0.1 --port 4174
```

Open the URL Vite prints. The shell renders, but the preview has no auth/data API and will explain that a server binding is needed. This is expected: no fake stations are supplied. `pnpm exec tsx scripts/verify-starter.ts` also builds, checks SDK drift, publishes into an isolated temporary local AppHost, verifies integrity/workflow metadata and checks real HTTP HTML/JS plus API isolation. It does not touch live apps.

To copy the example outside this repository, retain `src/generated`, install its package dependencies, then use `pnpm build` and `pnpm preview`. Regenerating the SDK still requires the Moria CLI. The generated client itself has no dependency on Moria server source. `contracts.typecheck.ts` verifies that actions, management endpoints, observations and unknown fields are unavailable in this contract.

## How it works

`app-contract.json` requests only five properties of `snow-stations`; `actions` and `endpoints` are empty. Auth helpers are included in every generated SDK. The app checks `/api/auth/session`, offers the server-configured provider buttons and queries the station route with an explicit projection and bounded pagination. Requests use same-origin cookies and cancellation. Session or permission errors clear visible station data. The server remains authoritative: typed methods are not grants.

`vite.config.ts` packages `moria-workflow.json` alongside the bundle, with app ID `station-explorer`, environment `snotel` and the generated contract. The lineage reader can show declared station consumption after publishing. This metadata is descriptive and publicly readable with the static artifact; never put secrets or credentials in it.

## Connect it to real data safely

Generic `moria apps serve` is currently **static-only**. Publishing this app does not register an API, OAuth callback, identity or grant. Generic app bindings are not automated yet. Use a separate staging instance for an end-to-end experiment, or implement an explicit server binding using the existing `createScopedAppServer` and `BrowserSessions` primitives.

For that binding, an instance operator must:

1. Choose a dedicated origin/port and app identity `station-explorer`; mount this artifact through `AppHost` under that app ID.
2. Bind `createScopedAppServer` to the staging Snow service's `snow-stations` object service only. Leave its actions map empty (the scoped host already does this).
3. Configure `BrowserSessions` for environment `snotel`, application `station-explorer`, that exact origin and `/` entry path. Use a separately authorized read principal/grant and only `snow-stations:read` application scope. Session signing/encryption material stays server-side.
4. Configure the Google provider and authorized users server-side, then register the exact `<origin>/api/auth/callback` redirect URI in Google. JavaScript origins have no path. Login and API routes must share the app origin; do not proxy requests to production with a copied privileged cookie.
5. Verify signed-in reads, uninvited-user rejection, revocation, absent actions/admin routes and CLI recovery before exposing the listener publicly. Preserve the existing management and Snow listeners.

Current CLI Google setup/recovery commands target the bundled `management` and `snotel` apps. A new custom binding needs explicit operator integration for provider configuration and recovery; do not assume `moria auth ... --app station-explorer` exists. An alternative for experimentation is a disposable staging Snow binding with the example UI, retaining its existing auth; that tests the UI but is not a distinct custom-app identity. Never replace the live Snow artifact for this exercise.

## Publish, promote and roll back an isolated static app

From the repository root after building:

```sh
pnpm moria apps publish station-explorer examples/station-explorer/dist --directory .local/starter-instance
pnpm moria apps promote station-explorer <version-from-publish> --directory .local/starter-instance
pnpm moria apps verify station-explorer --directory .local/starter-instance
pnpm moria apps serve station-explorer --port 4175 --directory .local/starter-instance
```

This serves only static assets. After publishing and promoting a second release, recover the previous release with:

```sh
pnpm moria apps rollback station-explorer --directory .local/starter-instance
```

Rollback requires a previous release. It changes the active artifact, not grants, data, Google settings or sessions. `apps recover` rebuilds bundled apps only; keep your custom app source/build output and use publish/promote/rollback for its recovery. App hosting commands remain usable when the management UI is unavailable.
