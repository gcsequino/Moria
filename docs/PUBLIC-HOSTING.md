# Public HTTPS hosting

Legacy installation runbook: these Windows instructions are retained for the
existing installation only. Linux is now the supported development and target
hosting OS. This policy change does not migrate or retire the existing instance;
production migration remains separately gated by the production roadmap.

The current local instance is published through a dedicated Cloudflare Tunnel:

- Management: https://moria.gsequino.dev/
- Snow Observatory: https://snow.gsequino.dev/snow/

The existing root domain and other applications are unchanged. This is a tunnel to the Windows computer, not an always-on cloud deployment. The computer, Moria process and cloudflared connector must remain running. No automatic Windows logon/reboot startup is installed.

## Google configuration

Add these Authorized redirect URIs to the existing Web OAuth client, preserving both localhost callbacks:

- https://moria.gsequino.dev/api/auth/callback
- https://snow.gsequino.dev/api/auth/callback

Use the existing administrator and snow invitations. No new identity grants are implied by DNS or deployment. Public and localhost cookies are separate, so sign in again on each public hostname. Live Google consent on the public hostnames must be verified after the callbacks are registered.

## Start and recover

From the repository root, run these in separate PowerShell terminals:

```powershell
.\deploy\start-public.ps1
.\deploy\start-tunnel.ps1
```

Stop an already-running local instance before starting another; PostgreSQL and HTTP ports must have a single owner. The public launcher enables MORIA_PUBLIC_ORIGIN and MORIA_SNOW_PUBLIC_ORIGIN. Their optional listener ports default to 3212 and 3213. Normal pnpm start without these variables remains local-only.

The connector executable and private token are under ignored .local/cloudflare/. Provision them from the official Cloudflare release and cf tunnels token get; do not commit or print the token. Cloudflare configuration is managed using cf, including tunnel configuration and proxied DNS records. The tunnel is named moria (7c5f1c6a-f505-454f-b96b-0ae0a75225c8). Route only the public listeners, never PostgreSQL or the localhost management listeners. Each hostname maps to its dedicated public listener with the matching HTTP Host header; unmatched traffic receives 404.

Local recovery remains independent of Google and Cloudflare:

```sh
pnpm moria auth recover --app management
pnpm moria auth recover --app snotel
pnpm moria apps recover management
pnpm moria apps recover snotel
```

Recovery links open localhost and are not accepted on the public listeners. If the tunnel is unavailable, use http://127.0.0.1:3210/ and http://127.0.0.1:3211/snow/. CLI ingestion remains unchanged. Stop the connector to remove external reachability without deleting data or DNS.

## Boundaries and verification

Public listeners bind to loopback and require an explicitly configured HTTPS origin, exact Host and exact Origin for browser writes. They require the proxy's HTTPS forwarding signal and never derive the trusted hostname from forwarded headers. The Cloudflare connector is the trusted TLS reverse proxy; do not expose these listeners directly or attach arbitrary proxies. Public responses include HSTS. Session cookies are Secure, HttpOnly, host-only and SameSite=Lax. Public recovery endpoints and CLI-key authentication are disabled. Management retains its explicit admin gate and Snow retains only station/observation reads.

Windows checks pass 72 tests. Integration coverage includes the public OAuth callback, Secure cookies, authorized inventory, forbidden host/origin requests and public recovery/CLI-key rejection against real PostgreSQL. Live HTTPS checks verified both shells, unauthenticated data denial and recovery rejection; local CLI recovery still issues tickets. Public Google sign-in is pending callback setup. TLS termination does not complete production hardening: auth rate limiting, periodic cleanup, operational monitoring, backups and always-on hosting remain separate work.
