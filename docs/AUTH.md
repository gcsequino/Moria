> Default startup hosts management only. Run `auth grant-admin --email your-account` after configuring Google; legacy reader invitations do not grant management access. Shared provider declarations are validated, but management reads credentials only for providers with administrator invitations.

# Hosted application authentication

Management and explicitly composed custom apps use Moria-owned opaque cookie sessions. Google is the first configured OpenID Connect provider. `openid-client` handles discovery, authorization-code exchange, PKCE, state, nonce, issuer/audience/expiry validation and JWKS signature verification. No Google SDK or provider credential ships to React.

## Google setup

Run `pnpm moria auth setup` from the repository root for values matching the configured local port.

1. Open [Google Auth Platform](https://console.cloud.google.com/auth/clients) and select or create a project. Configure Branding and Audience. While the application is in Testing, add your account as a test user.
2. Create a **Web application** OAuth client. Register this exact redirect URI for the default port: `http://127.0.0.1:3211/api/auth/callback`. `localhost` and `127.0.0.1` are different redirect URIs. JavaScript origins are not needed for this server-side flow.
3. Download the client JSON into a private local folder, outside the repository. Do not paste its client secret into chat or commit it.
4. Run `pnpm moria auth configure-google --config "/private/path/client.json" --email "you@gmail.com"`. The CLI checks that the downloaded Web client contains the exact callback and writes a credential-file reference and initial invitation into ignored `.local/workspace/auth/providers.json`.
5. Run `pnpm moria auth grant-admin --email "you@gmail.com"` for the explicit administrator invitation. Restart `pnpm start`, open `http://127.0.0.1:3210/`, and choose **Continue with Google**. This requests only `openid email profile`.

This first invitation flow supports Gmail and Google Workspace accounts. Google accounts backed by third-party email addresses require a separate ownership-verification flow and are rejected. Google's `email_verified` alone does not establish current ownership of such addresses. See [Google's identity guidance](https://developers.google.com/identity/gsi/web/guides/verify-google-id-token) and [OIDC setup](https://developers.google.com/identity/openid-connect/openid-connect).

Actual Google account sign-in is unverified until real credentials are supplied and the consent/callback flow succeeds. Tests use a local HTTP issuer with signed ID tokens and real PostgreSQL; they do not claim to test Google's service.

## Management administrators

Register the callback printed by `pnpm moria auth setup` on the Google Web client. Management invitations use the provider's `managementEmails` list. The first accepted management sign-in receives the operator-defined application grants. Returning sign-ins do not restore revoked grants.

The native console uses its authenticated runtime and resource routes. Operator
access requires current authorization. Source publication and application scopes
cannot grant additional permissions on their own.

## Trust boundaries and extension points

- `services/security/src/oidc.ts`: trusted provider adapter. Add another standards-compliant OIDC provider through configuration (`id`, `label`, HTTPS `issuer`, `credentialsFile`, `emails`). Credentials can use Google's `web` envelope or plain `client_id`/`client_secret`. The browser discovers provider labels through the shared session endpoint.
- `BrowserSessions`: persistent login transactions, identity resolution, session issuance, refresh and logout. External identities use `(environment, issuer, subject)` and map to stable Moria principal IDs. Email invitations bind once to a subject; identical emails at different issuers never merge principals.
- Existing `Authority`: current application scopes, principal state, grants and security epochs. Initial invitation acceptance grants only the server binding's operations. Returning logins do not restore revoked grants or disabled principals. Provider configuration changes invalidate existing provider sessions at restart; disabling and later re-enabling does not revive them.
- `services/app-host/src/auth.ts` and `apps/shared/auth.tsx`: reusable HTTP endpoints and React session hook. A new application supplies its trusted environment/application binding and allowed initial operations. It cannot expand access through a manifest or request header.

Other authentication protocols can feed the same principal/session layer through a future credential adapter; WebAuthn, SAML, SCIM/directory synchronization and a bundled Keycloak deployment are not implemented here. OIDC brokering is possible through a trusted HTTPS issuer. There is no automatic group-to-permission mapping.

## Session and synchronization behavior

Browser tokens are random, stored hashed in PostgreSQL, and bound to an environment and app. Cookies are HttpOnly, SameSite=Lax, scoped to `/api/`, and Secure for HTTPS bindings. The local server accepts only its exact loopback Host and Origin. State-changing browser requests require an exact Origin; OAuth callbacks additionally require the browser-bound, single-use state and PKCE transaction. The normal data path ignores caller-supplied workload bearers, profile selectors and legacy keys.

Sessions last at most one hour, bounded by the provider access-token lifetime. There are no offline refresh tokens. Returning users sign in again through their provider's SSO. Access tokens remain on the server, encrypted with AES-256-GCM using the persistent ignored `auth/session-key`. Keep that key with private instance backups; deleting it prevents refreshing existing provider sessions.

Active requests revalidate UserInfo after 30 seconds; protected transaction checks reject session verification older than 60 seconds. Provider errors fail closed. Refresh checks the same subject and email and does not enable a disabled principal or modify local policy attributes. Local grant/principal/session changes retain the existing transaction and epoch checks.

**UserInfo is not a directory synchronization API.** This checks the provider's acceptance of the access token and current profile, not a guaranteed 60-second propagation of Google account suspension or Workspace group changes. No external group claims are used for authorization. Full directory synchronization and its bounded-revocation contract remain pending; use Moria's authority for immediate local revocation.

Separate ports are separate browser origins but **not cookie security boundaries**. Cookie names and server-side audience bindings separate this local implementation's apps; production isolation of mutually untrusted apps requires separate hostnames, HTTPS and an appropriate session broker. Management has its own app-bound Google session and explicit admin invitation; the local operator credential remains available to the CLI for recovery. Static application bundles remain publicly readable on loopback.

## CLI recovery

- `pnpm moria auth recover`: defaults to management recovery. Snow recovery is retired.
- `pnpm moria auth recover --app management`: uses the private local operator credential to issue a single-use link for a 15-minute management recovery session, independently of Google. Recovery is labeled in the UI.
- `pnpm moria auth disable`: archives provider configuration, even if malformed. Restart the instance to invalidate provider sessions, then use recovery. The configuration can be restored from the archive; users must sign in again.
- `pnpm moria apps recover management`: rebuild and promote bundled UI assets without login or a running database. Existing application publication/rollback tools are unchanged.
- Snow commands and `--app snotel` fail explicitly.

Local recovery keys are never sent on ordinary browser data requests. Do not expose the local runtime as a production service. Production TLS/proxy bindings, per-user administrative UI, rate limiting, periodic expired-session cleanup, provider key rotation operations and deployment hardening remain work.

## Public origins

Explicit HTTPS proxy listeners are now available alongside localhost. See [PUBLIC-HOSTING.md](PUBLIC-HOSTING.md) for the separate public authentication listener and local recovery boundaries. Public Google sign-in requires registering those callbacks; localhost login does not transfer cookies to a public hostname.
