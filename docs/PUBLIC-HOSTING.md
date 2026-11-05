# Public management hosting

The combined native launcher can bind separate management and authentication listeners on loopback for a trusted HTTPS reverse proxy. Configure the public origins and route each hostname only to its corresponding listener.

| Setting                    | Purpose                                           |
| -------------------------- | ------------------------------------------------- |
| `MORIA_PUBLIC_ORIGIN`      | Exact HTTPS origin for management                 |
| `MORIA_PUBLIC_PORT`        | Loopback management listener, default 3212        |
| `MORIA_AUTH_PUBLIC_ORIGIN` | Exact HTTPS origin for the separate OIDC listener |
| `MORIA_AUTH_PUBLIC_PORT`   | Loopback OIDC listener, default 3213              |

Use distinct listener ports. Preserve the expected Host header and establish the trusted HTTPS forwarding signal at ingress. Register `/api/auth/callback` on the configured public authentication origin with the identity provider. Local and public origins use separate browser sessions. Keep PostgreSQL and local management and recovery listeners behind the host boundary. [Authentication](AUTH.md) · [HTTP transport ownership](production/HTTP-BOUNDARY.md)

Recover management locally with `pnpm moria auth recover --app management`. Public listeners have no local recovery or CLI credential binding. Snow origins and their launcher options are retired. Current composition evidence and open acceptance gates are in the [implementation checkpoint](IMPLEMENTATION.md#native-integration-and-production-goals--november-5-2026).
