# HTTP transport ownership

`services/http/src/server.ts` owns native HTTP construction, admission, timeouts,
body decoding, browser transport headers and safe error responses. Routing,
resource authorization, browser sessions and artifact delivery retain their
existing owners. There is no caller-supplied admission callback or bypass flag.

| Production caller | Trusted profile | Application work / contract coverage |
| --- | --- | --- |
| apps/api/src/server.ts | workload | Workload bearer authentication; real SQL HTTP and release-expiry tests |
| apps/local/src/server.ts | management | Browser sessions or local CLI; management auth and independent API process tests |
| services/app-host/src/scoped.ts | scoped | App-bound browser session or local app key; platform composition and auth tests |
| services/app-host/src/static-server.ts | static | Artifact files only; independent static process and HTTP boundary tests |

Management and scoped profiles may select public HTTPS ingress only from trusted
startup configuration. `browserProfile` verifies matching browser auth and disabled
local recovery, freezes the profile, and registers its identity privately. A
structurally similar object cannot manufacture a browser profile. Public requests
must arrive over loopback with the configured Host, exact applicable Origin and
`X-Forwarded-Proto: https`; responses include HSTS. The proxy must sanitize forwarded
headers and exclusively control that ingress. **Loopback trusts the host:** a local
process that can reach the public listener can impersonate this proxy. This is not
co-tenant isolation or a cryptographic proxy credential. Deployment must establish
the trusted host/proxy boundary; this change does not install or reconfigure one.
Forwarded headers never select a profile or upgrade a local listener into public mode.

All listeners require actual loopback socket ingress and exact Host. Workload
listeners reject Origin, retaining their nonbrowser/no-CORS contract. Other
listeners allow absent Origin for navigation and CLI and require an exact match
when supplied. Browser mutations require same-origin CSRF checks. Local operator
keys select CLI authority only without Origin or Sec-Fetch-Site; public keys never
select CLI authority. Local recovery-ticket creation separately checks the operator
key and absence of browser headers. Recovery consumption retains same-origin and
single-use ticket checks. Public listeners deny both recovery endpoints before auth
or provider work. Legacy key-only management/scoped requests with browser headers
now fail closed; browser usage goes through the existing cookie session flow.

Workload servers used as internal dispatchers still retain their normal admission
if later listened on. `dispatchHttp` accepts only a registered workload target and
the same request/response pair already admitted by the outer transport owner. The
outer origin, CLI classification and request ID carry through the nested call;
resource authorization still executes. Raw request-event emission is removed.

All profiles reject invalid origin-form URLs, malformed path encoding and declared
bodies over 64 KiB before application handlers. JSON endpoints share content-type,
identity encoding, fatal UTF-8 decoding, actual streamed 64 KiB limit and interrupted
body rejection. Request/header timeouts are 30/10 seconds, including static delivery.
JSON body parsing belongs to JSON routes; GET/HEAD/static delivery does not require
JSON. Shared CSP, nosniff and no-referrer headers precede application routing.

Intentional response differences:

- Workload JSON remains `application/json; charset=utf-8`; management/scoped JSON
  remains `application/json`. Nested workload responses retain workload media type.
- Static admission errors remain plain text (empty 403, `Bad request.` for 400).
  Existing artifact 404/503, GET/HEAD, redirect, HTML no-store and versioned immutable
  asset behavior stays in `serveApp`.
- Known domain errors retain their status mapping. Management resource/workload
  busy and idempotency conflict remain 409; workload resource busy is 503. Unknown
  exceptions return a bounded generic 500, with correlation IDs for JSON, never
  internal exception text. An exception after response headers destroys the response.

`pnpm check:architecture` discovers production source and restricts runtime
HTTP/HTTPS imports to this owner. Exact named outbound Agent/get exceptions are
registered for S3/AWDB clients; they do not permit namespace/dynamic listener access.
Unsupported raw net/TLS/HTTP2 transport imports fail. Typed HTTP Server listener
mutation/retrieval, timeout/limit overrides or request emission outside the owner fails; only lifecycle error/close/
listening event use is allowed. New factory/profile/dispatcher callers must be
registered in the protected policy, with existing contract files. Negative fixtures
cover aliases, re-exports, dynamic loading, new callers/profiles and event bypasses.
`tests/http-boundary.test.ts` sends real requests through all six transport profiles
and real API/management/scoped/static factories, asserting rejection before handler,
provider, operation or file work. Existing real database/auth/process tests remain.

These structural checks are not an arbitrary JavaScript sandbox or proof that a
test's assertions are sufficient. Reflective/erased types and edits to a trusted
owner require independent review. Test file registration proves presence only.
The separately reviewed factory policy must protect owner, callers, profiles,
checker, inventory, tests and workflow before ordinary changes are dispatched.
That policy is proposed, not installed. GitHub protection/ruleset APIs returned 403
under the current private-repository plan, and publishing credentials retain admin
rights. Independent non-bypassable enforcement and human transport review remain
unmet; a green local or CI check does not satisfy those administrative obligations.
