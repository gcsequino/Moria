# Internal workload serving API

`createApi` creates a Node HTTP server; it does not listen or deploy automatically.
Bind to loopback for local use. Trusted setup supplies resource bindings and mints
workload sessions. Authentication requires `Authorization: Bearer <session>`.
There is no browser login, CORS, public administration or credential-minting route.

Canonical routes:

- `GET /api/v1/objects/:type/:uid?properties=uid,title`
- `POST /api/v1/objects/:type/query`
- `POST /api/v1/actions/:action/invoke`

The prototype `/v1` prefix remains an alias, with the same authorization, cursor
and idempotency semantics. `/management/v1` is reserved and not implemented.

Queries accept the strict ObjectQuery contract implemented in
`services/ontology/src/query.ts`: UID ordering, typed filters, projections,
page size and an opaque cursor. Custom sorting, aggregation and traversal are
not implemented. A returned cursor preserves its original data revision and
expiry; current authorization is checked on each page.

Actions require an `Idempotency-Key` header and a JSON body containing only
`{"input": {...}}`. Retry the same logical action with the same key and inputs,
including after a lost response. Switching URL prefixes does not create a new
idempotency scope. Automatic client retries are not implemented.

Bodies must be UTF-8 JSON, at most 64 KiB; compression is unsupported. Errors use
`{"error":{"code","message","correlationId","retryable","details","requestId"}}`
(field names shown schematically). `requestId` aliases `correlationId` for
prototype compatibility. Details are currently empty. Retryable errors describe
transient contention/timeouts; they do not override idempotency requirements.

OpenAPI schemas, generated SDKs, human OIDC, production ingress, durable audit
coverage and release-owned bindings remain implementation gates. This server
factory and its loopback integration tests do not constitute a deployable API.

A runnable local browser wrapper now composes this API with trusted demo bootstrap,
CSV/JSON ingestion and persistent PostgreSQL. See ../local/README.md. The workload
API itself still does not implement human OIDC or a general management API.
