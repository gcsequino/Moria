# Generated TypeScript OSDK

Management and Snow now use generated, app-specific clients. The SDK is a browser-safe Fetch client for the hosted API. It uses same-origin cookie authentication; it does not contain keys, provider secrets, profile selectors or permissions. Generating methods never grants access: Moria still checks the current user and application on every request.

## Generate and check

```sh
pnpm sdk:generate
pnpm sdk:check
pnpm moria sdk generate apps/snotel/src/generated/contract.json --out ./work/my-snow-sdk
pnpm moria sdk generate apps/snotel/src/generated/contract.json --out ./work/my-snow-sdk --check
```

The bundled contracts live in packages/osdk/src/bundled.ts. Their object properties and action inputs are imported from packages/contracts/bundled-objects.ts, also used by the server bindings. Generated files are checked in under each app's src/generated. pnpm check rejects stale generated artifacts. Run generation after editing the contract and before building/recovering a bundle.

Each generated directory is standalone: index.ts, runtime.ts and contract.json. Copy it into a TypeScript app and import index.js (NodeNext-compatible source imports; Vite resolves the TypeScript source). No Moria server package or additional runtime dependency is needed. This is generated source distribution, not an npm registry publication.

## Use the scoped client

```ts
import { createClient, ApiError } from './generated/index.js';

const moria = createClient({
  onUnauthenticated: () => { /* show the sign-in screen */ },
});
const session = await moria.auth.session();
const stations = await moria.objects['snow-stations'].all({
  projection: ['uid', 'name', 'triplet'],
  pageSize: 1000,
});
const observations = await moria.objects['snow-observations'].all({
  filter: { op: 'eq', args: [{ field: 'triplet' }, { literal: stations[0].triplet }] },
}, { maxRows: 20000, signal: abortController.signal });
```

The Snow contract exposes only its two object types and status endpoint, with no actions or management endpoints. Management additionally exposes typed asset actions, workspace/app inventory and dataset import. A custom app can define a smaller contract matching its server-approved binding. The generator does not register a server route, create an application, or change grants.

The JSON contract uses version: 1, app, types, objects, actions and endpoints. Object entries name their exported TypeScript type and property ValueTypes; actions specify input ValueTypes. Named types and endpoint inputs/outputs support object properties, optional fields, lists, nullable values and refs to named types. Endpoints declare a fixed /api/ path and GET or POST. See either generated contract.json for a complete executable example. This narrow served-app contract is not the full deployment manifest or an OpenAPI document; generation from arbitrary resource manifests and OpenAPI publication remain future work.

## Query and mutation semantics

- query returns a page with revision, objects and optional nextCursor. get returns revision and object. pages is an async iterator; all collects pages with a default 20,000-row cap.
- Filters type-check field names and literal values. Projection changes the result type. The default explicitly requests all declared fields; use a smaller projection for callers with restricted property access. A forbidden requested property fails instead of being silently represented as a guaranteed field.
- Pagination preserves the query and projection. It never restarts after an expired or invalidated cursor. Repeated cursors fail. Every page remains subject to current server authorization.
- int64 and decimal values remain strings. Date/timestamp values remain wire-format strings. Nullable measurements remain null, not zero. Successful JSON responses are typed from the contract, not runtime schema-decoded; the server is responsible for validation.
- AbortSignal is forwarded to Fetch, including each page. Snow cancels old station/observation requests when selection or login changes. Network cancellation does not guarantee a server-side mutation was rolled back.
- Actions require an explicit idempotencyKey. Retain the same input and key when retrying an uncertain result. The SDK never automatically retries mutations or generates a fresh retry key.

```ts
// Management SDK only:
const key = crypto.randomUUID();
await moria.actions['edit-status'](
  { uid: asset.uid, status: 'Reviewed' },
  { idempotencyKey: key },
);
```

ApiError preserves HTTP status, code, message, correlationId, retryable and details where the server supplies them. Legacy management/auth envelopes may omit the optional diagnostics. Non-JSON responses become INVALID_RESPONSE. Network and abort exceptions propagate. Use the ApiError export from the same generated SDK that made the call; independent standalone SDK copies have independent error classes.

## Authentication and extension boundaries

The generated auth helpers handle session, login, logout and local recovery exchange; React's shared useAppSession hook handles navigation and presentation. No provider SDK ships to the browser. Public hosts continue rejecting recovery exchanges. Browser calls are restricted to the current origin; baseUrl is available for same-origin configuration and non-browser test adapters. The injectable Fetch implementation supports tests; there is no built-in agent credential/delegation flow yet.

The runtime supports the currently implemented object queries, point reads and single-object actions. It does not imply support for sorting, aggregation, relationship traversal, arbitrary backend hosting, a durable scheduler or generated React hooks.

## Verification

Compile-time negative cases reject Snow mutations/admin calls, invalid filter/projection fields, wrong action input types, missing idempotency keys and misuse of nullable values. Runtime tests cover deterministic generation, stale output, precise wire types, pagination, cancellation propagation and typed errors. Real PostgreSQL/Iceberg HTTP integration uses both generated clients to test imports, projected reads, multi-page queries, action replay/conflicts and revocation. Live UI checks are recorded in IMPLEMENTATION.md.
