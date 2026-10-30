# Increment function

This is a TypeScript function using the `typescript-v1` OCI runner ABI. It returns
`{"value": input.value + 1}` and has no external dependencies. The image uses
Node 24's native TypeScript type stripping; syntax requiring code generation must
be compiled in CI before packaging instead.

From the Moria source root, build and push to an operator-chosen registry:

```sh
docker buildx build --file examples/functions/Dockerfile --tag registry.example/team/increment --sbom=true --provenance=true --push .
```

Record the resulting manifest digest, preload that exact image on the worker with
`docker pull registry.example/team/increment@sha256:<digest>`, and use the Function
declaration in [the walkthrough](../../docs/FUNCTIONS.md). The declaration's
inputs/outputs each contain the required `value` float64 field.

In a standalone checked-out code repository, prepare the runtime with
`node scripts/build-function-runtime.mjs <runtime-directory>` from the matching
Moria release on Node 24. Copy that complete directory into the image and execute
`node /function/runtime/runner.mjs /function/function.ts`. It contains the runner,
shared value codecs and local module metadata. Keep it pinned alongside the
function source. The example Dockerfile prepares it in a separate build stage.
Publish
the source directory and CI export array using the filesystem workflow.

Invoke with `{"value": 41}` and a stable request key. Successful output is
`{"value": 42}`. The worker must be running; API-only processes never launch images.

For ontology access, generate the application's SDK during checkout/CI and compile
it to JavaScript alongside the function. Export its factory under the runner's
`createOsdk` name; imports ending in `.js` must exist in the packaged artifact.
The normal API and function supervisor must bind the same named object/actions.

```ts
import { createClient } from './generated/index.js';
import type { FunctionContext } from '<moria-release>/packages/platform/src/function-context.js';

export const createOsdk = createClient;

export default async function calculate(
  input: { uid: string },
  context: FunctionContext<ReturnType<typeof createClient>>,
) {
  const asset = await context.osdk.objects.assets.get(input.uid);
  const response = await context.platform.fetch('/api/management/v1/fs/home', {
    signal: context.signal,
  });
  if (!response.ok) throw new Error('Home lookup failed');
  context.log('Read asset and home');
  return { title: asset.object.title, home: (await response.json()).home.name };
}
```

Declare matching typed inputs/outputs on the Function resource. Package generated
SDK code and any dependencies into the immutable image; invocation does not
download them. The OSDK contains ontology operations. Use the separate platform
client with `createTransport({fetch:context.platform.fetch})` for filesystem and
function APIs, or issue relative API requests directly as above. Credentials and
caller identity are supplied by the supervisor, not request headers.

Function parameters and results use the shared version-1 envelope profile:
scalars, nested records/lists and optional nested fields. Int64 and decimal are
exact strings in TypeScript; date/timestamp/UID are canonical strings as well.
Return plain JSON data with finite numbers. The runner rejects lossy native
values before serialization, then the supervisor validates the wire result.
See [the platform type contract](../../docs/spec/core-contracts.md#c03--values-rows-and-schema-evolution)
for descriptors, normalization, Python native bindings and compatibility rules.

Choose `readOnly` for reads, `editProposal` for staged ontology action batches, or
`readWrite` for authorized immediate writes. An action call uses a stable logical
key, for example `{idempotencyKey:'update-title'}`. A later exception or cancellation
does not undo an already committed `readWrite` call. Inspect the invocation's
`calls` trace before deciding what to do after an uncertain failure. See the
[effect and proposal contract](../../docs/FUNCTIONS.md#function-context-and-result-safety).
