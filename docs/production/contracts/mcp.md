# Native scoped MCP contract

The endpoint is `/mcp` (also `/api/mcp`) on the native serving role. It implements
MCP revision `2026-07-28` Streamable HTTP: one POST per JSON-RPC request, required
version/capability request metadata and mirrored method/name headers, JSON
responses and no protocol session or background GET stream. Supported methods
are `server/discover`, `tools/list`, `resources/list`, `resources/read` and
`tools/call`. Unsupported versions and methods fail explicitly. The server does
not advertise prompts, sampling, subscriptions, arbitrary SQL or shell tools.

`MORIA_NATIVE_MCP_PROFILES` is a closed operator-configured array of environment,
application and ObjectType/Action UUID ceilings. The actual verified session
selects its profile. Client metadata cannot select a principal or application.
Use a dedicated bearer credential or an actual app-bound browser cookie with its
normal origin/CSRF checks. The local global CLI recovery key is rejected.
Remote service exposure uses the native HTTPS-origin/proxy configuration.

Discovery intersects the configured ceiling with current actor/application
operations, filesystem access, retained markings and native resource policy.
Descriptions expose authorized type/input schemas and immutable resource
identities, never policy expressions, provider configuration or credentials.
Each page inspects at most 100 configured items and releases at most 50 visible
items. Database cursors bind actor, profile, method and security/instance epochs,
expire after the original 15 minutes, and pin no object generation. Metadata
cache TTL is zero. A denial while filtering one item is rolled back to a
savepoint before discovery continues.

Query tools are `query_<ObjectType UUID>` and accept `{query}` using the existing
native query contract. Action tools are `action_<Action UUID>` and accept
`{idempotencyKey,input}`. Both call protected native serving directly; current
policy, hidden-field restrictions, transaction admission and stable action
receipts remain authoritative. Exact integers and decimals retain C03 string
wire representations. Tool failures contain a static safe code and `isError`.

The endpoint shares native connection/request budgets, caps request bodies at
64 KiB and response bodies at 8 MiB plus bounded protocol framing, and enforces a
30-second request deadline. Discovery metadata normally stays within 4 MiB per
page, with one bounded larger item. It does not spawn a compiler or Node proxy.

`moria mcp config` emits the endpoint, protocol and `MORIA_API_TOKEN` credential
reference without printing a credential. `moria mcp discover|tools` uses that
dedicated token, validates the instance origin and bounds response bytes.

The real integration fixture uses two native replicas, PostgreSQL, protected
objects/actions and the locked official Python SDK 2.3.0. Its first passing trial
recorded eight cases with zero skips in 7.19 seconds at
`native-models/20261030T211807Z`. Production workload qualification and restored
instance testing remain separate gates.
