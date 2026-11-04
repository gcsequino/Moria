# Model gateway v1

`NativeModels` owns inference protocol handling in Rust. An existing inference
engine runs the model. The independent role starts with `moria-runtime models
serve` and `MORIA_NATIVE_MODEL_LISTEN`; the current listener requires loopback.
Public hosting and the application release composer remain integration work.

Authenticated routes are `POST /api/v1/models/{resource}/chat`, `/embed`,
`/stream` and `/probe`. Resource references are URL-encoded path components.
Bearer sessions resolve current platform identity; JSON actor IDs, provider URLs,
credentials and caller-selected resource classes are never authority. Chat,
embedding and stream calls require a stable `idempotency-key` header. A probe
accepts `{}` and performs actual synthetic provider requests under the caller's
current invocation/destination permissions.

Trusted bindings pin the upstream model, declared model revision, environment
endpoint/secret references, destination policy, resource class, source markings,
dimension and requested capabilities. Generic OpenAI-compatible providers do not
necessarily expose immutable model bytes: the operator must supply a genuine
upstream revision where available. The local smoke test records observed Ollama
model digests. Endpoint URLs are pinned during each call and cannot change
mid-call. Provider redirects are rejected.

Chat input contains `messages`, `maxTokens` and optional `tools` and
`responseFormat`. Message roles are system/user/assistant/tool. Tool responses
carry `toolCallId`; assistant history can carry normalized `toolCalls` with ID,
name and JSON arguments. Tool definitions contain name, optional description and
parameters schema. A response format contains name and schema. This protocol profile accepts tools or a response format per call; their simultaneous use fails explicitly. Structured
content and tool arguments are independently validated against the actual
requested JSON Schema after the provider responds. The initial schema profile
has no references, a 256-node/24-depth limit, a 64 KiB schema limit and linear
regular expressions; unsupported schemas fail explicitly. The validator has
HTTP and filesystem reference resolution disabled.

Embeddings take `{"input":["text",...]}` with at most 64 texts and return vectors
in input order. Indices must be unique and complete, dimensions must match the
pinned generation, and every component must be finite and representable as
float32. Vectors that become all zero in that representation are rejected.
The trusted `embeddingProtocol` defaults to `openai-compatible-v1`. The
`ollama-v1` adapter uses the native `/api/embed` operation with `truncate:false`;
provider context overflow returns `MODEL_CONTEXT_LIMIT` and retains a terminal
failed call rather than silently discarding input. This explicit behavior follows
[Ollama's embedding API](https://docs.ollama.com/api/embed). The operator endpoint
remains the trusted `/v1/` base used by the compatible chat routes.

Successful results and capability inspection include conservative validated `markings` for downstream flow checks. Provider usage counters normalize to exact decimal strings; missing usage is
unknown, not zero. Provider response extensions, IDs and usage remain under `metadata.provider`; normalized content, tool argument strings and embedding arrays are not duplicated there. Stream extensions are retained in ordered `streamExtensions`.

SSE emits ordered text/tool/usage events and a terminal result. Tool deltas are
incomplete data: orchestration must wait for the validated terminal tool calls
before invoking tools. The parser handles fragmented UTF-8, CRLF/LF framing and
partial arguments with bounded frame/event/body sizes. A missing terminal marker,
broken body, invalid tool arguments or incomplete finish reason fails the stream.
After partial output there is no automatic restart. An eight-event bounded
channel propagates backpressure, and receiver cancellation drops the provider
stream. Current permissions and retained source/destination markings are checked
before event release and the final result commit.

Capabilities require successful persisted probes for the exact binding and
resolved endpoint. Probes expire after 24 hours; configuration flags alone do
not enable new calls. Runtime validation remains mandatory after a probe. A
binding change invalidates its previous capability evidence. Capability inspection and successful results expose an opaque `modelBindingFingerprint` of the complete binding plus resolved endpoint; downstream generations pin and compare that fingerprint without receiving credentials or endpoint configuration. When stream and tools or structured output are requested, additional `streamTools` and `streamStructured` probes validate those combinations. Independent flag probes do not enable a combination.

The process bounds active inference to eight and HTTP admission to 32. Trusted
resource classes additionally bound aggregate active calls across replicas and
share the same locked class row/count with execution tasks, publication owners
and external delivery. Model calls retain admission until completion or their
database-clock deadline, including uncertain attempts. Provider compute/cost can
outlive a disconnected client; provider-side budgets remain required.

Call keys are scoped by environment/application/principal/resource. Equivalent
requests replay a retained result after current authorization; a conflicting
request fails and a lost/unfinished call is never blindly re-executed. Every
upstream attempt is durable. Only a connection failure before a response is
retried once; an uncertain submission or a broken response is not retried.
Trusted `timeoutSeconds` sets the total database-clock call deadline from 1 to 120 seconds, defaulting to 120. A missing COMMIT acknowledgment is reconciled against the durable receipt within a bounded cleanup interval; success is never overwritten and inference is never resubmitted. No prompt content is written into
attempt traces; retained results remain protected by the original labels and
current authority.

The generic internal execution record is not a public agent-run API. Agent
orchestration, authorized trace inspection, retention, deployment composition,
fleet budgets/telemetry and full R04/R05 integration remain separate program
gates. Actual local inference evidence is retained under `docs/benchmarks` and
does not certify inference quality, the object workload ladder or HA.

## Derived response custody

A model call accepted using a native private job credential persists the actual
DB-derived execution ID. Request JSON cannot select, replace or clear that origin.
Agent-derived responses recheck the originating actor, application, retained
source release, object/file/failed-action evidence, nested inputs and unresolved
observations in the same protected transaction as receipt release. Public replay,
inspection and streaming replay do not bypass this gate. Current source markings
join the response markings and model destination-policy check. An actual live
same-execution private callback may finish its own pending observation; a public
credential cannot use that exception.

Search projection embeddings remain inside their native full-object purpose.
A retained embedding receipt requires a current private Search execution bound to
the same projector and collection, current service generations, and current read
authority for the source Object and Dataset. Ordinary public Model endpoints cannot
inspect those private receipts. Deterministic embedding reuse still checks the
current full-object grant before and after the call. Lost completion acknowledgments
reconcile only after the same current source and destination checks.
