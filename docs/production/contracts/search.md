# Protected retrieval and search projection v1

Native N12/N13 handlers have functional component evidence; the full R05 recovery,
scale and whole-instance acceptance gates remain open. R05 remains authoritative. `SearchCollection` is trusted immutable release
state: environment, resource/revision, object resource, selected UTF-8 string
property, embedding Model resource, source markings and chunker revision. Requests
cannot supply policy SQL, provider endpoints, credentials or a model revision that
mixes vectors into an existing generation. Restricted views remain terminal.

## Public operation boundary

`SearchExact(collection, text, topK, minimumToken?)` authenticates a verified session,
checks collection and source field access, embeds the query through NativeModels,
and returns at most 50 chunks ordered by exact pgvector cosine distance then stable
chunk ID. The query vector must have the generation's exact dimension and contain
finite float32 components with at least one nonzero component. No ANN or hybrid
ranking is enabled. Per-call text/response/deadline/admission limits are explicit.

A hit contains object UID, chunk ID, text, code-point start/end offsets, original
source revision, chunker/model revision, generation, score and complete visibility
token. Source/row/property policy from the indexed snapshot and current policy,
current source membership, effective tombstone and field access are applied in SQL
before distance ordering and LIMIT. The source binding/schema must still match.
Hidden or currently deleted documents cannot consume topK slots. A current policy
error fails closed. Selected text never comes from an unprotected side lookup.

The shared native listener exposes `POST /api/v1/search/{collection}/query`,
`/visibility`, `/rebuild` and `/status`. Query and rebuild require an
`idempotency-key`; visibility and rebuild accept only `{}`, while status accepts
only `{"execution":"UUID"}`. Rebuild/status require current collection project
permission. Status returns protected generation/execution state and its visibility
receipt; it does not return raw source rows, credentials or provider configuration.

`RequestRebuild(collection, requestKey)` pins the trusted collection/model definition,
exact dimension, chunker revision and source generation. `InspectVisibility` reports
active generation plus the complete covered commit boundary; it does not return
protected document counts. `minimumToken` waits only within a bounded deadline;
unknown incarnation, incompatible generation and an unavailable visibility boundary
produce explicit errors rather than stale success. Model changes require a new
staged generation. A previous complete generation remains active until replacement.

A collection's operator-approved projector binding names a registered service,
exact collection/target purpose, object field, Dataset origin and model destination.
The initiating caller does not become the full-data reader. Metadata acceptance
resolves the actual source stage's Dataset revision/snapshot and admits only those
declared data dependencies before pinning the fresh authority epoch. No ordinary
service login credential is minted. Storage and full-object reads require private
credentials from the actual registered task or publication lease; source-purpose,
lease, service, classification and destination revocation remain live.

Base scans use disjoint UID ranges and server-side row/byte prefixes. Embedding
requests batch up to 64 complete chunks across objects. Stable full-batch keys bind
model, chunk format, exact text and classification even when a cache is partly
warm. Committed cache bytes are immutable and reused across replacement builds.
The model invocation retains the sealed full-source grant, checks it during the
upstream call and in the model receipt transaction, and closes upstream work when
the projector purpose is revoked.

## Chunk and embedding identity

`unicode1000-overlap100-paragraph-v1` counts Unicode scalar values, never UTF-8 bytes
or UTF-16 units. A chunk contains at most 1,000 code points. Before the hard limit,
prefer the last double-newline paragraph boundary that leaves more than the
100-point overlap; include that delimiter. The next chunk starts 100 points before
the previous end. Empty documents produce no chunks; whitespace is preserved and
there is no text normalization. Offsets are half-open in the original document.
Every step advances, including long paragraphs and consecutive delimiters.

Chunk IDs bind UID, source revision/content digest, chunker revision, ordinal and
exact offsets. Persisted embedding cache identity binds complete chunk text digest,
provider/model revision, dimension, input-format revision and exact conservative source-marking set. Different classifications use separate cache entries; a shared cache hit cannot silently upgrade or discard a document’s labels. Exact retained text
or digest conflict is checked; a digest collision fails closed. A crashed projector
reuses committed embedding bytes and never silently invokes a different model.
Cache bytes and their original labels are immutable. Ranking additionally checks retained embedding labels before LIMIT. Cache retention is protected data with the original conservative markings; it does
not cache permission. Returned vectors are checked independently of capability
probes. Provider token truncation is not an accepted substitute for complete chunk
embedding: the provider profile must support the entire accepted UTF-8 chunk range.

## Snapshot, log and publication

A rebuild captures a source pin and revision R under the authority admission barrier
and acquires a durable change-log pin before scanning. It pages immutable source
rows plus the exact latest overlay at R off-lock. Tasks carry generation,
owner/fence, immutable input pin and bounded partition ranges. Output rows and
embedding results are staged/sealed; a task acknowledgment cannot activate them.
Workers renew leases and recheck current scoped model/storage authority around IO.

After the base scan, replay every committed transaction after R in complete-frame
order. Deduplicate affected UIDs within a frame and prepare their new chunk/embedding
references off-lock. A source-generation invalidation starts the corresponding
complete replacement build; it is never mistaken for one object update. Final
publication checks the current incarnation, owner/fence, collection/model revisions,
source pin and all task seals. The short authority barrier catches any final tail
and changes the active generation/reference set and covered revision atomically.
If the tail exceeds its budget, release the barrier and catch up again off-lock.

Incremental updates prepare a complete change frame before one transaction switches
all affected document references and advances the watermark. A reader sees the old
complete frame or the new complete frame. Failed/cancelled work cannot advance
visibility; stale workers cannot publish after takeover. The covered environment
revision may pass revisions without object changes only after capturing a committed
authority boundary and proving all preceding object frames complete. Numeric
revision gaps alone never imply missing data or permission to skip a frame.

Completed rebuild credentials are never reopened for later updates. Native
background planning admits a fresh registered-service execution for each captured
replay boundary, with its own real task/publication leases and accepted Dataset
pin. Each complete frame prepares references off-lock, switches all affected heads
and its watermark atomically, and only then advances retention. A separate durable
active-generation log pin survives the short execution's completion. Replacement
activation transfers that protection and retires the old generation in one commit.

Pins protect source/overlay generations, pending frame inputs and embedding bytes
until publication or cancellation. GC must prove that no active generation,
in-flight task, cursor, replay pin or recovery set references an object before
removing it. Restore rotates incarnation, keeps model/delivery dispatch disabled,
and requires recovery reconciliation before new publication.

## Required evidence

Use actual PostgreSQL with pinned pgvector and real NativeModels HTTP boundaries.
Verify Unicode/paragraph offsets, exact full chunk union, stable IDs, zero/dimension
rejection, before-LIMIT policy and deletion, deterministic tied ranking, current
revocation, complete multi-object transaction visibility, snapshot/log races,
persisted embedding reuse, source-generation/model rebuild, stale fence denial,
process loss before/after commit and minimum-token behavior. Include actual local
inference plus a complete native instance. Small fixtures do not certify the
1m/10m/50m workload ladder or HA.
