# S3 streaming storage adapter

Issue #44 (M2-01e), bounded prerequisite slice of #17 after #42. Baseline
`863a50896fddf4a34935ee404950f4bdcb077e42`. Follow C04/C08, S01–S03/S06–S08
and production P01/P02/P06. Iceberg still owns the dataset table format;
restricted views remain terminal. This is an object adapter, not catalog or
runtime integration. Parent #17 remains open.

## Trusted configuration and contract

`createS3Storage` and `createAuthorizedS3Storage` are trusted service composition
entry points. Both return only `StreamingStorage`: logical version-1 locations,
Web byte streams, exact sizes, ranges, cancellation, and capability discovery.
SDK commands, credentials, endpoints, buckets, physical keys and multipart IDs
stay inside the adapter/configuration boundary. Consumers never request multipart.

The service supplies an exact warehouse/table binding, an existing bucket,
prefix, region, endpoint, explicit credentials, maximum object bytes and chunk
bytes. Configuration and credential values are copied. There is no SDK default
credential chain, credential-file access, environment credential discovery,
redirect following or automatic region discovery. Trusted composition must obtain
credentials from an external store or environment and replace handles on rotation.
No credential values, SDK errors or physical addresses escape through errors.

Endpoints require HTTPS except literal loopback HTTP for isolated local testing.
Userinfo, non-root paths, queries and fragments are rejected. Buckets use the
conservative DNS subset of lowercase letters, digits and hyphens (3–63 chars).
Prefixes are empty or slash-separated logical identity tokens, at most 512 chars;
empty components, dots, percent encodings and path traversal are rejected.
The private key mapping is
`<prefix>/<warehouseId>/<tableId>/objects/<objectId>`. Locations cannot carry
provider fields or choose another endpoint, bucket, prefix, warehouse or table.
Host DNS, TLS, service configuration, provider administrators and other credential
holders are trusted. This does not isolate hostile code sharing a service process.

The shared verifier in `authority.ts` preserves exact object/operation scope,
immutable request snapshots, literal-true decisions, exception sanitization and
abortable current-authority checks for both providers. The S3 adapter checks before
HEAD/source acquisition, before every range GET, after GET before releasing bytes,
before and after every source read including EOF, before part upload and immediately
before final completion. No policy cache or speculative read queue is introduced.
As with local storage, this is not an atomic provider authorization fence: policy
can change after a check. The service must recheck current authorization and worker
fencing at the existing catalog publication boundary.

Reads use HEAD followed by demand-driven bounded range GETs with the HEAD ETag as
`If-Match`. Each response must have the exact requested content length, content
range and ETag; malformed, truncated or changed responses fail closed. Zero-length
ranges, including EOF, do not issue GET. Bytes beyond EOF are rejected. One output
buffer of at most `chunkBytes` is retained per pull, plus bounded HTTP/SDK transport
buffers. Cancelling destroys the active request/body; idle streams retain no socket.

Writes require exact size and chunks no larger than `chunkBytes` (1–1,048,576).
A single reusable 5 MiB part buffer accumulates source chunks; uploads are sequential
and the next source chunk is not requested while a full part awaits acknowledgment.
The buffer is smaller for objects below 5 MiB. At most one source chunk, one part
buffer, bounded SDK/transport working memory and 10,000 part descriptors are retained.
The maximum configurable object size is 50,000 MiB. These are per-transfer bounds;
caller queues and concurrent transfers need separate service budgets. Empty chunks
yield so abort timers can run. Empty objects use a zero-byte multipart part.

Every write explicitly initiates multipart, uploads parts, verifies exact EOF and
completes with `If-None-Match: *`. No HEAD-before-write race or overwrite fallback
exists. HTTP 412 completion becomes `ALREADY_EXISTS`; the existing object is never
deleted. Unknown capabilities, including overwrite/delete/publish/multipart, fail
with `UNSUPPORTED_CAPABILITY`. Other failures use the existing sanitized error codes.

Completion has no automatic retries. A failed completion exchange other than a
definite 412 returns `writeOutcome: 'uncertain'`: the provider may have committed.
Cancellation during that exchange also remains uncertain. A retry must be create-only;
object reconciliation/publication belongs to the service. Abort is attempted on every
known incomplete upload with an independent ten-second cleanup deadline, even if the
caller cancelled. Abort never deletes a completed object. A crash, lost initiation
acknowledgment (unknown upload ID), or failed cleanup can leave orphaned uploads.
Service-owned lifecycle/reconciliation is a remaining gate; no cleanup guarantee
is claimed during provider outage. Requests have connection/request timeouts and
caller abort support. Sources and authority checkers still require service deadlines.

Only providers independently proven to honor conditional multipart completion are
supported. S3 compatibility branding alone is insufficient; there is no safe way to
infer this guarantee from a successful ordinary upload. The local test target below
is qualified by actual concurrent requests. Another provider/configuration needs the
same qualification before trusted composition can use it. No paid cloud service was
used. See AWS's [conditional-write contract](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
and [official JavaScript v3 examples](https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/javascript_s3_code_examples.html).

## Pinned offline test prerequisite

The official AWS JavaScript v3 S3 client is pinned to `3.1156.0` in package.json and
the lockfile. The isolated test server is MinIO
`RELEASE.2025-09-07T16-13-09Z`, commit
`07c3a429bfed433e49018cb0f78a52145d4bedeb`, Go 1.24.6, Linux amd64,
GNU AGPLv3. Upstream [license](https://github.com/minio/minio/blob/RELEASE.2025-09-07T16-13-09Z/LICENSE)
and [release](https://github.com/minio/minio/releases/tag/RELEASE.2025-09-07T16-13-09Z)
retain their attribution. This version is only a loopback synthetic conformance
prerequisite, not a production server recommendation.

Exact binary package URL:

```text
https://github.com/minio/minio/releases/download/RELEASE.2025-09-07T16-13-09Z/minio_20250907161309.0.0_amd64.deb
```

Package SHA256 (checked against official release asset metadata and downloaded bytes):

```text
eeda08f699f6592d1b868ac8bda864ae2cacdb5ee1b888663366e8c8ff566249
```

Extracted `usr/local/bin/minio` SHA256 (also verified by every test fixture startup):

```text
7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f
```

`deploy/test/Dockerfile` downloads and verifies the package during image build,
extracts the binary, and installs it at `/usr/local/bin/minio` without running
package service scripts. This pin explicitly requires Linux amd64. The retired
`dl.min.io` archive/checksum URLs returned HTTP 410; provisioning uses the official
GitHub release asset instead. No download occurs during tests or `pnpm check`.

For a preprovisioned non-root checkout, extract the same checksum-verified package
outside the repository and set `MORIA_TEST_S3_BINARY` to its `usr/local/bin/minio`.
Missing binaries and checksum mismatches fail tests; there are no skips or mock
substitutions. The fixture invokes exactly:

```text
<verified-minio> server <fresh-temporary-directory> --address 127.0.0.1:<reserved-port>
```

Each server receives freshly randomized synthetic `MINIO_ROOT_USER` and
`MINIO_ROOT_PASSWORD` environment values and `MINIO_BROWSER=off`. Output is discarded,
credentials are never persisted as fixtures or logged, and no live instance is opened.
An authenticated bucket operation confirms readiness. The child is terminated and
its isolated temporary directory removed at teardown. No system service is started.

## Evidence and remaining gates

The conditional-finalization prerequisite was executed successfully **before**
implementing the adapter: two real concurrent completions produced one success and
one 412, a later completion also returned 412, winner bytes were preserved, and
aborting losers left no pending multipart uploads. Keep this proof mandatory in
`tests/storage-s3.test.ts`; normal `tests/*.test.ts` discovery includes it.

`tests/storage-contract.test.ts` runs identical behavior checks against actual local
files and isolated MinIO: logical bindings, sanitized errors, capabilities, exact
sizes, ranges, empty objects, bounded chunks, cancellation, create-only concurrency,
current authority and uncertain finalization. Local uncertainty uses the existing
real filesystem durability fault fixture. S3 uncertainty forwards a real completion
then drops its acknowledgment; complete bytes remain and a retry cannot overwrite.
Provider tests hold a real 5 MiB upload acknowledgment and verify only five 1 MiB
source chunks were requested, then abort and check the server's multipart inventory.
A successful 12 MiB write uses 5/5/2 MiB parts. Other tests verify endpoint/prefix
rejection, invalid credentials, copied configuration, post-read authority denial,
pre-completion denial, no speculative GET and malformed response rejection.

Adapter substitution changes trusted service configuration. Existing bytes, absolute
locations and Iceberg metadata references are **not** automatically relocated or
rewritten. Runtime consumers, engine/catalog integration, data migration, retention,
recovery, credential lifecycle, remote TLS and production S3 conformance remain
separate #17 gates. The whole platform is not yet provider-independent.

The one-task protected-file exception changes only the pinned AWS dependency/lock
entries and isolated test-server provisioning. No scripts, CI, delivery policy,
authorization rules, runtime consumers, catalog or lease code change. Human review
of the shared authorization invariant and latest-revision independent Linux CI
remain required. Changes stay uncommitted for controller-owned publication.

Verification on October 25, 2026 uses non-root Linux, Node 24.21.0, pnpm 11.25.0
and `.venv` Python 3.11.17. The initial full `pnpm check` passed 344 tests, zero
failures/skips, in 104.086 seconds of runtime tests, including real PostgreSQL and
Iceberg. Final review made all SDK profile-derived defaults explicit and added a
subprocess regression that intercepts and rejects ambient config/credential-file
reads before access; real signed write/read completes with zero attempted reads.
The focused S3 suite then passed all five tests. Required formatting and typechecks
passed. Final full-gate results are recorded in `docs/IMPLEMENTATION.md`.

Docker image execution/build remains unverified because the Docker daemon socket
is unavailable. The downloaded package checksum, extracted binary checksum,
version/license and executable invocation were verified directly in this workspace;
the same binary ran all local S3 tests. No real S3 runtime test was skipped.

Final `MORIA_TEST_S3_BINARY=/tmp/issue-44-minio-dist/usr/local/bin/minio pnpm check`
passed with exit 0: 345 tests passed, zero failures/cancellations/skips, runtime
test duration 104.438 seconds. All remaining gate stages passed. Local evidence:
`/tmp/issue-44-check-final.log`, `/tmp/issue-44-format.log` and
`/tmp/issue-44-final-format.log`. `git diff --check` passed. These are temporary
verification logs, not durable benchmark evidence or a production capacity claim.
