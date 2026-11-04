# Versioned provider and storage regression

Actual immutable-source proof passed 29 tests, zero failures and zero skips, in 217.48 seconds. Source snapshot: `/tmp/moria-proof-snapshots/20261030T232847333705Z-native-versioned-storage-v1`; manifest SHA-256: `3028c0a3e5daa6162f1e3e6b72d11fe5419e888b4aa36371865aa74148737062`.

The mandatory runtime/native-storage provenance preflight ran before fixture infrastructure. Executables were resolved from Cargo JSON and copied under the target lock. `selected-artifacts.json` records exact binary hashes; `preflight.txt` records the embedded source/schema digests. The source manifest, generated schema, build output, Cargo artifact output, and raw test result are retained.

Command: `/app/node_modules/.bin/tsx --test --test-concurrency=1 tests/native-storage.integration.test.ts` in `moria-test:native-prerequisites`, with exact copied runtime/indexing/storage-harness paths, two CPUs and 6 GiB memory. The pinned real MinIO and PostgreSQL fixtures were mandatory.

The new case enabled versioning on an isolated bucket, wrote through the native provider, created a newer version through the provider administrator, read the captured old version, denied a mismatched removal capability, deleted only the old version, proved the newer version survived, and proved repeat removal observed absence. Prior POSIX and MinIO bounds, immutable writes, semantic Iceberg closure, same-size corruption, current authorization, namespace disabling, multipart acknowledgement loss, crash/reopen, and ownership takeover controls remained green.

This qualifies the non-null version-ID provider primitive. It does not implement the N18 garbage-collection authority, retention policy, lifecycle reconciliation, or POSIX conditional removal. Unversioned/null-version S3 removal remains unsupported. The earlier ignored-If-Match delete failure is retained separately and was not reclassified as a pass.
