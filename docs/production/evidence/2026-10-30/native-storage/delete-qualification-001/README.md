# Conditional delete qualification failure

The pinned isolated MinIO server accepted `DELETE` with a deliberately wrong `If-Match` header, returned 204, and removed the object. The SDK build middleware confirmed that the conditional header was present. The assertion failed; this evidence is not a passing native storage test.

Command: `docker run --rm --cpus 2 --memory 2g -v /tmp/moria-reconcile-20261029:/work -v /tmp/moria-reconcile-20261029/node_modules:/work/node_modules:ro -w /work moria-test:native-prerequisites /app/node_modules/.bin/tsx .local/s3-delete-probe.ts`

The probe creates only its private synthetic object and closes the isolated server. The fixture verifies the MinIO executable checksum before starting it. No Moria runtime binary or catalog authority was exercised. `result.json` records the fixture and lockfile hashes; the script and raw failed output are retained.

ETag-only conditional deletion must remain disabled for this provider. A future S3 collector needs exact immutable provider version IDs with qualified versioning semantics, or a provider that passes actual conditional-delete tests. A distinct removal authority is required; ordinary immutable-output write grants cannot delete retained data.
