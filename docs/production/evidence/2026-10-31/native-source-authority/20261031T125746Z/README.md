# Source authority history payload qualification

Full TypeScript checking and 18/18 actual PostgreSQL Source tests pass with zero skips (45.31 seconds) against the immutable source identified in `manifest.json` and `source-manifest.json`. No native executable was rebuilt in this proof.

The recursive release work table carries only `(entry_id, digest)`. Exact immutable snapshot/marking payloads are joined where consumed. Current file permissions, retained historical classifications, dependency closure and exact pinned declaration-file liveness checks remain in force. This avoids recursively deduplicating and materializing whole Source snapshots; it adds no authority cache and makes no claim that total history traversal is constant-time.

A new real publisher test activates nine distinct releases for the same resource, then changes the contributing file classification and principal clearance. The live SQL authority reflects the new classification immediately and denies the revoked clearance. Existing cases retain future restrictions from old sources, distinguish fresh files from deleted pinned files, enforce independent filesystem browsing scopes, and preserve dependency/ownership safety.

The updated SQL baseline and standalone Model HTTP Secret status mapping still require the next compiled native integration gate. Production Source-aware history depth and target load remain separate qualification.
