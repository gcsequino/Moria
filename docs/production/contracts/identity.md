# Identity and domain ownership

Native implementation: `IdentityRegistry` in `services/runtime/src/identity.rs`.
The installation-wide PostgreSQL registry retains permanent canonical mappings
and UID reservations. Cells route allocation to the current domain owner; they do
not create independent registries. This implements one logical authority, not a
claim of independent database-cell availability or an automatic restore protocol.

Trusted operator composition registers a cell UUID with an external endpoint
reference and registers a domain's initial owner. No credentials or physical
endpoint is accepted from a workload request. Owner metadata is
`{cell:UUID,fence:decimal-string,incarnation:UUID}`; gRPC uses an unsigned fence
field constrained to the PostgreSQL positive bigint range.

`resolve(domain, canonicalKeys, owner)` admits at most 1,000 keys, 64 KiB per key
and 1 MiB total key bytes, subject also to encoded frame limits. Results preserve
input order and duplicates. A logical unique `(domain,canonical_key)` returns one stable UID. PostgreSQL indexes `(domain,SHA256(canonical_key))` while retaining the complete canonical key outside the B-tree tuple; every conflict checks exact key equality and a digest collision fails closed. Domain/key comparisons use the C collation. A separate unique UID constraint prevents cross-domain reuse. UID collisions
retry at most five times inside the authority. No operation deletes or reassigns
canonical mappings; a database trigger rejects update or deletion of permanent reservations. Domain ownership is locked through allocation commit.

`transfer(domain, expectedOwner, nextCell)` compares the exact owner/fence and
current incarnation, increments the fence and writes an ownership event in the
same transaction. It waits for already-admitted allocations. Later old-owner
calls fail. A lost transfer response is resolved with `inspect(domain)`; an old
expected fence never causes another move. Transfer does not copy identity rows.

Internal gRPC requires mutual TLS and a current registered peer. Resolve verifies
the peer's cell against the domain owner. Transfer requires `identity-admin`.
Service-peer authorization is locked and checked inside the mutation transaction;
the receiver also checks current peer validity before releasing its response.
The CLI is trusted operator tooling using the scoped runtime database role.

Legacy TypeScript allocation goes through `runtime_identity_resolve_legacy`.
Once a domain is registered as managed it fails `OWNERSHIP_REQUIRED`; it cannot
bypass a native ownership transfer. Its shared domain admission lock orders
against initial registration. Unmanaged legacy test/application domains retain
their original canonical-allocation behavior.

`tests/native-identity.integration.test.ts` exercises concurrent native processes,
full 64 KiB canonical keys, the real ownership SQL barrier, stale owners, both allocation/transfer lost COMMIT
acknowledgments and incarnation/disabled-owner denial. Multi-host routing, domain
transfer across physical database cells and a complete restore remain separate
P04/P05/P06 gates.
