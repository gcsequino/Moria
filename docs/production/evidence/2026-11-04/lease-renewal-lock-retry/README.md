# Lease renewal after a server lock timeout

A service Evaluation failed its root renewal on PostgreSQL `55P03` while its actual lease still had about fourteen seconds remaining. Read-only sampling mapped the waiting publication-lease statement to the root UUID through the locked execution tuple. The blocking transaction was actively evaluating current policy. Child failures followed the root failure. The five-second database lock timeout and thirty-second lease remained unchanged.

`ExecutionAuthority::renew` now makes at most four attempts for an actual PostgreSQL `55P03` server abort, with 10/20/40 ms backoff. Each attempt opens a fresh transaction and executes the complete existing incarnation, current lease, owner, fence and authority validation before updating the lease. Transport errors, uncertain commits, expiry and revocation errors are not retried. Every successful renewal remains capped at the original execution deadline.

## Qualification

Both paired snapshots passed all nine native artifacts, full static checks, native source provenance and 109 native unit tests with zero ignored tests. The complete source trees differ only in `services/runtime/src/execution.rs`; the TypeScript regression fixtures are identical.

The original authority cases plus six new real PostgreSQL boundaries ran with the original 120-second root watchdog. The passive PostgreSQL transport observer forwards every response and records only actual SQLSTATEs; it never holds a response.

- Baseline: 11 pass, 7 fail including the parent; all six new boundaries fail after the first `RESOURCE_BUSY`, 50.864 seconds, zero skips/cancellations.
- Fixed: 18/18 pass, 66.427 seconds, zero skips/cancellations.
- Task and publication leases each recover after a six-second shared-row lock and exactly one server-aborted renewal.
- Persistent contention returns after exactly four five-second server lock timeouts, 20.557 seconds, without extending the lease.
- Revocation, expiry and fence changes introduced after the first timeout each reject the fresh attempt without extending the lease.
- Existing current-authority, missed-disable-pulse, worker-generation, takeover and private credential negatives remain unchanged and pass.

Owned controllers, all three scratch pools and temporary directories were verified absent after both runs. Full original service Evaluation qualification is pending; this boundary result does not certify platform completion or production capacity. Earlier resource-accounting failures remain intermittent; two later diagnostic runs did not reproduce them and no accounting or completion-handshake guard was changed.

Raw proof archives and source inventories remain under `/home/gcseq/moria-proofs/renewal-lock-retry-native-20261105` and `/home/gcseq/moria-proofs/renewal-lock-retry-baseline-20261105`. The compact manifest records their hashes and exact native digests.
