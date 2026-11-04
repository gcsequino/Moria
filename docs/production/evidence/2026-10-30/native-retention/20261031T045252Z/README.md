# Exact-issued recovery retention verification

Result: **7/7 tests passed, zero skips**, 35.242 seconds. Schema verification, TypeScript checks, native harness compilation, and runtime binary compilation passed before the integration fixture. Exact commands, compiler-selected paths, binary digests, source manifest, and embedded schema/source metadata are recorded in `manifest.json`.

The proof uses isolated real PostgreSQL and MinIO, an HTTP hold barrier, the actual native recovery/backup authority, and the actual retention controller. A fresh `RecoveryAuthority.create` lease issues a real qualified backup write; retirement is denied during retention, a terminal producer cannot resume, and its backup namespace cannot be registered as Dataset bytes. Reclamation tests use separately labeled historical closed DATA fixtures, actual non-null S3 versions, and explicit policies. They verify retained-reference denial, exact version deletion, policy revocation after provider HEAD, killed-collector takeover after the actual database-clock lease expires with a higher fence, and no deletion for unqualified versions.

The historical fixture does not establish that a successful capture aged for 24 hours. Its past closed rows are fixture-admin data, not fabricated live producer authority. No active producer lease or controller lease deadline is shortened. The controller creates and maintains every collection claim under test.

This proof covers recovery-issued objects only. General Dataset/snapshot-history expiration and collection remain gated by the complete reference/pin/publication admission inventory in `contracts/retention.md`. POSIX and ETag-only deletion remain unsupported. This is functional/fault evidence, not a production capacity certificate.

Preserved preceding attempts: `../20261031T044717Z` and `../20261031T045149Z`. Those failed on fixture output/version-header assumptions; their outcomes are retained unchanged.
