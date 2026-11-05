# Source and revocation fixture reconciliation

The accepted Source contract requires an active published resource binding for
domain reads. Three positive fixtures previously inserted unbound filesystem
resource entries, so they failed before reaching their classification assertions.
The shared helper now uses the actual home/project, file write, Source register,
publish, plan and apply APIs. It never inserts binding rows directly. Catalog
metadata remains exercised against real local storage and MinIO.

Principal suspension permanently revokes issued sessions. Content and native
filesystem fixtures now assert that the old token remains invalid after reenable,
then issue a new token for later independent assertions. A directly queued
FilesystemService call observes FORBIDDEN while the identity is disabled; after
reenable the same old credential observes UNAUTHENTICATED. The queued mutation
still makes no namespace changes. Cleanup restores the fixture actor even if that
assertion fails. HTTP identity checks retain the accepted 401 response. The vector
fixture keeps its pinned-version and exact cosine assertions while using
CREATE EXTENSION IF NOT EXISTS for the quality image's preinstalled extension.

## Verification

Frozen base: `a6fb68ffc503f0c16e0ec0f7c86c65966d3441c2`. No production code changed.
Nine native executables were compiled together from this exact runtime source
(14.26 seconds, two compiler jobs, CPU limit 2 and memory limit 4 GiB).
`pnpm check:indexing` verified their provenance and passed 79 native units, with
zero ignored. `pnpm check:static` passed on the final formatted fixture image.

Each integration file ran separately through the standard quality controller,
with genuine PostgreSQL and its configured OCI scratch pools. Global integration
file concurrency stayed at two or fewer. Original operation deadlines, assertions
and fixture widths remain unchanged.

| File                        | Passed | Controller seconds |
| --------------------------- | -----: | -----------------: |
| catalog-storage             |     31 |              13.93 |
| catalog-storage-publication |     42 |              43.28 |
| filesystem-security         |     10 |               5.88 |
| filesystem-content          |     17 |              19.51 |
| native-vector-engine        |      1 |               4.94 |
| native-filesystem           |     26 |             111.15 |

All 127 tests passed with zero failures, skips or cancellations. Every controller,
scratch pool and temporary directory was verified removed after its file. Images
share identical runtime source and native executables. The first image predates
the queued fixture correction; the second contains it, and the final image differs
from the second only by Prettier whitespace in that same test. The final
filesystem-security file was rerun successfully. Hashes and exact image identities
are recorded in `source-files.json`, `native-artifacts.json` and `summary.json`.

The initial filesystem-security trial failed its imported UNAUTHENTICATED
expectation while the principal was disabled (actual FORBIDDEN), leaving a later
case without its reenabled actor. Its transcript is retained under
`red-filesystem-security`; the Source-based inherited-marking case already passed.
The final static gate initially caught only a formatting error in the cleanup
block; that failed output is also retained. No failed test was disabled.

This is focused functional and compatibility evidence. The complete independent
quality gate is still required for the integrated revision. These results do not
certify production capacity or complete the remaining distributed runtime work.
