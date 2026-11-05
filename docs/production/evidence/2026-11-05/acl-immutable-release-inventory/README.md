# Immutable Source release inventory

The filesystem ACL query now reads stored generated root/file/dependency IDs
from each immutable Source snapshot. PostgreSQL derives these values from the
snapshot on insertion; a caller cannot substitute an inventory that omits a
contributing file. Live policy, provenance, classification, dependency and
resource-file-proof checks remain in the current statement. This stores immutable
metadata, not authorization decisions.

A disposable-database first-16 producer experiment on the preceding qualified
native build completed all 16 candidates in 94.838 seconds, with 18.044 seconds
across 5,228 ACL calls. The custom-plan-only experiment used 32.806 seconds for
the same ACL call count. Both retain all 67 configured inputs and the original
120-second producer deadline. These isolated measurements do not qualify the
full Pipeline or evaluation gates or certify production capacity.

The actual generated-column SQL, query substitutions and override-denial check
pass full static checks, nine exact native artifacts, 109 native unit tests and
the full 21-case filesystem Source security suite, with zero skips/cancellations.
The original two-job compiler attempt was intentionally stopped after observed
memory thrashing at 3.98 GiB of its 4 GiB limit; the unchanged source passed with one
compiler job under the same limit. This is not recorded as a compiler code error
or an inferred out-of-memory kill. Both trial evidence sets are retained.

The combined v3/discovery/staging build, full Pipeline/evaluation suites and real
backup/restore remain required. No live installation migration is claimed here.
