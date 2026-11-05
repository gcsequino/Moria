# SQL publication, provenance and authentication contracts

The full baseline reproduced seven failures (41/48 passing). The corrected full
file passes all 48 cases, with zero skipped or cancelled, in 46.67 seconds.
Production provenance and publication guards remain enabled.

SNOTEL imports external NRCS responses; its old lineage keys `source` and
`request` held audit strings rather than Dataset inputs. Snapshot lineage now
truthfully contains no Dataset edges for this direct external import. The actual
request ID, configuration, staged raw/data pins and result remain in durable
`snow_syncs`; the status endpoint still returns the completed ID/configuration.
The real integration test verifies those audit records and all three published
heads. The internal workload ingestion session admits only the two fixed
source-to-object index pairs through Catalog.admitDataProvenance, before capturing
the publication security epoch. No direct dependency SQL or trigger bypass is used.
Existing initiating-actor, permission and atomic publication checks remain.

The legacy Iceberg indexer now performs the same early current permission/session
checks as distributed registration. Invalid sessions and missing permissions fail
with typed errors before PostgreSQL classification calls; publication still
rechecks authority and preserves the previous head on revocation.

Fixtures now assert all three initially created heads remain unchanged after the
stale-output rollback. The lineage metadata fixture admits its genuine dependency
using a verified workload session and uses a Dataset name rather than an arbitrary
audit key. Independent browser applications use distinct origins; the OIDC fixture
also closes its issuer if initialization fails. The shared permanent origin
reservation and credential revocation behavior are unchanged.

Frozen base: `adb825f`, plus the separately qualified cancellation fixture. All
112 native source inputs match the qualified cohort. All nine artifact provenance
checks and 79 native units pass; full static checks pass on the final six-file
patch. Exact file hashes and image identity are retained here.

The intermediate retry reached 46 passing and two failing case events, then
remained open because failed OIDC initialization leaked its issuer. Its owned
controller was explicitly stopped; it is retained as a failed interrupted retry,
not a completed aggregate result. The final full file exits normally. Controller,
scratch pools and temporary paths were verified removed for every trial.

These are functional compatibility results. Complete independent quality checks
remain required on the integrated revision; no production capacity is certified.
