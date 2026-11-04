# Operational PostgreSQL history retention

The native operator command is `history-retention approve|disable|plan|once|inspect`.
No policy exists by default. Approval binds one environment, the current installation
incarnation, a retention interval of at least one day, and at least two retained
source generations. An ordinary serving database role cannot invoke the private
maintenance functions. Reusing an approval key with different inputs fails;
disabling a policy cannot revive its old approval.

A plan records the first observation of a candidate revision boundary and requires
another full day before collection. Every bounded step rechecks the current policy,
incarnation and pins under the environment publication admission lock. The floor
can advance only to a complete transaction boundary, never past a live operational
cursor, retained read observation, source build, projection build, search generation
or change replay pin. A new older pin blocks an unpublished plan. Published floors
never rewind; unavailable historical reads return `HISTORY_EXPIRED`, never an empty
successful page or an implicit restart.

Read observations have normalized indexed execution/revision pins. Actual in-flight
executions retain them indefinitely; a terminal execution retains them until its
actual finish time plus the existing thirty-day result horizon. The same transaction
that records or inherits private observations writes their pins. Cursor pins retain
the original cursor expiry. Pin collection does not remove the private audit ledger.

Each step visits at most one thousand candidate data rows. Overlay and action-managed
link history keep the last value at or before the floor plus newer versions. Source
payload collection preserves the configured latest generations, the generation at
the floor, newer generations, shared payloads and active search roots. Generation
headers, identity domains, UID reservations, object homes, creations, receipts and
other audit metadata remain. Active projection intervals keep their current rows;
obsolete closed intervals use a dedicated partial index. An obsolete retired table
is dropped only when it is no longer retained by those generation/search rules.
That DDL additionally requires exclusive recovery admission, so an exported backup
snapshot cannot lose its table before PostgreSQL dump acquires its relation locks.
Complete change frames below the floor are reclaimed in bounded steps; consumers
cannot newly pin or read a position below the floor.

All effects are PostgreSQL transactions with a five-second statement deadline and
250 ms lock timeout. Concurrent operators serialize at the environment and collection
rows; a busy environment returns without deleting data. A killed session rolls back
its current step, while committed progress is durable. Disabling a policy stops
subsequent steps; it cannot restore already collected history.

This controller does not remove filesystem versions, external object versions or
Dataset/Iceberg snapshots. Those have separate exact-version custody and recovery
contracts. Snapshot captures retain a consistent PostgreSQL image; a ready independent
backup does not require live PostgreSQL payload rows to remain forever.

The first immutable functional checkpoint passes actual native projection interval
cleanup, unchanged protected HTTP reads, two-generation payload retention, immutable
identity/header preservation, cursor/read pin bookkeeping, transaction pagination and
fresh-policy grace denial. Positive aging cases use explicitly synthetic historical
clock fields, not a claim of an elapsed day. Evidence is under
`../evidence/2026-10-31/native-history/`; retired-table DDL, all concurrency/backup
races, the latest combined revision and production-scale maintenance remain pending.
