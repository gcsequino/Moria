# Object change replay v1

The native internal `ChangeLog` API captures inserted object overlay versions
and complete source generation activation in the originating SQL transaction.
There is no public subscription endpoint. Deployment grants this operator/service
API only to trusted replay workers; caller-supplied environment names do not
authorize public access.

Positions contain installation incarnation, environment and an actual committed
environment revision. Each returned frame contains ordered ordinals, exact
version references and a transaction-end marker. A deferred authority trigger
seals the frame at the final environment revision of its database transaction.
Several action/source revisions in one database transaction therefore cannot
be exposed as separately complete transactions. PostgreSQL visibility excludes
uncommitted frames and rollback removes both effects and intent. Revisions use
the existing transactional environment order; no timestamp or sequence infers
missing work. Setting deferred constraints immediate before further writes is
rejected by the sealed-frame guard.

Object events identify object type, UID and exact overlay revision, including
tombstones/restores. Consumers resolve that historical version and source map,
never whatever is latest. Source events explicitly require a snapshot rebuild
at the named generation revision; they do not pretend that a generation switch
contains a complete row-level delta. Generation stage UUIDs are included where
available; legacy source generations resolve by environment/type/revision.

Read batches contain at most 100 whole transactions and 10,000 events. A frame
that does not fit the caller's event budget fails explicitly; it is never split.
Replay is at least once. Consumers must publish their output and durable
checkpoint atomically or use immutable idempotent destination versions before
advancing their checkpoint. A checkpoint names an actual complete frame, or the
initial zero origin. Nonexistent intermediate revisions fail closed.

Replay pins use caller-stable UUIDs, trusted worker owners, exact incarnation,
monotonic progress and database-clock expiry of at most one hour. A renewal
cannot revive an expired pin, rewind progress or transfer ownership. The
retention controller must preserve all log/history dependencies of active pins;
this module does not delete history. Snapshot-plus-log handoff additionally
requires the rebuild admission transaction to pin its exact source map.

Binding-change capture and the retention collector remain separate composition
work. No search visibility or capacity claim follows from a passing replay test.
