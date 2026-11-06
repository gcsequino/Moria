# Bounded output preparation under the publication fence

Dataset IO under an actual publication lease now holds the execution row with
a shared lock. Task IO retains its exclusive execution/task fence, and final
grouped publication still takes the exclusive lock and rechecks every candidate
before the single commit. Ownership, status, fence, lease, current authority and
storage checks are unchanged.

The Pipeline worker prepares at most two outputs concurrently when their
individual spool allowances fit within the existing default preparation budget.
Larger configured preparations stay serial. All original per-output limits,
candidate order and the grouped publication/checkpoint transaction are retained.
The real Catalog producer fixture uses the same bounded preparation for its
valid grouped outputs, retaining the original assertions and fault cases.

The paired live Catalog check first fails with 55P03 on the old exclusive
preparation check. On the exact new build a concurrent shared reader succeeds
while an exclusive reader/writer lock is still denied. The original private
session, wrong-task, exact state and killed unpublished output checks remain.
The full Catalog file passes 21/21, zero skips/cancellations. Static checks,
all nine matching artifacts and 117 native units pass; cleanup was verified.
The first compile rejected a borrowed iterator future in the spawned worker;
moving already-owned output descriptors into the iterator resolved it.

The full original Pipeline timing file and composed local backend proof remain
required. This checkpoint does not certify production capacity.
