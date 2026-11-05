# Indexing candidate admission isolation

A revoked accepted build failed its authority check before a planning, task or
coordinator claim. That exception escaped the worker loop, left the rejected work
nonterminal and prevented unrelated valid builds from draining. The three claim
paths now share a savepoint check while retaining their selected-row lock. Only
explicit known P0001 authority/build rejections fail that selected build. Unknown,
transport and schema failures still propagate. The failure update preserves active
and terminal states; it invents no owner/fence and updates only the existing exact
owner/fence audit entry. Task admission performs no bulk task update. The caller
commits the rejection before reporting progress and trying another candidate.

The lifetime-schema-lease fixture also watched an obsolete SQL statement name.
Its captured real PostgreSQL diagnostic shows one idle lifetime connection and
another connection blocked by the fixture in indexing_check. It now identifies
that actual blocker PID, preserving the assertions that migration stays excluded
until checked-out connections drain, admission stops after lease loss and no
source generation is published. Original revocation and previous-head assertions
remain unchanged.

## Verification

Frozen base: `43de5387c051f4246b93dfb1a01dd13246301cf1`. The pre-fix full file
passed 8/12 tests and failed the barrier, revocation and independent-workspace
cases, including their parent wrapper. Failure-only SQL activity diagnostics and
the complete original failure transcript are retained under `red`.

The final full file passed 16/16 with no skips or cancellations in 35.70 seconds.
It retains every original case and adds real plan/task/advance admission cases.
Each queues a revoked accepted build before a valid independent workspace build,
then drains both: the rejected build has a durable AUTHORIZATION_CHANGED reason,
its prior object head stays unchanged and no generation references its stage,
while the independent build becomes active at its expected Source revision and
returns the new title. No lease timestamps,
assertion deadlines, task widths or publication checks were relaxed.

Full static checks passed before building. All nine debug executables were
compiled together from the frozen source, with two compiler jobs, CPU limit 2
and memory limit 4 GiB (76.61 seconds). Their provenance and all 79 native units
passed on the exact integration image. Source hashes, executable hashes, commands
and raw results are retained here. Controller, scratch pools and temporary paths
were verified removed after each file.

These are functional and failure-isolation results. Complete independent quality
checks remain required on the integrated revision; production capacity remains
unverified.
