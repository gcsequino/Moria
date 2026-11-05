# Function Source authority in one credential-bound statement

`Functions::authorize` previously resolved current session authority, called an
operation Source helper that resolved it again, applied pure native permission
checks, then performed a final fresh authority check. The new credential-bound
SQL helper obtains current state once, verifies the exact expected environment,
principal and application before Source lookup, and passes that actual state to
the existing private operation/Source helper. Its native caller receives the
actual state and labels together and retains the original permission checks and
separate fresh `Authority::finish`. The preceding resource-existence check is
unchanged. There is no caller-supplied trusted state, cross-call authority cache,
new HTTP endpoint, isolation change, lock change or deadline extension.

The real PostgreSQL file passes **8/8**, with zero skips/cancellations. PostgreSQL
function counters witness the previous SQL composition resolving the session
three times and the grouped composition twice, including the final fresh finish.
Both ordinary workload and actual issued private job credentials return exactly
the same state and Source labels. The counts measure this isolated authorization
composition; they are not a total Evaluation runtime or capacity result.

Denials cover exact verified identity before Source errors, stale actual task
fences before identity/Source errors, missing immutable proof, deleted resources,
new classifications, a committed policy deny between statements and permanent
session revocation. An ordinary database role can call the credential-bound
helper with a valid credential but cannot execute the private state-based helper.
The incomplete-lineage negative is confined to a rolled-back transaction; real
Source publication remains required for the positive fixture.

The complete original Function file passes **18/18**, zero skips/cancellations,
69.484 seconds in its root and 71.466 seconds including the driver. It includes
actual sandbox/current Source authority, exact invocation scopes without browsing,
nested preparation, worker-scope revocation, protected read evidence, cancellation,
SIGKILL and lost acceptance/publication COMMIT acknowledgements. All owned
controllers, labelled children, three scratch pools and temporary roots are
verified absent after both files.

The frozen combined source is based on `9aba9569` and includes the already merged
release inventory, native graph/retention changes, Connector discovery and width
validation. All nine native executables build with one compile job, two CPU and
4 GiB; full static/provenance checks and all 117 native units pass, none ignored.
[qualification.json](qualification.json) records the hashes and raw evidence path.
The final session-revocation test correction only changes TypeScript assertions;
all native source inputs remain byte-identical to the qualified image.

The initial PostgreSQL run retained the correct revocation denial but its cleanup
incorrectly attempted to reenable a revoked session. The revocation guard correctly
rejected that cleanup. The final test now asserts irreversible revocation and
continued denial. The earlier run is preserved in the raw evidence directory.

The complete original Agent file also passes **24/24**, zero skips/cancellations,
334.443 seconds including the driver. Its three roots finish in 79.017, 130.146
and 123.191 seconds with their original 300-second limits. All original SDK/CLI,
LangGraph, durable intent, child isolation, lease/worker fencing, cancellation,
SIGKILL, lost COMMIT, Source automation and pinned descriptor/current authority
checks execute on the same qualified native image. All owned resources are
verified absent after cleanup. Evaluation, whole-instance composition and the
complete independent quality gate remain open.
