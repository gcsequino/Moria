# Python process lifecycle ownership

`services/process/src/python.ts` is the only production importer of Node child
creation primitives. It exposes five fixed trusted tasks, not a configurable
command runner. Adapters retain domain/protocol checks and authority/publication
fences; they receive no raw child handles, streams, kill primitive or command hook.

| Task / adapter | Deadline ceiling | Total stdin / stdout | Frame ceiling |
| --- | --- | --- | --- |
| stage / workers/iceberg/stage.ts | 60 s | 64 / 64 MiB | 64 MiB |
| stream / workers/iceberg/stream.ts | 30 min | 1 / 256 MiB | 4 MiB |
| scoped-read / workers/iceberg/scoped-read.ts | 60 s | 192 / 64 MiB | 4 MiB |
| metadata / services/catalog/src/storage-publication.ts | 30 s | 2 MiB / 1 KiB | 2 MiB |
| snapshot / services/catalog/src/snapshot-publication.ts | 60 s | 96 MiB / 1 KiB | 2 MiB |

These are ceilings, not target workload sizes. Existing smaller row/object/frame
limits remain in adapters. Scoped input allows the existing 128 MiB transfer budget
after base64 framing; snapshot input covers the existing 64 MiB object budget.
The formerly unbounded materialized stage/read response is now capped at 64 MiB;
larger results must use the existing streaming adapter. Streaming output has a
256 MiB total ceiling in addition to bounded frames and the existing deadline.
No Python address-space/CPU limits are increased.

JSON serialization has a conservative frame budget before constructing its full
output. Raw writes are bounded before intake and await write completion, retaining
pipe backpressure. UTF-8 lines accumulate bounded chunks, with one copy per frame
and at most 65,536 retained fragments. Scoped lockstep framing retains rejection of
unsolicited bytes after a response frame. Adapters retain strict UTF-8, JSON shape,
row counts, snapshot identity, exact requested ranges and final authority checks.
Stderr is drained without retaining content and capped at 64 KiB plus a one-byte
overflow marker. Diagnostics contain no unrestricted stderr, rows, metadata,
environment, credentials or raw native exception text.

Stage/stream retain the declared MORIA_PYTHON override and existing local worker
environment. Metadata, scoped-read and snapshot use the fixed virtualenv interpreter
in isolated mode with the existing minimal scoped environment. Metadata validation
is now a fixed script; commands, arguments and environment cannot be selected by
request fields. These processes retain their existing filesystem/network authority;
this lifecycle owner is not a new OS sandbox.

An overall deadline and optional caller AbortSignal cover startup, writes, reads,
paused generators and exit. Timeout/cancellation initiates termination, closes
pipes and escalates SIGTERM to SIGKILL after 250 ms. Every adapter awaits the close
event before settling or finishing iterator cleanup. There is no abandoned cleanup
job after caller settlement. Iterator return/throw and protocol failure use the same
cleanup. Already-aborted requests start no child. Buffered output is checked before
each release; scoped authority/deadline checks also remain active after child exit.

`completion` reports the observed process exit; `ProcessFailure.outcome` preserves
the safe failure classification and actual exit code/signal, PID and bounded stderr
byte count. A later protocol failure can follow a successful OS process exit.
Categories are startup, IPC, unexpected exit, signal, timeout, cancellation, limit,
protocol and explicit invalid input. Termination signals caused by cleanup do not
replace the earlier IPC/deadline/cancellation cause. A signal does not imply OOM.
Unexplained nonzero exits remain unexplained rather than becoming invalid content.
Only the fixed metadata/snapshot validators' deliberate exit 2 for ValueError or
PyIceberg ValidationError denotes rejected content; uncaught exceptions/import failures/native crashes retain other outcomes.

Materialized staging throws a safe ProcessFailure, retaining its generic HTTP 500
behavior. Streaming retains existing Moria error codes with the safe process failure
as cause. Storage adapters retain StorageError codes and attach that cause: explicit
invalid input maps to INVALID_INPUT, bounds to LIMIT_EXCEEDED, cancellation/deadline
to CANCELLED and unknown execution/IPC/signal/protocol failures to IO_ERROR.

The only dependency-owned production process entry is embedded-postgres, imported
by `apps/local/src/development-database.ts`. Its implementation is installed library
code, outside the Python owner and source-import proof. The helper owns startup/
stop requests, requests cleanup after startup failure and waits for actual pg-client
end events before stopping its cluster. The catalog-storage fixture now reuses that
helper rather than assuming pool.end means sockets have disconnected. A deterministic
removed/idle-client barrier fails the old fixture and passes this shared owner.
`services/app-host/src/store.ts` retains its exact process.kill(pid, 0) liveness probe;
it does not terminate a process. Build/test process creation is outside production.

The architecture gate discovers source and rejects raw child_process imports,
aliases/re-exports, require/dynamic loads, builtin module loaders, unregistered lifecycle callers and raw
process termination. The dependency exception and exact contract inventory are
reviewed policy. Tests use fixed synthetic children by intercepting Node's spawn
binding in test code; production has no test-command bypass. One contract runs
through all five real adapters, including missing executable, success, explicit
invalid input, abnormal exit/signal, partial/malformed/UTF-8/oversized output,
stderr flood, stalled IO, EPIPE, deadline/cancellation, already-aborted work and
ignored graceful termination. Streaming return/throw and blocked stdin are covered,
with real PID reaping assertions. Existing real PostgreSQL/PyIceberg/local/S3 and
exact committed-receipt retry tests remain in the full gate.

Cleanup relies on normal OS SIGKILL and close-event semantics; it cannot guarantee
bounded kernel reaping for a process stuck in uninterruptible kernel state. It does
not manage arbitrary descendant process trees; the fixed Python programs do not
launch descendants. Installed libraries and reflective/erased JavaScript are not a
sandboxed threat model. New executables or subprocess trees require explicit review.
Independent host policy, required checks, restricted publishing credentials and
human lifecycle/publication review remain required. Proposed factory protection is
not installed, and existing private-repository administrative protection is unmet.
This refactor does not diagnose or close #67 or reset its repair budget. PR77's
PostgreSQL shutdown-race evidence is separate from PR75's validator exit evidence.
