# API and execution processes — M1-02a (#30)

This slice separates the management/Snow APIs from the existing fixed-handler
execution supervisor. Parent #14 remains open. The scheduler and worker remain
one serial supervisor; this is not general multi-environment scheduling or a
user-code execution service.

## Entry points and prerequisites

Use supported non-root Linux, Node 24, pnpm 11.25.0 and the repository-local Python
3.11 `.venv`, following README setup. Run from the repository root:

```sh
# Existing combined development launcher (unchanged command):
pnpm start

# Independent roles, in separate processes with the same runtime configuration:
pnpm exec tsx apps/local/src/api.ts
pnpm exec tsx apps/local/src/supervisor.ts
```

Independent roles require `MORIA_DATABASE_MODE=external`. Provision and version the
external PostgreSQL database separately using the existing database lifecycle.
Neither role initializes, migrates, owns, or stops that database. Existing
unversioned development databases remain unsupported; there is no adoption path
or migration expansion here. The combined launcher retains embedded development
ownership and also continues to accept external mode.

Set `MORIA_DATABASE_SECRET_REF` to the name of an environment variable containing
the runtime PostgreSQL URL. Supply secrets through the environment or external
credential management, never checked-in configuration, command output or logs.
The existing URL validation disallows query options and implicit credential-file
reads. `MORIA_DATABASE_TLS=verify` is the default; `disable` is for explicitly
trusted local test connections. The runtime role must have existing scoped DML
and sequence grants, schema-version read access, and no administrative powers,
table ownership or schema CREATE permission. Schema admission and the lifetime
shared schema lease remain mandatory; losing the lease fences pool checkouts.

Set `MORIA_DATABASE_POOL_MAX` (minimum 4), `MORIA_DATABASE_REPLICAS` and
`MORIA_DATABASE_CONNECTION_BUDGET` consistently across the installation. Replicas
counts **all simultaneously running API and supervisor processes**, including
replacement overlap. The existing validator requires
`pool maximum × replicas + 1 <= connection budget`, reserving one migration
connection. For example, three processes at pool maximum 4 require a budget of
at least 13. Configuration validation is not automatic fleet discovery.

Both roles need the same `MORIA_LOCAL_DIR` and durable shared warehouse paths
(`warehouse/` and `snow-warehouse/` under that directory). These paths must identify
the same storage from each process. Do not use independent copies of a warehouse.
API processes retain the existing local recovery credentials, session key, OIDC
configuration, scoped browser sessions and authorization behavior. Preserve that
authentication state across API restarts and provision it before running multiple
API replicas; concurrent first-time API credential creation is not introduced as
a supported topology. The supervisor does not load API credential files, create
browser sessions, bootstrap static apps or bind HTTP listeners.

API listeners retain `MORIA_PORT` (3210), `MORIA_SNOTEL_PORT` (3211), and existing
explicit public-origin/reverse-proxy settings. Static hosting remains attached to
the API. This slice does not extract a static host. Configuration validation stays
shared, including listener settings, even when the supervisor binds no listeners.

## Startup, execution and shutdown

The shared runtime initializes the same bundled Snow/weather services against a
verified catalog. Initialization may ensure bundled environments, grants and
schedule records; it performs no external schema migration. API-only constructs
the existing job interface for authorized enqueue, inspect, schedule and retry
requests, but never starts the supervisor loop. It does not fire schedules, claim
jobs or execute handlers. Supervisor-only starts that loop and returns before any
HTTP or authentication bootstrap. No CLI switch loads arbitrary handlers.

The existing global PostgreSQL supervisor advisory lock keeps execution serial
across processes and kinds. Active-job uniqueness, request-key coalescing,
scheduled firing keys, heartbeat, attempt recovery, publication transactions and
fencing are unchanged. Additional supervisors contend for the same lock; they do
not increase throughput. A handler that loses its lock can still have in-flight
work, but its stale publication must be rejected. This does not promise exactly
once arbitrary external side effects.

SIGINT/SIGTERM closes that process's HTTP listeners, drains its own active job,
releases its schema lease and ends its pool. Repeated close requests await the same
drain. Shutdown requested during startup waits for resource acquisition and any
pending listener bind to settle before draining. Startup checks shutdown before
and after each bind and before starting execution, so it cannot add later
listeners or start jobs after drain. Signal handlers remain installed through
drain, including repeated termination signals. API shutdown never stops the supervisor or external database. Supervisor
shutdown never stops API processes. Drain preserves existing handler behavior:
it waits for the active handler, with no new forced cancellation mechanism. An
operator-enforced hard kill leaves interruption recovery to the next supervisor;
publication is protected by the existing fence. Database errors use generic
messages and stop the affected process's services.

## Scope, progress and verification

Allowed changes are confined to `apps/local/src/`, the new process integration
test and its fixture directory, and this document. No database baseline, shared
contracts, dependencies, package scripts, delivery policy or existing tests change.
Progress is recorded here instead of `docs/IMPLEMENTATION.md`, as required by #30.
No commit, publication, live deployment, live download or model call is part of
this work.

The synthetic integration test owns a temporary PostgreSQL cluster externally to
the application children. Children use the validated external configuration,
scoped runtime role, real schema leases and minimum-size pools. A fixture supplies
fixed synthetic handlers through a module-level initialization parameter, never
through production CLI flags or environment controls. IPC barriers hold execution
at publication and observe supervisor ticks. Bounded database waits and cleanup
cover all child processes and the temporary cluster.

Evidence on non-root Linux (2026-10-25), Node 24.21.0, pnpm 11.25.0,
Python 3.11.17 from `.venv`, on branch `delivery/issue-30` based on `5cc57ce`
with the uncommitted changes described here:

- `pnpm typecheck`: passed during implementation.
- `pnpm exec tsx --test tests/execution-process.integration.test.ts`: passed during
  implementation, exercising authenticated enqueue/inspect/schedule, API restart
  during active work, shared request-key identity, competing supervisors, loss of
  the supervisor connection and rejection of stale publication, SIGKILL recovery,
  and graceful handler drain. Final gate results are recorded below.
- The final test also verifies authorized retry, API-only does not fire a due
  schedule or claim queued work, all four configured HTTP ports are available
  while only supervisors run, and graceful exits release runtime connections
  without stopping the externally owned database.

Initial implementation verification (before the lifecycle repair below):

- `pnpm format`: passed; no out-of-scope files changed.
- `pnpm check`: passed, exit 0. All 181 TypeScript/runtime tests passed with zero
  failures or skips (80.91 seconds for the test phase). This includes existing
  crash/replay, authorization, external-database and real Iceberg integration
  tests. Python formatting and validation, 14 Python tests, SDK drift checks,
  TypeScript checks, both application builds and starter verification passed.
- The process test passed in the full gate in 11.48 seconds against the bundled
  real PostgreSQL test cluster, with no downloads or model calls.
- Both runnable entry points were invoked with `MORIA_DATABASE_MODE=development`;
  each exited 1 with the expected external-mode requirement before startup.
- `git diff --check`: passed. Changes remain uncommitted and within the exclusive
  file scope. Local command output is in `/tmp/issue-30-format.log` and
  `/tmp/issue-30-check.log`; these are ephemeral workspace evidence, not CI logs.

No infrastructure-dependent gate was skipped in the local complete check. The
process publication fixture uses a synthetic SQL publication row and the real
job guard/completion transaction; it does not claim a new multi-process Iceberg
engine certification. Existing real Iceberg crash/replay tests remain in the gate.

Independent review of lifecycle and publication invariants and the Linux quality
CI job on the final published revision remain required and unverified here. The
controller owns publication; merges remain manual. This slice does not close #14:
scheduler/worker separation, static-host extraction, general multi-environment
concurrency and production deployment evidence remain later work.

## Startup/shutdown coordination repair

The review identified a race in which drain could pass a pending listener before
it bound, then startup could open further listeners or start combined-mode jobs
after stop. The runtime now records shutdown immediately, waits for startup to
settle before releasing resources, and stops startup at listener boundaries.
Startup cancellation exits normally; startup failures still report a generic
error and exit unsuccessfully. No CLI behavior or shared execution contract was
expanded.

The process fixture now pauses the native listen call with an explicit IPC
barrier. For both API and combined roles, the regression sends SIGTERM while the
first local listener or first public listener is pending, waits for signal
acknowledgment, then releases the bind. Each child must exit normally within the
bounded wait, make no subsequent listen or jobs.start calls, drain jobs exactly
once, release all four configured ports, and release its database connections.
The externally owned PostgreSQL cluster and an unrelated supervisor's existing
connections survive; that supervisor subsequently publishes its held synthetic
job using the real fenced transaction. These barriers exist only in the test
fixture, not in production entry points or environment configuration.

Repair verification on the same non-root Linux workspace:

- `pnpm exec tsx --test tests/execution-process.integration.test.ts`: passed,
  1 test, zero failures/skips, 15.23 seconds including all four startup barriers.
- `pnpm format`: passed; no out-of-scope changes.
- Complete `pnpm check`: passed, exit 0. All 181 TypeScript/runtime tests passed
  with zero failures or skips (82.37 seconds for the test phase). The process
  integration test passed in 13.54 seconds. Python formatting/validation and all
  14 Python tests, SDK drift, TypeScript checks, both application builds and
  starter verification passed using the workspace `.venv` (Python 3.11.17).
- `git diff --check`: passed. Repair output is recorded in the ephemeral local
  files `/tmp/issue-30-repair-format.log`, `/tmp/issue-30-repair-process.log` and
  `/tmp/issue-30-repair-check.log`. These are local evidence, not CI results.
  No infrastructure-dependent check was skipped.

The repair remains confined to the runtime, process integration test and fixture,
and this document. Shutdown synchronizes with existing initialization and handler
drain; it does not introduce a forced deadline for stalled external dependencies
or handlers. Independent review of the repair and Linux quality CI on the final
published revision remain unverified and are controller-owned follow-up work.
