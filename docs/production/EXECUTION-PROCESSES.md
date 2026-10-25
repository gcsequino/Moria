> Issue #47 update: default composition registers no workloads or domain environments. Trusted handlers are supplied explicitly; the process guarantees below remain unchanged. Snow/weather references describe historical fixtures, not current startup. See [SNOW-RETIREMENT.md](SNOW-RETIREMENT.md).

# API, scheduler and worker processes — M1-02b (#35)

This bounded successor to #30/#34 separates the fixed Snow/weather scheduler and
worker. Parent #14 remains open. It adds no execution concurrency, arbitrary
handlers, environment ownership, schema migration or capacity claim.

## Commands and prerequisites

Use non-root Linux, Node 24, pnpm 11.25.0 and the repository-local Python 3.11
`.venv`, following README setup. Run from the repository root:

```sh
pnpm start
pnpm exec tsx apps/local/src/api.ts
pnpm exec tsx apps/local/src/scheduler.ts
pnpm exec tsx apps/local/src/worker.ts
# Compatible combined scheduler/worker:
pnpm exec tsx apps/local/src/supervisor.ts
```

Independent entry points require `MORIA_DATABASE_MODE=external`. Provision the
versioned PostgreSQL schema separately through the existing lifecycle. These
processes neither initialize, migrate, own nor stop PostgreSQL. The combined
development launcher retains embedded database ownership and accepts external
mode. There is no adoption path for unversioned databases.

Set `MORIA_DATABASE_SECRET_REF` to the environment variable containing the runtime
PostgreSQL URL. Supply secrets through environment variables or external stores.
Existing URL validation prohibits query options and implicit credential-file
reads. TLS defaults to `verify`; `disable` is for explicitly trusted local tests.
Runtime admission requires the existing scoped DML/sequence grants and schema
version read access, without administrative powers, table ownership or schema
CREATE. All roles retain the lifetime shared schema lease and checkout fencing.

Set `MORIA_DATABASE_POOL_MAX` (minimum 4), `MORIA_DATABASE_REPLICAS` and
`MORIA_DATABASE_CONNECTION_BUDGET` consistently. Replicas counts every simultaneous
API, scheduler, worker and supervisor process, including replacement overlap.
Validation requires `pool maximum × replicas + 1 <= connection budget`; four
processes with pools of four need at least 17 connections. This is configuration
validation, not fleet discovery.

Use the same `MORIA_LOCAL_DIR` and durable shared warehouse paths across roles.
API processes retain recovery credentials, session keys, OIDC and authorization;
preserve that state across restarts and provision it before multiple API replicas.
Concurrent first-time credential creation is not a supported topology. Scheduler,
worker and supervisor return before API credential/authentication bootstrap,
static hosting or HTTP listener binding. Shared configuration validation still
includes listener settings.

## Responsibilities and coordination

API-only retains authorized enqueue, inspect, schedule configuration and retry.
It never starts either loop. Scheduler-only dispatches due fixed workloads; it
never claims work, recovers attempts or invokes handlers. Worker-only claims and
executes queued work and recovers interrupted attempts; it never dispatches or
advances schedules. Shared initialization verifies the catalog and ensures the
bundled environments, grants and initially paused schedule records.

Scheduler dispatch locks due schedule rows in kind order within a serializable
catalog transaction. Enqueue, durable request-key aliases and next-run advancement
commit together. A crash before commit rolls all of them back; a crash after
commit leaves both the firing and advancement durable. Existing keys remain
`schedule:<kind>:<due-time>`. Competing schedulers serialize on the same rows;
serialization/deadlock retries use the existing bounded catalog policy. Exhausted
contention leaves due work for the next loop rather than advancing it partially.

Schedule configuration updates lock those same rows, so configuration and
advancement serialize and stale dispatch cannot overwrite a newer configuration.
Missed intervals coalesce into one refresh, advancing from current time.
Equivalent active input reuses the job with a durable request alias. Different
manual input takes precedence: that scheduled interval advances without replacing
or duplicating the manual job. Existing manual request-key conflicts are unchanged.

Workers retain the global session advisory supervisor lock, one active job per
kind, interrupted-attempt recovery, unpredictable publication fences, heartbeat,
30-minute cooperative deadline and retry policy. Scheduling never acquires the
execution lock or modifies worker fences. Combined mode and supervisor-only run
the same scheduler and worker paths as independent loops, allowing scheduling
during a blocked handler without additional handler concurrency. Direct `tick()`
callers retain schedule-then-work behavior.

These are trusted fixed handlers in one bundled environment, not database roles
with newly narrowed table privileges. A disconnected worker may still have
in-flight work, but its stale publication is rejected. Arbitrary external effects
are not promised exactly-once execution.

## Lifecycle

SIGINT/SIGTERM stops new loop iterations and drains in-flight scheduler
transactions and handlers before releasing schema leases and pools. Repeated
close requests share the same drain. Startup still records shutdown immediately,
waits for pending acquisition/listen operations and checks shutdown before later
listeners or execution start, preserving #34.

Stopping API or scheduler processes does not stop workers or external PostgreSQL.
Worker shutdown drains its active handler; hard termination leaves recovery to
the next fenced worker. No forced handler cancellation mechanism is introduced.
Database failures retain generic diagnostics. Static hosting remains with the API.

## Scope and verification

Changes are limited to the issue's execution/runtime directories, process tests
and fixtures, and this document. Evidence is recorded here instead of the shared
implementation progress file. No dependency, schema baseline, storage, delivery
policy or protected-path change is required.

The new real PostgreSQL child-process test uses a temporary synthetic cluster,
validated external configuration, scoped runtime grants and actual schema leases.
An IPC fixture intercepts the SQL call immediately after enqueue and before
advancement inside the real transaction; no production test flags exist.
It covers crash rollback/restart, competing schedulers, durable firing identity,
scheduler-only non-execution, worker-only non-dispatch, configuration serialization,
active-job aliasing, scheduling during a blocked handler with an unchanged fence,
and scheduler/worker shutdown isolation. Bounded database barriers observe locks
and state; race ordering does not depend on sleeps.

The existing process regression retains authenticated API enqueue/inspect/schedule/
retry, API restart isolation, competing supervisors, connection-loss stale
publication rejection, hard-kill recovery, graceful drain and all four #34 startup
listener barriers. Existing database, authorization and real Iceberg publication
checks remain in the complete gate.

Verification results for the uncommitted issue-35 workspace are recorded below.

Independent human review of coordination, recovery and publication invariants and
exact-head Linux quality CI remain required and unverified here. The controller
owns publication and merges remain manual. Static-host extraction, broader API
role separation, general multi-environment scheduling/concurrency and production
deployment evidence remain subsequent bounded work under #14.

### Local verification evidence (2026-10-25)

Non-root Linux, Node 24.21.0, pnpm 11.25.0 and workspace Python 3.11.17:

- `pnpm format`: passed. No out-of-scope files changed.
- Final `pnpm check`: passed, exit 0. All 273 TypeScript/runtime tests passed
  with zero failures or skips (86.62 seconds for the runtime test phase).
  Python formatting/validation and all 14 Python tests, SDK drift, TypeScript,
  both application builds and starter verification passed.
- In that final gate, the scheduler process test passed in 5.87 seconds and the
  existing execution process regression passed in 13.71 seconds. Recovery
  replacements in the latter now use worker-only mode; combined supervisors
  remain covered alongside API lifecycle and #34 barriers.
- Both new entry points invoked with `MORIA_DATABASE_MODE=development` exited 1
  with the expected external-mode requirement before startup.
- `git diff --check`: passed. Changes remain uncommitted.
- Local logs: `/tmp/issue-35-format.log` and
  `/tmp/issue-35-final-check.log`. These are ephemeral workspace evidence, not CI.

An intermediate gate caught a missing explicit receiver type in the fixture's
SQL interception; it was corrected before the final passing gate. No checks were
disabled or infrastructure-dependent tests skipped. All cluster/data fixtures
were synthetic; no live installation, download or model API was used. This does
not establish a new multi-process Iceberg engine certification or production
capacity. Independent review and exact-head CI remain unverified as noted above.
