# Management and Snow API processes — M1-02c (#37)

This bounded successor to #35 separates listener and API process lifecycles.
Parent #14 remains open. API contracts, authorization and execution concurrency
are unchanged. Progress is recorded here under the issue's exclusive file scope,
rather than in the shared implementation log. No protected paths are changed.

## Commands and configuration

Use non-root Linux, Node 24, pnpm 11.25.0 and the workspace Python 3.11 `.venv`.
From the repository root, with external PostgreSQL configuration supplied:

```sh
pnpm exec tsx apps/local/src/management.ts
pnpm exec tsx apps/local/src/snow.ts
pnpm exec tsx apps/local/src/scheduler.ts
pnpm exec tsx apps/local/src/worker.ts
```

`management.ts` binds `MORIA_PORT` (default 3210) and, when configured,
`MORIA_PUBLIC_PORT` (3212) for `MORIA_PUBLIC_ORIGIN`. `snow.ts` binds
`MORIA_SNOTEL_PORT` (3211) and, when configured, `MORIA_SNOW_PUBLIC_PORT` (3213)
for `MORIA_SNOW_PUBLIC_ORIGIN`. All listeners bind loopback. The other role's
ports may already be occupied. Shared configuration validation still requires
valid, distinct listener port settings and separate exact HTTPS public origins.
Reverse proxies must preserve the configured Host and supply HTTPS forwarding;
forwarded headers do not establish a new trusted origin. Public recovery stays
disabled. Local recovery, OIDC provider invitations and session binding retain
the existing authorization checks.

Compatibility commands remain:

```sh
pnpm exec tsx apps/local/src/api.ts
pnpm start
pnpm exec tsx apps/local/src/supervisor.ts
```

`api.ts` serves both APIs without execution loops. `pnpm start` retains both APIs
and combined execution, including embedded development database ownership.
`supervisor.ts` retains combined scheduling and work without HTTP listeners.
Neither new API role starts execution loops or initializes, migrates, owns or
stops PostgreSQL. They require `MORIA_DATABASE_MODE=external`, the separately
provisioned versioned schema, scoped runtime grants and lifetime schema leases.

Set `MORIA_DATABASE_SECRET_REF` to the environment variable holding the runtime
PostgreSQL URL. Supply secrets through environment variables or external stores;
do not use an owner, administrative or unrestricted database identity. Existing
admission checks require scoped DML/sequence permissions, read-only schema-version
access, protected audit grants and no table ownership/schema CREATE. TLS defaults
to verification; explicit disable is appropriate only for trusted local fixtures.

## Resources and shared dependencies

Count every simultaneous management, Snow, scheduler, worker, compatible API and
supervisor process, including restart overlap, in `MORIA_DATABASE_REPLICAS`.
Set `MORIA_DATABASE_POOL_MAX` and `MORIA_DATABASE_CONNECTION_BUDGET` consistently:
`pool maximum × replicas + 1 <= connection budget`. Four processes at the minimum
pool maximum of four require at least 17 connections; replacement overlap adds
another pool. This is configuration validation, not fleet discovery or a capacity
claim. Each API also consumes a Node process and its own hosting/cache memory;
bundled asset builds can temporarily add CPU and memory during bootstrap. No new
execution concurrency or approved production workload target is introduced.

Use the existing durable `MORIA_LOCAL_DIR` and warehouse layout consistently.
Management initializes its workspace, management app artifact, local recovery
credential and management browser sessions. Snow initializes its Snow artifact,
Snow recovery key and Snow browser sessions, without the management workspace,
credential or management artifact. Initialize durable credentials before running
multiple replicas of the same role; concurrent first-time credential generation
is not a supported topology. Preserve keys across restart so sessions survive.

Both APIs retain the existing shared auth configuration loader, session signing
key and configured OIDC provider credentials. Management selects the explicit
management invitation list; Snow selects its existing invitation list. Shared
key material does not grant a session access to another application or origin.
Provider credential configuration remains the existing operator-provisioned
mechanism; this slice does not introduce a new credential distribution system.
No credential contents are needed for development verification.

This is process isolation, not complete domain or database-privilege isolation.
Both roles verify the catalog and initialize the existing Snow/weather backend
and job records. Management requires those services for authorized Snow status,
enqueue, schedules, retry and administration; these capabilities remain intact.
Management's app inventory uses the shared app-host store and can observe Snow
artifacts when provisioned by Snow. Snow status still includes weather state.
The schema, authority and durable storage remain shared dependencies. Existing
scoped database grants are unchanged, not newly reduced to per-domain tables.

## Lifecycle and remaining work

The existing startup barrier owns acquisition through pending listener startup.
A signal records shutdown immediately; after that acquisition resolves, no later
listener or execution loop starts. All close calls share one drain, which closes
owned listeners, stops job resources once, releases the schema lease and ends
only the process's pool. Independent APIs and workers remain usable when either
API stops; PostgreSQL remains externally owned.

Static delivery remains in each API process. External static hosting, independent
artifact distribution, environment/stream ownership, multi-environment execution,
and production deployment/capacity evidence remain future bounded work. This
slice does not certify a new Iceberg engine topology or perform live operations.

## Verification and progress

The extended `tests/execution-process.integration.test.ts` uses real temporary
PostgreSQL with synthetic data, scoped runtime grants and child processes. New
coverage verifies fresh role-specific hosting/credential state, opposite-role
ports occupied at startup, local and public listener ownership, authenticated
management enqueue and Snow status, anonymous/cross-application/cross-origin
denials, proxy HTTPS enforcement, and both independent restarts while execution
is blocked before fenced publication. IPC barriers cover termination during each
new role's local and public listener startup; repeated close must drain once.
Existing API/combined startup barriers, worker recovery and fencing checks remain.
A successful combined startup also serves both authenticated APIs, starts the
execution loop and drains a blocked synthetic handler on shutdown. New race ordering uses bounded IPC/database observations, with no
live downloads or timing sleeps establishing the race.

Local verification commands: `pnpm format`, `pnpm typecheck`,
`pnpm exec tsx --test tests/execution-process.integration.test.ts`, `pnpm check`,
and `git diff --check`. An initial targeted run caught a proxy test client that
did not preserve the Host header; the probe now uses Node HTTP with explicit Host
and retains the expected denial assertions.

Independent review of the affected lifecycle and authorization invariants,
exact-head Linux CI, and current-main integration remain unverified and required
before merge. The controller owns publication; all changes remain uncommitted,
and merges remain manual. No delivery gates are relaxed.

### Local results (2026-10-25)

Verified on non-root Linux with Node 24.21.0, pnpm 11.25.0 and the workspace
Python 3.11 environment:

- `pnpm format`, `pnpm typecheck` and `git diff --check`: passed.
- Final targeted process regression: passed, 29.82 seconds.
- Final `pnpm check`: passed, exit 0. All 273 runtime tests passed with zero
  failures or skips (114.57 seconds for the runtime phase). The expanded process
  regression passed in 21.53 seconds within that gate. All 14 Python tests,
  Python formatting/specification/workload validation, SDK drift, TypeScript,
  both application builds and starter verification passed.
- Both new entry points with `MORIA_DATABASE_MODE=development` exited 1 with
  the expected external-mode requirement before acquiring runtime resources.
- Ephemeral local logs: `/tmp/issue-37-format.log`,
  `/tmp/issue-37-targeted-final.log` and `/tmp/issue-37-final-check.log`.

The initial full gate also passed before the successful combined-startup probe
was added; the final gate above includes that probe. No checks were disabled,
no unavailable infrastructure was counted as verified, and no live installation
or paid model API was used. The tests use synthetic PostgreSQL and local fixtures.
Local evidence does not replace independent review, exact-head Linux CI or
current-main integration; those remain unverified controller gates.
