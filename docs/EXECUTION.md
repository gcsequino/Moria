# Trusted platform execution

Default startup registers no workloads. Trusted server composition can supply real
named handlers through `createPlatformJobs(catalog, handlers)` or the execution
factory passed to `runProcess`. This is in-process trusted code, not arbitrary-code
admission, a plugin upload API, or a distributed execution claim.

Management retains authorized job history, retry for registered handlers and lineage.
Authorized administrators can enqueue a registered workload through
`POST /api/management/v1/jobs/<kind>/enqueue` with `requestKey` and optional `input`,
and configure its schedule through `POST /api/management/v1/jobs/<kind>/schedule`
with `enabled` and `intervalMinutes`. Unknown kinds fail. These are trusted workload
controls, not the public universal run API excluded by R01. Retired Snow/weather
routes fail explicitly in default composition. Historical jobs remain visible;
unregistered workloads cannot be retried through management.

Jobs scheduling, durable request aliases, serial worker admission, retry policy,
transactional fencing and graceful drain are unchanged. Handlers must authorize
their own effects and use the supplied guard/completion transaction to publish
atomically. Process tests use synthetic SQL effects, independent processes and
real PostgreSQL restart/fencing barriers. No scheduler or lease redesign is included.

See [process responsibilities](production/EXECUTION-PROCESSES.md) and the
[coverage-transfer matrix](production/SNOW-RETIREMENT.md). Legacy domain source,
build, SDK and recovery fixtures remain for existing checks; they are not a working
bundled application. This slice does not complete M1 or M3.
