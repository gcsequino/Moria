# Durable execution and Front Range conditions

Moria now runs two fixed, trusted workloads through a PostgreSQL-backed queue: `snotel` and `weather`. This is an instance-local execution service, not an arbitrary-code worker platform or container sandbox. It uses the existing scoped ingestion identity; browser callers cannot select worker identities, endpoints or code.

## Scheduling and operations

Management offers Sync now, job status, schedule controls, retries and recorded provenance. Default intervals are six hours for NRCS observations and one hour for NWS weather. Installation creates paused schedules; the operator enables them explicitly. Enabled schedules persist through restart. Missed intervals coalesce into one refresh rather than replaying every missed forecast. The computer and Moria runtime must be running; the current tunnel deployment is not an always-on cloud host.

Use `pnpm moria snotel jobs`, `sync`, `weather-sync`, `retry <id>`, `schedule on|off`, or `weather-schedule on|off`. Sync commands now return an accepted job immediately; inspect jobs to learn whether publication succeeded. Optional `sync --config file.json` retains explicit station/date configuration. An ordinary scheduled refresh uses the existing five stations and a rolling 385-day period through the current date in America/Denver. The exact resolved configuration is persisted before processing, so retries across midnight do not change it.

Jobs have queued/running/retrying/succeeded/failed states and append-only numbered attempt records. Three unsuccessful attempts exhaust automatic retries; an operator may retry the same job while retaining history. Retries wait one minute. Failed permanent authorization/input errors need operator attention. Request keys are durable, including aliases created when equivalent active work is coalesced. Different active input is rejected rather than silently substituted. Schedule pause prevents future dispatch and does not cancel existing work.

## Publication and recovery

One PostgreSQL session advisory lock elects the current supervisor. Each claimed job gets an unpredictable fence and a heartbeat; publication checks and locks that exact running job in its transaction. Losing the worker connection, exceeding the thirty-minute cooperative deadline or losing freshness blocks publication. A replacement supervisor marks old attempts interrupted, invalidates their fences and rediscovers work. The queue supports fixed trusted handlers only; deadlines do not forcibly kill arbitrary code, and shutdown waits for an active bounded handler.

SNOTEL persists a plan with staged Iceberg metadata and snapshot IDs before publishing the dataset catalog. A retry reuses that plan and the catalog publication receipt. The previous served station/observation generations remain visible until both new generations and the sync result/job completion commit together. An intervening newer dataset head prevents an old failed job from restoring stale serving data. Dataset publication and serving publication are still two transactions; management lineage can show this intermediate state. Staged artifacts and history are retained; garbage collection remains future work.

Weather publishes a bounded PostgreSQL forecast document and raw-source receipt atomically with job completion. It is deliberately an operational forecast product rather than another Iceberg dataset. Lost acknowledgment replays the receipt without downloading again. Upstream errors preserve the last published forecast. Failed initial loads stay unavailable instead of showing sample forecasts.

## Permissions and provenance

Only current instance administrators or the local operator CLI may inspect/control jobs, schedules and instance lineage. Enqueue/schedule changes check the initiating session transactionally. Once accepted, work is delegated to the existing ingestion workload; it does not depend on a browser remaining logged in. Current workload grants are checked before source work and publication. Pausing schedules does not revoke worker authority. CLI recovery remains local-only and independent of Google.

Snow retains its existing read-only station/observation permissions. Its protected status response also includes the fixed public-weather product; it exposes no queue, mutation, schedule or administrative lineage endpoint. This is a trusted binding for the bundled Snow app, not a general mechanism for giving arbitrary apps access to new resources.

The admin lineage explorer reads recorded catalog publications, snapshots, indexing provenance, object generations, jobs, attempts and Snow syncs. It is bounded and explicitly reports missing provenance. It never guesses attempt-to-publication relationships or exposes raw source bodies, credentials, arbitrary result payloads or warehouse paths. Folder permissions and automatic marking inheritance are separate work.

## Front Range conditions

Representative forecast points cover Berthoud Pass, Jones Pass/Butler Gulch, Loveland Pass/Dry Gulch, Indian Peaks/Brainard Lake and Cameron Pass. Coordinates represent areas, not individual ski routes. Weather comes from the free [National Weather Service API](https://www.weather.gov/documentation/services-web-api): server-side bounded sequential requests, official-host allowlist, no redirects and no API key. Source update/retrieval times and stale-state labels remain visible. Forecast periods include temperature, wind, precipitation probability and official forecast text; no proprietary forecast or OpenSnow content is copied.

The app links to [CAIC](https://avalanche.state.co.us/forecasts) for current avalanche forecasts and [Front Range Ski Mountaineering](https://www.frontrangeskimo.com/) for route research. It does not calculate avalanche danger or route safety. SNOTEL snow depth and SWE are measurements; depth change is not equivalent to new snowfall. This first version does not include snowfall-model totals, radar, hourly forecasts, route mapping or full OpenSnow feature parity.

## Verification

Real PostgreSQL tests kill worker subprocesses before commit and after commit-before-acknowledgment. They verify rollback/recovery and one publication. Real Iceberg tests interrupt Snow after catalog publication, recreate the service, forbid a new download, and verify identical snapshot heads and one served publication; failed completion rolls back serving and resumes safely. Additional tests cover schedule coalescing, request aliases, retry history, stale fences, weather receipts/rollback, lineage truthfulness, Google-like OIDC administration, CSRF and CLI recovery. See IMPLEMENTATION.md for the final live deployment checkpoint.
