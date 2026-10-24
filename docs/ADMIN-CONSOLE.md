# Moria administration console

The bundled management application is the human interface for operating an instance. It uses the generated management OSDK and the current browser session. API authorization remains authoritative: UI visibility does not grant permission.

## Access today

The console currently requires an explicit **instance administrator** grant. Google sign-in alone does not grant this role. It is not yet a general resource explorer for users with arbitrary subsets of permissions. The interface states this limitation instead of presenting an incomplete view as a complete inventory.

Snow is a separate application with its own read-only scope. Management refresh controls do not give Snow permission to enqueue jobs, change schedules, import data, or edit objects. The console keeps the identity visible and offers sign-out; CLI recovery remains independent of Google and the console.

## Workflows

- **Overview:** asset publication state, installed applications, active executions and failures in recent history. Counts summarize the records returned by the instance, not all historical executions.
- **Assets:** import CSV/JSON, browse authorized objects, edit persistent overrides and restore source values. Import replaces the source dataset; stable IDs retain overrides.
- **Applications:** inspect published release hashes and retained versions, with operator recovery commands. “Published” describes deployment metadata, not a live application health probe.
- **Executions:** run **Sync now** for snow observations or mountain weather; pause or enable recurring schedules; filter recent jobs; inspect phase, attempt, requester, error and result provenance; retry failed jobs. The endpoint returns the 60 most recently created jobs. Pausing a schedule does not cancel work already queued or running. Default intervals are six hours for snow observations and one hour for weather; the console preserves an existing interval when toggling its schedule.
- **Lineage:** choose All, Data, Workflow or Executions, then search resources by name, kind or ID and follow recorded upstream/downstream relationships. Data shows snapshot/object provenance (weather publications live in PostgreSQL, not Iceberg). Workflow shows the object types, actions and endpoints declared by active application artifacts; these declarations are neither proof of runtime usage nor permission grants. Executions shows jobs, attempts and exactly correlated publications. Each perspective includes only edges whose endpoints it contains; a notice offers All when a resource has connections outside that perspective. Metadata and coverage limitations remain visible. Missing edges are not inferred, and the bounded graph is not a complete audit archive.

Overview and Executions refresh status every ten seconds while the tab is visible. Navigating away stops polling. Lineage has an explicit refresh control. Failed refreshes are shown rather than silently replacing them with healthy status. No successful job result is attributed to a failed or merely queued run.

A failed or interrupted Sync now request keeps its request key for a retry during that mounted console session. The server owns deduplication, job recovery and publication safety. After navigating away or reloading, inspect existing jobs before submitting another refresh.

## CLI operations and recovery

Run these from the Moria repository on the instance host:

```sh
pnpm moria snotel jobs
pnpm moria snotel sync
pnpm moria snotel weather-sync
pnpm moria snotel retry <job-id>
pnpm moria snotel schedule on
pnpm moria snotel schedule off
pnpm moria snotel weather-schedule on
pnpm moria snotel weather-schedule off
```

Use local recovery when browser sign-in or the console is unavailable:

```sh
pnpm moria auth recover --app management
pnpm moria apps list
pnpm moria apps recover management
```

Recovery credentials belong on the local host. Do not publish them in application URLs or documentation. Rebuilding the management application does not erase stored data, edits or execution history.

## Operational health evidence

Overview includes a refresh-health card for snow observations and mountain weather, with direct links to recent/successful jobs and lineage. It distinguishes the last recorded publication from a fallback successful-job completion when publication metadata is unavailable. A failed newer job never replaces the last successful timestamp.

The checks have explicit policies:

- An enabled schedule is due when its recorded `next_run` precedes the displayed status snapshot. No grace period is applied; the UI notes that ordinary scheduler claiming can briefly produce this condition.
- Publication/completion age is compared with that workload's configured schedule interval. Paused schedules do not generate interval or overdue warnings.
- Weather source-age warnings use the existing Snow app policy of **12 hours** since the NWS issue timestamp. Only area nodes connected to the latest recorded committed weather publication are assessed. Unknown timestamps remain unknown. This overview does not evaluate forecast expiration or actual mountain safety.
- SNOTEL publication freshness measures ingestion, not observation age or NRCS reporting delay.

The overview fetches bounded lineage alongside recent jobs. If lineage retrieval fails, it says publication evidence is unavailable and can still show job status. No green health claim is inferred from absent history. The displayed snapshot time identifies when these calculations were made. Lineage fetches occur only while Overview is active; hidden-tab polling remains paused.
