# Moria administration console

The bundled management application is the human interface for operating an instance. It uses the generated management OSDK and the current browser session. API authorization remains authoritative: UI visibility does not grant permission.

## Access today

The console currently requires an explicit **instance administrator** grant. Google sign-in alone does not grant this role. It is not yet a general resource explorer for users with arbitrary subsets of permissions. The interface states this limitation instead of presenting an incomplete view as a complete inventory.

The console keeps identity visible and offers sign-out. Local management recovery remains independent of Google. The Snow/weather POC is retired.

## Workflows

- **Overview:** asset publication state, installed applications, active executions and failures in recent history. Counts summarize the records returned by the instance, not all historical executions.
- **Assets:** import CSV/JSON, browse authorized objects, edit persistent overrides and restore source values. Import replaces the source dataset; stable IDs retain overrides.
- **Applications:** inspect published release hashes and retained versions, with operator recovery commands. “Published” describes deployment metadata, not a live application health probe.
- **Executions:** filter the 60 most recent jobs; inspect phase, attempt, requester, error and result provenance; retry failed jobs only when their trusted handler is registered. Default composition has no workloads. Historical jobs remain visible.
- **Lineage:** choose All, Data, Workflow or Executions, then search resources by name, kind or ID and follow recorded upstream/downstream relationships. Data shows snapshot/object provenance (weather publications live in PostgreSQL, not Iceberg). Workflow shows the object types, actions and endpoints declared by active application artifacts; these declarations are neither proof of runtime usage nor permission grants. Executions shows jobs, attempts and exactly correlated publications. Each perspective includes only edges whose endpoints it contains; a notice offers All when a resource has connections outside that perspective. Metadata and coverage limitations remain visible. Missing edges are not inferred, and the bounded graph is not a complete audit archive.

Overview and Executions refresh status every ten seconds while the tab is visible. Navigating away stops polling. Lineage has an explicit refresh control. Failed refreshes are shown rather than silently replacing them with healthy status. No successful job result is attributed to a failed or merely queued run.

## CLI operations and recovery

Use local recovery when browser sign-in or the console is unavailable:

```sh
pnpm moria auth recover --app management
pnpm moria apps list
pnpm moria apps recover management
```

Recovery credentials belong on the local host. Do not publish them in application URLs or documentation. Rebuilding the management application does not erase stored data, edits or execution history.

## Operational evidence

Job history and lineage report recorded evidence. The default console has no Snow/weather refresh or health cards. Missing evidence is not a health claim.
