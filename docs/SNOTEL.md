# SNOTEL Snow Observatory

Moria hosts a bundled React app on a separate loopback origin, backed by real NRCS AWDB ingestion, Iceberg snapshots, and authorized station/observation objects.

From the repository root:

```sh
pnpm start
pnpm moria snotel sync
pnpm moria snotel open
pnpm moria snotel status
```

`start` prints separate management (3210) and snow (3211) addresses. `open` prints the snow app's plain address. Snow uses cookie sessions; see [Google setup and CLI recovery](AUTH.md). `sync` requires the running instance and local operator credentials. The browser reads persisted Moria data; its Reload button does not contact NRCS. No automatic refresh schedule is installed.

Configure an explicit selection with `pnpm moria snotel sync --config stations.json`:

```json
{
  "source": "report-csv",
  "stations": ["663:CO:SNTL", "335:CO:SNTL"],
  "beginDate": "2025-04-01",
  "endDate": "2025-04-30"
}
```

Each sync replaces the current serving selection, with stable identities for station triplets and triplet/date observations. Previous Iceberg snapshots are retained. Limits: 1–20 explicit SNOTEL triplets and 1–400 inclusive days. Defaults are five Colorado stations over October 1, 2025–October 20, 2026. Use an explicit configuration for a different period; defaults do not advance automatically.

The default CLI sync uses the official [NRCS Report Generator CSV export](https://www.nrcs.usda.gov/resources/data-and-reports/report-generator). It downloads each station's metadata and daily SNWD/WTEQ values together, so it does not depend on a separate AWDB REST metadata request. The requested URL explicitly selects `end_of_period`, inches, and feet; the parser checks the report's period/frequency, exact unit-bearing headers, station identity, consistent metadata, date range, duplicate dates, and numeric values. Missing dates/blank values remain null; zero remains zero. The app shows the latest observation date independently of the requested end date.

Select `"source": "report-csv"` in a configuration file for CSV, or `"source": "awdb"` for the existing [AWDB REST API](https://wcc.sc.egov.usda.gov/awdbRestApi/swagger-ui/index.html). Existing explicit configurations without `source` retain AWDB behavior for compatibility. The no-argument CLI default selects CSV. There is no silent source fallback: the completed sync records the chosen source.

CSV does not supply QC/QA flags, county, or a station data-time-zone offset in this export. Those values remain unknown (empty flags/county, null offset); the UI states the limitation and does not claim that the AWDB suspect-data filter was applied. NRCS report end-of-day dates are preserved verbatim. AWDB mode continues to request flags and exclude suspect data.

The original CSV text, request URL, SHA-256 hash, and retrieval timestamp are retained in `snow-raw-source`. AWDB mode retains reserialized parsed JSON, not exact HTTP wire bytes. Both modes publish normalized station and observation Iceberg snapshots through the same authorization and identity paths. Source fetch or validation failure publishes nothing. Three dataset heads publish atomically; the two serving object types and completed-sync marker then publish together in a separate transaction. If indexing fails after dataset publication, prior serving objects remain visible; rerun sync. A PostgreSQL advisory lock rejects concurrent syncs. Interrupted runs are marked on the next sync. There is no durable background job scheduler, resume protocol or garbage collection yet.

CSV successfully loaded five Colorado stations and 1,925 station-days for October 1, 2025 through October 20, 2026; 1,918 station-days contained both measurements. This is observed success, not an upstream availability guarantee. All reads continue to use persisted Moria objects if NRCS subsequently becomes unavailable.

## Scope and hosting

The `snotel` environment's `snow-browser` workload has only `snow-stations:read` and `snow-observations:read`. Its trusted server binding routes only those object types and read-only sync status. It cannot invoke actions, read asset-demo objects, trigger ingestion, or call management. Caller-provided profiles/bearer tokens cannot change that identity. Current scopes/grants are checked by Moria; bootstrap preserves existing revocations. The ingestion workload is separate and writes only its source datasets and indexes these types.

The app uses per-user OIDC sessions with current Moria scopes/grants. Its independent key in ignored `.local/workspace/snow-access-key` is now reserved for CLI recovery-ticket issuance; normal browser queries no longer accept it. Recovery is a separate audited read-only workload session. Static artifacts remain readable on loopback. See [authentication boundaries and limitations](AUTH.md).

`MORIA_LOCAL_DIR`, `MORIA_PORT`, `MORIA_DB_PORT`, and `MORIA_SNOTEL_PORT` configure the local instance. Do not expose these loopback-development servers as production services.

## Build and recovery

```sh
pnpm moria apps bootstrap snotel
pnpm moria apps recover snotel
pnpm moria apps verify snotel
pnpm moria apps rollback snotel
```

Bootstrap/recovery build React from checked-out source, publish a hash-addressed artifact and promote its active pointer, without requiring the UI or database. Recovery does not modify datasets. `pnpm start` composes both hosts and their trusted API bindings. Generic `apps serve` remains static-only and does not install this binding. After promotion, reopen the root app URL to use the new release; existing release URLs remain pinned.

The readable app address is `http://127.0.0.1:3211/snow/` (using the configured port if overridden). Startup/CLI access links use this path. The launch key disappears from the address bar after the app consumes it. The page URL stays readable; release hashes are used only for versioned assets and optional pinned-release links. Old release links still work.

### Front Range comparison view

Snow compares each persisted NWS forecast point alongside its geographically nearest station among the instance's currently loaded SNOTEL stations. The match uses great-circle distance from the actual stored coordinates; it does not claim the station represents a particular route, aspect, elevation, or avalanche zone. Station links select the matching station's detailed charts.

The table shows the next non-expired NWS forecast period and its issuance time separately from daily observations. Observations use each station's latest report with at least one non-null metric within eight days of requested coverage. Both metrics use that same report date; missing fields remain missing. Actual report dates and requested coverage are labeled separately, with exact one-day and seven-day endpoint differences anchored to each report. Missing/withheld endpoints produce “Unavailable,” never zero or a substituted older value. Retrieval dates and source QC/QA flags remain visible. Snow-depth changes are explicitly not snowfall totals. A warning appears when the saved observation window is over three days old; forecast issuance over twelve hours old is labeled potentially stale.

The comparison reads only the existing scoped generated observation API, with cancellation and a 1,000-row cap over fifteen days, so the oldest eligible report still has a seven-day comparison endpoint. No additional permissions or ingestion controls are introduced. Station matching covers loaded sites only; multiple areas can share one station, and a nearest station can still be far away. The table scrolls horizontally on small screens. The source provides period-level forecasts, not model snowfall accumulation or avalanche ratings.
