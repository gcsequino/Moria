# Default Snow composition retirement — issue #47

## Scope and dependency trace (before behavior changes)

The supplied checkout is on `delivery/issue-47`. This slice removes default domain
composition, not domain storage or historical fixtures. No migration, catalog,
lease, generated SDK, build script or delivery policy changes are authorized.

| Dependency / contract | Existing coverage | Coverage transfer and retained evidence |
|---|---|---|
| Local runtime → SnowService → Snow warehouse, authority, keys and weather jobs | SQL integration and execution process tests | Default runtime uses an explicit trusted handler factory with an empty default registry. Neutral process handlers publish real SQL effects atomically with completion. Legacy SQL coverage stays intact. |
| Runtime → bundled Snow build, local/public listeners | AppHost, public hosting and process tests | Only management is bootstrapped by default. Explicit retired configuration fails. Generic AppHost/scoped hosting and cross-application/origin tests remain; add neutral composition coverage. |
| Management → Snow cards and generated endpoints | Management and SQL fixture tests | Remove domain controls from the mounted UI; retain job history/retry, assets, lineage, admin guards and app hosting. Generated contracts remain historical build inputs. |
| CLI → Snow command, auth and bundled recovery defaults | Auth config and AppHost tests | Default auth targets management; retired CLI requests fail explicitly. Generic publishing/serving remains. |
| Auth configuration → every enabled credential file | Auth config and browser session tests | Validate all shared provider declarations; resolve credentials only for the selected role. Add unused unavailable vs required unavailable regressions. |
| Jobs durable scheduling, duplicate dispatch, recovery, fencing, drain | Execution/scheduler process tests and SQL integration | Rename process workloads to synthetic kinds, retaining real PostgreSQL transactions, independent processes, fault barriers and assertions. Jobs implementation is unchanged. |
| Catalog/ontology/Authority/BrowserSessions | Existing real SQL, auth, hosting and isolation tests | Preserve all existing standalone fixture tests. No claim of new arbitrary-code or distributed execution support. |

Accepted obligations: core C01–C08, security S01–S08 and runtime R01–R03 remain
mandatory; production acceptance is defined in ACCEPTANCE.md. Restricted views
remain terminal and Iceberg retains dataset ownership.

## Residual cleanup and validation

Snow domain code, source/build/SDK fixtures and migration history remain for existing
checks. Physical deletion is not claimed. Protected `package.json`, SDK generation
and backup verification need a separate bounded cleanup. Parent #14 remains open;
this slice does not complete M1 or M3. PR #39 and lease repair are excluded.

Validation results and limitations will be recorded in docs/IMPLEMENTATION.md.
Exact-head Linux CI, current-main integration and independent review must be supplied
by the controller before merge; this checkout task does not publish or merge.

## Existing instance artifacts

Startup preserves promoted management artifacts. After updating source, explicitly
run `pnpm moria apps recover management` to replace an older console that still
contains Snow controls. No artifact or data is deleted automatically. Retired
routes fail even when an old console remains. This is a source change, not an
installation migration or permission to operate a live instance.
