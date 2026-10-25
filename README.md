# Moria

An open-source, self-hosted data and ontology platform. Named for a dwarven civilization of foundries: delve too greedily and too deep, and awaken a Balrog.

**Status: runnable local asset workspace and SNOTEL React app; broader platform and production deployment remain unfinished.** The accepted design is in [docs/spec/README.md](docs/spec/README.md); current evidence and next steps are in [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md).

Run `pnpm start` after installing the development prerequisites below. Open the printed local URL to import CSV/JSON into Iceberg, browse assets with server-enforced administrator permissions, and save property overrides that survive refreshes and restarts. PostgreSQL starts automatically and persists with the warehouse in `.local/workspace/`. See the [local workspace guide](apps/local/README.md) for the complete walkthrough, input format, and boundaries.

Implemented so far: validate/compile CLI, contextual policy checking, real PostgreSQL/Iceberg integration, transactional publication, permanent UIDs, versioned source/overlay storage, current security authority, protected point reads/actions, cursor storage and a local model gateway. A narrow workload HTTP server factory adds short-lived bearer sessions; human OIDC login is implemented for the bundled apps; production deployment is pending. Windows and non-root Linux tests cover process crashes, concurrency and authorization. Persistence/administration methods remain trusted internal interfaces. Performance evidence and remaining bottlenecks are in [docs/PERFORMANCE.md](docs/PERFORMANCE.md).

The bundled [Snow Observatory](docs/SNOTEL.md) browses stations and charts snow depth/SWE from NRCS data ingested into Moria. Start the instance, run `pnpm moria snotel sync`, then `pnpm moria snotel open` for its separate read-only access link.

## Development

For a visual tour of the implemented components, read the [technical diagrams](docs/TECHNICAL-DIAGRAMS.md): architecture, protected queries, actions, Iceberg indexing, and object state.

Linux is the supported development and target hosting OS. Use Node 24,
pnpm 11.25.0, and Python 3.11. On Windows hosts, develop inside a Linux container
or WSL2 Linux filesystem; native Windows development is unsupported.

```sh
pnpm install --frozen-lockfile
python3 -m venv .venv
.venv/bin/python -m pip install -r workers/iceberg/requirements.txt -r requirements-dev.txt -r docs/spec/schemas/requirements.txt
pnpm check
pnpm moria validate examples/maintenance.json --attributes examples/principal-attributes.json --json
```

Run the Linux test harness with `docker build -f deploy/test/Dockerfile -t moria-test:local .` followed by `docker run --rm --network none moria-test:local`. This is test packaging, not a production deployment.

`pnpm check` runs formatting, SDK drift checks, TypeScript checks, runtime tests,
Python formatting, specification/workload validation, both bundled application
builds, and the Station Explorer type/build/hosting smoke checks. Python commands
use `.venv` or the explicit `MORIA_PYTHON` executable. Specification validation
uses `--check` to avoid rewriting the historical report.

The Quality workflow runs on pull requests and main with the Linux quality job.
Inspect that job and its logs on the latest revision before merging. Required
checks and review protection must be configured separately when the repository's
GitHub plan supports them; until then, the owner enforces this review procedure.

### OpenHands pilot

Use an isolated non-root Linux workspace with Node 24, pnpm 11.25.0, Python 3.11,
and the OS libraries in `deploy/test/Dockerfile`. The repository setup script
installs locked JavaScript and constrained Python dependencies; network access is
needed for installation. The Stop hook runs `pnpm check` with a 30-minute limit.
The Linux CI job exercises setup twice and the quality hook with networking disabled.
An actual OpenHands session remains a separate integration check.

Start with one bounded task and submit a PR for human review. Do not supply live
instance directories, production credentials, or merge/deployment authority.
Hooks do not enforce GitHub permissions. Suggested next tasks, in dependency order:

- PERF-01 slice: parse the versioned workload manifest and reject unsupported
  capabilities explicitly; verify invalid profiles with deterministic fixtures.
- PERF-01 slice: implement scheduled-arrival accounting with injected saturation
  and timeouts; prove offered work is never silently omitted from results.
- M1-01 slice: extract validated database configuration with redacted errors;
  preserve the embedded development launcher and add external-DB regression tests.

These are slices of issues #20 and #13, not permission to close either parent issue.
Authorization, migrations, publication, retention, and recovery changes require
review against their specific invariants before merge.

### Formatting

Reusable verification, independent review, browser checks and configuration
comparisons are documented in [development workflows](docs/development/WORKFLOWS.md).

Run `pnpm format` to format TypeScript, JSON and YAML. `pnpm check` includes
`pnpm format:check`, so formatting regressions fail the normal checks. Prettier
is pinned in the lockfile; `.editorconfig` supplies matching editor defaults.
Raw benchmark evidence under `docs/benchmarks` is excluded.

For Python, install the development formatter with
`python -m pip install -r requirements-dev.txt`, then run
`python -m black workers benchmarks docs/spec/schemas` (or add `--check`).
Use the virtual environment's Python executable. The Go and Rust benchmark
sources use `gofmt` and `cargo fmt --manifest-path benchmarks/ontology/rust/Cargo.toml`.

Dataset format: Apache Iceberg with Parquet files. PostgreSQL is the planned transactional authority. Restricted views are terminal read resources and may back object types, but cannot be transform inputs. React applications and agents will share generated OSDK contracts.

Licensed under Apache-2.0; see [LICENSE](LICENSE). Dependency/model licenses are separate. No third-party model, credential, dataset or binary is checked into this repository.

Google sign-in for Snow Observatory: run `pnpm moria auth setup` and follow [the auth guide](docs/AUTH.md). `pnpm moria auth recover` provides temporary local access independently of Google.

Management uses the same Google provider with explicit administrator invitations: `pnpm moria auth grant-admin --email "you@gmail.com"`. Register the additional port-3210 callback described in [the auth guide](docs/AUTH.md). `pnpm moria auth recover --app management` retains independent local recovery.

Public HTTPS deployment and restart instructions: [PUBLIC-HOSTING.md](docs/PUBLIC-HOSTING.md).

Generate a typed, scoped client for hosted apps with the [OSDK workflow](docs/OSDK.md). Both bundled React apps use it; `pnpm sdk:generate` updates their clients and `pnpm check` detects stale output.

The management console now includes execution controls and lineage. See [administration](docs/ADMIN-CONSOLE.md), [durable execution and Front Range conditions](docs/EXECUTION.md), and [data/workflow lineage](docs/LINEAGE.md). Snow refreshes and NWS forecasts can run on persisted schedules; operator CLI recovery remains available.

For custom-app development, see the runnable [Station Explorer starter](examples/station-explorer/README.md). For backup scope, an isolated restore drill and current relocation limitations, see [BACKUP-RESTORE.md](docs/BACKUP-RESTORE.md).

## Production roadmap

The [production contract](docs/production/README.md) and [GitHub goal](https://github.com/gcsequino/Moria/issues/1) track the long-term portable deployment architecture. Proposed calibration targets are not production certifications. See the [source baseline record](docs/SOURCE-BASELINE.md) for this snapshot's scope and validation.
