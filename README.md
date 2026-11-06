# Moria

An open-source, self-hosted data and ontology platform. Named for a dwarven civilization of foundries: delve too greedily and too deep, and awaken a Balrog.

The default Linux launcher runs the native management API, bundled React console and scheduler. Use [the local instance guide](apps/local/README.md) to start a fresh workspace and sign in. The [implementation checkpoint](docs/IMPLEMENTATION.md#native-integration-and-production-goals--november-5-2026) records which workflows passed and which release gates remain open.

Default startup hosts management only. The bundled Snow/weather application is retired. [Retirement scope](docs/production/SNOW-RETIREMENT.md)

## Development

The [platform filesystem](docs/FILESYSTEM.md) organizes human and agent homes,
group projects, ontologies and code repositories with inherited access, verified
versioned files and checkout/push/publication. Open **Files** in the management
console or run `pnpm moria fs home`. This branch requires a fresh schema version 5 database.

The [Rust runtime program](docs/production/RUST-PLATFORM-PROGRAM.md) records the
component inventory, contracts, verification evidence and remaining integration.
The shared crate is in `services/runtime`; runtime roles execute independently.
The [Rust indexing service](services/indexing/README.md) captures durable
dataset-to-object work and runs concurrent, fenced executors independently of
the API. Its first supported input is copy-on-write Iceberg on shared POSIX storage.

[TypeScript functions](docs/FUNCTIONS.md) connect repository exports to private,
versioned invocations through API/SDK/CLI and the Files console. An explicit
function worker owns container execution; API processes do not control Docker.
Docker is the current injected backend. Stronger production isolation remains part of the [sandbox contract](docs/production/contracts/sandbox.md).

The [shared value model](docs/spec/core-contracts.md#c03--values-rows-and-schema-evolution)
defines exact scalar encodings, stored scalar arrays, structured API/function
envelopes, schema compatibility and versioned typed fingerprints. TypeScript and
Python codecs share conformance cases; generated SDKs include runtime decoding.

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

Keep the authoritative checkout in a persistent Linux directory, for example
`~/src/Moria`; `/tmp` is for disposable test state. Commit one bounded change
with its focused validation, then push the working branch before starting the
next component. Keep credentials and running-instance state outside Git.

Use smaller gates while working:

```sh
pnpm check:static
pnpm check:indexing
pnpm test:local
pnpm test:integration tests/native-functions.integration.test.ts
pnpm check
```

`check:static` catches formatting, contract/SDK drift, TypeScript, Python,
workload validation, application builds and the starter smoke test before
expensive integration runs. `test:local` selects files without the integration
suffix, including some embedded PostgreSQL tests. `test:integration` accepts
explicit existing integration files; omit the arguments to run them all.
The final `check` still runs every test with the same two-file concurrency and
original test deadlines. Smaller gates do not replace the full gate. A focused
Docker integration run uses the same fresh scratch, controller ownership and
cleanup as the full run:

```sh
sudo python3 deploy/test/run-quality.py moria-test:ci pnpm test:integration tests/native-functions.integration.test.ts
```

Use the commands in the [Linux quality workflow](.github/workflows/quality.yml)
to build the test image, preload pinned function images and run the offline gate.
On the disposable Docker host, run `sudo python3 deploy/test/run-quality.py moria-test:ci`
after building the image and preloading its pinned prerequisites. This provisions
separate two-slot ext4 scratch pools for Pipeline, whole-instance, Connector
custody, Connector discovery, and the discovery restart. It passes the exact devices
and host paths to the non-root quality container and removes only those owned
resources after the controller stops. Separate pools preserve permanent instance
ownership markers. The quality container receives
the daemon socket; bootstrap and function sandboxes receive no socket. This is
test packaging, not a production deployment.

`pnpm check` runs formatting, architecture ownership, SDK drift and TypeScript checks, runtime tests,
Python formatting, specification/workload validation, both bundled application
builds, and the Station Explorer type/build/hosting smoke checks. Python commands
use `.venv` or the explicit `MORIA_PYTHON` executable. Specification validation
uses `--check` to avoid rewriting the historical report.

The Quality workflow first runs the Static quality job from the `static-check`
Docker stage, which does not compile Rust. Linux quality starts only after that
passes, then builds the native artifacts and runs the complete offline gate.
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
Hooks do not enforce GitHub permissions. Use the [roadmap](https://github.com/gcsequino/Moria/issues/1) and [current checkpoint](docs/IMPLEMENTATION.md#native-integration-and-production-goals--november-5-2026) to choose a bounded issue. Authorization, migrations, publication, retention and recovery require review against their invariants before merge.

### Formatting

Run `pnpm format` to format TypeScript, JSON and YAML. `pnpm check` includes
`pnpm format:check`, so formatting regressions fail the normal checks. Prettier
is pinned in the lockfile; `.editorconfig` supplies matching editor defaults.
Raw benchmark evidence under `docs/benchmarks` is excluded.

For Python, install the development formatter with
`python -m pip install -r requirements-dev.txt`, then run
`python -m black workers benchmarks docs/spec/schemas scripts deploy/test` (or add `--check`).
Use the virtual environment's Python executable. The Go and Rust benchmark
sources use `gofmt` and `cargo fmt --manifest-path benchmarks/ontology/rust/Cargo.toml`.

Dataset format: Apache Iceberg with Parquet files. PostgreSQL is the transactional authority. Restricted views are terminal read resources and may back object types, but cannot be transform inputs. React applications and agents use generated OSDK contracts.

Licensed under Apache-2.0; see [LICENSE](LICENSE). Dependency/model licenses are separate. No third-party model, credential, dataset or binary is checked into this repository.

Google sign-in for management: run `pnpm moria auth setup` and follow [the auth guide](docs/AUTH.md). `pnpm moria auth recover` provides temporary local access independently of Google.

Management uses the same Google provider with explicit administrator invitations: `pnpm moria auth grant-admin --email "you@gmail.com"`. Register the exact callback printed by `pnpm moria auth setup`. The native default uses port 3211. [Authentication guide](docs/AUTH.md) `pnpm moria auth recover --app management` retains independent local recovery.

Public HTTPS deployment and restart instructions: [PUBLIC-HOSTING.md](docs/PUBLIC-HOSTING.md).

Generate a typed, scoped client for hosted apps with the [OSDK workflow](docs/OSDK.md). Management and retained historical fixtures use it; `pnpm sdk:generate` updates their clients and `pnpm check` detects stale output.

The management console includes Files and Source, native resource forms, and runtime status. See [administration](docs/ADMIN-CONSOLE.md), [trusted platform execution](docs/EXECUTION.md), and the [native caller audit](docs/production/NATIVE-CALLER-AUDIT.md). Configure required workers explicitly.

For custom-app development, see the runnable [Station Explorer starter](examples/station-explorer/README.md). For backup scope, an isolated restore drill and current relocation limitations, see [BACKUP-RESTORE.md](docs/BACKUP-RESTORE.md).

## Production roadmap

The [production contract](docs/production/README.md) and [GitHub goal](https://github.com/gcsequino/Moria/issues/1) track the long-term portable deployment architecture. The approved workload targets and their evidence requirements are in the [workload contract](docs/production/WORKLOADS.md).
