# Moria implementation

Follow docs/spec for accepted contracts. Restricted views are terminal: Transform inputs must resolve to Dataset resources. Iceberg owns the dataset table format. Never replace real transactional, authorization or isolation tests with mock-only claims.

Use local model inference for real-model smoke tests; never use paid model APIs by default. Credentials belong in environment variables or external credential stores, never repository fixtures or logs.

Run pnpm check for TypeScript changes. Keep progress and deviations in docs/IMPLEMENTATION.md. Runtime tests requiring unavailable infrastructure must be reported as unverified.

## Development environment and code map

Linux is the sole supported development and target hosting OS. Windows hosts must
use a Linux container or WSL2 Linux filesystem; native Windows is unsupported.
Use Node 24, pnpm 11.25.0, and Python 3.11 with a repository-local `.venv`.
Follow README development setup; the complete gate is `pnpm check` for every PR.
Linux requires a non-root user and the libraries listed in deploy/test/Dockerfile.
OpenHands setup uses `.openhands/setup.sh` after provisioning those prerequisites;
its completion hook runs the same gate. Never skip database tests to obtain a pass.

`apps` contains entry points and React applications; `packages` contains contracts,
authorization, compilation, and SDK generation; `services` implements stateful
runtime behavior; `workers/iceberg` contains Python data processing. Tests and
isolated fixtures live in `tests`. Regenerate SDK files with `pnpm sdk:generate`.
Read docs/production/ACCEPTANCE.md for production obligations and docs/spec for
existing semantic contracts. Proposed workload numbers are not approved targets.

## Bounded delivery

Work on a branch against explicit acceptance criteria. Record scope, dependencies,
commands, results, and limitations in each PR. Keep production credentials and
live instance directories outside development workspaces. Use synthetic fixtures
for fault/load experiments. Do not migrate, deploy, or restore a live installation
as part of a code task. Changes to authorization, migration ownership, publication,
retention, and recovery require explicit human review of the affected invariants.

The Linux quality job must pass on the latest PR revision before human review and merge.
Do not merge automatically or push directly to main. Local hooks provide feedback;
they are not a security boundary or substitute for independent CI. Never weaken an
acceptance criterion or disable checks to complete a task. If blocked, report the
failing command and required infrastructure or decision.

Use the configured human Git identity. Do not add development-assistance credits,
co-author trailers, generated-by notices, or session links to repository content.

Keep source readable and formatter-clean. Run `pnpm format` for TypeScript/JSON/YAML; `pnpm check` enforces that formatting. Use Black for Python (`python -m black workers benchmarks docs/spec/schemas`, configured in pyproject.toml), gofmt for Go, and rustfmt for Rust. Do not compress statements or control flow onto one line. Preserve raw benchmark evidence in docs/benchmarks without reformatting it.

When interacting with Cloudflare, use the cf CLI unless the project has a Wrangler configuration file.

## Verification and review workflows

Repository workflows live in `.agents/skills/`. Use `moria-verify` when validating
a change, `moria-review` for an independent review, and `moria-ui-check` for browser
behavior changes. Tools without skill discovery should read the matching SKILL.md.
The existing OpenHands setup and quality hook remain the executable gate.

Prefer the smallest complete change and reuse existing code before adding a new
dependency. Optional minimalism plugins do not override readability, contracts,
security checks, or the full quality gate.

For meaningful changes, obtain an independent review of the complete diff and
relevant callers/tests. Record the reviewed revision; a changed revision needs
renewed review. A truncated diff or confidence score is not a review verdict.
Use the existing independent reviewer when the delivery controller owns the task;
do not create a second job or edit its active checkout.

Review rules: restricted views cannot become Transform inputs; authorization must
use current authority at protected boundaries; publication must preserve fencing
and atomicity; errors must not disclose credential values. Report a concrete
trigger, affected location and consequence for each finding. Keep mechanical
formatting and generated-file checks in `pnpm check`.
