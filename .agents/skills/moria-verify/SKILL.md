---
name: moria-verify
description: Validate Moria changes using its supported Linux environment, targeted regression checks, and complete quality gate.
---

# Moria verification

Read root AGENTS.md and package.json. Use Linux, Node 24, pnpm 11.25.0,
Python 3.11 and a non-root user. On Windows, run in a Linux container or
WSL Linux filesystem. Keep live instance data outside the checkout.

For an existing provisioned workspace, run `bash .openhands/setup.sh` only when
dependencies need installation. It uses the lockfile and Python constraints.
For a clean reproducible check from the repository root:

```sh
docker build -f deploy/test/Dockerfile -t moria-test:verification .
docker run --rm moria-test:verification bash -c 'bash .openhands/setup.sh && bash .openhands/setup.sh'
docker run --rm --network none moria-test:verification bash .openhands/hooks/quality-gate.sh
```

Use a unique image tag for concurrent checkouts. The image runs as non-root.
Build and bootstrap require network; the final gate deliberately runs offline.
These are the same operations as `.github/workflows/quality.yml`.

During iteration select existing affected tests, for example
`pnpm exec tsx --test tests/database-config.test.ts`. Consult imports/callers to
select coverage; filenames alone are insufficient. For a bug, demonstrate that
the regression check fails before the fix and passes after it when practical.
Run `pnpm sdk:generate` when generated contracts change, then inspect the diff.

Before handoff, run the full `pnpm check` or the container gate above. Preserve
real database, Iceberg and S3 integration checks. Never replace them with mocks
or skipped tests to make the gate pass. UI behavior also needs `moria-ui-check`.

Report source revision and any uncommitted diff, exact commands, exit results,
test skips, infrastructure limitations and log paths. Build success does not
establish browser behavior. A local pass does not establish exact-head CI success
or authorize merge/deployment.
