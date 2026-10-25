# Development workflows

The repository supplies three focused workflows under `.agents/skills`: verification,
independent review and browser checks. They share the existing Linux environment
and `pnpm check`; they do not introduce an alternate quality gate. Codex discovers
these repository skills. Other coding tools can read the matching SKILL.md via
the root AGENTS.md routing instructions.

## Environment

Use the pinned `deploy/test/Dockerfile` for a clean environment. It provisions
Node 24, pnpm 11.25.0, Python, database libraries and the checksum-verified S3 test
server. `.openhands/setup.sh` installs dependencies in a provisioned non-root Linux
workspace. CI verifies that setup can run twice and that the gate works offline.
See `.agents/skills/moria-verify/SKILL.md` for the exact commands.

Keep each concurrent change in its own checkout. Existing delivery workers and
their reviewer retain their assigned scopes; these workflows do not authorize
interrupting jobs, changing controller policy or merging changes.

## Optional minimalism plugin

Ponytail can complement these workflows. Use its normal/full setting initially;
Moria's contracts, readable formatting and verification requirements still apply.
The plugin is a user-tool installation, not a runtime dependency. Install it in
each development environment that should use it; a Windows installation does not
configure WSL or OpenHands automatically. Review its hooks in the tool's trust UI
and start a new session after installation. Do not copy its instructions into
every repository prompt or enable additional telemetry integrations by default.

## Comparing configurations

Use [the repair benchmark](../../benchmarks/development/README.md) to compare the
current configuration with one change at a time, initially Ponytail off versus
full. Keep model, reasoning setting, tools, baseline and time limit identical.
Correctness comes first; fewer changed lines are useful only after required
behavior and checks pass. The suite is a small synthetic regression benchmark,
not proof of general development quality or production readiness.
