# Development repair benchmark

Five synthetic regressions exercise real Moria implementations and existing
regression tests at baseline `42491b01e691802becbc4fbf57eb706f84e487b9`:
calendar dates, signed integer overflow, IPv6 database hosts, connection budgets,
and pagination verdict precedence. `tasks.json` is the evaluator's mutation catalog.
This is an open-book smoke benchmark, not a hidden test set or a broad quality score.

Run the harness from a separate evaluator checkout in Linux. Each task gets a new
detached worktree outside that checkout. No active delivery workspace is reused.

```sh
python3 benchmarks/development/run.py prepare database-ipv6 /tmp/moria-ipv6-trial1
cd /tmp/moria-ipv6-trial1
bash .openhands/setup.sh
pnpm exec tsx --test tests/database-config.test.ts
# Expected: the injected regression fails. Now start a fresh coding session here.
```

Give the session only the printed task prompt and normal repository context.
Allow edits to the named implementation and new root-level `tests/*.test.ts`
regression tests; existing tests,
dependencies and gate scripts must remain intact. Do not give it the mutation
catalog or reference patch. Git history is visible, so record if the session uses
history to recover the solution. Setup time is excluded from task timing.

Grade from the evaluator checkout after the repair:

```sh
python3 benchmarks/development/run.py grade database-ipv6 /tmp/moria-ipv6-trial1
```

The grader rejects changes outside scope, runs the original focused tests and then
the full quality gate. It handles committed and uncommitted tracked changes. Run
it only on a disposable non-root Linux development environment. A zero exit code
is necessary but still requires independent review of the actual patch, added
tests and any skipped checks. The grader executes candidate code, not a sandbox.
Preparation records the injected starting implementation in worktree Git metadata.
Repair line counts compare against that state; new-test lines are counted separately.
The baseline diff is for scope inspection, not a measurement of repair effort.

Repeat for the other task IDs in `tasks.json`. Use a fresh worktree and conversation
for each run. Keep the baseline, model, reasoning setting, tools and time budget
fixed. Run at least three paired trials per task, alternating configuration order.
For a Ponytail comparison use explicit off versus full mode and verify the loaded
instructions in each session. Record separately if the current workflow skills
are supplied: the historical baseline predates them. Change only one factor per
comparison. Stop at the agreed time/token budget and record unfinished work.

Record task, trial, baseline, configuration/plugin versions, model/settings,
elapsed task time, available token/cost measurements, pass/fail, independent review
findings, changed lines/files, and unnecessary changes. Leave unavailable cost or
usage fields null; do not estimate them from response length. Keep raw logs and
patches in ignored `artifacts/development-benchmark/` or an external evidence folder.
Report per-task pass counts before comparing time or diff size among passing runs.
Do not label this as a measured improvement until paired trials have been run.

Before changing a baseline/mutation, show the baseline focused test passes, the
mutant fails on the intended assertion (not infrastructure), and restoring the
implementation passes. Keep baseline tests unchanged. Remove disposable worktrees
with `git worktree remove` only after preserving required evidence and checking
their resolved paths; never automatically force-remove user work.
