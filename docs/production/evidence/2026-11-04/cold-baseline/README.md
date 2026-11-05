# Cold release baseline and CI budget

The frozen `54d839e47fbfb24a915dd0ccf8b8c7b02f457fd2` snapshot builds all nine
release artifacts, passes repeatable offline bootstrap twice, static checks and
77 native units. Its complete functional suite fails: 1,196 passed, 67 failed,
zero skipped or cancelled, 1,263 results in 1,965.45 seconds. Failures include
parent results; they are not 67 independent defects. This snapshot precedes the
private-session and ACL corrections. This is not capacity certification.

Independent CI run 41906573977 on `4e2b6617c4158ed8a3de6df2d84e1ca281917184`
passes static checks, build and bootstrap, but its Linux job reaches the overall
45-minute watchdog before the complete suite finishes. Preparation takes
26m24s; the baseline suite alone takes 32m45s. The CI job budget is now 90 minutes
and the completion-hook budget is 60 minutes. Per-operation and individual test
deadlines, limits and assertions are unchanged.

Native build preparation now requests the library and six integration harnesses
in one Cargo invocation. Cargo emits both normal executables for the integration
targets; the exact nine executable artifacts are selected from its JSON output.
The selection was checked against actual emitted artifacts and all nine hashes
match. This avoids repeated feature-graph switches between separate build/test
invocations. A new cold release timing has not been measured.

`summary.json` records source/archive identity, terminal AWS/CI status and all
failed result labels. `failures.json` records the detailed errors and source
locations. Raw cloud logs are retained at the private result prefix recorded in
the summary. The existing runner is stopped; no automatic restart is scheduled.
