# Moria development handoff

Resume from the tip of `codex/durable-development-gates` in [draft PR #81](https://github.com/gcsequino/Moria/pull/81). The same tip is mirrored at `codex/implementation-checkpoint-20261104`. The `codex/moria-handoff-20261106` ref retains the local experiment histories and stash. Fetch it before checking out any archived commit below. The [archive manifest](production/evidence/2026-11-06/checkpoint-refs.tsv) maps original refs to exact commits. The archive preserves the checkpoint tree and uses parent links to retain the other histories. Use the draft PR for integration work.

## Verified locally

- The bounded Pipeline build passed 10/10 and provenance passed 7/7. The 64-input/64-output subtest took 20.335 seconds including assertions under its 30-second deadline. Grouped publication processed 18,000 rows and 37,400,280 bytes, and committed two outputs with one receipt in 60.268 seconds. Readback, replay, and spill cleanup passed. [Pipeline evidence](production/evidence/2026-11-05/linux-pipeline-admission.md)
- Both composed browser tests and all seven native recovery tests passed on `77a50db` with matching local binaries. Browser readback found three objects and exactly one top-level Function receipt with value 42. The suite also checked live Source revocation. The earlier release build passed all seven native recovery tests, and the managed indexing suite passed 23/23 including typed Iceberg and retained snapshot readback after another publication. [Implementation evidence](IMPLEMENTATION.md)
- The Function child admission retry passed 18/18 Function integration tests, 126 native unit tests, and static checks. A held live admission reproduced a PostgreSQL conflict when the parent Agent lease renewed with the same fence. The original build withheld the callback and recorded zero child executions and receipts. The fixed build committed exactly one child, one Function receipt, and one Agent receipt. [Reproduction](production/evidence/2026-11-06/function-child-admission/README.md)
- The runner gives Catalog, Evaluation, and Pipeline dedicated time and keeps the remaining files at a maximum of two in parallel. Selector tests and a deliberate first-group failure verified complete scheduling and exit status propagation. The Catalog counter correction passed its 21 isolated checks. Both Evaluation scheduler fixtures passed after waiting for actual worker startup before sending SIGTERM.

## Hosted results and next gate

Hosted run `42144771702` tested `77a50db` before the runner isolation and Function retry. Static passed. Linux reported 1,340 passes, 22 failures, one cancellation, and zero skips out of 1,363 tests. The 64-output case passed in 21.838 seconds. The 64×64 request returned HTTP 504 at 30.009 seconds, and grouped publication reached the 120-second deadline before seal. Treat the following receipt and readback failures as dependent until reproduced. The Agent LangGraph case returned `FUNCTION_OBSERVATION_INCOMPLETE`. Evaluation still missed its 90-second barriers. Dataset v3 replacement and the tail of Connector custody also failed. [Hosted run](https://github.com/gcsequino/Moria/actions/runs/42144771702)

The focused hosted run `42142993828` passed 19/32. Evaluation timed out and Catalog missed two preparation barriers when run alongside it. The earlier intermittent `FUNCTION_WORKER_ACCOUNTING` error did not recur, so its cause remains open. [Focused diagnostic](https://github.com/gcsequino/Moria/actions/runs/42142993828)

1. Check the run attached to the current PR tip. Keep the original 30-second Pipeline admission, 120-second grouped execution, and 90-second Evaluation fixture barriers. Start with the isolated Catalog, Evaluation, and Pipeline results.
2. Recheck the original Evaluation service and lost acknowledgement cases after the Function retry. A prior optimized isolated run still exceeded 90 seconds with the real five intermittent conflicts, four persistent conflicts, exact resume, and held parent lock verified. The combined candidate below still timed out. Preserve the exact case and receipt checks while reducing load.
3. If the isolated hosted 64×64 still misses, profile admission under the existing environment and filesystem locks. The remaining repeated work includes registered-service ACL and state checks in `resolve_dataset_pin_registered_in` after the Pipeline has already checked service labels. Any batching must retain the exact current grants, provenance and namespace guards, and pin epochs. Private requests and graph overlap retain their earlier path.
4. Diagnose the v3 and Connector failures independently if they recur. Complete the full independent Linux job on the final proposed revision, followed by the explicit human review required by `AGENTS.md` for authority, migration, publication, retention, and recovery.

Issues [#82](https://github.com/gcsequino/Moria/issues/82), [#83](https://github.com/gcsequino/Moria/issues/83), and [#84](https://github.com/gcsequino/Moria/issues/84) track Linux, Pipeline, and browser/recovery. Production follow-ups include [live sparse Dataset writes #85](https://github.com/gcsequino/Moria/issues/85), [incremental Source and Search #86](https://github.com/gcsequino/Moria/issues/86), and [Action-origin Connector writeback #87](https://github.com/gcsequino/Moria/issues/87). The production workload and qualification remain in [#1](https://github.com/gcsequino/Moria/issues/1), [#20](https://github.com/gcsequino/Moria/issues/20), and [#11](https://github.com/gcsequino/Moria/issues/11).

## Archived experiments

The following commits are preserved for inspection and selective integration. Evaluation task/session reuse passed the held Agent group and 19 Job Authority checks, including lock retention and revocation. The combined release experiment still missed the service deadline. Shared case binding verified one binding and ten distinct cases, tasks, and delegations. The fixture-only completion observer improved progress from four to five completed cases by the same deadline. Its failure cleanup is still unqualified. Source intervals and Connector writeback remain unfinished.

| Work | Commit |
| --- | --- |
| Function child admission retry | [`589df89`](https://github.com/gcsequino/Moria/commit/589df89abb1bee089e9e70a4e137426324633257) |
| Evaluation combined release experiment | [`e4c0867`](https://github.com/gcsequino/Moria/commit/e4c0867470d0642765b8d11d2c538161448fa307) |
| Evaluation fixture polling and cleanup | [`4c23044`](https://github.com/gcsequino/Moria/commit/4c230442453417f499ebf86ad1ff0d3639c67dd8) |
| Evaluation case binding experiment | [`9d61f06`](https://github.com/gcsequino/Moria/commit/9d61f064cb4385e60615dab9b8a25c1ef91926a8) |
| Earlier Evaluation replay fixture | [`4a5d005`](https://github.com/gcsequino/Moria/commit/4a5d00580a2963bcc326e557b5bbc4b3c9f79668) |
| Sandbox accounting diagnostics | [`2a4967e`](https://github.com/gcsequino/Moria/commit/2a4967ee09b18b27c1b75160e1a383c4e053c37f) |
| Source row intervals | [`a463968`](https://github.com/gcsequino/Moria/commit/a46396864fb3eaefa997e32abd893ef4ae7bd530) |
| Connector Action writeback | [`b2818e3`](https://github.com/gcsequino/Moria/commit/b2818e3260ef3a2895ff2c45fa66b173df100a66) |
| Grouped authority stash | [`a467f62`](https://github.com/gcsequino/Moria/commit/a467f629d8bf8bc33789da0fc5b383d645791186) |

## Local workflow

Use the persistent Ubuntu-24.04 checkout at `/home/gcseq/moria-current-publish-20261104` when resuming on the same machine. Create a worktree for each candidate, keep small commits, and keep the checkpoint branch current. Build the canonical Linux image from the exact source tree. Use matching native binaries throughout and use the release profile for timing comparisons.

```bash
docker build -f deploy/test/Dockerfile -t moria-test:handoff .
sudo python3 deploy/test/run-quality.py moria-test:handoff pnpm test:integration tests/native-evaluations.integration.test.ts
sudo python3 deploy/test/run-quality.py moria-test:handoff pnpm test:integration tests/native-pipelines.integration.test.ts
sudo python3 deploy/test/run-quality.py moria-test:handoff
```

Use one local build at a time and give timed runs the host to themselves. The runner owns five scratch pools and one temporary controller per run. The local proof records are in `/home/gcseq/moria-proofs` on the original machine. The checked-in evidence and linked GitHub logs carry the results needed from a fresh clone. Run the browser proof with the composed Source fixture and a local model. Verify each created record and the Function receipt before releasing the fixture's continuation marker. Resume AWS work only within its separate authorized test scope and billing instructions. This handoff used local and GitHub-hosted tests.
