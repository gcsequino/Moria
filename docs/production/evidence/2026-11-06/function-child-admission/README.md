# Function child admission during Agent lease renewal

The nested Function takes a Repeatable Read snapshot before it resolves the live
parent Agent task. The fixture holds that resolution and uses the native
`execution renew` command to update the same live task with its owner and fence
intact. The old admission aborted with `CONCURRENT_MODIFICATION`. The Agent
retained its pending, blocked observation and published zero children and receipts.

The retry uses the same parent, key, and input in a fresh transaction after
PostgreSQL reports a serialization failure or deadlock. The fixed run advanced
the same lease, cleared the observation, and committed exactly one child, one
Function receipt, and one Agent receipt. Both witness runs passed their expected
assertions. Static checks, 126 native unit tests, and the full 18-test Function
file passed on the matching local build. The local proof used unoptimized native
artifacts.

Run from the repository root on an isolated Linux Docker host. Build
`moria-test:base` using `deploy/test/Dockerfile` from `77a50db` for the original
outcome or from the revision containing `589df89` for the fixed outcome.

```bash
proof_dir=$(mktemp -d)
git show 0103728:tests/native-evaluations.integration.test.ts |
  python3 docs/production/evidence/2026-11-06/function-child-admission/prepare.py   > "$proof_dir/probe.ts"
cat > "$proof_dir/Dockerfile" <<'DOCKERFILE'
FROM moria-test:base
COPY --chown=node:node probe.ts /app/tests/native-evaluations.integration.test.ts
DOCKERFILE
docker build --network none -t moria-test:function-admission-probe "$proof_dir"
sudo python3 deploy/test/run-quality.py moria-test:function-admission-probe   env MORIA_FUNCTION_ADMIT_RACE_FIXED=1   pnpm test:integration tests/native-evaluations.integration.test.ts
```

Use `MORIA_FUNCTION_ADMIT_RACE_FIXED=0` on the original revision. The
`function_admission_race` event records the exact outcome and receipt counts.
The persisted nested Function test also injects a single rolled-back child
admission and checks that only one child commits.
