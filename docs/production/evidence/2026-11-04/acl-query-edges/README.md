# Filesystem permission graph checkpoint

The filesystem permission resolver now appends per-entry dependency edges with
`UNION ALL`. The enclosing recursive `UNION` still deduplicates the reachable set
and terminates cycles. This removes eight repeated edge hash aggregates while
retaining live credentials, source proof, markings, deny precedence, inactive
resource checks, and the existing 4,096-target bound. No authorization cache or
runtime deadline changed.

The real Pipeline fixture also assigns separate Dataset outputs to its Fanout and
Maximum producers. The previous shared outputs violated the compiler's single
producer contract. All 67 physical inputs, the 3-to-64 and 64-to-64 Transform
widths, 199 Dataset definitions, and 194 granted operations remain exercised.

The Agent fixture now closes its already acquired PostgreSQL fixture if image
acquisition fails, then rethrows the original error. A controlled acquisition
failure exercised all three real database setup paths and exited with the
expected three failed roots in 7.20 seconds, without hanging or leaving owned
resources behind. This negative probe is a cleanup check, not a product pass.

## Verification

- Complete `pnpm check:static` passed.
- All nine native executables match the frozen schema/source; all 77 native units
  passed with zero ignored tests. These are debug artifacts.
- Full `tests/filesystem-source.integration.test.ts` passed 19/19 with zero skips
  in 22.07 seconds. The new real database regression covers duplicate parent and
  provenance edges, a provenance cycle, inherited live markings, and a later
  explicit deny. Existing current/pinned source proof and privilege checks remain.
- The Agent cleanup overlay passed formatting, type checking, native provenance,
  and the same 77 native units.
- Three real dependency geometries produced identical reachable sets, labels,
  required entries, and source-proof decisions before and after the SQL change.
  The sampled leaf plan decreased from 20.356 ms to 2.782 ms; JIT was unavailable.
  A diagnostic completed the first 16-head producer group within its unchanged
  120-second deadline. That diagnostic did not run the Pipeline acceptance cases.

The full affected-file command was:

```sh
pnpm test:integration tests/native-pipelines.integration.test.ts \
  tests/native-evaluations.integration.test.ts \
  tests/native-agents.integration.test.ts tests/native-functions.integration.test.ts
```

It completed all four files with 46/52 reported tests passing, six failures
including parent wrappers, zero skips/cancellations, and 546.02 seconds of test
runtime. All 17 Function subcases and their root passed, including permanently
lost COMMIT acknowledgement handling. Agent passed 20/21 subcases. Evaluation's
initial ten-case run passed, while repeat comparison and service dispatch still
failed. Pipeline fixture seeding expired after 14 of the first 16 candidates,
before any Pipeline acceptance subcase. These failures remain unresolved in this
checkpoint; the accepted deadlines and assertions are unchanged.

An earlier attempt failed when the host Docker bridge disappeared. Only its
owned controller was stopped, and all owned pools and temporary paths were
removed. The host bridge was restored and a fresh real registry request succeeded
before the complete affected-file retry above. The environmental attempt is not
included in the passing counts. Raw attempts and immutable source manifests are
retained outside the checkout; the adjacent files record artifact identities,
measured geometry, test summaries, and verified cleanup.

This qualifies the SQL equivalence and focused permission behavior. It does not
qualify the complete platform, release build performance, production throughput,
HA, recovery targets, or cost targets.
