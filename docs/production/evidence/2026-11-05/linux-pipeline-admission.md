# Linux Pipeline admission at 64 inputs and 64 outputs

The requester-batching baseline returned HTTP 504 after 30.007 seconds in
the 64-input and 64-output test. The same test accepted the complete
profile and pinned all input heads in 25.487 seconds after the provenance
batching change in `c977846`. The 64-output case went from 17.523 to
14.969 seconds. Both isolated runs used the original 30-second HTTP
deadline and real Dataset heads. The maximum-width case measures admission
through the trusted execution binding before guest code starts.

The precheck authorizes inputs and outputs in order under the current
service authority. An idempotent replay checks the current graph once.
New edges still require a second check of the updated graph in the same
transaction. The provenance suite passed all seven tests, including
revocation on replay with the admitted edges and security epoch intact.

The full Pipeline integration passed all nine subtests and the parent.
The execution cases checked grouped publication of two outputs from
18,000 input rows, SDK and CLI receipt replay, exact Parquet readback, and
hard spill exhaustion followed by scratch reuse. The run completed in
509.313 seconds of test time. Its controller, five scratch pools, and
temporary directory were removed. Static checks and 126 native unit
tests passed against the same production inputs.

Local Linux proof image digest

```text
sha256:ddf51c84cd6e858654f59a9a7ee4a7c1ccb99e536b30640d61dcd09bcb7c14a5
```

## Follow-up after hosted Linux

Hosted revision `3f24ad7` passed 64-output admission in 27.094 seconds.
The 64-input and 64-output case still returned HTTP 504 at 30 seconds.
[Hosted Linux result](https://github.com/gcsequino/Moria/actions/runs/42111707604)

Revision `33a2266` checks paired service permissions against one current
protected resource read and reuses the output declaration within admission.
The requester read batch covers both inputs and outputs. Input namespace
and physical pin checks still run against the current state. The live
grant regression verified that removing the output write grant denied the
pair while preserving the authorized read.

The corrected local run passed all ten tests including the parent. The
64-output and 64-input/64-output subtests took 11.466 and 22.663 seconds,
including their assertions and cancellation. Grouped publication passed
the exact 120-second database deadline, and SDK and CLI replay, independent
Parquet readback, and scratch reuse passed. The whole test took 597.142
seconds. All scratch pools and the controller were removed. The first
trial of this production code missed the grouped publication barrier, so
the local pass still needs independent hosted confirmation.

The run used nine matching native artifacts and passed static checks,
126 native unit tests, and seven provenance tests. The final image reused
the native artifacts after a test-only correction.

```text
sha256:e589bbe233625d61b5b17cc8d9f81ea2713ca6078c8a5201d357fd03270c4e0b
```
