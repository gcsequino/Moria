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
