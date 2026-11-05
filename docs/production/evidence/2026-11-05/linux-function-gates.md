# Linux Function gate corrections

The Docker compatibility failure occurred after valid API 1.44 accounting.
The diagnostic reproduced the CI result on a real Docker daemon. The 1 ms
invocation recorded 90.739 ms of CPU usage and two removal
operations before the worker call returned. One removal succeeded and the other
returned a Docker operation error, which replaced the CPU-limit error at the
Function boundary. The early stop and final cleanup paths could run concurrently. A single
per-invocation removal promise now serves the early stop and final cleanup
paths. The Functions service retains its existing cleanup retry. The same
diagnostic passed with the expected CPU-limit failure and no competing
removal. The canonical compatibility test also requires an accounting probe
for each invocation and passed locally in 10.757 seconds. [Linux CI log](https://github.com/gcsequino/Moria/actions/runs/42078685523/job/127426500382)

The native lease test used sandbox startup as a barrier for an assertion about
an interrupted callback. A running sandbox can have no pending observation.
The test now waits for the holding guest's log to commit and its observation to
clear, then revokes the exact worker scope. It requires a terminal failure
with no result or receipt and verified sandbox cleanup. The existing test
that interrupts a private callback still requires a 409 response with no
protected failure details or logs.

The local proofs used byte-identical native inputs (152 files) and database
source (46 files) from `e232e1146d37e54799ff85b5bce6981f98381cef`. The fixture
overlays reused the existing nine native artifacts. Static and indexing checks
passed with 126 native unit tests. The full native Functions integration passed
18 of 18 tests with its original root deadline. The Docker proof preserved all
worker deadlines and resource limits. The owned controllers, containers,
scratch pools and temporary paths were absent after each gate.
