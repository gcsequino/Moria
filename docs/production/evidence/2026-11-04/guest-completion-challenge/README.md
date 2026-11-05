# Guest completion challenge

A real cold baseline accepted a guest that wrote a result and immediately exited
before consuming `complete`. Checking that its process was alive and successfully
writing to its input pipe did not prove consumption of that control message.

The native supervisor and existing TypeScript Docker backend now generate an
unpredictable UUID challenge after current ownership and CPU validation. A guest
must flush one exact `completed` acknowledgement containing that challenge before
successful exit. A preemitted acknowledgement cannot predict the challenge.
Early, absent, wrong, duplicate and trailing replies fail closed. Final broker
authorization, typed result validation, cleanup and lease checks still precede
publication; all existing resource and frame bounds remain in effect.

The shared Node Function/Agent runner and Python engine runner implement this
control handshake. Source value version 1 is unchanged. Older immutable guest
images without the acknowledgement fail closed until rebuilt and redeployed.
There is no legacy acceptance fallback. See the [sandbox contract](../../../contracts/sandbox.md).

## Checkpoint validation

- Base: native Docker API checkpoint `582aeff08518b63771507e91980f6b505d6fdd76`.
- Frozen native digest: `7988218af28168cca79ecfb6254047beafc034f04368543caf50f30b76515dc6`.
- All nine native debug artifacts compiled in 30.30 seconds with two jobs,
  two CPUs and 4 GiB memory. `check:indexing` verified matching provenance and
  all 79 native unit tests passed, with no ignored or skipped cases.
- Full `check:static` passed in 28.87 seconds with one CPU and 3 GiB memory.
- The actual Node guest test passed its successful challenge/flush flow and four
  malformed completion challenges; two test results passed with no skips.
- The original premature-exit OCI assertion remains. Its fixture now also checks
  missing, early, preemitted, wrong, duplicate and trailing acknowledgements and a
  correct acknowledgement followed by nonzero exit. The read-only root positive
  case acknowledges its actual challenge.

## Subsequent real execution

The frozen combined image `sha256:c391776c90e69d561048bbfd1a680621447c89d0b5c42dcea9f3983b0c3266fa`
ran against actual Docker 28.0.4, API 1.48, in a separate network namespace:

- The full native sandbox file passed 9/9 test results without skips in 45.91
  seconds including controller setup/cleanup. The existing read-only root positive
  case and all nine malformed/early/absent/wrong/duplicate/trailing/nonzero
  completion modes passed their original assertions, including premature exit.
- The existing TypeScript Docker compatibility file passed 1/1 without skips in
  10.52 seconds, including successful use of the shared runner and real CPU-budget
  exhaustion. No accounting limit was weakened.
- The actual canonical Python runner and protocol, using matching locked
  DuckDB/PyArrow packages, passed five direct control cases: valid challenge
  acknowledged before exit; absent, malformed, boolean-version and extra-field
  challenges rejected without acknowledgement. Each guest was removed.
- The Python proof uses a metadata-only pipe broker and a no-op transform. It
  proves runner control transport, not native Dataset IO, admission, publication
  or full engine integration. Its transcripts and scope are in
  `actual-python-control/`.
- Standard fresh scratch provisioning and owned controller/registry/guest cleanup
  remained active. Every controller, temporary directory and scratch pool was
  verified absent afterward. The private daemon was empty after both native/TS
  files. Native execution transcripts and exact helper socket adaptation are in
  `../native-docker-api/actual-docker28/`.

No complete platform gate, independent CI pass or production capacity is claimed.

After testing, the owned private Docker daemon and containerd were stopped and
their private network namespace removed. Existing host bridge identity and
existing container processes were unchanged. Local caches and proof files remain.
