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
- Real OCI execution and Python engine integration are pending at this checkpoint.
  This is not a complete platform gate, independent CI pass or capacity proof.
