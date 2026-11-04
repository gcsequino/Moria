# November 4 checkpoint validation

Source was recovered from the immutable local `moria-test:gate-v27` image recorded
in `recovery.json`. It contains all files from the Windows mirror and later
shared changes. Twelve obsolete `services/indexing` crate files inherited from
the base image were removed according to the mirror's intentional tracked
deletions. Further changes are limited to checkpoint documentation, validation evidence,
and Git attributes that preserve raw evidence bytes.

The initial `/work` dependency-symlink harness attempts failed before checks:
pnpm correctly refused to replace a modules directory outside its project root.
The retries and errors are retained; no check was disabled. Verification then
ran offline inside the original frozen image at `/app`, with its matching locked
dependencies and exact compiled artifact source hashes.

- Formatting, architecture, SDK drift, TypeScript and artifact checks passed.
- All 77 native unit tests passed, with zero ignored cases.
- Python Black and specification validation passed. The subsequent workload
  validator failed: it still requires `numericTargetsApproved: false` and the
  old proposed status, while workload v0.2 records approved implementation
  targets. `approvalScope` and `approvedOn` are also not accepted by that schema.
  Later Python checks did not run.
- Both React production builds passed in a separate command. The starter check
  failed because `examples/station-explorer/src/generated/runtime.ts` is stale.
- The full integration suite was not rerun for this preservation checkpoint.
  There is no recovered terminal result for the interrupted whole-program gate.

This snapshot is unfinished and is not a release or production certification.
