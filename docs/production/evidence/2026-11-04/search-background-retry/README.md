# Background Search retry admission

A background initial rebuild that failed during a control-plane authority change could be retried at the same environment revision with the same internal request key. The unique rebuild key rejected that retry with SQLSTATE 23505 (`DUPLICATE_UID`), stopping the Search worker and the combined local instance. Background rebuild keys now include the actual new generation UUID. Caller-supplied public idempotency keys are unchanged.

The regression admits a real background build, disables its accepted model Secret approval, verifies failure without provider dispatch, reapproves and admits exactly one fresh background generation at the same Dataset revision. A second disable closes the retry before the original public credential-revocation, replay, embedding reuse, expired-worker takeover, model replacement and live-purpose revocation cases run.

The original native artifact reproduced `DUPLICATE_UID`. The final full file passed all eight reported cases, zero failures/cancellations/skips, in 109.761 seconds. Static checks, all nine exact native artifacts and 100 native unit tests passed. The final TypeScript-only fixture overlay leaves all native inputs unchanged; hashes and immutable image IDs are retained here.

Two intermediate fixture failures are retained in the external proof: a preclaim rejection can schedule another attempt in the same worker cycle, and the old operator cancellation helper does not support this background binding. The final regression uses actual approval revocation and reapproval instead. No production deadline, approval check or fixture width changed.

This is functional Search qualification. The complete local-instance rerun and production-scale capacity gates remain open.
