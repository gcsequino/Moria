# Combined v24 native qualification

The exact frozen source, derived schema, compiled schema, executable hashes and commands are in `manifest.json`, `source-manifest.json` and the phase logs.

TypeScript and all 77 native library tests passed. Eight integration files ran 55 tests: 53 passed, two failed (one revocation subtest and its parent), zero skipped, in 451.135 seconds. The revocation subtest expected `FULL_SOURCE_PURPOSE_REVOKED` and observed `AUTHORIZATION_CHANGED` from an independent current Model guard. The original assertion stopped before the remaining provider-abort and visibility checks; this run does not qualify those checks. A separately archived rerun keeps both current guard codes explicit and retains the socket-abort, unchanged coverage and unchanged visible-head assertions.

This is a failed combined qualification, not a full-platform acceptance result.
