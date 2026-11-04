# Current private-session reuse and authority qualification

This full unfiltered native-job-authority integration file runs on the exact
fixed native/schema image in manifest.json. There are no source overlays.

All 11 tests pass with zero skips and cancellations in 13.030 seconds. Actual
PostgreSQL parent and child execution SHARE locks remain held by a separate
connection. First credential creation still requires its exclusive fence and
is denied under those held readers using a test-local 500ms timeout. Existing
exact credentials can be revalidated and reused with the same readers held.
Both ordinary task and null-task publication leases are exercised.

Current service purpose, grants, actor clearance, worker registration/scope,
missed disable pulses, lease expiry and takeover remain fenced. Expired and
replaced exact lease tuples cannot recover their old credential. This is not
an authority cache or a lock-timeout increase.

The previous immutable native/schema image fails the held-reader reuse
regression; its separate red diagnostic is job-session-reuse-red-20261104.
Commands and owned-resource absence are recorded here. Full Evaluation and
Automation composition remain separate proofs.
