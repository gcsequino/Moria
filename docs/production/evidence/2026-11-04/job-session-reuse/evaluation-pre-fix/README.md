# Evaluation first-case lock diagnostic

The frozen native/schema source remains the unchanged pre-fix workflow image.
The TypeScript overlay adds current PostgreSQL activity/lock sampling and maps
backend client ports to actual native worker socket descriptors. Seven later
subcases explicitly skip. All first-case assertions, concurrency, operation
waits and native lock timeouts remain unchanged. Worker handles stop only in
first-case cleanup before restoring its Source declarations.

Result: the first subcase and its root fail; seven explicit diagnostic skips;
no cancellation. The Evaluation worker exits with RESOURCE_BUSY. One Evaluation
backend repeatedly waits in runtime_job_session_issue for roughly 12.8 seconds
on current descendant/ancestor authorization readers. Another backend in the
same Evaluation process subsequently waits behind that issuer while trying to
record EVALUATION_FAILED; it reaches the unchanged lock timeout and the worker
exits. Trial003 separately captured the preparing-lease renewal also waiting
behind the issue statement, before EVALUATION_FAILED/55P03 was recorded.

Exact credential reissue unnecessarily upgrades current read validation to an
execution UPDATE lock. The proposed narrow correction recovers only the exact
immutable execution/task/owner/fence/host/incarnation credential and runs the
existing full current private-session origin guard. First issuance keeps its
exclusive lease fence. The red held-reader regression reproduces the problem
independently of model/container timing. Post-fix full-suite proof is separate.

Full raw logs and source overlays remain here. lock-analysis.json and
blocking-excerpts.json retain bounded causal excerpts, process mapping, hashes
and explicit limitations. Trial001 was an instrumentation-name parse error;
trial002 lacked the explicit catalog binary environment path. Trial003's root
name filter unexpectedly ran later cases and was stopped; those later errors
must not be used as first-case qualification.
