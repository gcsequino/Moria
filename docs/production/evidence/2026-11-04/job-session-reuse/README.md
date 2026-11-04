# Current private job credential reuse

A real Evaluation trace maps the controller's `runtime_job_session_issue` to
long waits behind child Source authorization readers. The controller's own lease
renewal/failure handling then waits behind that issuance. First-case-only
pre-fix tracing is diagnostic (seven explicit other-case skips), not qualification.
Its bounded lock analysis, overlay, command and raw-log hash are retained here;
the large raw log remains in the persistent external proof directory.

The SQL function now first looks up its exact immutable lease tuple. Reuse calls
`runtime_job_session_origin` to revalidate current incarnation, worker scope and
generation, task/execution fence and lease, accepted delegation and current parent
purpose. First creation keeps the existing exclusive execution fence. Proposed
credential collision checks remain; no lock timeout or deadline is increased.

The new held-parent-and-child shared-lock regression fails on the old schema at
existing credential reuse. The rebuilt runtime passes the full authority file:
11/11 cases with zero skips in 13.03 seconds, including both task and publication
credentials, expected first-issuance fencing and stale/expired/disabled rejection.
Controller and owned pool cleanup are independently confirmed.

The frozen native rebuild and all nine artifacts have exact source provenance;
Rust formatting, complete static validation and all 77 native units pass.
Broader Function/Pipeline/Search/Model/recovery and Evaluation/Automation proofs
are separate runs and remain in progress at this checkpoint.
