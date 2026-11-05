# Per-call administrator capability evaluation

Filesystem ACL evaluation now calculates the current administrator grant/scope conjunction once per call and uses it while evaluating each applicable grant. The input state is immutable within this invocation. Every later call recomputes it; policy geometry, required-source reads, inherited markings, deny precedence and live revocation checks are unchanged.

The actual PostgreSQL regression installs an administrator-only root policy and a512-entry capability padding set. It checks absent, grant-only, scope-only and complete administrator authority, then removes the grant and verifies immediate denial. Permissions and policy are restored in finally. All21 filesystem/Source cases passed, including duplicate/cyclic provenance, mixed-operation geometry, error ordering and concurrent policy-snapshot tests. Static quality, all nine native artifacts and109 native units passed.

This qualifies security behavior. The existing first16 producer fixture remains the performance comparison; no throughput or latency improvement is claimed before that measurement.
