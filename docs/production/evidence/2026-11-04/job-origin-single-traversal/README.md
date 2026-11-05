# One accepted-delegation traversal per current credential check

A current private credential previously traversed the same accepted delegation in
`runtime_job_session_origin`, then again through `runtime_execution_check` and
`runtime_binding_authority`. The session resolver now projects the identical
immutable delegation payload and returns it only after the complete execution
validation has checked that delegation. Current capability intersection still
occurs in `native_serving_accepted_delegation`; worker registration, generation,
lease, fence, incarnation, binding, service purpose, parent and source checks stay
in place.

A domain failure (`raise_exception`, SQLSTATE P0001) reruns the original
delegation guard before rethrowing, preserving service/parent revocation error
precedence over generic execution epoch errors. Serialization, deadlock and
query-cancel errors are not intercepted. The validation helpers read and lock
state; they do not persist mutations. There is no authorization cache, proof skip
flag, new public function signature, or deadline change.

## Evidence

- On the previous artifacts, the new real PostgreSQL counter regression failed
  with exactly two traversals where one is required. The other authority cases
  remained active; this was an expected red gate.
- The corrected source passed all 12 authority tests, with zero skips or
  cancellations, in 11.17 seconds. The new case also compares the complete
  returned origin payload to the existing delegation guard. All original
  service-disable generation pulses, grants/clearance, actor/application/instance
  reapproval, worker expiry, fence takeover, descendant scope and shared-ancestor
  lock tests remain unchanged and passed.
- Complete `pnpm check:static` and `pnpm check:indexing` passed. All nine native
  executables match the exact frozen source/schema; all 77 native units passed
  with zero ignored tests. These are debug builds.
- Owned controllers, proof-labelled containers, scratch pools and temporary
  paths were verified absent after both red and green authority runs.

The focused real gate was:

```sh
pnpm test:integration tests/native-job-authority.integration.test.ts
```

The motivating service Automation diagnostic observed 2,462 filesystem ACL
resolutions (5.57 seconds of self time), 239 private-session origin resolutions,
and 1,797 service-authority resolutions during the original ten-second child
admission window. That profile identified duplicate work; it did not establish a
lock wait or qualify Automation completion. The adjacent evidence qualifies this
shared credential correction. Automation, Evaluation, Pipeline, complete-platform
integration and production capacity still require their own passing gates.
