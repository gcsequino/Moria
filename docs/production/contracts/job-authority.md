# Accepted job authority

An ordinary browser or workload credential authorizes acceptance. Its immutable execution context retains that original hash and actor. A bounded accepted job may outlive the ordinary credential's expiry; this does not extend the credential's public lifetime.

`runtime_job_delegations` records the accepted environment/principal/application, original ordinary origin, exact granted operations, application scopes and clearance, plus a bounded parent chain. Acceptance verifies these fields against the actual live authenticated state in the same transaction. The rows are immutable. Current authority always intersects the accepted sets with current sets; a later grant cannot broaden an already accepted job. Attributes, instance bindings, principal/application enablement, explicit session disable, provider revision/freshness, parent cancellation and current policy remain authoritative. Only ordinary expiry is treated differently for the accepted profile.

`runtime_workers` records the trusted owner, host, incarnation, domain and resource classes. A monotonic generation advances on enablement or scope changes, expiry shortening, and renewal after an actual expiry. Registered claims capture that generation in the task or publication lease and its private credential. An unobserved disable/restore or expired registration cannot revive an earlier attempt. Registry triggers do not update attempt rows. The Function compatibility view preserves its existing columns and remains updatable. A worker cannot register broader capabilities through a task request. Release/operator composition provisions the registration; startup requires it.

`execution::job_session` issues an internal `VerifiedSession` only from an exact live registered task or publication lease. Its hashed credential is bound to execution, task, owner, increasing fence, host and incarnation, and expires no later than the execution deadline. Every private lookup locks and checks the current lease, registration and parent control rows. A stale fence, changed host/class, cancellation, deadline or incarnation prevents further use. Public bearer authentication accepts ordinary browser/workload origins only. Neither credential hashes nor provider secrets enter guest messages, result bodies or logs.

The sandbox's active private hash is separate from its original accepted context. Pending Function/Agent work first checks current accepted authority, claims the exact lease and obtains its own private capability. The required broker `start` hook then checks current filesystem visibility, typed input/read evidence and markings before any Docker creation or user code. A rejected preflight records an explicit `noCodeStart` outcome; it cannot become a transparent retry of started code. Generic native jobs keep their existing strict epoch requirements.

A readWrite filesystem operation holds environment/filesystem exclusive admission. Each actual security mutation tentatively advances the function's acknowledged epochs inside that same transaction before subsequent private-profile authority checks. It rechecks the exact lease, old anchor, current actor/source visibility and complete classification. Failure rolls back the mutation, acknowledgement and audit together. This capability is private; there is no guest epoch acknowledgement or generic epoch bypass.

Other durable domains with their own real lease tables may provide an independently fenced private-origin proof to the same current-origin/capability normalizer. They must retain their actual build/task/deadline authority and must not manufacture generic execution rows. Registered service-origin delegation is a separate explicit profile; absence of an ordinary session never implicitly grants service authority.

## Registered services

An operator-approved `runtime_service_authorities` row fixes the workload principal/application, exact operations, marking ceiling, domains, resource classes and instance incarnation. `runtime_service_targets` fixes each approved environment/resource/domain/class and immutable binding revision. `runtime_execution_services` associates an immutable execution binding with that exact registration. Only enablement may change; changing purpose requires a new registration or revision.

Changing service or target enablement advances the environment security epoch in the same transaction. Restoring enablement permits newly admitted work; it cannot refresh the immutable authority of previously accepted work. Pipeline and delivery destination enablement uses the same revocation rule. Operational dispatch pause remains a separate control.

The private `jobs::RegisteredService` constructor loads and rechecks this registration. It can resolve current filesystem/resource labels, native Dataset snapshot metadata and model capability/destination metadata. It is not a session and cannot authorize data reads or inference. Its binding helper derives the service actor from the registration and records the separately verified requester for audit. Public request JSON supplies neither actor nor service proof.

An accepted service execution has `origin_kind=service`, no ordinary origin hash, and an exact registered root binding. Actual registered worker claims remain necessary before obtaining a private `VerifiedSession`. Every use rechecks current workload identity, application, permissions, clearance, enabled target, pinned target revision, host/class/domain, fence and incarnation. Public bearer authentication never accepts the private hash. Service descendants in this profile stay within the root's exact environment/resource/domain/class and accepted capabilities. Dispatch to separately registered descendant targets requires explicit composition rather than widening the inherited actor.

The [combined native proof](../../benchmarks/native-service-profile-20261030-trial-003-green.tap) passed 42 tests with no skips, including registration without an ordinary session, pre-lease issuance rejection, wrong host/fence, exact-target descendants, revocation, takeover, expired ordinary sessions, real Agent SDK/CLI execution and Function regressions. The [trial ledger](../../benchmarks/native-service-profile-20261030-trials.json) retains the preceding failed trials and their corrections.

## Bounded human identity verification

The retained OIDC control plane may refresh identity evidence for actual active accepted
human jobs while the UI is closed. `BrowserSessions.startAcceptedRefresh` selects at most
16 stale bound-origin sessions per 10-second tick using indexed session/delegation keys,
with one active batch and a per-origin advisory verifier lock. It derives the origin from
immutable accepted execution metadata and the current live delegation guard; no caller
supplies an actor or session hash. Actual IdP UserInfo verification happens outside
environment/filesystem locks. The same original subject, email, provider revision,
encrypted token, enabled principal/application/provider and live accepted origin are
checked again under admission before a compare-and-set updates only identity freshness.
Ordinary session expiry is never extended, and no credential is minted by this path.
Unknown errors, stale/cancelled jobs or operator revocation leave freshness unchanged.
Accepted origin rows survive ordinary session cleanup while their delegation is retained.

Native browser UI refresh uses a same-host/same-scheme bounded OIDC iframe with an exact
configured callback origin and native cookie target origin. Only the actual OIDC listener
verifies provider evidence; its acknowledgment is not authentication. The UI validates the
message origin and iframe source and then asks native serving for the current session.
The iframe CSP names the exact native frame ancestor and uses an external script without
`unsafe-eval`. Business APIs and workers remain native; this is retained TS authentication
control-plane work.
