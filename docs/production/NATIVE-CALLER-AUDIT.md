# Native caller and retained control-plane audit

October 31, 2026. This records the supported process/caller paths against N01–N28;
it does not certify capacity or replace the complete integration gate.

| Program area | Default operational path                                                                                | Retained TypeScript boundary                                                                                                                                                                                                                                                                                  |
| ------------ | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| N01–N03      | Native HTTP/RPC authentication, current SQL authority and managed-cell fences                           | Bootstrap, bounded security administration and OIDC verification use the same guarded current SQL kernels. Accepted-human refresh requires actual IdP evidence and never extends ordinary expiry.                                                                                                             |
| N04–N06      | Native reads, typed query/projection, actions and transactional change frames                           | SDK/CLI request serialization and shared compiler/reference conformance. No Node ObjectService is constructed by the default native launcher.                                                                                                                                                                 |
| N07–N11      | Native storage scopes, snapshot verification/publication, pipeline coordination and indexing roles      | Trusted declarations and operator metadata; user engine programs run in the accepted isolated OCI adapter. Legacy Node catalog/index services remain compatibility fixtures, not the default bulk path.                                                                                                       |
| N12–N13      | Native projector, embedding gateway and protected pgvector retrieval                                    | Browser/CLI clients only. No Node search or per-request subprocess bridge.                                                                                                                                                                                                                                    |
| N14–N17      | Native scheduler, automation factory, execution, sandbox broker and delivery roles                      | The launcher may replace itself with the native worker executable. Guest JavaScript/Python is published user code, not host authority.                                                                                                                                                                        |
| N18–N19      | Native history, Dataset, backup and filesystem reclamation/recovery commands                            | Operator orchestration and declaration/CLI serialization. Physical deletion remains provider-qualified and requires its own authority.                                                                                                                                                                        |
| N20–N22      | Native HTTP admission, readiness/drain, filesystem bytes/metadata, Source validation and publication    | The local launcher owns processes, migrations and the optional development database. The bounded OIDC listener handles navigation and verification, never forwards operational requests. CLI/browser compilation is bounded; native publication independently validates the actual immutable source/artifact. |
| N23–N27      | Native Model, serving projection, Agent/Evaluation and MCP domains                                      | UI/SDK clients and accepted OCI programs; inference/SQL engines remain integrated dependencies.                                                                                                                                                                                                               |
| N28          | Native default API, scheduler and explicitly configured worker domains; native protected static hosting | React, generated SDKs, compiler, CLI, bootstrap, bounded administration/OIDC and separate static-only preview.                                                                                                                                                                                                |

`apps/local/src/runtime.ts` selects `runNativeProcess` before constructing its
legacy workspace, ObjectService, Functions or Jobs. `MORIA_LOCAL_RUNTIME=legacy`
and a custom legacy execution factory remain explicit development/test
compatibility paths; production rejects them. API/scheduler/supervisor/worker
entrypoints share that selection. The native launcher starts `serve`,
`execution scheduler`, and only the finite domains in `MORIA_NATIVE_WORKERS`.
The explicit `automation` domain maps to `automations worker`. Missing native
configuration fails startup; it does not fall back to Node execution.

`apps/cli` operational Function/Agent/Evaluation/Pipeline/filesystem commands use
authenticated persistent HTTP. Worker commands use `execve` before creating a
workspace or pool. Artifact publish/promote/recover is trusted operator filesystem
configuration. Static-only custom-app preview uses a separate origin and grants
no management cookies or API access. The bundled Snow command is retired.

The management React entry now uses native authentication, filesystem/Source,
resource-specific operations and `/api/management/v1/runtime`. It no longer calls
the retired demo workspace/assets/jobs/weather endpoints. Published definitions
come from the current filesystem resource resolver; independent data/invoke
permissions are still required. Function, Agent, Evaluation and Pipeline views
use the existing native lifecycle clients. Object/action/link/model/search forms
call their native routes and retain an idempotency key for the same canonical
request. Errors clear previously returned data. Legacy demo UI modules remain
outside this entry for compatibility tests; no native endpoint fabricates their
old data shape.

## Evidence and remaining acceptance

The composed Source/private frontend/SDK and both default launcher modes passed
7/7 real cases in immutable source `20261031T065101356371Z` (86.14 seconds).
The native filesystem upload/multipart collector passed its exact-provider fault
case in immutable source `20261031T073038927146Z` (12.81 seconds), including 17
actual lost Initiate acknowledgements, separate operator permission, a full-grace
fixture, retained history and exact neighboring-ID protection. POSIX physical
removal remains explicitly unsupported; no unconditional fallback is allowed.

Additional operational history branches passed 3/3 cases against that exact
native artifact with separately archived TypeScript fixture/helper changes:
`evidence/2026-10-31/native-history/branches-external-20261031T074802Z`. Five actual
native projection generations and native Link actions produced the data; a
separately labeled structural Search registration tested collector confinement
without fabricating a live worker lease or claiming a Search lifecycle run.

Independent Source applications require durable immutable origin ownership;
same-origin cookies or frontend digest checks alone are not JavaScript isolation.
The canonical origin/reservation/restart and native browser/auth/bootstrap
regressions passed 9/9 in immutable source `20261031T103716307840Z`, including
external-script recovery retry without an inline/eval CSP exception. Provider,
application and principal disable pulses permanently revoke old browser/workload
credentials. The focused real IdP/queued-human-origin proof passed 3/3 in
`native-browser/auth-revocation-20261031T091705Z`. The November 5 composed fixture passed its backend checks and timed out waiting
for interactive browser verification. [Current evidence](../IMPLEMENTATION.md#native-integration-and-production-goals--november-5-2026)

Ordinary scalar and typed single/multi-target overlay Actions now prepare under
shared admission and recheck their complete observed footprint before short
exclusive publication. The 26-case Source/serving proof is recorded in
`native-serving/native-action-preparation-20261031T105001Z`. Create/link and
Function-prepared proposals retain their existing transaction path.

Full latest-revision checks, the complete Model/Search/Agent/Evaluation/Pipeline/
Automation browser topology and production workload qualification remain separate
gates. The v8 million-object exact oracle passed; at 20 requests/second all
600 arrivals succeeded with 318 ms overall p95, while 50 and 100 requests/second
still exceeded admission capacity. Those measurements precede the ordinary
Action publication change and do not certify the production profile.

Source Application scope entries are requested capabilities. They cannot grant
permissions by publication. Actual authority is the intersection of the current
operator-approved application scopes and principal grants. An operator may
independently approve an exact resource operation after the resource exists; the
Source declaration is not represented here as an additional permanent scope ceiling.
The whole-instance fixture uses the actual published browser Application UUID for
Agent admission and fresh browser recovery. Future Function/Model operations must
first fail before their independent operator approvals. Independent application
origins remain permanently reserved to their environment/application tuple.

The default local launcher selects native HTTP, scheduler and explicitly configured
native domain workers. Production startup rejects the retained legacy runtime and
custom Node execution factories. Retained Node Source/administration, migration,
OIDC, compiler, CLI, SDK and React paths do not proxy operational native requests.
Legacy ObjectStore overlay writes remain test/development compatibility and are not
evidence that production incremental Source publication has a safe writer lock order.
Current ordinary native Actions acquire sorted permanent UID homes before environment
admission; broker, create, Link and attachment paths still require conversion before
incremental Source publication can replace bounded try-admission with fair waiting.
