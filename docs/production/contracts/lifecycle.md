# Native serving lifecycle

HTTP serving and internal gRPC have a finite graceful shutdown period. SIGINT or
SIGTERM stops admission, asks the listener to drain, and waits at most the trusted
`MORIA_NATIVE_DRAIN_SECONDS` value (default 30, range 1–300). A stalled drain exits
with `NATIVE_DRAIN_TIMEOUT`; it does not report successful completion of an
unfinished request. Accepted asynchronous jobs retain their separate execution,
lease, deadline and reconciliation rules. Listener termination is not proof that
an external effect did not happen.

The HTTP front door exposes two small, credential-free GET/HEAD probes under its
normal trusted host/origin/HTTPS-proxy checks. `/livez` reports process liveness;
`/readyz` additionally checks current schema admission, restore dispatch gates,
configured cell registration/incarnation and local drain state within two seconds.
This is role readiness. Individual Source resources additionally require their
actual generation, model capability, frontend digest, worker and other dependency
readiness checks; a healthy listener does not make them ready.

`GET /api/management/v1/runtime` requires current principal and application
`instance:manage`, including delegation restrictions and the final authority
check. It reports bounded process-local request counts, response byte counts,
latency buckets and available request/compiler slots. Counters are decimal
strings, reset with the process and are point observations rather than a billing
or transaction ledger. Streaming observations finish on body EOF or record an
aborted response when dropped; they cannot imply rollback of committed effects.
No paths, request contents, credentials or model text enter these metrics.

HTTP role budgets remain 256 accepted sockets, 64 shared requests, 32 model
requests and four compiler requests. Socket permits last for the connection;
request permits last through response streaming. Connections with no read or
write progress expire after 60 seconds, including stalled pre-header clients.
Per-operation body, execution and provider deadlines remain independent. Database
pools and declared replica counts must fit the configured aggregate budget;
normal runtime roles remain non-owning and non-administrative.

Configured public HTTPS origins receive HSTS on every native response, including
assets, authentication, models, errors and probes. Trusted proxy HTTPS validation
and application-bound cookie/origin checks remain separate requirements. These
lifecycle changes have focused unit coverage and actual listener evidence: all
nine admission cases passed in the v10 combined run, including current readiness,
protected metrics, disabled approval across restart, and an admitted unfinished
body that exits at the configured two-second drain deadline. The combined run
was not green because its separate indexing harness executable path was absent.
Raw results and source/artifact identities are retained in
`../evidence/2026-10-30/native-recovery/20261031T051111Z-test.txt` and its JSON
record. This does not establish worker-role readiness, SLOs or capacity.
