# Native environment directory contract v1

`EnvironmentDirectory` runs in the native runtime. Trusted operators use
`moria-runtime directory place|inspect|freeze|activate`; authenticated internal
callers use the generated `EnvironmentDirectory` gRPC service. The transport
requires mutual TLS and the current `environment-router` or `environment-admin`
peer role. Endpoint references resolve through operator configuration and never
carry credentials or caller-selected URLs.

A placement is `(environment, cell, fence, incarnation, serving, leasesFenced)`.
Initial placement is paused. Admission takes a PostgreSQL shared lock on the
current placement and cell, checks the trusted connection's `moria.cell_id`, and
requires an enabled cell, current incarnation, serving state and fenced leases.
The routing availability function is a scheduling hint; it is never authority.
Authority outages deny operational release and commit.

Transfer commits the paused destination and increments the fence before fencing
old execution, indexing and delivery leases and recording uncertain model calls.
Fencing runs in a separate transaction so placement and attempt lock ordering
cannot form a cycle. Activation requires that exact destination/fence/incarnation
and completed fencing. A lost freeze acknowledgment leaves the environment
paused. Retrying the original CAS reconciles its stored transfer tuple without
creating another ownership move. Returning to an earlier cell does not revive
old leases.

The current backend shares one PostgreSQL authority. Real two-process and mutual
TLS tests cover transfer, an admitted-transaction barrier, lost COMMIT reply and
a database network partition. This does not establish independent cell failure
domains or availability during PostgreSQL failure.
