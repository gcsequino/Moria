# Delivery operator reconciliation

The native `delivery reconcile` command uses the operator schema lease.
PostgreSQL independently requires the operator login. The current uncertain
fence is locked while evidence is appended and the delivery is resolved;
resuming dispatch also checks current destination authority. Worker logins
cannot insert reconciliation evidence or change the status, fence, or existence
of an uncertain delivery directly. Reconciliation history is append-only.

The old binary and SQL were tested with the existing real HTTP and PostgreSQL
fixture plus one negative assertion. The non-owning worker reconciled the current
uncertain fence successfully, so the expected rejection failed. The old image
included unrelated source differences, but its delivery implementation, CLI
routing, SQL and original fixture were byte-identical to the pre-change versions
at `8fc66a64b18fb6111a9dbe0c6b3a2443cd94e1c2`. Ten tests passed and the focused
assertion and its parent failed; no cases were cancelled or skipped.

The fixed frozen source passed `pnpm check:static` and `pnpm check:indexing` with
all nine native artifacts and 120 native unit tests, zero ignored. The complete
`tests/native-delivery.integration.test.ts` file passed **12/12** with the
original 180-second root limit, zero skips or cancellations (10.324 seconds
including controller setup and cleanup). It used the actual non-owning worker
and a separate operator login only for reconciliation. It checked worker CLI,
SQL and direct DML denials; stale fence rejection; immutable evidence; current
revocation rollback; both operator outcomes; real HTTP response loss, SIGKILL,
cancellation, and same-payload retries. The red and green controllers, proof
containers, scratch pools and temporary paths were verified absent afterward.

- Fixed image: `sha256:6791149b2ca32b1b6a5f883132c178054f2d01463b8ba3895c09e5a01982ad6d`.
- Native source digest: `cb4bc2d61b63c6a87bd3d6f23743837e271213d429cc34f97abc42b651c60646`.
- Old comparator image with the single fixture assertion: `sha256:df4e41283b0fe0cb3d87eda9920816f2f5a40ab8b688ab6da76567a7a0180a4e`.

This gate qualifies the delivery reconciliation boundary. Connector writeback,
production throughput, and restored dispatch are separate qualification gates.
