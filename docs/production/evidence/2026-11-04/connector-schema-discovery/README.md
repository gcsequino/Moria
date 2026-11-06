# Native Connector schema discovery

The native `schemaDiscovery` capability proposes typed metadata through the
existing admitted Connection and fixed credential transport. The proposal never
updates a Dataset binding, head or checkpoint. Normal replace/full `batchRead`
retains its original publication path.

The frozen component build is based on `0118a645`, with all nine executables built
from its recorded native inputs. `pnpm check:static` and `pnpm check:indexing`
pass; all 113 native units pass with none ignored. Four units cover typed schema
and ordered-key validation, malformed/oversized proposals, and executable
capability/profile combinations. [qualification.json](qualification.json) records
artifact and archive fingerprints and persistent raw evidence locations.

The integration file uses real PostgreSQL, Source publication, a registered
native worker, an OCI Python guest and deterministic HTTP peers. It checks exact
caller/service/Secret authority, current Source-file denial, SDK and CLI results
without Dataset scope, immutable stream selection, cancellation, in-flight
revocation, transport narrowing, malformed/oversized proposals and reflected
credential denial. The same actor/Connection/request key is exercised for both
discovery and normal batch publication, with independent typed Dataset readback.
The lost-acknowledgement test holds the actual PostgreSQL COMMIT response and
checks one sealed report, one guest/provider attempt, exact retained replay and
unchanged Dataset/checkpoint state.

The first live run exposed a test restart collision: scratch registration correctly
rejected reuse of the same UUIDs. The corrected caller registers a second fresh
pool supplied by the existing driver. The second run passed the actual lost-commit
invariants and exposed an incorrect final error expectation: disabled custody
returns `SECRET_APPROVAL_REQUIRED`, while changed approval identity returns
`SECRET_APPROVAL_REVOKED`. The corrected test asserts both exact states. These
failures remain retained in the raw proof directory.

The complete discovery file passes **15/15**, with no failures, skips or
cancellations: 94.859 seconds inside its original 300-second root budget and
106.197 seconds including the driver. The 120-second execution and 10-second
in-flight socket-closure limits remain unchanged. Its native inputs are exactly
the qualified component inputs; only the two test-caller corrections described
above are overlaid. The controller, labelled children, all three scratch pools
and temporary root are verified absent after terminal cleanup.

The original Connector custody cohort also passes **15/15**, with no failures,
skips or cancellations: 216.438 seconds inside its original 300-second root budget
and 228.101 seconds including the driver. A focused entry point invokes the
byte-identical `connectorCustody` fixture, preserving every subcase and limit.
It covers eight exact Secret slots, queued file-ACL denial, independent typed
Dataset readback, cursor reuse, three credential-reflection encodings, redirects,
truncated/oversized responses, unavailable/time-limited providers and in-flight
revocation. Its nine native artifacts are the same component artifacts. Every
owned controller, labelled child, scratch pool and temporary root is absent after
cleanup.

This evidence is functional qualification, not production capacity certification.
The combined platform revision still requires its complete independent quality
gate. Continuous read, writeback and additional batch write modes remain open.
