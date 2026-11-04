# Native protected exact query evidence

These are immutable whole-source snapshot runs on this Linux host. Each trial
retains commands, schema-generation/build/test output, compiled schema/source
fingerprint, exact executable SHA-256, complete captured source-file manifest and
the generated schema. Source generation happens inside the captured tree before
compilation; ongoing working-tree edits cannot change its test database baseline.

- `20261030T142449Z`: seven real cases, 4.83 seconds, initial protected query.
- `20261030T144128Z`: seven real cases after immutable document/head separation.
- `20261030T144521Z`: nine real cases, 5.32 seconds, separate cache classifications,
  immutable embedding bytes/labels, retained-label filtering and current canonical
  row policy across actual source replacement.

Actual PostgreSQL 18 uses pgvector 0.8.7 cosine and the actual Rust model gateway
calls a deterministic HTTP embedding provider. Normal source objects are
published through ObjectStore with permanent catalog identities. The search
fixture generation is sealed by trusted fixture setup using real model gateway
embeddings; it does not exercise or claim a production search projector. Separate
actual local Qwen3/Nomic inference evidence is under `docs/benchmarks`.

Passing cases prove authorization before ranking/LIMIT, stable ties, exact model
and visibility identity, current tombstone/effective/canonical policy, property
checks before model billing, immutable classified cache data, and revocation
during inference. They do not complete N12, public native instance composition,
whole-platform integration, workload throughput, recovery/HA or retention gates.
