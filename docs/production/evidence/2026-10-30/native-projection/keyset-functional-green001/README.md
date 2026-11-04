# Bounded projection keys: functional regression

The corrected projection page selects bounded ordered keys from source and creation populations before overlay lookup or payload validation. This preserves the exact creation/source union, required-field checks, byte limits, current authority, fences, and activation barrier. The separate incomplete million-row trial records the measured reason for this change.

Actual immutable-source functional proof passed 13 tests with zero skips in 83.34 seconds. It ran `tests/native-projection.integration.test.ts` and `tests/native-source-runtime.integration.test.ts`, covering native source/indexing/projection, typed protected HTTP, policy/cursor behavior, creation/attachment, Dataset refresh, and required-field fallback rejection. Source snapshot: `/tmp/moria-proof-snapshots/20261031T000014011395Z-native-projection-keyset-v1`; manifest SHA-256: `bef3b271fdc57d8f497ec7b5048acacb751c6ab6018b0c1d429c2c4eb48feacf`.

The mandatory exact-runtime/catalog provenance preflight preceded fixture setup. Cargo-selected copied binary hashes, generated schema, embedded metadata, build output and raw test output are retained. The two-CPU/6 GiB native proof held the shared host resource lock for its full execution.

This is a functional regression, not a million-row or SLO pass. A fresh complete pipeline, streaming oracle and protected open-loop workload remain required before population increases.
