# Strict pending-read v25 native build

Full TypeScript checking, all 77 native library tests, both runtime/indexer binaries, the library test executable and all six native harnesses compile successfully from the immutable source recorded in `manifest.json`. Compiled schema admission and exact artifact hashes are recorded alongside their phase logs.

This directory records a build/unit slice. Agent pending-read and full-platform integration results are separate. The earlier `20261031T144412Z` driver attempt used an incorrect Cargo target name while selecting the successful library-test artifact; its failure is preserved. The corrected driver selects the actual `moria_runtime` compiler artifact from the same frozen source.
