# Native data authority trials

`authority-trial004.tap.txt` passed 65 tests with zero failures/skips in 41.690 seconds. It ran the real PostgreSQL/MinIO storage, native Dataset catalog, V2 snapshot indexing, resource provenance and N25 protected HTTP fixtures together. This is functional evidence, not population or throughput certification.

The source snapshot was `/tmp/moria-proof-snapshots/20261030T155540356409Z-indexing-provenance-authority-v4`. Its exact source manifest and executable/schema fingerprints are retained beside the TAP log. Schema version 5 had checksum `a4b643b45442056b7de74a70e80ef56f8cbb572451ed3f482d0a8f1030347f46`; native source digest was `13513dbf127c53ab937acf062103003476e3e7f70119b81b06ada40f6f51b56d`.

The test command inside `moria-test:native-prerequisites` was:

```sh
MORIA_CATALOG_TEST_BINARY=/work/.proof-artifacts/native-catalog \
MORIA_STORAGE_TEST_BINARY=/work/.proof-artifacts/native-storage \
MORIA_RUNTIME_BINARY=/work/.proof-artifacts/moria-runtime \
/app/node_modules/.bin/tsx --test --test-reporter=tap \
  tests/native-provenance.integration.test.ts \
  tests/native-snapshot-indexing.integration.test.ts \
  tests/native-catalog.integration.test.ts \
  tests/native-storage.integration.test.ts \
  tests/native-projection.integration.test.ts
```

The snapshot was mounted at `/work`; the original project `node_modules` was a separate read-only dependency mount. Schema generation and both native binaries/harnesses were built against that same snapshot. Executables were copied into `.proof-artifacts` before tests and never replaced during the trial.

The cases include actual native source publication/index activation followed by a newly enabled secret namespace, immediate Node/native derived-read denial, and retained classification after disable. A four-second ordinary credential expiry was configured before accepting an indexing build. The test stopped a real coordinator, issued private authority from its exact admitted attempt, crossed the wall-clock expiry, verified ordinary/public authentication denial, exercised current disable/grant/scope/marking/fence/incarnation controls, then resumed native work to activation. No ordinary TTL or accepted build deadline was extended.

Dataset tests resolve the current metadata pin, advance the head, then scan an older explicitly declared immutable snapshot under a real lease. An undeclared Dataset is denied. Concurrent Arrow reads retain all authority and fence checks without a SHARE-to-UPDATE lock-upgrade deadlock. An inherited private input cannot be written into an underclassified destination namespace even when whole-Dataset history retains the private label.

Prior failed trials remain as evidence:

- Trial 001 exposed the Dataset read lock-upgrade deadlock and an indexing fixture that reused a stale registration after policy changes.
- Trial 002 retained the PostgreSQL deadlock diagnostic. Its actual native ordinary-TTL/private indexing subcase passed, but the combined run failed.
- Trial 003 fixed the deadlock and passed the indexing/provenance cases. It exposed the destination namespace marking-floor bypass caused by treating retained whole-Dataset classification as the physical write floor.

Later private metadata-kernel confinement, registered Sync producer credentials, completed-private receipt replay and service projector admission are separate changes requiring their own evidence. The failed one-million-row preparation trial is retained in `../native-projection/one-million-trial001`; it did not activate a source generation or projection and does not establish a capacity result.
