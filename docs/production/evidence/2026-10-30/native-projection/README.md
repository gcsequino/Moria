# Native projection and storage authority evidence

These are functional integration runs against real isolated PostgreSQL, native
runtime processes, actual PyIceberg/Arrow/Parquet files and MinIO. They are not a
million-object capacity, mixed-load, HA, recovery-tier or full-profile pass.

The exact compiled schema/source identity is in [compiled-schema.json](compiled-schema.json),
and native executable SHA-256 values are in [artifacts.sha256.txt](artifacts.sha256.txt).
The captured schema is version 5, checksum
`170e81fc7d7231e24eb453fd2cac5c9ac0e856a6db65da07b857a067aa667873`;
compiled native source digest is
`c12d483fca7faa8c01fccd2b882559f522f982ec6defb3e8c64d5c953a8b65ad`.

## Retained results

- [core.tap.txt](core.tap.txt): 8/8 passed, zero skipped, 15.164 seconds. Actual
  native source indexing prepares bounded pages outside the environment lock;
  a stopped coordinator permits a concurrent native HTTP action, then replays
  that action before atomic source/index activation. Further cases prove fenced
  takeover, oversized final-tail release and retry, stale-layout refusal,
  layout-only reindex of the same Dataset head, retained edits/UIDs/history,
  exact overflow membership, tombstone/restore/clear maintenance and C03 order.
  Scalar cases include signed int64 extrema, decimal extrema, maximum/subnormal
  floats, microsecond timestamps, Unicode code points and nulls. The retained
  unforced 300-row EXPLAIN actually uses the declared index-only B-tree scan.
- [catalog.tap.txt](catalog.tap.txt): 13/13 passed, zero skipped, 30.142 seconds.
  Grouped heads/checkpoint/lineage/receipt/native execution completion, private
  candidate invisibility, conflict/revocation/fence failure, lost commit reply,
  read-only native snapshot scan and actual S3 multipart source publication.
  Namespace history also proves fresh and idempotent UPSERT without permitting
  revision reuse after replacement, disable or deletion.
- [storage.tap.txt](storage.tap.txt): 25/25 passed, zero skipped, 32.489 seconds.
  Bounded immutable POSIX/MinIO transfers, underclassified artifact-write denial,
  physical-version change, provider-completed multipart recovery after a lost
  reply, per-transfer revocation/fences, durable SQLite crash/reopen and native
  semantic closure validation including retained deleted files.

The original implementation trials caught an inconsistent SQL parameter type in
the test serving binding, the projection revision ledger reserving a fresh
revision twice during UPSERT, and a test comparator using unsupported policy
ordering operands for UUID/boolean. Those trials failed; their full raw TAP was
not retained. This paragraph records the observed failures from the command
transcript, rather than reconstructing raw output. The production ledger now
records revisions only after a successful insert or revision change. The final
tests independently verify ordering and exact revision-history behavior.

## Commands

Working checkout: `/tmp/moria-reconcile-20261029`. Schema generation preceded
compilation under a coordinated schema hold:

```sh
docker run --rm --mount type=bind,src=/tmp/moria-reconcile-20261029,dst=/work -w /work moria-test:rust-indexing /app/node_modules/.bin/tsx scripts/indexing-schema.ts --write
docker run --rm --name moria-data-build --mount type=bind,src=/tmp/moria-reconcile-20261029,dst=/work --mount type=volume,src=moria-indexing-cargo,dst=/usr/local/cargo --mount type=volume,src=moria-indexing-target,dst=/work/services/runtime/target -w /work/services/runtime moria-indexing-toolchain:local sh -c 'cargo test --test native_catalog --test native_storage --test native_sandbox --no-run --locked -j 2 && cargo build --bins --locked -j 2'
```

The following Docker commands were invoked through
`wsl -d Ubuntu-24.04 -u root --`; PowerShell `Tee-Object` retained the named TAP
files. Each wrapper requires its actual native binary and executes its required
native integration case; missing artifacts fail.

```sh
docker run --rm --name moria-projection-proof --user 1000:1000 --mount type=bind,src=/tmp/moria-reconcile-20261029,dst=/work --mount type=bind,src=/var/lib/docker/volumes/moria-indexing-target/_data/debug/deps/native_catalog-6893d4754de6d6d0,dst=/usr/local/bin/moria-catalog-tests,readonly --mount type=bind,src=/var/lib/docker/volumes/moria-indexing-target/_data/debug/moria-runtime,dst=/usr/local/bin/moria-runtime,readonly -e MORIA_RUNTIME_BINARY=/usr/local/bin/moria-runtime -w /work moria-test:rust-indexing /app/node_modules/.bin/tsx --test --test-reporter=tap tests/native-projection.integration.test.ts
docker run --rm --name moria-catalog-proof --user 1000:1000 --mount type=bind,src=/tmp/moria-reconcile-20261029,dst=/work --mount type=bind,src=/var/lib/docker/volumes/moria-indexing-target/_data/debug/deps/native_catalog-6893d4754de6d6d0,dst=/usr/local/bin/moria-catalog-tests,readonly -w /work moria-test:rust-indexing /app/node_modules/.bin/tsx --test --test-reporter=tap tests/native-catalog.integration.test.ts
docker run --rm --name moria-storage-proof --user 1000:1000 --mount type=bind,src=/tmp/moria-reconcile-20261029,dst=/work --mount type=bind,src=/var/lib/docker/volumes/moria-indexing-target/_data/debug/deps/native_storage-165818cc78d5156d,dst=/usr/local/bin/moria-storage-tests,readonly -w /work moria-test:rust-indexing /app/node_modules/.bin/tsx --test --test-reporter=tap tests/native-storage.integration.test.ts
```

All runtime database roles were nonowner/non-CREATE and carried the ordinary
schema-admission guard. Projection tables were created by the guarded schema-owner
provisioner. Source paths/provider endpoints were trusted fixture configuration;
public references contained logical IDs only. Operator-private raw EXPLAIN and
history counts did not become caller-visible population statistics.
