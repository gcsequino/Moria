# Rust audit

The audit baseline is `167a0948c1ff8f7c79d7ba793144d5d266ea4fb4`, the head
of PR #88 at inventory time. The ledger covers 130 runtime Rust files, one ontology
benchmark file, and ten native integration fixtures. An archived production
evidence file is outside the build inventory.

Each listed file received a source screen for ownership copies and intermediate
collections, panic paths, synchronous operations, task lifecycle, and ignored
errors. The notes identify the boundaries traced through callers, validation,
error handling and commit or cleanup. A screen records coverage of the search,
and a trace records review of the named paths. Findings require a concrete
failure mode or avoidable cost at an accepted input size.

## Findings and verification

| Finding | Change | Regression evidence |
| --- | --- | --- |
| R01 | Source release merge indexes references once and compacts removals once. | Native unit covers active ordering, replacement, new resources, dependencies and missing removals. |
| R02 | Search writes the unique missing-cache batch in one PostgreSQL statement. | Search fixture counts rows per INSERT statement and requires a multi-row write. |
| R03 | Iceberg async reads open the sealed spool once for scope, owner and entry checks and perform the SQLite work on a blocking thread. | Catalog tests cover qualified snapshots and reads under live authority. |
| R04 | Dataset primary-key extraction binds projected columns once and decodes each key cell once. | Native Dataset primary-key and graph tests cover typed identity, ordering and bounded reads. |
| R05 | Canonical fingerprints feed SHA-256 directly with the shared canonical encoder. | Native canonical and cross-runtime fixtures cover UTF-16 key order, numeric formatting and typed identities. |

The local Linux release image passed static quality, both pinned performance
lint checks, native artifact provenance, all 131 Rust unit tests, nine value
model tests and 35 focused integration cases. The focused files cover the
legacy API process, Source activation, Search, Dataset graph and primary keys,
and Catalog. The Search statement-count assertion failed at the baseline
runtime with the new fixture and passed on the revised runtime.

## Future changes

Use the [Rust review rules](../AGENTS.md#rust-ownership-and-data-flow) when
changing these paths. Run the focused regression, `pnpm check:indexing` for
native changes, and the independent Linux quality gate on the latest PR
revision. That gate runs release `clippy::perf` checks for both crates on the
pinned toolchain. For new Rust files, record the crate or fixture entry point
and reviewed invariants in the PR. Count rows, allocations, queries, or bytes
where a deterministic count can distinguish a regression. For authorization,
publication, retention and recovery changes, record the affected invariants for
explicit human review.

An audit is complete for a stated revision and checklist when each path has an
explicit disposition, every confirmed finding has a fix or an open follow-up,
and the required checks pass. Use the inventory below when opening the next
pass. Carry open fault-injection and workload questions forward with the
affected path and the evidence still needed.

## Independent Linux gate

The preceding hosted run ([42989779395](https://github.com/gcsequino/Moria/actions/runs/42989779395))
failed in the legacy API process fixture, native Evaluation tests and one
native Agent case. The legacy fixture now resolves its loader before switching
to the empty working directory. Its isolated Linux run passed. The Evaluation
failures also appear in the [earlier Linux run](https://github.com/gcsequino/Moria/actions/runs/42130357789). In the paired local baseline runs,
the Agent file passed all 24 cases on both images. The Evaluation file had the
same one remaining failing leaf on both images. The Source automation case
failed at its lifecycle barrier locally and at the child-progress lock probe
in hosted CI.
Keep these failures on the Linux quality gate until their fixes are verified.

## File coverage

To verify the inventory from the repository root, compare the tracked Rust
paths at the baseline to the paths below.

```bash
baseline=167a0948c1ff8f7c79d7ba793144d5d266ea4fb4
diff -u \
  <(git ls-tree -r --name-only "$baseline" -- \
    services/runtime benchmarks/ontology/rust tests/fixtures | rg '\.rs$' | sort) \
  <(rg '^(services/runtime|benchmarks/ontology/rust|tests/fixtures)/.*\.rs$' \
    docs/RUST-AUDIT.md | sort)
```

### Source activation and authority (11)

Traced plan and activation through desired-resource construction, origin
validation, and publication. Rechecked active ordering and the separation of
owned resources from dependencies. The other Source paths received the
inventory screen.

```text
services/runtime/src/filesystem_source.rs
services/runtime/src/filesystem_source_activation.rs
services/runtime/src/filesystem_source_validation.rs
services/runtime/src/full_object_source.rs
services/runtime/src/source_actions.rs
services/runtime/src/source_bindings.rs
services/runtime/src/source_links.rs
services/runtime/src/source_runtime.rs
services/runtime/src/source_security.rs
services/runtime/src/source_store.rs
services/runtime/src/worker/source.rs
```

### Search and retrieval (7)

Traced cache lookup, provider request, marking and model checks, transactional
cache writes, and document commit. The remaining installation, worker and query
paths received the inventory screen.

```text
services/runtime/src/retrieval.rs
services/runtime/src/search.rs
services/runtime/src/search_background.rs
services/runtime/src/search_embedding.rs
services/runtime/src/search_install.rs
services/runtime/src/search_projector.rs
services/runtime/src/search_worker.rs
```

### Dataset, Parquet and indexing (35)

Traced projected primary-key batches, row-byte bounds and typed key encoding.
The prior pass traced the packed-key codec, bounded snapshot scans and Pipeline
compaction. Other Dataset graph, mutation and indexing paths received the
inventory screen.

```text
services/runtime/src/dataset_commit_delta.rs
services/runtime/src/dataset_graph.rs
services/runtime/src/dataset_graph_read.rs
services/runtime/src/dataset_writer.rs
services/runtime/src/dataset_writer/bootstrap_v2.rs
services/runtime/src/dataset_writer/contract.rs
services/runtime/src/dataset_writer/graph_builder.rs
services/runtime/src/dataset_writer/graph_output.rs
services/runtime/src/dataset_writer/key_run.rs
services/runtime/src/dataset_writer/key_run_io.rs
services/runtime/src/dataset_writer/key_run_merge.rs
services/runtime/src/dataset_writer/key_run_set.rs
services/runtime/src/dataset_writer/key_run_set/update.rs
services/runtime/src/dataset_writer/keys.rs
services/runtime/src/dataset_writer/mutation_inputs.rs
services/runtime/src/dataset_writer/mutation_standard.rs
services/runtime/src/dataset_writer/position_delete.rs
services/runtime/src/dataset_writer/replace_graph.rs
services/runtime/src/dataset_writer/row_selection.rs
services/runtime/src/dataset_writer/rows.rs
services/runtime/src/dataset_writer/task_graph.rs
services/runtime/src/dataset_writer/visibility.rs
services/runtime/src/indexing_service.rs
services/runtime/src/native_snapshot_v3.rs
services/runtime/src/native_snapshot_v3/batch.rs
services/runtime/src/native_snapshot_v3/index_summary.rs
services/runtime/src/native_snapshot_v3/mutation.rs
services/runtime/src/native_snapshot_v3/point.rs
services/runtime/src/native_snapshot_v3/scan.rs
services/runtime/src/native_snapshot_v3/validate.rs
services/runtime/src/pipelines.rs
services/runtime/src/pipelines/discovery.rs
services/runtime/src/snapshot_parquet_bounds.rs
services/runtime/src/snapshot_parquet_read.rs
services/runtime/src/worker.rs
```

### Catalog, snapshot and object I/O (10)

Traced Iceberg async metadata and physical reads through sealed spool lookup,
owner checks and range reads. The synchronous Iceberg input hook still checks
membership. Snapshot verification and retention still have synchronous spool
operations to profile separately.

```text
services/runtime/src/authenticated_file.rs
services/runtime/src/catalog.rs
services/runtime/src/catalog/snapshot.rs
services/runtime/src/engine_io.rs
services/runtime/src/filesystem.rs
services/runtime/src/filesystem_content.rs
services/runtime/src/filesystem_management.rs
services/runtime/src/io.rs
services/runtime/src/snapshot.rs
services/runtime/src/storage.rs
```

### Admission, serving and publication (27)

Traced current authority at Search writes and the Function publication lease
and observation checks. Prior work reviewed serving policy-reader sharing and
final HTTP query admission. Remaining routes and policy actions received the
inventory screen.

```text
services/runtime/src/app_host.rs
services/runtime/src/attestation.rs
services/runtime/src/authority.rs
services/runtime/src/authorization.rs
services/runtime/src/changes.rs
services/runtime/src/directory.rs
services/runtime/src/filesystem_governance.rs
services/runtime/src/history.rs
services/runtime/src/http_admission.rs
services/runtime/src/http_auth.rs
services/runtime/src/http_browser.rs
services/runtime/src/identity.rs
services/runtime/src/mcp.rs
services/runtime/src/persistent_directory.rs
services/runtime/src/persistent_directory/graph.rs
services/runtime/src/persistent_directory/read.rs
services/runtime/src/platform.rs
services/runtime/src/platform_links.rs
services/runtime/src/projection.rs
services/runtime/src/serving.rs
services/runtime/src/serving_actions.rs
services/runtime/src/serving_creations.rs
services/runtime/src/serving_link_evidence.rs
services/runtime/src/serving_links.rs
services/runtime/src/serving_narrow.rs
services/runtime/src/serving_ordinary.rs
services/runtime/src/serving_policy.rs
```

### Execution, cancellation and cleanup (15)

Reviewed execution cancellation and fenced settlement, Function publication
retry, and the prior model-stream disconnect fix. The remaining scheduler and
sandbox branches received the inventory screen. Retry, worker loss and
cancellation interleavings still need targeted fault injection.

```text
services/runtime/src/agents.rs
services/runtime/src/automations.rs
services/runtime/src/connector_discovery.rs
services/runtime/src/delivery.rs
services/runtime/src/evaluations.rs
services/runtime/src/execution.rs
services/runtime/src/functions.rs
services/runtime/src/jobs.rs
services/runtime/src/lifecycle.rs
services/runtime/src/model_http.rs
services/runtime/src/model_protocol.rs
services/runtime/src/models.rs
services/runtime/src/runtime.rs
services/runtime/src/sandbox.rs
services/runtime/src/scratch.rs
```

### Retention and recovery (14)

Reviewed retention claim renewal, version-qualified removal and fenced
settlement. Traced recovery capture from sealed manifest write to the ready
receipt. Other deletion and restore branches received the inventory screen and
need dedicated recovery and retention fault scenarios.

```text
services/runtime/src/dataset_retention.rs
services/runtime/src/dataset_retention/collect.rs
services/runtime/src/dataset_retention/operator.rs
services/runtime/src/dataset_retention/prune_graph.rs
services/runtime/src/filesystem_gc.rs
services/runtime/src/recovery.rs
services/runtime/src/recovery_content.rs
services/runtime/src/recovery_content/graph.rs
services/runtime/src/recovery_content/offline.rs
services/runtime/src/recovery_content/offline/graph.rs
services/runtime/src/recovery_control.rs
services/runtime/src/recovery_keys.rs
services/runtime/src/recovery_metadata.rs
services/runtime/src/retention.rs
```

### Build, codecs and transport (11)

Traced canonical serialization and digest output. Reviewed the pinned runtime
build and lint targets. The other transport and encoding paths received the
inventory screen.

```text
services/runtime/build.rs
services/runtime/src/bin/moria-runtime.rs
services/runtime/src/canonical.rs
services/runtime/src/database.rs
services/runtime/src/engine_ipc.rs
services/runtime/src/lib.rs
services/runtime/src/main.rs
services/runtime/src/secrets.rs
services/runtime/src/transport.rs
services/runtime/src/values.rs
services/runtime/src/wire.rs
```

### Ontology benchmark (1)

Reviewed the connection checkout and cancellation path during the prior pass.
Both formatting and performance lints run on the pinned build.

```text
benchmarks/ontology/rust/src/main.rs
```

### Native Rust fixtures (10)

Screened the standalone fixtures and their shared artifact helper. Run them
through their existing integration entry points for behavior evidence.

```text
tests/fixtures/native-catalog/capacity.rs
tests/fixtures/native-catalog/harness.rs
tests/fixtures/native-catalog/reader_window.rs
tests/fixtures/native-catalog/standard_export.rs
tests/fixtures/native-common/artifact.rs
tests/fixtures/native-policy/harness.rs
tests/fixtures/native-retention/harness.rs
tests/fixtures/native-sandbox/harness.rs
tests/fixtures/native-storage/harness.rs
tests/fixtures/native-transport/harness.rs
```
