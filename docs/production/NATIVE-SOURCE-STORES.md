# Native Source stores: full-build bootstrap

The native indexing worker bootstraps a managed Source store from the existing immutable `staged_source_rows` population. The store ID is the actual indexing build/stage UUID. Publication adds an immutable alias from the ObjectType environment revision to that store and Source version `1`, together with the actual indexed Dataset revision, snapshot and qualified root. The environment revision, Dataset revision and store-local Source version are separate positions.

This step retains full replacement indexing. It does not activate Dataset delta ingestion, sparse Source writes, Source version `2`, or the uninstalled `dataset-delta-schema.ts` prototype. A Dataset without a declared primary key gets an empty key map and retains full replacement semantics. Object UIDs do not become fabricated Dataset keys. Retained legacy registrations without a managed serving/projection profile continue through the original full-build path.

## Database contract

`native_source_row_stores` records immutable build/profile identity and the real owner/fence. `native_source_key_map` stores complete typed ordered Dataset keys and canonical Object UIDs. Its compact SHA-256 hash selects a bounded collision bucket; equality always compares the complete encoded key. A store without declared keys has zero map rows. `native_source_generation_aliases` fixes the store/version and indexed Dataset position for one published environment revision.

The existing full row population is not duplicated. The Rust reducer inserts map entries in the same bounded transaction as its existing staged row batch, using the raw normalized Dataset row before ObjectType mapping. JSON and Arrow use the same typed ordered-key encoding, including exact int64 strings, arrays and float zero normalization. Bulk map insertion has a normal set-based path; full-key collision handling is bounded to 64 entries per hash.

The worker uses these SQL functions:

| Operation | Authority and result |
| --- | --- |
| `runtime_source_store_open(build,worker,fence)` | Current coordinator claim; opens/resumes the immutable bootstrap store, or returns NULL for retained unmanaged builds. |
| `runtime_source_store_task(build,task,worker,fence)` | Current reducer task claim; returns ordered declared keys and source column descriptors. |
| `runtime_source_store_keys(build,task,worker,fence,ids,keys)` | Current reducer claim; at most 1,000 keys and 16 MiB, matching real staged raw rows. |
| `runtime_source_store_seal(build,worker,fence)` | Current coordinator; all actual scan/reduce receipts sealed; row/key counts equal the committed scan count. |
| `runtime_source_store_publish(build,worker,fence,revision)` | Current coordinator, inside `indexing_activate`; publishes the alias and store with the actual generation and Dataset pin. |
| `runtime_source_store_retry(build)` | Existing failed-build retry authority; refuses published stores and clears only the unpublished map. |
| `runtime_source_stage(stage,environment,ObjectType,revision?)` | Validates immutable generation/store scope and returns the physical stage; retained legacy stages remain readable. |

A deferred generation constraint verifies store publication, alias position and the active build's actual receipt at transaction commit. Alias mutation and metadata identity mutation fail closed. Worker takeover advances the real coordinator fence. No owner or lease is synthesized.

## Readers and lifecycle

`runtime_object_base` resolves the generation alias at the pinned environment revision. `runtime_object_base_stage` validates the physical store for bounded projection preparation and retained stage readers. Retained TypeScript ObjectStore/query paths resolve the physical stage without replacing their UID seek plans. Typed projection tables remain tied to the real full-build stage; their existing source/effective-row and overlay interval kernels are reused.

FullObjectSource separately validates the immutable physical store and the logical Source/environment revision. Its page and exact-UID reads resolve canonical rows at the pinned logical revision, while retaining current registered-service, Dataset, model and execution checks. Ordinary row-policy queries do not acquire registered full-data access.

History collection deletes matching key entries in the same existing bounded 1,000-UID payload batch. It retires the header only after the approved generation's payload is exhausted. Existing history floor, retained generations, cursor/read pins, structural Search pins and backup admission remain authoritative. Immutable aliases remain as publication metadata; retired payload resolution fails closed. Recovery includes aliases, headers, key maps and canonical rows in the real database backup.

## Remaining activation work

The resolver currently accepts bootstrap version `1`; its logical generation stage and physical store are the same UUID. Version `2` activation must resolve the alias before looking up the base store, compare each alias against its actual consumer build/Dataset position instead of the bootstrap profile, and retain shared base rows while any live alias or structural reader pin depends on them. Current version constraints deliberately reject sparse aliases.

Before Source deltas can be enabled, implement and qualify the real DatasetCommitDelta producer/journal, contiguous-chain build pins, bounded net-change spill, sparse row/projection intervals, key-map changes and exact affected-UID Search journals. Convert every direct stage reader and writer footprint needed by sparse versions. Preserve Action creation precedence, explicit attachments, delete/restore tombstones and old v2/legacy reads. Final environment/filesystem admission remains try-only for future sparse mutations until all competing Action/create/link/broker lock orders are converted. Provider I/O must complete before the bounded mutation transaction.


The connected reader/writer footprint includes the central canonical functions, prepublication projection base paging, `projection.rs` generation/stage matching, ordinary serving policy capture, the retained TypeScript ObjectStore/query paths, FullObjectSource pin/page/UID reads, Search rebuild schema selection in `search_projector.rs`, the reducer, retry and history collection. In particular, Search rebuild schema selection currently relies on full-build generation stage equaling the physical store; sparse activation must remove that assumption without changing retained full-build pins.

Terminal unpublished full-build staging does not yet have an age/grace collector. Retry clears its own unpublished map and rows; normal history collection only visits published generations. Bounded collection for failed, cancelled or superseded unpublished stores is a separate required lifecycle step, with a real store write barrier and terminal-build/alias/pin checks before deletion.
