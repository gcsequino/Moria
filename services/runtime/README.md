# Native Moria runtime

The shared Rust crate builds `moria-runtime` and the compatible
`moria-indexing` executable. The native API, scheduler, workers and operator
commands run as separate roles. [Process entry points](../../docs/production/EXECUTION-PROCESSES.md) ·
[Native component inventory](../../docs/production/RUST-PLATFORM-PROGRAM.md)

## Build and verify

Use Linux and the Rust toolchain specified in `Cargo.toml`. The development
database needs schema version 5 and its exact baseline checksum.

```sh
cargo build --release --locked --manifest-path services/runtime/Cargo.toml
cargo test --locked --manifest-path services/runtime/Cargo.toml --lib
```

Set `MORIA_RUNTIME_BINARY` to the matching executable for local startup.
The test Dockerfile builds the runtime and its integration harnesses.
`pnpm check:indexing` checks artifact provenance and runs native unit tests.
[Database setup](../../docs/DATABASE.md) · [Local instance](../../apps/local/README.md)

## Indexing operations

Dataset publication records durable indexing work for registered bindings.
Independent workers claim fenced tasks, scan pinned inputs, resolve permanent
UIDs and activate complete Source generations. Search has its own projector,
generation and visibility protocol.

Provision a scoped external database role. Set
`MORIA_INDEXING_DATABASE_SECRET_REF` to the name of an environment variable
containing its URL. TLS verifies certificates by default. Set
`MORIA_INDEXING_DATABASE_CA` for a private CA. TLS `disable` is reserved for
isolated loopback fixtures.

```sh
moria-indexing worker
moria-indexing status
moria-indexing cancel BUILD_UUID
moria-indexing retry BUILD_UUID
```

Cancellation and retry preserve ownership, source pins and current authority.
Use the [indexing contract](../../docs/production/contracts/indexing-service.md)
and [Source stores](../../docs/production/NATIVE-SOURCE-STORES.md) for accepted
formats, storage and activation boundaries. Qualified v2 and v3 replacement,
retained reads and metadata pruning have separate evidence. Live sparse writes
and downstream mutation publication remain open. [Verification and production targets](../../docs/IMPLEMENTATION.md#native-integration-and-production-goals--november-5-2026)
