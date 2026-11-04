# Native Dataset key and retained-history proof

The frozen v6 runtime/catalog/retention artifacts passed **3/3 tests, zero skips**, in 125.88 seconds. Build inputs and executable metadata are recorded in `manifest.json`; the complete test output is in `integration.txt`.

The tests publish actual Source Dataset definitions and use the native Parquet writer/read path. They cover:

- Metadata compaction from two snapshots to one while a real old-root reader remains live, with zero expired physical roots, exact current/old typed rows, scheduling-time historical selection, and an idempotent second maintenance run.
- Declared Dataset key enforcement on replacement, duplicate rejection before a new head, an actual current key certificate, and rejection of direct head publication without its required history/contract.
- An ordered composite key spanning exact int64, float64, decimal, date and timestamp columns. Adjacent integers beyond JavaScript's safe integer limit remain distinct. Canonical decimal/timestamp duplicates and actual null key values reject publication; the previous head and typed rows remain exact. Nullable non-key columns remain supported. Declared key columns remain non-null under the existing Source contract.

The integration JSON transport normalizes negative zero; the physical Arrow negative/positive zero equality case is covered by the separate Rust key unit test. This fixture makes no negative-zero Parquet claim. This is a correctness proof, not a capacity certification or activation of incremental write modes.
