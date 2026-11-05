# Indexing staging width and blocked-row deferral

The real native fixture admits two rows with 21 required float arrays and 99,862 elements per array. Each row uses 4,194,273 normalized value bytes under the existing 4 MiB decoded-row limit. Singleton row groups preserve the existing 64 MiB conservative Parquet allocation bound; the rejected two-row-group attempt is retained.

Actual ordinary PostgreSQL storage was 471,656 bytes per spill row, with 6,291,882 SQL text bytes and 25,165,867 bytes in the inline source JSONB representation. The fixture then re-encodes identical temporary spill content under a separately labelled EXTERNAL legacy layout, restoring the configured EXTENDED policy before collection. Actual stored spill size becomes 25,165,933 bytes. No canonical data, identity, owner or fence is fabricated.

The collector preserves its 16 MiB actual-storage budget, records the exact closure, phase, cursor and candidate size, and defers the blocked build. Its existing operator inspection returns at most 20 indexed diagnostics. A different genuinely accepted and cancelled build still drains. The blocked cursor, temporary payload and canonical rows remain unchanged.

Trial013 passes all three cases, zero skips/cancellations, full static checks, all nine exact native artifacts and 106 native units. Cleanup verifies the controller, three scratch pools and temporary directory are absent. The original 20-case queue gate remains separate retained evidence and was not repeated for this new profile.

This qualifies blocked-row diagnostics and starvation avoidance. It does not prove a universal ordinary physical-width bound, automatic reclamation of oversized legacy rows, a live migration, or production capacity.
