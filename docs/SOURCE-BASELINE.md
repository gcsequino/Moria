# Source baseline — October 24, 2026

This initial published snapshot contains the current implementation, historical benchmark evidence, examples, tests, deployment launchers, and the proposed production architecture contract.

## Scope

243 existing source/evidence files were inventoried and copied with matching SHA-256 digests before documentation-only publication edits. This record is an additional file. Runtime workspace data, credentials, local configuration, installed dependencies, build outputs, and previous repository metadata are excluded.

The previous local checkout and commit history remain intact separately. Historical commit IDs in benchmark reports refer to that local history and may not resolve in this published snapshot. Existing source code, test fixtures, and raw benchmark results were not rewritten for publication. Documentation edits remove an absolute workstation path, clarify historical verification limits, and update source-publication navigation/status.

Secret-pattern and sensitive-filename checks found only reviewed test fixtures and a dynamically generated localhost benchmark connection string. No live credential was found in the selected files. This is a scoped review, not a guarantee that heuristic scanning detects every possible secret.

## Verification

On October 24, the source passed pnpm check on Windows:

- Prettier formatting check.
- Generated SDK consistency check.
- TypeScript type checking.
- 87 tests passed; zero failures, cancellations, or skips.

Tests include real PostgreSQL and Iceberg integration and isolated process-failure cases. Source files outside the documented Markdown publication edits match the tested checkout. Production runtime data and live services were not migrated or restarted by this publication.

The workload contract remains proposed. These checks are not new production-scale performance, high-availability, or disaster-recovery certification.

## Reproduction and next work

Use the prerequisites and dependency setup in [README](../README.md), then run pnpm check. The environment and hardware must be recorded when comparing future results. Historical reports retain their original limits and dates.

Source publication is tracked in [SRC-01](https://github.com/gcsequino/Moria/issues/12). The next runtime milestone starts with [external PostgreSQL and configuration lifecycle](https://github.com/gcsequino/Moria/issues/13). [M0](https://github.com/gcsequino/Moria/issues/2) remains open for workload/architecture review.
