# Moria implementation

Follow docs/spec for accepted contracts. Restricted views are terminal: Transform inputs must resolve to Dataset resources. Iceberg owns the dataset table format. Never replace real transactional, authorization or isolation tests with mock-only claims.

Use local model inference for real-model smoke tests; never use paid model APIs by default. Credentials belong in environment variables or external credential stores, never repository fixtures or logs.

Run pnpm check for TypeScript changes. Keep progress and deviations in docs/IMPLEMENTATION.md. Runtime tests requiring unavailable infrastructure must be reported as unverified.

Keep source readable and formatter-clean. Run `pnpm format` for TypeScript/JSON/YAML; `pnpm check` enforces that formatting. Use Black for Python (`python -m black workers benchmarks docs/spec/schemas`, configured in pyproject.toml), gofmt for Go, and rustfmt for Rust. Do not compress statements or control flow onto one line. Preserve raw benchmark evidence in docs/benchmarks without reformatting it.

When interacting with Cloudflare, use the cf CLI unless the project has a Wrangler configuration file.
