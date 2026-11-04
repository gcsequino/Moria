# Development feedback gates

The first new gate run caught an implicit TypeScript parameter type in the test
selector; it failed before expensive integration. The typed selector retry
passes the complete `check:static`, exact compiled native-artifact checks,
77 native unit tests and two selector tests with zero skips. Commands, immutable
image ID and both original/retry outputs are retained here.

The verification image overlays current source on the retained gate-v27
prerequisite image and runs a locked offline pnpm install. No Rust source or
native input changed; the compiled source digest is checked by `check:indexing`.
The authoritative checkout is on persistent Linux storage. The local-named and
selected Agent integration runs are separate and are not claimed here.

The complete quality gate remains required; these short checks do not replace
integration, independent CI or production workload qualification.
