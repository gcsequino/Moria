# Protected-operation ownership

ObjectService owns protected object point reads, queries/cursors and actions.
Its private protectedRead lifecycle pins the environment, checks current authority
and schema, evaluates the read and checks authority again before returning. Both
read forms use a 30-second statement timeout and the existing shared environment
admission. The final check includes cursor storage, so a slow cursor write cannot
outlive session validity unnoticed. Actions retain Catalog.action transaction
ownership, including current checks on receipt replay and commit ordering.

The store, authority and binding are private fields. Bindings and incoming action
declarations are copied; the public binding getter returns a copy for existing
diagnostics. requiring only adds management gates to an existing binding. It
cannot replace or remove earlier gates. Trusted bootstrap code still constructs
services from raw stores; it is an explicit owner, not an application permission.

## Structural enforcement

Run pnpm check:architecture, also included in pnpm check and Linux quality.
scripts/protected-boundaries.ts lists exact owners of raw stores, cursors, current
authority, catalog operations, database construction, and live object dispatchers.
The checker scans apps/packages/services/workers without an opt-in file list. It
uses the pinned TypeScript 7 native semantic API to follow method declarations,
import aliases and re-exports; imports, property access and calls are checked.
Computed capability imports and unregistered public object operations fail.
Type-only declarations do not grant executable access. Source symlinks fail.

The registered dispatchers are the workload API, management wrapper and scoped-app
wrapper. Their existing HTTP authorization tests remain required. The shared
release contract exercises real PostgreSQL service reads and actual workload HTTP
point/query routes. The outer management/scoped wrappers retain their existing
identity, origin and gate tests. New dispatchers require explicit registration
and contract evidence; the registration file itself requires owner review.

## Scope and remaining gates

This is a structural guard against known typed capability bypasses, not a sandbox
for arbitrary hostile JavaScript or a proof about every reflective/untyped data
flow. In particular, runtime code generation and arbitrary casts need independent
review. Test-file existence checks detect missing registered suites, not semantic
proof that an edited suite retains its assertions. The trusted external factory
policy must protect the checker, policy and contract tests from candidate edits.
The factory repository tracks that separate enforcement obligation in issue #2.

GitHub required branch protection and ruleset reads currently return HTTP 403 for
this private repository on the current plan, and the publishing account has admin
permission. Those external gates remain unmet. A candidate-controlled CI pass or
CODEOWNERS alone is not independent enforcement. No access settings were changed.

Other release boundaries keep their existing owners: management metadata and jobs
use managementGuard and Jobs; catalog storage checks at transfer/publication; the
indexer checks source and final fenced activation; the model gateway checks before
dispatch and release. Their end-to-end law coverage is follow-up work, not claimed
by the object-serving slice. No new streaming object API is introduced.

The validator failure tracked by #67 remains unresolved. An exact-head CI failure
on the stacked base PR #75 also produced INVALID_INPUT in reference recovery; no
root cause is inferred from that shared error. Do not merge or waive the gate.
