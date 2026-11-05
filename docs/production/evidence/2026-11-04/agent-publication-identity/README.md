# Agent publication identity reuse

Agent publication previously ran two more full context checks around reconstructing
the same credential immediately after platform publication's final context check.
The platform now returns the already verified identity after that unchanged final
check. The Agent uses its immutable identity and credential hash for the original
fresh retained Source, model, ledger and classification checks in the same
serializable transaction. The Function caller ignores the returned identity.
No grants or authorization snapshots are reused. The effect checks, final
platform guard, execution completion, lock modes and deadlines remain unchanged.

A real committed Agent receipt now requires **four** context worker checks before
its receipt insert, down from **six** on the previous native binary. Both runs
assert the original successful result and exactly one receipt. The counter
matches the committed receipt's transaction ID, scopes recording to the actual
one-shot worker's unique non-owning role and writable transactions, and retains
the exact original worker guard. After the worker exits, the original function
and permission metadata are verified restored, and proof objects are removed. This measures repeated
work at the receipt boundary, not total runtime or achieved capacity.

The old exact-transaction run finishes in 83.873 seconds, with the five original
sibling subcases passing and only the new count assertion failing after actual
successful publication. The earlier function-statistics probe saw seven calls
but could not tie all of them to the receipt transaction; it remains recorded
and is not used for the paired result.

The complete original Agent file passes **24/24**, zero skips or cancellations,
373.228 seconds including the driver, on the fixed native image. The unchanged
three root limits remain 300 seconds each. SDK/CLI, real model/tool callbacks,
child isolation, lease and worker fences, cancellation, SIGKILL, lost COMMIT,
Source automation and retained Source/authority all execute. All controller,
labelled-child, scratch-pool and temporary-root cleanup checks pass.

The frozen source is based on `0c36f77`. All 144 native inputs match the qualified
checkout. The only native input differences from the old paired artifact are the
three small Rust caller/return changes; the generated schema is identical. All
nine artifacts build, full static/provenance checks pass, and all 117 native units
pass with none ignored. [qualification.json](qualification.json) records exact
hashes and persistent raw proof paths.

The complete original Function file also passes **18/18**, zero skips or
cancellations, on the same artifacts: 73.448 seconds in its root and 75.714 seconds
including the driver. Original current-authority, effects, nested preparation,
worker-scope revocation, cancellation, SIGKILL and lost-COMMIT checks pass, with
all owned fixtures absent after cleanup. An earlier WSL launch timed out before
starting any test; the full retry is the reported result.

The previous original service Evaluation still missed both first Agents'
publication deadlines after sealing. A fresh Evaluation gate, whole-instance
composition and the independent complete quality gate remain required.
