# Registered Automation child dispatch

Registered Agent-to-Function child construction failed because
`runtime_sandbox_service_child_binding_guard` declared a PL/pgSQL record named
`binding` and queried an unqualified execution column with that same name.
Qualifying `e.binding` resolves the ambiguity without changing the selected
binding, parent proof, target approval, lease/fence or retained-label checks.
The existing end-to-end factory case exercised this path and exposed the failure.

The later inspector setup also needed its intended filesystem ACL on both code
proofs and runtime placements. Source materializes runtime entries beneath the
project environment tree, outside its code repository. The fixture now grants
inherited read/list on that project and restores the original policy afterward.
The inspector's logical application scopes remain exactly `Automation:run`.
A new denial assertion confirms that this logical scope alone does not bypass the
placement ACL. The API and production authorization guards are unchanged.

## Verification

- Complete static checks passed, and all nine native executables match the frozen
  source/schema. All 79 native units passed with zero ignored tests.
- All six original Agent service/current-authority subcases and their root
  passed: 7/7 reported tests, zero skips/cancellations, 163.09 seconds.
- The complete factory case passed real registered Agent dispatch, Function-child
  failure observation and inherited private labels, actual lost COMMIT response
  recovery, accepted release/schema pinning, 51-firing cursor pagination, withheld
  child fields, missing-clearance denial and missed service-disable pulses.
- The other five cases preserved expired ordinary-session behavior, bounded
  accepted capabilities, pre-code filesystem rejection, deleted descriptor
  protection, pinned code and current authority for retained results.
- Owned controllers, proof-labelled containers, scratch pools and temporary paths
  were verified absent after the focused run.

The focused entrypoint is a captured copy of the existing Agent fixture with only
its three-group loop narrowed to the complete service/current-authority group.
All selected assertions and deadlines are unchanged; its content hashes and the
exact native artifacts are recorded alongside the output. The original full
Agent file retains all three groups.

This checkpoint qualifies the selected service/current-authority behavior. The
full Agent file, Evaluation, Pipeline, complete local instance and production
capacity remain separate required gates. These native artifacts are debug builds.
