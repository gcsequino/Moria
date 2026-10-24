## Change and scope

Link the bounded issue or acceptance criteria. Describe observable behavior and preserved contracts.

## Verification

Record the source revision, commands, results, and any unverified checks. Include regression evidence for changed behavior. `pnpm check` and both Quality jobs must pass on the latest revision before merge.

## Review and operations

Identify migration, authorization, publication, retention, or recovery implications. Production operations require a separately reviewed plan. A human reviews and merges; a passing hook is not merge authorization.
