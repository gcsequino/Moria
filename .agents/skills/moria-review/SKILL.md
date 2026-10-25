---
name: moria-review
description: Independently review a Moria change for concrete correctness regressions and contract violations, with revision-bound evidence.
---

# Moria independent review

Use a reviewer separate from the implementer. When the controller already owns a
review slot, use that review rather than launching a duplicate. Otherwise request
a fresh reviewer with the acceptance criteria, base/head revisions, full diff,
repository instructions and test evidence. Do not supply a desired verdict.

Review is read-only. Read the complete diff, relevant full functions, callers,
tests and applicable accepted contracts in docs/spec. Verify the base and head
before reviewing. If content is truncated, fetch the omitted files; if unavailable,
mark the review incomplete rather than issuing a whole-change correctness verdict.

Check the scoped rules in AGENTS.md. Follow data through authorization boundaries,
transaction/publication paths and failure handling where affected. Distinguish
the effect of the change from pre-existing limitations. Read test assertions;
passing tests alone do not establish that the requested behavior was tested.

For each actionable finding give severity, file/line, triggering conditions,
observable consequence, and a suggested validation. Do not ask for speculative
abstractions or stylistic preferences. Report no findings when warranted, with
coverage and limitations. Do not convert classifier confidence into defect facts.

Return base/head, reviewed files, findings and tests actually run. Renew the review
after repairs or base integration; previous findings can guide the next pass but
an old verdict cannot approve new code. The reviewer does not merge or deploy.
