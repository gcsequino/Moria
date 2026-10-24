# Specification validation report

Executed locally for the review package. This is not a platform runtime test report.

- JSON Schema Draft 2020-12 meta-validation: 2 schemas passed.
- Resource examples: 36 passed; all 23 declared kinds covered.
- Negative structural cases: 12 rejected as expected.
- Static cross-resource checks: example manifest passed.
- Negative semantic cases: 12 structurally valid cases rejected as expected.
- Local Markdown file links: 19 resolved.
- Validator: Python jsonschema 4.26.0, format checking enabled; schema references resolved offline.

Install `schemas/requirements.txt`, then run `python schemas/validate.py` from this package. The validator rewrites this report. The example uses placeholder artifact digests, endpoints, project IDs and UIDs; it is not a deployable platform.

Not tested: SQL policy enforcement, transaction races, worker isolation, model integrations, performance, installation or restore. Full expression typing, current grants, dependency packages, ownership and server-side execution checks are future compiler/runtime work. No Foundry reference conformance was executed.
