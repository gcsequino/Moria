# Moria declaration schemas v0.2

- [resources.schema.json](resources.schema.json): 23 resource kinds, closed metadata/spec objects and a bounded expression grammar.
- [manifest.schema.json](manifest.schema.json): project/deployment-unit identities, resources and pinned contract dependencies.
- [maintenance.json](../examples/maintenance.json): 36 declarations covering every resource kind.
- [invalid-resources.json](../examples/invalid-resources.json): deliberately invalid structural cases.
- [validate.py](validate.py): reproducible schema/static review checks; install [requirements.txt](requirements.txt) for the tested validator and format support.

JSON Schema dialect: Draft 2020-12. Source YAML is permitted only as a JSON-compatible representation: reject duplicate keys, non-string mapping keys, custom tags and implicit timestamp objects. JSON examples avoid loader ambiguity. apiVersion is `moria/v1alpha2`; no compatibility with the old illustrative v1alpha1 syntax is promised. The schema $id uses a nonresolving example domain intentionally; local/offline resolution is required.

All declaration objects reject unknown fields except deliberate extension boundaries: connector configuration and its JSON Schema, automation input, and named field/property maps. Configuration must validate against the chosen connector's schema; secret values must be supplied through SecretReference bindings rather than embedded configuration. A structural schema cannot prove a string contains no secret.

## Compiler obligations beyond JSON Schema

The production compiler must resolve qualified references and contract dependencies; verify namespace/deployment ownership; type expressions and enforce their context/size limits; reject transform cycles and duplicate producers; check dataset/object/link mappings; validate operation names for target kinds; and enforce requested capabilities. Name/reference uniqueness is semantic, not a JSON Schema feature.

It must also enforce decimal scale <= precision; non-null array elements; UID primary keys; required keys for upsert/delete; exactly mapped source properties; link cardinality/key types; property-policy types; input/output action types; immutable keys; allowed model protocols/URI schemes; chunk overlap < size; embedding dimension/model compatibility; and nonempty meaningful policy bindings. Source keys, object UIDs and resource UUIDs are distinct concepts.

Policy field operands refer to the current row (or action's target row). A property-policy expression cannot depend on row values in v0.1. Action `targetParameter` selects the typed input UID identifying its primary target; per-edit UID expressions may reference other inputs, with all target authorization rechecked. Multi-target predicates beyond the primary row require a later typed condition extension or separate actions; a function proposal cannot bypass transaction validation. Creation uses `identityKey` as a request-bound canonical identity key; allocator results are returned with action output.

`validate.py` implements a clearly limited subset of those static checks so the examples do not merely pass a loose shape test. It is a documentation validator, not the platform compiler. It does not contact endpoints, fetch artifacts, register IDs, evaluate grants, enforce runtime policies or deploy resources. The example grant is intentionally illustrative rather than a complete install-time role assignment; artifact/endpoint values are placeholders. The first acceptance fixture will supply the complete grants, real artifact digests and data rows.

Creation-only actions may omit targetParameter and cannot reference an existing target row in their condition. The allocator supplies the new object's primary UID; create values must not set it. Actions affecting existing objects or using a function require targetParameter. These contextual rules are compiler checks.

## Resource-kind coverage

Namespace, Dataset, Transform, ConnectorImplementation, Connection, ObjectType, LinkType, Action, Function, Application, Agent, AgentAdapter, ModelProvider, Model, RetrievalCollection, EvaluationSuite, Automation, Policy, SecretReference, Marking, RestrictedView, Grant and ObjectSet.

RestrictedView is terminal for transform authoring: Transform.inputs must resolve to Dataset resources only. ObjectType.source.resource may resolve to Dataset or RestrictedView. The static validator checks this distinction, and the production compiler must enforce it before execution.

Every Dataset uses Iceberg; a per-resource storage selector is unnecessary in the initial version. Iceberg catalog/snapshot IDs are compiled/runtime metadata, not user-supplied declaration fields.

Resource schemas are authoring inputs. Compiled releases add logical IDs, immutable revisions, bindings, fingerprints and validated policy IR. Runtime objects such as builds, action results, snapshots and events are domain records rather than user resource declarations. Their route/OpenAPI schemas are Phase 1–5 deliverables; this package specifies their required behavior in prose.
