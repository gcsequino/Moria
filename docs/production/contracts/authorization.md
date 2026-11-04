# Native authorization kernel contract

Version 1. The kernel is the `moria_runtime::authorization` library. Native
serving and worker services call it directly; it is not an authorization server,
a session verifier, or a per-request subprocess bridge.

## Authority ownership

`Identity` contains `principal`, `application`, `environment`, current
`granted_operations`, `application_scopes`, `markings`, `disabled`, and optional
boxed `delegated` identity. These values must be constructed from current trusted
authority, never accepted as authorization from request JSON. An operation checks
current database/session/filesystem/execution authority before evaluation and
again before releasing results. The kernel cannot establish currency by itself.
The protected-operation owner also records declared and current filesystem
markings and read versions in execution evidence. Pure kernel decisions do not
replace that evidence or reproduce a JavaScript ambient access-flow context.

`authorize(identity, resource, operation, markings)` requires the exact
`resource:operation` in both grants and application scopes for every identity.
Every identity must be enabled, share the same environment and possess every
required marking. There are at most eight delegation edges. Missing permissions
and malformed/disabled/cross-environment delegation return `FORBIDDEN`.

`authorize_destination(identity, labels, policy, destination)` additionally
requires an explicit destination allowlist and a marking allowlist. Every identity
must clear all labels. An absent policy, foreign/disabled delegation, missing
marking or unknown destination returns `DESTINATION_FORBIDDEN`.

## Typed expressions

`ExpressionTypes` and `ExpressionValues` contain `fields`, `parameters` and
`attributes`, each a `BTreeMap<String, serde_json::Value>`. Missing maps default
to empty; unknown wrapper fields are rejected. Type descriptors use the existing
value-model-v1 stored-column profile: exact scalars or one scalar-array level
with non-null elements. Unknown descriptor members and untyped references fail.
Each map has at most 10,000 entries.

`CompiledExpression::compile(&expression, types)` accepts exactly one AST form:

```json
{"literal": true}
{"field": "region"}
{"parameter": "value"}
{"principalAttribute": "region"}
{"op": "eq", "args": [{"field": "region"}, {"principalAttribute": "region"}]}
```

Operators are `and`, `or`, `not`, `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `in`,
`contains`, and `isNull`. Unary operators take one argument, comparisons and
membership take two, and `and`/`or` take at least two. Maximum depth is 16 edges,
maximum node count is 256, and literal scalar arrays contain at most 100 entries.
Extra AST keys, heterogeneous/nested literal arrays, structured literals,
unknown names, invalid arity and a non-boolean root return `POLICY_INVALID`.
Equality to null is invalid; callers use `isNull`. Ordering booleans or arrays
is invalid. Compatible typed string literals preserve exact int64, decimal, UID,
date and timestamp semantics. Scalar column nullability remains enforced;
principal attributes may be null even if their descriptor is non-nullable.

`evaluate(&ExpressionValues) -> Result<Option<bool>>` returns true, false or
unknown (`None`). Missing references are unknown. Only provided, declared values
are validated; unused extra value keys do not affect an expression. Invalid
declared values return `VALUE_INVALID`. `allows(values)` returns true only for
`Some(true)`. Decimal comparison does not convert through float or a scaled
fixed-width integer. Strings use Unicode code-point order; timestamps preserve
UTC microseconds. Membership and boolean operators use SQL three-valued logic.

## Parameterized SQL

`sql(values, field_columns, parameter_offset) -> Result<SqlFragment>` emits
`SqlFragment { text, parameters: Vec<SqlParameter> }`. Field mappings are trusted
internal identifiers matching `[A-Za-z_][A-Za-z0-9_]*`; missing or unsafe mappings
return `POLICY_INVALID`. User field names and values are never SQL identifiers.
Parameter numbering starts at `parameter_offset + 1`; the final index must not
exceed 65,535.

`SqlParameter::Scalar(Option<String>)` binds nullable UTF-8 text.
`SqlParameter::Array(Option<Vec<Option<String>>>)` binds a nullable text array.
`SqlParameter::pg()` exposes the `tokio_postgres::types::ToSql` reference.
Explicit casts select only known scalar/array PostgreSQL types; bigint/decimal
values never pass through floating point. `postgres_type(schema)` returns the
validated target type. String comparison uses PostgreSQL `C` collation. All-null
or empty literal arrays obtain the declared opposite operand's element type.
The caller applies predicates before limiting, counting, ranking, grouping or
returning rows; post-filtering a previously limited result is insufficient.

## Object policy and projection

`ObjectPolicies` declares `resource`, `markings`, `properties`, `source_columns`,
`attributes`, optional `row` and `object_read` expressions, and `property_read`
expressions. Its JSON names use camel case. Row restrictions see canonical source
columns; object restrictions see effective properties; property restrictions see
principal attributes only. Unknown protected properties fail compilation.

`ObjectReader::compile(policy).prepare(identity, attributes, projection,
predicate_properties, delegated_attributes)` authorizes the resource before any
data is inspected. There must be exactly one current attribute map per delegated
identity. Explicit projection and predicate fields must exist and be accessible
to every identity; errors are `PROPERTY_UNKNOWN` or `PROPERTY_FORBIDDEN`, even
for an empty population. An omitted projection selects all permitted properties.

`PreparedProjection::selected()` exposes the selected names.
`project(pinned, current) -> Result<Option<ValueMap>>` consumes `ObjectRow`
values containing `source` and `effective` maps. Both rows and every delegated
attribute context must satisfy canonical and effective restrictions. Missing or
currently deleted/hidden objects return `None`; an overlay cannot broaden a
canonical source restriction. Returned properties come from the pinned effective
row. A prepared projection is scoped to one protected operation and must not be
cached as a continuing permission grant.

## Errors and verification

`PolicyError { code: &'static str }` carries no row values, credentials or policy
details. Codes are `POLICY_INVALID`, `VALUE_INVALID`, `FORBIDDEN`,
`PROPERTY_UNKNOWN`, `PROPERTY_FORBIDDEN`, and `DESTINATION_FORBIDDEN`.

The shared fixture is `packages/contracts/policy-conformance.json` version 1.
Ordinary native unit tests have no ignored cases. The dedicated `native_policy`
test binary has one ignored-by-default `sql_policy_parity` case, invoked
mandatorily by `tests/native-policy.integration.test.ts` with an actual isolated
PostgreSQL URL in `MORIA_POLICY_TEST_DATABASE_URL`. The integration test requires
the native artifact (default `/usr/local/bin/moria-policy-tests`, override
`MORIA_POLICY_TEST_BINARY`), asserts one executed case, and compares TypeScript,
native evaluation and PostgreSQL results. Missing artifacts or database access
fail the gate. No test SQL is mocked.

`CompiledValueExpression::compile(expression, types, expected_schema)` compiles a
typed action value using the same bounded AST. `evaluate(&ExpressionValues)`
returns a C03-normalized scalar or scalar array and rejects a type or nullability
mismatch. Policy `CompiledExpression::compile` continues to require a Boolean
root. Parameter, field and principal-attribute references retain declared types;
literal values must satisfy the exact expected descriptor. No arithmetic or
additional expression operator is introduced.

`conformance(request)` is a bounded pure fixture helper returning evaluation and
parameterized SQL JSON; it does not authenticate users or publish an endpoint.
It accepts `expression`, optional `types`, `values`, `fieldColumns` and
`parameterOffset`. It is used by native fixtures, not production request routing.

This kernel does not implement current-authority transport, operational query
planning, cursor lifecycle, actions, cell ownership or capacity certification.
