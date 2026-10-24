# Moria — Iceberg and restricted-view follow-up

Status: resolved. Iceberg and project name accepted; D03 finalized as terminal restricted views that cannot be transform inputs. The options below are historical research, not outstanding implementation requirements. Research date: October 14, 2026. This note distinguishes reference evidence from our proposed behavior.

## Iceberg decision

Use Apache Iceberg tables with Parquet data files. Iceberg supplies table metadata, snapshots and manifests; Parquet remains a file encoding underneath. This replaces the previous bespoke-manifest proposal. The relevant standard is the [Iceberg format specification](https://iceberg.apache.org/spec/).

DuckLake is an alternative with a SQL metadata catalog and DuckDB integration. That is attractive for one host, but it does not establish a better fit than the requested Iceberg boundary. We have no benchmark or concrete incompatibility justifying a substitution. Keep Iceberg, and test the catalog/reader integration before implementation builds on it. [DuckLake introduction](https://ducklake.select/docs/stable/duckdb/introduction).

The accepted all-or-nothing output/checkpoint contract needs explicit integration: stage standard Iceberg metadata, then have Moria's PostgreSQL catalog authority atomically publish metadata pointers together with output heads, lineage and checkpoints. Do not assume that independent table commits followed by a Moria transaction are equivalent. Iceberg's documented Java transaction groups operations on a table; that alone does not prove our broader atomicity. [Iceberg transaction API](https://iceberg.apache.org/docs/latest/api/). Phase 0 must prove the chosen adapter, including loss of acknowledgment, competing writers and snapshot retention. PyIceberg exposes SQL and custom catalog options, but a shared transaction with Moria bookkeeping remains work to validate. [PyIceberg configuration](https://py.iceberg.apache.org/configuration/).

## What current Foundry documentation says

The official management guide explicitly excludes restricted views from batch derivation that writes datasets. Its rationale includes caller-dependent results and inability to ensure policy columns survive downstream transforms. It does allow analytical consumption; transformed analysis and saved batch output are different capabilities. [Manage restricted views](https://www.palantir.com/docs/foundry/platform-security-management/manage-restricted-views).

The restricted-view concept page also documents **marking-backed restricted views**: the backing dataset contains string-array columns of marking IDs, and the view compares those IDs with the reader's markings. That could be the column-based workflow the user remembers; this is a possible explanation, not confirmation of a past instance's behavior. [Restricted views, including marking-backed views](https://www.palantir.com/docs/foundry/security/restricted-views).

The propagation guide distinguishes mandatory controls from read filters. Read-layer row/property policies do not automatically travel with returned bytes. [Access-control propagation](https://www.palantir.com/docs/foundry/security/access-control-propagation).

## Proposed Moria pattern: preserve policy inputs, enforce on output

For the first version, retain caller-independent builds:

```text
protected source datasets + policy attributes
                |
     authorized transform service identity
     (join / consolidate / carry policy attributes)
                |
        protected output Iceberg dataset
                |
        output RestrictedView policy
                |
       each reader's authorized objects/rows
```

Example: source records contain `region`. A transform derives work-order summaries while preserving the appropriate region information. The output view compares that information with the current reader's region. A build does not run once for Alice and persist only Alice's visible subset; different readers receive different subsets of the same completed output.

This pattern fits the existing source-policy AST and the accepted transform-owned canonicalization boundary. It requires explicit output view/policy declarations, matching-policy columns, and tests showing denied readers cannot access the raw output dataset. The source RestrictedView itself is not the transform input. This proposed clarification does not by itself remove the current direct-input prohibition.

## Why automatic preservation needs more than keeping a column

| Operation | Conservative rule for a future automatic mechanism |
|---|---|
| Filter / projection / scalar derivation | Preserve all policy inputs needed to protect each contributing row; dropping them fails validation. |
| Union | Keep each row's own security provenance. Different source policies cannot silently collapse to one weaker policy. |
| Join / canonical coalescing | Derived rows must satisfy restrictions from all contributing sensitive records. For conjunctive marking labels, union required labels; do not choose one source's labels because its value won coalescing. |
| Aggregation | Restrict a result according to all contributors, or compute an explicitly defined caller-specific aggregate. Never silently expose a restricted contribution through a total. |
| Outer/anti join, absence test, ranking or window | Information may depend on rows not present in the returned output. Row lineage alone is insufficient; require a proven operator rule or conservative broader restriction. |
| Arbitrary Python / UDF | Moria cannot infer complete information dependencies. Reject claims of automatic propagation, or retain conservative whole-input protection. |

For region equality, combining East and West into one result cannot simply select East as the output region. One option is to preserve a set of required regions and require eligibility for all; another is to aggregate separately by region. The correct choice is explicit domain logic. An all-required-set operator would need a typed policy/schema extension; it is not silently added here.

Marking-ID columns are data unless the platform validates and enforces them. They do not replace mandatory dataset markings. Under the accepted union rule, a dataset derived from A- and B-marked sources remains A+B marked even if individual rows list only A or B. Letting a reader with only A see selected rows would require a separately authorized serving/declassification boundary, with audited label completeness and current checks. It must not happen because a transform dropped B from an array. That broader row-label security mode remains a separate design choice.

## Final decision and status

The user selected the simpler terminal boundary: restricted views cannot be Transform inputs. They remain usable for authorized reads and Ontology backing, and are not frozen snapshots. Transform authors operate on separately authorized Dataset inputs and can expose final outputs through restricted views. No automatic restricted-view policy propagation feature is scheduled.

D03 is closed and recorded in the security contract and compiler acceptance cases. The earlier policy-preservation options are retained solely as research history.
