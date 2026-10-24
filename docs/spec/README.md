> October 24, 2026: this package records the historical first-slice review. The proposed long-term production extension and contract disposition are in [docs/production](../production/README.md), tracked by [M0](https://github.com/gcsequino/Moria/issues/2). Existing semantic guarantees are preserved pending explicit review.

# Moria — implementation-readiness package

Prepared October 14, 2026. Status: **reviewed defaults accepted, Iceberg adopted, restricted-view boundary finalized; no platform implementation yet**.

This package turns the foundation into implementable contracts. It preserves the accepted scope in [reference/accepted-scope.md](reference/accepted-scope.md). The user accepted the unchallenged defaults, selected Iceberg, and named the project Moria. Restricted views are finalized as terminal data-consumption resources that cannot be used as transform inputs. This package replaces conflicting foundation v0.1 proposals; its implementation mechanisms still require runtime validation and are not claims about Palantir internals. The historical research pack remains evidence/background.

## Read in this order

1. [Decisions and follow-up](review-decisions.md) — accepted defaults, including the finalized restricted-view boundary; [Iceberg and row-security research](storage-and-row-security.md) explains the changes.
2. [Core contracts](core-contracts.md) — identities, data, queries, actions, deployment and recovery.
3. [Security contracts](security-contracts.md) — markings, restricted views, granular policies and AI data handling.
4. [Runtime and AI contracts](runtime-contracts.md) — domain lifecycles, extension boundaries and gateway behavior.
5. [Architecture decisions](architecture.md) — stack, alternatives, risks and primary-source references.
6. [Implementation and acceptance plan](implementation-plan.md) — phases, test cases and release gates.
7. [Schema guide](schemas/README.md), [resource schema](schemas/resources.schema.json), [example manifest](examples/maintenance.json) and [validation report](validation-report.md).

## What is settled

- Own a fully open source, self-hostable data platform; CLI-first, local authoring, generated OSDK shared by React frontends and agents.
- No hosted IDE, visual authoring system, or arbitrary user API hosting; no bundled production connectors initially.
- Domain-specific execution commands, one platform implementation monorepo, independently deployable user repositories.
- Markings, restricted views and object/property policies in the first completed working slice.
- One Linux host and a single trusted organization initially.
- Project name Moria; Apache-2.0 for original implementation code.
- Apache Iceberg tables with Parquet data files, not a proprietary dataset manifest format.
- Permanent object UIDs. Returning UIDs restore edits. Transforms consolidate multiple source records into one canonical object record.

## Important review boundaries

The first completed slice includes security and AI, but earlier engineering phases are internal milestones, not a reduced product promise. Exact version pins and image digests are an implementation-phase deliverable after compatibility checks; this review does not assert any installed or benchmarked stack. The schemas validate declaration shape. Cross-resource and runtime rules remain compiler/server responsibilities, explicitly listed in the schema guide.

No wire compatibility with Foundry or existing Palantir SDKs is promised. Our generated SDK is called OSDK descriptively; the project is Moria and the CLI executable will be `moria`. Behavioral differences must be tracked against reference research. The chosen database and frameworks are our implementation choices.

## Readiness outcome

The package records accepted defaults for the previously identified blockers, including the finalized prohibition on restricted views as transform inputs. New details from the Iceberg revision are documented explicitly. Phase 0 proves the risky mechanisms before the larger implementation; failed probes require revising the relevant decision rather than quietly weakening a contract. Security and transactional acceptance cases must pass before the first working slice is called complete.
