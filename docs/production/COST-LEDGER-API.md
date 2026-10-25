# Offline cost ledger API

API version 1 implements the bounded validation and summary slice of PERF-01c (#27),
using [COST-MODEL.md version 0.2](COST-MODEL.md). The executable fixture is in
[benchmark-cost-ledger.test.ts](../../tests/benchmark-cost-ledger.test.ts).
This is synthetic arithmetic evidence, not a collector, billing integration,
approved budget, provider quote or certification of economic viability. Parent
#20 remains open. This slice depends on the completed source baseline #12; it does
not require arrival accounting #25 or database lifecycle work #13.

## Entry points and envelope

Import `summarizeCostLedger`, `formatCost`, and the exported TypeScript interfaces
from `benchmarks/cost-ledger.ts` (`.js` in NodeNext imports).

- `summarizeCostLedger(input: unknown): LedgerResult` validates JSON-shaped data
  without filesystem, network, clock, provider, model or runtime access. It returns
  `{ ok: false, errors: [{ path, message }] }` for the first invalid field, or
  `{ ok: true, summary }`. Correct an error and resubmit to find subsequent errors.
  Validation never mutates caller-owned data, including rejected or frozen inputs.
- `formatCost(value: Exact, decimalPlaces = 2, maxInputLength = 161): string` formats a known exact value
  for display only, with half-up rounding and 0–20 decimal places. It throws on
  malformed values, invalid precision or invalid limits. It cannot format `null`
  unknowns. By default it rejects values exceeding 161 characters. Exact aggregates
  and ratios can grow beyond that limit; callers must explicitly opt in with a
  larger `maxInputLength`, a positive safe integer bounding the entire exact string
  (including any fraction separator). For a known non-null generated result, use
  `formatCost(result, 2, Math.max(161, result.length))`, or choose a fixed larger
  application limit. Numeric syntax validation and exact half-up rounding still
  apply. This option never changes the ledger validator's 161-character limit.

The required envelope is `CostLedger`:

| Field | Contract |
|---|---|
| `version` | Literal string `"1"`; numeric versions are rejected |
| `currency` | One case-sensitive declared code matching `[A-Z][A-Z0-9]{1,11}`, including fictional `CU`; every charge must match |
| `window` | `start`, `end`: valid UTC `YYYY-MM-DDTHH:mm:ss.sssZ`, with end strictly later than start |
| `context` | Nonempty `run`, `workload`, `profileRevision`, `topology`, `location`, `ratesSource`, `reservedCapacityTreatment`, `taxTreatment`, `staffingAssumptions`; `pricingDate` is a valid `YYYY-MM-DD` |
| `charges` | Nonempty array of uniquely identified charges |
| `ratios` | Array of ratio requests, including `[]` when none are requested; one request per metric |

All charges are attributed to this envelope's window. Submit separate envelopes
for distinct windows or currencies. Mixed currencies are rejected even if a caller
supplies an exchange-rate field; conversion is outside version 1. Date validation
rejects nonexistent calendar dates rather than allowing JavaScript date rollover.
Window and pricing dates are preserved; pricing may predate the usage window.
Context documents billing, commitment and staffing assumptions without judging them.

Each `Charge` requires:

- `id`, `currency`, `category`, nonempty unique `purposes`, and nonempty `notes`.
  IDs, tags and attribution keys are case-sensitive; strings are not trimmed or
  rewritten. Purposes are open vocabulary, e.g. `serving`, `ingest`, `transform`,
  `analytics`, `backup`, `DR`, `shared control`.
- Exact `usage`, `unit`, nullable `rate`, `rateUnit`, nullable `amount`.
- `rateProvenance: { source, date }` with a nonempty source and valid `YYYY-MM-DD`.
  For an unknown rate, identify the missing source/assumption and its as-of date;
  do not fabricate a quotation. This metadata is still required.
- `status: "measured" | "estimated"`, preserved per row. It labels the submitted
  evidence, not a claim that a rate was observed. Provenance separately describes
  assumed rates. Mixed measured and estimated rows remain visible in the summary.
- Nonempty `allocations: [{ kind, key, percent }]`. `kind` is `workload`, `shared`
  or `unallocated`; `key` is nonempty and unique within its kind per row. Percentages
  must sum to exactly 100. Explicit shared or unallocated allocations record
  uncertain attribution. Zero-percent entries are permitted and contribute no
  cost or unknowns. A missing allocation array is not automatic shared attribution.

Categories are exactly `compute`, `storage capacity`, `storage operations`,
`network`, `software/support`, `facility/power`, and `labor`. Required fields cannot
be omitted or replaced with `undefined`. Unknown extra fields are not interpreted
or retained; keep evidence needed for interpretation in the documented fields.
The API accepts ordinary JSON data, not executable objects, getters or proxies.

## Exact numbers and units

`Exact` is a string containing a nonnegative integer, decimal, or integer fraction:
`"24"`, `"0.50"`, `"1/15"`. Ledger input strings are limited to 161 characters. No signs,
exponents, whitespace, leading integer zeroes, numeric JavaScript values, NaN,
infinity, or zero fraction denominators are accepted. Decimal trailing zeroes are
allowed. Fractions need not be reduced on input. Percentages and ratio denominators
use the same representation. Null is allowed only for rates, amounts and ratio
denominator values; it means unknown, not zero.

Supported usage/rate units are:

| Units | Meaning |
|---|---|
| `node-hour`, `hour`, `GiB-hour` | Node, elapsed/labor, or memory time; GiB = 2^30 bytes |
| `B`, `GB`, `TB`, `PB` | Decimal data volume: 10^0, 10^9, 10^12, 10^15 bytes |
| `TB-month` | Integrated physical byte-hours / (10^12 × 720), a normalized 30-day month |
| `request`, `million requests` | Resource usage requests, possibly including retries; one million = 10^6 |
| `license-hour`, `kWh` | Software time and energy meters |

`rateUnit` must exactly equal `unit`. A rate is currency per that unit. There are
no implicit byte, request, time, replication or exchange conversions. Record any
conversion in notes and supply exact converted quantities. Actual calendar-month
invoices are not silently treated as normalized 720-hour months.

Calculations use reduced BigInt rational numbers throughout. When both rate and
amount are known, `usage × rate = amount` must hold exactly; there is no tolerance.
Arithmetic, percentage allocation, sums and divisions never round. Summary numeric
results are canonical strings: integers when denominator is 1, otherwise reduced
`numerator/denominator`. The returned ledger preserves submitted numeric strings.
`formatCost` is independent presentation logic. Never submit rounded display values
as exact evidence; in particular the rounded storage rows in the contract table
must be represented by fractions.

## Summary and unknown evidence

The result contains a detached `ledger` preserving the documented metadata, and
`platform`, `categories`, `allocations`, `purposes`, and `ratios`. Charges sort by ID;
purpose tags and ratio metrics sort lexically; allocations sort by kind then key.
Categories use the exported category order. Sorting uses code-unit comparisons,
not locale settings. Reordering input arrays does not change the summary.

Every cost aggregate is:

```ts
{
  knownSubtotal: '473/10',
  total: null,
  unknowns: [{ chargeId: 'transform', fields: ['rate', 'amount'] }]
}
```

Known amounts contribute to `knownSubtotal` once, even if their rate is unknown.
`total` is non-null only when every contributing row has both a known rate and
amount. Thus an amount with an unknown rate can have a full-looking subtotal while
still lacking complete arithmetic evidence. An unknown amount is never inferred
from usage and rate and is never counted as zero. Zero usage does not erase missing
rates or amounts. Each allocation receives its exact percentage of the known
amount; unknown fields propagate to each positive allocation. Category summaries
include empty categories at zero; an absent category/allocation numerator produces
an unavailable ratio rather than inferred zero unit cost.

Platform and category totals count each unique charge once. Allocation shares
partition each row exactly. Purpose summaries are **overlapping filtered views**;
a backup row tagged both `backup` and `DR` appears in both views but only once in
the platform/category totals. Do not add purpose subtotals or ratios to platform
cost. The validator cannot detect one real resource submitted under different IDs,
a separately entered component already bundled in a managed-service charge, or
double-entered hardware purchase and amortization. These remain evidence-review
obligations under the cost contract.

## Declared unit-cost ratios

A `RatioRequest` declares `metric`, `numerator: { kind, key }`, and
`denominator: { value, unit, evidence }`. Evidence is a required nonempty description
of the denominator for this window. The following exact pairings are enforced:

| Metric | Numerator kind:key | Denominator unit and required meaning |
|---|---|---|
| `retention` | `category:storage capacity` | `TB-month`: integrated physical occupancy, with protection/copy layout in evidence |
| `transformation` | `allocation:transformation` | `TB`: decimal TB read into transform stages |
| `ingestion` | `allocation:ingestion` | `TB`: unique source payload durably accepted, excluding retransmissions |
| `reads` | `allocation:reads` | `million successful reads`: successful logical reads, excluding retries |
| `actions` | `allocation:actions` | `million successful actions`: successful logical actions, excluding retries |
| `fully-loaded-unique-input` | `total:platform` | `TB`: unique source input; separately labeled fully loaded cost |

Allocation numerator selectors match only `kind: "workload"` entries; shared or
unallocated entries with the same key cannot be mistaken for attributed work.
Transformation additionally requires
`transformEvidence: { uniqueSourceTB, passCount, retryReadTB }`, all exact values;
pass count must be integral. Stage-read volume and retry-read volume are declared
independently; the API does not infer a successful-work count from either or assume
that every stage reads the same volume. Document whether retries are included in
the stage-read denominator in its evidence. Read/action retries increase resource
cost, never successful logical operation counts. Request usage is not reused as
a success denominator.

Each returned ratio includes its actual numerator aggregate, denominator, exact
`value`, and `unavailable`. Unavailability is checked in this order: `missing denominator` (null), `zero denominator`, `missing numerator`, and
`incomplete numerator`. A valid ratio has
`unavailable: null`. Ratios are never summed. Declared denominator units and
numerator selectors are checked, but the API cannot verify empirical counters,
copy layout or the truth of the evidence text. No arrival-accounting integration
is claimed. Idle, marginal burst and recovery comparisons require additional
baseline/window contracts and are outside this API version.

## Examples

A complete single-charge ledger:

```ts
import { summarizeCostLedger, formatCost } from '../../benchmarks/cost-ledger.js';

const result = summarizeCostLedger({
  version: '1',
  currency: 'CU',
  window: { start: '2026-10-23T00:00:00.000Z', end: '2026-10-24T00:00:00.000Z' },
  context: {
    run: 'example', workload: 'synthetic', profileRevision: '0.2',
    topology: 'fictional', location: 'fictional', pricingDate: '2026-10-24',
    ratesSource: 'invented', reservedCapacityTreatment: 'none',
    taxTreatment: 'excluded', staffingAssumptions: 'not included in this example',
  },
  charges: [{
    id: 'transform', currency: 'CU', category: 'compute', purposes: ['transform'],
    usage: '12', unit: 'node-hour', rate: '0.75', rateUnit: 'node-hour', amount: '9',
    rateProvenance: { source: 'invented', date: '2026-10-24' },
    status: 'estimated', notes: 'Synthetic arithmetic only',
    allocations: [{ kind: 'workload', key: 'transformation', percent: '100' }],
  }],
  ratios: [{
    metric: 'transformation',
    numerator: { kind: 'allocation', key: 'transformation' },
    denominator: { value: '3', unit: 'TB', evidence: 'Three stages; no retries' },
    transformEvidence: { uniqueSourceTB: '1', passCount: '3', retryReadTB: '0' },
  }],
});
if (result.ok && result.summary.platform.total !== null) {
  formatCost(result.summary.platform.total); // '9.00'
  result.summary.ratios[0].value; // '3' CU/TB
}
```

The test fixture reproduces the complete fictional 24-hour contract example:

| Charge | Exact usage | CU/unit | Exact CU | Allocation |
|---|---:|---:|---:|---|
| Operational compute | 48 node-hour | 0.50 | 24 | serving |
| Transform compute | 12 node-hour | 0.75 | 9 | transformation |
| Primary storage | 1/15 TB-month | 20 | 4/3 | retention |
| Backup storage | 1/30 TB-month | 20 | 2/3 | retention |
| Storage operations | 2 million requests | 0.40 | 0.80 | unallocated:shared/unallocated |
| Network transfer | 0.10 TB | 5 | 0.50 | unallocated:shared/unallocated |
| Operations labor | 0.50 hour | 40 | 20 | unallocated:shared/unallocated |

Each allocation is 100%. Storage quantities derive from 2 TB × 24/720 and
1 TB × 24/720, totaling 1/10 normalized TB-month. Total is `563/10`, displayed as
**56.30 CU**. Capacity cost is **2 CU / 0.10 TB-month = 20 CU/TB-month**.
Transformation is **9 CU / 3 decimal TB = 3 CU/TB**, with 1 TB unique source input,
three passes and zero retry reads separately recorded. The fully loaded
unique-input ratio is **56.30 CU/TB**, never added to the transformation ratio.
The 21.30 CU remainder is explicitly unallocated. Backup is already in the total,
including when its row is tagged both backup and DR. Six-place display rounding
of storage amounts yields 1.333333 and 0.666667; validation uses 4/3 and 2/3.

## Implementation and verification record

Scope is limited to the three new files `benchmarks/cost-ledger.ts`,
`tests/benchmark-cost-ledger.test.ts` and this document. No dependencies, scripts,
runtime code, production contracts, shared progress files or delivery policy are
changed. Per the bounded task instruction, progress is recorded here instead of
`docs/IMPLEMENTATION.md`. No other scope deviations.

Baseline revision: `7633bc20e67abd9fdaa7855d3976e364980e1fd4`, branch
`delivery/issue-27`. Changes remain uncommitted for controller publication; this
baseline is not a claim of a final committed revision or a Linux CI pass.
Environment: non-root Linux user `openhands` (UID 10001), Node 24.21.0,
pnpm 11.25.0, repository `.venv` Python 3.11.17. Fixtures use only synthetic data.
No credentials, live installation, model inference or external service is needed
by this module.

Verification completed (2026-10-24) against the uncommitted implementation:

- `pnpm format`: completed. Only allowed new TypeScript files changed.
- `pnpm typecheck`: passed.
- `pnpm exec tsx --test tests/benchmark-cost-ledger.test.ts`: initial run found a
  test assertion comparing original versus sorted purpose order; assertion corrected
  to the documented canonical order. Final focused run: all 79 tests passed,
  zero failures or skips.
- Complete `pnpm check`: passed (exit 0) on supported non-root Linux. Formatting,
  SDK drift, TypeScript checks and all 166 TypeScript tests passed (zero failures,
  zero skips), including the existing real database tests. The Python formatting,
  schema/workload checks and 14 Python tests passed using the workspace `.venv`.
  Both application builds and Station Explorer type/build/hosting checks passed.
  All existing database and isolation tests remained enabled and unchanged.
- `git diff --check`: passed; final status contains only the three permitted new
  files. No commits, pushes, PR creation, deployment or live-data operations.

Offline coverage includes the synthetic ledger, metadata preservation and detached
output, reordered inputs, duplicate IDs/allocations/purposes, invalid numbers,
units/dates/windows, exact arithmetic, under/over allocation, overlapping tags,
currency mismatch, explicit unknowns, matching ratio selectors, absent/zero
denominators, successful-work counts, and display rounding. Rejected inputs are
frozen and compared with a pre-validation copy. This evidence does not certify
production throughput, costs, topology, accounting completeness, or runtime
integration. Linux quality CI must still pass on the exact final published
revision before human review and manual merge; that controller-owned gate is
unverified here.

### Display-limit review repair (2026-10-24)

`formatCost` retains its default 161-character guard and boundary regression.
A validated third argument explicitly permits larger generated totals and ratios,
without changing ledger input limits, numeric syntax or rounding rules. Regression
coverage uses two accepted 161-digit charges to produce a 162-digit total and a
larger fractional ratio through a declared small denominator. Both reject default
formatting and succeed with an explicit sufficient limit; insufficient and invalid
limits still reject. Malformed values reject with either default or larger limits.

The repair remains within the three permitted files, with no scope deviations.
`pnpm format` and `pnpm exec tsx --test tests/benchmark-cost-ledger.test.ts` passed
(83 tests, no failures or skips). Complete `pnpm check` passed (exit 0) for this
repair on the non-root Linux environment recorded above: formatting, SDK drift,
typecheck, all 170 TypeScript tests (no failures or skips), Python formatting and
schema/workload checks, all 14 Python tests using `.venv`, both application builds,
and Station Explorer checks. Existing database and isolation tests stayed enabled
and unchanged. `git diff --check` passed. Changes remain uncommitted; Linux quality
CI on the final published revision remains unverified and controller-owned.
