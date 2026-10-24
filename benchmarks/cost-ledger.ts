/** Offline cost accounting. Numeric values are exact nonnegative decimal/fraction strings. */
export type Exact = string;
export const categories = [
  'compute',
  'storage capacity',
  'storage operations',
  'network',
  'software/support',
  'facility/power',
  'labor',
] as const;
export const units = [
  'node-hour',
  'hour',
  'GiB-hour',
  'B',
  'GB',
  'TB',
  'PB',
  'TB-month',
  'request',
  'million requests',
  'license-hour',
  'kWh',
] as const;
export type Category = (typeof categories)[number];
export type Unit = (typeof units)[number];
export interface Allocation {
  kind: 'workload' | 'shared' | 'unallocated';
  key: string;
  percent: Exact;
}
export interface Charge {
  id: string;
  currency: string;
  category: Category;
  purposes: string[];
  usage: Exact;
  unit: Unit;
  rate: Exact | null;
  rateUnit: Unit;
  amount: Exact | null;
  rateProvenance: { source: string; date: string };
  status: 'measured' | 'estimated';
  notes: string;
  allocations: Allocation[];
}
const metricDefinitions = {
  retention: { kind: 'category', key: 'storage capacity', unit: 'TB-month' },
  transformation: { kind: 'allocation', key: 'transformation', unit: 'TB' },
  ingestion: { kind: 'allocation', key: 'ingestion', unit: 'TB' },
  reads: { kind: 'allocation', key: 'reads', unit: 'million successful reads' },
  actions: { kind: 'allocation', key: 'actions', unit: 'million successful actions' },
  'fully-loaded-unique-input': { kind: 'total', key: 'platform', unit: 'TB' },
} as const;
export type Metric = keyof typeof metricDefinitions;
export interface RatioRequest {
  metric: Metric;
  numerator: { kind: 'category' | 'allocation' | 'total'; key: string };
  denominator: { value: Exact | null; unit: string; evidence: string };
  /** Required for transformation: unique source input, stage passes and retry reads separately. */
  transformEvidence?: { uniqueSourceTB: Exact; passCount: Exact; retryReadTB: Exact };
}
export interface CostLedger {
  version: '1';
  currency: string;
  window: { start: string; end: string };
  context: {
    run: string;
    workload: string;
    profileRevision: string;
    topology: string;
    location: string;
    pricingDate: string;
    ratesSource: string;
    reservedCapacityTreatment: string;
    taxTreatment: string;
    staffingAssumptions: string;
  };
  charges: Charge[];
  ratios: RatioRequest[];
}
export interface CostTotal {
  knownSubtotal: Exact;
  total: Exact | null;
  unknowns: { chargeId: string; fields: ('rate' | 'amount')[] }[];
}
export interface CostSummary {
  ledger: CostLedger;
  platform: CostTotal;
  categories: { category: Category; cost: CostTotal }[];
  allocations: { kind: Allocation['kind']; key: string; cost: CostTotal }[];
  /** Overlapping filtered views; never sum these to obtain the platform total. */
  purposes: { purpose: string; cost: CostTotal }[];
  ratios: {
    metric: Metric;
    value: Exact | null;
    unavailable:
      | 'missing denominator'
      | 'zero denominator'
      | 'incomplete numerator'
      | 'missing numerator'
      | null;
    numerator: CostTotal;
    denominator: RatioRequest['denominator'];
  }[];
}
export type LedgerResult =
  { ok: true; summary: CostSummary } | { ok: false; errors: { path: string; message: string }[] };

type Rational = { n: bigint; d: bigint };
const zero: Rational = { n: 0n, d: 1n };
function rational(n: bigint, d: bigint): Rational {
  let a = n;
  let b = d;
  while (b !== 0n) {
    [a, b] = [b, a % b];
  }
  return { n: n / a, d: d / a };
}
function add(a: Rational, b: Rational): Rational {
  return rational(a.n * b.d + b.n * a.d, a.d * b.d);
}
function multiply(a: Rational, b: Rational): Rational {
  return rational(a.n * b.n, a.d * b.d);
}
function encode(a: Rational): Exact {
  return a.d === 1n ? String(a.n) : `${a.n}/${a.d}`;
}
class Invalid extends Error {
  constructor(
    readonly path: string,
    message: string,
  ) {
    super(message);
  }
}
function fail(path: string, message: string): never {
  throw new Invalid(path, message);
}
function exact(value: unknown, path: string): Rational {
  if (typeof value !== 'string' || !/^(?:0|[1-9]\d*)(?:\.\d+|\/[1-9]\d*)?$/.test(value)) {
    fail(
      path,
      'Expected a nonnegative decimal or integer fraction string (at most 161 characters); no numbers, signs or exponents.',
    );
  }
  if (value.includes('/')) {
    const [n, d] = value.split('/');
    return rational(BigInt(n), BigInt(d));
  }
  const [whole, fraction = ''] = value.split('.');
  return rational(BigInt(whole + fraction), 10n ** BigInt(fraction.length));
}
function object(value: unknown, path: string): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    fail(path, 'Expected an object.');
  }
  return value as Record<string, unknown>;
}
function string(value: unknown, path: string): string {
  if (typeof value !== 'string' || value.trim().length === 0) {
    fail(path, 'Expected a nonempty string.');
  }
  return value;
}
function array(value: unknown, path: string): unknown[] {
  if (!Array.isArray(value)) {
    fail(path, 'Expected an array.');
  }
  return value;
}
function choice<T extends string>(value: unknown, values: readonly T[], path: string): T {
  if (typeof value !== 'string' || !values.includes(value as T)) {
    fail(path, `Expected one of: ${values.join(', ')}.`);
  }
  return value as T;
}
function date(value: unknown, path: string, timestamp = false): string {
  const text = string(value, path);
  const pattern = timestamp
    ? /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/
    : /^\d{4}-\d{2}-\d{2}$/;
  const parsed = new Date(timestamp ? text : `${text}T00:00:00.000Z`);
  if (
    !pattern.test(text) ||
    !Number.isFinite(parsed.getTime()) ||
    parsed.toISOString() !== (timestamp ? text : `${text}T00:00:00.000Z`)
  ) {
    fail(
      path,
      timestamp
        ? 'Expected a valid UTC timestamp YYYY-MM-DDTHH:mm:ss.sssZ.'
        : 'Expected a valid date YYYY-MM-DD.',
    );
  }
  return text;
}
function numeric(value: unknown, path: string): Exact {
  if (typeof value === 'string' && value.length > 161) {
    fail(path, 'Exact input strings must have at most 161 characters.');
  }
  exact(value, path);
  return value as string;
}
function nullable(value: unknown, path: string): Exact | null {
  return value === null ? null : numeric(value, path);
}
function compare(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}
function unique(values: string[], path: string): void {
  if (new Set(values).size !== values.length) {
    fail(path, 'Duplicate identifiers are not allowed.');
  }
}
function parse(input: unknown): CostLedger {
  const root = object(input, '$');
  const currency = string(root.currency, 'currency');
  if (!/^[A-Z][A-Z0-9]{1,11}$/.test(currency)) {
    fail('currency', 'Use a declared uppercase currency code, including fictional CU.');
  }
  const window = object(root.window, 'window');
  const start = date(window.start, 'window.start', true);
  const end = date(window.end, 'window.end', true);
  if (start >= end) {
    fail('window.end', 'Window end must be later than start.');
  }
  const context = object(root.context, 'context');
  const parsedContext = {
    run: string(context.run, 'context.run'),
    workload: string(context.workload, 'context.workload'),
    profileRevision: string(context.profileRevision, 'context.profileRevision'),
    topology: string(context.topology, 'context.topology'),
    location: string(context.location, 'context.location'),
    pricingDate: date(context.pricingDate, 'context.pricingDate'),
    ratesSource: string(context.ratesSource, 'context.ratesSource'),
    reservedCapacityTreatment: string(
      context.reservedCapacityTreatment,
      'context.reservedCapacityTreatment',
    ),
    taxTreatment: string(context.taxTreatment, 'context.taxTreatment'),
    staffingAssumptions: string(context.staffingAssumptions, 'context.staffingAssumptions'),
  };
  const charges = array(root.charges, 'charges')
    .map((value, index): Charge => {
      const path = `charges[${index}]`;
      const row = object(value, path);
      const rowCurrency = string(row.currency, `${path}.currency`);
      if (rowCurrency !== currency) {
        fail(
          `${path}.currency`,
          `Must match ledger currency ${currency}; exchange conversion is unsupported.`,
        );
      }
      const unit = choice(row.unit, units, `${path}.unit`);
      const rateUnit = choice(row.rateUnit, units, `${path}.rateUnit`);
      if (unit !== rateUnit) {
        fail(
          `${path}.rateUnit`,
          'Rate unit must exactly match usage unit; convert explicitly before submitting.',
        );
      }
      const usage = numeric(row.usage, `${path}.usage`);
      const rate = nullable(row.rate, `${path}.rate`);
      const amount = nullable(row.amount, `${path}.amount`);
      if (
        rate !== null &&
        amount !== null &&
        encode(multiply(exact(usage, path), exact(rate, path))) !== encode(exact(amount, path))
      ) {
        fail(
          `${path}.amount`,
          'Amount must equal usage × rate exactly, without intermediate rounding.',
        );
      }
      const allocations = array(row.allocations, `${path}.allocations`)
        .map((value, index): Allocation => {
          const ap = `${path}.allocations[${index}]`;
          const item = object(value, ap);
          return {
            kind: choice(item.kind, ['workload', 'shared', 'unallocated'], `${ap}.kind`),
            key: string(item.key, `${ap}.key`),
            percent: numeric(item.percent, `${ap}.percent`),
          };
        })
        .sort((a, b) => compare(a.kind, b.kind) || compare(a.key, b.key));
      unique(
        allocations.map((a) => JSON.stringify([a.kind, a.key])),
        `${path}.allocations`,
      );
      if (
        encode(allocations.reduce((sum, a) => add(sum, exact(a.percent, path)), zero)) !== '100'
      ) {
        fail(`${path}.allocations`, 'Allocation percentages must sum to exactly 100 per charge.');
      }
      const purposes = array(row.purposes, `${path}.purposes`)
        .map((p, i) => string(p, `${path}.purposes[${i}]`))
        .sort(compare);
      if (purposes.length === 0) {
        fail(`${path}.purposes`, 'Declare at least one purpose tag.');
      }
      unique(purposes, `${path}.purposes`);
      const provenance = object(row.rateProvenance, `${path}.rateProvenance`);
      return {
        id: string(row.id, `${path}.id`),
        currency: rowCurrency,
        category: choice(row.category, categories, `${path}.category`),
        purposes,
        usage,
        unit,
        rate,
        rateUnit,
        amount,
        rateProvenance: {
          source: string(provenance.source, `${path}.rateProvenance.source`),
          date: date(provenance.date, `${path}.rateProvenance.date`),
        },
        status: choice(row.status, ['measured', 'estimated'], `${path}.status`),
        notes: string(row.notes, `${path}.notes`),
        allocations,
      };
    })
    .sort((a, b) => compare(a.id, b.id));
  if (charges.length === 0) {
    fail('charges', 'Declare at least one charge; an empty ledger is not evidence of zero cost.');
  }
  unique(
    charges.map((c) => c.id),
    'charges',
  );
  const ratios = array(root.ratios, 'ratios')
    .map((value, index): RatioRequest => {
      const path = `ratios[${index}]`;
      const row = object(value, path);
      const metric = choice(
        row.metric,
        Object.keys(metricDefinitions) as Metric[],
        `${path}.metric`,
      );
      const expected = metricDefinitions[metric];
      const numerator = object(row.numerator, `${path}.numerator`);
      if (numerator.kind !== expected.kind || numerator.key !== expected.key) {
        fail(`${path}.numerator`, `For ${metric}, declare ${expected.kind}:${expected.key}.`);
      }
      const denominator = object(row.denominator, `${path}.denominator`);
      if (denominator.unit !== expected.unit) {
        fail(`${path}.denominator.unit`, `For ${metric}, use ${expected.unit}.`);
      }
      const result: RatioRequest = {
        metric,
        numerator: { kind: expected.kind, key: expected.key },
        denominator: {
          value: nullable(denominator.value, `${path}.denominator.value`),
          unit: expected.unit,
          evidence: string(denominator.evidence, `${path}.denominator.evidence`),
        },
      };
      if (metric === 'transformation') {
        const evidence = object(row.transformEvidence, `${path}.transformEvidence`);
        result.transformEvidence = {
          uniqueSourceTB: numeric(
            evidence.uniqueSourceTB,
            `${path}.transformEvidence.uniqueSourceTB`,
          ),
          passCount: numeric(evidence.passCount, `${path}.transformEvidence.passCount`),
          retryReadTB: numeric(evidence.retryReadTB, `${path}.transformEvidence.retryReadTB`),
        };
        if (exact(result.transformEvidence.passCount, path).d !== 1n) {
          fail(`${path}.transformEvidence.passCount`, 'Pass count must be an integer.');
        }
      }
      return result;
    })
    .sort((a, b) => compare(a.metric, b.metric));
  unique(
    ratios.map((r) => r.metric),
    'ratios',
  );
  return {
    version: choice(root.version, ['1'], 'version'),
    currency,
    window: { start, end },
    context: parsedContext,
    charges,
    ratios,
  };
}
function total(rows: { charge: Charge; share: Rational }[]): CostTotal {
  let sum = zero;
  const unknowns: CostTotal['unknowns'] = [];
  for (const { charge, share } of rows) {
    if (share.n === 0n) {
      continue;
    }
    if (charge.amount !== null) {
      sum = add(sum, multiply(exact(charge.amount, charge.id), share));
    }
    const fields: ('rate' | 'amount')[] = [];
    if (charge.rate === null) {
      fields.push('rate');
    }
    if (charge.amount === null) {
      fields.push('amount');
    }
    if (fields.length > 0) {
      unknowns.push({ chargeId: charge.id, fields });
    }
  }
  return {
    knownSubtotal: encode(sum),
    total: unknowns.length === 0 ? encode(sum) : null,
    unknowns,
  };
}
/** Validates untrusted JSON-shaped input, returning the first actionable error without mutation. */
export function summarizeCostLedger(input: unknown): LedgerResult {
  let ledger: CostLedger;
  try {
    ledger = parse(input);
  } catch (error) {
    if (error instanceof Invalid) {
      return { ok: false, errors: [{ path: error.path, message: error.message }] };
    }
    throw error;
  }
  const rows = ledger.charges.map((charge) => ({ charge, share: { n: 1n, d: 1n } }));
  const platform = total(rows);
  const categorySummaries = categories.map((category) => ({
    category,
    cost: total(rows.filter((r) => r.charge.category === category)),
  }));
  const allocationKeys = new Map<string, Allocation>();
  for (const charge of ledger.charges) {
    for (const allocation of charge.allocations) {
      allocationKeys.set(JSON.stringify([allocation.kind, allocation.key]), allocation);
    }
  }
  const allocations = [...allocationKeys.values()]
    .sort((a, b) => compare(a.kind, b.kind) || compare(a.key, b.key))
    .map(({ kind, key }) => ({
      kind,
      key,
      cost: total(
        ledger.charges.flatMap((charge) =>
          charge.allocations
            .filter((a) => a.kind === kind && a.key === key)
            .map((a) => ({ charge, share: multiply(exact(a.percent, key), { n: 1n, d: 100n }) })),
        ),
      ),
    }));
  const purposes = [...new Set(ledger.charges.flatMap((c) => c.purposes))]
    .sort(compare)
    .map((purpose) => ({
      purpose,
      cost: total(rows.filter((r) => r.charge.purposes.includes(purpose))),
    }));
  const ratios = ledger.ratios.map((request): CostSummary['ratios'][number] => {
    const numerator =
      request.numerator.kind === 'total'
        ? platform
        : request.numerator.kind === 'category'
          ? categorySummaries.find((c) => c.category === request.numerator.key)!.cost
          : (allocations.find((a) => a.kind === 'workload' && a.key === request.numerator.key)
              ?.cost ?? total([]));
    const hasNumerator =
      request.numerator.kind === 'total' ||
      (request.numerator.kind === 'category'
        ? ledger.charges.some((c) => c.category === request.numerator.key)
        : allocations.some((a) => a.kind === 'workload' && a.key === request.numerator.key));
    const denominator =
      request.denominator.value === null ? null : exact(request.denominator.value, request.metric);
    const unavailable =
      denominator === null
        ? 'missing denominator'
        : denominator.n === 0n
          ? 'zero denominator'
          : !hasNumerator
            ? 'missing numerator'
            : numerator.total === null
              ? 'incomplete numerator'
              : null;
    return {
      metric: request.metric,
      numerator,
      denominator: { ...request.denominator },
      unavailable,
      value:
        unavailable === null
          ? encode(
              multiply(exact(numerator.total!, request.metric), {
                n: denominator!.d,
                d: denominator!.n,
              }),
            )
          : null,
    };
  });
  return {
    ok: true,
    summary: { ledger, platform, categories: categorySummaries, allocations, purposes, ratios },
  };
}
/** Display only: exact half-up rounding, never used by validation or aggregation. */
export function formatCost(value: Exact, decimalPlaces = 2): string {
  if (!Number.isInteger(decimalPlaces) || decimalPlaces < 0 || decimalPlaces > 20) {
    throw new RangeError('decimalPlaces must be an integer from 0 through 20.');
  }
  const amount = exact(value, 'value');
  const scale = 10n ** BigInt(decimalPlaces);
  const rounded = (2n * amount.n * scale + amount.d) / (2n * amount.d);
  if (decimalPlaces === 0) {
    return String(rounded);
  }
  return `${rounded / scale}.${String(rounded % scale).padStart(decimalPlaces, '0')}`;
}
