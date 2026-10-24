import assert from 'node:assert/strict';
import test from 'node:test';
import {
  formatCost,
  summarizeCostLedger,
  type Charge,
  type CostLedger,
  type CostSummary,
  type RatioRequest,
} from '../benchmarks/cost-ledger.js';

function fixture(): CostLedger {
  const charge = (
    id: string,
    category: Charge['category'],
    usage: string,
    unit: Charge['unit'],
    rate: string,
    amount: string,
    key: string,
    purposes: string[],
  ): Charge => ({
    id,
    currency: 'CU',
    category,
    usage,
    unit,
    rate,
    rateUnit: unit,
    amount,
    purposes,
    rateProvenance: { source: 'Fictional arithmetic fixture', date: '2026-10-24' },
    status: 'estimated',
    notes: 'Synthetic; no provider price or measured run.',
    allocations: [
      { kind: key === 'shared/unallocated' ? 'unallocated' : 'workload', key, percent: '100' },
    ],
  });
  return {
    version: '1',
    currency: 'CU',
    window: { start: '2026-10-23T00:00:00.000Z', end: '2026-10-24T00:00:00.000Z' },
    context: {
      run: 'synthetic-24h',
      workload: 'COST-MODEL synthetic example',
      profileRevision: '0.2',
      topology: 'fictional',
      location: 'fictional',
      pricingDate: '2026-10-24',
      ratesSource: 'COST-MODEL.md invented CU rates',
      reservedCapacityTreatment: 'none',
      taxTreatment: 'excluded',
      staffingAssumptions: '0.5 hours at 40 CU/hour',
    },
    charges: [
      charge('operational', 'compute', '48', 'node-hour', '0.50', '24', 'serving', ['serving']),
      charge('transform', 'compute', '12', 'node-hour', '0.75', '9', 'transformation', [
        'transform',
      ]),
      charge('primary', 'storage capacity', '1/15', 'TB-month', '20', '4/3', 'retention', [
        'serving',
      ]),
      charge('backup', 'storage capacity', '1/30', 'TB-month', '20', '2/3', 'retention', [
        'backup',
        'DR',
      ]),
      charge(
        'operations',
        'storage operations',
        '2',
        'million requests',
        '0.40',
        '0.80',
        'shared/unallocated',
        ['shared control'],
      ),
      charge('network', 'network', '0.10', 'TB', '5', '0.50', 'shared/unallocated', [
        'shared control',
      ]),
      charge('labor', 'labor', '0.50', 'hour', '40', '20', 'shared/unallocated', [
        'shared control',
      ]),
    ],
    ratios: [
      {
        metric: 'retention',
        numerator: { kind: 'category', key: 'storage capacity' },
        denominator: {
          value: '0.10',
          unit: 'TB-month',
          evidence:
            '(2 + 1) decimal TB × 24 hours / 720 hours; primary plus backup physical occupancy',
        },
      },
      {
        metric: 'transformation',
        numerator: { kind: 'allocation', key: 'transformation' },
        denominator: { value: '3', unit: 'TB', evidence: '3 decimal TB read into stages' },
        transformEvidence: { uniqueSourceTB: '1', passCount: '3', retryReadTB: '0' },
      },
      {
        metric: 'fully-loaded-unique-input',
        numerator: { kind: 'total', key: 'platform' },
        denominator: {
          value: '1',
          unit: 'TB',
          evidence: '1 decimal TB unique source input; no retransmissions',
        },
      },
    ],
  };
}
function summary(input: unknown): CostSummary {
  const result = summarizeCostLedger(input);
  assert.equal(result.ok, true, JSON.stringify(result));
  if (!result.ok) {
    throw new Error('Expected a summary');
  }
  return result.summary;
}
function freeze(value: unknown): void {
  if (value !== null && typeof value === 'object') {
    Object.freeze(value);
    for (const child of Object.values(value)) {
      freeze(child);
    }
  }
}
function rejected(input: unknown, path: string): void {
  const before = structuredClone(input);
  freeze(input);
  const result = summarizeCostLedger(input);
  assert.equal(result.ok, false);
  if (!result.ok) {
    assert.equal(result.errors[0].path, path);
    assert.ok(result.errors[0].message.length > 10);
  }
  assert.deepEqual(input, before, 'Rejected input must not mutate caller-owned data');
}

test('exact synthetic 24-hour CU evidence matches the accepted cost model', () => {
  const input = fixture();
  freeze(input);
  const output = summary(input);
  assert.deepEqual(output.platform, { knownSubtotal: '563/10', total: '563/10', unknowns: [] });
  assert.equal(formatCost(output.platform.total!), '56.30');
  assert.equal(output.categories.find((c) => c.category === 'storage capacity')?.cost.total, '2');
  assert.equal(output.allocations.find((a) => a.key === 'transformation')?.cost.total, '9');
  assert.equal(output.allocations.find((a) => a.kind === 'unallocated')?.cost.total, '213/10');
  assert.deepEqual(
    output.ratios.map((r) => [r.metric, r.value]),
    [
      ['fully-loaded-unique-input', '563/10'],
      ['retention', '20'],
      ['transformation', '3'],
    ],
  );
  assert.equal(output.ratios.find((r) => r.metric === 'retention')?.denominator.value, '0.10');
  assert.equal(output.ratios.find((r) => r.metric === 'transformation')?.denominator.value, '3');
  assert.deepEqual(output.ledger.window, input.window);
  assert.deepEqual(output.ledger.context, input.context);
  assert.deepEqual(
    output.ledger.charges.find((c) => c.id === 'backup'),
    { ...input.charges[3], purposes: ['DR', 'backup'] },
  );
  assert.deepEqual(
    output.ledger.ratios.find((r) => r.metric === 'transformation')?.transformEvidence,
    { uniqueSourceTB: '1', passCount: '3', retryReadTB: '0' },
  );
  output.ledger.context.run = 'changed output';
  assert.equal(input.context.run, 'synthetic-24h');
});

test('charge, purpose, allocation and ratio order do not change the summary', () => {
  const input = fixture();
  input.charges[0].allocations = [
    { kind: 'shared', key: 'control', percent: '25' },
    { kind: 'workload', key: 'serving', percent: '75' },
  ];
  const reordered = structuredClone(input);
  reordered.charges.reverse();
  reordered.ratios.reverse();
  for (const row of reordered.charges) {
    row.purposes.reverse();
    row.allocations.reverse();
  }
  assert.deepEqual(summary(reordered), summary(input));
  const output = summary(input);
  assert.equal(output.allocations.find((a) => a.kind === 'shared')?.cost.total, '6');
  assert.equal(output.allocations.find((a) => a.key === 'serving')?.cost.total, '18');
});

test('backup and DR are overlapping views, counted once in platform and category totals', () => {
  const input = fixture();
  const output = summary(input);
  assert.equal(output.purposes.find((p) => p.purpose === 'backup')?.cost.total, '2/3');
  assert.equal(output.purposes.find((p) => p.purpose === 'DR')?.cost.total, '2/3');
  input.charges[3].purposes.push('analytics');
  assert.deepEqual(summary(input).platform, output.platform);
  assert.deepEqual(summary(input).categories, output.categories);
  assert.deepEqual(summary(input).allocations, output.allocations);
});

const invalidCases: [string, (input: CostLedger) => void, string][] = [
  ['duplicate charge', (l) => l.charges.push(structuredClone(l.charges[0])), 'charges'],
  [
    'duplicate allocation',
    (l) =>
      (l.charges[0].allocations = [
        { kind: 'workload', key: 'serving', percent: '50' },
        { kind: 'workload', key: 'serving', percent: '50' },
      ]),
    'charges[0].allocations',
  ],
  [
    'under allocation',
    (l) => (l.charges[0].allocations[0].percent = '99.999999999999999999'),
    'charges[0].allocations',
  ],
  [
    'over allocation',
    (l) => (l.charges[0].allocations[0].percent = '100.000000000000000001'),
    'charges[0].allocations',
  ],
  ['rounded storage usage', (l) => (l.charges[2].usage = '0.066667'), 'charges[2].amount'],
  ['rounded storage amount', (l) => (l.charges[2].amount = '1.333333'), 'charges[2].amount'],
  ['wrong arithmetic', (l) => (l.charges[0].amount = '24.000000000000000001'), 'charges[0].amount'],
  ['unsupported unit', (l) => Reflect.set(l.charges[0], 'unit', 'TiB'), 'charges[0].unit'],
  ['incompatible rate unit', (l) => (l.charges[0].rateUnit = 'hour'), 'charges[0].rateUnit'],
  ['currency mismatch', (l) => (l.charges[0].currency = 'USD'), 'charges[0].currency'],
  ['missing currency', (l) => Reflect.deleteProperty(l, 'currency'), 'currency'],
  ['unknown version', (l) => Reflect.set(l, 'version', '2'), 'version'],
  ['missing rate field', (l) => Reflect.deleteProperty(l.charges[0], 'rate'), 'charges[0].rate'],
  [
    'missing amount field',
    (l) => Reflect.deleteProperty(l.charges[0], 'amount'),
    'charges[0].amount',
  ],
  [
    'missing provenance',
    (l) => Reflect.deleteProperty(l.charges[0], 'rateProvenance'),
    'charges[0].rateProvenance',
  ],
  ['invalid pricing date', (l) => (l.context.pricingDate = '2026-02-30'), 'context.pricingDate'],
  [
    'invalid rate date',
    (l) => (l.charges[0].rateProvenance.date = '2026-13-01'),
    'charges[0].rateProvenance.date',
  ],
  ['invalid status', (l) => Reflect.set(l.charges[0], 'status', 'actual'), 'charges[0].status'],
  ['empty purpose', (l) => (l.charges[0].purposes = []), 'charges[0].purposes'],
  ['duplicate purpose', (l) => l.charges[0].purposes.push('serving'), 'charges[0].purposes'],
  ['missing notes', (l) => Reflect.deleteProperty(l.charges[0], 'notes'), 'charges[0].notes'],
  [
    'missing context',
    (l) => Reflect.deleteProperty(l.context, 'staffingAssumptions'),
    'context.staffingAssumptions',
  ],
  ['reversed window', (l) => (l.window.end = '2026-10-22T00:00:00.000Z'), 'window.end'],
  ['empty window', (l) => (l.window.end = l.window.start), 'window.end'],
  ['invalid calendar day', (l) => (l.window.start = '2026-02-30T00:00:00.000Z'), 'window.start'],
  ['invalid timestamp', (l) => (l.window.start = 'yesterday'), 'window.start'],
  ['empty charges', (l) => (l.charges = []), 'charges'],
  ['wrong ratio numerator', (l) => (l.ratios[0].numerator.key = 'compute'), 'ratios[0].numerator'],
  [
    'wrong ratio denominator unit',
    (l) => (l.ratios[0].denominator.unit = 'TB'),
    'ratios[0].denominator.unit',
  ],
  [
    'missing transformation counters',
    (l) => Reflect.deleteProperty(l.ratios[1], 'transformEvidence'),
    'ratios[1].transformEvidence',
  ],
  [
    'fractional pass count',
    (l) => (l.ratios[1].transformEvidence!.passCount = '1.5'),
    'ratios[1].transformEvidence.passCount',
  ],
  ['duplicate ratio', (l) => l.ratios.push(structuredClone(l.ratios[0])), 'ratios'],
];
for (const [name, mutate, path] of invalidCases) {
  test(`rejects ${name} without mutation`, () => {
    const input = fixture();
    mutate(input);
    rejected(input, path);
  });
}
for (const field of ['usage', 'rate', 'amount'] as const) {
  for (const value of [
    -1,
    0,
    NaN,
    Infinity,
    '-1',
    'NaN',
    'Infinity',
    '1e2',
    ' 1',
    '',
    '1/0',
    '01',
    '0'.repeat(162),
  ]) {
    test(`rejects malformed ${field} ${String(value)} (${typeof value})`, () => {
      const input = fixture();
      Reflect.set(input.charges[0], field, value);
      rejected(input, `charges[0].${field}`);
    });
  }
}

test('missing rates and amounts remain explicit and cannot yield complete totals or ratios', () => {
  const input = fixture();
  input.charges[1].rate = null;
  let output = summary(input);
  assert.deepEqual(output.platform, {
    knownSubtotal: '563/10',
    total: null,
    unknowns: [{ chargeId: 'transform', fields: ['rate'] }],
  });
  assert.equal(
    output.ratios.find((r) => r.metric === 'transformation')?.unavailable,
    'incomplete numerator',
  );
  assert.equal(output.ratios.find((r) => r.metric === 'retention')?.value, '20');
  input.charges[1].amount = null;
  output = summary(input);
  assert.deepEqual(output.platform, {
    knownSubtotal: '473/10',
    total: null,
    unknowns: [{ chargeId: 'transform', fields: ['rate', 'amount'] }],
  });
  input.charges[1].rate = '0.75';
  output = summary(input);
  assert.equal(
    output.platform.knownSubtotal,
    '473/10',
    'Do not impute amount from known usage and rate',
  );
  assert.deepEqual(output.platform.unknowns, [{ chargeId: 'transform', fields: ['amount'] }]);
});

test('unknown amounts propagate through fractional allocations and purpose views', () => {
  const input = fixture();
  input.charges[3].amount = null;
  input.charges[3].allocations = [
    { kind: 'shared', key: 'control', percent: '50' },
    { kind: 'unallocated', key: 'unknown', percent: '50' },
    { kind: 'workload', key: 'empty', percent: '0' },
  ];
  const output = summary(input);
  assert.equal(output.allocations.find((a) => a.kind === 'shared')?.cost.total, null);
  assert.equal(output.allocations.find((a) => a.key === 'unknown')?.cost.total, null);
  assert.equal(output.allocations.find((a) => a.key === 'empty')?.cost.total, '0');
  assert.equal(output.purposes.find((p) => p.purpose === 'DR')?.cost.total, null);
});

test('zero, missing denominators and absent numerator attribution are unavailable', () => {
  for (const value of [null, '0', '0/30']) {
    const input = fixture();
    for (const ratio of input.ratios) {
      ratio.denominator.value = value;
    }
    for (const ratio of summary(input).ratios) {
      assert.equal(ratio.value, null);
      assert.equal(ratio.unavailable, value === null ? 'missing denominator' : 'zero denominator');
    }
  }
  const input = fixture();
  input.charges[1].allocations[0].kind = 'shared';
  const ratio = summary(input).ratios.find((r) => r.metric === 'transformation');
  assert.equal(ratio?.value, null);
  assert.equal(ratio?.unavailable, 'missing numerator');
});

test('successful read/action and accepted ingestion denominators stay separate from retry overhead', () => {
  const input = fixture();
  const requests: RatioRequest[] = [
    {
      metric: 'reads',
      numerator: { kind: 'allocation', key: 'reads' },
      denominator: {
        value: '2',
        unit: 'million successful reads',
        evidence: '2 million successful logical reads; 1 million retries excluded',
      },
    },
    {
      metric: 'actions',
      numerator: { kind: 'allocation', key: 'actions' },
      denominator: {
        value: '1',
        unit: 'million successful actions',
        evidence: '1 million successful logical actions; retries excluded',
      },
    },
    {
      metric: 'ingestion',
      numerator: { kind: 'allocation', key: 'ingestion' },
      denominator: {
        value: '1',
        unit: 'TB',
        evidence: '1 TB unique source payload durably accepted; retransmissions excluded',
      },
    },
  ];
  input.ratios = requests;
  input.charges[0].allocations = [
    { kind: 'workload', key: 'reads', percent: '50' },
    { kind: 'workload', key: 'actions', percent: '25' },
    { kind: 'workload', key: 'ingestion', percent: '25' },
  ];
  assert.deepEqual(
    summary(input).ratios.map((r) => [r.metric, r.value]),
    [
      ['actions', '6'],
      ['ingestion', '6'],
      ['reads', '6'],
    ],
  );
  input.charges[0].usage = '96';
  input.charges[0].amount = '48';
  assert.deepEqual(
    summary(input).ratios.map((r) => r.value),
    ['12', '12', '12'],
  );
});

test('display rounding is separate from exact arithmetic', () => {
  assert.equal(formatCost('4/3', 6), '1.333333');
  assert.equal(formatCost('2/3', 6), '0.666667');
  assert.equal(formatCost('1.005'), '1.01');
  assert.equal(formatCost('0'), '0.00');
  assert.equal(formatCost('999/1000'), '1.00');
  assert.equal(formatCost('1/2', 0), '1');
  assert.throws(() => formatCost('1', -1), RangeError);
  assert.throws(() => formatCost('1', 21), RangeError);
});
