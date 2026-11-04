import { readFileSync, writeFileSync } from 'node:fs';
import { canonicalJson } from '../packages/contracts/values.js';
import { fingerprint, valueFingerprint } from '../packages/compiler/src/index.js';

const path = new URL('../packages/contracts/canonical-conformance.json', import.meta.url);
const shared = JSON.parse(
  readFileSync(new URL('../packages/contracts/value-conformance.json', import.meta.url), 'utf8'),
);
const numbers = [
  0,
  -0,
  1,
  -1,
  1.5,
  1e-7,
  1e-6,
  1e20,
  1e21,
  1e30,
  -1e21,
  Number.MIN_VALUE,
  Number.MAX_VALUE,
  0.1 + 0.2,
  9007199254740992,
  1000000000000000100,
];
const canonical = [
  ...numbers.map((value, i) => ({ name: `number-${i}`, value })),
  { name: 'utf16-order', value: { '\ue000': 1, '😀': 2, alpha: 3 } },
  {
    name: 'escaped-strings',
    value: ['\b\t\n\f\r\u0000', 'quote" slash/ backslash\\', '雪😀\u2028\u2029'],
  },
  { name: 'nested-order', value: { z: [{ b: null, a: true }], a: false } },
].map((entry) => ({
  ...entry,
  canonical: canonicalJson(entry.value),
  fingerprint: fingerprint(entry.value),
}));
const typed = [
  ...shared.valid,
  {
    name: 'optional-field-order',
    schema: {
      type: 'object',
      properties: {
        a: { type: 'string', nullable: false },
        z: { type: 'float64', nullable: false },
      },
      optional: ['z', 'a'],
    },
    value: { z: -0 },
  },
  {
    name: 'empty-optionals',
    schema: { type: 'object', properties: {}, optional: [] },
    value: {},
  },
  {
    name: 'hyphenated-property',
    schema: { type: 'object', properties: { 'field-name': { type: 'int64', nullable: false } } },
    value: { 'field-name': '9223372036854775807' },
  },
].map((entry) => ({
  name: entry.name,
  schema: entry.schema,
  definitions: entry.definitions ?? {},
  value: entry.value,
  fingerprint: valueFingerprint(entry.value, entry.schema, entry.definitions ?? {}),
}));
const expected = { version: 1, canonical, typed };
if (process.argv.includes('--write')) writeFileSync(path, JSON.stringify(expected, null, 2) + '\n');
else if (canonicalJson(JSON.parse(readFileSync(path, 'utf8'))) !== canonicalJson(expected))
  throw new Error(
    'Native canonical fixtures are stale; run scripts/generate-native-conformance.ts --write.',
  );
console.log(`Verified ${canonical.length} canonical and ${typed.length} typed fingerprint cases.`);
