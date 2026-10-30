import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { resolve } from 'node:path';
import { createRequire } from 'node:module';
import {
  canonicalValue,
  decodeValue,
  normalizeType,
  validateRow,
  validateValue,
} from '../packages/contracts/values.js';
import { VALUE_VERSION } from '../packages/contracts/types.js';
import { acceptsType, compareColumns } from '../packages/contracts/compatibility.js';
import { compileExpression } from '../packages/authorization/src/expressions.js';
import { functionsClient } from '../packages/platform/src/functions.js';
import { createTransport } from '../packages/platform/src/transport.js';
import { valueFingerprint, canonical, validateManifest } from '../packages/compiler/src/index.js';

const cases = JSON.parse(
  readFileSync(new URL('../packages/contracts/value-conformance.json', import.meta.url), 'utf8'),
);

test('the versioned value contract uses the same strict wire vectors in every component', () => {
  assert.equal(cases.version, VALUE_VERSION);
  for (const item of cases.valid) {
    assert.deepEqual(
      decodeValue(item.value, item.schema, item.definitions),
      item.normalized,
      item.name,
    );
  }
  for (const item of cases.invalid)
    assert.throws(() => decodeValue(item.value, item.schema), { code: 'VALUE_INVALID' }, item.name);
  for (const item of cases.invalidTypes)
    assert.throws(
      () => normalizeType(item.schema, item.definitions),
      { code: 'TYPE_INVALID' },
      item.name,
    );
  assert.equal(
    canonicalValue(1, { type: 'float64', nullable: false }),
    '\u007b"schema":{"nullable":false,"type":"float64"},"value":{"$float64":"3ff0000000000000"},"version":1}',
  );
  for (const value of [NaN, Infinity, -Infinity])
    assert.throws(() => validateValue(value, { type: 'float64', nullable: false }));
  assert.throws(() =>
    validateValue(new Array(2), {
      type: 'array',
      nullable: false,
      items: { type: 'string', nullable: false },
    }),
  );
  assert.throws(() => decodeValue(new Date(), { type: 'object', properties: {} }));
  assert.throws(() => decodeValue({ [Symbol('hidden')]: 1 }, { type: 'object', properties: {} }));
  const cycle: any = {};
  cycle.value = cycle;
  assert.throws(() => canonical(cycle), { code: 'INVALID_JSON' });
  assert.equal(validateManifest(cycle)[0].code, 'SCHEMA_INVALID');
  const changed: any = { type: 'string', nullable: false };
  validateValue('first', changed);
  changed.nullable = 'yes';
  assert.throws(() => validateValue(null, changed), { code: 'TYPE_INVALID' });
});

test('TypeScript and Python produce identical normalized values and canonical typed bytes', () => {
  const result = spawnSync(
    process.env.MORIA_PYTHON ?? resolve('.venv/bin/python'),
    [
      '-c',
      `import json,sys
from workers.value_codec import normalize_wire,canonical_value,value_fingerprint
cases=json.load(open(sys.argv[1],encoding='utf-8'))
print(json.dumps([{'normalized':normalize_wire(case['value'],case['schema'],case.get('definitions')),'canonical':canonical_value(case['value'],case['schema'],case.get('definitions')),'fingerprint':value_fingerprint(case['value'],case['schema'],case.get('definitions'))} for case in cases['valid']],ensure_ascii=True,allow_nan=False))`,
      resolve('packages/contracts/value-conformance.json'),
    ],
    { encoding: 'utf8', maxBuffer: 5 * 1024 * 1024 },
  );
  assert.equal(result.status, 0, result.stderr || result.error?.message || 'Python codec failed');
  const python = JSON.parse(result.stdout);
  cases.valid.forEach((item: any, index: number) => {
    assert.deepEqual(python[index].normalized, item.normalized, item.name);
    assert.equal(
      python[index].canonical,
      canonicalValue(item.value, item.schema, item.definitions),
      item.name,
    );
    assert.equal(
      python[index].fingerprint,
      valueFingerprint(item.value, item.schema, item.definitions),
      item.name,
    );
  });
});

test('the published JSON schema and runtime descriptor validation agree on supported shapes', () => {
  const require = createRequire(import.meta.url);
  const Ajv = require('ajv/dist/2020').default;
  const ajv = new Ajv({ strict: false });
  const schema = JSON.parse(
    readFileSync(
      new URL('../packages/contracts/schemas/values.schema.json', import.meta.url),
      'utf8',
    ),
  );
  assert.deepEqual(
    schema,
    JSON.parse(
      readFileSync(new URL('../docs/spec/schemas/values.schema.json', import.meta.url), 'utf8'),
    ),
  );
  const validate = ajv.compile(schema);
  for (const item of cases.valid) assert(validate(item.schema), JSON.stringify(validate.errors));
  const column = ajv.compile({ $ref: schema.$id + '#/$defs/Column' });
  for (const item of cases.valid.filter(
    (item: any) => ['object', 'list'].includes(item.schema.type) || item.schema.ref,
  ))
    assert.equal(column(item.schema), false);
});

test('column profiles preserve scalar arrays while function and API records preserve omission', () => {
  const payload: any = {
    type: 'object',
    properties: {
      items: { type: 'list', items: { type: 'decimal', precision: 5, scale: 2, nullable: false } },
      note: { type: 'string', nullable: true },
    },
    optional: ['note'],
  };
  assert.deepEqual(decodeValue({ items: ['1.2'] }, payload), { items: ['1.20'] });
  assert.deepEqual(decodeValue({ items: [], note: null }, payload), { items: [], note: null });
  assert.deepEqual(validateRow({ payload: { items: ['1.2'] } }, { payload }, 'value'), {
    payload: { items: ['1.20'] },
  });
  assert.throws(() => validateRow({ payload: { items: [] } }, { payload } as any), {
    code: 'TYPE_INVALID',
  });
  assert.throws(
    () =>
      normalizeType(
        { type: 'array', nullable: false, items: { type: 'string', nullable: true } },
        {},
        'column',
      ),
    { code: 'TYPE_INVALID' },
  );
  assert.deepEqual(
    validateRow(
      { name: 'a' },
      { name: { type: 'string', nullable: false }, note: { type: 'string', nullable: true } },
    ),
    { name: 'a', note: null },
  );
});

test('compatibility rejects narrowing and destructive column changes without a migration', () => {
  const customer = {
    type: 'ontologyObject' as const,
    objectType: '00000000-0000-4000-8000-000000000004',
    nullable: false,
  };
  assert(acceptsType(customer, { ...customer, nullable: true }));
  assert(
    !acceptsType(customer, { ...customer, objectType: '00000000-0000-4000-8000-000000000005' }),
  );
  const integer = { type: 'int64', nullable: false } as const;
  assert(acceptsType(integer, { ...integer, nullable: true }));
  assert(!acceptsType({ ...integer, nullable: true }, integer));
  assert(!acceptsType(integer, { type: 'float64', nullable: false }));
  assert(
    acceptsType(
      { type: 'decimal', nullable: false, precision: 8, scale: 2 },
      { type: 'decimal', nullable: false, precision: 12, scale: 2 },
    ),
  );
  assert(
    !acceptsType(
      { type: 'decimal', nullable: false, precision: 8, scale: 2 },
      { type: 'decimal', nullable: false, precision: 12, scale: 3 },
    ),
  );
  assert(
    compareColumns({ value: integer }, { value: integer, note: { type: 'string', nullable: true } })
      .compatible,
  );
  assert(
    !compareColumns({ value: integer }, { value: { type: 'string', nullable: false } }).compatible,
  );
  assert(!compareColumns({ value: integer }, {}).compatible);
  assert(!compareColumns({}, { value: integer }).compatible);
  assert(
    acceptsType(
      { ref: 'Count' },
      { ref: 'Count' },
      { Count: integer },
      { Count: { ...integer, nullable: true } },
    ),
  );
  const record = { type: 'object', properties: { count: integer } } as const;
  assert(
    acceptsType(record, {
      type: 'object',
      properties: { count: integer, note: { type: 'string', nullable: false } },
      optional: ['note'],
    }),
  );
  assert(
    !acceptsType(record, {
      type: 'object',
      properties: { count: integer, note: { type: 'string', nullable: true } },
    }),
  );
  assert(
    !acceptsType({ type: 'object', properties: { count: integer }, optional: ['count'] }, record),
  );
  assert(!acceptsType(record, { type: 'object', properties: {} }));
});

test('policy literals and decimal comparisons use the same exact value semantics', () => {
  const types = {
    fields: {
      a: { type: 'decimal', nullable: false, precision: 4, scale: 1 },
      b: { type: 'decimal', nullable: false, precision: 5, scale: 2 },
      numbers: { type: 'array', nullable: false, items: { type: 'int64', nullable: false } },
    },
  } as const;
  assert(
    compileExpression({ op: 'eq', args: [{ field: 'a' }, { field: 'b' }] }, types).allows({
      fields: { a: '1.2', b: '1.20' },
    }),
  );
  assert(
    compileExpression(
      { op: 'contains', args: [{ field: 'numbers' }, { literal: '9223372036854775807' }] },
      types,
    ).allows({ fields: { numbers: ['9223372036854775807'] } }),
  );
  assert.throws(
    () => compileExpression({ op: 'eq', args: [{ field: 'a' }, { literal: '1000.0' }] }, types),
    { code: 'POLICY_INVALID' },
  );
  assert.throws(
    () => compileExpression({ op: 'eq', args: [{ literal: '\0' }, { literal: '\0' }] }, {}),
    { code: 'POLICY_INVALID' },
  );
});

test('platform clients reject an invocation with an unsupported value version', async () => {
  const client = functionsClient(
    createTransport({
      fetch: async () =>
        new Response(JSON.stringify({ valueVersion: 2 }), {
          headers: { 'content-type': 'application/json' },
        }),
    }),
  );
  await assert.rejects(client.get('00000000-0000-0000-0000-000000000000'), {
    code: 'UNSUPPORTED_VALUE_VERSION',
  });
});

test('JSON transports reject lossy native values before issuing a request', async () => {
  let requests = 0;
  const transport = createTransport({
    fetch: async () => {
      requests++;
      return Response.json({});
    },
  });
  for (const value of [NaN, Infinity, undefined, 1n, new Date(), new Array(1)])
    await assert.rejects(transport.request('/api/typed', 'POST', { value }), {
      code: 'INVALID_JSON',
    });
  assert.equal(requests, 0);
});

test('JSON transports reject malformed UTF-8 instead of replacing bytes inside values', async () => {
  const transport = createTransport({
    fetch: async () => new Response(new Uint8Array([34, 0xc3, 0x28, 34])),
  });
  await assert.rejects(transport.request('/api/typed', 'GET'), { code: 'INVALID_RESPONSE' });
});
