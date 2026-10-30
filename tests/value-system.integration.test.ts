import test from 'node:test';
import assert from 'node:assert/strict';
import { join } from 'node:path';
import { writeFile, readdir } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { database } from './fixtures/catalog-storage/database.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { ObjectStore, type ObjectSchema } from '../services/ontology/src/store.js';
import { ObjectService, type ActionBinding } from '../services/ontology/src/service.js';
import { createApi } from '../apps/api/src/server.js';
import { icebergSchema } from '../packages/contracts/iceberg.js';
import { validateRow } from '../packages/contracts/values.js';
import { iceberg } from '../workers/iceberg/stage.js';
import { icebergRows } from '../workers/iceberg/stream.js';
import { writeSdk } from '../packages/osdk/src/generate.js';

test(
  'all column types round-trip exactly through Iceberg, PostgreSQL, authenticated APIs and a generated SDK',
  { timeout: 120000 },
  async (t) => {
    const db = await database();
    t.after(db.close);
    const environment = 'value-system',
      principal = 'reader',
      application = 'typed-app';
    await db.catalog.ensureEnvironment(environment);
    const authority = new Authority(db.catalog),
      store = new ObjectStore(db.catalog);
    const operations = ['values:read', 'edit-values:invoke'];
    await authority.syncPrincipal(environment, 'setup', principal, {
      kind: 'workload',
      enabled: true,
      attributes: {},
      markings: [],
    });
    await authority.setApplication(environment, 'setup', application, operations);
    await authority.setGrants(environment, 'setup', principal, application, operations);
    const context = { environment, principal, application };
    const uid = await db.catalog.resolveIdentity('value-system', 'one');
    const schema: ObjectSchema = {
      primaryKey: 'uid',
      properties: {
        uid: { type: 'uid', nullable: false },
        text: { type: 'string', nullable: false },
        flag: { type: 'boolean', nullable: false },
        count: { type: 'int64', nullable: false },
        ratio: { type: 'float64', nullable: false },
        amount: { type: 'decimal', nullable: false, precision: 38, scale: 9 },
        day: { type: 'date', nullable: false },
        instant: { type: 'timestamp', nullable: false },
        labels: { type: 'array', nullable: false, items: { type: 'string', nullable: false } },
        numbers: { type: 'array', nullable: false, items: { type: 'int64', nullable: false } },
        amounts: {
          type: 'array',
          nullable: false,
          items: { type: 'decimal', nullable: false, precision: 8, scale: 2 },
        },
        flags: { type: 'array', nullable: false, items: { type: 'boolean', nullable: false } },
        ratios: { type: 'array', nullable: false, items: { type: 'float64', nullable: false } },
        days: { type: 'array', nullable: false, items: { type: 'date', nullable: false } },
        instants: { type: 'array', nullable: false, items: { type: 'timestamp', nullable: false } },
        identities: { type: 'array', nullable: false, items: { type: 'uid', nullable: false } },
        optionalNumbers: {
          type: 'array',
          nullable: true,
          items: { type: 'int64', nullable: false },
        },
        note: { type: 'string', nullable: true },
      },
    };
    const row = {
      uid,
      text: '雪😀',
      flag: true,
      count: '9223372036854775807',
      ratio: 0.125,
      amount: '12345678901234567890123456789.123456789',
      day: '2000-02-29',
      instant: '2026-10-29T00:00:00.123456Z',
      labels: ['b', 'a', 'b'],
      numbers: ['-9223372036854775808', '9223372036854775807'],
      amounts: ['1.2', '-0'],
      flags: [true, false],
      ratios: [0.125, -0],
      days: ['0001-01-01', '2000-02-29'],
      instants: ['0001-01-01T00:00:00.000001Z', '2026-10-29T00:00:00Z'],
      identities: [uid, uid],
    };
    const expected = validateRow(row, schema.properties);
    const pin = await iceberg({
      operation: 'stage',
      warehouse: join(db.root, 'iceberg'),
      schema: icebergSchema(schema.properties),
      rows: [row],
    });
    assert.equal(pin.valueVersion, 1);
    assert.deepEqual(
      (await iceberg({ operation: 'read', metadataLocation: pin.metadataLocation })).rows,
      [expected],
    );
    const streamed = [];
    for await (const value of icebergRows(pin, { batchRows: 1 })) streamed.push(value);
    assert.deepEqual(streamed, [expected]);
    const empty = await iceberg({
      operation: 'stage',
      warehouse: join(db.root, 'empty'),
      schema: icebergSchema(schema.properties),
      rows: [],
    });
    assert.deepEqual(
      (await iceberg({ operation: 'read', metadataLocation: empty.metadataLocation })).rows,
      [],
    );
    const emptyRows = [];
    for await (const value of icebergRows(empty)) emptyRows.push(value);
    assert.deepEqual(emptyRows, []);
    const state = await authority.current(environment, principal, application);
    await store.activate(environment, 'values', schema, streamed, state.epoch, '0');
    const service = new ObjectService(store, authority, {
      objectTypeId: '00000000-0000-4000-8000-000000000005',
      schema,
      policies: {
        resource: 'values',
        markings: [],
        properties: schema.properties,
        sourceColumns: schema.properties,
        attributes: {},
      },
    });
    const action: ActionBinding = {
      resource: 'edit-values',
      revision: '1',
      markings: [],
      inputs: { uid: schema.properties.uid, amount: schema.properties.amount },
      targetParameter: 'uid',
      condition: { literal: true },
      edits: [{ op: 'set', property: 'amount', value: { parameter: 'amount' } }],
    };
    const sessions = new WorkloadSessions(authority),
      session = await sessions.mint(context, 'setup');
    const server = createApi(sessions, {
      objects: new Map([['values', service]]),
      actions: new Map([['edit-values', { service, action }]]),
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    t.after(() => {
      server.closeAllConnections();
      return new Promise<void>((resolve) => server.close(() => resolve()));
    });
    const baseUrl = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
    await writeFile(join(db.root, 'package.json'), '\u007b"type":"module"}');
    const sdk = join(db.root, 'sdk'),
      output = join(db.root, 'compiled');
    await writeSdk(
      {
        version: 1,
        app: 'types',
        types: {},
        objects: {
          values: {
            id: '00000000-0000-4000-8000-000000000005',
            type: 'TypedRecord',
            properties: schema.properties,
          },
        },
        actions: { 'edit-values': { inputs: action.inputs } },
        endpoints: {},
      },
      sdk,
    );
    await promisify(execFile)(
      'pnpm',
      [
        'exec',
        'tsc',
        '--ignoreConfig',
        '--target',
        'ES2024',
        '--module',
        'NodeNext',
        '--moduleResolution',
        'NodeNext',
        '--skipLibCheck',
        '--outDir',
        output,
        ...(await readdir(sdk))
          .filter((name) => name.endsWith('.ts'))
          .map((name) => join(sdk, name)),
      ],
      { timeout: 30000, maxBuffer: 1048576 },
    );
    const { createClient, VALUE_VERSION } = await import(
      pathToFileURL(join(output, 'index.js')).href
    );
    assert.equal(VALUE_VERSION, 1);
    const client = createClient({
      baseUrl,
      fetch: (url: string, init: RequestInit) =>
        fetch(url, {
          ...init,
          headers: { ...init.headers, authorization: `Bearer ${session.token}` },
        }),
    });
    assert.deepEqual((await client.objects.values.get(uid)).object, expected);
    assert.equal(
      (
        await client.objects.values.query({
          filter: { op: 'in', args: [{ field: 'count' }, { literal: ['9223372036854775807'] }] },
        })
      ).objects.length,
      1,
    );
    assert.equal(
      (
        await client.objects.values.query({
          filter: { op: 'in', args: [{ field: 'count' }, { literal: [] }] },
        })
      ).objects.length,
      0,
    );
    assert.equal(
      (
        await client.objects.values.query({
          filter: { op: 'contains', args: [{ field: 'amounts' }, { literal: '1.2' }] },
        })
      ).objects.length,
      1,
    );
    await client.actions['edit-values']({ uid, amount: '1.2' }, { idempotencyKey: 'typed-edit' });
    assert.equal((await client.objects.values.get(uid)).object.amount, '1.200000000');
    const revision = (
      await db.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [environment])
    ).rows[0].revision;
    const changed = {
      ...schema,
      properties: { ...schema.properties, extra: { type: 'string' as const, nullable: true } },
    };
    await store.activate(environment, 'values', changed, streamed, state.epoch, revision);
    const current = (
      await db.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [environment])
    ).rows[0].revision;
    await assert.rejects(
      store.stage(
        environment,
        'values',
        {
          ...changed,
          properties: { ...changed.properties, count: { type: 'string', nullable: false } },
        },
        streamed,
        state.epoch,
        current,
      ),
      { code: 'MIGRATION_REQUIRED' },
    );
    await assert.rejects(
      iceberg({ operation: 'read', metadataLocation: pin.metadataLocation, valueVersion: 2 }),
      { code: 'UNSUPPORTED_VALUE_VERSION' },
    );
    for (const value of [NaN, undefined, 1n])
      await assert.rejects(
        iceberg({
          operation: 'stage',
          warehouse: join(db.root, 'invalid-native'),
          schema: {
            type: 'struct',
            'schema-id': 0,
            fields: [{ id: 1, name: 'value', type: 'double', required: false }],
          },
          rows: [{ value }],
        }),
        { code: 'VALUE_INVALID' },
      );
    await assert.rejects(
      iceberg({
        operation: 'stage',
        warehouse: join(db.root, 'unsafe'),
        schema: {
          type: 'struct',
          'schema-id': 0,
          fields: [{ id: 1, name: 'value', type: 'long', required: true }],
        },
        rows: [{ value: 9007199254740992 }],
      }),
      /safe JSON range/,
    );
    await assert.rejects(
      iceberg({
        operation: 'stage',
        warehouse: join(db.root, 'float32'),
        schema: {
          type: 'struct',
          'schema-id': 0,
          fields: [{ id: 1, name: 'value', type: 'float', required: true }],
        },
        rows: [{ value: 0.1 }],
      }),
      /narrowing/,
    );
  },
);
