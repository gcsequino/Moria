import { createClient } from './generated/index.js';
import { createTransport } from '../../../packages/platform/src/transport.js';
import { filesystemClient } from '../../../packages/platform/src/filesystem.js';
import { functionsClient } from '../../../packages/platform/src/functions.js';
import type { AssetProperties as Asset, SourceRow } from './generated/index.js';
export type { AssetProperties as Asset, SourceRow, HostedApp } from './generated/index.js';
export type Page = import('./generated/index.js').Page<Asset>;
const options = {
  onUnauthenticated: () => window.dispatchEvent(new Event('moria:unauthenticated')),
};
export const sdk = createClient(options);
const transport = createTransport(options);
export const platform = {
  filesystem: filesystemClient(transport),
  functions: functionsClient(transport),
};
export const sample: SourceRow[] = [
  {
    id: 'PUMP-001',
    title: 'North intake pump',
    region: 'West',
    status: 'Inspection due',
    notes: 'Inspect the inlet seal.',
  },
  {
    id: 'VALVE-002',
    title: 'Cooling loop valve',
    region: 'West',
    status: 'Operational',
    notes: 'Quarterly inspection complete.',
  },
  {
    id: 'MOTOR-003',
    title: 'Conveyor drive motor',
    region: 'East',
    status: 'Maintenance',
    notes: 'Bearing replacement scheduled.',
  },
  {
    id: 'SENSOR-004',
    title: 'Pressure sensor',
    region: 'East',
    status: 'Operational',
    notes: 'Calibrated this month.',
  },
];
export function parseCSV(text: string): SourceRow[] {
  const rows: string[][] = [],
    row: string[] = [];
  let field = '',
    quoted = false,
    afterQuote = false;
  text = text.replace(/^\uFEFF/, '');
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (quoted) {
      if (c === '"' && text[i + 1] === '"') {
        field += '"';
        i++;
      } else if (c === '"') {
        quoted = false;
        afterQuote = true;
      } else field += c;
    } else if (c === '"' && !field && !afterQuote) quoted = true;
    else if (c === ',' || c === '\n' || c === '\r') {
      row.push(field);
      field = '';
      afterQuote = false;
      if (c !== ',') {
        if (c === '\r' && text[i + 1] === '\n') i++;
        if (row.some(Boolean)) rows.push([...row]);
        row.length = 0;
      }
    } else {
      if (afterQuote || c === '"') throw new Error('Malformed CSV quoting.');
      field += c;
    }
  }
  if (quoted) throw new Error('Unclosed CSV quote.');
  if (field || row.length || afterQuote) {
    row.push(field);
    rows.push(row);
  }
  const header = rows.shift(),
    expected = ['id', 'title', 'region', 'status', 'notes'];
  if (
    !header ||
    header.length !== 5 ||
    new Set(header).size !== 5 ||
    expected.some((name) => !header.includes(name))
  )
    throw new Error('CSV header must contain id,title,region,status,notes.');
  return rows.map((values, index) => {
    if (values.length !== header.length)
      throw new Error(`CSV row ${index + 2} has the wrong number of columns.`);
    return Object.fromEntries(header.map((name, i) => [name, values[i]])) as unknown as SourceRow;
  });
}
