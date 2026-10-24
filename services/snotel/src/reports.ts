import { createHash } from 'node:crypto';
import { MoriaError } from '../../../packages/contracts/types.js';
import { validDate } from '../../../packages/contracts/values.js';
import type {
  Download,
  JsonLoader,
  SyncConfig,
  SnowObservation,
  SnowStation,
} from './connector.js';
const columns = [
  'Date',
  'Station Name',
  'Station Id',
  'State Code',
  'Network Code',
  'Latitude',
  'Longitude',
  'Elevation (ft)',
  'Snow Water Equivalent (in) End of Day Values',
  'Snow Depth (in) End of Day Values',
];
const fail = (): never => {
  throw new MoriaError(
    'SOURCE_INVALID',
    'NRCS CSV has unexpected metadata, units, dates, or values.',
  );
};
function csv(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [],
    field = '',
    quoted = false,
    closed = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (quoted) {
      if (c === '"' && text[i + 1] === '"') {
        field += '"';
        i++;
      } else if (c === '"') {
        quoted = false;
        closed = true;
      } else field += c;
    } else if (c === '"' && !field && !closed) quoted = true;
    else if (c === ',' || c === '\n' || c === '\r') {
      row.push(field);
      field = '';
      closed = false;
      if (c !== ',') {
        if (c === '\r' && text[i + 1] === '\n') i++;
        if (row.some(Boolean)) rows.push(row);
        row = [];
      }
    } else {
      if (closed || c === '"') fail();
      field += c;
    }
  }
  if (quoted) fail();
  if (field || row.length) {
    row.push(field);
    rows.push(row);
  }
  return rows;
}
export function parseReport(
  body: string,
  triplet: string,
  config: SyncConfig,
  retrievedAt: string,
): { station: SnowStation; observations: SnowObservation[] } {
  if (
    !/^# Reporting Frequency: Daily\s*$/m.test(body) ||
    !/^# Report Instantaneous Data As: End of Day\s*$/m.test(body)
  )
    fail();
  const table = csv(
    body
      .replace(/^\uFEFF/, '')
      .split(/\r?\n/)
      .filter((line) => !line.startsWith('#'))
      .join('\n'),
  );
  if (JSON.stringify(table.shift()) !== JSON.stringify(columns) || !table.length) fail();
  const first = table[0];
  if (first.length !== columns.length) fail();
  const number = (s: string) => {
    if (!/^-?\d+(\.\d+)?$/.test(s) || !Number.isFinite(Number(s))) return fail();
    return Number(s);
  };
  const station: SnowStation = {
    triplet,
    name: first[1],
    state: first[3],
    latitude: number(first[5]),
    longitude: number(first[6]),
    elevationFt: number(first[7]),
    county: '',
    timeZoneOffset: null,
    retrievedAt,
  };
  if (
    !station.name ||
    station.name.length > 200 ||
    Math.abs(station.latitude) > 90 ||
    Math.abs(station.longitude) > 180
  )
    fail();
  const days = new Map<string, SnowObservation>();
  for (
    let d = config.beginDate;
    d <= config.endDate;
    d = new Date(Date.parse(d) + 86400000).toISOString().slice(0, 10)
  )
    days.set(d, {
      triplet,
      date: d,
      snowDepth: null,
      swe: null,
      depthQc: '',
      depthQa: '',
      sweQc: '',
      sweQa: '',
      unit: 'in',
      periodRef: 'END',
      retrievedAt,
    });
  const seen = new Set<string>();
  for (const row of table) {
    if (
      row.length !== columns.length ||
      `${row[2]}:${row[3]}:${row[4]}` !== triplet ||
      row.slice(1, 8).some((v, i) => v !== first[i + 1]) ||
      !validDate(row[0]) ||
      !days.has(row[0]) ||
      seen.has(row[0])
    )
      fail();
    seen.add(row[0]);
    const day = days.get(row[0])!;
    day.swe = row[8] === '' ? null : number(row[8]);
    day.snowDepth = row[9] === '' ? null : number(row[9]);
  }
  return { station, observations: [...days.values()] };
}
export async function downloadReports(config: SyncConfig, load: JsonLoader): Promise<Download> {
  const retrievedAt = new Date().toISOString();
  const result: Download = { config, retrievedAt, stations: [], observations: [], raw: [] };
  for (const triplet of config.stations) {
    const url = new URL(
      `https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customSingleStationReport/daily/end_of_period/${triplet}/${config.beginDate},${config.endDate}/name,stationId,state.code,network.code,latitude,longitude,elevation,WTEQ::value,SNWD::value`,
    );
    const body = await load(url);
    if (typeof body !== 'string') fail();
    const parsed = parseReport(body as string, triplet, config, retrievedAt);
    result.stations.push(parsed.station);
    result.observations.push(...parsed.observations);
    result.raw.push({
      url: url.href,
      body: body as string,
      sha256: createHash('sha256')
        .update(body as string)
        .digest('hex'),
    });
  }
  if (!result.observations.some((r) => r.snowDepth !== null || r.swe !== null))
    throw new MoriaError(
      'SOURCE_EMPTY',
      'No observations were returned; the previous data is retained.',
    );
  return result;
}
