import { resolve4 } from 'node:dns/promises';
import { get } from 'node:https';
import { createHash } from 'node:crypto';
import { MoriaError } from '../../../packages/contracts/types.js';
import { validDate } from '../../../packages/contracts/values.js';
export const AWDB = 'https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/';
export interface SyncConfig {
  stations: string[];
  beginDate: string;
  endDate: string;
  source?: 'awdb' | 'report-csv';
}
export const defaultConfig: SyncConfig = {
  source: 'report-csv',
  stations: ['663:CO:SNTL', '335:CO:SNTL', '602:CO:SNTL', '842:CO:SNTL', '485:CO:SNTL'],
  beginDate: '2025-10-01',
  endDate: '2026-10-20',
};
export function configInput(input: unknown): SyncConfig {
  const c = input as SyncConfig;
  if (
    !c ||
    typeof c !== 'object' ||
    Object.keys(c).some((k) => !['stations', 'beginDate', 'endDate', 'source'].includes(k)) ||
    (c.source !== undefined && !['awdb', 'report-csv'].includes(c.source)) ||
    !Array.isArray(c.stations) ||
    c.stations.length < 1 ||
    c.stations.length > 20 ||
    c.stations.some((s) => typeof s !== 'string' || !/^\d{1,8}:[A-Z]{2}:SNTL$/.test(s)) ||
    new Set(c.stations).size !== c.stations.length ||
    typeof c.beginDate !== 'string' ||
    typeof c.endDate !== 'string' ||
    !validDate(c.beginDate) ||
    !validDate(c.endDate)
  )
    throw new MoriaError(
      'INPUT_INVALID',
      'Specify 1–20 unique SNOTEL triplets and valid beginDate/endDate.',
    );
  const days = (Date.parse(c.endDate) - Date.parse(c.beginDate)) / 86400000;
  if (days < 0 || days > 399)
    throw new MoriaError('INPUT_INVALID', 'Choose an inclusive range of 1–400 days.');
  return structuredClone(c);
}
export type JsonLoader = (url: URL) => Promise<unknown>;
let preferredAddress: string | undefined;
/** Try only addresses resolved for the official host. TLS/SNI still verifies that host. */
export const fetchNrcsText = async (url: URL): Promise<string> => {
  if (
    url.origin !== new URL(AWDB).origin ||
    !(
      url.pathname.startsWith(new URL(AWDB).pathname) ||
      url.pathname.startsWith('/reportGenerator/view_csv/')
    )
  )
    throw new MoriaError('SOURCE_INVALID', 'Only the official AWDB service is allowed.');
  const addresses = await resolve4(url.hostname);
  if (preferredAddress && addresses.includes(preferredAddress))
    addresses.sort((a, b) => Number(b === preferredAddress) - Number(a === preferredAddress));
  let last: unknown;
  for (let attempt = 0; attempt < Math.min(3, addresses.length + 1); attempt++) {
    const address = addresses[attempt % addresses.length];
    try {
      const result = await new Promise<string>((done, fail) => {
        let size = 0;
        const chunks: Buffer[] = [];
        const req = get(
          url,
          {
            // A fresh socket makes retrying a different DNS address effective.
            agent: false,
            family: 4,
            lookup: (_h, _o, cb) => cb(null, address, 4),
            headers: {
              accept: 'application/json,text/csv,text/plain',
              'user-agent': 'Moria-SNOTEL/0.1',
            },
          },
          (res) => {
            if (res.statusCode !== 200) {
              res.resume();
              fail(new MoriaError('UPSTREAM_HTTP', `NRCS returned HTTP ${res.statusCode}.`));
              return;
            }
            res.on('data', (bytes: Buffer) => {
              size += bytes.length;
              if (size > 8 * 1024 * 1024) req.destroy(new Error('NRCS response exceeds 8 MiB'));
              else chunks.push(bytes);
            });
            res.on('error', fail);
            res.on('end', () => {
              try {
                done(Buffer.concat(chunks).toString('utf8'));
              } catch {
                fail(new Error('NRCS returned invalid JSON'));
              }
            });
          },
        );
        const connectDeadline = setTimeout(
          () => req.destroy(new Error('NRCS connection timed out')),
          6000,
        );
        req.once('socket', (socket) =>
          socket.once('secureConnect', () => clearTimeout(connectDeadline)),
        );
        const deadline = setTimeout(() => req.destroy(new Error('NRCS request timed out')), 60000);
        req.setTimeout(45000, () => req.destroy(new Error('NRCS connection stalled')));
        req.once('error', fail);
        req.once('close', () => {
          clearTimeout(deadline);
          clearTimeout(connectDeadline);
        });
      });
      preferredAddress = address;
      return result;
    } catch (error) {
      last = error;
    }
  }
  throw new MoriaError(
    'UPSTREAM_UNAVAILABLE',
    `NRCS could not be reached after bounded retries: ${last instanceof Error ? last.message : 'request failed'}`,
  );
};
export const fetchAwdb: JsonLoader = async (url) => {
  try {
    return JSON.parse(await fetchNrcsText(url));
  } catch (error) {
    if (error instanceof MoriaError) throw error;
    throw new MoriaError('SOURCE_INVALID', 'NRCS returned invalid JSON.');
  }
};
interface Element {
  elementCode: string;
  ordinal: number;
  heightDepth?: number | null;
  durationName: string;
  storedUnitCode: string;
}
interface Point {
  date: string;
  value?: number | null;
  qcFlag?: string;
  qaFlag?: string;
}
interface StationPayload {
  stationTriplet: string;
  name: string;
  stateCode: string;
  latitude: number;
  longitude: number;
  elevation: number;
  dataTimeZone: number;
  countyName?: string;
}
interface DataPayload {
  stationTriplet: string;
  data: { stationElement: Element; values: Point[]; error?: string }[];
}
export interface SnowStation {
  triplet: string;
  name: string;
  state: string;
  latitude: number;
  longitude: number;
  elevationFt: number;
  timeZoneOffset: number | null;
  county: string;
  retrievedAt: string;
}
export interface SnowObservation {
  triplet: string;
  date: string;
  snowDepth: number | null;
  swe: number | null;
  depthQc: string;
  depthQa: string;
  sweQc: string;
  sweQa: string;
  unit: string;
  periodRef: string;
  retrievedAt: string;
}
export interface Download {
  stations: SnowStation[];
  observations: SnowObservation[];
  raw: { url: string; sha256: string; body: string }[];
  retrievedAt: string;
  config: SyncConfig;
}
export async function downloadSnow(input: unknown, load?: JsonLoader): Promise<Download> {
  const config = configInput(input);
  if (config.source === 'report-csv') {
    const { downloadReports } = await import('./reports.js');
    return downloadReports(config, load ?? fetchNrcsText);
  }
  const loader = load ?? fetchAwdb;
  const retrievedAt = new Date().toISOString(),
    raw: Download['raw'] = [];
  const request = async (path: string, params: Record<string, string>) => {
    const url = new URL(path, AWDB);
    url.search = new URLSearchParams(params).toString();
    const payload = await loader(url);
    const body = JSON.stringify(payload);
    raw.push({ url: url.href, sha256: createHash('sha256').update(body).digest('hex'), body });
    return payload;
  };
  const metadata = (await request('stations', {
    stationTriplets: config.stations.join(','),
    activeOnly: 'false',
  })) as StationPayload[];
  if (!Array.isArray(metadata) || metadata.length !== config.stations.length)
    throw new MoriaError('SOURCE_INVALID', 'NRCS did not return every requested station.');
  const stations: SnowStation[] = [],
    observations: SnowObservation[] = [],
    seenStations = new Set<string>();
  for (const row of metadata) {
    if (
      !config.stations.includes(row.stationTriplet) ||
      seenStations.has(row.stationTriplet) ||
      typeof row.name !== 'string' ||
      row.name.length > 200 ||
      ![row.latitude, row.longitude, row.elevation, row.dataTimeZone].every(Number.isFinite) ||
      row.stateCode !== row.stationTriplet.split(':')[1] ||
      Math.abs(row.latitude) > 90 ||
      Math.abs(row.longitude) > 180 ||
      Math.abs(row.dataTimeZone) > 14
    )
      throw new MoriaError('SOURCE_INVALID', 'Invalid NRCS station metadata.');
    seenStations.add(row.stationTriplet);
    stations.push({
      triplet: row.stationTriplet,
      name: row.name,
      state: row.stateCode,
      latitude: row.latitude,
      longitude: row.longitude,
      elevationFt: row.elevation,
      timeZoneOffset: row.dataTimeZone,
      county: row.countyName ?? '',
      retrievedAt,
    });
    const payload = (await request('data', {
      stationTriplets: row.stationTriplet,
      elements: 'SNWD,WTEQ',
      duration: 'DAILY',
      beginDate: config.beginDate,
      endDate: config.endDate,
      periodRef: 'END',
      returnFlags: 'true',
      returnSuspectData: 'false',
    })) as DataPayload[];
    if (
      !Array.isArray(payload) ||
      payload.length !== 1 ||
      payload[0].stationTriplet !== row.stationTriplet ||
      !Array.isArray(payload[0].data)
    )
      throw new MoriaError('SOURCE_INVALID', 'NRCS returned an unexpected observation response.');
    const days = new Map<string, SnowObservation>();
    for (
      let date = config.beginDate;
      date <= config.endDate;
      date = new Date(Date.parse(date) + 86400000).toISOString().slice(0, 10)
    )
      days.set(date, {
        triplet: row.stationTriplet,
        date,
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
    const elements = new Set<string>();
    for (const series of payload[0].data) {
      if (series.error)
        throw new MoriaError(
          'SOURCE_INVALID',
          `NRCS reported an element error for ${row.stationTriplet}.`,
        );
      const e = series.stationElement;
      if (
        !e ||
        !['SNWD', 'WTEQ'].includes(e.elementCode) ||
        e.ordinal !== 1 ||
        e.heightDepth != null ||
        e.durationName !== 'DAILY' ||
        e.storedUnitCode !== 'in' ||
        elements.has(e.elementCode) ||
        !Array.isArray(series.values)
      )
        throw new MoriaError('SOURCE_INVALID', 'Unexpected NRCS element, duration, or units.');
      elements.add(e.elementCode);
      const dates = new Set<string>();
      for (const value of series.values) {
        if (
          !validDate(value.date) ||
          !days.has(value.date) ||
          dates.has(value.date) ||
          (value.value != null &&
            (typeof value.value !== 'number' || !Number.isFinite(value.value)))
        )
          throw new MoriaError('SOURCE_INVALID', 'Invalid or duplicate NRCS daily value.');
        for (const flag of [value.qcFlag, value.qaFlag])
          if (flag !== undefined && (typeof flag !== 'string' || flag.length > 32))
            throw new MoriaError('SOURCE_INVALID', 'Invalid quality flag.');
        dates.add(value.date);
        const day = days.get(value.date)!;
        if (e.elementCode === 'SNWD') {
          day.snowDepth = value.value ?? null;
          day.depthQc = value.qcFlag ?? '';
          day.depthQa = value.qaFlag ?? '';
        } else {
          day.swe = value.value ?? null;
          day.sweQc = value.qcFlag ?? '';
          day.sweQa = value.qaFlag ?? '';
        }
      }
    }
    observations.push(...days.values());
  }
  if (!observations.some((o) => o.snowDepth !== null || o.swe !== null))
    throw new MoriaError(
      'SOURCE_EMPTY',
      'No observations were returned; the previous data is retained.',
    );
  return { stations, observations, raw, retrievedAt, config };
}
