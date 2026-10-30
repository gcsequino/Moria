import type {
  ObservationProperties as Observation,
  StationProperties as Station,
} from './generated/index.js';

export function distanceMiles(
  a: { latitude: number; longitude: number },
  b: { latitude: number; longitude: number },
) {
  const radians = (n: number) => (n * Math.PI) / 180;
  const latitude = radians(b.latitude - a.latitude),
    longitude = radians(b.longitude - a.longitude);
  const h =
    Math.sin(latitude / 2) ** 2 +
    Math.cos(radians(a.latitude)) * Math.cos(radians(b.latitude)) * Math.sin(longitude / 2) ** 2;
  return 3958.7613 * 2 * Math.asin(Math.sqrt(Math.min(1, Math.max(0, h))));
}
export function nearestStation(
  point: { latitude: number; longitude: number },
  stations: Station[],
) {
  return stations
    .filter(
      (s) =>
        Number.isFinite(s.latitude) &&
        Number.isFinite(s.longitude) &&
        Math.abs(s.latitude) <= 90 &&
        Math.abs(s.longitude) <= 180,
    )
    .map((station) => ({ station, miles: distanceMiles(point, station) }))
    .sort((a, b) => a.miles - b.miles || a.station.triplet.localeCompare(b.station.triplet))[0];
}
export function daysBefore(date: string, days: number) {
  return new Date(Date.parse(`${date}T00:00:00Z`) - days * 86400000).toISOString().slice(0, 10);
}
/** Compare exact daily endpoints only. Never substitute zero or bridge missing reports. */
export function snowChange(
  rows: Observation[],
  date: string,
  days: number,
  field: 'snowDepth' | 'swe',
) {
  const end = rows.find((r) => r.date === date),
    start = rows.find((r) => r.date === daysBefore(date, days));
  if (
    !end ||
    !start ||
    end.unit !== 'in' ||
    start.unit !== 'in' ||
    end[field] === null ||
    start[field] === null ||
    !Number.isFinite(end[field]) ||
    !Number.isFinite(start[field])
  )
    return null;
  return end[field]! - start[field]!;
}

/** One report for both metrics: never mix dates or treat empty placeholders as reports. */
export function latestReport(rows: Observation[], through: string) {
  const earliest = daysBefore(through, 7);
  return rows
    .filter(
      (row) =>
        row.date >= earliest &&
        row.date <= through &&
        row.unit === 'in' &&
        ((row.snowDepth !== null && Number.isFinite(row.snowDepth)) ||
          (row.swe !== null && Number.isFinite(row.swe))),
    )
    .sort((a, b) => b.date.localeCompare(a.date))[0];
}
export function reportIsStale(date: string, now = Date.now()) {
  return now - Date.parse(`${date}T00:00:00Z`) > 3 * 86400000;
}
