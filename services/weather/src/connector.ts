import { createHash } from 'node:crypto';

export const areas = [
  { id: 'berthoud', name: 'Berthoud Pass', latitude: 39.798, longitude: -105.777 },
  { id: 'jones', name: 'Jones Pass / Butler Gulch', latitude: 39.779, longitude: -105.874 },
  { id: 'loveland', name: 'Loveland Pass / Dry Gulch', latitude: 39.664, longitude: -105.879 },
  {
    id: 'indian-peaks',
    name: 'Indian Peaks / Brainard Lake',
    latitude: 40.077,
    longitude: -105.585,
  },
  { id: 'cameron', name: 'Cameron Pass', latitude: 40.521, longitude: -105.893 },
] as const;
export type ForecastPeriod = {
  name: string;
  startTime: string;
  endTime: string;
  isDaytime: boolean;
  temperature: number;
  temperatureUnit: string;
  windSpeed: string;
  windDirection: string;
  precipitationProbability: number | null;
  shortForecast: string;
  detailedForecast: string;
};
export type WeatherArea = {
  id: string;
  name: string;
  latitude: number;
  longitude: number;
  forecastUrl: string;
  sourceUpdatedAt: string;
  retrievedAt: string;
  periods: ForecastPeriod[];
};
export type WeatherView = {
  publishedAt: string | null;
  jobId: string | null;
  areas: WeatherArea[];
};
export type RawResponse = { url: string; sha256: string; body: string; retrievedAt: string };
export function officialUrl(value: string): string {
  const url = new URL(value);
  if (
    url.protocol !== 'https:' ||
    url.hostname !== 'api.weather.gov' ||
    url.port ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    !/^\/(?:points\/-?\d+(?:\.\d+)?,-?\d+(?:\.\d+)?|gridpoints\/[A-Z]{3}\/\d+,\d+\/forecast)$/.test(
      url.pathname,
    )
  ) {
    throw new Error('NWS response contained an unsupported source URL');
  }
  return url.href;
}
export async function loadNws(url: string): Promise<RawResponse> {
  const response = await fetch(officialUrl(url), {
    headers: {
      'User-Agent': 'MoriaSnow/1.0 (https://snow.gsequino.dev)',
      Accept: 'application/geo+json',
    },
    redirect: 'error',
    signal: AbortSignal.timeout(20000),
  });
  if (!response.ok) throw new Error(`NWS returned HTTP ${response.status}`);
  if (!response.body) throw new Error('NWS response was empty');
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      size += next.value.length;
      if (size > 2_000_000) throw new Error('NWS response exceeded 2 MB');
      chunks.push(next.value);
    }
  } finally {
    await reader.cancel();
  }
  const body = Buffer.concat(chunks).toString('utf8');
  return {
    url,
    body,
    sha256: createHash('sha256').update(body).digest('hex'),
    retrievedAt: new Date().toISOString(),
  };
}
function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('Malformed NWS object');
  return value as Record<string, unknown>;
}
function str(value: unknown): string {
  if (typeof value !== 'string' || value.length > 20000) throw new Error('Malformed NWS text');
  return value;
}
function timestamp(value: unknown): string {
  const result = str(value);
  if (!Number.isFinite(Date.parse(result))) throw new Error('Malformed NWS timestamp');
  return result;
}
export function parseForecast(body: string): {
  sourceUpdatedAt: string;
  periods: ForecastPeriod[];
} {
  const props = object(object(JSON.parse(body)).properties);
  if (!Array.isArray(props.periods) || !props.periods.length || props.periods.length > 32)
    throw new Error('Malformed NWS periods');
  const periods = props.periods.map((value) => {
    const p = object(value),
      probability = object(p.probabilityOfPrecipitation).value;
    if (
      typeof p.temperature !== 'number' ||
      !Number.isFinite(p.temperature) ||
      typeof p.isDaytime !== 'boolean' ||
      (probability !== null &&
        (typeof probability !== 'number' ||
          !Number.isFinite(probability) ||
          probability < 0 ||
          probability > 100))
    )
      throw new Error('Malformed NWS forecast values');
    const startTime = timestamp(p.startTime),
      endTime = timestamp(p.endTime);
    if (Date.parse(endTime) <= Date.parse(startTime))
      throw new Error('Invalid NWS period interval');
    return {
      name: str(p.name),
      startTime,
      endTime,
      isDaytime: p.isDaytime,
      temperature: p.temperature,
      temperatureUnit: str(p.temperatureUnit),
      windSpeed: str(p.windSpeed),
      windDirection: str(p.windDirection),
      precipitationProbability: probability as number | null,
      shortForecast: str(p.shortForecast),
      detailedForecast: str(p.detailedForecast),
    };
  });
  return { sourceUpdatedAt: timestamp(props.updateTime), periods };
}
export async function downloadWeather(guard: () => Promise<void>, load = loadNws) {
  const forecasts: WeatherArea[] = [],
    raw: RawResponse[] = [];
  // Sequential requests keep this small, scheduled workload gentle on the public API.
  for (const area of areas) {
    await guard();
    const point = await load(`https://api.weather.gov/points/${area.latitude},${area.longitude}`);
    const forecastUrl = officialUrl(
      str(object(object(JSON.parse(point.body)).properties).forecast),
    );
    await guard();
    const forecast = await load(forecastUrl);
    forecasts.push({
      ...area,
      forecastUrl,
      retrievedAt: forecast.retrievedAt,
      ...parseForecast(forecast.body),
    });
    raw.push(point, forecast);
  }
  return { areas: forecasts, raw };
}
