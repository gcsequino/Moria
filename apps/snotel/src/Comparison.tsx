import React, { useEffect, useState } from 'react';

import { sdk, type Observation, type Station, type Status } from './client.js';

import {
  daysBefore,
  nearestStation,
  snowChange,
  latestReport,
  reportIsStale,
} from './snow-comparison.js';

type WeatherView = NonNullable<Status['weather']>;

const change = (value: number | null) =>
  value === null ? 'Unavailable' : `${value > 0 ? '+' : ''}${value.toFixed(1)} in`;

export function Comparison({
  weather,

  stations,

  observationEnd,

  onStation,
}: {
  weather: WeatherView;

  stations: Station[];

  observationEnd?: string;

  onStation: (triplet: string) => void;
}) {
  const [rows, setRows] = useState<Observation[]>([]),
    [error, setError] = useState(''),
    [loading, setLoading] = useState(false);

  useEffect(() => {
    const controller = new AbortController();

    setRows([]);

    setError('');

    if (!observationEnd || !stations.length) {
      setLoading(false);

      return;
    }

    setLoading(true);

    void sdk.objects['snow-observations']

      .all(
        {
          filter: {
            op: 'and',

            args: [
              { op: 'gte', args: [{ field: 'date' }, { literal: daysBefore(observationEnd, 14) }] },

              { op: 'lte', args: [{ field: 'date' }, { literal: observationEnd }] },

              {
                op: 'in',

                args: [{ field: 'triplet' }, { literal: stations.map((s) => s.triplet) }],
              },
            ],
          },

          pageSize: 1000,
        },

        { signal: controller.signal, maxRows: 1000 },
      )

      .then((result) => {
        if (!controller.signal.aborted) setRows(result);
      })

      .catch((e) => {
        if (!controller.signal.aborted)
          setError(e instanceof Error ? e.message : 'Could not load recent observations.');
      })

      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });

    return () => controller.abort();
  }, [observationEnd, stations]);

  const now = Date.now();

  const aged = observationEnd && now - Date.parse(`${observationEnd}T00:00:00Z`) > 3 * 86400000;

  return (
    <section className="comparison" aria-label="Compare Front Range areas">
      <h3>Compare your areas</h3>

      <p>
        Forecasts for representative points, alongside the nearest station among this instance’s
        loaded SNOTEL sites. Distance is straight-line; terrain, aspect and elevation can differ
        substantially.
      </p>

      {loading && <p role="status">Loading recent station reports…</p>}

      {error && (
        <p className="comparison-error" role="alert">
          {error}
        </p>
      )}

      <div
        className="comparison-scroll"

        tabIndex={0}

        role="region"

        aria-label="Area comparison table, scroll horizontally on smaller screens"
      >
        <table>
          <caption>
            Next forecast period and observed snowpack
            {observationEnd ? ` · requested coverage through ${observationEnd}` : ''}
          </caption>

          <thead>
            <tr>
              <th scope="col">Area / forecast point</th>

              <th scope="col">Next NWS period</th>

              <th scope="col">Nearby loaded station</th>

              <th scope="col">Observed snowpack</th>

              <th scope="col">Depth change, 1 / 7 days</th>

              <th scope="col">SWE change, 1 / 7 days</th>
            </tr>
          </thead>

          <tbody>
            {weather.areas.map((area) => {
              const forecast = area.periods.find((p) => Date.parse(p.endTime) > now),
                nearest = nearestStation(area, stations);

              const observations = nearest
                ? rows.filter((r) => r.triplet === nearest.station.triplet)
                : [];

              const latest = observationEnd
                ? latestReport(observations, observationEnd)
                : undefined;

              const usable = latest?.unit === 'in';

              return (
                <tr key={area.id}>
                  <th scope="row">
                    {area.name}

                    <small>
                      {area.latitude.toFixed(3)}°, {area.longitude.toFixed(3)}°
                    </small>
                  </th>

                  <td>
                    {forecast ? (
                      <>
                        <strong>
                          {forecast.name} · {forecast.temperature}°{forecast.temperatureUnit}
                        </strong>

                        <span>{forecast.shortForecast}</span>

                        <span>
                          {forecast.windDirection} {forecast.windSpeed}
                        </span>

                        <span>
                          Precipitation:{' '}
                          {forecast.precipitationProbability == null
                            ? 'not supplied'
                            : `${forecast.precipitationProbability}%`}
                        </span>

                        <small>
                          {new Date(forecast.startTime).toLocaleString()} –{' '}
                          {new Date(forecast.endTime).toLocaleString()}
                        </small>
                      </>
                    ) : (
                      <strong>Forecast expired</strong>
                    )}

                    <small>
                      {now - Date.parse(area.sourceUpdatedAt) > 12 * 3600000
                        ? 'May be stale · '
                        : ''}
                      Issued {new Date(area.sourceUpdatedAt).toLocaleString()}
                    </small>
                  </td>

                  <td>
                    {nearest ? (
                      <>
                        <button onClick={() => onStation(nearest.station.triplet)}>
                          {nearest.station.name} ↘
                        </button>

                        <span>
                          {nearest.miles.toFixed(1)} mi away ·{' '}
                          {nearest.station.elevationFt.toLocaleString()} ft
                        </span>

                        <small>
                          {nearest.station.triplet} · {nearest.station.latitude.toFixed(3)}°,{' '}
                          {nearest.station.longitude.toFixed(3)}°
                        </small>
                      </>
                    ) : (
                      'No loaded station'
                    )}
                  </td>

                  <td>
                    {loading ? (
                      'Loading…'
                    ) : error ? (
                      'Unavailable'
                    ) : (
                      <>
                        <span>
                          Depth:{' '}
                          {usable && latest.snowDepth != null
                            ? `${latest.snowDepth.toFixed(1)} in`
                            : 'not reported'}
                        </span>

                        <span>
                          SWE:{' '}
                          {usable && latest.swe != null
                            ? `${latest.swe.toFixed(1)} in`
                            : 'not reported'}
                        </span>

                        <small>
                          {latest
                            ? `Latest available: ${latest.date} · NRCS period-end`
                            : 'No reported values in the last eight days of requested coverage'}
                        </small>
                        {latest && reportIsStale(latest.date, now) && (
                          <strong className="comparison-error">
                            Report is over three days old
                          </strong>
                        )}
                        {latest && latest.date !== observationEnd && (
                          <small>
                            Requested through {observationEnd}; no later usable report loaded.
                          </small>
                        )}

                        {latest && (
                          <small>
                            Retrieved {new Date(latest.retrievedAt).toLocaleString()}
                            <br />
                            Depth QC/QA: {latest.depthQc || '—'}/{latest.depthQa || '—'} · SWE:{' '}
                            {latest.sweQc || '—'}/{latest.sweQa || '—'}
                          </small>
                        )}
                      </>
                    )}
                  </td>

                  <td>
                    {latest && !loading && !error
                      ? `${change(snowChange(observations, latest.date, 1, 'snowDepth'))} / ${change(snowChange(observations, latest.date, 7, 'snowDepth'))}`
                      : 'Unavailable'}
                  </td>

                  <td>
                    {latest && !loading && !error
                      ? `${change(snowChange(observations, latest.date, 1, 'swe'))} / ${change(snowChange(observations, latest.date, 7, 'swe'))}`
                      : 'Unavailable'}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {aged && (
        <p className="comparison-error">
          Observation window is more than three days old. These values describe the saved period,
          not today’s conditions.
        </p>
      )}

      <p className="comparison-note">
        Changes are anchored to each station’s displayed report date and compare exact daily
        endpoints; unavailable means an endpoint is missing or withheld, not zero. Snow depth change
        includes settling, melt and wind redistribution and is not new snowfall. SWE measures water
        stored in the snowpack. Precipitation probability is not snowfall accumulation.
      </p>
    </section>
  );
}
