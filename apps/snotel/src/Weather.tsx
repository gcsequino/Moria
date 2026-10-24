import React, { useState } from 'react';
import { Comparison } from './Comparison.js';
import type { Station, Status } from './generated/index.js';
type WeatherView = NonNullable<Status['weather']>;

export function Weather({
  weather,
  stations,
  observationEnd,
  onStation,
}: {
  weather?: WeatherView;
  stations: Station[];
  observationEnd?: string;
  onStation: (triplet: string) => void;
}) {
  const [selected, setSelected] = useState('berthoud');
  const area = weather?.areas.find((item) => item.id === selected) ?? weather?.areas[0];
  const periods = area?.periods.filter((p) => Date.parse(p.endTime) > Date.now()) ?? [];
  const stale =
    !area || Date.now() - Date.parse(area.sourceUpdatedAt) > 12 * 3600000 || !periods.length;
  return (
    <section className="weather-panel" aria-label="Front Range weather">
      <div className="weather-heading">
        <div>
          <div className="eyebrow">FRONT RANGE / NWS FORECASTS</div>
          <h2>Plan around the conditions.</h2>
        </div>
        <div className="weather-links">
          <a href="https://avalanche.state.co.us/" target="_blank" rel="noreferrer">
            CAIC avalanche forecast ↗
          </a>
          <a href="https://www.frontrangeskimo.com/" target="_blank" rel="noreferrer">
            Front Range route guide ↗
          </a>
        </div>
      </div>
      <p>
        Representative mountain points for your usual areas. Forecast grids and SNOTEL stations do
        not describe every slope or elevation. Check the current CAIC forecast before choosing
        avalanche terrain.
      </p>
      {!area ? (
        <div className="weather-empty">
          No weather forecast has been published yet. An instance operator can run a weather refresh
          in management.
        </div>
      ) : (
        <>
          <Comparison
            weather={weather!}
            stations={stations}
            observationEnd={observationEnd}
            onStation={onStation}
          />
          <h3 className="forecast-detail-title">Explore an area forecast</h3>
          <div className="weather-tabs" role="group" aria-label="Forecast area">
            {weather!.areas.map((item) => (
              <button
                key={item.id}
                aria-pressed={item.id === area.id}
                onClick={() => setSelected(item.id)}
              >
                {item.name}
              </button>
            ))}
          </div>
          <div className="weather-meta">
            <span>
              {area.latitude.toFixed(3)}°, {area.longitude.toFixed(3)}° · representative point
            </span>
            <a
              href={`https://forecast.weather.gov/MapClick.php?lat=${area.latitude}&lon=${area.longitude}`}
              target="_blank"
              rel="noreferrer"
            >
              Full NWS forecast ↗
            </a>
          </div>
          <div className={stale ? 'weather-freshness stale' : 'weather-freshness'} role="status">
            {stale ? 'Forecast may be stale. ' : ''}NWS issued{' '}
            {new Date(area.sourceUpdatedAt).toLocaleString()} · retrieved{' '}
            {new Date(area.retrievedAt).toLocaleString()}
          </div>
          {!periods.length ? (
            <p>
              The saved forecast has expired. Consult the current NWS forecast while the next
              refresh runs.
            </p>
          ) : (
            <div className="forecast-periods">
              {periods.slice(0, 8).map((period) => (
                <article key={period.startTime} className="forecast-period">
                  <h3>{period.name}</h3>
                  <div className="forecast-temperature">
                    {period.temperature}°{period.temperatureUnit}
                  </div>
                  <p className="forecast-summary">{period.shortForecast}</p>
                  <dl>
                    <div>
                      <dt>Wind</dt>
                      <dd>
                        {period.windDirection} {period.windSpeed}
                      </dd>
                    </div>
                    <div>
                      <dt>Precipitation chance</dt>
                      <dd>
                        {period.precipitationProbability == null
                          ? 'Not supplied'
                          : `${period.precipitationProbability}%`}
                      </dd>
                    </div>
                  </dl>
                  <details>
                    <summary>Forecast details</summary>
                    <p>{period.detailedForecast}</p>
                    <small>
                      {new Date(period.startTime).toLocaleString()} –{' '}
                      {new Date(period.endTime).toLocaleString()}
                    </small>
                  </details>
                </article>
              ))}
            </div>
          )}
          <details className="weather-provenance">
            <summary>Source and refresh details</summary>
            <p>
              National Weather Service ·{' '}
              <a href={area.forecastUrl} target="_blank" rel="noreferrer">
                Source response
              </a>{' '}
              · published {new Date(weather!.publishedAt!).toLocaleString()}
            </p>
            <code>Refresh {weather!.jobId}</code>
          </details>
        </>
      )}
      <p className="weather-note">
        Precipitation chance is not predicted snowfall accumulation. The station charts below show
        measured snow depth and snow-water equivalent; changes in depth are not a snowfall total.
      </p>
    </section>
  );
}
