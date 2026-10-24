import type { Catalog } from '../../catalog/src/catalog.js';
import { areas as weatherAreas, officialUrl } from '../../weather/src/connector.js';

export interface LineageGraph {
  nodes: { id: string; label: string; kind: string; status: string | null; detail: string }[];
  edges: { from: string; to: string; label: string }[];
  limitations: string[];
}

/** Trusted metadata reader. The HTTP caller must enforce instance administration
 * access; this is not a per-resource authorization boundary. Never return arbitrary
 * lineage/result/config/error JSON: those fields may contain secrets or file paths. */
export async function readLineage(catalog: Catalog): Promise<LineageGraph> {
  return catalog.transaction(
    async (client) => {
      const graph: LineageGraph = {
        nodes: [],
        edges: [],
        limitations: [
          'Bounded metadata view: up to 50 snapshots by revision, 50 recent index stages, 50 object generations by revision, 50 recent jobs, 200 attempts, and 50 recent Snow syncs. This is not the complete instance graph.',
          'Edges represent persisted relationships only. Missing provenance is not inferred from names, timestamps, or the latest attempt.',
          'Containment permissions and automatic marking propagation are not represented by this graph.',
        ],
      };
      const nodes = new Map<string, LineageGraph['nodes'][number]>();
      const edges = new Set<string>();
      const id = (kind: string, ...parts: unknown[]) => JSON.stringify([kind, ...parts]);
      const node = (
        key: string,
        label: string,
        kind: string,
        status: string | null,
        detail: Record<string, unknown>,
      ) => {
        if (!nodes.has(key)) {
          const value = { id: key, label, kind, status, detail: JSON.stringify(detail) };
          nodes.set(key, value);
          graph.nodes.push(value);
        }
        return key;
      };
      const edge = (from: string, to: string, label: string) => {
        const key = JSON.stringify([from, to, label]);
        if (!edges.has(key)) {
          edges.add(key);
          graph.edges.push({ from, to, label });
        }
      };
      const exists = async (table: string) =>
        Boolean(
          (await client.query('SELECT to_regclass($1) AS name', [`moria.${table}`])).rows[0].name,
        );
      const bounded = <T>(rows: T[], limit: number, name: string) => {
        if (rows.length > limit) graph.limitations.push(`${name} truncated at ${limit} records.`);
        return rows.slice(0, limit);
      };
      const snapshot = (r: {
        environment: string;
        dataset: string;
        revision: string;
        snapshot_id: string;
      }) => {
        const dataset = node(id('dataset', r.environment, r.dataset), r.dataset, 'dataset', null, {
          environment: r.environment,
        });
        const key = node(
          id('snapshot', r.environment, r.dataset, r.revision),
          `${r.dataset} · revision ${r.revision}`,
          'snapshot',
          'published',
          {
            environment: r.environment,
            dataset: r.dataset,
            revision: r.revision,
            snapshotId: r.snapshot_id,
          },
        );
        edge(dataset, key, 'version');
        return key;
      };
      const publicationKeys = new Map<string, string[]>();
      const snapshots = bounded(
        (
          await client.query(`SELECT s.environment,s.dataset,s.revision,s.snapshot_id,s.execution,
        p.receipt->>'revision' AS publication_revision
        FROM moria.snapshot_history s LEFT JOIN moria.publications p
        ON p.environment=s.environment AND p.execution=s.execution
        ORDER BY s.revision DESC,s.environment,s.dataset LIMIT 51`)
        ).rows,
        50,
        'Snapshots',
      );
      for (const r of snapshots) {
        const target = snapshot(r);
        if (r.publication_revision !== null) {
          const key = node(
            id('publication', r.environment, r.execution),
            `Publication ${r.publication_revision}`,
            'publication',
            'committed',
            {
              environment: r.environment,
              execution: r.execution,
              revision: r.publication_revision,
            },
          );
          edge(key, target, 'published');
          const keys = publicationKeys.get(r.execution) ?? [];
          if (!keys.includes(key)) keys.push(key);
          publicationKeys.set(r.execution, keys);
        }
      }
      if (await exists('index_provenance')) {
        const indexes = bounded(
          (
            await client.query(`SELECT p.environment,p.source_dataset AS dataset,p.source_revision AS revision,
          p.snapshot_id,p.contract,p.application,p.stage,s.object_type,s.status,s.published_revision,s.created_at
          FROM moria.index_provenance p JOIN moria.source_stages s ON s.id=p.stage
          ORDER BY s.created_at DESC,s.id LIMIT 51`)
          ).rows,
          50,
          'Index stages',
        );
        for (const r of indexes) {
          const source = snapshot(r);
          const published = r.status === 'published' && r.published_revision !== null;
          const kind = published ? 'object-generation' : 'index-stage';
          const key = node(
            published
              ? id(kind, r.environment, r.object_type, r.published_revision)
              : id(kind, r.stage),
            published
              ? `${r.object_type} · generation ${r.published_revision}`
              : `${r.object_type} · staged`,
            kind,
            r.status,
            {
              environment: r.environment,
              stage: r.stage,
              revision: r.published_revision,
              contract: r.contract,
              application: r.application,
              createdAt: r.created_at,
            },
          );
          edge(source, key, published ? 'indexed' : 'staged from');
          const type = node(
            id('object-type', r.environment, r.object_type),
            r.object_type,
            'object-type',
            null,
            { environment: r.environment },
          );
          edge(type, key, published ? 'version' : 'stage');
        }
      } else graph.limitations.push('Index provenance has not been initialized.');
      if (await exists('source_generations')) {
        const generations = bounded(
          (
            await client.query(`SELECT environment,object_type,revision,stage
          FROM moria.source_generations ORDER BY revision DESC,environment,object_type LIMIT 51`)
          ).rows,
          50,
          'Object generations',
        );
        let missing = 0;
        for (const r of generations) {
          const key = id('object-generation', r.environment, r.object_type, r.revision);
          if (nodes.has(key)) continue;
          missing++;
          node(
            key,
            `${r.object_type} · generation ${r.revision}`,
            'object-generation',
            'published',
            {
              environment: r.environment,
              revision: r.revision,
              stage: r.stage,
              provenance: 'Source snapshot relationship unavailable in this bounded view.',
            },
          );
          const type = node(
            id('object-type', r.environment, r.object_type),
            r.object_type,
            'object-type',
            null,
            { environment: r.environment },
          );
          edge(type, key, 'version');
        }
        if (missing)
          graph.limitations.push(
            `${missing} displayed object generations have no source snapshot edge in this view; provenance may be missing or outside the selected window.`,
          );
      }
      const jobKeys = new Map<string, string>();
      if (await exists('jobs')) {
        const jobs = bounded(
          (
            await client.query(`SELECT id,kind,status,phase,attempt,created_at,updated_at
          FROM moria.jobs ORDER BY created_at DESC,id LIMIT 51`)
          ).rows,
          50,
          'Jobs',
        );
        for (const r of jobs) {
          const key = node(id('job', r.id), `${r.kind} · ${r.id}`, 'job', r.status, {
            jobId: r.id,
            kind: r.kind,
            phase: r.phase,
            attempt: r.attempt,
            createdAt: r.created_at,
            updatedAt: r.updated_at,
          });
          jobKeys.set(r.id, key);
          for (const publication of publicationKeys.get(r.id) ?? [])
            edge(key, publication, 'published');
        }
        if (await exists('job_attempts')) {
          const attempts = bounded(
            (
              await client.query(
                `SELECT job_id,attempt,status,started_at,finished_at
            FROM moria.job_attempts WHERE job_id=ANY($1::uuid[])
            ORDER BY started_at DESC,job_id,attempt LIMIT 201`,
                [[...jobKeys.keys()]],
              )
            ).rows,
            200,
            'Attempts',
          );
          for (const r of attempts) {
            const key = node(
              id('attempt', r.job_id, r.attempt),
              `Attempt ${r.attempt}`,
              'attempt',
              r.status,
              {
                jobId: r.job_id,
                attempt: r.attempt,
                startedAt: r.started_at,
                finishedAt: r.finished_at,
              },
            );
            edge(jobKeys.get(r.job_id)!, key, 'attempt');
          }
        }
        graph.limitations.push(
          'Publication records identify the execution, not its successful attempt; no attempt-to-publication edge is asserted.',
        );
      } else graph.limitations.push('Durable job metadata has not been initialized.');
      if (await exists('snow_syncs')) {
        const syncs = bounded(
          (
            await client.query(`SELECT id,status,started_at,completed_at,station_count,observation_count
          FROM moria.snow_syncs ORDER BY started_at DESC,id LIMIT 51`)
          ).rows,
          50,
          'Snow syncs',
        );
        for (const r of syncs) {
          const key = node(id('snow-sync', r.id), `Snow refresh ${r.id}`, 'snow-sync', r.status, {
            syncId: r.id,
            startedAt: r.started_at,
            completedAt: r.completed_at,
            stationCount: r.station_count,
            observationCount: r.observation_count,
          });
          if (jobKeys.has(r.id)) edge(jobKeys.get(r.id)!, key, 'runs');
          for (const publication of publicationKeys.get(r.id) ?? [])
            edge(key, publication, 'published');
        }
      }
      if (await exists('weather_publications')) {
        const weather = bounded(
          (
            await client.query(`SELECT job_id,published_at,
          (SELECT jsonb_agg(jsonb_build_object('url',v->>'url','sha256',v->>'sha256','retrievedAt',v->>'retrievedAt'))
            FROM (SELECT v FROM jsonb_array_elements(CASE WHEN jsonb_typeof(p.raw)='array' THEN p.raw ELSE '[]'::jsonb END) v LIMIT 11) sources) AS sources,
          (SELECT jsonb_agg(jsonb_build_object('id',v->>'id','forecastUrl',v->>'forecastUrl','sourceUpdatedAt',v->>'sourceUpdatedAt','retrievedAt',v->>'retrievedAt'))
            FROM (SELECT v FROM jsonb_array_elements(CASE WHEN jsonb_typeof(p.view->'areas')='array' THEN p.view->'areas' ELSE '[]'::jsonb END) v LIMIT 11) areas) AS areas
          FROM moria.weather_publications p ORDER BY published_at DESC,job_id LIMIT 51`)
          ).rows,
          50,
          'Weather publications',
        );
        const safeUrl = (value: unknown): string | null => {
          if (typeof value !== 'string') return null;
          try {
            return officialUrl(value);
          } catch {
            return null;
          }
        };
        const safeTime = (value: unknown) =>
          typeof value === 'string' && Number.isFinite(Date.parse(value))
            ? new Date(value).toISOString()
            : null;
        let omittedSources = 0;
        for (const r of weather) {
          const publication = node(
            id('weather-publication', r.job_id),
            `Weather publication ${r.job_id}`,
            'weather-publication',
            'committed',
            { jobId: r.job_id, publishedAt: r.published_at, storage: 'PostgreSQL' },
          );
          if (jobKeys.has(r.job_id)) edge(jobKeys.get(r.job_id)!, publication, 'published');
          for (const source of bounded<any>(r.sources ?? [], 10, 'Weather source responses')) {
            const url = safeUrl(source.url);
            if (
              !url ||
              typeof source.sha256 !== 'string' ||
              !/^[a-f0-9]{64}$/i.test(source.sha256)
            ) {
              omittedSources++;
              continue;
            }
            const sourceId = node(
              id('weather-source', r.job_id, url, source.sha256),
              new URL(url).pathname,
              'weather-source',
              'captured',
              { url, sha256: source.sha256, retrievedAt: safeTime(source.retrievedAt) },
            );
            edge(sourceId, publication, 'captured by');
          }
          for (const area of bounded<any>(r.areas ?? [], 10, 'Weather forecast areas')) {
            const configured = weatherAreas.find((a) => a.id === area.id);
            if (!configured) continue;
            const forecast = node(
              id('area-forecast', r.job_id, configured.id),
              configured.name,
              'area-forecast',
              'published',
              {
                areaId: configured.id,
                publication: r.job_id,
                forecastUrl: safeUrl(area.forecastUrl),
                sourceUpdatedAt: safeTime(area.sourceUpdatedAt),
                retrievedAt: safeTime(area.retrievedAt),
              },
            );
            edge(publication, forecast, 'contains');
          }
        }
        graph.limitations.push(
          'Weather provenance includes up to 50 publications, 10 official source receipts and 10 configured areas per publication. Raw response bodies and forecast text are omitted.',
        );
        if (omittedSources)
          graph.limitations.push(
            `${omittedSources} weather source receipts with unsupported URLs or hashes were omitted.`,
          );
      }
      return graph;
    },
    undefined,
    'repeatable-read',
  );
}
