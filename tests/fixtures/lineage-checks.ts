import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import type { Catalog } from '../../services/catalog/src/catalog.js';
import { readLineage } from '../../services/lineage/src/service.js';
import { ObjectStore } from '../../services/ontology/src/store.js';
import { IcebergIndexer } from '../../services/ontology/src/indexer.js';
import { Authority } from '../../services/security/src/authority.js';
import { WorkloadSessions } from '../../services/security/src/sessions.js';
import { Jobs } from '../../services/execution/src/jobs.js';
import { createWeatherService } from '../../services/weather/src/service.js';

/** Real PostgreSQL metadata fixture. It does not claim to run an Iceberg build. */
export async function checkLineage(catalog: Catalog) {
  const environment = `lineage-${randomUUID()}`;
  const jobId = randomUUID();
  const stage = randomUUID();
  const privateValue = `private-path-or-secret-${randomUUID()}`;
  const store = new ObjectStore(catalog);
  await store.migrate();
  await new Jobs(catalog, {}).migrate();
  await new IcebergIndexer(store, new Authority(catalog), {
    source: { kind: 'dataset', resource: 'source', markings: [] },
    target: {
      resource: 'objects',
      markings: [],
      schema: { primaryKey: 'key', properties: { key: { type: 'string', nullable: false } } },
    },
  }).migrate();
  await catalog.ensureEnvironment(environment);
  await catalog.ensureDataset(environment, 'source');
  const authority = new Authority(catalog),
    operations = ['source:read', 'objects:index'];
  await authority.syncPrincipal(environment, 'fixture', 'worker', {
    kind: 'workload',
    enabled: true,
    attributes: {},
    markings: [],
  });
  await authority.setApplication(environment, 'fixture', 'indexer', operations);
  await authority.setGrants(environment, 'fixture', 'worker', 'indexer', operations);
  const sessions = new WorkloadSessions(authority),
    credential = await sessions.mint(
      { environment, principal: 'worker', application: 'indexer' },
      'fixture',
    ),
    context = await sessions.authenticate(`Bearer ${credential.token}`);
  await catalog.transaction(
    (client) =>
      catalog.admitDataProvenance(client, context.sessionHash!, ['objects'], ['source'], 'index'),
    environment,
  );
  // Use a high revision so the bounded graph includes the fixture even if the
  // shared integration database already contains many lower-revision resources.
  const revision = '900000000';
  await catalog.pool.query(
    `INSERT INTO moria.snapshot_history(environment,dataset,revision,metadata_location,snapshot_id,execution,lineage)
     VALUES($1,'source',$2,$3,'snapshot-123',$4,$5)`,
    [environment, revision, privateValue, jobId, JSON.stringify({ source: privateValue })],
  );
  await catalog.pool.query(
    `INSERT INTO moria.publications(environment,execution,request_hash,receipt) VALUES($1,$2,'hash',$3)`,
    [environment, jobId, JSON.stringify({ revision, secret: privateValue })],
  );
  await catalog.pool.query(
    `INSERT INTO moria.source_stages(id,environment,object_type,expected_revision,security_epoch,status,published_revision)
     VALUES($1,$2,'objects',0,0,'published',$3)`,
    [stage, environment, revision],
  );
  const publication = await catalog.pool.connect();
  try {
    await publication.query('BEGIN');
    await publication.query('UPDATE moria.environments SET revision=$2 WHERE id=$1', [
      environment,
      revision,
    ]);
    await publication.query(
      `INSERT INTO moria.source_generations(environment,object_type,revision,stage)
       VALUES($1,'objects',$2,$3),($1,'unlinked',$2,NULL)`,
      [environment, revision, stage],
    );
    await publication.query('COMMIT');
  } catch (error) {
    await publication.query('ROLLBACK');
    throw error;
  } finally {
    publication.release();
  }
  await catalog.pool.query(
    `INSERT INTO moria.index_provenance(stage,environment,principal,application,contract,source_dataset,source_revision,metadata_location,snapshot_id)
     VALUES($1,$2,'worker','indexer','contract-fingerprint','source',$3,$4,'snapshot-123')`,
    [stage, environment, revision, privateValue],
  );
  await catalog.pool.query(
    `INSERT INTO moria.jobs(id,kind,input,request_key,requested_by,status,result,error)
     VALUES($1,'fixture',$2,$4,'operator','succeeded',$2,$3)`,
    [jobId, JSON.stringify({ secret: privateValue }), privateValue, `lineage:${jobId}`],
  );
  await catalog.pool.query(
    `INSERT INTO moria.job_attempts(job_id,attempt,status,error) VALUES($1,1,'interrupted',$2),($1,2,'succeeded',NULL)`,
    [jobId, privateValue],
  );
  await createWeatherService(catalog);
  const forecastUrl = 'https://api.weather.gov/gridpoints/BOU/50,70/forecast';
  const hash = 'a'.repeat(64);
  await catalog.pool.query(
    'INSERT INTO moria.weather_publications(job_id,view,raw) VALUES($1,$2,$3)',
    [
      jobId,
      JSON.stringify({
        areas: [
          {
            id: 'berthoud',
            name: privateValue,
            forecastUrl,
            periods: [{ detailedForecast: privateValue }],
          },
        ],
      }),
      JSON.stringify([
        { url: forecastUrl, sha256: hash, body: privateValue, retrievedAt: '2026-10-21T00:00:00Z' },
        { url: `https://evil.test/${privateValue}`, sha256: hash, body: privateValue },
        {
          url: `https://api.weather.gov/gridpoints/BOU/50,70/forecast?token=${privateValue}`,
          sha256: hash,
        },
      ]),
    ],
  );
  const graph = await readLineage(catalog);
  const key = (kind: string, ...parts: unknown[]) => JSON.stringify([kind, ...parts]);
  const hasEdge = (from: string, to: string, label: string) =>
    graph.edges.some((e) => e.from === from && e.to === to && e.label === label);
  assert(hasEdge(key('job', jobId), key('publication', environment, jobId), 'published'));
  assert(
    hasEdge(
      key('publication', environment, jobId),
      key('snapshot', environment, 'source', revision),
      'published',
    ),
  );
  assert(
    hasEdge(
      key('snapshot', environment, 'source', revision),
      key('object-generation', environment, 'objects', revision),
      'indexed',
    ),
  );
  assert(hasEdge(key('job', jobId), key('attempt', jobId, 1), 'attempt'));
  assert(hasEdge(key('job', jobId), key('attempt', jobId, 2), 'attempt'));
  assert(!graph.edges.some((e) => e.from === key('attempt', jobId, 2) && e.label === 'published'));
  assert(!JSON.stringify(graph).includes(privateValue));
  assert(hasEdge(key('job', jobId), key('weather-publication', jobId), 'published'));
  assert(
    hasEdge(
      key('weather-source', jobId, forecastUrl, hash),
      key('weather-publication', jobId),
      'captured by',
    ),
  );
  assert(
    hasEdge(key('weather-publication', jobId), key('area-forecast', jobId, 'berthoud'), 'contains'),
  );
  assert(graph.limitations.some((l) => l.includes('2 weather source receipts')));
  assert(
    graph.nodes.some((n) => n.id === key('object-generation', environment, 'unlinked', revision)),
  );
  assert(
    !graph.edges.some(
      (e) =>
        e.to === key('object-generation', environment, 'unlinked', revision) &&
        e.label === 'indexed',
    ),
  );
  assert(graph.limitations.some((value) => value.includes('not the complete instance graph')));
  const ids = new Set(graph.nodes.map((n) => n.id));
  assert.equal(ids.size, graph.nodes.length);
  assert(graph.edges.every((e) => ids.has(e.from) && ids.has(e.to)));
}
