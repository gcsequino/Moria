import { randomUUID } from 'node:crypto';
import type { PoolClient } from 'pg';
import { Catalog } from '../../catalog/src/catalog.js';
import { Authority } from '../../security/src/authority.js';
import { WorkloadSessions } from '../../security/src/sessions.js';
import { ObjectStore, type ObjectSchema } from '../../ontology/src/store.js';
import { ObjectService, type SessionContext } from '../../ontology/src/service.js';
import { type ValueType, MoriaError } from '../../../packages/contracts/types.js';
import { authorize } from '../../../packages/authorization/src/access.js';
import { iceberg } from '../../../workers/iceberg/stage.js';
import { icebergRows } from '../../../workers/iceberg/stream.js';
import type { JobContext } from '../../execution/src/jobs.js';
import { fingerprint } from '../../../packages/compiler/src/index.js';
import { downloadSnow, type JsonLoader } from './connector.js';

export const snowEnvironment = 'snotel';
export const snowViewer = {
  environment: snowEnvironment,
  principal: 'snow-reader',
  application: 'snow-browser',
};
const ingest = {
  environment: snowEnvironment,
  principal: 'snow-connector',
  application: 'snow-ingestion',
};
import { stationSchema, observationSchema } from '../../../packages/contracts/bundled-objects.js';
export { stationSchema, observationSchema };
const str: ValueType = { type: 'string', nullable: false },
  uid: ValueType = { type: 'uid', nullable: false };
const rawSchema: ObjectSchema = {
  primaryKey: 'uid',
  properties: { uid, url: str, sha256: str, body: str, retrievedAt: str },
};
function icebergSchema(schema: ObjectSchema) {
  return {
    type: 'struct',
    'schema-id': 0,
    fields: Object.entries(schema.properties).map(([name, type], i) => ({
      id: i + 1,
      name,
      required: !type.nullable,
      type: type.type === 'float64' ? 'double' : 'string',
    })),
  };
}
export async function createSnowService(catalog: Catalog, warehouse: string) {
  await catalog.migrate();
  await catalog.ensureEnvironment(snowEnvironment);
  const authority = new Authority(catalog),
    store = new ObjectStore(catalog);
  await authority.migrate();
  await store.migrate();
  const datasets = ['snow-stations-source', 'snow-observations-source', 'snow-raw-source'];
  for (const dataset of datasets) await catalog.ensureDataset(snowEnvironment, dataset);
  const reads = ['snow-stations:read', 'snow-observations:read'];
  const writer = [
    ...reads,
    'snow-stations:index',
    'snow-observations:index',
    ...datasets.flatMap((d) => [`${d}:read`, `${d}:write`]),
  ];
  for (const [context, operations] of [
    [snowViewer, reads],
    [ingest, writer],
  ] as const) {
    // Bootstrap only missing identities. Restarts must not undo revocations.
    const present = await catalog.pool.query(
      'SELECT 1 FROM moria.permissions WHERE environment=$1 AND principal=$2 AND application=$3',
      [snowEnvironment, context.principal, context.application],
    );
    if (present.rowCount) continue;
    await authority.setApplication(snowEnvironment, 'snow-bootstrap', context.application, [
      ...operations,
    ]);
    await authority.syncPrincipal(snowEnvironment, 'snow-bootstrap', context.principal, {
      enabled: true,
      kind: 'workload',
      attributes: {},
      markings: [],
    });
    await authority.setGrants(
      snowEnvironment,
      'snow-bootstrap',
      context.principal,
      context.application,
      [...operations],
    );
  }
  const service = (resource: string, schema: ObjectSchema) =>
    new ObjectService(store, authority, {
      schema,
      policies: {
        resource,
        markings: [],
        properties: schema.properties,
        sourceColumns: schema.properties,
        attributes: {},
      },
    });
  const stations = service('snow-stations', stationSchema),
    observations = service('snow-observations', observationSchema),
    sessions = new WorkloadSessions(authority);
  let session: { token: string; expiresAt: string } | undefined;
  async function token() {
    if (!session || Date.parse(session.expiresAt) < Date.now() + 30000)
      session = await sessions.mint(snowViewer, 'snow-browser');
    return session.token;
  }
  async function status(
    context: SessionContext = snowViewer,
    extra?: (c: PoolClient) => Promise<Record<string, unknown>>,
  ) {
    const state = await authority.current(
      context.environment,
      context.principal,
      context.application,
    );
    for (const permission of ['snow-stations', 'snow-observations'])
      authorize(state.identity, permission, 'read');
    return catalog.transaction(async (c) => {
      await authority.assertCurrent(
        c,
        context.environment,
        context.principal,
        context.application,
        state.epoch,
        context.sessionHash,
      );
      const completed =
        (
          await c.query(
            "SELECT id,config,completed_at,station_count,observation_count FROM moria.snow_syncs WHERE status='completed' ORDER BY completed_at DESC LIMIT 1",
          )
        ).rows[0] ?? null;
      const latest =
        (
          await c.query(
            'SELECT id,status,started_at,completed_at,error FROM moria.snow_syncs ORDER BY started_at DESC LIMIT 1',
          )
        ).rows[0] ?? null;
      return { completed, latest, ...(await extra?.(c)) };
    }, context.environment);
  }
  async function sync(
    input: unknown,
    load?: JsonLoader,
    checkInitiator?: (client?: PoolClient) => Promise<void>,
    job?: JobContext,
  ) {
    await checkInitiator?.();
    // PostgreSQL advisory lock spans fetching/staging; readers use separate connections.
    const lock = await catalog.pool.connect();
    let locked = false,
      lockLost = false;
    const lockError = () => {
      lockLost = true;
    };
    lock.on('error', lockError);
    const suppliedCheck = checkInitiator;
    checkInitiator = async (c) => {
      if (lockLost) throw new MoriaError('LEASE_LOST', 'The ingestion lock connection was lost.');
      await suppliedCheck?.(c);
    };
    const run = job?.id ?? randomUUID();
    try {
      locked = (
        await lock.query(
          "SELECT pg_try_advisory_lock(hashtextextended('moria/snotel/sync',0)) AS acquired",
        )
      ).rows[0].acquired;
      if (!locked) throw new MoriaError('RESOURCE_BUSY', 'A SNOTEL sync is already running.');
      const { configInput } = await import('./connector.js');
      const previous = (
        await catalog.pool.query('SELECT * FROM moria.snow_syncs WHERE id=$1', [run])
      ).rows[0];
      const config = configInput(previous?.config ?? input);
      if (previous?.status === 'completed') {
        await checkInitiator?.();
        if (job) await catalog.transaction((c) => job.complete(c, previous.result));
        return previous.result;
      }
      await catalog.pool.query(
        "UPDATE moria.snow_syncs SET status='interrupted',error='Previous sync process stopped before completion.' WHERE status='running' AND id<>$1",
        [run],
      );
      await catalog.pool.query(
        "INSERT INTO moria.snow_syncs(id,status,config) VALUES($1,'running',$2) ON CONFLICT(id) DO UPDATE SET status='running',error=NULL",
        [run, JSON.stringify(config)],
      );
      type Plan = {
        pins: {
          dataset: string;
          expectedRevision: string;
          metadataLocation: string;
          snapshotId: string;
        }[];
        stations: number;
        observations: number;
        retrievedAt: string;
      };
      let plan: Plan | undefined = previous?.plan;
      await checkInitiator?.();
      const state = await authority.current(snowEnvironment, ingest.principal, ingest.application);
      for (const d of datasets) authorize(state.identity, d, 'write');
      for (const type of ['snow-stations', 'snow-observations'])
        authorize(state.identity, type, 'index');
      if (!plan) {
        await job?.phase('Downloading official NRCS observations');
        const downloaded = await downloadSnow(config, load);
        await checkInitiator?.();
        const sets = [
          {
            dataset: datasets[0],
            schema: stationSchema,
            rows: downloaded.stations.map((row) => ({ ...row })),
          },
          {
            dataset: datasets[1],
            schema: observationSchema,
            rows: downloaded.observations.map((row) => ({ ...row })),
          },
          {
            dataset: datasets[2],
            schema: rawSchema,
            rows: downloaded.raw.map((row) => ({ ...row, retrievedAt: downloaded.retrievedAt })),
          },
        ];
        const heads = await catalog.heads(snowEnvironment, datasets),
          pins = [];
        for (const [i, set] of sets.entries()) {
          const rows = [];
          for (const row of set.rows) {
            const data = row as Record<string, unknown>;
            const canonical =
              i === 0
                ? String(data.triplet)
                : i === 1
                  ? `${data.triplet}/${data.date}`
                  : String(data.url);
            rows.push({ ...row, uid: await catalog.resolveIdentity(set.dataset, canonical) });
          }
          const head = heads.find((h) => h.dataset === set.dataset)!;
          const pin = await iceberg({
            operation: 'stage',
            warehouse,
            schema: icebergSchema(set.schema),
            rows,
            ...(head.metadataLocation ? { baseMetadataLocation: head.metadataLocation } : {}),
          });
          pins.push({
            dataset: set.dataset,
            expectedRevision: head.revision,
            metadataLocation: pin.metadataLocation as string,
            snapshotId: pin.snapshotId as string,
          });
        }
        plan = {
          pins,
          stations: downloaded.stations.length,
          observations: downloaded.observations.length,
          retrievedAt: downloaded.retrievedAt,
        };
        await checkInitiator?.();
        await catalog.pool.query('UPDATE moria.snow_syncs SET plan=$2 WHERE id=$1', [
          run,
          JSON.stringify(plan),
        ]);
      }
      const pins = plan.pins;
      // The SNOTEL session advisory lock excludes every other sync. On recovery,
      // expire only this execution's former catalog lease before assigning a new fence.
      await checkInitiator?.();
      await catalog.pool.query(
        'UPDATE moria.leases SET expires_at=clock_timestamp() WHERE environment=$1 AND execution=$2',
        [snowEnvironment, run],
      );
      await job?.phase('Publishing dataset snapshots');
      const execution = run,
        worker = 'snow-ingestion',
        fence = await catalog.claim(snowEnvironment, execution, worker, 600);
      await checkInitiator?.();
      const publication = await catalog.publish(
        {
          environment: snowEnvironment,
          execution,
          worker,
          fence,
          securityEpoch: state.epoch,
          outputs: pins,
          lineage: {
            source: config.source === 'report-csv' ? 'NRCS Report Generator CSV' : 'NRCS AWDB REST',
            request: run,
          },
        },
        checkInitiator,
      );
      const revision = (
        await catalog.pool.query('SELECT revision FROM moria.environments WHERE id=$1', [
          snowEnvironment,
        ])
      ).rows[0].revision;
      const currentHeads = await catalog.heads(snowEnvironment, datasets);
      if (
        pins.some(
          (p) => currentHeads.find((h) => h.dataset === p.dataset)?.snapshotId !== p.snapshotId,
        )
      )
        throw new MoriaError(
          'HEAD_CONFLICT',
          'A newer dataset replaced this execution. Start a new sync.',
        );
      const stages = [];
      await job?.phase('Indexing authorized snow objects');
      for (const [i, type] of ['snow-stations', 'snow-observations'].entries()) {
        const schema = i === 0 ? stationSchema : observationSchema;
        const stage = await store.stage(
          snowEnvironment,
          type,
          schema,
          icebergRows(pins[i]),
          state.epoch,
          revision,
          { execution, worker, fence },
          [],
        );
        stages.push(stage);
        const sourceRevision = publication.outputs.find(
          (o) => o.dataset === pins[i].dataset,
        )!.revision;
        await catalog.pool.query(
          'INSERT INTO moria.index_provenance VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)',
          [
            stage,
            snowEnvironment,
            ingest.principal,
            ingest.application,
            fingerprint({ type, schema }),
            pins[i].dataset,
            sourceRevision,
            pins[i].metadataLocation,
            pins[i].snapshotId,
          ],
        );
      }
      const result = {
        id: run,
        stations: plan.stations,
        observations: plan.observations,
        config,
        retrievedAt: plan.retrievedAt,
        snapshots: publication.outputs.map((o) => ({
          dataset: o.dataset,
          revision: o.revision,
          snapshotId: pins.find((p) => p.dataset === o.dataset)!.snapshotId,
        })),
        stages,
      };
      await checkInitiator?.();
      await store.publishStages(
        stages,
        async (c) => {
          await checkInitiator?.(c);
          await authority.assertCurrent(
            c,
            snowEnvironment,
            ingest.principal,
            ingest.application,
            state.epoch,
          );
        },
        async (c) => {
          await c.query(
            "UPDATE moria.snow_syncs SET status='completed',completed_at=clock_timestamp(),station_count=$2,observation_count=$3,result=$4 WHERE id=$1",
            [run, plan.stations, plan.observations, JSON.stringify(result)],
          );
          await job?.complete(c, result);
        },
      );
      return result;
    } catch (error) {
      if (locked && !lockLost) {
        await catalog
          .transaction(async (c) => {
            await job?.guard(c);
            await c.query(
              "UPDATE moria.snow_syncs SET status='failed',completed_at=clock_timestamp(),error=$2 WHERE id=$1 AND status<>'completed'",
              [
                run,
                error instanceof MoriaError
                  ? error.message
                  : 'Sync failed; prior published objects remain available.',
              ],
            );
          })
          .catch(() => {}); // A replaced worker must not overwrite its successor's status.
      }
      throw error;
    } finally {
      if (locked && !lockLost)
        await lock.query("SELECT pg_advisory_unlock(hashtextextended('moria/snotel/sync',0))");
      lock.removeListener('error', lockError);
      lock.release(lockLost ? new Error('Ingestion lock lost') : undefined);
    }
  }
  async function ingestionGuard(c?: PoolClient) {
    if (c)
      await c.query('SELECT 1 FROM moria.environments WHERE id=$1 FOR SHARE', [snowEnvironment]);
    const state = await authority.current(snowEnvironment, ingest.principal, ingest.application);
    for (const dataset of datasets) authorize(state.identity, dataset, 'write');
    if (c)
      await authority.assertCurrent(
        c,
        snowEnvironment,
        ingest.principal,
        ingest.application,
        state.epoch,
      );
  }
  return { stations, observations, sessions, token, status, sync, authority, ingestionGuard };
}
export type SnowService = Awaited<ReturnType<typeof createSnowService>>;
