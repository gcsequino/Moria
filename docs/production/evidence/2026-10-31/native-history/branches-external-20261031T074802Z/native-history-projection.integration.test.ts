import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { Authority } from '../services/security/src/authority.js';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { nativeProjectionCapacity } from '../benchmarks/native-projection.js';

test(
  'history collection prunes actual native projection intervals without changing protected reads',
  { timeout: 240000 },
  async (t) => {
    const output = await mkdtemp(join(tmpdir(), 'moria-history-projection-'));
    await nativeProjectionCapacity({
      rows: 64,
      rowBytes: 4096,
      output,
      onVerified: async (fixture) => {
        const { db, environment, target, schema, token, runtime, nativeEnv } = fixture;
        await db.pool.query(
          'INSERT INTO moria.native_serving_bindings(environment,resource,object_type_id,revision,definition) VALUES($1,$2,$3,$4,$5)',
          [
            environment,
            target,
            target,
            'history-v1',
            {
              objectTypeId: target,
              schema,
              policies: {
                resource: target,
                markings: ['private', 'enterprise'],
                properties: schema.properties,
                sourceColumns: schema.properties,
                attributes: {},
                row: { literal: true },
              },
            },
          ],
        );
        await db.pool.query(
          'INSERT INTO moria.native_serving_actions(environment,resource,object_resource,revision,definition) VALUES($1,$2,$3,$4,$5)',
          [
            environment,
            'edit',
            target,
            'history-v1',
            {
              resource: 'edit',
              revision: 'history-v1',
              markings: ['private', 'enterprise'],
              targetParameter: 'uid',
              inputs: { uid: schema.properties.uid, name: schema.properties.name },
              condition: { literal: true },
              edits: [{ op: 'set', property: 'name', value: { parameter: 'name' } }],
            },
          ],
        );
        const projection = (
          await db.pool.query(
            "SELECT id,stage FROM moria.runtime_projections WHERE environment=$1 AND resource=$2 AND status='active'",
            [environment, target],
          )
        ).rows[0];
        const uid = (
          await db.pool.query(
            'SELECT uid FROM moria.staged_source_rows WHERE stage=$1 ORDER BY uid LIMIT 1',
            [projection.stage],
          )
        ).rows[0].uid;
        const relation = 'moria.runtime_projection_' + String(projection.id).replaceAll('-', '');
        const child = spawn(runtime, ['serve'], {
          env: { ...nativeEnv, MORIA_NATIVE_LISTEN: '127.0.0.1:0' },
          stdio: ['ignore', 'pipe', 'pipe'],
        });
        const exited = new Promise<void>((resolve) => child.once('exit', () => resolve()));
        let out = '',
          errors = '';
        child.stdout.on('data', (part) => {
          out = (out + part).slice(-65536);
        });
        child.stderr.on('data', (part) => {
          errors = (errors + part).slice(-65536);
        });
        t.after(async () => {
          child.kill('SIGKILL');
          await exited;
        });
        try {
          let base = '';
          for (let i = 0; i < 500 && !base; i++) {
            for (const line of out.split('\n')) {
              try {
                const value = JSON.parse(line);
                if (value.event === 'serving') base = 'http://' + value.address;
              } catch {}
            }
            if (child.exitCode !== null) throw new Error(errors);
            if (!base) await delay(10);
          }
          assert.ok(base, 'native HTTP listener');
          const headers = { authorization: 'Bearer ' + token, 'content-type': 'application/json' };
          for (let i = 0; i < 4; i++) {
            const response = await fetch(base + '/api/v1/actions/edit/invoke', {
              method: 'POST',
              headers: { ...headers, 'idempotency-key': randomUUID() },
              body: JSON.stringify({ input: { uid, name: 'retained-' + i } }),
            });
            assert.equal(response.status, 200, await response.text());
          }
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*) FROM ' + relation + ' WHERE to_revision IS NOT NULL',
              )
            ).rows[0].count,
            '4',
          );
          const read = async () => {
            const response = await fetch(base + '/api/v1/objects/' + target + '/' + uid, {
              headers,
            });
            const value = await response.json();
            assert.equal(response.status, 200, JSON.stringify(value));
            return value;
          };
          const before = await read();
          assert.equal(before.object.name, 'retained-3');
          const policy = (
            await db.pool.query('SELECT moria.runtime_history_approve($1,$2,86400,2) AS id', [
              environment,
              'projection-history-' + environment,
            ])
          ).rows[0].id;
          // Synthetic historical timestamps exercise deletion; the runtime and mutations above are actual.
          await db.pool.query(
            "UPDATE moria.runtime_history_revisions SET observed_at=clock_timestamp()-interval '3 days' WHERE environment=$1",
            [environment],
          );
          const collection = (
            await db.pool.query('SELECT moria.runtime_history_plan($1) AS id', [policy])
          ).rows[0].id;
          await db.pool.query(
            "UPDATE moria.runtime_history_collections SET observed_at=clock_timestamp()-interval '3 days',not_before=clock_timestamp()-interval '1 day' WHERE id=$1",
            [collection],
          );
          let result: any;
          for (let i = 0; i < 30; i++) {
            result = (
              await db.pool.query('SELECT moria.runtime_history_step($1) AS value', [policy])
            ).rows[0].value;
            if (result.state === 'complete') break;
          }
          assert.equal(result.state, 'complete', JSON.stringify(result));
          assert.equal(
            (
              await db.pool.query(
                'SELECT count(*) FROM ' + relation + ' WHERE to_revision IS NOT NULL',
              )
            ).rows[0].count,
            '0',
          );
          assert.equal(
            (await db.pool.query('SELECT count(*) FROM ' + relation + ' WHERE to_revision IS NULL'))
              .rows[0].count,
            '64',
          );
          assert.deepEqual(await read(), before);
          assert.equal(
            (
              await db.pool.query('SELECT count(*) FROM moria.staged_source_rows WHERE stage=$1', [
                projection.stage,
              ])
            ).rows[0].count,
            '64',
          );
          const index = (
            await db.pool.query(
              "SELECT indexdef FROM pg_indexes WHERE schemaname='moria' AND tablename=$1 AND indexname LIKE '%_history'",
              [relation.split('.')[1]],
            )
          ).rows;
          assert.equal(index.length, 1);
          assert.match(index[0].indexdef, /to_revision, uid, from_revision/);

          await t.test(
            'retired native tables obey current, latest-two, floor and structural Search pins, including backup admission',
            async () => {
              const authority = new Authority(db.catalog),
                state = await authority.current(environment, 'pipeline', 'capacity');
              const operations = [
                ...state.identity.grantedOperations,
                `${target}:write`,
                'history-link:read',
                'history-link:write',
                'history-link-on:invoke',
                'history-link-off:invoke',
              ];
              const collection = randomUUID(),
                model = randomUUID(),
                service = randomUUID();
              operations.push(`${collection}:project`, `${model}:invoke`);
              await authority.setApplication(environment, 'fixture', 'capacity', operations);
              await authority.setGrants(environment, 'fixture', 'pipeline', 'capacity', operations);
              await db.pool.query(
                'INSERT INTO moria.native_serving_links(environment,resource,revision,source_resource,target_resource,definition) VALUES($1,$2,$3,$4,$4,$5)',
                [
                  environment,
                  'history-link',
                  'history-link-v1',
                  target,
                  {
                    mode: 'actionManaged',
                    cardinality: 'manyToMany',
                    markings: ['private', 'enterprise'],
                  },
                ],
              );
              for (const [operation, resource] of [
                ['link', 'history-link-on'],
                ['unlink', 'history-link-off'],
              ]) {
                await db.pool.query(
                  'INSERT INTO moria.native_serving_actions(environment,resource,object_resource,revision,definition) VALUES($1,$2,$3,$4,$5)',
                  [
                    environment,
                    resource,
                    target,
                    'history-link-action-v1',
                    {
                      resource,
                      revision: 'history-link-action-v1',
                      markings: ['private', 'enterprise'],
                      targetParameter: 'uid',
                      inputs: { uid: schema.properties.uid, target: schema.properties.uid },
                      condition: { literal: true },
                      edits: [],
                      typedEdits: [
                        {
                          op: operation,
                          linkType: 'history-link',
                          sourceUid: { parameter: 'uid' },
                          targetUid: { parameter: 'target' },
                        },
                      ],
                    },
                  ],
                );
              }
              // Complete normalized registration metadata, without dispatching a Search worker/provider.
              await db.pool.query(
                'INSERT INTO moria.native_model_bindings(environment,resource,revision,definition) VALUES($1,$2,$3,$4)',
                [
                  environment,
                  model,
                  'history-model-v1',
                  {
                    endpointRef: 'HISTORY_UNUSED_ENDPOINT',
                    model: 'history-model',
                    modelRevision: 'history-model-v1',
                    resourceClass: 'models',
                    destination: 'local-model',
                    markings: ['private', 'enterprise'],
                    destinationPolicy: {
                      allowedDestinations: ['local-model'],
                      allowedMarkings: ['private', 'enterprise'],
                    },
                    capabilities: ['embed'],
                    dimensions: 3,
                  },
                ],
              );
              await db.pool.query(
                "INSERT INTO moria.runtime_service_authorities(id,environment,principal,application,operations,markings,domains,resource_classes,incarnation) SELECT $1,$2,'pipeline','capacity',$3,ARRAY['private','enterprise'],ARRAY['search'],ARRAY['search'],incarnation FROM moria.indexing_instance",
                [
                  service,
                  environment,
                  [
                    `${collection}:project`,
                    `${target}:read`,
                    `${fixture.source}:read`,
                    `${model}:invoke`,
                  ],
                ],
              );
              await db.pool.query(
                "INSERT INTO moria.runtime_service_targets(environment,resource,domain,binding_revision,resource_class,authority) VALUES($1,$2,'search','history-service-v1','search',$3)",
                [environment, collection, service],
              );
              await db.pool.query(
                'INSERT INTO moria.native_search_collections(id,environment,resource,revision,definition) VALUES($1,$2,$3,$4,$5)',
                [
                  collection,
                  environment,
                  collection,
                  'history-search-v1',
                  {
                    source: target,
                    embeddingModel: model,
                    textProperty: 'name',
                    chunkSize: 1000,
                    chunkOverlap: 100,
                    metric: 'cosine',
                    searchMode: 'exact',
                    markings: ['private', 'enterprise'],
                  },
                ],
              );
              const projector = (
                await db.pool.query(
                  "INSERT INTO moria.native_search_projector_bindings(revision,environment,collection,collection_id,service_id,service_binding_revision,source_resource,text_property,source_dataset,model_resource,model_destination,markings) VALUES('history-projector-v1',$1,$2,$2::text::uuid,$3,'history-service-v1',$4,'name',$5,$6,'local-model',ARRAY['private','enterprise']) RETURNING id",
                  [environment, collection, service, target, fixture.source, model],
                )
              ).rows[0].id;
              await db.pool.query(
                'UPDATE moria.native_search_collections SET projector_binding=$3 WHERE environment=$1 AND resource=$2',
                [environment, collection, projector],
              );
              const generations = [
                {
                  stage: projection.stage,
                  projection: projection.id,
                  revision: (
                    await db.pool.query(
                      'SELECT revision::text FROM moria.source_generations WHERE environment=$1 AND object_type=$2 AND stage=$3',
                      [environment, target, projection.stage],
                    )
                  ).rows[0].revision,
                },
              ];
              generations.push(await fixture.reindex('history-second'));
              generations.push(await fixture.reindex('history-third'));
              const other = (
                await db.pool.query(
                  'SELECT uid FROM moria.staged_source_rows WHERE stage=$1 AND uid<>$2 ORDER BY uid LIMIT 1',
                  [generations[2].stage, uid],
                )
              ).rows[0].uid;
              for (let i = 0; i < 6; i++) {
                const response = await fetch(
                  base +
                    '/api/v1/actions/' +
                    (i % 2 === 0 ? 'history-link-on' : 'history-link-off') +
                    '/invoke',
                  {
                    method: 'POST',
                    headers: { ...headers, 'idempotency-key': randomUUID() },
                    body: JSON.stringify({ input: { uid, target: other } }),
                  },
                );
                assert.equal(response.status, 200, await response.text());
              }
              assert.equal(
                (
                  await db.pool.query(
                    'SELECT count(*)::int n FROM moria.native_link_edge_versions WHERE environment=$1 AND link=$2',
                    [environment, 'history-link'],
                  )
                ).rows[0].n,
                6,
              );
              const input = { projection: ['uid', 'name'], pageSize: 1 };
              const query = async (body: unknown) => {
                const response = await fetch(base + '/api/v1/objects/' + target + '/query', {
                  method: 'POST',
                  headers,
                  body: JSON.stringify(body),
                });
                const value = await response.json();
                assert.equal(response.status, 200, JSON.stringify(value));
                return value;
              };
              const pinned = await query(input);
              assert.equal(typeof pinned.nextCursor, 'string');
              const pinHash = createHash('sha256').update(pinned.nextCursor).digest('hex');
              const expiry = (
                await db.pool.query(
                  'SELECT expires_at::text FROM moria.read_cursors WHERE token_hash=$1',
                  [pinHash],
                )
              ).rows[0].expires_at;
              generations.push(await fixture.reindex('history-fourth'));
              generations.push(await fixture.reindex('history-fifth'));
              const response = await fetch(base + '/api/v1/actions/edit/invoke', {
                method: 'POST',
                headers: { ...headers, 'idempotency-key': randomUUID() },
                body: JSON.stringify({ input: { uid, name: 'latest-history' } }),
              });
              assert.equal(response.status, 200, await response.text());
              const latest = (
                await db.pool.query('SELECT revision::text FROM moria.environments WHERE id=$1', [
                  environment,
                ])
              ).rows[0].revision;
              // Structural Search-confinement fixture only: actual admitted queued execution, no fabricated worker or lease.
              // Native Search lifecycle and provider work are proved independently by its component integration suite.
              const current = await authority.current(environment, 'pipeline', 'capacity');
              const executionBinding = (
                await db.pool.query(
                  "INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation) SELECT $1,$2,$3,$4,$5,ARRAY['private','enterprise'],$6,s.security_epoch,i.incarnation FROM moria.instance_state s,moria.indexing_instance i RETURNING id",
                  [
                    environment,
                    collection,
                    {
                      version: 1,
                      domain: 'search',
                      release: 'a'.repeat(64),
                      resourceClass: 'search',
                      timeoutSeconds: 120,
                      retryPolicy: 'idempotent',
                      inputSchema: { type: 'object', properties: {} },
                      pinnedInputs: [],
                      expectedHeads: {},
                    },
                    {
                      environment,
                      principal: 'pipeline',
                      application: 'capacity',
                      sessionHash: createHash('sha256').update(token).digest('hex'),
                    },
                    [`${collection}:project`],
                    current.epoch,
                  ],
                )
              ).rows[0].id;
              const execution = (
                await db.pool.query(
                  "SELECT moria.runtime_execution_request($1,$2,'{}'::jsonb,$3,$4::jsonb) id",
                  [
                    executionBinding,
                    randomUUID(),
                    'b'.repeat(64),
                    JSON.stringify([{ key: 'main' }]),
                  ],
                )
              ).rows[0].id;
              const search = (
                await db.pool.query(
                  "INSERT INTO moria.native_search_generations(environment,resource,collection_revision,definition,object_binding_revision,model_fingerprint,model_revision,dimensions,chunker_revision,incarnation,base_revision,covered_revision,status) SELECT $1,$2,'history-search-v1','{}','history-object-v1','history-model-v1','history-model-v1',3,'history-chunker-v1',incarnation,$3,$4,'active' FROM moria.indexing_instance RETURNING id",
                  [environment, collection, generations[1].revision, latest],
                )
              ).rows[0].id;
              await db.pool.query(
                "INSERT INTO moria.native_search_rebuilds(generation,execution,environment,resource,principal,application,request_key,source_stage,source_revision,source_schema,partitions,markings,base_verified,projector_binding) VALUES($1,$2,$3,$4,'pipeline','capacity',$5,$6,$7,$8,1,ARRAY['private','enterprise'],true,$9)",
                [
                  search,
                  execution,
                  environment,
                  collection,
                  randomUUID(),
                  generations[1].stage,
                  generations[1].revision,
                  schema,
                  projector,
                ],
              );
              await db.pool.query(
                'UPDATE moria.native_search_collections SET active_generation=$3 WHERE environment=$1 AND resource=$2',
                [environment, collection, search],
              );
              const table = (id: string) => 'moria.runtime_projection_' + id.replaceAll('-', '');
              const exists = async (id: string) =>
                (await db.pool.query('SELECT to_regclass($1) name', [table(id)])).rows[0].name !==
                null;
              const sourceRows = async (stage: string) =>
                (
                  await db.pool.query(
                    'SELECT count(*)::int n FROM moria.staged_source_rows WHERE stage=$1',
                    [stage],
                  )
                ).rows[0].n;
              const plan = async () => {
                await db.pool.query(
                  "UPDATE moria.runtime_history_revisions SET observed_at=clock_timestamp()-interval '3 days' WHERE environment=$1",
                  [environment],
                );
                const id = (
                  await db.pool.query('SELECT moria.runtime_history_plan($1) id', [policy])
                ).rows[0].id;
                await db.pool.query(
                  "UPDATE moria.runtime_history_collections SET observed_at=clock_timestamp()-interval '3 days',not_before=clock_timestamp()-interval '1 day' WHERE id=$1",
                  [id],
                );
                return id;
              };
              const finish = async () => {
                let result: any;
                for (let i = 0; i < 100; i++) {
                  result = (
                    await db.pool.query('SELECT moria.runtime_history_step($1) value', [policy])
                  ).rows[0].value;
                  if (result.state === 'complete') return result;
                }
                assert.fail(JSON.stringify(result));
              };
              const planned = await plan();
              assert.equal(
                (
                  await db.pool.query(
                    'SELECT floor::text FROM moria.runtime_history_collections WHERE id=$1',
                    [planned],
                  )
                ).rows[0].floor,
                pinned.revision,
              );
              const backup = await db.pool.connect();
              try {
                await backup.query(
                  "SELECT pg_advisory_lock_shared(hashtextextended('moria/recovery-retention/v1',0))",
                );
                let stopped: any;
                for (let i = 0; i < 100; i++) {
                  stopped = (
                    await db.pool.query('SELECT moria.runtime_history_step($1) value', [policy])
                  ).rows[0].value;
                  if (stopped.state === 'backupBusy') break;
                }
                assert.equal(stopped.state, 'backupBusy', JSON.stringify(stopped));
                assert.equal(await exists(generations[0].projection), true);
                assert.equal(
                  (
                    await db.pool.query(
                      'SELECT dropped_tables::text FROM moria.runtime_history_collections WHERE id=$1',
                      [planned],
                    )
                  ).rows[0].dropped_tables,
                  '0',
                );
                await backup.query(
                  "SELECT pg_advisory_unlock_shared(hashtextextended('moria/recovery-retention/v1',0))",
                );
              } finally {
                backup.release();
              }
              await finish();
              assert.equal(
                await exists(generations[0].projection),
                false,
                'unreferenced retired table is actually dropped',
              );
              for (const generation of generations.slice(1))
                assert.equal(await exists(generation.projection), true);
              assert.equal(await sourceRows(generations[0].stage), 0);
              for (const generation of generations.slice(1))
                assert.equal(await sourceRows(generation.stage), 64);
              assert.equal(
                (
                  await db.pool.query(
                    'SELECT count(*)::int n FROM moria.native_link_edge_versions WHERE environment=$1 AND link=$2',
                    [environment, 'history-link'],
                  )
                ).rows[0].n,
                1,
              );
              assert.equal(
                (
                  await db.pool.query(
                    'SELECT linked FROM moria.native_link_edge_versions WHERE environment=$1 AND link=$2',
                    [environment, 'history-link'],
                  )
                ).rows[0].linked,
                false,
              );
              const continuation = await query({ ...input, cursor: pinned.nextCursor });
              assert.equal(continuation.revision, pinned.revision);
              assert.equal(
                (
                  await db.pool.query(
                    'SELECT expires_at::text FROM moria.read_cursors WHERE token_hash=$1',
                    [createHash('sha256').update(continuation.nextCursor).digest('hex')],
                  )
                ).rows[0].expires_at,
                expiry,
              );
              assert.equal((await read()).object.name, 'latest-history');
              // Explicitly release the fixture cursors after proving their original pinned order/expiry.
              await db.pool.query('DELETE FROM moria.read_cursors WHERE environment=$1', [
                environment,
              ]);
              await plan();
              await finish();
              assert.equal(
                await exists(generations[2].projection),
                false,
                'released floor anchor is now collectible',
              );
              assert.equal(
                await exists(generations[1].projection),
                true,
                'active Search source-stage pin still protects its table',
              );
              assert.equal(
                await exists(generations[3].projection),
                true,
                'latest-two policy protects the prior generation below the floor',
              );
              assert.equal(
                await exists(generations[4].projection),
                true,
                'current table remains present',
              );
              assert.equal(await sourceRows(generations[1].stage), 64);
              assert.equal(await sourceRows(generations[2].stage), 0);
              await db.pool.query(
                "UPDATE moria.native_search_generations SET status='retired' WHERE id=$1",
                [search],
              );
              await plan();
              await finish();
              assert.equal(
                await exists(generations[1].projection),
                false,
                'released Search stage becomes collectible',
              );
              assert.equal(await sourceRows(generations[1].stage), 0);
              assert.equal(await sourceRows(generations[3].stage), 64);
              assert.equal(await sourceRows(generations[4].stage), 64);
              assert.equal(
                (
                  await db.pool.query(
                    'SELECT count(*)::int n FROM moria.source_generations WHERE environment=$1 AND object_type=$2',
                    [environment, target],
                  )
                ).rows[0].n,
                5,
              );
              assert.equal((await read()).object.name, 'latest-history');
            },
          );
        } finally {
          child.kill('SIGKILL');
          await exited;
        }
      },
    });
  },
);
