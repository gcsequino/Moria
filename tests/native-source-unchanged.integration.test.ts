import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { actionClient, objectClient } from '../packages/osdk/src/runtime.js';
import { filesystemClient } from '../packages/platform/src/filesystem.js';
import { Authority } from '../services/security/src/authority.js';
import { publishNativeSource } from './fixtures/native-source/setup.js';
import { prepareSourceStoreProjector } from './fixtures/native-source-store/projector.js';
import type { ObjectSchema } from '../services/ontology/src/store.js';
import { managedRetentionDataset } from './fixtures/native-retention/dataset.js';
import { ageClosedDatasetHistory } from './fixtures/native-retention/history.js';
import { prepareManagedSource } from './fixtures/native-source-store/managed.js';

for (const graph of [false, true])
  test(
    'certified metadata-only ' +
      (graph ? 'v3' : 'v2') +
      ' commits reuse the Source rows, keys, typed projection and Action state',
    { timeout: 300000 },
    async (t) => {
      const columns: ObjectSchema['properties'] = {
        uid: { type: 'uid', nullable: false },
        tags: { type: 'array', nullable: false, items: { type: 'string', nullable: false } },
        note: { type: 'string', nullable: true },
      };
      const fixture = await managedRetentionDataset({
        primaryKey: ['tags'],
        columns,
        declaredMarking: graph,
      });
      t.after(fixture.close);
      const { db, environment, dataset, token } = fixture;
      const domain = environment + '/source-unchanged';
      const owner = await fixture.command(['identity', 'register-domain'], {
        domain,
        cell: fixture.cell,
      });
      const identity = await fixture.command(['identity', 'resolve'], {
        domain,
        keys: ['one', 'two', 'three', 'four'],
        owner,
      });
      const rows = (identity.uids as string[]).sort().map((uid, index) => ({
        uid,
        tags: [String(index)],
        note: 'initial-' + index,
      }));
      await fixture.publish(rows, { graph });
      const second = await fixture.publish(rows, { graph });
      const basis = await fixture.publish(rows, { graph });
      const resources: { path: string; text: string }[] = [
        {
          path: '.ontology.json',
          text: JSON.stringify({
            version: 1,
            name: 'Search model',
            namespace: 'v3_search',
            dependencies: [
              { rootId: fixture.published.root.id, digest: fixture.published.publication.digest },
            ],
          }),
        },
      ];
      const projector = graph
        ? await prepareSourceStoreProjector(
            async (path, value) => {
              resources.push({ path, text: JSON.stringify(value) });
            },
            t,
            {
              namespace: 'v3_search',
              markings: fixture.markings,
              textProperty: 'note',
              overlayText: 'retained overlay',
            },
          )
        : undefined;
      const modelRef = 'v3_search/Model/SourceEmbedding';
      const modelSource = projector
        ? await publishNativeSource(fixture.baseUrl, token, resources)
        : undefined;
      const model = modelSource?.resource(modelRef);
      if (projector) Object.assign(fixture.nativeEnv, projector.env);
      const managed = await prepareManagedSource(fixture, columns, {
        actions: true,
        profiles:
          projector && modelSource
            ? [
                {
                  environment,
                  projectId: modelSource.role.projectId,
                  reference: modelRef,
                  configuration: { binding: projector.binding },
                },
              ]
            : [],
        beforeApply: modelSource
          ? async (modelFiles) => {
              const plan = await modelFiles.plan(
                modelSource.root.id,
                modelSource.publication.digest,
              );
              const installed = (await modelFiles.apply(plan.id)) as {
                runtimeConfigured?: boolean;
              };
              assert.equal(installed.runtimeConfigured, true);
            }
          : undefined,
      });
      const reader = managed.transport(token);
      await managed.drain();
      const objects = objectClient<(typeof rows)[number]>(reader, managed.target, columns);
      const aliases = async () =>
        (
          await db.pool.query(
            'SELECT * FROM moria.native_source_generation_aliases WHERE environment=$1 AND object_type=$2 ORDER BY revision',
            [environment, managed.target],
          )
        ).rows;
      const projection = async () =>
        (
          await db.pool.query(
            "SELECT id,stage,activation_revision::text,closed_revision::text,binding_revision FROM moria.runtime_projections WHERE environment=$1 AND resource=$2 AND status='active'",
            [environment, managed.target],
          )
        ).rows[0];
      const [initial] = await aliases();
      assert.equal(initial.indexed_build, initial.store);
      assert.equal(initial.indexed_dataset_revision, basis.head.revision);
      const firstProjection = await projection();
      assert.equal(firstProjection.stage, initial.store);
      const revokedCursor = await objects.query({ pageSize: 1 });
      assert.equal(revokedCursor.objects[0].uid, rows[0].uid);
      assert(revokedCursor.nextCursor);
      await actionClient<{ uid: string; note: string }>(reader, managed.actions[0], {
        uid: columns.uid,
        note: columns.note,
      })({ uid: rows[1].uid, note: 'retained overlay' }, { idempotencyKey: randomUUID() });
      await actionClient<{ uid: string }>(reader, managed.actions[1], { uid: columns.uid })(
        { uid: rows[0].uid },
        { idempotencyKey: randomUUID() },
      );
      await managed.sessions.revoke(
        await managed.sessions.authenticate('Bearer ' + managed.registrar),
        'fixture',
      );
      await assert.rejects(objects.query({ pageSize: 1, cursor: revokedCursor.nextCursor }), {
        code: 'CURSOR_INVALIDATED',
      });
      await ageClosedDatasetHistory(fixture);
      const policy = await fixture.command(
        ['dataset-retention', 'approve'],
        {
          requestKey: randomUUID(),
          environment,
          dataset,
          retainSeconds: 86400,
          recheckSeconds: 30,
        },
        fixture.operatorEnv,
      );
      let held: Awaited<ReturnType<typeof objects.query>> | undefined;
      const advance = async () => {
        // Acquire under the final authority epoch, after registrar revocation and Search admission.
        held = await objects.query({ pageSize: 1 });
        assert.deepEqual(held.objects, [{ ...rows[1], note: 'retained overlay' }]);
        assert(held.nextCursor);
        for (const [index, beforeRevision] of [
          second.head.revision,
          basis.head.revision,
        ].entries()) {
          const expired = await fixture.command(
            ['dataset-retention', 'expire'],
            {
              policy: policy.policy,
              requestKey: randomUUID(),
              owner: randomUUID(),
              beforeRevision,
              maximumRoots: 1,
              timeSeconds: 120,
            },
            fixture.operatorEnv,
          );
          assert.equal(expired.headChanged, true);
          const targetHead = (await db.catalog.heads(environment, [dataset]))[0];
          const pending = (
            await db.pool.query(
              "SELECT id FROM moria.indexing_builds WHERE environment=$1 AND target=$2 AND source_revision=$3 AND status='pending'",
              [environment, managed.target, targetHead.revision],
            )
          ).rows[0].id;
          const output = await managed.drain();
          const currentAliases = await aliases();
          assert.equal(currentAliases.length, index + 2, output);
          const current = currentAliases.at(-1)!;
          assert.equal(current.store, initial.store);
          assert.equal(current.source_version, initial.source_version);
          assert.equal(current.indexed_build, pending);
          assert.notEqual(current.indexed_build, current.store);
          assert.equal(current.indexed_dataset_revision, targetHead.revision);
          assert.equal(current.indexed_snapshot, targetHead.snapshotId);
          const custody = (
            await db.pool.query(
              'SELECT b.receipt,b.status,w.status AS window,w.closed_at IS NOT NULL AS closed,w.basis_generation, (SELECT count(*)::int FROM moria.indexing_tasks WHERE build=b.id) AS tasks, (SELECT count(*)::int FROM moria.staged_source_rows WHERE stage=b.id) AS staged, (SELECT count(*)::int FROM moria.runtime_change_items item JOIN moria.runtime_change_transactions frame ON frame.id=item.transaction_id WHERE frame.environment=b.environment AND item.revision=$2::bigint AND item.object_type=b.target) AS items FROM moria.indexing_builds b JOIN moria.native_source_delta_windows w ON w.build=b.id WHERE b.id=$1',
              [pending, current.revision],
            )
          ).rows[0];
          assert.deepEqual(custody.receipt, {
            kind: 'unchanged',
            stage: initial.store,
            indexedBuild: pending,
            revision: current.revision,
            sourceRevision: targetHead.revision,
          });
          assert.equal(custody.status, 'active');
          assert.equal(custody.window, 'qualified');
          assert.equal(custody.closed, true);
          assert.equal(custody.basis_generation, currentAliases.at(-2)!.revision);
          assert.deepEqual([custody.tasks, custody.staged, custody.items], [0, 0, 0]);
          assert.deepEqual(await projection(), firstProjection);
          assert.deepEqual((await db.catalog.heads(environment, [dataset]))[0], targetHead);
        }
      };
      if (projector && model) {
        await projector.verify({
          db,
          environment,
          token,
          source: dataset,
          target: managed.target,
          authority: new Authority(db.catalog),
          principal: fixture.principal,
          application: fixture.application,
          model,
          base: managed.baseUrl,
          runtime: fixture.runtime,
          env: fixture.nativeEnv,
          expectedRows: 3,
          overlayUid: rows[1].uid,
          deletedUid: rows[0].uid,
          store: initial.store,
          installedModel: true,
          afterBuild: advance,
        });
      } else await advance();
      const status = await filesystemClient(reader).runtimeStatus(managed.dependent.root.id);
      assert.deepEqual(
        status.components.find((component) => component.reference === 'v3_source/ObjectType/Record')
          ?.readiness,
        { ready: true },
      );
      const fresh = await objects.query();
      assert.deepEqual(fresh.objects, [{ ...rows[1], note: 'retained overlay' }, ...rows.slice(2)]);
      assert(held);
      const historical = await objects.query({ pageSize: 1, cursor: held.nextCursor });
      assert.equal(historical.revision, held.revision);
      assert.deepEqual(historical.objects, [rows[2]]);
      const retained = (
        await db.pool.query('SELECT value FROM moria.runtime_object_base($1,$2,$3) ORDER BY uid', [
          environment,
          managed.target,
          initial.revision,
        ])
      ).rows;
      assert.deepEqual(
        retained.map((item) => item.value),
        rows,
      );
      const physical = (
        await db.pool.query(
          'SELECT (SELECT count(*)::int FROM moria.native_source_row_stores WHERE environment=$1 AND object_type=$2) AS stores, (SELECT count(*)::int FROM moria.staged_source_rows WHERE stage=$3) AS rows, (SELECT count(*)::int FROM moria.native_source_key_map WHERE store=$3) AS keys, (SELECT count(*)::int FROM moria.runtime_projections WHERE environment=$1 AND resource=$2) AS projections',
          [environment, managed.target, initial.store],
        )
      ).rows[0];
      assert.deepEqual(physical, { stores: 1, rows: 4, keys: 4, projections: 1 });
    },
  );
