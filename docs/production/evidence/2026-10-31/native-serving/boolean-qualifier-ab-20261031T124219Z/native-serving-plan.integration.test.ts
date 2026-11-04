import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { fingerprint } from '../packages/compiler/src/index.js';
import { nativeProjectionCapacity } from '../benchmarks/native-projection.js';

test(
  'exact protected seven-field page plans retain native policy and byte guards',
  { timeout: 180000 },
  async (t) => {
    const output = await mkdtemp(join(tmpdir(), 'moria-protected-page-plan-'));
    t.after(() => rm(output, { recursive: true, force: true }));
    await nativeProjectionCapacity({
      rows: 300,
      rowBytes: 4096,
      output,
      onVerified: async (fixture) => {
        const { db, environment, target, schema, runtime, nativeEnv, token } = fixture;
        const definition = {
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
        };
        await db.pool.query(
          'INSERT INTO moria.native_serving_bindings(environment,resource,object_type_id,revision,definition) VALUES($1,$2,$3,$4,$5)',
          [environment, target, target, fingerprint(definition), definition],
        );
        const child = spawn(runtime, ['serve'], {
          env: {
            ...nativeEnv,
            MORIA_NATIVE_LISTEN: '127.0.0.1:0',
            MORIA_NATIVE_EXPLAIN_SAMPLES: '2',
          },
          stdio: ['ignore', 'pipe', 'pipe'],
        });
        const exited = new Promise<void>((resolve) => child.once('exit', () => resolve()));
        let stdout = '',
          stderr = '',
          base = '';
        child.stdout.on('data', (bytes) => {
          stdout += bytes.toString();
        });
        child.stderr.on('data', (bytes) => {
          stderr += bytes.toString();
          assert(stderr.length <= 1024 * 1024);
        });
        try {
          for (let i = 0; i < 500 && !base; i++) {
            for (const line of stdout.split('\n')) {
              try {
                const event = JSON.parse(line);
                if (event.event === 'serving') base = 'http://' + event.address;
              } catch {}
            }
            if (child.exitCode !== null) throw Error('Native page-plan server exited');
            if (!base) await delay(10);
          }
          assert(base, 'Native page-plan server starts');
          const uid = (
            await db.pool.query(
              'SELECT uid FROM moria.staged_source_rows WHERE stage=(SELECT g.stage FROM moria.source_generations g WHERE g.environment=$1 AND g.object_type=$2 ORDER BY g.revision DESC LIMIT 1) ORDER BY uid OFFSET 100 LIMIT 1',
              [environment, target],
            )
          ).rows[0].uid;
          const relation = (await db.pool.query('SELECT id FROM moria.runtime_projections WHERE environment=$1 AND resource=$2 AND status=$3', [environment,target,'active'])).rows[0].id.replaceAll('-','');
          for (const wrapper of [true,false]) {
            const expression = wrapper ? '(uid >= $1::uuid) IS TRUE' : 'uid >= $1::uuid';
            const plan = (await db.pool.query('EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) SELECT uid FROM moria."runtime_projection_'+relation+'" WHERE to_revision IS NULL AND NOT deleted AND k0 AND '+expression+' ORDER BY uid LIMIT 51',[uid])).rows[0]['QUERY PLAN'];
            const nodes: any[]=[];const visit=(p:any)=>{nodes.push(p);for(const child of p.Plans??[])visit(child);};visit(plan[0].Plan);
            console.log(JSON.stringify({event:'boolean_qualifier_ab',wrapper,executionMs:plan[0]['Execution Time'],nodes:nodes.map(p=>({type:p['Node Type'],rows:p['Actual Rows'],filtered:p['Rows Removed by Filter'],indexCondition:typeof p['Index Cond']==='string' ? p['Index Cond'].replaceAll(/[0-9a-f]{8}-[0-9a-f-]{27}/g,'[uid]'):null,buffers:p['Shared Hit Blocks']}))}));
          }
          const projection = ['uid', 'id', 'name', 'region', 'enabled', 'rank', 'amount'];
          const inputs = [
            {
              pageSize: 50,
              projection,
              filter: { op: 'gte', args: [{ field: 'uid' }, { literal: uid }] },
            },
            {
              pageSize: 50,
              projection,
              filter: { op: 'gte', args: [{ field: 'rank' }, { literal: '0' }] },
              sort: [{ property: 'rank', direction: 'desc' }],
            },
          ];
          for (const input of inputs) {
            const response = await fetch(base + `/api/v1/objects/${target}/query`, {
              method: 'POST',
              headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
              body: JSON.stringify(input),
            });
            const page = await response.json();
            assert.equal(response.status, 200, JSON.stringify(page));
            assert.equal(page.objects.length, 50);
            assert(
              page.objects.every(
                (row: Record<string, unknown>) =>
                  Object.keys(row).length === 7 && !Object.hasOwn(row, 'payload'),
              ),
            );
          }
          const plans = stderr.split('\n').flatMap((line) => {
            try {
              const value = JSON.parse(line);
              return value.event === 'serving_plan' ? [value] : [];
            } catch {
              return [];
            }
          });
          assert.equal(
            plans.length,
            2,
            'Capture the exact protected SQL and bound parameters for both actual HTTP requests',
          );
          for (const plan of plans) {
            assert.equal(plan.class, 'projection-layout-with-overflow');
            const inspect = (value: unknown) => {
              if (!value || typeof value !== 'object') return;
              assert(!Object.hasOwn(value, 'Filter'), 'Private plan expressions remain redacted');
              for (const child of Object.values(value)) inspect(child);
            };
            inspect(plan);
          }
          console.log(
            JSON.stringify({
              event: 'protected_page_plans',
              scope: '300-row functional diagnostic; no capacity qualification',
              rows: 300,
              rowBytes: 4096,
              projection,
              plans,
            }),
          );
        } finally {
          child.kill('SIGTERM');
          await Promise.race([
            exited,
            delay(10000, undefined, { ref: false }).then(() => {
              child.kill('SIGKILL');
            }),
          ]);
          await exited;
        }
      },
    });
  },
);
