import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { AppHost } from '../services/app-host/src/store.js';
import { readWorkflow } from '../services/lineage/src/workflow.js';
import { snowContract, managementContract } from '../packages/osdk/src/bundled.js';
test('workflow lineage follows active immutable contracts and never invents access or usage', async () => {
  const root = await mkdtemp(join(tmpdir(), 'moria-workflow-'));
  const host = new AppHost(join(root, 'host'));
  for (const contract of [snowContract, managementContract]) {
    const dir = join(root, contract.app);
    await mkdir(dir);
    await writeFile(join(dir, 'index.html'), '<html>Workflow fixture</html>');
    await writeFile(
      join(dir, 'moria-workflow.json'),
      JSON.stringify({
        version: 1,
        environment: contract.app === 'management' ? 'local-workspace' : 'snotel',
        contract,
      }),
    );
    const release = await host.publish(contract.app, dir);
    await host.promote(contract.app, release.version);
  }
  const graph = await readWorkflow(host);
  const snow = graph.nodes.find((n) => n.kind === 'application' && n.label === 'snotel')!;
  assert.ok(snow);
  assert.equal(
    graph.edges.filter((e) => e.to === snow.id && e.label === 'declares read').length,
    2,
  );
  assert.equal(
    graph.edges.filter((e) => e.from === snow.id && e.label === 'declares invoke').length,
    0,
  );
  assert.ok(graph.edges.some((e) => e.label === 'declares invoke'));
  assert.ok(graph.limitations.some((s) => s.includes('not observed usage')));
  const source = join(root, 'snotel');
  await writeFile(
    join(source, 'moria-workflow.json'),
    JSON.stringify({
      version: 1,
      environment: 'snotel',
      contract: { ...snowContract, actions: { 'new-action': { inputs: {} } } },
    }),
  );
  const next = await host.publish('snotel', source);
  await host.promote('snotel', next.version);
  assert.ok((await readWorkflow(host)).nodes.some((n) => n.label === 'new-action'));
  await host.rollback('snotel');
  assert.equal(
    (await readWorkflow(host)).nodes.some((n) => n.label === 'new-action'),
    false,
  );
});
