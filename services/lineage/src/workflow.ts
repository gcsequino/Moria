import type { AppHost } from '../../app-host/src/store.js';
import type { LineageGraph } from './service.js';
import { validateContract } from '../../../packages/osdk/src/generate.js';

/** Declared client dependencies pinned to an integrity-checked deployed artifact.
 * This graph is descriptive and must never be used to grant application authority. */
export async function readWorkflow(host: AppHost): Promise<LineageGraph> {
  const graph: LineageGraph = {
    nodes: [],
    edges: [],
    limitations: [
      'Workflow relationships describe the SDK surface packaged with each active app release, not observed usage or effective permissions. Property-level usage, action edit targets, functions and models are not yet recorded.',
    ],
  };
  for (const entry of (await host.list()).slice(0, 50)) {
    if (!entry.deployment || entry.error) {
      graph.limitations.push(`Workflow unavailable for ${entry.app}.`);
      continue;
    }
    try {
      const bytes = await host.file(entry.app, entry.deployment.current, 'moria-workflow.json');
      const metadata = JSON.parse(bytes.toString('utf8'));
      if (
        metadata.version !== 1 ||
        typeof metadata.environment !== 'string' ||
        !/^[-a-zA-Z0-9_]{1,100}$/.test(metadata.environment)
      )
        throw new Error('Invalid metadata');
      const contract = validateContract(metadata.contract);
      if (contract.app !== entry.app) throw new Error('Mismatched app');
      const app = JSON.stringify(['application', entry.app, entry.deployment.current]);
      graph.nodes.push({
        id: app,
        label: entry.app,
        kind: 'application',
        status: 'active',
        detail: JSON.stringify({
          release: entry.deployment.current,
          promotedAt: entry.deployment.promotedAt,
          environment: metadata.environment,
          evidence: 'Immutable bundled SDK contract',
        }),
      });
      for (const [name, definition] of Object.entries(contract.objects)) {
        const id = JSON.stringify(['object-type', metadata.environment, name]);
        graph.nodes.push({
          id,
          label: name,
          kind: 'object-type',
          status: null,
          detail: JSON.stringify({
            environment: metadata.environment,
            properties: Object.keys(definition.properties),
            evidence: 'Declared object read surface',
          }),
        });
        graph.edges.push({ from: id, to: app, label: 'declares read' });
      }
      for (const [name, definition] of Object.entries(contract.actions)) {
        const id = JSON.stringify(['action', metadata.environment, name]);
        graph.nodes.push({
          id,
          label: name,
          kind: 'action',
          status: 'declared',
          detail: JSON.stringify({
            environment: metadata.environment,
            inputs: Object.keys(definition.inputs),
          }),
        });
        graph.edges.push({ from: app, to: id, label: 'declares invoke' });
      }
      for (const [name, definition] of Object.entries(contract.endpoints)) {
        const id = JSON.stringify(['endpoint', entry.app, name]);
        graph.nodes.push({
          id,
          label: name,
          kind: 'endpoint',
          status: 'declared',
          detail: JSON.stringify({ path: definition.path, method: definition.method }),
        });
        graph.edges.push({ from: app, to: id, label: 'declares endpoint' });
      }
    } catch {
      graph.limitations.push(
        `No valid workflow metadata in active ${entry.app} release. Rebuild with the current CLI to record dependencies.`,
      );
    }
  }
  return graph;
}
