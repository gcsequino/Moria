import { createTransport } from '../../../packages/platform/src/transport.js';
import { filesystemClient } from '../../../packages/platform/src/filesystem.js';
import { functionsClient } from '../../../packages/platform/src/functions.js';
import { agentsClient } from '../../../packages/platform/src/agents.js';
import { evaluationsClient } from '../../../packages/platform/src/evaluations.js';
import { pipelinesClient } from '../../../packages/platform/src/pipelines.js';
const transport = createTransport({
  onUnauthenticated: () => window.dispatchEvent(new Event('moria:unauthenticated')),
});
export const platform = {
  transport,
  auth: transport.auth,
  filesystem: filesystemClient(transport),
  functions: functionsClient(transport),
  agents: agentsClient(transport),
  evaluations: evaluationsClient(transport),
  pipelines: pipelinesClient(transport),
};
