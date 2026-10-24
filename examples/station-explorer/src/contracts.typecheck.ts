import { createClient } from './generated/index.js';
// Compile-only examples: the generated surface has no writes or admin API.
export function checkContract(client: ReturnType<typeof createClient>) {
  // @ts-expect-error No management operations in this app contract.
  client.endpoints.jobs();
  // @ts-expect-error No action capability in this app contract.
  client.actions['edit-status']({});
  // @ts-expect-error Snow observations were not requested by this app.
  client.objects['snow-observations'].query();
  // @ts-expect-error Invalid station property.
  client.objects['snow-stations'].query({ projection: ['password'] });
}
