// Compiled by pnpm typecheck; this function is never invoked.
import { createClient as snow } from '../apps/snotel/src/generated/index.js';
import { createClient as management } from '../apps/management/src/generated/index.js';
async function compileChecks() {
  const s = snow(),
    m = management();
  // @ts-expect-error Snow exposes no management objects
  s.objects.assets.query();
  // @ts-expect-error Snow exposes no mutation actions
  s.actions['edit-status']({ uid: 'x', status: 'bad' }, { idempotencyKey: 'x' });
  // @ts-expect-error Snow has no management import endpoint
  s.endpoints.importAssets([]);
  // @ts-expect-error Missing action input
  m.actions['edit-status']({ uid: 'x' }, { idempotencyKey: 'x' });
  // @ts-expect-error Action input has wrong value type
  m.actions['edit-status']({ uid: 'x', status: 5 }, { idempotencyKey: 'x' });
  // @ts-expect-error Mutation requires explicit retry identity
  m.actions['edit-status']({ uid: 'x', status: 'x' });
  // @ts-expect-error Projection names come from the object schema
  s.objects['snow-stations'].query({ projection: ['secret'] });
  s.objects['snow-stations'].query({
    // @ts-expect-error Filter value matches the selected field
    filter: { op: 'eq', args: [{ field: 'latitude' }, { literal: 'bad' }] },
  });
  const result = await s.objects['snow-stations'].query({ projection: ['name'] });
  const name: string = result.objects[0].name;
  // @ts-expect-error Unrequested properties are absent from the result type
  result.objects[0].latitude;
  const obs = await s.objects['snow-observations'].get('id');
  const depth: number | null = obs.object.snowDepth;
  // @ts-expect-error Missing measurements remain nullable
  const bad: number = obs.object.snowDepth;
  return { name, depth, bad };
}
void compileChecks;
