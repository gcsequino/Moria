import { evaluationsClient } from '../../../packages/platform/src/evaluations.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import { filesystemConnection, type FilesystemOptions } from './filesystem.js';

export async function evaluationsCommand(
  args: string[],
  values: FilesystemOptions & { key?: string; async?: boolean },
) {
  const [command, id, other, ...extra] = args;
  if (!id || extra.length || (other !== undefined && command !== 'compare'))
    throw new MoriaError(
      'USAGE',
      'moria evaluations invoke|get|cases|events|cancel|wait <uuid>; compare <uuid> <other-uuid>',
    );
  const client = evaluationsClient((await filesystemConnection(values)).transport);
  if (command === 'invoke') {
    if (!values.key)
      throw new MoriaError('USAGE', 'evaluations invoke <suite-uuid> --key <stable-key> [--async]');
    return client.invoke(id, { idempotencyKey: values.key, wait: !values.async });
  }
  if (command === 'get') return client.get(id);
  if (command === 'cases') return client.cases(id, values.cursor);
  if (command === 'events') return client.events(id, values.cursor ?? '0');
  if (command === 'cancel') return client.cancel(id);
  if (command === 'compare' && other) return client.compare(id, other);
  if (command === 'wait') {
    const deadline = Date.now() + 30000;
    do {
      const run = await client.get(id);
      if (!['queued', 'running'].includes(run.status)) return run;
      await new Promise((resolve) => setTimeout(resolve, 250));
    } while (Date.now() < deadline);
    return client.get(id);
  }
  throw new MoriaError('USAGE', 'Unknown evaluations command');
}
