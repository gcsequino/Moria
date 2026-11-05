import { pipelinesClient } from '../../../packages/platform/src/pipelines.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import { filesystemConnection, type FilesystemOptions } from './filesystem.js';

export async function pipelinesCommand(
  args: string[],
  values: FilesystemOptions & { key?: string; async?: boolean },
) {
  const [command, id, ...extra] = args;
  if (!id || extra.length)
    throw new MoriaError('USAGE', 'moria pipelines invoke|discover|get|events|cancel|wait <uuid>');
  const client = pipelinesClient((await filesystemConnection(values)).transport);
  if (command === 'invoke' || command === 'discover') {
    if (!values.key)
      throw new MoriaError(
        'USAGE',
        `pipelines ${command} <resource-uuid> --key <stable-key> [--async]`,
      );
    return command === 'discover'
      ? client.discover(id, { idempotencyKey: values.key, wait: !values.async })
      : client.invoke(id, { idempotencyKey: values.key, wait: !values.async });
  }
  if (command === 'get') return client.get(id);
  if (command === 'events') return client.events(id, values.cursor ?? '0');
  if (command === 'cancel') return client.cancel(id);
  if (command === 'wait') {
    const deadline = Date.now() + 30000;
    do {
      const execution = await client.get(id);
      if (!['queued', 'running', 'preparing'].includes(execution.status)) return execution;
      await new Promise((resolve) => setTimeout(resolve, 250));
    } while (Date.now() < deadline);
    return client.get(id);
  }
  throw new MoriaError('USAGE', 'Unknown pipelines command');
}
