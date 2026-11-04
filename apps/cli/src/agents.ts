import { readFile } from 'node:fs/promises';
import { agentsClient } from '../../../packages/platform/src/agents.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import { filesystemConnection, type FilesystemOptions } from './filesystem.js';

export async function agentsCommand(
  args: string[],
  values: FilesystemOptions & { key?: string; async?: boolean },
) {
  const [command, id, ...extra] = args;
  if (!id || extra.length)
    throw new MoriaError(
      'USAGE',
      'moria agents invoke|get|events|cancel|wait <resource-or-execution-uuid>',
    );
  const client = agentsClient((await filesystemConnection(values)).transport);
  if (command === 'invoke') {
    if (!values.config || !values.key)
      throw new MoriaError(
        'USAGE',
        'agents invoke <resource-uuid> --config input.json --key <stable-key> [--async]',
      );
    return client.invoke(id, JSON.parse(await readFile(values.config, 'utf8')), {
      idempotencyKey: values.key,
      wait: !values.async,
    });
  }
  if (command === 'get') return client.get(id);
  if (command === 'events') return client.events(id, values.cursor ?? '0');
  if (command === 'cancel') return client.cancel(id);
  if (command === 'wait') {
    const deadline = Date.now() + 30000;
    do {
      const execution = await client.get(id);
      if (!['queued', 'running'].includes(execution.status)) return execution;
      await new Promise((resolve) => setTimeout(resolve, 250));
    } while (Date.now() < deadline);
    return client.get(id);
  }
  throw new MoriaError('USAGE', 'Unknown agents command');
}
