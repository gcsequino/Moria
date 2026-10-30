import { readFile } from 'node:fs/promises';
import { functionsClient } from '../../../packages/platform/src/functions.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import { filesystemConnection, type FilesystemOptions } from './filesystem.js';

export async function functionsCommand(
  args: string[],
  values: FilesystemOptions & { key?: string; async?: boolean },
) {
  const [command, id, ...extra] = args;
  if (!id || extra.length)
    throw new MoriaError(
      'USAGE',
      'moria functions invoke|get|events|cancel|wait <resource-or-invocation-uuid>',
    );
  const client = functionsClient((await filesystemConnection(values)).transport);
  if (command === 'invoke') {
    if (!values.config || !values.key)
      throw new MoriaError(
        'USAGE',
        'functions invoke <resource-uuid> --config input.json --key <stable-call-key> [--async]',
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
    const until = Date.now() + 30000;
    do {
      const invocation = await client.get(id);
      if (!['accepted', 'running'].includes(invocation.status)) return invocation;
      await new Promise((resolve) => setTimeout(resolve, 250));
    } while (Date.now() < until);
    return client.get(id);
  }
  throw new MoriaError('USAGE', 'Unknown functions command');
}
