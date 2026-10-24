import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { defaultConfig } from '../../../services/snotel/src/connector.js';
import { MoriaError } from '../../../packages/contracts/types.js';
export async function snowCommand(
  args: string[],
  options: { directory?: string; port?: string; config?: string },
) {
  const [command, ...extra] = args;
  if (
    ![
      'sync',
      'status',
      'open',
      'jobs',
      'retry',
      'schedule',
      'weather-sync',
      'weather-schedule',
    ].includes(command) ||
    (['retry', 'schedule', 'weather-schedule'].includes(command)
      ? extra.length !== 1
      : extra.length !== 0)
  )
    throw new MoriaError(
      'USAGE',
      'snotel sync [--config file.json] | weather-sync | status | jobs | retry <id> | schedule on|off | weather-schedule on|off | open',
    );
  const directory = resolve(options.directory ?? process.env.MORIA_LOCAL_DIR ?? '.local/workspace');
  if (command === 'open') {
    return { url: `http://127.0.0.1:${process.env.MORIA_SNOTEL_PORT ?? 3211}/snow/` };
  }
  const port = Number(options.port ?? process.env.MORIA_PORT ?? 3210);
  if (!Number.isInteger(port) || port < 1024 || port > 65535)
    throw new MoriaError('USAGE', 'Invalid local management port.');
  const credentials = JSON.parse(await readFile(resolve(directory, 'credentials.json'), 'utf8'));
  const schedule = command === 'schedule' || command === 'weather-schedule';
  if (schedule && !['on', 'off'].includes(extra[0]))
    throw new MoriaError('USAGE', 'Schedule expects on or off.');
  const mutation = ['sync', 'weather-sync', 'retry', 'schedule', 'weather-schedule'].includes(
    command,
  );
  const path =
    command === 'jobs'
      ? 'jobs'
      : command === 'retry'
        ? 'jobs/retry'
        : command === 'weather-sync'
          ? 'weather/sync'
          : command === 'weather-schedule'
            ? 'weather/schedule'
            : `snotel/${command}`;
  const input =
    command === 'retry'
      ? { id: extra[0] }
      : schedule
        ? { enabled: extra[0] === 'on', intervalMinutes: command === 'weather-schedule' ? 60 : 360 }
        : {
            requestKey: randomUUID(),
            ...(options.config
              ? { config: JSON.parse(await readFile(options.config, 'utf8')) }
              : {}),
          };
  const response = await fetch(`http://127.0.0.1:${port}/management/v1/${path}`, {
    method: mutation ? 'POST' : 'GET',
    headers: {
      'content-type': 'application/json',
      'x-moria-key': credentials.accessKey,
      'x-moria-profile': 'operator',
    },
    ...(mutation ? { body: JSON.stringify(input) } : {}),
    signal: AbortSignal.timeout(30000),
  });
  const result = (await response.json()) as { error?: { message: string } };
  if (!response.ok)
    throw new MoriaError('SYNC_FAILED', result.error?.message ?? 'SNOTEL operation failed.');
  return result;
}
