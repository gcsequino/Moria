import { MoriaError } from '../../../packages/contracts/types.js';

export async function snowCommand(
  _args: string[],
  _options: { directory?: string; config?: string },
) {
  throw new MoriaError(
    'USAGE',
    'The bundled Snow/weather POC is retired. Use management for platform job history.',
  );
}
