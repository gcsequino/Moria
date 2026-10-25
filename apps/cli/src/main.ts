import { writeSdk } from '../../../packages/osdk/src/generate.js';
import { readFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { compileManifest } from '../../../packages/compiler/src/index.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import { appCommand } from './apps.js';
import { snowCommand } from './snotel.js';
import { authCommand } from './auth.js';

try {
  const { positionals, values } = parseArgs({
    allowPositionals: true,
    options: {
      json: { type: 'boolean' },
      out: { type: 'string' },
      check: { type: 'boolean' },
      help: { type: 'boolean' },
      attributes: { type: 'string' },
      directory: { type: 'string' },
      port: { type: 'string' },
      config: { type: 'string' },
      email: { type: 'string' },
      app: { type: 'string' },
    },
  });
  if (values.help || !positionals.length) {
    console.log(
      'Moria\n\n  moria sdk generate <app-contract.json> --out <directory> [--check]\n  moria auth setup|recover|disable [--app management]\n  moria auth grant-admin --email you@gmail.com\n  moria auth configure-google --config client.json --email you@gmail.com\n  moria validate|compile <manifest.json> [--attributes types.json] [--json]\n  moria apps bootstrap|recover [management] [--directory instance-dir]\n  moria apps list\n  moria apps publish <id> <built-directory>\n  moria apps promote <id> <version>\n  moria apps rollback|verify|unlock <id>\n  moria apps serve <id> --port 3211\n\nApp commands run locally without the management UI or database. All accept --directory and --json. Start the bundled console with pnpm start.',
    );
  } else if (positionals[0] === 'sdk') {
    if (positionals[1] !== 'generate' || !positionals[2] || positionals.length !== 3 || !values.out)
      throw new MoriaError(
        'USAGE',
        'moria sdk generate <app-contract.json> --out <directory> [--check]',
      );
    console.log(
      JSON.stringify(
        await writeSdk(
          JSON.parse(await readFile(positionals[2], 'utf8')),
          values.out,
          values.check,
        ),
        null,
        2,
      ),
    );
  } else if (positionals[0] === 'auth') {
    console.log(JSON.stringify(await authCommand(positionals.slice(1), values), null, 2));
  } else if (positionals[0] === 'snotel') {
    console.log(JSON.stringify(await snowCommand(positionals.slice(1), values), null, 2));
  } else if (positionals[0] === 'apps') {
    console.log(JSON.stringify(await appCommand(positionals.slice(1), values), null, 2));
  } else {
    const [command, file, ...extra] = positionals;
    if (!['validate', 'compile'].includes(command) || !file || extra.length)
      throw new MoriaError('USAGE', 'Expected validate|compile <manifest.json>');
    const context = values.attributes
      ? { principalAttributes: JSON.parse(await readFile(values.attributes, 'utf8')) }
      : undefined;
    const result = compileManifest(JSON.parse(await readFile(file, 'utf8')), context);
    const output =
      command === 'compile'
        ? result
        : {
            valid: true,
            resources: result.order.length,
            fingerprint: result.fingerprint,
            policyTypesChecked: result.policyTypesChecked,
          };
    if (values.json || command === 'compile') console.log(JSON.stringify(output, null, 2));
    else console.log(`Validated ${result.order.length} resources. ${result.fingerprint}`);
  }
} catch (error) {
  const e =
    error instanceof MoriaError
      ? error
      : new MoriaError('INPUT_INVALID', error instanceof Error ? error.message : 'Invalid input');
  console.error(
    JSON.stringify({ error: { code: e.code, message: e.message, details: e.details } }),
  );
  process.exitCode = 2;
}
