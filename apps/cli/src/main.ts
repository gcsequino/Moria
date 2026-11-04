import { writeSdk } from '../../../packages/osdk/src/generate.js';
import { readFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { compileManifest } from '../../../packages/compiler/src/index.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import { appCommand } from './apps.js';
import { snowCommand } from './snotel.js';
import { authCommand } from './auth.js';
import { filesystemCommand } from './filesystem.js';
import { functionsCommand } from './functions.js';
import { ApiError } from '../../../packages/osdk/src/runtime.js';
import { nativeFunctionsWorker, nativeArtifactWorker } from '../../local/src/native-functions.js';
import { agentsCommand } from './agents.js';
import { evaluationsCommand } from './evaluations.js';
import { pipelinesCommand } from './pipelines.js';
import { mcpCommand } from './mcp.js';

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
      url: { type: 'string' },
      profile: { type: 'string' },
      project: { type: 'string' },
      cursor: { type: 'string' },
      kind: { type: 'string' },
      namespace: { type: 'string' },
      runtime: { type: 'string' },
      exports: { type: 'string' },
      revision: { type: 'string' },
      key: { type: 'string' },
      async: { type: 'boolean' },
    },
  });
  if (values.help || !positionals.length) {
    console.log(
      'MCP: moria mcp config|discover|tools [--url https://instance.example]; use a dedicated MORIA_API_TOKEN.',
    );
    console.log(
      'Evaluations: moria evaluations invoke|get|cases|events|cancel|wait <uuid>; compare <uuid> <other-uuid>; moria evaluations worker.',
    );
    console.log(
      'Agents: moria agents invoke|get|events|cancel|wait <uuid>; moria agents worker. See docs/production/contracts/agents-evaluations.md.',
    );
    console.log(
      'Functions: moria functions invoke|get|events|cancel|wait <uuid>; moria functions worker. See docs/FUNCTIONS.md.',
    );
    console.log(
      'Filesystem: moria fs home|ls|stat|history|mkdir|mv|cp|rm|get|put|project|group|init|register|checkout|bind|push|publish|plan|apply|resource|policy|retire|enable|disable|collect\nUse --url https://instance.example and MORIA_API_TOKEN for remote CI; see docs/FILESYSTEM.md for arguments.',
    );
    console.log(
      'Moria\n\n  moria sdk generate <app-contract.json> --out <directory> [--check]\n  moria auth setup|recover|disable [--app management]\n  moria auth grant-admin --email you@gmail.com\n  moria auth configure-google --config client.json --email you@gmail.com\n  moria validate|compile <manifest.json> [--attributes types.json] [--json]\n  moria apps bootstrap|recover [management] [--directory instance-dir]\n  moria apps list\n  moria apps publish <id> <built-directory>\n  moria apps promote <id> <version>\n  moria apps rollback|verify|unlock <id>\n  moria apps serve <id> --port 3211\n\nApp commands run locally without the management UI or database. All accept --directory and --json. Start the bundled console with pnpm start.',
    );
  } else if (positionals[0] === 'mcp') {
    console.log(JSON.stringify(await mcpCommand(positionals.slice(1), values), null, 2));
  } else if (positionals[0] === 'fs') {
    console.log(JSON.stringify(await filesystemCommand(positionals.slice(1), values), null, 2));
  } else if (positionals[0] === 'pipelines') {
    if (positionals[1] === 'worker' && positionals.length === 2) nativeArtifactWorker('pipelines');
    console.log(JSON.stringify(await pipelinesCommand(positionals.slice(1), values), null, 2));
  } else if (positionals[0] === 'evaluations') {
    if (positionals[1] === 'worker' && positionals.length === 2)
      nativeArtifactWorker('evaluations');
    else
      console.log(JSON.stringify(await evaluationsCommand(positionals.slice(1), values), null, 2));
  } else if (positionals[0] === 'agents') {
    if (positionals[1] === 'worker' && positionals.length === 2) nativeArtifactWorker('agents');
    else console.log(JSON.stringify(await agentsCommand(positionals.slice(1), values), null, 2));
  } else if (positionals[0] === 'functions') {
    if (positionals[1] === 'worker' && positionals.length === 2) {
      nativeFunctionsWorker();
    } else {
      console.log(JSON.stringify(await functionsCommand(positionals.slice(1), values), null, 2));
    }
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
    error instanceof MoriaError || error instanceof ApiError
      ? error
      : new MoriaError('INPUT_INVALID', error instanceof Error ? error.message : 'Invalid input');
  console.error(
    JSON.stringify({ error: { code: e.code, message: e.message, details: e.details } }),
  );
  process.exitCode = 2;
}
