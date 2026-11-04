import { readFile } from 'node:fs/promises';
import { writeSdk } from '../packages/osdk/src/generate.js';
import { managementContract, snowContract } from '../packages/osdk/src/bundled.js';
for (const contract of [managementContract, snowContract])
  await writeSdk(
    JSON.parse(JSON.stringify(contract)),
    `apps/${contract.app}/src/generated`,
    process.argv.includes('--check'),
  );

const starterContract = JSON.parse(
  await readFile('examples/station-explorer/app-contract.json', 'utf8'),
);
await writeSdk(
  starterContract,
  'examples/station-explorer/src/generated',
  process.argv.includes('--check'),
);
