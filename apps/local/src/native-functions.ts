import { resolve } from 'node:path';
import { MoriaError } from '../../../packages/contracts/types.js';

/** Replace the CLI process before any workspace, database pool or worker is created. */
export function nativeFunctionsWorker(): never {
  return nativeArtifactWorker('functions');
}

export function nativeArtifactWorker(
  domain: 'functions' | 'agents' | 'evaluations' | 'pipelines',
): never {
  const prefix = {
    functions: 'FUNCTION',
    agents: 'AGENT',
    evaluations: 'EVALUATION',
    pipelines: 'PIPELINE',
  }[domain];
  const code = `${prefix}_WORKER_UNAVAILABLE`;
  const execute = process.execve;
  if (
    process.platform !== 'linux' ||
    Number(process.versions.node.split('.')[0]) !== 24 ||
    typeof execute !== 'function'
  )
    throw new MoriaError(code, 'Native workers require Linux and Node 24.');
  const binary = process.env.MORIA_RUNTIME_BINARY ?? process.env.MORIA_INDEXING_BINARY;
  if (!binary)
    throw new MoriaError(code, 'Set MORIA_RUNTIME_BINARY to the native runtime executable.');
  const reference = process.env.MORIA_INDEXING_DATABASE_SECRET_REF;
  if (!reference || !/^[A-Za-z0-9_]+$/.test(reference) || !process.env[reference])
    throw new MoriaError(code, 'Configure the native database secret reference.');
  for (const name of [`MORIA_${prefix}_WORKER_OWNER`, `MORIA_${prefix}_HOST_ID`])
    if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(process.env[name] ?? ''))
      throw new MoriaError(code, `Configure ${name} for a registered native worker.`);
  const path = resolve(binary);
  execute(path, [path, domain, 'worker'], process.env);
  throw new MoriaError(code, 'Native worker startup failed.');
}
