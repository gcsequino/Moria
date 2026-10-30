import { spawn } from 'node:child_process';
import { resolve } from 'node:path';
import { VALUE_VERSION, MoriaError } from '../../packages/contracts/types.js';
import { canonicalJson } from '../../packages/contracts/values.js';

export function iceberg(request: unknown): Promise<any> {
  if (
    !request ||
    typeof request !== 'object' ||
    Array.isArray(request) ||
    ((request as any).valueVersion !== undefined && (request as any).valueVersion !== VALUE_VERSION)
  )
    return Promise.reject(
      new MoriaError('UNSUPPORTED_VALUE_VERSION', 'Unsupported worker value version'),
    );
  let input: string;
  try {
    input = canonicalJson({ ...request, valueVersion: VALUE_VERSION });
  } catch {
    return Promise.reject(
      new MoriaError('VALUE_INVALID', 'Worker request must contain JSON wire values'),
    );
  }
  const python =
    process.env.MORIA_PYTHON ??
    resolve(process.platform === 'win32' ? '.venv/Scripts/python.exe' : '.venv/bin/python');
  return new Promise((resolveResult, reject) => {
    const child = spawn(python, ['workers/iceberg/worker.py'], {
      stdio: ['pipe', 'pipe', 'pipe'],
      env: { ...process.env, PYTHONUTF8: '1' },
    });
    let stdout = '',
      stderr = '';
    const timer = setTimeout(() => {
      child.kill();
      reject(new Error('Iceberg worker timeout'));
    }, 60000);
    child.stdout.on('data', (c) => (stdout += c));
    child.stderr.on('data', (c) => (stderr += c));
    child.on('error', (e) => {
      clearTimeout(timer);
      reject(e);
    });
    child.on('close', (code) => {
      clearTimeout(timer);
      if (code !== 0) return reject(new Error(`Iceberg worker failed: ${stderr}`));
      try {
        const result = JSON.parse(stdout);
        if (result.valueVersion !== VALUE_VERSION)
          throw new MoriaError('UNSUPPORTED_VALUE_VERSION', 'Unsupported worker value version');
        resolveResult(result);
      } catch (e) {
        reject(e);
      }
    });
    child.stdin.on('error', () => {});
    child.stdin.end(input);
  });
}
