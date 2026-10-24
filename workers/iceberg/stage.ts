import { spawn } from 'node:child_process';
import { resolve } from 'node:path';

export function iceberg(request: unknown): Promise<any> {
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
        resolveResult(JSON.parse(stdout));
      } catch (e) {
        reject(e);
      }
    });
    child.stdin.on('error', () => {});
    child.stdin.end(JSON.stringify(request));
  });
}
