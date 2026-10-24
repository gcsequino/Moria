import { spawnSync } from 'node:child_process';
import { resolve } from 'node:path';

const python = process.env.MORIA_PYTHON ?? resolve('.venv/bin/python');
for (const args of [
  ['-m', 'black', '--check', 'workers', 'benchmarks', 'docs/spec/schemas', 'scripts'],
  ['docs/spec/schemas/validate.py', '--check'],
  ['scripts/validate-production-workloads.py', '--self-test'],
  ['-m', 'unittest', 'benchmarks.test_workloads'],
]) {
  const result = spawnSync(python, args, {
    stdio: 'inherit',
    env: { ...process.env, PYTHONUTF8: '1' },
  });
  if (result.error) console.error(`Cannot run ${python}: ${result.error.message}`);
  if (result.status !== 0) process.exit(result.status ?? 1);
}
