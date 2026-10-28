import { closeSync } from 'node:fs';
import { once } from 'node:events';

const [mode, task] = process.argv.slice(2);
process.on('SIGTERM', () => {
  if (mode !== 'ignore-term') process.exit(0);
});
const alive = setInterval(() => {}, 1000);
if (mode === 'stall' || mode === 'ignore-term') {
  process.stdin.pause();
  process.stderr.write('ready\n');
} else if (mode === 'epipe') {
  closeSync(0);
} else if (mode === 'stderr') {
  process.stderr.write('sensitive fixture data'.repeat(4000));
} else if (mode === 'oversized') {
  const bytes =
    task === 'stage'
      ? 65 * 1024 * 1024
      : task === 'stream' || task === 'scoped-read'
        ? 5 * 1024 * 1024
        : 2048;
  for (let offset = 0; offset < bytes; offset += 65536) {
    if (!process.stdout.write('x'.repeat(Math.min(65536, bytes - offset))))
      await once(process.stdout, 'drain');
  }
} else {
  let pending = '',
    step = 0;
  const emit = (frame) => process.stdout.write(JSON.stringify(frame) + '\n');
  const finish = () => {
    clearInterval(alive);
    process.exitCode = 0;
    process.stdin.destroy();
  };
  const run = () => {
    if (mode === 'exit') process.exit(7);
    if (mode === 'invalid') process.exit(2);
    if (mode === 'signal') process.kill(process.pid, 'SIGUSR2');
    if (mode === 'utf8') {
      process.stdout.write(Buffer.from([255, 10]));
      finish();
      return;
    }
    if (mode === 'partial') {
      process.stdout.write('{');
      finish();
      return;
    }
    if (mode === 'malformed') {
      process.stdout.write('not-json\n');
      finish();
      return;
    }
    if (task === 'stage') {
      emit({ ok: true });
      finish();
    } else if (task === 'stream') {
      emit({ type: 'start', snapshotId: '1', tableUuid: 'fixture' });
      emit({
        type: 'batch',
        rows: mode === 'late-exit' ? [{ uid: '1' }, { uid: '2' }] : [{ uid: '1' }],
      });
      if (mode === 'late-exit') {
        setTimeout(() => process.exit(7), 25);
        return;
      }
      emit({ type: 'end', rows: '1' });
      finish();
    } else finish();
  };
  process.stdin.setEncoding('utf8');
  process.stdin.on('data', (chunk) => {
    if (task !== 'scoped-read') return;
    pending += chunk;
    while (pending.includes('\n')) {
      const end = pending.indexOf('\n');
      pending = pending.slice(end + 1);
      if (mode === 'exit-during-check') {
        emit({
          type: 'start',
          snapshotId: '1',
          tableUuid: 'fixture',
          schema: { type: 'struct', fields: [{ name: 'uid' }] },
        });
        setTimeout(() => process.exit(7), 25);
        return;
      }
      if (!['success', 'late-exit'].includes(mode)) {
        run();
        return;
      }
      if (step++ === 0)
        emit({
          type: 'start',
          snapshotId: '1',
          tableUuid: 'fixture',
          schema: { type: 'struct', fields: [{ name: 'uid' }] },
        });
      else if (step === 2) {
        emit({
          type: 'batch',
          rows: mode === 'late-exit' ? [{ uid: '1' }, { uid: '2' }] : [{ uid: '1' }],
        });
        if (mode === 'late-exit') setTimeout(() => process.exit(7), 25);
      } else if (step === 3) emit({ type: 'end', rows: '1' });
      else finish();
    }
  });
  process.stdin.on('end', () => {
    if (task !== 'scoped-read') run();
  });
}
