import assert from 'node:assert/strict';
import test from 'node:test';
import childProcess from 'node:child_process';
import { syncBuiltinESMExports } from 'node:module';
import { resolve } from 'node:path';
import { ProcessFailure, openPython, type PythonTask } from '../services/process/src/python.js';
import { iceberg } from '../workers/iceberg/stage.js';
import { icebergRows } from '../workers/iceberg/stream.js';
import { scopedRows } from '../workers/iceberg/scoped-read.js';
import { StorageError } from '../services/storage/src/locations.js';
import { validateMetadata } from '../services/catalog/src/storage-publication.js';
import { validateSnapshot } from '../services/catalog/src/snapshot-publication.js';

const spawn = childProcess.spawn;
const delay = globalThis.setTimeout;
function scoped(signal?: AbortSignal, large = false) {
  const bytes = Buffer.from(large ? 'x'.repeat(512 * 1024) : '{}');
  return scopedRows(
    {
      metadata: async () =>
        new ReadableStream({
          start(controller) {
            controller.enqueue(bytes);
            controller.close();
          },
        }),
      snapshotId: '1',
      tableUuid: 'fixture',
      objects: [],
      check: async () => {},
      read: async () => {
        throw new Error('Unexpected storage read');
      },
    },
    { signal },
  );
}
async function collect(rows: AsyncIterable<unknown>) {
  const result = [];
  for await (const row of rows) result.push(row);
  return result;
}
async function run(task: PythonTask, signal?: AbortSignal, large = false) {
  const bytes = Buffer.from(large ? 'x'.repeat(512 * 1024) : '{}');
  if (task === 'stage') return iceberg({ operation: 'stage', value: bytes.toString() }, { signal });
  if (task === 'stream')
    return collect(
      icebergRows({ metadataLocation: bytes.toString(), snapshotId: '1' }, { signal }),
    );
  if (task === 'scoped-read') return collect(scoped(signal, large));
  if (task === 'metadata') return validateMetadata(bytes, 'fixture', '1', signal);
  return validateSnapshot(
    bytes,
    [],
    'fixture',
    '1',
    'moria://warehouse/table',
    signal ?? new AbortController().signal,
  );
}

test(
  'one lifecycle contract exercises every production Python adapter',
  { timeout: 60000 },
  async (t) => {
    for (const task of ['stage', 'stream', 'scoped-read', 'metadata', 'snapshot'] as const) {
      for (const mode of [
        'success',
        'missing',
        'invalid',
        'launch-error',
        'exit',
        'signal',
        'partial',
        'malformed',
        'utf8',
        'oversized',
        'stderr',
        'stall',
        'ignore-term',
        'cancel',
        'aborted',
        'epipe',
      ] as const) {
        await t.test(`${task}: ${mode}`, async (t) => {
          const children: ReturnType<typeof spawn>[] = [];
          const controller = new AbortController();
          const selected = mode === 'cancel' ? 'ignore-term' : mode;
          const mock = t.mock.method(childProcess, 'spawn', ((
            _command: unknown,
            _args: unknown,
            options: Parameters<typeof spawn>[2],
          ) => {
            const child =
              mode === 'missing'
                ? spawn('/nonexistent/moria-python', [], options)
                : mode === 'launch-error'
                  ? spawn(
                      resolve('.venv/bin/python'),
                      ['-I', '/nonexistent/moria-validator.py'],
                      options,
                    )
                  : spawn(
                      process.execPath,
                      [resolve('tests/fixtures/lifecycle-child.mjs'), selected, task],
                      options,
                    );
            children.push(child);
            if (mode === 'cancel') child.stderr?.once('data', () => controller.abort());
            return child;
          }) as typeof spawn);
          syncBuiltinESMExports();
          const timerMock =
            mode === 'stall' || mode === 'ignore-term'
              ? t.mock.method(globalThis, 'setTimeout', ((
                  callback: (...args: unknown[]) => void,
                  ms?: number,
                  ...args: unknown[]
                ) =>
                  delay(
                    callback,
                    [30000, 60000, 1800000].includes(ms ?? 0) ? 300 : ms,
                    ...args,
                  )) as typeof setTimeout)
              : undefined;
          if (mode === 'aborted') controller.abort();
          const started = performance.now();
          try {
            if (mode === 'success') {
              const result = await run(task, controller.signal);
              if (task === 'stage') assert.deepEqual(result, { ok: true });
              else if (task === 'stream' || task === 'scoped-read')
                assert.deepEqual(result, [{ uid: '1' }]);
              else assert.equal(result, undefined);
            } else if (mode === 'aborted') {
              await assert.rejects(run(task, controller.signal), (error: unknown) =>
                error instanceof ProcessFailure
                  ? error.outcome.kind === 'cancelled'
                  : (error as { code?: string }).code === 'CANCELLED',
              );
              assert.equal(children.length, 0, 'already cancelled work must not spawn');
            } else {
              await assert.rejects(
                run(task, controller.signal, mode === 'epipe'),
                (error: unknown) => {
                  const failure = error instanceof ProcessFailure ? error : (error as Error).cause;
                  assert.ok(
                    failure instanceof ProcessFailure,
                    `missing safe process outcome: ${String(error)}`,
                  );
                  const outcome = failure.outcome;
                  assert.equal(outcome.task, task);
                  const expected =
                    mode === 'missing'
                      ? 'startup'
                      : mode === 'invalid'
                        ? task === 'metadata' || task === 'snapshot'
                          ? 'invalid-input'
                          : 'exit'
                        : mode === 'exit' || mode === 'launch-error'
                          ? 'exit'
                          : mode === 'signal'
                            ? 'signal'
                            : ['partial', 'malformed', 'utf8'].includes(mode)
                              ? 'protocol'
                              : ['oversized', 'stderr'].includes(mode)
                                ? 'limit'
                                : ['stall', 'ignore-term'].includes(mode)
                                  ? 'timeout'
                                  : mode === 'cancel'
                                    ? 'cancelled'
                                    : 'ipc';
                  assert.equal(outcome.kind, expected);
                  if (mode === 'exit') assert.equal(outcome.code, 7);
                  if (mode === 'invalid')
                    assert.equal(outcome.code, task === 'metadata' || task === 'snapshot' ? 65 : 2);
                  if (mode === 'launch-error') assert.equal(outcome.code, 2);
                  if (mode === 'signal') assert.equal(outcome.signal, 'SIGUSR2');
                  if (mode === 'ignore-term' || mode === 'cancel')
                    assert.equal(outcome.signal, 'SIGKILL');
                  assert.ok(outcome.stderrBytes <= 65537);
                  assert.doesNotMatch(
                    JSON.stringify(outcome) + failure.message + (error as Error).message,
                    /sensitive fixture data|Traceback|password/,
                  );
                  return true;
                },
              );
              assert.ok(
                performance.now() - started < 3000,
                'caller settlement and cleanup must be bounded',
              );
            }
            for (const child of children)
              if (child.pid) assert.throws(() => process.kill(child.pid!, 0), { code: 'ESRCH' });
          } finally {
            for (const child of children)
              if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
            timerMock?.mock.restore();
            mock.mock.restore();
            syncBuiltinESMExports();
          }
        });
      }
    }
  },
);

test('stream iterator return and throw reap a paused child and discard buffered rows', async (t) => {
  for (const task of ['stream', 'scoped-read'] as const) {
    for (const operation of ['return', 'throw'] as const) {
      await t.test(`${task}: ${operation}`, async (t) => {
        let pid: number | undefined;
        const mock = t.mock.method(childProcess, 'spawn', ((
          _command: unknown,
          _args: unknown,
          options: Parameters<typeof spawn>[2],
        ) => {
          const child = spawn(
            process.execPath,
            [resolve('tests/fixtures/lifecycle-child.mjs'), 'success', task],
            options,
          );
          pid = child.pid;
          return child;
        }) as typeof spawn);
        syncBuiltinESMExports();
        try {
          const rows =
            task === 'stream'
              ? icebergRows({ metadataLocation: 'fixture', snapshotId: '1' })
              : scoped();
          assert.deepEqual((await rows.next()).value, { uid: '1' });
          if (operation === 'return') await rows.return(undefined);
          else {
            const sentinel = new Error('consumer stopped');
            await assert.rejects(rows.throw(sentinel), (error) => error === sentinel);
          }
          assert.equal((await rows.next()).done, true);
          assert.ok(pid);
          assert.throws(() => process.kill(pid!, 0), { code: 'ESRCH' });
        } finally {
          mock.mock.restore();
          syncBuiltinESMExports();
        }
      });
    }
  }
});

test('the lifecycle owner rejects oversized input before writing and closes blocked stdin', async (t) => {
  const mock = t.mock.method(childProcess, 'spawn', ((
    _command: unknown,
    _args: unknown,
    options: Parameters<typeof spawn>[2],
  ) =>
    spawn(
      process.execPath,
      [resolve('tests/fixtures/lifecycle-child.mjs'), 'ignore-term', 'stream'],
      options,
    )) as typeof spawn);
  syncBuiltinESMExports();
  const worker = openPython('stream', { timeoutMs: 300 });
  try {
    await assert.rejects(
      worker.writeJson({ huge: 'x'.repeat(1024 * 1024 + 1) }),
      (error: unknown) => error instanceof ProcessFailure && error.outcome.kind === 'limit',
    );
    await worker.close();
    const outcome = await worker.completion;
    assert.equal(outcome.kind, 'limit');
    if (outcome.pid) assert.throws(() => process.kill(outcome.pid!, 0), { code: 'ESRCH' });
    const blocked = openPython('stage', { timeoutMs: 300 });
    try {
      await assert.rejects(
        blocked.write(Buffer.alloc(4 * 1024 * 1024)),
        (error: unknown) => error instanceof ProcessFailure && error.outcome.kind === 'timeout',
      );
      await blocked.close();
      const stopped = await blocked.completion;
      assert.equal(stopped.signal, 'SIGKILL');
      if (stopped.pid) assert.throws(() => process.kill(stopped.pid!, 0), { code: 'ESRCH' });
    } finally {
      await blocked.close();
    }
  } finally {
    await worker.close();
    mock.mock.restore();
    syncBuiltinESMExports();
  }
});

test('an abnormal exit denies buffered rows before stdio cleanup completes', async (t) => {
  for (const task of ['stream', 'scoped-read'] as const) {
    await t.test(task, async (t) => {
      let exited!: Promise<void>;
      const mock = t.mock.method(childProcess, 'spawn', ((
        _command: unknown,
        _args: unknown,
        options: Parameters<typeof spawn>[2],
      ) => {
        const child = spawn(
          process.execPath,
          [resolve('tests/fixtures/lifecycle-child.mjs'), 'late-exit', task],
          options,
        );
        exited = new Promise<void>((resolve) => child.once('exit', () => resolve()));
        return child;
      }) as typeof spawn);
      syncBuiltinESMExports();
      const rows =
        task === 'stream'
          ? icebergRows({ metadataLocation: 'fixture', snapshotId: '1' })
          : scoped();
      try {
        assert.deepEqual((await rows.next()).value, { uid: '1' });
        await exited;
        await assert.rejects(rows.next(), (error: unknown) => {
          const failure = (error as Error).cause;
          assert.ok(failure instanceof ProcessFailure);
          assert.equal(failure.outcome.kind, 'exit');
          assert.equal(failure.outcome.code, 7);
          return true;
        });
      } finally {
        await rows.return(undefined);
        mock.mock.restore();
        syncBuiltinESMExports();
      }
    });
  }
});

test('a child crash during a contended authority check retains its native outcome', async (t) => {
  let started = false;
  const mock = t.mock.method(childProcess, 'spawn', ((
    _command: unknown,
    _args: unknown,
    options: Parameters<typeof spawn>[2],
  ) => {
    started = true;
    return spawn(
      process.execPath,
      [resolve('tests/fixtures/lifecycle-child.mjs'), 'exit-during-check', 'scoped-read'],
      options,
    );
  }) as typeof spawn);
  syncBuiltinESMExports();
  const rows = scopedRows({
    metadata: async () =>
      new ReadableStream({
        start(controller) {
          controller.enqueue(Buffer.from('{}'));
          controller.close();
        },
      }),
    snapshotId: '1',
    tableUuid: 'fixture',
    objects: [],
    check: async (signal) => {
      if (started)
        await new Promise<void>((_resolve, reject) =>
          signal.addEventListener('abort', () => reject(new StorageError('CANCELLED')), {
            once: true,
          }),
        );
    },
    read: async () => {
      throw new Error('Unexpected read');
    },
  });
  try {
    await assert.rejects(rows.next(), (error: unknown) => {
      const failure = (error as Error).cause;
      assert.ok(failure instanceof ProcessFailure);
      assert.equal(failure.outcome.kind, 'exit');
      assert.equal(failure.outcome.code, 7);
      return true;
    });
  } finally {
    await rows.return(undefined);
    mock.mock.restore();
    syncBuiltinESMExports();
  }
});
