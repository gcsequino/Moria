import { createInterface } from 'node:readline';
import { pathToFileURL } from 'node:url';
import { format } from 'node:util';
import { validateRow, bindOntologyObjects } from './values.js';

const send = (value, flushed) =>
  process.stdout.write(JSON.stringify({ version: 1, ...value }) + '\n', flushed);
const pending = new Map();
let counter = 0,
  started = false,
  completed = false;
const cancellation = new AbortController();
process.on('SIGTERM', () => {
  cancellation.abort();
  if (completed) process.exit(1);
});
const log = (...values) => send({ type: 'log', message: format(...values) });
for (const method of ['log', 'info', 'warn', 'error', 'debug']) console[method] = log;
const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
lines.on('line', (line) => {
  const message = JSON.parse(line);
  if (message.version !== 1) throw new Error('Unsupported protocol');
  if (message.type === 'start' && !started) {
    started = true;
    void (async () => {
      try {
        if (message.valueVersion !== 1) throw new Error('Unsupported value version');
        const { default: invoke, createOsdk } = await import(pathToFileURL(process.argv[2]).href);
        if (typeof invoke !== 'function') throw new Error('Default function export required');
        const fetcher = async (path, options = {}) => {
          const id = String(++counter);
          const value = await new Promise((resolve, reject) => {
            pending.set(id, { resolve, reject });
            send({
              type: 'osdk',
              id,
              path: String(path),
              method: options.method ?? 'GET',
              input: options.body === undefined ? null : JSON.parse(String(options.body)),
            });
          });
          return new Response(JSON.stringify(value), {
            status: 200,
            headers: { 'content-type': 'application/json' },
          });
        };
        const platformFetch = async (path, options = {}) => {
          cancellation.signal.throwIfAborted();
          options.signal?.throwIfAborted();
          const headers = Object.fromEntries(new Headers(options.headers));
          const bytes =
            options.body === undefined
              ? null
              : Buffer.from(await new Response(options.body).arrayBuffer());
          if (bytes && bytes.length > 32768) throw new Error('Platform request exceeds bounds');
          const id = String(++counter);
          const response = await new Promise((resolve, reject) => {
            pending.set(id, { resolve, reject });
            send({
              type: 'http',
              id,
              path: String(path),
              method: options.method ?? 'GET',
              headers,
              body: bytes?.toString('base64') ?? null,
            });
          });
          return new Response(Buffer.from(response.body, 'base64'), {
            status: response.status,
            headers: response.headers,
          });
        };
        const osdk =
          typeof createOsdk === 'function' ? createOsdk({ fetch: platformFetch }) : undefined;
        const input = await bindOntologyObjects(
          validateRow(message.input, message.inputs, 'value'),
          { type: 'object', properties: message.inputs },
          (identity) => {
            const candidates = Object.values(osdk?.objects ?? {}).filter(
              (object) => object.objectType === identity.objectType,
            );
            if (candidates.length !== 1)
              throw new Error('Ontology object type is not bound by the generated SDK');
            return candidates[0].object(identity.uid);
          },
        );
        const value = await invoke(input, {
          invocationId: message.invocationId,
          valueVersion: message.valueVersion,
          signal: cancellation.signal,
          fetch: fetcher,
          osdk,
          platform: { fetch: platformFetch },
          effect: message.effect,
          log,
        });
        if (pending.size) throw new Error('Function returned with unfinished platform calls');
        completed = true;
        send({ type: 'result', value: validateRow(value, message.outputs, 'value') });
      } catch {
        // The supervisor drains protected callbacks before owning failure cleanup.
        send({ type: 'error' });
      }
    })();
  } else if (message.type === 'complete' && completed) process.exit(0);
  else if (
    ['osdkResult', 'osdkError', 'httpResult', 'httpError'].includes(message.type) &&
    pending.has(message.id)
  ) {
    const request = pending.get(message.id);
    pending.delete(message.id);
    if (['osdkResult', 'httpResult'].includes(message.type)) request.resolve(message.value);
    else request.reject(new Error(message.code));
  } else throw new Error('Invalid protocol');
});
