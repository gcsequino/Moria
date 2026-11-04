import React, { useEffect, useRef, useState } from 'react';
import { platform } from './native-client.js';
import { ApiError } from '../../../packages/platform/src/transport.js';
import type { FunctionInvocation, FunctionEvent } from '../../../packages/contracts/functions.js';

export function FunctionInvocations({ resourceId }: { resourceId: string }) {
  const [input, setInput] = useState('{}'),
    [invocation, setInvocation] = useState<FunctionInvocation>(),
    [events, setEvents] = useState<FunctionEvent[]>([]),
    [message, setMessage] = useState(''),
    [busy, setBusy] = useState(false);
  const request = useRef<{ input: string; key: string } | undefined>(undefined),
    activeId = useRef<string | undefined>(undefined);
  useEffect(() => {
    setInvocation(undefined);
    setEvents([]);
    setMessage('');
    request.current = undefined;
    let stopped = false,
      running = false;
    const refresh = async () => {
      const id = activeId.current;
      if (!id || running) return;
      running = true;
      try {
        const current = await platform.functions.get(id),
          page = await platform.functions.events(id);
        if (page.events.length === 100)
          page.events.push(
            ...(await platform.functions.events(id, page.events.at(-1)!.sequence)).events,
          );
        if (!stopped) {
          setInvocation(current);
          setEvents(page.events);
          setMessage('');
        }
      } catch (error) {
        if (!stopped) {
          setInvocation(undefined);
          setEvents([]);
          const pending =
            error instanceof ApiError &&
            error.status === 409 &&
            error.code === 'FUNCTION_OBSERVATION_INCOMPLETE';
          setMessage(
            pending
              ? 'Invocation accepted. Details are currently unavailable.'
              : error instanceof Error
                ? error.message
                : 'Inspection failed',
          );
        }
      } finally {
        running = false;
      }
    };
    const timer = setInterval(() => void refresh(), 1000);
    return () => {
      stopped = true;
      clearInterval(timer);
      activeId.current = undefined;
    };
  }, [resourceId]);
  return (
    <section aria-label="Function invocation">
      <h4>Invoke function</h4>
      <label>
        Input JSON
        <textarea
          value={input}
          disabled={busy}
          onChange={(event) => setInput(event.target.value)}
          rows={8}
        />
      </label>
      <button
        disabled={busy}
        onClick={() => {
          setBusy(true);
          void (async () => {
            try {
              const payload = JSON.parse(input);
              if (request.current?.input !== input)
                request.current = { input, key: crypto.randomUUID() };
              const current = await platform.functions.invoke(resourceId, payload, {
                idempotencyKey: request.current!.key,
                wait: false,
              });
              activeId.current = current.id;
              setInvocation(current);
              setEvents([]);
              setMessage('');
            } catch (error) {
              activeId.current = undefined;
              setInvocation(undefined);
              setEvents([]);
              setMessage(error instanceof Error ? error.message : 'Invocation failed');
            } finally {
              setBusy(false);
            }
          })();
        }}
      >
        Invoke / retry same request
      </button>
      <button
        disabled={busy}
        onClick={() => {
          request.current = undefined;
          setMessage('The next invocation will use a new request key.');
        }}
      >
        New request
      </button>
      {activeId.current && (
        <button
          disabled={busy}
          onClick={() => {
            setBusy(true);
            void platform.functions
              .cancel(activeId.current!)
              .then(() => setMessage('Cancellation requested.'))
              .catch((error) => {
                setInvocation(undefined);
                setEvents([]);
                setMessage(error.message);
              })
              .finally(() => setBusy(false));
          }}
        >
          Cancel invocation
        </button>
      )}
      {invocation && (
        <>
          <p>
            Invocation {invocation.id}: {invocation.status}
          </p>
          <pre>{JSON.stringify(invocation, null, 2)}</pre>
          <pre aria-label="Invocation events">
            {events.map((event) => `${event.sequence} ${event.type}: ${event.message}`).join('\n')}
          </pre>
        </>
      )}
      <p role="status">{message}</p>
    </section>
  );
}
