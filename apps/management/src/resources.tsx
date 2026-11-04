import React, { useEffect, useRef, useState } from 'react';
import { canonicalJson } from '../../../packages/contracts/values.js';
import type { Resource } from '../../../packages/contracts/types.js';
import { platform } from './native-client.js';
import { ApiError } from '../../../packages/platform/src/transport.js';
import { FunctionInvocations } from './functions.js';
export function isResource(value: unknown): value is Resource {
  return (
    !!value &&
    typeof value === 'object' &&
    'kind' in value &&
    typeof value.kind === 'string' &&
    'metadata' in value &&
    !!value.metadata &&
    typeof value.metadata === 'object' &&
    'spec' in value &&
    !!value.spec &&
    typeof value.spec === 'object'
  );
}
type Operation = { label: string; path: string; body: unknown; write?: boolean };
function operations(kind: string, id: string): Operation[] {
  const resource = encodeURIComponent(id);
  switch (kind) {
    case 'ObjectType':
      return [
        {
          label: 'Query objects',
          path: `/api/v1/objects/${resource}/query`,
          body: { pageSize: 25 },
        },
        {
          label: 'Aggregate objects',
          path: `/api/v1/objects/${resource}/aggregate`,
          body: {
            groupBy: [],
            metrics: { count: { op: 'count' } },
            budget: { scanRows: 10000, groups: 100, responseBytes: 1048576, timeMs: 5000 },
          },
        },
      ];
    case 'Action':
      return [
        {
          label: 'Invoke action',
          path: `/api/v1/actions/${resource}/invoke`,
          body: { input: {} },
          write: true,
        },
      ];
    case 'LinkType':
      return [
        {
          label: 'Traverse links',
          path: `/api/v1/links/${resource}/traverse`,
          body: {
            uid: '',
            direction: 'out',
            fanout: 25,
            budget: { scanRows: 1000, responseBytes: 1048576, timeMs: 5000 },
          },
        },
      ];
    case 'Model':
      return [
        {
          label: 'Inspect capabilities',
          path: `/api/v1/models/${resource}/capabilities`,
          body: {},
        },
        {
          label: 'Verify capabilities',
          path: `/api/v1/models/${resource}/probe`,
          body: {},
          write: true,
        },
        {
          label: 'Generate text',
          path: `/api/v1/models/${resource}/chat`,
          body: { messages: [{ role: 'user', content: '' }], maxTokens: 128 },
          write: true,
        },
        {
          label: 'Embed text',
          path: `/api/v1/models/${resource}/embed`,
          body: { input: [''] },
          write: true,
        },
      ];
    case 'RetrievalCollection':
      return [
        { label: 'Inspect visibility', path: `/api/v1/search/${resource}/visibility`, body: {} },
        {
          label: 'Search',
          path: `/api/v1/search/${resource}/query`,
          body: { text: '', topK: 10 },
          write: true,
        },
        {
          label: 'Rebuild search',
          path: `/api/v1/search/${resource}/rebuild`,
          body: {},
          write: true,
        },
      ];
    default:
      return [];
  }
}
function Execution({ resourceId, kind }: { resourceId: string; kind: string }) {
  const [input, setInput] = useState('{}'),
    [result, setResult] = useState<unknown>(),
    [events, setEvents] = useState<unknown>(),
    [error, setError] = useState(''),
    [unavailable, setUnavailable] = useState(false),
    [busy, setBusy] = useState(false);
  const current = useRef<string | undefined>(undefined),
    request = useRef<{ body: string; key: string } | undefined>(undefined);
  const client =
    kind === 'Agent'
      ? platform.agents
      : kind === 'EvaluationSuite'
        ? platform.evaluations
        : platform.pipelines;
  useEffect(() => {
    setResult(undefined);
    setEvents(undefined);
    setError('');
    setUnavailable(false);
    request.current = undefined;
    current.current = undefined;
    let disposed = false,
      reading = false;
    const timer = setInterval(() => {
      const id = current.current;
      if (!id || reading || document.visibilityState !== 'visible') return;
      reading = true;
      void Promise.all([client.get(id), client.events(id)])
        .then(([value, history]) => {
          if (!disposed) {
            setResult(value);
            setEvents(history);
            setError('');
            setUnavailable(false);
          }
        })
        .catch((error) => {
          if (!disposed) {
            setResult(undefined);
            setEvents(undefined);
            const pending =
              error instanceof ApiError &&
              error.status === 409 &&
              error.code === 'FUNCTION_OBSERVATION_INCOMPLETE';
            setUnavailable(pending);
            setError(pending ? '' : error instanceof Error ? error.message : 'Inspection failed');
          }
        })
        .finally(() => (reading = false));
    }, 1000);
    return () => {
      disposed = true;
      clearInterval(timer);
      current.current = undefined;
    };
  }, [client, resourceId]);
  return (
    <section aria-label="Resource execution">
      <h4>
        {kind === 'Agent'
          ? 'Invoke agent'
          : kind === 'EvaluationSuite'
            ? 'Run evaluation'
            : 'Run pipeline'}
      </h4>
      {kind === 'Agent' && (
        <label>
          Input JSON
          <textarea
            rows={6}
            disabled={busy}
            value={input}
            onChange={(event) => setInput(event.target.value)}
          />
        </label>
      )}
      <button
        disabled={busy}
        onClick={() => {
          setBusy(true);
          void (async () => {
            try {
              const value = kind === 'Agent' ? JSON.parse(input) : {};
              const body = canonicalJson(value);
              if (request.current?.body !== body)
                request.current = { body, key: crypto.randomUUID() };
              const settings = { idempotencyKey: request.current!.key, wait: false };
              const accepted =
                kind === 'Agent'
                  ? await platform.agents.invoke(resourceId, value, settings)
                  : kind === 'EvaluationSuite'
                    ? await platform.evaluations.invoke(resourceId, settings)
                    : await platform.pipelines.invoke(resourceId, settings);
              current.current = accepted.id;
              setResult(accepted);
              setEvents(undefined);
              setError('');
              setUnavailable(false);
            } catch (error) {
              current.current = undefined;
              setUnavailable(false);
              setResult(undefined);
              setEvents(undefined);
              setError(error instanceof Error ? error.message : 'Invocation failed');
            } finally {
              setBusy(false);
            }
          })();
        }}
      >
        Run / retry same request
      </button>
      <button disabled={busy} onClick={() => (request.current = undefined)}>
        New request
      </button>
      {current.current && (
        <button
          disabled={busy}
          onClick={() => {
            setBusy(true);
            void client
              .cancel(current.current!)
              .then((receipt) => {
                setResult(receipt);
                setEvents(undefined);
                setError('');
              })
              .catch((error) => {
                setResult(undefined);
                setEvents(undefined);
                setError(error.message);
              })
              .finally(() => setBusy(false));
          }}
        >
          Cancel execution
        </button>
      )}
      {unavailable && <p role="status">Execution accepted. Details are currently unavailable.</p>}
      {error && (
        <p role="alert" className="error">
          {error}
        </p>
      )}
      {result !== undefined && (
        <pre aria-label="Execution result">{JSON.stringify(result, null, 2)}</pre>
      )}
      {events !== undefined && (
        <pre aria-label="Execution events">{JSON.stringify(events, null, 2)}</pre>
      )}
    </section>
  );
}
function AutomationAudit({ resourceId }: { resourceId: string }) {
  const [value, setValue] = useState<{ firings: unknown[]; nextAfter: string | null }>(),
    [error, setError] = useState(''),
    [busy, setBusy] = useState(false);
  const read = (after?: string) => {
    setBusy(true);
    void platform.transport
      .request<{ firings: unknown[]; nextAfter: string | null }>(
        `/api/v1/automations/${encodeURIComponent(resourceId)}/firings${after ? '?' + new URLSearchParams({ after }) : ''}`,
        'GET',
      )
      .then((value) => {
        setValue(value);
        setError('');
      })
      .catch((error) => {
        setValue(undefined);
        setError(error instanceof Error ? error.message : 'Audit unavailable');
      })
      .finally(() => setBusy(false));
  };
  return (
    <section aria-label="Automation firing audit">
      <h4>Scheduled firings</h4>
      <p>
        Current and retained classifications protect each audit page. This view does not expose
        child inputs or results.
      </p>
      <button disabled={busy} onClick={() => read()}>
        Refresh firing audit
      </button>
      {value?.nextAfter && (
        <button disabled={busy} onClick={() => read(value.nextAfter!)}>
          Next firing page
        </button>
      )}
      {error && (
        <p role="alert" className="error">
          {error}
        </p>
      )}
      {value && <pre>{JSON.stringify(value, null, 2)}</pre>}
    </section>
  );
}
export function ResourceInspector({
  resourceId,
  resource,
}: {
  resourceId: string;
  resource: Resource;
}) {
  const available = operations(resource.kind, resourceId),
    [choice, setChoice] = useState(0),
    [input, setInput] = useState(() =>
      available[0] ? JSON.stringify(available[0].body, null, 2) : '{}',
    ),
    [result, setResult] = useState<unknown>(),
    [error, setError] = useState(''),
    [busy, setBusy] = useState(false);
  const pending = useRef<{ body: string; key: string } | undefined>(undefined);
  return (
    <section className="panel apps-panel" aria-label="Published resource">
      <h3>
        {resource.metadata.name} <small>{resource.kind}</small>
      </h3>
      <p>
        <code>{resourceId}</code>
      </p>
      <details>
        <summary>Published definition</summary>
        <pre>{JSON.stringify(resource, null, 2)}</pre>
      </details>
      {resource.kind === 'Function' ? (
        <FunctionInvocations resourceId={resourceId} />
      ) : resource.kind === 'Automation' ? (
        <AutomationAudit resourceId={resourceId} />
      ) : ['Agent', 'EvaluationSuite', 'Transform', 'Connection'].includes(resource.kind) ? (
        <Execution resourceId={resourceId} kind={resource.kind} />
      ) : available.length > 0 ? (
        <>
          <label>
            Operation{' '}
            <select
              value={choice}
              disabled={busy}
              onChange={(event) => {
                const next = Number(event.target.value);
                setChoice(next);
                setInput(JSON.stringify(available[next].body, null, 2));
                setResult(undefined);
                setError('');
              }}
            >
              {available.map((operation, index) => (
                <option key={operation.path} value={index}>
                  {operation.label}
                </option>
              ))}
            </select>
          </label>
          <label>
            Request JSON
            <textarea
              rows={10}
              disabled={busy}
              value={input}
              onChange={(event) => setInput(event.target.value)}
            />
          </label>
          <p className="hint">
            Integer and decimal values use exact JSON strings. The current server contract validates
            this request.
          </p>
          <button
            disabled={busy}
            onClick={() => {
              setBusy(true);
              void (async () => {
                try {
                  const value = JSON.parse(input),
                    operation = available[choice],
                    body = operation.path + '\n' + canonicalJson(value);
                  if (pending.current?.body !== body)
                    pending.current = { body, key: crypto.randomUUID() };
                  const result = await platform.transport.request(
                    operation.path,
                    'POST',
                    value,
                    operation.write ? { idempotencyKey: pending.current!.key } : {},
                  );
                  setResult(result);
                  setError('');
                } catch (error) {
                  setResult(undefined);
                  setError(error instanceof Error ? error.message : 'Request failed');
                } finally {
                  setBusy(false);
                }
              })();
            }}
          >
            {available[choice].label} / retry
          </button>
          {available[choice].write && (
            <button disabled={busy} onClick={() => (pending.current = undefined)}>
              New request
            </button>
          )}
          {error && (
            <p role="alert" className="error">
              {error}
            </p>
          )}
          {result !== undefined && (
            <pre aria-label="Resource result">{JSON.stringify(result, null, 2)}</pre>
          )}
        </>
      ) : (
        <p>
          This published definition is available to authorized dependent resources and source
          configuration.
        </p>
      )}
    </section>
  );
}
export function Resources() {
  const [id, setId] = useState(''),
    [value, setValue] = useState<{ resourceId: string; resource: Resource }>(),
    [error, setError] = useState(''),
    [busy, setBusy] = useState(false);
  return (
    <>
      <section className="panel apps-panel">
        <h2>Open a published resource</h2>
        <p>
          Use a resource ID from Files and sources. Reading its definition grants no permission to
          query or invoke it.
        </p>
        <form
          onSubmit={(event) => {
            event.preventDefault();
            setBusy(true);
            void platform.filesystem
              .resource(id.trim())
              .then((value) => {
                if (!isResource(value.resource)) throw new Error('Unsupported resource definition');
                setValue({ resourceId: value.resourceId, resource: value.resource });
                setError('');
              })
              .catch((error) => {
                setValue(undefined);
                setError(error instanceof Error ? error.message : 'Resource is unavailable');
              })
              .finally(() => setBusy(false));
          }}
        >
          <label>
            Resource ID{' '}
            <input required value={id} onChange={(event) => setId(event.target.value)} />
          </label>
          <button disabled={busy}>Open resource</button>
        </form>
        {error && (
          <p role="alert" className="error">
            {error}
          </p>
        )}
      </section>
      {value && <ResourceInspector key={value.resourceId} {...value} />}
    </>
  );
}
