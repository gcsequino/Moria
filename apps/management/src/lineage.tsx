import React, { useEffect, useState } from 'react';
import { sdk } from './client.js';

type LineageData = Awaited<ReturnType<typeof sdk.endpoints.lineage>>;
type Perspective = 'all' | 'data' | 'workflow' | 'executions';
const perspectives: { id: Perspective; label: string; kinds?: string[]; description: string }[] = [
  {
    id: 'all',
    label: 'All',
    description:
      'See recorded data provenance, application declarations and execution events together. Select a resource to follow its connections.',
  },
  {
    id: 'data',
    label: 'Data',
    kinds: [
      'dataset',
      'snapshot',
      'object-generation',
      'object-type',
      'index-stage',
      'weather-publication',
      'weather-area',
      'source-response',
      'publication',
      'snow-sync',
      'weather-source',
      'area-forecast',
    ],
    description:
      'Trace published data through snapshots and object generations. Weather publications are stored in PostgreSQL; they are not Iceberg snapshots.',
  },
  {
    id: 'workflow',
    label: 'Workflow',
    kinds: ['application', 'object-type', 'action', 'endpoint', 'link'],
    description:
      'Inspect the object types, actions and endpoints declared by active application releases. Declarations describe the app contract; they do not prove runtime usage or grant permission.',
  },
  {
    id: 'executions',
    label: 'Executions',
    kinds: ['job', 'attempt', 'publication', 'snow-sync', 'weather-publication'],
    description:
      'Follow persisted jobs, attempts and their recorded publications. These execution events are separate from application workflow declarations.',
  },
];
function ResourceDetail({ value }: { value: string }) {
  try {
    const parsed: unknown = JSON.parse(value);
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      return (
        <dl className="detail-grid">
          {Object.entries(parsed).map(([key, entry]) => (
            <div key={key}>
              <dt>{key.replace(/_/g, ' ').replace(/([a-z])([A-Z])/g, '$1 $2')}</dt>
              <dd>
                {entry === null
                  ? 'Not recorded'
                  : typeof entry === 'object'
                    ? JSON.stringify(entry)
                    : String(entry)}
              </dd>
            </div>
          ))}
        </dl>
      );
    }
  } catch {
    /* Plain text detail is valid too. */
  }
  return <p className="resource-detail">{value}</p>;
}
export function Lineage() {
  const [data, setData] = useState<LineageData>();
  const [selected, setSelected] = useState<string>();
  const [error, setError] = useState('');
  const [search, setSearch] = useState('');
  const [version, setVersion] = useState(0);
  const [perspective, setPerspective] = useState<Perspective>('all');
  useEffect(() => {
    const controller = new AbortController();
    void sdk.endpoints
      .lineage({ signal: controller.signal })
      .then((result) => {
        if (controller.signal.aborted) return;
        setData(result);
        setError('');
        setSelected((current) =>
          result.nodes.some((node) => node.id === current) ? current : result.nodes[0]?.id,
        );
      })
      .catch((e: unknown) => {
        if (controller.signal.aborted) return;
        setError(e instanceof Error ? e.message : 'Could not load lineage.');
        setData(undefined);
      });
    return () => controller.abort();
  }, [version]);
  const allNodes = data?.nodes ?? [];
  const activePerspective = perspectives.find((item) => item.id === perspective)!;
  const nodes = allNodes.filter(
    (node) => !activePerspective.kinds || activePerspective.kinds.includes(node.kind),
  );
  const nodeIds = new Set(nodes.map((node) => node.id));
  const edges = (data?.edges ?? []).filter(
    (edge) => nodeIds.has(edge.from) && nodeIds.has(edge.to),
  );
  const current = nodes.find((node) => node.id === selected) ?? nodes[0];
  const shown = nodes.filter((node) =>
    `${node.label} ${node.kind} ${node.id}`.toLowerCase().includes(search.toLowerCase()),
  );
  const kinds = [...new Set(shown.map((node) => node.kind))];
  function neighbor(id: string, edgeLabel: string, key: string) {
    const node = nodes.find((candidate) => candidate.id === id);
    return (
      <button
        key={key}
        className="lineage-neighbor"
        onClick={() => setSelected(id)}
        disabled={!node}
      >
        <small>{edgeLabel}</small>
        <strong>{node?.label ?? id}</strong>
        <span>{node?.kind}</span>
      </button>
    );
  }
  const incoming = edges.filter((edge) => edge.to === current?.id);
  const outgoing = edges.filter((edge) => edge.from === current?.id);
  const omittedConnections = (data?.edges ?? []).filter(
    (edge) =>
      (edge.from === current?.id || edge.to === current?.id) &&
      (!nodeIds.has(edge.from) || !nodeIds.has(edge.to)),
  ).length;
  return (
    <>
      <div className="platform-toolbar">
        <span>Recorded provenance and declared application dependencies</span>
        <button className="quiet" onClick={() => setVersion((value) => value + 1)}>
          Refresh lineage
        </button>
      </div>
      <section className="lineage-perspectives" aria-label="Lineage perspective">
        <div className="perspective-tabs">
          {perspectives.map((item) => (
            <button
              key={item.id}
              className={item.id === perspective ? 'selected' : ''}
              aria-pressed={item.id === perspective}
              onClick={() => {
                setPerspective(item.id);
                setSearch('');
              }}
            >
              {item.label}
              <span>
                {allNodes.filter((node) => !item.kinds || item.kinds.includes(node.kind)).length}
              </span>
            </button>
          ))}
        </div>
        <p>{activePerspective.description}</p>
      </section>
      {error && (
        <p className="operation-error" role="alert">
          {error}
        </p>
      )}
      <div className="lineage-layout">
        <section className="panel lineage-resources">
          <label htmlFor="resource-search">Find a resource</label>
          <input
            id="resource-search"
            type="search"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Name, kind or ID"
          />
          {!data && !error && <p>Loading recorded lineage…</p>}
          {data && !shown.length && <p>No matching resources.</p>}
          {kinds.map((kind) => (
            <div key={kind} className="resource-group">
              <h2>{kind}</h2>
              {shown
                .filter((node) => node.kind === kind)
                .map((node) => (
                  <button
                    key={node.id}
                    className={`resource-link ${current?.id === node.id ? 'selected' : ''}`}
                    onClick={() => setSelected(node.id)}
                    aria-pressed={current?.id === node.id}
                  >
                    <strong>{node.label}</strong>
                    {node.status && <small>{node.status}</small>}
                  </button>
                ))}
            </div>
          ))}
        </section>
        <section className="panel lineage-focus">
          {current ? (
            <>
              <div className="eyebrow">{current.kind}</div>
              <h2>{current.label}</h2>
              <code className="resource-id">{current.id}</code>
              {current.status && (
                <p>
                  <span className="job-status">{current.status}</span>
                </p>
              )}
              <ResourceDetail value={current.detail} />
              {omittedConnections > 0 && (
                <p className="perspective-context">
                  {omittedConnections} connection(s) lead outside this perspective.{' '}
                  <button
                    className="quiet"
                    onClick={() => {
                      setSelected(current.id);
                      setPerspective('all');
                    }}
                  >
                    Show all connections
                  </button>
                </p>
              )}
              <div className="lineage-flow">
                <section>
                  <h3>Upstream</h3>
                  <p>Recorded inputs and dependencies</p>
                  {incoming.length ? (
                    incoming.map((edge, index) =>
                      neighbor(edge.from, edge.label, `${edge.from}-${index}`),
                    )
                  ) : (
                    <p className="lineage-empty">No upstream relationships in this view.</p>
                  )}
                </section>
                <div className="lineage-current">
                  <span aria-hidden="true">→</span>
                  <strong>{current.label}</strong>
                  <span aria-hidden="true">→</span>
                </div>
                <section>
                  <h3>Downstream</h3>
                  <p>Recorded consumers and outputs</p>
                  {outgoing.length ? (
                    outgoing.map((edge, index) =>
                      neighbor(edge.to, edge.label, `${edge.to}-${index}`),
                    )
                  ) : (
                    <p className="lineage-empty">No downstream relationships in this view.</p>
                  )}
                </section>
              </div>
            </>
          ) : (
            <div className="empty">
              <h3>Select a resource.</h3>
              <p>Inspect its recorded inputs, outputs and workflow relationships.</p>
            </div>
          )}
        </section>
      </div>
      {!!data?.limitations.length && (
        <section className="recovery-note">
          <h3>Coverage of this view</h3>
          <ul>
            {data.limitations.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </section>
      )}
    </>
  );
}
