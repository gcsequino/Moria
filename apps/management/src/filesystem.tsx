import React, { useEffect, useRef, useState } from 'react';
import { platform } from './native-client.js';
import type {
  FilesystemEntry,
  SourcePlan,
  SourceRuntimeStatus,
} from '../../../packages/contracts/filesystem.js';
import { ResourceInspector, isResource } from './resources.js';
import type { Resource } from '../../../packages/contracts/types.js';

const fs = platform.filesystem;
type Role = Awaited<ReturnType<typeof fs.role>>;
export function Filesystem() {
  const [directory, setDirectory] = useState<FilesystemEntry>();
  const [entries, setEntries] = useState<FilesystemEntry[]>([]);
  const [cursor, setCursor] = useState<string>();
  const [selected, setSelected] = useState<FilesystemEntry>();
  const [role, setRole] = useState<Role>(null);
  const [text, setText] = useState('');
  const [name, setName] = useState('');
  const [path, setPath] = useState('/');
  const [history, setHistory] = useState<FilesystemEntry[]>([]);
  const [plan, setPlan] = useState<SourcePlan>();
  const [runtime, setRuntime] = useState<SourceRuntimeStatus>();
  const [notice, setNotice] = useState('Loading your files…');
  const [busy, setBusy] = useState(false);
  const [policy, setPolicy] = useState<{ revision: string; text: string }>();
  const [resolved, setResolved] = useState<{ resourceId: string; resource: Resource }>();
  const gate = useRef(false),
    alive = useRef(true);
  async function run(work: () => Promise<void>) {
    if (gate.current) return;
    gate.current = true;
    setBusy(true);
    try {
      await work();
    } catch (error) {
      if (alive.current) {
        setEntries([]);
        setDirectory(undefined);
        setCursor(undefined);
        setSelected(undefined);
        setText('');
        setHistory([]);
        setResolved(undefined);
        setPlan(undefined);
        setRuntime(undefined);
        setPolicy(undefined);
        setNotice(error instanceof Error ? error.message : 'Operation failed');
      }
    } finally {
      gate.current = false;
      if (alive.current) setBusy(false);
    }
  }
  async function open(entry: FilesystemEntry, append = false) {
    const page = await fs.list(entry.id, append ? cursor : undefined);
    const currentRole = await fs.role(entry.id);
    if (!alive.current) return;
    setDirectory(await fs.get(entry.id));
    setEntries((previous) => (append ? [...previous, ...page.entries] : page.entries));
    setCursor(page.nextCursor);
    setRole(currentRole);
    setSelected(undefined);
    setResolved(undefined);
    setHistory([]);
    setPlan(undefined);
    setRuntime(undefined);
    setPolicy(undefined);
    setNotice('Files are filtered by your current access.');
  }
  useEffect(() => {
    alive.current = true;
    void run(async () => {
      const home = await fs.home();
      await open(home.home);
    });
    return () => {
      alive.current = false;
    };
  }, []);
  async function upload(bytes: Uint8Array, filename: string, current?: FilesystemEntry) {
    if (!directory) return;
    if (bytes.length > 64 * 1024 * 1024) throw new Error('Files must be at most 64 MiB.');
    const digest =
      'sha256:' +
      Array.from(
        new Uint8Array(await crypto.subtle.digest('SHA-256', new Uint8Array(bytes))),
        (byte) => byte.toString(16).padStart(2, '0'),
      ).join('');
    const content = {
      digest,
      size: bytes.length,
      mediaType:
        current?.content?.mediaType ??
        (filename.endsWith('.json')
          ? 'application/json'
          : /\.(?:ts|tsx|js|md|py|scala|txt|toml|yml|yaml|sql)$/.test(filename)
            ? 'text/plain'
            : 'application/octet-stream'),
    };
    await fs.writeFile(
      current
        ? { op: 'write', id: current.id, expectedRevision: current.revision, content }
        : {
            op: 'create',
            parentId: directory.id,
            expectedParentRevision: directory.revision,
            name: filename,
            content,
          },
      bytes,
      { idempotencyKey: crypto.randomUUID() },
    );
    await open(directory);
    setNotice(`Saved ${filename}.`);
  }
  async function inspect(entry: FilesystemEntry) {
    const versions = await fs.history(entry.id);
    let value = '';
    if (
      entry.kind === 'file' &&
      entry.content &&
      entry.content.size <= 1024 * 1024 &&
      /^(text\/|application\/json)/.test(entry.content.mediaType)
    )
      value = await (await fs.readFile(entry.id, entry.revision)).text();
    if (!alive.current) return;
    setSelected(entry);
    setText(value);
    setHistory(versions.entries);
  }
  return (
    <section className="panel apps-panel filesystem-panel" aria-label="Filesystem" aria-busy={busy}>
      <div className="toolbar">
        <button disabled={busy} onClick={() => void run(async () => open((await fs.home()).home))}>
          My home
        </button>
        <button disabled={busy} onClick={() => void run(async () => open(await fs.resolve('/')))}>
          Root
        </button>
        <button
          disabled={busy || !directory?.parentId}
          onClick={() => void run(async () => open(await fs.get(directory!.parentId!)))}
        >
          Parent
        </button>
        <button
          disabled={busy || !directory}
          onClick={() => void run(async () => open(directory!))}
        >
          Refresh
        </button>
      </div>
      <form
        onSubmit={(event) => {
          event.preventDefault();
          void run(async () => open(await fs.resolve(path)));
        }}
      >
        <label>
          Open path <input value={path} onChange={(event) => setPath(event.target.value)} />
        </label>{' '}
        <button disabled={busy}>Open</button>
      </form>
      <p role="status" aria-live="polite">
        {notice}
      </p>
      {directory && (
        <>
          <h2>
            {directory.name || '/'}{' '}
            <small>
              · revision {directory.revision}
              {role ? ` · ${role.kind}` : ''}
            </small>
          </h2>
          <code>{directory.id}</code>
          {!role && directory.projectId && !directory.environmentId && (
            <div className="toolbar">
              {(['ontology', 'repository'] as const).map((kind) => (
                <button
                  key={kind}
                  disabled={busy}
                  onClick={() =>
                    void run(async () => {
                      await fs.register(directory.id, kind, directory.revision);
                      await open(directory);
                      setNotice(`Registered ${kind}.`);
                    })
                  }
                >
                  Register {kind}
                </button>
              ))}
              <small>Upload the matching metadata dotfile before registration.</small>
            </div>
          )}
          <details>
            <summary>Directory policy</summary>
            <button
              disabled={busy}
              onClick={() =>
                void run(async () => {
                  const value = await fs.policy(directory.id);
                  setPolicy({
                    revision: value.revision,
                    text: JSON.stringify(
                      { markings: value.markings, grants: value.grants },
                      null,
                      2,
                    ),
                  });
                })
              }
            >
              Load policy
            </button>
            {policy && (
              <>
                <label>
                  Policy JSON
                  <textarea
                    rows={12}
                    value={policy.text}
                    onChange={(event) => setPolicy({ ...policy, text: event.target.value })}
                  />
                </label>
                <button
                  disabled={busy}
                  onClick={() =>
                    void run(async () => {
                      await fs.setPolicy(directory.id, policy.revision, JSON.parse(policy.text));
                      await open(directory);
                      setNotice('Policy updated.');
                    })
                  }
                >
                  Save policy
                </button>
              </>
            )}
          </details>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Type</th>
                  <th>Revision</th>
                </tr>
              </thead>
              <tbody>
                {entries.map((entry) => (
                  <tr key={entry.id}>
                    <td>
                      <button
                        disabled={busy}
                        onClick={() =>
                          void run(async () =>
                            entry.kind === 'directory' ? open(entry) : inspect(entry),
                          )
                        }
                      >
                        {entry.name}
                      </button>
                    </td>
                    <td>{entry.kind}</td>
                    <td>{entry.revision}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {!entries.length && <p>No visible entries on this page.</p>}
          {cursor && (
            <button disabled={busy} onClick={() => void run(async () => open(directory, true))}>
              Load more
            </button>
          )}
          <form
            onSubmit={(event) => {
              event.preventDefault();
              void run(async () => {
                await fs.changes(
                  [
                    {
                      op: 'create',
                      kind: 'directory',
                      parentId: directory.id,
                      expectedParentRevision: directory.revision,
                      name,
                    },
                  ],
                  { idempotencyKey: crypto.randomUUID() },
                );
                setName('');
                await open(directory);
              });
            }}
          >
            <label>
              New directory{' '}
              <input required value={name} onChange={(event) => setName(event.target.value)} />
            </label>{' '}
            <button disabled={busy}>Create directory</button>
            <button
              type="button"
              disabled={busy || !name}
              onClick={() =>
                void run(async () => {
                  await fs.createProject(directory.id, name, directory.revision);
                  setName('');
                  await open(directory);
                })
              }
            >
              Create project
            </button>
          </form>
          <label>
            Upload a new file{' '}
            <input
              type="file"
              disabled={busy}
              onChange={(event) => {
                const file = event.target.files?.[0];
                if (file)
                  void run(async () => upload(new Uint8Array(await file.arrayBuffer()), file.name));
                event.target.value = '';
              }}
            />
          </label>
          {(role?.kind === 'ontology' || role?.kind === 'repository') && (
            <div>
              <button
                disabled={busy}
                onClick={() =>
                  void run(async () => {
                    const { publishBrowserSource } =
                      await import('../../../packages/platform/src/source.js');
                    const release = await publishBrowserSource(fs, directory.id, setNotice);
                    const proposed = await fs.plan(directory.id, release.digest);
                    setPlan(proposed);
                    setNotice('Release published. Review the activation plan below.');
                  })
                }
              >
                Publish and plan
              </button>
              <p>Repository CI can publish its exported resource definitions with the CLI.</p>
              <button
                disabled={busy}
                onClick={() =>
                  void run(async () => {
                    setRuntime(await fs.runtimeStatus(directory.id));
                    setNotice('Checked current native readiness for your access.');
                  })
                }
              >
                Check runtime readiness
              </button>
            </div>
          )}
          {plan && (
            <div>
              <h3>Activation plan</h3>
              <pre>{JSON.stringify(plan, null, 2)}</pre>
              <button
                disabled={busy}
                onClick={() =>
                  void run(async () => {
                    const result = await fs.apply(plan.id);
                    setRuntime(result.runtime);
                    setPlan(undefined);
                    setNotice(
                      `Activated definitions at revision ${result.revision}. ${result.runtimeConfigured ? 'Native configuration is complete.' : 'Native configuration is pending.'}`,
                    );
                  })
                }
              >
                Apply this plan
              </button>
            </div>
          )}
          {runtime && (
            <section aria-label="Runtime readiness">
              <h3>Runtime readiness</h3>
              <p>
                {runtime.withheld ? (
                  'Operational readiness is withheld. Instance management permission is required.'
                ) : (
                  <>
                    {runtime.configured
                      ? 'Native configuration is complete.'
                      : 'Native configuration is pending.'}{' '}
                    {runtime.ready ? 'Ready for your access.' : 'Not ready for your access.'}
                  </>
                )}
              </p>
              <pre>{JSON.stringify(runtime, null, 2)}</pre>
            </section>
          )}
        </>
      )}
      {selected && (
        <section aria-label="Selected file">
          <h3>{selected.name}</h3>
          <pre>{JSON.stringify(selected, null, 2)}</pre>
          <button
            disabled={busy}
            onClick={() =>
              void run(async () => {
                await fs.changes(
                  [
                    {
                      op: 'delete',
                      id: selected.id,
                      expectedRevision: selected.revision,
                      expectedParentRevision: directory!.revision,
                    },
                  ],
                  { idempotencyKey: crypto.randomUUID() },
                );
                await open(directory!);
                setNotice('Entry deleted. Retained history remains available by ID.');
              })
            }
          >
            Delete entry
          </button>
          {selected.kind === 'resource' && (
            <button
              disabled={busy}
              onClick={() =>
                void run(async () => {
                  const resource = await fs.resource(selected.resourceId!);
                  setNotice(JSON.stringify(resource, null, 2));
                  if (!isResource(resource.resource))
                    throw new Error('Unsupported resource definition');
                  setResolved({ resourceId: resource.resourceId, resource: resource.resource });
                })
              }
            >
              Resolve resource
            </button>
          )}
          {resolved && selected.resourceId === resolved.resourceId && (
            <ResourceInspector key={resolved.resourceId} {...resolved} />
          )}
          {selected.kind === 'file' && (
            <button
              disabled={busy}
              onClick={() =>
                void run(async () => {
                  const response = await fs.readFile(selected.id, selected.revision);
                  const url = URL.createObjectURL(await response.blob());
                  const link = document.createElement('a');
                  link.href = url;
                  link.download = selected.name;
                  link.click();
                  setTimeout(() => URL.revokeObjectURL(url), 1000);
                })
              }
            >
              Download
            </button>
          )}
          {selected.content &&
            selected.content.size <= 1024 * 1024 &&
            /^(text\/|application\/json)/.test(selected.content.mediaType) && (
              <>
                <label>
                  File contents
                  <textarea
                    rows={16}
                    value={text}
                    onChange={(event) => setText(event.target.value)}
                  />
                </label>
                <button
                  disabled={busy}
                  onClick={() =>
                    void run(async () =>
                      upload(new TextEncoder().encode(text), selected.name, selected),
                    )
                  }
                >
                  Save new revision
                </button>
              </>
            )}
          <details>
            <summary>Retained revisions</summary>
            <ul>
              {history.map((version) => (
                <li key={version.revision}>
                  <button
                    disabled={busy}
                    onClick={() =>
                      void run(async () => {
                        const content = await fs.readFile(version.id, version.revision);
                        if ((version.content?.size ?? Infinity) > 1024 * 1024)
                          throw new Error('Download large revisions with the CLI.');
                        setText(await content.text());
                        setNotice(
                          `Viewing retained revision ${version.revision}. Saving uses the current head.`,
                        );
                      })
                    }
                  >
                    Revision {version.revision}
                    {version.deleted ? ' · deleted' : ''}
                  </button>
                </li>
              ))}
            </ul>
          </details>
        </section>
      )}
    </section>
  );
}
