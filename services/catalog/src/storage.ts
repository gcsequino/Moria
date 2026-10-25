import {
  scopedObjects,
  scopedRows,
  type ScopedObject,
} from '../../../workers/iceberg/scoped-read.js';
import { authorize } from '../../../packages/authorization/src/access.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import type { SessionContext } from '../../ontology/src/service.js';
import { Authority } from '../../security/src/authority.js';
import { cancelled, transferSignal } from '../../storage/src/transfer.js';
import type {
  StorageAuthorityScope,
  TrustedStorageAuthority,
} from '../../storage/src/authority.js';
import {
  identity,
  location,
  StorageError,
  type StorageLocation,
  type StreamingStorage,
} from '../../storage/src/locations.js';

import type { Publication, Receipt } from './catalog.js';
import {
  metadataBytes,
  metadataLimit,
  metadataObjectId,
  metadataStream,
  validateMetadata,
  type MetadataPublication,
} from './storage-publication.js';

const prefix = 'moria-storage:v1:';

/** Canonical encoding: duplicate keys, alternate spellings and extra fields are rejected. */
export function encodeMetadataLocation(value: StorageLocation): string {
  return prefix + JSON.stringify(location(value));
}

export function decodeMetadataLocation(value: string): StorageLocation {
  try {
    if (typeof value !== 'string' || value.length > 512 || !value.startsWith(prefix)) {
      throw new StorageError('INVALID_INPUT');
    }
    const decoded = location(JSON.parse(value.slice(prefix.length)));
    if (encodeMetadataLocation(decoded) !== value) {
      throw new StorageError('INVALID_INPUT');
    }
    return decoded;
  } catch {
    throw new StorageError('INVALID_INPUT');
  }
}

/** Service-owned Dataset resolution, never populated from request fields. */
export interface CatalogStorageBinding {
  readonly environment: string;
  readonly dataset: string;
  readonly kind: 'Dataset';
  /** Complete effective namespace/resource/inherited markings for this binding. */
  readonly markings: readonly string[];
  readonly warehouseId: string;
  readonly tableId: string;
  /** Required for publication; service-owned Iceberg table UUID, never request-selected. */
  readonly icebergTableUuid?: string;
  /** Exact immutable engine names, supplied only by trusted service composition. */
  readonly readObjects?: readonly ScopedObject[];
  /** Must compose an existing createAuthorized*Storage adapter with these arguments. */
  readonly storage: (
    scope: StorageAuthorityScope,
    authority: TrustedStorageAuthority,
  ) => Promise<StreamingStorage>;
}

export interface PublishedMetadata {
  readonly environment: string;
  readonly dataset: string;
  readonly revision: string;
  readonly snapshotId: string;
  readonly execution: string;
  readonly stream: ReadableStream<Uint8Array>;
}

/** Trusted service seam only; no public route, storage handle or caller-selected location. */
export class CatalogStorage {
  private readonly bindings: readonly CatalogStorageBinding[];

  constructor(
    private readonly authority: Authority,
    bindings: readonly CatalogStorageBinding[],
  ) {
    const keys = new Set<string>();
    this.bindings = bindings.map((binding) => {
      const key = JSON.stringify([binding.environment, binding.dataset]);
      identity(binding.warehouseId);
      identity(binding.tableId);
      if (
        binding.kind !== 'Dataset' ||
        !binding.environment ||
        !binding.dataset ||
        !Array.isArray(binding.markings) ||
        binding.markings.some((marking) => typeof marking !== 'string' || !marking) ||
        typeof binding.storage !== 'function' ||
        keys.has(key)
      ) {
        throw new StorageError('INVALID_INPUT');
      }
      keys.add(key);
      return Object.freeze({
        ...binding,
        markings: Object.freeze([...binding.markings]),
        readObjects:
          binding.readObjects === undefined
            ? undefined
            : scopedObjects(binding.readObjects, binding.warehouseId, binding.tableId),
      });
    });
  }

  async publish(
    authenticated: SessionContext,
    request: MetadataPublication,
    signal?: AbortSignal,
  ): Promise<Receipt> {
    signal = transferSignal({ signal });
    cancelled(signal);
    const session = Object.freeze({ ...authenticated });
    if (!session.sessionHash) {
      throw new MoriaError('UNAUTHENTICATED', 'An authenticated session is required');
    }
    // Capture declarations before awaiting engine streams or authority. No caller callbacks
    // or mutable request objects are passed to the publication transaction.
    const exactKeys = (value: object, allowed: string[]) => {
      if (Object.keys(value).some((key) => !allowed.includes(key))) {
        throw new StorageError('INVALID_INPUT');
      }
    };
    exactKeys(request, ['execution', 'worker', 'fence', 'lineage', 'checkpoint', 'outputs']);
    for (const output of request.outputs) {
      exactKeys(output, ['dataset', 'expectedRevision', 'snapshotId', 'size', 'stream']);
    }
    const declaration = structuredClone({
      execution: request.execution,
      worker: request.worker,
      fence: request.fence,
      lineage: request.lineage,
      checkpoint: request.checkpoint,
      outputs: request.outputs.map(({ stream: _stream, ...output }) => output),
    });
    const streams = request.outputs.map((output) => output.stream);
    if (
      !declaration.execution ||
      !declaration.worker ||
      !/^[1-9][0-9]*$/.test(declaration.fence) ||
      declaration.outputs.length < 1 ||
      declaration.outputs.length > 16 ||
      new Set(declaration.outputs.map((output) => output.dataset)).size !==
        declaration.outputs.length ||
      declaration.outputs.some(
        (output) =>
          !/^[0-9]+$/.test(output.expectedRevision) ||
          !/^[1-9][0-9]*$/.test(output.snapshotId) ||
          !Number.isSafeInteger(output.size) ||
          output.size < 1 ||
          output.size > metadataLimit,
      )
    ) {
      throw new StorageError('INVALID_INPUT');
    }
    const bindingFor = (dataset: string) => {
      const binding = this.bindings.find(
        (entry) => entry.environment === session.environment && entry.dataset === dataset,
      );
      if (!binding) {
        throw new StorageError('AUTHORITY_DENIED');
      }
      return binding;
    };
    const bindings = declaration.outputs.map((output) => bindingFor(output.dataset));
    if (bindings.some((binding) => !binding.icebergTableUuid)) {
      throw new StorageError('BINDING_MISMATCH');
    }
    const state = await this.authority.current(
      session.environment,
      session.principal,
      session.application,
    );
    for (const binding of bindings) {
      authorize(state.identity, binding.dataset, 'write', binding.markings);
    }
    for (const dataset of Object.keys(declaration.lineage)) {
      const input = bindingFor(dataset);
      authorize(state.identity, dataset, 'read', input.markings);
      if (
        bindings.some((binding) =>
          input.markings.some((marking) => !binding.markings.includes(marking)),
        )
      ) {
        throw new StorageError('AUTHORITY_DENIED');
      }
    }
    const catalog = this.authority.catalog;
    const checkSession = async (client: import('pg').PoolClient) => {
      cancelled(signal);
      await this.authority.assertCurrent(
        client,
        session.environment,
        session.principal,
        session.application,
        state.epoch,
        session.sessionHash,
      );
      cancelled(signal);
    };
    const check = async () => {
      await catalog.transaction(checkSession, session.environment, 'repeatable-read', 'shared');
    };
    const current: TrustedStorageAuthority = {
      checkCurrent: async () => {
        await check();
        return true;
      },
    };
    await check();
    const candidates: {
      binding: CatalogStorageBinding;
      bytes: Buffer;
      logical: StorageLocation;
    }[] = [];
    for (const [index, output] of declaration.outputs.entries()) {
      const binding = bindings[index];
      const bytes = await metadataBytes(streams[index], output.size, check, signal);
      await validateMetadata(bytes, binding.icebergTableUuid!, output.snapshotId, signal);
      const logical = location({
        version: 1,
        warehouseId: binding.warehouseId,
        tableId: binding.tableId,
        objectId: metadataObjectId(
          session.environment,
          declaration.execution,
          output.dataset,
          bytes,
        ),
      });
      candidates.push({ binding, bytes, logical });
    }
    const publication: Publication = {
      environment: session.environment,
      execution: declaration.execution,
      worker: declaration.worker,
      fence: declaration.fence,
      securityEpoch: state.epoch,
      lineage: declaration.lineage,
      checkpoint: declaration.checkpoint,
      outputs: declaration.outputs.map((output, index) => ({
        dataset: output.dataset,
        expectedRevision: output.expectedRevision,
        snapshotId: output.snapshotId,
        metadataLocation: encodeMetadataLocation(candidates[index].logical),
      })),
    };
    // This is only an optimization for acknowledgment retry. Catalog.publish alone
    // validates the request hash and returns the receipt under current authority.
    const committed = async () => {
      await check();
      return (
        (
          await catalog.pool.query(
            'SELECT 1 FROM moria.publications WHERE environment=$1 AND execution=$2',
            [session.environment, declaration.execution],
          )
        ).rowCount === 1
      );
    };
    if (!(await committed())) {
      for (const { binding, bytes, logical } of candidates) {
        await check();
        const storage = await binding.storage(
          Object.freeze({ location: logical, operations: Object.freeze(['create'] as const) }),
          current,
        );
        try {
          await storage.write(logical, metadataStream(bytes), { size: bytes.length, signal });
        } catch (error) {
          // An object collision is never evidence of matching bytes or durability.
          // Only an already committed catalog receipt can resolve a concurrent retry.
          if (
            error instanceof StorageError &&
            error.code === 'ALREADY_EXISTS' &&
            (await committed())
          ) {
            return catalog.publish(publication, checkSession);
          }
          throw error;
        }
      }
    }
    await check();
    return catalog.publish(publication, checkSession);
  }

  async readRows(
    authenticated: SessionContext,
    dataset: string,
    options: { signal?: AbortSignal; timeoutMs?: number } = {},
  ) {
    options = Object.freeze({ ...options });
    const signal = transferSignal(options);
    const timeout = options.timeoutMs ?? 60000;
    if (!Number.isSafeInteger(timeout) || timeout < 1 || timeout > 60000) {
      throw new StorageError('INVALID_INPUT');
    }
    cancelled(signal);
    const controller = new AbortController();
    const stop = () => {
      controller.abort();
      clearTimeout(timer);
      signal?.removeEventListener('abort', stop);
    };
    const timer = setTimeout(stop, timeout);
    signal?.addEventListener('abort', stop, { once: true });
    try {
      let onAbort: (() => void) | undefined;
      const aborted = new Promise<never>((_resolve, reject) => {
        onAbort = () => reject(new StorageError('CANCELLED'));
        controller.signal.addEventListener('abort', onAbort, { once: true });
      });
      let pinned;
      try {
        // Observe the entire open even after cancellation: Catalog.transaction
        // retains its client until rollback/unlock/release have safely completed.
        pinned = await Promise.race([
          this.openRead(authenticated, dataset, controller.signal),
          aborted,
        ]);
        cancelled(controller.signal);
      } finally {
        if (onAbort) controller.signal.removeEventListener('abort', onAbort);
      }
      const { binding, current, logical, storage, ...provenance } = pinned;
      if (!binding.icebergTableUuid || !binding.readObjects) {
        throw new StorageError('BINDING_MISMATCH');
      }
      const rows = scopedRows(
        {
          metadata: (signal) => storage.read(logical, { signal }),
          snapshotId: pinned.snapshotId,
          tableUuid: binding.icebergTableUuid,
          objects: binding.readObjects,
          check: async (signal) => {
            await current.checkCurrent({ location: logical, operation: 'read' }, signal);
          },
          read: async (object, offset, length, signal) => {
            const storage = await binding.storage(
              Object.freeze({
                location: object.location,
                operations: Object.freeze(['read'] as const),
              }),
              current,
            );
            return storage.read(object.location, { range: { offset, length }, signal });
          },
        },
        { ...options, signal: controller.signal },
      );
      // Own cleanup even when return/throw happens before the generator starts.
      const iterator = {
        [Symbol.asyncIterator]() {
          return this;
        },
        async next() {
          try {
            const result = await rows.next();
            if (result.done) stop();
            return result;
          } catch (error) {
            stop();
            throw error;
          }
        },
        async return(value?: void) {
          stop();
          return rows.return(value);
        },
        async throw(error?: unknown) {
          stop();
          return rows.throw(error);
        },
      };
      return Object.freeze({ ...provenance, rows: iterator });
    } catch (error) {
      stop();
      throw error;
    }
  }

  async read(
    authenticated: SessionContext,
    dataset: string,
    signal?: AbortSignal,
  ): Promise<PublishedMetadata> {
    const {
      binding: _binding,
      current: _current,
      logical,
      storage,
      ...published
    } = await this.openRead(authenticated, dataset, signal);
    return Object.freeze({ ...published, stream: await storage.read(logical, { signal }) });
  }

  private async openRead(authenticated: SessionContext, dataset: string, signal?: AbortSignal) {
    signal = transferSignal({ signal });
    cancelled(signal);
    // Snapshot trusted authentication before the first await; callers cannot retarget a stream.
    const session = Object.freeze({ ...authenticated });
    if (!session.sessionHash) {
      throw new MoriaError('UNAUTHENTICATED', 'An authenticated session is required');
    }
    const binding = this.bindings.find(
      (entry) => entry.environment === session.environment && entry.dataset === dataset,
    );
    if (!binding) {
      throw new StorageError('AUTHORITY_DENIED');
    }
    const state = await this.authority.current(
      session.environment,
      session.principal,
      session.application,
    );
    cancelled(signal);
    authorize(state.identity, dataset, 'read', binding.markings);
    const catalog = this.authority.catalog;
    const checkSession = async (client: import('pg').PoolClient) => {
      await this.authority.assertCurrent(
        client,
        session.environment,
        session.principal,
        session.application,
        state.epoch,
        session.sessionHash,
      );
    };
    const pinned = await catalog.transaction(
      async (client) => {
        cancelled(signal);
        await checkSession(client);
        cancelled(signal);
        // One MVCC statement resolves both pointers, including equality of snapshot and location.
        const result = await client.query<{
          revision: string;
          snapshotId: string;
          execution: string;
          metadataLocation: string;
        }>(
          `SELECT h.revision,h.snapshot_id AS "snapshotId",s.execution,
          h.metadata_location AS "metadataLocation"
          FROM moria.dataset_heads h JOIN moria.snapshot_history s
          ON s.environment=h.environment AND s.dataset=h.dataset AND s.revision=h.revision
          AND s.snapshot_id=h.snapshot_id AND s.metadata_location=h.metadata_location
          WHERE h.environment=$1 AND h.dataset=$2`,
          [session.environment, dataset],
        );
        if (result.rows.length !== 1) {
          throw new StorageError('NOT_FOUND');
        }
        return result.rows[0];
      },
      session.environment,
      'repeatable-read',
      'shared',
    );
    const logical = decodeMetadataLocation(pinned.metadataLocation);
    if (logical.warehouseId !== binding.warehouseId || logical.tableId !== binding.tableId) {
      throw new StorageError('BINDING_MISMATCH');
    }
    const current: TrustedStorageAuthority = {
      checkCurrent: async (_request, signal) => {
        cancelled(signal);
        // A new transaction per check observes current revocation, expiry and policy epoch.
        await catalog.transaction(
          async (client) => {
            cancelled(signal);
            await checkSession(client);
          },
          session.environment,
          'repeatable-read',
          'shared',
        );
        cancelled(signal);
        return true;
      },
    };
    // No provider initialization or network I/O occurs inside the SQL transaction.
    cancelled(signal);
    await current.checkCurrent({ location: logical, operation: 'read' }, signal);
    cancelled(signal);
    const storage = await binding.storage(
      Object.freeze({ location: logical, operations: Object.freeze(['read'] as const) }),
      current,
    );
    cancelled(signal);
    return Object.freeze({
      environment: session.environment,
      dataset,
      revision: pinned.revision,
      snapshotId: pinned.snapshotId,
      execution: pinned.execution,
      storage,
      binding,
      current,
      logical,
    });
  }
}
