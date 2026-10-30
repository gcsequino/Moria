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
  /** Must compose an existing createAuthorized*Storage adapter with these arguments. */
  readonly storage: (
    scope: StorageAuthorityScope,
    authority: TrustedStorageAuthority,
  ) => Promise<StreamingStorage>;
}

export interface PublishedMetadata {
  readonly markings: readonly string[];
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
      return Object.freeze({ ...binding, markings: Object.freeze([...binding.markings]) });
    });
  }

  async read(
    authenticated: SessionContext,
    dataset: string,
    signal?: AbortSignal,
  ): Promise<PublishedMetadata> {
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
    authorize(state.identity, dataset, 'read', binding.markings);
    const catalog = this.authority.catalog;
    let filesystemMarkings: string[] | undefined;
    const checkSession = async (client: import('pg').PoolClient) => {
      const labels = await this.authority.resourceAccess(client, session, dataset);
      if (filesystemMarkings && JSON.stringify(labels) !== JSON.stringify(filesystemMarkings))
        throw new MoriaError(
          'AUTHORIZATION_CHANGED',
          'Dataset filesystem markings changed during transfer',
        );
      filesystemMarkings = labels;
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
        await checkSession(client);
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
      checkCurrent: async () => {
        // A new transaction per check observes current revocation, expiry and policy epoch.
        await catalog.transaction(checkSession, session.environment, 'repeatable-read', 'shared');
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
    const stream = await storage.read(logical, { signal });
    return Object.freeze({
      markings: Object.freeze([...new Set([...binding.markings, ...filesystemMarkings!])]),
      environment: session.environment,
      dataset,
      revision: pinned.revision,
      snapshotId: pinned.snapshotId,
      execution: pinned.execution,
      stream,
    });
  }
}
