import { createS3Storage, type S3StorageBinding } from './s3.js';
import { createLocalStorage, type LocalStorageBinding } from './local.js';
import {
  location,
  StorageError,
  type StorageLocation,
  type StreamingStorage,
} from './locations.js';

export type StorageOperation = 'read' | 'create' | 'delete';

/** Exact logical object scope selected by the trusted service, never a request grant. */
export interface StorageAuthorityScope {
  readonly location: StorageLocation;
  readonly operations: readonly StorageOperation[];
}

export interface StorageAuthorityRequest {
  readonly location: StorageLocation;
  readonly operation: StorageOperation;
}

/**
 * Integration seam for the service's existing current-policy authority. Bind the
 * authenticated execution/identity in this service-owned implementation. Only
 * literal true permits access; it must include current expiry/revocation checks.
 */
export interface TrustedStorageAuthority {
  checkCurrent(request: StorageAuthorityRequest, signal?: AbortSignal): Promise<boolean>;
}

/** No raw adapter, physical root, authority object or credentials escape this handle. */
export async function createAuthorizedLocalStorage(
  binding: LocalStorageBinding,
  scope: StorageAuthorityScope,
  authority: TrustedStorageAuthority,
): Promise<StreamingStorage> {
  return Object.freeze(
    await createLocalStorage(binding, createStorageVerifier(binding, scope, authority)),
  );
}

export async function createAuthorizedS3Storage(
  binding: S3StorageBinding,
  scope: StorageAuthorityScope,
  authority: TrustedStorageAuthority,
): Promise<StreamingStorage> {
  return Object.freeze(
    await createS3Storage(binding, createStorageVerifier(binding, scope, authority)),
  );
}

/** Shared exact scope and current-authority checks; adapters choose transfer boundaries. */
function createStorageVerifier(
  binding: { readonly warehouseId: string; readonly tableId: string },
  scope: StorageAuthorityScope,
  authority: TrustedStorageAuthority,
) {
  let scoped: StorageLocation;
  let operations: Set<StorageOperation>;
  let check: TrustedStorageAuthority['checkCurrent'];
  try {
    scoped = location(scope.location);
    if (
      !Array.isArray(scope.operations) ||
      scope.operations.some((operation) => !['read', 'create', 'delete'].includes(operation)) ||
      typeof authority.checkCurrent !== 'function'
    ) {
      throw new StorageError('INVALID_INPUT');
    }
    operations = new Set(scope.operations);
    check = authority.checkCurrent.bind(authority);
  } catch {
    throw new StorageError('INVALID_INPUT');
  }
  if (scoped.warehouseId !== binding.warehouseId || scoped.tableId !== binding.tableId) {
    throw new StorageError('BINDING_MISMATCH');
  }
  const verify = async (
    logical: StorageLocation,
    operation: StorageOperation,
    signal?: AbortSignal,
  ): Promise<void> => {
    if (
      logical.warehouseId !== scoped.warehouseId ||
      logical.tableId !== scoped.tableId ||
      logical.objectId !== scoped.objectId ||
      !operations.has(operation)
    ) {
      throw new StorageError('AUTHORITY_DENIED');
    }
    if (signal?.aborted) {
      throw new StorageError('CANCELLED');
    }
    let abort: (() => void) | undefined;
    try {
      const decision = Promise.resolve().then(() =>
        check(Object.freeze({ location: logical, operation }), signal),
      );
      const cancelled = new Promise<never>((_resolve, reject) => {
        abort = () => reject(new StorageError('CANCELLED'));
        if (signal) {
          EventTarget.prototype.addEventListener.call(signal, 'abort', abort, { once: true });
          if (signal.aborted) {
            abort();
          }
        }
      });
      if ((await Promise.race([decision, cancelled])) !== true) {
        throw new StorageError('AUTHORITY_DENIED');
      }
    } catch {
      throw new StorageError(signal?.aborted ? 'CANCELLED' : 'AUTHORITY_DENIED');
    } finally {
      if (signal && abort) {
        EventTarget.prototype.removeEventListener.call(signal, 'abort', abort);
      }
    }
  };
  return verify;
}
