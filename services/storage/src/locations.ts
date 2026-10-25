/** Logical identities contain no provider paths, secrets, or implied grants. */
export interface StorageLocation {
  readonly version: 1;
  readonly warehouseId: string;
  readonly tableId: string;
  readonly objectId: string;
}

export type StorageErrorCode =
  | 'INVALID_INPUT'
  | 'BINDING_MISMATCH'
  | 'UNSAFE_PATH'
  | 'NOT_FOUND'
  | 'ALREADY_EXISTS'
  | 'LIMIT_EXCEEDED'
  | 'CANCELLED'
  | 'UNSUPPORTED_CAPABILITY'
  | 'IO_ERROR';

export class StorageError extends Error {
  constructor(
    readonly code: StorageErrorCode,
    /** The final name was created, but successful write acknowledgment was not reached. */
    readonly writeOutcome?: 'uncertain',
  ) {
    super(code);
    this.name = 'StorageError';
  }
}

export function identity(value: unknown): asserts value is string {
  if (typeof value !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$/.test(value)) {
    throw new StorageError('INVALID_INPUT');
  }
}

export function location(value: unknown): StorageLocation {
  if (!value || typeof value !== 'object') {
    throw new StorageError('INVALID_INPUT');
  }
  const record = value as Record<string, unknown>;
  if (
    record.version !== 1 ||
    Object.keys(record).sort().join(',') !== 'objectId,tableId,version,warehouseId'
  ) {
    throw new StorageError('INVALID_INPUT');
  }
  identity(record.warehouseId);
  identity(record.tableId);
  identity(record.objectId);
  return Object.freeze({
    version: 1,
    warehouseId: record.warehouseId,
    tableId: record.tableId,
    objectId: record.objectId,
  });
}

export interface TransferOptions {
  readonly signal?: AbortSignal;
}

/** Range is [offset, offset + length), strictly within the object. */
export interface ReadOptions extends TransferOptions {
  readonly range?: { readonly offset: number; readonly length: number };
}

export interface WriteOptions extends TransferOptions {
  readonly size: number;
}

export interface StreamingStorage {
  readonly version: 1;
  read(location: StorageLocation, options?: ReadOptions): Promise<ReadableStream<Uint8Array>>;
  write(
    location: StorageLocation,
    source: ReadableStream<Uint8Array>,
    options: WriteOptions,
  ): Promise<void>;
  requireCapability(capability: string): void;
}
