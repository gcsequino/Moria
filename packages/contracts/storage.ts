/** Opaque provider-independent location; possession never grants access. */
export interface StorageLocation {
  readonly version: 1;
  readonly warehouseId: string;
  readonly tableId: string;
  readonly objectId: string;
}
