import type { ValueType } from './types.js';
export interface ObjectSchema {
  primaryKey: string;
  properties: Record<string, ValueType>;
}
const str: ValueType = { type: 'string', nullable: false },
  num: ValueType = { type: 'float64', nullable: false },
  optional: ValueType = { type: 'float64', nullable: true };
const uid: ValueType = { type: 'uid', nullable: false };
export const stationSchema: ObjectSchema = {
  primaryKey: 'uid',
  properties: {
    uid,
    triplet: str,
    name: str,
    state: str,
    latitude: num,
    longitude: num,
    elevationFt: num,
    timeZoneOffset: optional,
    county: str,
    retrievedAt: str,
  },
};
export const observationSchema: ObjectSchema = {
  primaryKey: 'uid',
  properties: {
    uid,
    triplet: str,
    date: { type: 'date', nullable: false },
    snowDepth: optional,
    swe: optional,
    depthQc: str,
    depthQa: str,
    sweQc: str,
    sweQa: str,
    unit: str,
    periodRef: str,
    retrievedAt: str,
  },
};

export const assetFields = ['id', 'title', 'region', 'status', 'notes'] as const;
export const assetSchema: ObjectSchema = {
  primaryKey: 'uid',
  properties: Object.fromEntries(
    ['uid', ...assetFields].map((k) => [
      k,
      { type: k === 'uid' ? 'uid' : 'string', nullable: false },
    ]),
  ),
};
export const assetActionInputs: Record<string, Record<string, ValueType>> = {
  'edit-asset': Object.fromEntries(
    ['uid', 'title', 'status', 'notes'].map((k) => [k, assetSchema.properties[k]]),
  ),
  'reset-asset': { uid: assetSchema.properties.uid },
  ...Object.fromEntries(
    ['title', 'status', 'notes'].map((k) => [
      'edit-' + k,
      { uid: assetSchema.properties.uid, [k]: assetSchema.properties[k] },
    ]),
  ),
};
