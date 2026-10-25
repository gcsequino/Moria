import assert from 'node:assert/strict';
import fs from 'node:fs';
import promises from 'node:fs/promises';
import { syncBuiltinESMExports } from 'node:module';
import { startS3 } from './server.js';
import { createS3Storage } from '../../../services/storage/src/s3.js';
import { logical, limits, source, collect } from '../storage-contract/helpers.js';

// Intercept before any filesystem access; no credential/config file is opened.
let attempts = 0;
function guard(path: unknown) {
  const value = String(path);
  if (value.includes('.aws') || value.includes('moria-forbidden-profile')) {
    attempts++;
    throw new Error('Ambient profile access forbidden');
  }
}
const read = promises.readFile.bind(promises);
promises.readFile = ((...args: Parameters<typeof promises.readFile>) => {
  guard(args[0]);
  return read(...args);
}) as typeof promises.readFile;
const readCallback = fs.readFile.bind(fs);
fs.readFile = ((...args: Parameters<typeof fs.readFile>) => {
  guard(args[0]);
  return readCallback(...args);
}) as typeof fs.readFile;
const readSync = fs.readFileSync.bind(fs);
fs.readFileSync = ((...args: Parameters<typeof fs.readFileSync>) => {
  guard(args[0]);
  return readSync(...args);
}) as typeof fs.readFileSync;
syncBuiltinESMExports();
process.env.AWS_CONFIG_FILE = '/moria-forbidden-profile-config';
process.env.AWS_SHARED_CREDENTIALS_FILE = '/moria-forbidden-profile-credentials';
const server = await startS3();
try {
  const storage = await createS3Storage({ ...limits, ...server, prefix: '', region: 'us-east-1' });
  await storage.write(logical, source([[1, 2]]), { size: 2 });
  assert.deepEqual(await collect(storage), [1, 2]);
  assert.equal(attempts, 0);
} finally {
  await server.close();
}
