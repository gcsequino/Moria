import test from 'node:test';
import { connectorDiscovery } from './fixtures/native-connectors/discovery.js';

test(
  'native Connector schema discovery retains read-only authority and typed immutable proposals',
  { timeout: 300000 },
  connectorDiscovery,
);
